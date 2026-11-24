//! Durability around the local backend's atomic publications.
use crate::Error;
use object_store::{local::LocalFileSystem, path::Path};
use std::{
    collections::HashSet,
    fs::File,
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) struct Local {
    pub(crate) backend: LocalFileSystem,
    durable_dirs: Mutex<HashSet<PathBuf>>,
}
impl Local {
    pub(crate) fn open(root: PathBuf, read_only: bool) -> Result<Arc<Self>, Error> {
        if !read_only {
            std::fs::create_dir_all(&root)?;
            // The backend only syncs directories below its prefix. Persist the prefix itself,
            // including any newly created ancestors, before it can hold acknowledged objects.
            for dir in std::fs::canonicalize(&root)?.ancestors() {
                File::open(dir)?.sync_all()?;
            }
        }
        let root = std::fs::canonicalize(root)?;
        let backend = LocalFileSystem::new_with_prefix(&root)?.with_fsync(true);
        Ok(Arc::new(Self {
            backend,
            durable_dirs: Mutex::new(HashSet::from([root])),
        }))
    }

    pub(crate) async fn prepare(self: &Arc<Self>, path: &Path) -> Result<(), Error> {
        let path = self.backend.path_to_filesystem(path)?;
        if path.parent().is_some_and(|dir| {
            self.durable_dirs
                .try_lock()
                .is_ok_and(|known| known.contains(dir))
        }) {
            return Ok(());
        }
        // A contended lock can be held across directory fsyncs. Wait on the blocking pool,
        // rather than making a runtime worker wait for another writer's disk I/O.
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            // Directory creation must finish durably before another local writer can use it.
            // Otherwise, a concurrent create_dir_all can see the directory before the creator
            // has synced its parent. Directories are never removed by this backend, so remember
            // completed barriers instead of syncing every ancestor on every WAL group.
            let mut known = this.durable_dirs.lock().expect("local directory lock");
            let mut missing = Vec::new();
            let mut dir = path
                .parent()
                .ok_or(Error::Config("object needs a parent"))?;
            while !known.contains(dir) {
                missing.push(dir.to_path_buf());
                dir = dir
                    .parent()
                    .ok_or(Error::Config("object outside local root"))?;
            }
            for dir in missing.into_iter().rev() {
                std::fs::create_dir_all(&dir)?;
                File::open(&dir)?.sync_all()?;
                File::open(dir.parent().expect("directory below root"))?.sync_all()?;
                known.insert(dir);
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Config("local directory task failed"))?
    }

    pub(crate) async fn recover(self: &Arc<Self>, path: &Path) -> Result<(), Error> {
        let path = self.backend.path_to_filesystem(path)?;
        tokio::task::spawn_blocking(move || {
            // Visibility alone cannot resolve an fsync failure after link/rename. Also covers
            // an identical retry racing the original publisher before its directory fsync.
            File::open(&path)?.sync_all()?;
            File::open(path.parent().expect("object below root"))?.sync_all()?;
            Ok(())
        })
        .await
        .map_err(|_| Error::Config("local recovery task failed"))?
    }
}
