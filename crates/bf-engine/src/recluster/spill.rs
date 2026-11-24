//! Local spill ownership and recovery. Locks are local only, and never touch the bucket format.
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

/// An open spill, removed on every exit, including cancellation and panic unwinding.
/// Each build has a process-owned directory, so concurrent builds have independent lifetimes.
pub(crate) struct Spill {
    pub file: Arc<File>,
    directory: PathBuf,
    // Held until after directory removal. A crashed process releases it automatically.
    _owner: File,
    root: File,
}

/// Lock the root directory itself: unlike an auxiliary lock file, it need not be left behind.
/// Creation, recovery, and removal share this lock, including the interval before owner.lock exists.
fn root_lock(root: &Path) -> io::Result<File> {
    let file = File::open(root)?;
    file.lock()?;
    Ok(file)
}

/// Sweep only directories whose ownership lock can be acquired. Missing locks, unexpected names,
/// symlinks, and legacy loose spills are left alone: their ownership cannot be proved here.
fn sweep_locked(root: &Path) -> io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir()
            || !entry.file_name().to_string_lossy().starts_with("process-")
        {
            continue;
        }
        let directory = entry.path();
        let Ok(owner) = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join("owner.lock"))
        else {
            continue;
        };
        match owner.try_lock() {
            Ok(()) => fs::remove_dir_all(directory)?,
            Err(std::fs::TryLockError::WouldBlock) => (),
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }
    }
    Ok(())
}

/// Recover abandoned spills at startup, without creating a scratch directory for idle engines.
pub(crate) fn sweep(root: &Path) -> io::Result<()> {
    if !root.try_exists()? {
        return Ok(());
    }
    let _root = root_lock(root)?;
    sweep_locked(root)
}

impl Spill {
    /// The exclusively owned directory, for additional disposable construction files.
    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }

    pub(crate) fn create(root: &Path, bytes: u64) -> io::Result<Self> {
        fs::create_dir_all(root)?;
        let root_file = root_lock(root)?;
        sweep_locked(root)?;
        let directory = root.join(format!(
            "process-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&directory)?;
        // Arm cleanup before any fallible file operation.
        let result = (|| {
            let owner = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(directory.join("owner.lock"))?;
            owner.lock()?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(directory.join("rows.bin"))?;
            file.set_len(bytes)?;
            Ok((owner, file))
        })();
        let (owner, file) = match result {
            Ok(files) => files,
            Err(error) => {
                let _ = fs::remove_dir_all(&directory);
                return Err(error);
            }
        };
        let spill = Self {
            file: Arc::new(file),
            directory,
            _owner: owner,
            root: root_file,
        };
        spill.root.unlock()?;
        Ok(spill)
    }
}

impl Drop for Spill {
    fn drop(&mut self) {
        // The owner lock remains held. Serializing with sweep avoids removing owner.lock while
        // another creator is examining it. Open worker descriptors survive the unlink.
        if let Err(error) = self.root.lock().and_then(|()| {
            let result = fs::remove_dir_all(&self.directory);
            let unlocked = self.root.unlock();
            result.and(unlocked)
        }) {
            tracing::warn!(%error, "recluster: spill cleanup failed; recovery will retry");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::FileExt;

    #[test]
    fn panic_unlinks_even_while_a_worker_has_the_file_open() {
        let root = tempfile::tempdir().unwrap();
        let mut worker = None;
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let spill = Spill::create(root.path(), 64).unwrap();
                worker = Some(spill.file.clone());
                panic!("injected build panic");
            }))
            .is_err()
        );
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
        worker.unwrap().write_all_at(&[7], 0).unwrap();
    }

    // Run this helper in a separate process, whose lock must survive a sweep in the parent.
    #[test]
    fn child_owns_spill() {
        let Some(root) = std::env::var_os("BF_TEST_SPILL_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let spill = Spill::create(&root, 64).unwrap();
        spill.file.write_all_at(&[42], 0).unwrap();
        fs::write(root.join("ready"), b"").unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn sweep_keeps_another_process_and_recovers_it_after_death() {
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let root = tempfile::tempdir().unwrap();
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "recluster::spill::tests::child_owns_spill"])
                .env("BF_TEST_SPILL_ROOT", root.path())
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        let start = std::time::Instant::now();
        while !root.path().join("ready").exists() {
            assert!(start.elapsed() < std::time::Duration::from_secs(10));
            assert!(child.0.try_wait().unwrap().is_none());
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        fs::remove_file(root.path().join("ready")).unwrap();
        sweep(root.path()).unwrap();
        let directory = fs::read_dir(root.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(fs::read(directory.join("rows.bin")).unwrap()[0], 42);
        drop(child);
        sweep(root.path()).unwrap();
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
