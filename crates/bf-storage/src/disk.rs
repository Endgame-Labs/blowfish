//! Disposable, checksummed, file-per-block LRU storage. All calls run on blocking workers.
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

const HEADER: usize = 40;
const MAGIC: &[u8; 8] = b"BFCACHE1";

pub(crate) fn digest(input: impl AsRef<[u8]>) -> String {
    Sha256::digest(input.as_ref())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

pub(crate) struct Disk {
    dir: PathBuf,
    budget: u64,
    bytes: u64,
    clock: u64,
    entries: HashMap<String, (u64, u64)>,
    order: BTreeSet<(u64, String)>,
    _lock: File,
}

impl Disk {
    pub(crate) fn open(dir: &Path, namespace: &str, budget: u64) -> io::Result<Self> {
        let dir = dir.join(digest(namespace));
        fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("lock"))?;
        lock.try_lock().map_err(io::Error::other)?;
        let mut disk = Self {
            dir,
            budget,
            bytes: 0,
            clock: 0,
            entries: HashMap::new(),
            order: BTreeSet::new(),
            _lock: lock,
        };
        let mut files = Vec::new();
        for entry in fs::read_dir(&disk.dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".tmp") {
                fs::remove_file(entry.path())?;
            } else if name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
                let meta = entry.metadata()?;
                if meta.is_file() {
                    files.push((meta.modified()?, name, meta.len()));
                }
            }
        }
        files.sort();
        for (_, name, size) in files {
            disk.track(name, size);
        }
        disk.evict(0)?;
        metrics::gauge!("blowfish_cache_bytes", "tier" => "disk").increment(disk.bytes as f64);
        Ok(disk)
    }

    fn track(&mut self, name: String, size: u64) {
        self.clock += 1;
        self.entries.insert(name.clone(), (size, self.clock));
        self.order.insert((self.clock, name));
        self.bytes += size;
    }

    fn remove(&mut self, name: &str) -> io::Result<()> {
        match fs::remove_file(self.dir.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        if let Some((size, clock)) = self.entries.remove(name) {
            self.order.remove(&(clock, name.to_owned()));
            self.bytes -= size;
        }
        Ok(())
    }

    fn evict(&mut self, incoming: u64) -> io::Result<()> {
        while self.bytes.saturating_add(incoming) > self.budget {
            let Some((_, name)) = self.order.first().cloned() else {
                break;
            };
            self.remove(&name)?;
            metrics::counter!("blowfish_cache_evictions_total", "tier" => "disk").increment(1);
        }
        Ok(())
    }

    pub(crate) fn get(&mut self, name: &str, expected: usize) -> Option<Bytes> {
        let &(size, clock) = self.entries.get(name)?;
        let result = fs::read(self.dir.join(name));
        let value = result.ok().filter(|v| {
            v.len() == HEADER + expected
                && &v[..8] == MAGIC
                && Sha256::digest(&v[HEADER..])[..] == v[8..HEADER]
        });
        match value {
            Some(value) => {
                self.order.remove(&(clock, name.to_owned()));
                self.clock += 1;
                self.order.insert((self.clock, name.to_owned()));
                self.entries.insert(name.to_owned(), (size, self.clock));
                Some(Bytes::from(value).slice(HEADER..))
            }
            None => {
                let before = self.bytes;
                let _ = self.remove(name);
                metrics::gauge!("blowfish_cache_bytes", "tier" => "disk")
                    .decrement((before - self.bytes) as f64);
                None
            }
        }
    }

    pub(crate) fn insert(&mut self, name: &str, value: &Bytes) -> io::Result<()> {
        let size = (HEADER + value.len()) as u64;
        if size > self.budget || self.entries.contains_key(name) {
            return Ok(());
        }
        let before = self.bytes;
        // Publish accounting even if an eviction or write fails partway through.
        let result = (|| {
            self.evict(size)?;
            let tmp = self.dir.join(format!("{name}.tmp"));
            let mut file = File::create(&tmp)?;
            file.write_all(MAGIC)?;
            file.write_all(&Sha256::digest(value))?;
            file.write_all(value)?;
            file.flush()?;
            fs::rename(tmp, self.dir.join(name))?;
            self.track(name.to_owned(), size);
            Ok(())
        })();
        metrics::gauge!("blowfish_cache_bytes", "tier" => "disk")
            .increment(self.bytes as f64 - before as f64);
        result
    }
}
impl Drop for Disk {
    fn drop(&mut self) {
        metrics::gauge!("blowfish_cache_bytes", "tier" => "disk").decrement(self.bytes as f64);
    }
}
