//! Disposable, checksummed, file-per-block LRU storage. All calls run on blocking workers.
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

/// Magic (8 bytes) followed by a CRC-32 of the payload and four reserved bytes.
const HEADER: usize = 16;
const MAGIC: &[u8; 8] = b"BFCACHE2";

pub(crate) fn digest(input: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for b in Sha256::digest(input.as_ref()) {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 15) as usize] as char);
    }
    out
}

/// Read and verify one cache file. Runs without any lock held. The buffer is allocated
/// at its exact size, so the returned `Bytes` retains no spare capacity.
pub(crate) fn read_block(path: &Path, expected: usize) -> Option<Bytes> {
    let mut file = File::open(path).ok()?;
    let mut bytes = vec![0u8; HEADER + expected];
    file.read_exact(&mut bytes).ok()?;
    // A longer file is corrupt: it cannot be a block of the expected size.
    let mut extra = [0u8; 1];
    if file.read(&mut extra).ok()? != 0 {
        return None;
    }
    let crc = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    (&bytes[..8] == MAGIC && crc32fast::hash(&bytes[HEADER..]) == crc)
        .then(|| Bytes::from(bytes).slice(HEADER..))
}

pub(crate) struct Disk {
    dir: PathBuf,
    budget: u64,
    bytes: u64,
    reported: u64,
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
            reported: 0,
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
        disk.publish_bytes();
        Ok(disk)
    }

    fn publish_bytes(&mut self) {
        metrics::gauge!("blowfish_cache_bytes", "tier" => "disk")
            .increment(self.bytes as f64 - self.reported as f64);
        self.reported = self.bytes;
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

    /// Path of a tracked block, without touching the file.
    pub(crate) fn locate(&self, name: &str) -> Option<PathBuf> {
        self.entries.contains_key(name).then(|| self.dir.join(name))
    }

    /// Record a successful read (LRU touch) or drop a block that failed verification.
    pub(crate) fn finish_read(&mut self, name: &str, ok: bool) {
        if ok {
            if let Some(&(size, clock)) = self.entries.get(name) {
                self.order.remove(&(clock, name.to_owned()));
                self.clock += 1;
                self.order.insert((self.clock, name.to_owned()));
                self.entries.insert(name.to_owned(), (size, self.clock));
            }
        } else {
            let _ = self.remove(name);
            self.publish_bytes();
        }
    }

    pub(crate) fn insert(&mut self, name: &str, value: &Bytes) -> io::Result<()> {
        let size = (HEADER + value.len()) as u64;
        if size > self.budget || self.entries.contains_key(name) {
            return Ok(());
        }
        let tmp = self.dir.join(format!("{name}.tmp"));
        // Publish accounting even if an eviction or write fails partway through.
        let result = (|| {
            self.evict(size)?;
            let mut file = File::create(&tmp)?;
            file.write_all(MAGIC)?;
            file.write_all(&crc32fast::hash(value).to_le_bytes())?;
            file.write_all(&[0u8; 4])?;
            file.write_all(value)?;
            file.flush()?;
            fs::rename(&tmp, self.dir.join(name))?;
            self.track(name.to_owned(), size);
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(tmp);
        }
        self.publish_bytes();
        result
    }
}
impl Drop for Disk {
    fn drop(&mut self) {
        metrics::gauge!("blowfish_cache_bytes", "tier" => "disk").decrement(self.reported as f64);
    }
}
