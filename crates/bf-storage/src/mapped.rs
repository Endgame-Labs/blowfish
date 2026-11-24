//! Immutable local-object mappings, sharing file pages with the operating system.
use crate::{Error, diagnostics};
use bytes::Bytes;
use moka::sync::Cache;
use object_store::{local::LocalFileSystem, path::Path};
use std::{
    fs::File,
    ops::Range,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

struct Mapping {
    file: File,
    bytes: Bytes,
}
/// Bounds descriptors and virtual address retention; resident pages remain reclaimable by the OS.
pub(crate) struct Mapped {
    backend: LocalFileSystem,
    files: Cache<(String, bool), Arc<Mapping>>,
}
impl Mapped {
    pub(crate) fn new(root: PathBuf) -> Result<Arc<Self>, Error> {
        let value = Arc::new(Self {
            backend: LocalFileSystem::new_with_prefix(root)?,
            files: Cache::builder()
                .max_capacity(16 * 1024)
                // At least 32 MiB of virtual-budget charge also caps descriptors at 512.
                .weigher(|_: &(String, bool), value: &Arc<Mapping>| {
                    value
                        .bytes
                        .len()
                        .div_ceil(1 << 20)
                        .clamp(32, u32::MAX as usize) as u32
                })
                .time_to_idle(Duration::from_secs(60))
                .build(),
        });
        let weak = Arc::downgrade(&value);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let Some(value) = weak.upgrade() else { break };
                // Expire idle mappings even if the process receives no more queries, so old
                // unlinked generations do not retain disk extents indefinitely.
                value.files.run_pending_tasks();
            }
        });
        Ok(value)
    }
    fn open(&self, path: &Path, random: bool) -> Result<Arc<Mapping>, Error> {
        let file = File::open(self.backend.path_to_filesystem(path)?).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound
            } else {
                Error::Io(error)
            }
        })?;
        let bytes = if file.metadata()?.len() == 0 {
            Bytes::new()
        } else {
            // SAFETY: RangeReader's contract requires immutable objects. Blowfish publishes
            // files atomically and never truncates published objects. Unlinking during GC is
            // safe: the mapping owns its original inode until all returned Bytes are dropped.
            let mapping = unsafe { memmap2::MmapOptions::new().map(&file)? };
            #[cfg(unix)]
            if random {
                // Sparse faults must not fill small budgets with unrequested vector neighbors.
                let _ = mapping.advise(memmap2::Advice::Random);
            }
            Bytes::from_owner(mapping)
        };
        Ok(Arc::new(Mapping { file, bytes }))
    }
    pub(crate) fn read(
        &self,
        path: &Path,
        expected: u64,
        offset: u64,
        ranges: &[Range<u64>],
        admit: bool,
        random: bool,
    ) -> Result<Vec<Bytes>, Error> {
        if ranges.iter().all(Range::is_empty) {
            return Ok(ranges.iter().map(|_| Bytes::new()).collect());
        }
        let now = Instant::now();
        metrics::counter!("blowfish_object_store_attempts_total", "op" => "mapped_read")
            .increment(1);
        let result = (|| {
            // Large directory and ID reads need bulk I/O on a cold disk; small reads and
            // sparse vector batches must not trigger read-ahead. Both views share OS pages.
            let random = random || ranges.iter().all(|r| r.end - r.start < 64 << 10);
            let key = (path.to_string(), random);
            let mapping = if admit {
                self.files
                    .try_get_with(key, || self.open(path, random))
                    .map_err(Error::from_shared)?
            } else {
                match self.files.get(&key) {
                    Some(value) => value,
                    None => self.open(path, random)?,
                }
            };
            // Report an ordinary short-object error before accessing a file truncated outside
            // the immutable-object contract. Concurrent external mutation remains unsupported.
            if (mapping.bytes.len() as u64) < expected || mapping.file.metadata()?.len() < expected
            {
                return Err(Error::Config("short mapped local object"));
            }
            Ok(ranges
                .iter()
                .map(|range| {
                    mapping
                        .bytes
                        .slice((range.start + offset) as usize..(range.end + offset) as usize)
                })
                .collect::<Vec<_>>())
        })();
        let elapsed = now.elapsed();
        let bytes = result.as_ref().map_or(0, |values| {
            values.iter().map(|value| value.len() as u64).sum()
        });
        let status = match &result {
            Ok(_) => "ok",
            Err(Error::NotFound) => "not_found",
            Err(_) => "error",
        };
        diagnostics::record("mapped_read", bytes, elapsed);
        metrics::counter!("blowfish_object_store_requests_total", "op" => "mapped_read", "status" => status).increment(1);
        metrics::counter!("blowfish_object_store_bytes_total", "op" => "mapped_read")
            .increment(bytes);
        metrics::histogram!("blowfish_object_store_request_seconds", "op" => "mapped_read")
            .record(elapsed.as_secs_f64());
        result
    }
    pub(crate) fn remove(&self, path: &Path) {
        for random in [false, true] {
            self.files.invalidate(&(path.to_string(), random));
        }
        self.files.run_pending_tasks();
    }
    pub(crate) fn clear(&self) {
        self.files.invalidate_all();
        self.files.run_pending_tasks();
    }
}
