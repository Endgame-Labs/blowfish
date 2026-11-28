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
    bytes: Bytes,
}
/// Bounds mapping count and virtual address retention; resident pages remain reclaimable by the OS.
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
                // At least 32 MiB of virtual-budget charge also caps retained mappings at 512.
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
    /// The size and modification time of the object at `path`, from one `stat` on the calling
    /// thread. A stat takes microseconds, and the object store's own `head` would hand it to the
    /// blocking pool, where thousands of queries a second spin on the pool's one mutex.
    pub(crate) fn head(&self, path: &Path) -> Result<(u64, chrono::DateTime<chrono::Utc>), Error> {
        let metadata =
            std::fs::metadata(self.backend.path_to_filesystem(path)?).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Error::NotFound
                } else {
                    Error::Io(error)
                }
            })?;
        if metadata.is_dir() {
            return Err(Error::NotFound);
        }
        Ok((metadata.len(), metadata.modified()?.into()))
    }
    fn open(&self, path: &Path, random: bool) -> Result<Arc<Mapping>, Error> {
        let file = File::open(self.backend.path_to_filesystem(path)?).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound
            } else {
                Error::Io(error)
            }
        })?;
        let len = usize::try_from(file.metadata()?.len())
            .map_err(|_| Error::Config("mapped local object exceeds address space"))?;
        let bytes = if len == 0 {
            Bytes::new()
        } else {
            // SAFETY: RangeReader's contract requires immutable objects. Blowfish publishes
            // files atomically and never truncates published objects. Unlinking during GC is
            // safe: the mapping owns its original inode until all returned Bytes are dropped.
            let mapping = unsafe { memmap2::MmapOptions::new().len(len).map(&file)? };
            #[cfg(unix)]
            if random {
                // Sparse faults must not fill small budgets with unrequested vector neighbors.
                let _ = mapping.advise(memmap2::Advice::Random);
            }
            Bytes::from_owner(mapping)
        };
        Ok(Arc::new(Mapping { bytes }))
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
        read_metrics().attempts.increment(1);
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
            // The immutable object's length was checked when opening its mapping. Cached
            // reads compare that length without another fstat, including readers with a newer
            // expected size. External truncation violates the RangeReader contract.
            if (mapping.bytes.len() as u64) < expected {
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
        let m = read_metrics();
        let (cell, status) = match status {
            "ok" => (&m.ok, "ok"),
            "not_found" => (&m.not_found, "not_found"),
            _ => (&m.error, "error"),
        };
        cell.get_or_init(|| {
            metrics::counter!("blowfish_object_store_requests_total", "op" => "mapped_read", "status" => status)
        })
        .increment(1);
        m.bytes.increment(bytes);
        m.seconds.record(elapsed.as_secs_f64());
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

/// The metric handles of a mapped read, resolved once: the recorder's key construction and
/// registry lookup stay off the query path.
struct ReadMetrics {
    attempts: metrics::Counter,
    // Registered on first use, so a status that never happens exports no series.
    ok: std::sync::OnceLock<metrics::Counter>,
    not_found: std::sync::OnceLock<metrics::Counter>,
    error: std::sync::OnceLock<metrics::Counter>,
    bytes: metrics::Counter,
    seconds: metrics::Histogram,
}
fn read_metrics() -> &'static ReadMetrics {
    static HANDLES: std::sync::OnceLock<ReadMetrics> = std::sync::OnceLock::new();
    HANDLES.get_or_init(|| {
        ReadMetrics {
            attempts: metrics::counter!("blowfish_object_store_attempts_total", "op" => "mapped_read"),
            ok: Default::default(),
            not_found: Default::default(),
            error: Default::default(),
            bytes: metrics::counter!("blowfish_object_store_bytes_total", "op" => "mapped_read"),
            seconds: metrics::histogram!("blowfish_object_store_request_seconds", "op" => "mapped_read"),
        }
    })
}
