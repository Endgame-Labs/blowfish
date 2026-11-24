//! Disposable, generation-keyed ANN routing and code files. Vectors remain in object storage.
use crate::read_cache;
use bf_storage::Storage;
use bf_wal::{SegmentFile, SlabSet};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self},
    ops::Range,
    os::unix::{
        ffi::OsStrExt,
        fs::{DirBuilderExt, FileExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const MAX_BYTES: u64 = 20 << 30;
const MAX_FILES: usize = 4096;
const MAX_STAGES: usize = 128;
const BLOCK: usize = 16 << 10;
const PIECE: u64 = 8 << 20;
const RETRY: Duration = Duration::from_secs(60);

struct StagedFile {
    file: File,
    len: u64,
    crc: Vec<u32>,
}
impl StagedFile {
    /// Align before merging, so overlapping requests read and verify each block once.
    fn aligned(&self, ranges: &[Range<u64>]) -> io::Result<Vec<Range<u64>>> {
        let mut aligned = Vec::with_capacity(ranges.len());
        for r in ranges {
            if r.start > r.end || r.end > self.len {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if r.is_empty() {
                continue;
            }
            let start = r.start / BLOCK as u64 * BLOCK as u64;
            let end = ((r.end - 1) / BLOCK as u64 * BLOCK as u64)
                .saturating_add(BLOCK as u64)
                .min(self.len);
            aligned.push(start..end);
        }
        aligned.sort_unstable_by_key(|r| r.start);
        let mut merged: Vec<Range<u64>> = Vec::with_capacity(aligned.len());
        for r in aligned {
            if let Some(last) = merged.last_mut()
                && r.start <= last.end
            {
                last.end = last.end.max(r.end);
            } else {
                merged.push(r);
            }
        }
        Ok(merged)
    }

    /// Submit every scattered extent before waiting for the first one. Advice is optional:
    /// only the subsequent positional reads and checksum checks establish a cache hit.
    fn advise(&self, ranges: &[Range<u64>]) {
        #[cfg(target_os = "linux")]
        for range in ranges {
            let (Ok(start), Ok(len)) = (
                libc::off_t::try_from(range.start),
                libc::off_t::try_from(range.end - range.start),
            ) else {
                continue;
            };
            // SAFETY: this borrows a live descriptor, and supplies checked offsets. Failure
            // merely leaves the ordinary read to submit its I/O below.
            unsafe {
                libc::posix_fadvise(self.file.as_raw_fd(), start, len, libc::POSIX_FADV_WILLNEED);
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = ranges;
    }

    fn read(&self, ranges: &[Range<u64>]) -> io::Result<Vec<Bytes>> {
        let trace = crate::ann::trace_stages();
        let started = Instant::now();
        let aligned = self.aligned(ranges)?;
        self.advise(&aligned);
        let (mut read_ns, mut crc_ns, mut blocks, mut bytes) = (0, 0, 0, 0);
        let mut extents = BTreeMap::new();
        for range in aligned {
            let len = usize::try_from(range.end - range.start)
                .map_err(|_| io::Error::other("staged range exceeds address space"))?;
            let mut data = vec![0; len];
            let at = trace.then(Instant::now);
            self.file.read_exact_at(&mut data, range.start)?;
            if let Some(at) = at {
                read_ns += at.elapsed().as_nanos();
            }
            let at = trace.then(Instant::now);
            for (n, block) in data.chunks(BLOCK).enumerate() {
                let index = range.start / BLOCK as u64 + n as u64;
                if self.crc.get(index as usize).copied() != Some(crc32fast::hash(block)) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "staged ANN checksum",
                    ));
                }
                blocks += 1;
            }
            if let Some(at) = at {
                crc_ns += at.elapsed().as_nanos();
            }
            bytes += data.len();
            extents.insert(range.start, Bytes::from(data));
        }
        let copied = Instant::now();
        let result = ranges
            .iter()
            .map(|r| {
                if r.is_empty() {
                    return Ok(Bytes::new());
                }
                let (&start, bytes) = extents
                    .range(..=r.start)
                    .next_back()
                    .ok_or_else(|| io::Error::other("missing staged extent"))?;
                // Each nonempty input is wholly contained by one merged extent.
                Ok(bytes.slice((r.start - start) as usize..(r.end - start) as usize))
            })
            .collect();
        if trace {
            eprintln!(
                "LOCAL_READ ranges={} blocks={blocks} bytes={bytes} read_us={} crc_us={} copy_us={} total_us={} end_us={}",
                ranges.len(),
                read_ns / 1000,
                crc_ns / 1000,
                copied.elapsed().as_micros(),
                started.elapsed().as_micros(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_micros())
            );
        }
        result
    }
}

struct Lease {
    manager: Weak<Manager>,
    bytes: u64,
    files: usize,
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let mut state = manager.state.lock().expect("ANN staging lock");
            state.used -= self.bytes;
            state.files_used -= self.files;
        }
    }
}
struct Stage {
    path: PathBuf,
    files: HashMap<String, StagedFile>,
    bad: AtomicBool,
    _lease: Lease,
}
impl Drop for Stage {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
struct Hint {
    object: Option<String>,
    len: u64,
}
#[derive(Default)]
struct State {
    used: u64,
    files_used: usize,
    clock: u64,
    ready: HashMap<String, (u64, Arc<Stage>)>,
    files: HashMap<String, Weak<Stage>>,
    pending: HashSet<String>,
    failed: HashMap<String, Instant>,
    hints: HashMap<String, Hint>,
}

/// One engine's bounded disk staging area; scoped engines share its budget, but separate keys.
pub(crate) struct Manager {
    path: PathBuf,
    capacity: u64,
    file_capacity: usize,
    state: Mutex<State>,
    _lock: File,
    stop: Arc<crate::stop::Stop>,
}
impl Drop for Manager {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
fn free_bytes(path: &Path) -> io::Result<u64> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: the path is NUL-terminated, and statvfs writes the complete output on success.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: statvfs succeeded above.
    let stats = unsafe { stats.assume_init() };
    Ok(stats.f_bavail.saturating_mul(stats.f_frsize))
}
fn file_capacity() -> io::Result<usize> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes to a valid, exclusively borrowed output structure.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Leave half the descriptors and a small margin for backend connections, write-behind
    // cache files, and the runtime. Active-reader leases retain their file reservation too.
    Ok((limit.rlim_cur / 2)
        .saturating_sub(64)
        .min(MAX_FILES as libc::rlim_t) as usize)
}
impl Manager {
    pub fn for_store(store: &Storage, stop: Arc<crate::stop::Stop>) -> Option<Arc<Self>> {
        let (memory, disk) = store.cache_limits();
        // Local mapped objects already provide this locality without another copy.
        if memory == 0 {
            return None;
        }
        Self::open(&disk?, None, stop).ok()
    }
    fn open(
        parent: &Path,
        limit: Option<u64>,
        stop: Arc<crate::stop::Stop>,
    ) -> io::Result<Arc<Self>> {
        let base = parent.join("ann-staging-v1");
        fs::create_dir_all(&base)?;
        let guard = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(base.join(".lock"))?;
        guard.lock()?;
        // Only an unlocked owner directory is orphaned. Never share files between engines or
        // storage backends, even when two tenants chose identical immutable object names.
        for entry in fs::read_dir(&base)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir()
                || !entry.file_name().to_string_lossy().starts_with("owner-")
            {
                continue;
            }
            match OpenOptions::new()
                .read(true)
                .write(true)
                .open(entry.path().join(".lock"))
            {
                Ok(lock) if lock.try_lock().is_ok() => {
                    let _ = fs::remove_dir_all(entry.path());
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let _ = fs::remove_dir_all(entry.path());
                }
                _ => {}
            }
        }
        let capacity = limit
            .unwrap_or(MAX_BYTES)
            .min(MAX_BYTES)
            .min(free_bytes(&base)? / 4);
        let path = base.join(format!("owner-{}", uuid::Uuid::new_v4()));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path.join(".lock"))?;
        lock.lock()?;
        drop(guard);
        Ok(Arc::new(Self {
            path,
            capacity,
            file_capacity: file_capacity()?,
            state: Mutex::default(),
            _lock: lock,
            stop,
        }))
    }
    fn reserve(self: &Arc<Self>, bytes: u64, files: usize) -> Option<Lease> {
        if bytes > self.capacity
            || files > self.file_capacity
            || free_bytes(&self.path).ok()? < bytes.saturating_mul(2)
        {
            return None;
        }
        loop {
            let mut state = self.state.lock().expect("ANN staging lock");
            if state.used.saturating_add(bytes) <= self.capacity
                && state.files_used.saturating_add(files) <= self.file_capacity
                && state.ready.len() < MAX_STAGES
            {
                state.used += bytes;
                state.files_used += files;
                return Some(Lease {
                    manager: Arc::downgrade(self),
                    bytes,
                    files,
                });
            }
            let oldest = state
                .ready
                .iter()
                .min_by_key(|(_, (used, _))| *used)
                .map(|(k, _)| k.clone())?;
            let (_, evicted) = state.ready.remove(&oldest)?;
            let weak = Arc::downgrade(&evicted);
            state.files.retain(|_, value| !Weak::ptr_eq(value, &weak));
            drop(state);
            // A reader may retain this stage until its read finishes. Its lease stays charged.
            drop(evicted);
        }
    }
    fn file(&self, key: &str) -> Option<Arc<Stage>> {
        let mut state = self.state.lock().expect("ANN staging lock");
        let stage = state.files.get(key)?.upgrade()?;
        if stage.bad.load(Ordering::Relaxed) {
            return None;
        }
        state.clock += 1;
        let clock = state.clock;
        for (used, value) in state.ready.values_mut() {
            if Arc::ptr_eq(value, &stage) {
                *used = clock;
                break;
            }
        }
        Some(stage)
    }
    fn read(&self, scoped: &str, object: &str, ranges: &[Range<u64>]) -> Option<Vec<Bytes>> {
        let stage = self.file(scoped)?;
        let file = stage.files.get(object)?;
        if ranges.iter().any(|r| r.start > r.end || r.end > file.len) {
            return None;
        }
        match file.read(ranges) {
            Ok(bytes) => Some(bytes),
            Err(error) => {
                stage.bad.store(true, Ordering::Relaxed);
                metrics::counter!("blowfish_ann_staging_total", "outcome" => "corrupt")
                    .increment(1);
                tracing::debug!(%error, "discarding local ANN stage");
                None
            }
        }
    }
    fn observe(
        self: &Arc<Self>,
        store: &Storage,
        scope: &str,
        set: &SlabSet,
        dims: usize,
        leaves: u64,
    ) {
        if self.stop.stopping() {
            return;
        }
        let key = format!("{scope}stage:{}", set.dir.key);
        {
            let mut state = self.state.lock().expect("ANN staging lock");
            if let Some((_, stage)) = state.ready.get(&key) {
                if !stage.bad.load(Ordering::Relaxed) {
                    return;
                }
                let (_, bad) = state.ready.remove(&key).expect("present stage");
                let weak = Arc::downgrade(&bad);
                state.files.retain(|_, value| !Weak::ptr_eq(value, &weak));
                drop(state);
                drop(bad);
                state = self.state.lock().expect("ANN staging lock");
            }
            if !state.pending.is_empty()
                || state
                    .failed
                    .get(&key)
                    .is_some_and(|at| at.elapsed() < RETRY)
            {
                return;
            }
            state.pending.insert(key.clone());
        }
        let estimate = set
            .rows()
            .saturating_mul((dims.next_power_of_two() / 8 + 48) as u64)
            .saturating_add(leaves.saturating_mul((dims.next_power_of_two() * 4 + 128) as u64))
            .saturating_add(set.dir.size)
            .saturating_add(set.pages.as_ref().map_or(0, |f| f.size));
        let files = set.slabs.iter().map(|s| s.files.len()).sum::<usize>()
            + 1
            + usize::from(set.pages.is_some());
        let Some(lease) = self.reserve(estimate, files) else {
            self.state
                .lock()
                .expect("ANN staging lock")
                .pending
                .remove(&key);
            return;
        };
        let (manager, store, scope, set) =
            (self.clone(), store.clone(), scope.to_owned(), set.clone());
        tokio::spawn(async move {
            let result = manager
                .stop
                .run(manager.build(&store, &scope, &set, lease))
                .await;
            let mut state = manager.state.lock().expect("ANN staging lock");
            state.pending.remove(&key);
            match result {
                Some(Ok(stage)) => {
                    let stage = Arc::new(stage);
                    for object in stage.files.keys() {
                        state
                            .files
                            .insert(format!("{scope}{object}"), Arc::downgrade(&stage));
                    }
                    state.clock += 1;
                    let clock = state.clock;
                    state.ready.insert(key, (clock, stage));
                    metrics::counter!("blowfish_ann_staging_total", "outcome" => "ready")
                        .increment(1);
                }
                Some(Err(error)) => {
                    if state.failed.len() >= 128 {
                        state.failed.clear();
                    }
                    state.failed.insert(key, Instant::now());
                    metrics::counter!("blowfish_ann_staging_total", "outcome" => "error")
                        .increment(1);
                    tracing::debug!(%error, "ANN staging unavailable; using object storage");
                }
                None => {}
            }
        });
    }
    async fn build(
        self: &Arc<Self>,
        store: &Storage,
        _scope: &str,
        set: &SlabSet,
        lease: Lease,
    ) -> io::Result<Stage> {
        let path = self.path.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir(&path)?;
        let mut stage = Stage {
            path,
            files: HashMap::new(),
            bad: AtomicBool::new(false),
            _lease: lease,
        };
        let directory = self
            .download(store, &set.dir, set.dir.size, &stage.path.join("directory"))
            .await?;
        let raw = directory
            .read(std::slice::from_ref(&(0..directory.len)))?
            .remove(0);
        let dir = bf_vector::upgrade::directory(&raw).map_err(io::Error::other)?;
        let refs: HashMap<_, _> = set
            .slabs
            .iter()
            .flat_map(|s| s.files.values())
            .map(|f| (f.key.as_str(), f))
            .collect();
        let mut limits = BTreeMap::<String, u64>::new();
        for e in dir.entries() {
            let seg = &dir.segments()[e.segment];
            if !seg.codes_in_fragment {
                return Err(io::Error::other("ANN layout has separate row-major codes"));
            }
            let end = e.blob_range().end.max(e.codes_range().end);
            limits
                .entry(seg.fragment.clone())
                .and_modify(|n| *n = (*n).max(end))
                .or_insert(end);
            if seg.key != seg.fragment {
                limits.entry(seg.key.clone()).or_insert(48);
            }
        }
        let actual = limits
            .values()
            .try_fold(
                set.dir.size + set.pages.as_ref().map_or(0, |p| p.size),
                |sum, n| sum.checked_add(*n),
            )
            .ok_or_else(|| io::Error::other("ANN stage size overflow"))?;
        if actual > stage._lease.bytes {
            return Err(io::Error::other("ANN stage exceeds reservation"));
        }
        stage.files.insert(set.dir.key.clone(), directory);
        if let Some(file) = &set.pages {
            let bytes = self
                .download(store, file, file.size, &stage.path.join("pages"))
                .await?;
            stage.files.insert(file.key.clone(), bytes);
        }
        for (n, (key, len)) in limits.into_iter().enumerate() {
            let file = refs
                .get(key.as_str())
                .ok_or_else(|| io::Error::other("ANN stage source missing"))?;
            if len > file.size {
                return Err(io::Error::other("ANN prefix exceeds object"));
            }
            let staged = self
                .download(store, file, len, &stage.path.join(format!("object-{n}")))
                .await?;
            stage.files.insert(key, staged);
        }
        let released = stage._lease.bytes - actual;
        let mut state = self.state.lock().expect("ANN staging lock");
        state.used -= released;
        state.files_used -= stage._lease.files - stage.files.len();
        drop(state);
        stage._lease.bytes = actual;
        stage._lease.files = stage.files.len();
        metrics::histogram!("blowfish_ann_staging_bytes").record(actual as f64);
        Ok(stage)
    }
    async fn download(
        &self,
        store: &Storage,
        source: &SegmentFile,
        len: u64,
        path: &Path,
    ) -> io::Result<StagedFile> {
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .await?;
        let reader = store
            .range_reader_with_length(&source.key, source.size, true)
            .map_err(io::Error::other)?
            .bypass_cache();
        let ranges: Vec<_> = (0..len)
            .step_by(PIECE as usize)
            .map(|start| start..(start + PIECE).min(len))
            .collect();
        let mut reads =
            futures::stream::iter(ranges.into_iter().map(|r| reader.read(r))).buffered(4);
        let mut crc = Vec::with_capacity(len.div_ceil(BLOCK as u64) as usize);
        while let Some(bytes) = reads.try_next().await.map_err(io::Error::other)? {
            file.write_all(&bytes).await?;
            crc.extend(bytes.chunks(BLOCK).map(crc32fast::hash));
            tokio::task::yield_now().await;
        }
        file.sync_all().await?;
        Ok(StagedFile {
            file: file.into_std().await,
            len,
            crc,
        })
    }
}

pub(crate) fn observe(store: &Storage, set: &SlabSet, dims: usize, leaves: u64, attribute: &str) {
    let Some(context) = read_cache::staging_context() else {
        return;
    };
    let Some(manager) = &context.cache.staging else {
        return;
    };
    if !context.admit || store.cache_limits().0 == 0 {
        return;
    }
    if let Some(ns) = set
        .dir
        .key
        .strip_prefix("ns/")
        .and_then(|s| s.split('/').next())
    {
        let flat = crate::ann_dir::hint_key(ns, &crate::ann_dir::Layout::Slabs.dir_key(attribute));
        note_hint(&flat, Some(&set.dir.key), set.dir.metadata_len);
        let paged = crate::ann_dir::hint_key(ns, &crate::ann_dir::paged_hint_name(attribute));
        note_hint(
            &paged,
            set.pages.as_ref().map(|p| p.key.as_str()),
            set.pages.as_ref().map_or(0, |p| p.metadata_len),
        );
    }
    // `key("")` includes the separator used for every storage-scoped cache key.
    manager.observe(store, &context.cache.key(""), set, dims, leaves);
}
pub(crate) fn prefetch(key: &str, ranges: &[Range<u64>]) -> bool {
    let Some(context) = read_cache::staging_context() else {
        return false;
    };
    let Some(manager) = &context.cache.staging else {
        return false;
    };
    if !context.admit {
        return false;
    }
    let Some(stage) = manager.file(&context.cache.key(key)) else {
        return false;
    };
    let Some(file) = stage.files.get(key) else {
        return false;
    };
    let Ok(aligned) = file.aligned(ranges) else {
        return false;
    };
    file.advise(&aligned);
    true
}
pub(crate) fn read(key: &str, ranges: &[Range<u64>]) -> Option<Vec<Bytes>> {
    let context = read_cache::staging_context()?;
    let manager = context.cache.staging.as_ref()?;
    if !context.admit {
        return None;
    }
    let bytes = manager.read(&context.cache.key(key), key, ranges)?;
    let logical: u64 = bytes.iter().map(|b| b.len() as u64).sum();
    context
        .local_ann_bytes
        .fetch_add(logical, Ordering::Relaxed);
    metrics::counter!("blowfish_cache_hits_total", "tier" => "disk").increment(ranges.len() as u64);
    Some(bytes)
}
pub(crate) fn note_hint(hint: &str, object: Option<&str>, len: u64) {
    let Some(context) = read_cache::staging_context() else {
        return;
    };
    let Some(manager) = &context.cache.staging else {
        return;
    };
    let mut state = manager.state.lock().expect("ANN staging lock");
    if state.hints.len() >= 1024 {
        state.hints.clear();
    }
    state.hints.insert(
        context.cache.key(hint),
        Hint {
            object: object.map(str::to_owned),
            len,
        },
    );
}
pub(crate) fn hint(hint: &str) -> Option<Option<(String, Bytes)>> {
    let context = read_cache::staging_context()?;
    let manager = context.cache.staging.as_ref()?;
    let (object, len) = {
        let state = manager.state.lock().expect("ANN staging lock");
        let hint = state.hints.get(&context.cache.key(hint))?;
        (hint.object.clone(), hint.len)
    };
    let Some(object) = object else {
        return Some(None);
    };
    let bytes = read(&object, std::slice::from_ref(&(0..len)))?.remove(0);
    Some(Some((object, bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bf_storage::{CacheConfig, StorageConfig};
    use bf_vector::{BuildParams, Directory, Fragment, IndexBuilder, Metric};

    fn scratch() -> tempfile::TempDir {
        let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
        fs::create_dir_all(&parent).unwrap();
        tempfile::Builder::new()
            .prefix("ann-staging-test-")
            .tempdir_in(parent)
            .unwrap()
    }

    #[test]
    fn scattered_overlapping_ranges_share_an_open_file_without_a_shared_cursor() {
        let work = scratch();
        let temporary = tempfile::NamedTempFile::new_in(work.path()).unwrap();
        let data: Vec<u8> = (0..BLOCK * 17 + 123)
            .map(|i| (i * 37 % 251) as u8)
            .collect();
        temporary.as_file().write_all_at(&data, 0).unwrap();
        let staged = StagedFile {
            file: temporary.as_file().try_clone().unwrap(),
            len: data.len() as u64,
            crc: data.chunks(BLOCK).map(crc32fast::hash).collect(),
        };
        // The live descriptor, rather than a reopened pathname, owns the immutable bytes.
        temporary.close().unwrap();
        std::thread::scope(|threads| {
            for salt in 0..8u64 {
                let (staged, data) = (&staged, &data);
                threads.spawn(move || {
                    let mut seed = salt + 67;
                    for _ in 0..40 {
                        let mut ranges = vec![staged.len..staged.len, 0..37, 0..37];
                        for _ in 0..12 {
                            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                            let a = seed % (staged.len + 1);
                            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                            let b = seed % (staged.len + 1);
                            ranges.push(a.min(b)..a.max(b));
                        }
                        for (bytes, range) in staged.read(&ranges).unwrap().iter().zip(ranges) {
                            assert_eq!(&bytes[..], &data[range.start as usize..range.end as usize]);
                        }
                    }
                });
            }
        });
        assert!(
            staged
                .read(std::slice::from_ref(&Range { start: 1, end: 0 }))
                .is_err()
        );
        assert!(
            staged
                .read(std::slice::from_ref(&(0..staged.len + 1)))
                .is_err()
        );
        staged
            .file
            .write_all_at(&[data[BLOCK - 1] ^ 0xff], (BLOCK - 1) as u64)
            .unwrap();
        assert!(
            staged.read(std::slice::from_ref(&(0..1))).is_err(),
            "verify the entire checksum block, including unrequested bytes"
        );
        staged.file.set_len(BLOCK as u64).unwrap();
        assert!(
            staged
                .read(std::slice::from_ref(&(BLOCK as u64..staged.len)))
                .is_err()
        );
    }
    fn vector(id: u32, salt: u32) -> Vec<f32> {
        (0..32)
            .map(|j| ((id.wrapping_mul(31) + j * 17 + salt * 7) % 997) as f32 / 997.)
            .collect()
    }
    fn file(key: &str, size: u64, metadata_len: u64) -> SegmentFile {
        SegmentFile {
            key: key.into(),
            size,
            metadata_len,
            stats_index: None,
            text_header: None,
        }
    }
    async fn fixture(salt: u32) -> (Storage, SlabSet, Vec<u8>, u64) {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let builder = IndexBuilder::new(32, Metric::EuclideanSquared)
            .unwrap()
            .params(BuildParams {
                flat_threshold: 0,
                clusters: Some(16),
                ..Default::default()
            });
        let rows: Vec<_> = (0..2048).map(|id| (id, vector(id, salt))).collect();
        let blob = builder.build(&rows).unwrap();
        let (ann, meta) = Fragment::build_ann(&blob).unwrap();
        let key = "ns/test/slabs/s/ann.bfa";
        let dir_key = format!("ns/test/segments/directory-{salt}");
        let fragment = Fragment::decode(&ann[..meta as usize]).unwrap();
        let raw = Directory::build(&[("s".into(), key.into(), key.into(), fragment)])
            .unwrap()
            .unwrap();
        let dir = Directory::decode(&raw).unwrap();
        let end = dir
            .entries()
            .iter()
            .map(|e| e.blob_range().end.max(e.codes_range().end))
            .max()
            .unwrap();
        assert!(end > BLOCK as u64 + 23);
        let ann_file = file(key, ann.len() as u64, meta);
        let dir_file = file(&dir_key, raw.len() as u64, raw.len() as u64);
        store.create_unique(key, ann.clone()).await.unwrap();
        store.create_unique(&dir_key, raw).await.unwrap();
        let set = SlabSet {
            covered: vec![bf_wal::SlabCover {
                segment: "docs".into(),
                base: 0,
                rows: 2048,
                live_generation: 0,
            }],
            slabs: vec![bf_wal::Segment {
                id: "s".into(),
                wal_start: 0,
                wal_end: 0,
                row_count: 2048,
                schema: Default::default(),
                files: BTreeMap::from([("annv2:vector".into(), ann_file.clone())]),
                live: ann_file,
                live_generation: 0,
            }],
            dir: dir_file,
            pages: None,
            format: bf_wal::TIER_FORMAT_VERSION,
            coarse: None,
            coarse_rows: 0,
        };
        (store, set, ann, end)
    }
    async fn ready(manager: &Arc<Manager>, scope: &str, set: &SlabSet) -> Arc<Stage> {
        let key = format!("{scope}stage:{}", set.dir.key);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some((_, stage)) = manager.state.lock().unwrap().ready.get(&key) {
                    return stage.clone();
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn verified_prefixes_preserve_ranges_and_corruption_falls_back_to_storage() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let (store, set, ann, end) = fixture(1).await;
        manager.observe(&store, "left\0", &set, 32, 16);
        let stage = ready(&manager, "left\0", &set).await;
        let key = &set.slabs[0].files["annv2:vector"].key;
        let ranges = [0..37, BLOCK as u64 - 7..BLOCK as u64 + 23, end..end];
        let actual = manager.read(&format!("left\0{key}"), key, &ranges).unwrap();
        for (bytes, range) in actual.iter().zip(&ranges) {
            assert_eq!(&bytes[..], &ann[range.start as usize..range.end as usize]);
        }
        // A vector read is never covered by a staged prefix.
        assert!(
            manager
                .read(
                    &format!("left\0{key}"),
                    key,
                    std::slice::from_ref(&(end..end + 4))
                )
                .is_none()
        );
        assert!(
            manager
                .read(
                    &format!("right\0{key}"),
                    key,
                    std::slice::from_ref(&(0..37))
                )
                .is_none()
        );
        stage.files[key].file.write_at(&[ann[0] ^ 0xff], 0).unwrap();
        let mut cache = read_cache::DecodedCache::new(1 << 20, 1).scoped("left");
        cache.staging = Some(manager.clone());
        let context = read_cache::ReadContext::new(Arc::new(cache));
        let source = &set.slabs[0].files["annv2:vector"];
        let actual = read_cache::scope(
            context,
            crate::segment::ObjectReader::new(&store, source).read(0..37),
        )
        .await
        .unwrap();
        assert_eq!(&actual[..], &ann[..37]);
        assert!(stage.bad.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn generations_and_storage_scopes_never_alias() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let (left, a, ann_a, end_a) = fixture(1).await;
        let (right, b, ann_b, end_b) = fixture(2).await;
        manager.observe(&left, "left\0", &a, 32, 16);
        let _a = ready(&manager, "left\0", &a).await;
        manager.observe(&right, "right\0", &b, 32, 16);
        let _b = ready(&manager, "right\0", &b).await;
        let key = &a.slabs[0].files["annv2:vector"].key;
        assert_ne!(&ann_a[..end_a as usize], &ann_b[..end_b as usize]);
        assert_eq!(
            manager
                .read(
                    &format!("left\0{key}"),
                    key,
                    std::slice::from_ref(&(0..end_a))
                )
                .unwrap()[0],
            &ann_a[..end_a as usize]
        );
        assert_eq!(
            manager
                .read(
                    &format!("right\0{key}"),
                    key,
                    std::slice::from_ref(&(0..end_b))
                )
                .unwrap()[0],
            &ann_b[..end_b as usize]
        );
        // A new tier with a new immutable key cannot accidentally read the old generation.
        let new_key = format!("{key}.new-generation");
        assert!(
            manager
                .read(
                    &format!("left\0{new_key}"),
                    &new_key,
                    std::slice::from_ref(&(0..37))
                )
                .is_none()
        );
    }

    #[tokio::test]
    async fn reservations_and_shutdown_release_every_pending_byte() {
        let work = scratch();
        let stop = Arc::new(crate::stop::Stop::default());
        let manager = Manager::open(work.path(), Some(1 << 20), stop.clone()).unwrap();
        let lease = manager.reserve(800_000, 1).unwrap();
        assert!(manager.reserve(300_000, 1).is_none());
        assert_eq!(manager.state.lock().unwrap().used, 800_000);
        drop(lease);
        assert_eq!(manager.state.lock().unwrap().used, 0);
        let (store, set, _, _) = fixture(3).await;
        manager.observe(&store, "", &set, 32, 16);
        stop.begin();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !manager.state.lock().unwrap().pending.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(manager.state.lock().unwrap().used, 0);
        assert!(manager.state.lock().unwrap().ready.is_empty());
        manager.observe(&store, "", &set, 32, 16);
        assert!(manager.state.lock().unwrap().pending.is_empty());
    }

    #[tokio::test]
    async fn eviction_removes_lookup_before_releasing_an_active_reader() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let (store, set, _, _) = fixture(7).await;
        manager.observe(&store, "", &set, 32, 16);
        let active_reader = ready(&manager, "", &set).await;
        let key = &set.slabs[0].files["annv2:vector"].key;
        let used = manager.state.lock().unwrap().used;
        assert!(manager.reserve(manager.capacity, 1).is_none());
        assert!(manager.file(key).is_none());
        assert!(active_reader.path.exists());
        assert_eq!(manager.state.lock().unwrap().used, used);
        let path = active_reader.path.clone();
        drop(active_reader);
        assert!(!path.exists());
        assert_eq!(manager.state.lock().unwrap().used, 0);
        assert!(manager.reserve(manager.capacity, 1).is_some());
    }

    #[test]
    fn live_owner_locks_survive_cleanup_and_dead_owners_are_removed() {
        let work = scratch();
        let first = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let orphan = work
            .path()
            .join("ann-staging-v1")
            .join(format!("owner-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&orphan).unwrap();
        File::create(orphan.join(".lock")).unwrap();
        let second = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        assert!(first.path.exists());
        assert!(!orphan.exists());
        let first_path = first.path.clone();
        drop(first);
        assert!(!first_path.exists());
        assert!(second.path.exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn query_cache_drop_keeps_staging_and_reports_disk_hits() {
        let work = scratch();
        let store = Storage::from_backend_with_cache(
            Arc::new(object_store::memory::InMemory::new()),
            object_store::path::Path::default(),
            false,
            false,
            CacheConfig {
                memory_bytes: 8 << 20,
                disk_bytes: 32 << 20,
                disk_dir: Some(work.path().join("cache")),
                namespace: Some(uuid::Uuid::new_v4().to_string()),
                block_size: 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let engine = crate::Engine::with_config(
            store,
            crate::EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                decoded_cache_bytes: 16 << 20,
                ..Default::default()
            },
        );
        let rows: Vec<_> = (0..5000)
            .map(|id| serde_json::json!({"id":id,"vector":vector(id, 5)}))
            .collect();
        engine
            .write(
                "test",
                serde_json::from_value(
                    serde_json::json!({"distance_metric":"euclidean_squared","upsert_rows":rows}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("test", true).await.unwrap();
        engine
            .recluster(
                "test",
                &crate::ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 4,
                    cluster_rows: 200,
                    max_slab_rows: 2000,
                    scratch: work.path().join("build"),
                },
            )
            .await
            .unwrap();
        let request: bf_types::QueryRequest = serde_json::from_value(serde_json::json!({"rank_by":["vector","ANN",vector(17,5)],"top_k":10,"include_attributes":["id"]})).unwrap();
        engine.drop_caches().await;
        let (before, cold) = engine
            .query_with_diagnostics("test", request.clone())
            .await
            .unwrap();
        let manager = engine.0.read_cache.staging.clone().unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while manager.state.lock().unwrap().ready.is_empty() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        engine.drop_caches().await;
        let (after, diagnostics) = engine
            .query_with_diagnostics("test", request)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&before.result.rows).unwrap(),
            serde_json::to_value(&after.result.rows).unwrap()
        );
        let snapshot_bytes = |measurements: &[bf_storage::BackendMeasurement]| -> u64 {
            measurements
                .iter()
                .filter(|m| m.stage == "snapshot" && m.operation == "get")
                .map(|m| m.bytes)
                .sum()
        };
        assert!(
            snapshot_bytes(&diagnostics.backend) < snapshot_bytes(&cold.backend),
            "a cold snapshot must read routing hints from the published stage"
        );
        assert!(diagnostics.cache.disk_bytes > 0);
        assert_eq!(after.performance.cache_temperature, "warm");
        assert_eq!(
            after.performance.cache_hit_ratio,
            diagnostics.cache.cache_hit_ratio()
        );
        assert!(engine.shutdown(Duration::from_secs(5)).await);
    }
}
