//! Durable, identity-checked ANN routing, codes, and capacity-admitted exact vectors.
use crate::{local_ann_codec as codec, read_cache};
use bf_storage::{ObjectIdentity, Storage};
use bf_wal::{SegmentFile, SlabSet};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
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
const MAX_STAGES: usize = 256;
/// Concurrent small GETs while gathering companions into a pack.
const COMPANION_FETCHES: usize = 32;
/// Buffered companion bytes before one positional write to the pack.
const PACK_FLUSH: usize = 4 << 20;
const BLOCK: usize = codec::BLOCK;
const PIECE: u64 = 8 << 20;
const DOWNLOADS: usize = 4;
const RETRY: Duration = Duration::from_secs(60);

struct StagedFile {
    /// Shared by every companion of a pack, each at its own `offset`.
    file: Arc<File>,
    /// Where the stored bytes start in `file`: zero unless this is a section of a pack.
    offset: u64,
    /// Object offset of the first stored byte: zero for a prefix, or where a staged tail starts.
    base: u64,
    len: u64,
    crc: Vec<u32>,
    /// Cumulative physical block ends for losslessly packed local bytes.
    encoded: Option<Vec<u32>>,
    identity: Option<ObjectIdentity>,
    /// The object whose identity covers these bytes when they are derived from it.
    source: Option<String>,
}
impl StagedFile {
    fn stored_len(&self) -> u64 {
        self.encoded.as_ref().map_or(self.len, |ends| {
            u64::from(ends.last().copied().unwrap_or(0))
        })
    }
    fn physical(&self, range: &Range<u64>) -> Range<u64> {
        self.encoded.as_ref().map_or_else(
            || range.clone(),
            |ends| {
                let first = range.start as usize / BLOCK;
                let last = range.end.div_ceil(BLOCK as u64) as usize;
                let start = if first == 0 { 0 } else { ends[first - 1] };
                let end = if last == 0 { 0 } else { ends[last - 1] };
                u64::from(start)..u64::from(end)
            },
        )
    }

    /// Object ranges as offsets into the stored bytes, or `None` when any is not wholly stored.
    fn local(&self, ranges: &[Range<u64>]) -> Option<Vec<Range<u64>>> {
        let end = self.base.checked_add(self.len)?;
        ranges
            .iter()
            .map(|r| {
                (r.start <= r.end && r.start >= self.base && r.end <= end)
                    .then(|| r.start - self.base..r.end - self.base)
            })
            .collect()
    }
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
            let range = self.physical(range);
            let (Ok(start), Ok(len)) = (
                libc::off_t::try_from(range.start + self.offset),
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
        let ranges = &self
            .local(ranges)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let trace = crate::ann::trace_stages();
        let started = Instant::now();
        let aligned = self.aligned(ranges)?;
        self.advise(&aligned);
        let (mut read_ns, mut crc_ns, mut blocks, mut bytes) = (0, 0, 0, 0);
        let mut extents = BTreeMap::new();
        for range in aligned {
            let physical = self.physical(&range);
            let len = usize::try_from(physical.end - physical.start)
                .map_err(|_| io::Error::other("staged range exceeds address space"))?;
            let mut data = vec![0; len];
            let at = trace.then(Instant::now);
            self.file
                .read_exact_at(&mut data, physical.start + self.offset)?;
            if let Some(at) = at {
                read_ns += at.elapsed().as_nanos();
            }
            let at = trace.then(Instant::now);
            let mut decoded = self
                .encoded
                .as_ref()
                .map(|_| Vec::with_capacity((range.end - range.start) as usize));
            for index in range.start / BLOCK as u64..range.end.div_ceil(BLOCK as u64) {
                let raw_start = index * BLOCK as u64;
                let raw_end = (raw_start + BLOCK as u64).min(self.len);
                let stored = self.physical(&(raw_start..raw_end));
                let block = &data[(stored.start - physical.start) as usize
                    ..(stored.end - physical.start) as usize];
                if self.crc.get(index as usize).copied() != Some(crc32fast::hash(block)) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "staged ANN checksum",
                    ));
                }
                if let Some(decoded) = &mut decoded {
                    decoded.extend(codec::decode(block, (raw_end - raw_start) as usize)?);
                }
                blocks += 1;
            }
            if let Some(decoded) = decoded {
                data = decoded;
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
    durable: bool,
}
impl Drop for Stage {
    fn drop(&mut self) {
        if !self.durable || self.bad.load(Ordering::Relaxed) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
struct Hint {
    object: Option<String>,
    len: u64,
}
/// Advisory WAL starting point, usable only after a fresh manifest confirms both fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub generation: u64,
    pub indexed_through: u64,
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
    cancelled: HashSet<String>,
    /// Tiers whose companion phase has finished (or had nothing to add) in this process.
    settled: HashSet<String>,
    hints: HashMap<String, Hint>,
    /// Bounded, scoped checkpoint hints survive query-cache eviction with the stages.
    checkpoints: VecDeque<(String, Checkpoint)>,
    /// Immutable checkpoint metadata, independent of disposable query caches and WAL overlays.
    snapshots: HashMap<String, Arc<crate::NamespaceState>>,
    /// Slab bytes a local recluster wrote while uploading, by the tier's stage key.
    prestaged: HashMap<String, Arc<PreInner>>,
}

/// A completion record binds the manifest's tier, scoped object identities, local lengths,
/// and block CRCs. Its SHA-256 envelope detects damaged metadata before any field is trusted.
#[derive(Serialize, Deserialize)]
struct Record {
    scope: String,
    set: SlabSet,
    hints: BTreeMap<String, (Option<String>, u64)>,
    files: BTreeMap<String, FileRecord>,
    /// Whether this record holds only the tier's companions (a pack). Records written before
    /// packs hold one stage that includes any companions, and never set this.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    companions: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint: Option<Checkpoint>,
}
fn stage_key(scope: &str, dir_key: &str, companions: bool) -> String {
    format!(
        "{scope}stage:{dir_key}{}",
        if companions { COMPANIONS } else { "" }
    )
}
/// Suffix of the key a tier's companion pack is stored under.
const COMPANIONS: &str = "#companions";
/// The local file holding a tier's companions.
const PACK: &str = "pack";
#[derive(Serialize, Deserialize)]
struct FileRecord {
    name: String,
    len: u64,
    crc: Vec<u32>,
    /// Cumulative physical block ends for losslessly packed local bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    encoded: Option<Vec<u32>>,
    identity: ObjectIdentity,
    /// Where the stored bytes start in the object. Prefix copies (and records written before
    /// tails were staged) start at zero.
    #[serde(default)]
    base: u64,
    /// Where the bytes start in the local file. Companions share one pack file, and records
    /// written before packs name one file per entry (offset zero).
    #[serde(default, skip_serializing_if = "is_zero")]
    offset: u64,
    /// The object these bytes were derived from, when the key names a derived file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<String>,
}
impl FileRecord {
    fn stored_len(&self) -> u64 {
        self.encoded.as_ref().map_or(self.len, |ends| {
            u64::from(ends.last().copied().unwrap_or(0))
        })
    }
}
fn is_zero(n: &u64) -> bool {
    *n == 0
}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn read_record(path: &Path) -> io::Result<Record> {
    let mut file = File::open(path.join("complete"))?;
    // A 20 GiB stage has fewer than 1.4 million CRCs. Bound damaged metadata allocations.
    if file.metadata()?.len() > 32 << 20 {
        return Err(io::Error::other("ANN completion record too large"));
    }
    let mut hash = [0; 32];
    file.read_exact(&mut hash)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if Sha256::digest(&bytes)[..] != hash
        || path.file_name().and_then(|s| s.to_str()) != Some(digest(&bytes).as_str())
    {
        return Err(io::Error::other("ANN completion checksum"));
    }
    serde_json::from_slice(&bytes).map_err(io::Error::other)
}

/// What hydration reads on every query from a source segment, for a stage to hold.
#[derive(Clone)]
pub(crate) struct Extra {
    pub file: SegmentFile,
    pub kind: ExtraKind,
}
#[derive(Clone, Copy)]
pub(crate) enum ExtraKind {
    /// The object's last bytes from this offset: a column directory.
    Tail(u64),
    /// A derived map from row to ID, 8 bytes per row, when every ID of the object's ID index
    /// (of this many rows) is an unsigned integer.
    NumericIds(u32),
}
impl Extra {
    fn len(&self) -> u64 {
        match self.kind {
            ExtraKind::Tail(base) => self.file.size - base,
            ExtraKind::NumericIds(rows) => u64::from(rows) * 8,
        }
    }
    /// The key a stage files these bytes under.
    fn key(&self) -> String {
        match self.kind {
            ExtraKind::Tail(_) => self.file.key.clone(),
            ExtraKind::NumericIds(_) => rowid_key(&self.file.key),
        }
    }
}
fn rowid_key(ids_key: &str) -> String {
    format!("{ids_key}#rowid")
}

/// One engine's bounded disk staging area; scoped engines share its budget, but separate keys.
pub(crate) struct Manager {
    path: PathBuf,
    capacity: u64,
    file_capacity: usize,
    state: Mutex<State>,
    _lock: File,
    candidates: Mutex<Vec<PathBuf>>,
    recovered: tokio::sync::OnceCell<()>,
    stop: Arc<crate::stop::Stop>,
}
impl Drop for Manager {
    fn drop(&mut self) {
        // Eviction and validation failures delete stages explicitly. Clean shutdown keeps
        // completed files, and an interrupted download never has a completion record.
        for (_, (_, stage)) in self
            .state
            .get_mut()
            .expect("ANN staging lock")
            .ready
            .drain()
        {
            drop(stage);
        }
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
    // Field widths differ by platform (u32 blocks on macOS, u64 on Linux); widen before multiplying.
    let free = u128::from(stats.f_bavail) * u128::from(stats.f_frsize);
    Ok(u64::try_from(free).unwrap_or(u64::MAX))
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
/// Remove caches from the disposable format after an upgrade, preserving live owners.
fn sweep_legacy(parent: &Path) -> io::Result<()> {
    sweep_format(parent, "ann-staging-v1", true)?;
    sweep_format(parent, "ann-staging-v2", false)
}
fn sweep_format(parent: &Path, name: &str, owner_prefix: bool) -> io::Result<()> {
    let base = parent.join(name);
    if !base.is_dir() {
        return Ok(());
    }
    let guard = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(base.join(".lock"))?;
    if guard.try_lock().is_err() {
        return Ok(());
    }
    for entry in fs::read_dir(base)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir()
            || (owner_prefix && !entry.file_name().to_string_lossy().starts_with("owner-"))
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
    Ok(())
}
impl Manager {
    pub fn for_store(store: &Storage, stop: Arc<crate::stop::Stop>) -> Option<Arc<Self>> {
        let (memory, disk) = store.cache_limits();
        // Local mapped objects already provide this locality without another copy.
        if memory == 0 {
            return None;
        }
        let manager = Self::open_named(&disk?, &store.cache_identity()?, None, stop).ok()?;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let (manager, store) = (manager.clone(), store.clone());
            runtime.spawn(async move {
                manager.recover(&store).await;
            });
        }
        Some(manager)
    }
    #[cfg(test)]
    fn open(
        parent: &Path,
        limit: Option<u64>,
        stop: Arc<crate::stop::Stop>,
    ) -> io::Result<Arc<Self>> {
        Self::open_named(parent, "test", limit, stop)
    }
    fn open_named(
        parent: &Path,
        identity: &str,
        limit: Option<u64>,
        stop: Arc<crate::stop::Stop>,
    ) -> io::Result<Arc<Self>> {
        sweep_legacy(parent)?;
        let base = parent.join("ann-staging-v3");
        fs::create_dir_all(&base)?;
        // Serialize ownership changes, so deleting an unlocked backend cannot race a
        // new engine opening that backend's lock file.
        let guard = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(base.join(".lock"))?;
        guard.lock()?;
        let path = base.join(identity);
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path.join(".lock"))?;
        // A second live engine for this backend falls back to S3 rather than sharing files.
        lock.try_lock().map_err(io::Error::other)?;
        // A changed bucket, prefix, or endpoint must not leave another full staging
        // allowance on disk forever. Live backends retain their own locked areas.
        for entry in fs::read_dir(&base)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() || entry.path() == path {
                continue;
            }
            match OpenOptions::new()
                .read(true)
                .write(true)
                .open(entry.path().join(".lock"))
            {
                Ok(other) if other.try_lock().is_ok() => {
                    let _ = fs::remove_dir_all(entry.path());
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let _ = fs::remove_dir_all(entry.path());
                }
                _ => {}
            }
        }
        let mut candidates = Vec::new();
        let mut retained = 0u64;
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let stage = entry.path();
            let bytes = read_record(&stage).ok().and_then(|r| {
                r.files.values().try_fold(
                    fs::metadata(stage.join("complete")).ok()?.len(),
                    |sum, f| sum.checked_add(f.stored_len()),
                )
            });
            if let Some(bytes) = bytes.filter(|_| candidates.len() < MAX_STAGES) {
                retained = retained.saturating_add(bytes);
                candidates.push((stage, bytes));
            } else {
                let _ = fs::remove_dir_all(stage);
            }
        }
        // Add back our own retained bytes so restarting does not repeatedly shrink the cap.
        let capacity = limit
            .unwrap_or(MAX_BYTES)
            .min(MAX_BYTES)
            .min(free_bytes(&base)?.saturating_add(retained) / 4);
        let mut kept = 0u64;
        candidates.retain(|(path, bytes)| {
            if kept.saturating_add(*bytes) <= capacity {
                kept += bytes;
                true
            } else {
                let _ = fs::remove_dir_all(path);
                false
            }
        });
        Ok(Arc::new(Self {
            path,
            capacity,
            file_capacity: file_capacity()?,
            state: Mutex::default(),
            _lock: lock,
            candidates: Mutex::new(candidates.into_iter().map(|(p, _)| p).collect()),
            recovered: tokio::sync::OnceCell::new(),
            stop,
        }))
    }
    /// Validate all retained stages once. Queries wait for this task, so their first routing
    /// read can use disk, and concurrent queries cannot race an incomplete adoption.
    pub(crate) fn recover<'a>(
        self: &'a Arc<Self>,
        store: &'a Storage,
    ) -> futures::future::BoxFuture<'a, ()> {
        if self.recovered.get().is_some() {
            return Box::pin(std::future::ready(()));
        }
        Box::pin(async move {
            self.recovered
            .get_or_init(|| Box::pin(async {
                let started = Instant::now();
                let paths =
                    std::mem::take(&mut *self.candidates.lock().expect("ANN candidates lock"));
                for path in paths {
                    let result = self.stop.run(self.adopt(store, &path)).await;
                    match result {
                        Some(Ok((record, stage))) => {
                            let key =
                                stage_key(&record.scope, &record.set.dir.key, record.companions);
                            let mut state = self.state.lock().expect("ANN staging lock");
                            for (hint, (object, len)) in record.hints {
                                state.hints.insert(hint, Hint { object, len });
                            }
                            Self::install(&mut state, key, &record.scope, stage);
                            metrics::counter!("blowfish_ann_staging_total", "outcome" => "adopted")
                                .increment(1);
                        }
                        Some(Err(_)) => {
                            let _ = fs::remove_dir_all(path);
                        }
                        None => {}
                    }
                }
                metrics::histogram!("blowfish_ann_adoption_seconds")
                    .record(started.elapsed().as_secs_f64());
            }))
            .await;
        })
    }
    async fn adopt(self: &Arc<Self>, store: &Storage, path: &Path) -> io::Result<(Record, Stage)> {
        let mut record = read_record(path)?;
        let scoped = store.scoped(record.scope.trim_end_matches('\0'));
        let ns = record
            .set
            .dir
            .key
            .strip_prefix("ns/")
            .and_then(|s| s.split('/').next())
            .ok_or_else(|| io::Error::other("invalid staged namespace"))?;
        let wal = bf_wal::Wal::new(scoped.clone(), ns).map_err(io::Error::other)?;
        let (generation, manifest) = wal
            .manifest_view_after(0, bf_wal::ManifestView::Reader)
            .await
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("deleted staged namespace"))?;
        let expected = serde_json::to_vec(&record.set).map_err(io::Error::other)?;
        if manifest.deleted
            || !manifest
                .ann_tiers
                .values()
                .flatten()
                .any(|set| serde_json::to_vec(set).ok().as_ref() == Some(&expected))
        {
            return Err(io::Error::other("unreferenced ANN stage"));
        }
        // Deletion is fenced in the WAL before the indexer publishes a tombstone
        // manifest. tail_bounded starts at the last tombstone, so a small decoded
        // budget still observes any deletion after this checkpoint, including recreation.
        let tail = wal
            .tail_bounded(manifest.indexed_through, 1 << 20)
            .await
            .map_err(io::Error::other)?;
        if tail.iter().any(|(_, entry)| entry.tombstone) {
            return Err(io::Error::other("ANN namespace deleted after checkpoint"));
        }
        let objects: Vec<_> = record
            .files
            .iter()
            .map(|(key, file)| {
                (
                    file.source.clone().unwrap_or_else(|| key.clone()),
                    file.identity.clone(),
                )
            })
            .collect();
        futures::stream::iter(objects.into_iter().map(|(key, expected)| {
            let scoped = scoped.clone();
            async move {
                let current = scoped
                    .object_identity(&key)
                    .await
                    .map_err(io::Error::other)?;
                if current != expected || (current.etag.is_none() && current.version.is_none()) {
                    return Err(io::Error::other("ANN source identity changed"));
                }
                Ok(())
            }
        }))
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await?;
        let bytes = record
            .files
            .values()
            .try_fold(fs::metadata(path.join("complete"))?.len(), |sum, f| {
                sum.checked_add(f.stored_len())
            })
            .ok_or_else(|| io::Error::other("ANN size overflow"))?;
        let physical: HashSet<_> = record.files.values().map(|f| f.name.as_str()).collect();
        let lease = self
            .reserve(bytes, physical.len())
            .ok_or_else(|| io::Error::other("ANN adoption capacity"))?;
        let mut stage = Stage {
            path: path.to_owned(),
            files: HashMap::new(),
            bad: AtomicBool::new(false),
            _lease: lease,
            durable: false,
        };
        // Entries of a pack share one local file, so each file is opened and measured once,
        // and must be exactly as long as its furthest section ends.
        let mut ends: HashMap<&str, u64> = HashMap::new();
        for f in record.files.values() {
            let end = f
                .offset
                .checked_add(f.stored_len())
                .ok_or_else(|| io::Error::other("ANN size overflow"))?;
            let slot = ends.entry(f.name.as_str()).or_default();
            *slot = (*slot).max(end);
        }
        let mut opened: HashMap<&str, Arc<File>> = HashMap::new();
        for (name, end) in ends {
            if name.contains('/') || name.contains('\\') {
                return Err(io::Error::other("invalid ANN file metadata"));
            }
            let handle = File::open(path.join(name))?;
            if !handle.metadata()?.is_file() || handle.metadata()?.len() != end {
                return Err(io::Error::other("incomplete ANN staged file"));
            }
            opened.insert(name, Arc::new(handle));
        }
        for (key, f) in &record.files {
            if let Some(ends) = &f.encoded {
                codec::validate(ends, f.len)?;
            }
            let packed = record.companions && f.name == PACK;
            if !(f.name.starts_with("object-") || packed)
                || f.name.contains('/')
                || f.name.contains('\\')
                || (f.offset != 0 && !packed)
                || f.crc.len() as u64 != f.len.div_ceil(BLOCK as u64)
            {
                return Err(io::Error::other("invalid ANN file metadata"));
            }
            stage.files.insert(
                key.clone(),
                StagedFile {
                    file: opened[f.name.as_str()].clone(),
                    offset: f.offset,
                    base: f.base,
                    len: f.len,
                    crc: f.crc.clone(),
                    encoded: f.encoded.clone(),
                    identity: Some(f.identity.clone()),
                    source: f.source.clone(),
                },
            );
        }
        // Only metadata is inspected here. Every 16 KiB payload block is still CRC-checked
        // on every read, including bytes outside the requested subrange.
        // The saved checkpoint is advisory, and may predate a maintenance publication.
        // Rebuild it from the manifest already read during adoption, never from disk alone.
        record.checkpoint =
            (!manifest.deleted && manifest.indexed_through > 0).then_some(Checkpoint {
                generation,
                indexed_through: manifest.indexed_through,
            });
        self.note_checkpoint(&record.scope, ns, record.checkpoint);
        stage.durable = true;
        Ok((record, stage))
    }
    /// Remember an authoritative observation without retaining an unbounded namespace map.
    pub(crate) fn note_checkpoint(&self, scope: &str, ns: &str, checkpoint: Option<Checkpoint>) {
        let key = format!("{scope}ns/{ns}/");
        let mut state = self.state.lock().expect("ANN staging lock");
        if let Some(i) = state.checkpoints.iter().position(|(k, _)| k == &key) {
            if checkpoint.is_some_and(|c| c.generation < state.checkpoints[i].1.generation) {
                return;
            }
            state.checkpoints.remove(i);
        }
        if state.snapshots.get(&key).is_some_and(|s| {
            checkpoint.is_none_or(|c| {
                c.generation != s.manifest_seq || c.indexed_through != s.indexed_through
            })
        }) {
            state.snapshots.remove(&key);
        }
        if let Some(checkpoint) = checkpoint {
            state.checkpoints.push_back((key, checkpoint));
            if state.checkpoints.len() > MAX_STAGES
                && let Some((key, _)) = state.checkpoints.pop_front()
            {
                state.snapshots.remove(&key);
            }
        }
    }
    /// Retain only a manifest-confirmed checkpoint, before replaying any WAL overlay.
    pub(crate) fn note_snapshot(&self, scope: &str, ns: &str, snapshot: &crate::NamespaceState) {
        if !snapshot.exists
            || snapshot.seq != snapshot.indexed_through
            || snapshot.tail_bytes != 0
            || !snapshot.docs.is_empty()
            || !snapshot.shadowed.is_empty()
        {
            return;
        }
        let key = format!("{scope}ns/{ns}/");
        let mut state = self.state.lock().expect("ANN staging lock");
        if state.checkpoints.iter().any(|(k, c)| {
            k == &key
                && c.generation == snapshot.manifest_seq
                && c.indexed_through == snapshot.indexed_through
        }) {
            state.snapshots.insert(key, Arc::new(snapshot.clone()));
        }
    }
    /// Advisory state for a cold staged search. A fresh snapshot must validate every result.
    pub(crate) fn snapshot(
        &self,
        scope: &str,
        ns: &str,
        attribute: &str,
    ) -> Option<Arc<crate::NamespaceState>> {
        let checkpoint = self.checkpoint(scope, ns)?;
        let state = self.state.lock().expect("ANN staging lock");
        let snapshot = state
            .snapshots
            .get(&format!("{scope}ns/{ns}/"))
            .filter(|s| {
                s.manifest_seq == checkpoint.generation
                    && s.indexed_through == checkpoint.indexed_through
            })
            .cloned()?;
        let sets = snapshot
            .ann_tiers
            .get(attribute)
            .filter(|sets| !sets.is_empty())?;
        sets.iter()
            .all(|set| {
                state
                    .ready
                    .get(&stage_key(scope, &set.dir.key, false))
                    .is_some_and(|(_, stage)| !stage.bad.load(Ordering::Relaxed))
            })
            .then_some(snapshot)
    }
    fn known_checkpoint(&self, scope: &str, ns: &str) -> Option<Checkpoint> {
        let key = format!("{scope}ns/{ns}/");
        self.state
            .lock()
            .expect("ANN staging lock")
            .checkpoints
            .iter()
            .find_map(|(k, checkpoint)| (k == &key).then_some(*checkpoint))
    }
    /// Only provisioned namespaces speculate; ordinary S3 queries keep their read plan.
    pub(crate) fn checkpoint(&self, scope: &str, ns: &str) -> Option<Checkpoint> {
        let prefix = format!("{scope}stage:ns/{ns}/");
        let state = self.state.lock().expect("ANN staging lock");
        if !state
            .ready
            .iter()
            .any(|(key, (_, stage))| key.starts_with(&prefix) && !stage.bad.load(Ordering::Relaxed))
        {
            return None;
        }
        let key = format!("{scope}ns/{ns}/");
        state
            .checkpoints
            .iter()
            .find_map(|(k, checkpoint)| (k == &key).then_some(*checkpoint))
    }
    fn install(state: &mut State, key: String, scope: &str, stage: Stage) {
        let stage = Arc::new(stage);
        for object in stage.files.keys() {
            state
                .files
                .insert(format!("{scope}{object}"), Arc::downgrade(&stage));
        }
        state.clock += 1;
        state.ready.insert(key, (state.clock, stage));
    }
    pub(crate) fn remove_namespace(&self, scope: &str, ns: &str) {
        self.note_checkpoint(scope, ns, None);
        let prefix = format!("{scope}stage:ns/{ns}/");
        let mut state = self.state.lock().expect("ANN staging lock");
        let pending: Vec<_> = state
            .pending
            .iter()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        state.cancelled.extend(pending);
        let keys: Vec<_> = state
            .ready
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        let mut removed = Vec::new();
        for key in keys {
            if let Some((_, stage)) = state.ready.remove(&key) {
                stage.bad.store(true, Ordering::Relaxed);
                removed.push(stage);
            }
        }
        let doomed: Vec<_> = state
            .prestaged
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        let doomed: Vec<_> = doomed
            .iter()
            .filter_map(|key| state.prestaged.remove(key))
            .collect();
        state.files.retain(|_, stage| {
            stage
                .upgrade()
                .is_some_and(|s| !s.bad.load(Ordering::Relaxed))
        });
        state
            .hints
            .retain(|key, _| !key.starts_with(&format!("{scope}ns/{ns}/")));
        drop(state);
        drop(removed);
        drop(doomed);
    }
    pub(crate) fn retain_namespace(
        &self,
        scope: &str,
        ns: &str,
        tiers: &BTreeMap<String, Vec<SlabSet>>,
    ) {
        let prefix = format!("{scope}stage:ns/{ns}/");
        let live: HashSet<_> = tiers
            .values()
            .flatten()
            .flat_map(|s| {
                [
                    stage_key(scope, &s.dir.key, false),
                    stage_key(scope, &s.dir.key, true),
                ]
            })
            .collect();
        let mut state = self.state.lock().expect("ANN staging lock");
        let obsolete: Vec<_> = state
            .ready
            .keys()
            .chain(state.pending.iter())
            .filter(|key| key.starts_with(&prefix) && !live.contains(*key))
            .cloned()
            .collect();
        // A committed tier that the manifest no longer names was superseded before any query
        // used it. One awaiting its commit is not in the manifest yet, and stays.
        let stale: Vec<_> = state
            .prestaged
            .iter()
            .filter(|(key, p)| {
                key.starts_with(&prefix) && p.active.load(Ordering::Acquire) && !live.contains(*key)
            })
            .map(|(key, _)| key.clone())
            .collect();
        let stale: Vec<_> = stale
            .iter()
            .filter_map(|key| state.prestaged.remove(key))
            .collect();
        let mut removed = Vec::new();
        for key in obsolete {
            if state.pending.contains(&key) {
                state.cancelled.insert(key.clone());
            }
            if let Some((_, stage)) = state.ready.remove(&key) {
                stage.bad.store(true, Ordering::Relaxed);
                removed.push(stage);
            }
        }
        state.files.retain(|_, stage| {
            stage
                .upgrade()
                .is_some_and(|s| !s.bad.load(Ordering::Relaxed))
        });
        drop(state);
        drop(removed);
        drop(stale);
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
            evicted.bad.store(true, Ordering::Relaxed);
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
        file.local(ranges)?;
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
        extras: &dyn Fn() -> Vec<Extra>,
    ) {
        if self.stop.stopping() {
            return;
        }
        let key = stage_key(scope, &set.dir.key, false);
        let ckey = stage_key(scope, &set.dir.key, true);
        let mut doomed = Vec::new();
        let mut pre = None;
        let (go, companion_only) = {
            let mut state = self.state.lock().expect("ANN staging lock");
            for k in [&key, &ckey] {
                if state
                    .ready
                    .get(k)
                    .is_some_and(|(_, s)| s.bad.load(Ordering::Relaxed))
                {
                    let (_, bad) = state.ready.remove(k).expect("present stage");
                    let weak = Arc::downgrade(&bad);
                    state.files.retain(|_, value| !Weak::ptr_eq(value, &weak));
                    state.settled.remove(&key);
                    doomed.push(bad);
                }
            }
            let main = state.ready.contains_key(&key);
            let retry = if main { &ckey } else { &key };
            // Prestaged bytes wait for the manifest commit that publishes their tier: until then
            // this tier is not staged at all, and the next query asks again.
            let uncommitted = state
                .prestaged
                .get(&key)
                .is_some_and(|p| !p.active.load(Ordering::Acquire));
            let go = !(main && state.settled.contains(&key))
                && state.pending.is_empty()
                && !uncommitted
                && !state
                    .failed
                    .get(retry)
                    .is_some_and(|at| at.elapsed() < RETRY);
            if go {
                state.pending.insert(key.clone());
                pre = state.prestaged.remove(&key);
            }
            (go, main)
        };
        drop(doomed);
        if !go {
            return;
        }
        let (manager, store, scope, set) =
            (self.clone(), store.clone(), scope.to_owned(), set.clone());
        if companion_only {
            // The code stage is already published (adopted after a restart, or from a version
            // that staged no pack). Add whichever companions no stage holds yet.
            let mut lease = self.empty_lease();
            let extras = self.plan_companions(&scope, extras(), &|_| false, &mut lease);
            if extras.is_empty() {
                let mut state = self.state.lock().expect("ANN staging lock");
                state.pending.remove(&key);
                state.settled.insert(key);
                return;
            }
            tokio::spawn(async move {
                manager
                    .finish_companions(&store, &scope, &set, key, Some((lease, extras)))
                    .await;
            });
            return;
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
        // The prestage's own charge is released first, and the stage reserves its estimate as
        // usual: the bytes are on disk, and the sum never exceeds the cap.
        if let Some(pre) = &pre {
            drop(pre.lease.lock().expect("prestage lease").take());
        }
        let Some(lease) = self.reserve(estimate, files) else {
            self.state
                .lock()
                .expect("ANN staging lock")
                .pending
                .remove(&key);
            return;
        };
        let extras = extras();
        tokio::spawn(async move {
            // Phase one: routing, codes, and any exact vectors. Queries use it at once.
            let result = manager
                .stop
                .run(manager.build(&store, &scope, &set, lease, extras, pre))
                .await;
            let job = {
                let mut state = manager.state.lock().expect("ANN staging lock");
                let cancelled = state.cancelled.contains(&key);
                let mut doomed = None;
                let job = match result {
                    Some(Ok((stage, _))) if cancelled => {
                        stage.bad.store(true, Ordering::Relaxed);
                        doomed = Some(stage);
                        None
                    }
                    Some(Ok((stage, job))) => {
                        Self::install(&mut state, key.clone(), &scope, stage);
                        metrics::counter!("blowfish_ann_staging_total", "outcome" => "ready")
                            .increment(1);
                        Some(job)
                    }
                    Some(Err(error)) => {
                        if state.failed.len() >= 128 {
                            state.failed.clear();
                        }
                        state.failed.insert(key.clone(), Instant::now());
                        metrics::counter!("blowfish_ann_staging_total", "outcome" => "error")
                            .increment(1);
                        tracing::debug!(%error, "ANN staging unavailable; using object storage");
                        None
                    }
                    None => None,
                };
                if job.is_none() {
                    state.pending.remove(&key);
                    state.cancelled.remove(&key);
                }
                drop(state);
                drop(doomed);
                job
            };
            let Some(job) = job else { return };
            manager
                .finish_companions(&store, &scope, &set, key, job)
                .await;
        });
    }
    fn empty_lease(self: &Arc<Self>) -> Lease {
        Lease {
            manager: Arc::downgrade(self),
            bytes: 0,
            files: 0,
        }
    }
    /// Phase two: gather the tier's companions into one pack and publish it beside the code
    /// stage. The pending marker taken for the tier is released here.
    async fn finish_companions(
        self: &Arc<Self>,
        store: &Storage,
        scope: &str,
        set: &SlabSet,
        key: String,
        job: Option<(Lease, Vec<Extra>)>,
    ) {
        let ckey = stage_key(scope, &set.dir.key, true);
        let result = match job {
            Some((lease, extras)) => {
                if self
                    .state
                    .lock()
                    .expect("ANN staging lock")
                    .cancelled
                    .contains(&key)
                {
                    None
                } else {
                    self.stop
                        .run(self.build_companions(store, scope, set, lease, extras))
                        .await
                }
            }
            None => Some(Ok(None)),
        };
        let mut state = self.state.lock().expect("ANN staging lock");
        state.pending.remove(&key);
        let cancelled = state.cancelled.remove(&key);
        match result {
            Some(Ok(Some(stage))) if cancelled => {
                stage.bad.store(true, Ordering::Relaxed);
                drop(state);
                drop(stage);
            }
            Some(Ok(stage)) if !cancelled => {
                if let Some(stage) = stage {
                    Self::install(&mut state, ckey, scope, stage);
                }
                metrics::counter!("blowfish_ann_staging_total", "outcome" => "companions")
                    .increment(1);
                if state.settled.len() >= 1024 {
                    state.settled.clear();
                }
                state.settled.insert(key);
            }
            Some(Err(error)) => {
                if state.failed.len() >= 128 {
                    state.failed.clear();
                }
                state.failed.insert(ckey, Instant::now());
                tracing::debug!(%error, "ANN companions unavailable; using object storage");
            }
            _ => {}
        }
    }
    /// Admit companions that no stage holds yet, smallest first (the column directories
    /// before the ID maps), charging `lease` without evicting anything. They are far smaller
    /// than the vectors they serve alongside, and are admitted first.
    fn plan_companions(
        &self,
        scope: &str,
        mut extras: Vec<Extra>,
        skip: &dyn Fn(&Extra) -> bool,
        lease: &mut Lease,
    ) -> Vec<Extra> {
        extras.sort_by_key(|e| (e.len(), e.key()));
        let mut planned: Vec<Extra> = Vec::new();
        for extra in extras {
            let staged = self
                .file(&format!("{scope}{}", extra.key()))
                .is_some_and(|s| s.files.contains_key(&extra.key()));
            if staged || skip(&extra) {
                continue;
            }
            let len = extra.len();
            let cost = len + len.div_ceil(BLOCK as u64) * 11 + extra.key().len() as u64 + 1024;
            // The pack is a single descriptor, however many companions it holds.
            if (planned.is_empty() && !self.extend_files(lease, 1)) || !self.extend(lease, cost) {
                break;
            }
            planned.push(extra);
        }
        planned
    }
    /// Optional vectors cannot evict another tier's codes. Keep the original reservation
    /// on refusal, and charge the extra bytes for the entire active-reader lifetime.
    fn extend(&self, lease: &mut Lease, extra: u64) -> bool {
        if extra == 0 {
            return true;
        }
        let Some(bytes) = lease.bytes.checked_add(extra) else {
            return false;
        };
        if bytes > self.capacity
            || !free_bytes(&self.path).is_ok_and(|n| n >= bytes.saturating_mul(2))
        {
            return false;
        }
        let mut state = self.state.lock().expect("ANN staging lock");
        if state.used.saturating_add(extra) > self.capacity {
            return false;
        }
        state.used += extra;
        lease.bytes = bytes;
        true
    }
    /// Reserve descriptors for optional files, within the manager's file limit.
    fn extend_files(&self, lease: &mut Lease, files: usize) -> bool {
        let mut state = self.state.lock().expect("ANN staging lock");
        if state.files_used.saturating_add(files) > self.file_capacity {
            return false;
        }
        state.files_used += files;
        lease.files += files;
        true
    }
    async fn build(
        self: &Arc<Self>,
        store: &Storage,
        scope: &str,
        set: &SlabSet,
        lease: Lease,
        extras: Vec<Extra>,
        pre: Option<Arc<PreInner>>,
    ) -> io::Result<(Stage, Option<(Lease, Vec<Extra>)>)> {
        // A prestage's directory becomes this stage's: from here the stage owns its deletion.
        let path = match &pre {
            Some(pre) => {
                pre.taken.store(true, Ordering::Release);
                pre.dir.clone()
            }
            None => {
                let path = self.path.join(uuid::Uuid::new_v4().to_string());
                fs::create_dir(&path)?;
                path
            }
        };
        let mut stage = Stage {
            path,
            files: HashMap::new(),
            bad: AtomicBool::new(false),
            _lease: lease,
            durable: false,
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
        let mut exact = limits.clone();
        for entry in dir.entries() {
            let key = &dir.segments()[entry.segment].key;
            exact
                .entry(key.clone())
                .and_modify(|n| *n = (*n).max(entry.vectors_range().end))
                .or_insert(entry.vectors_range().end);
        }
        // Codes keep priority. Release unused estimation slack, retaining the completion
        // reader's full metadata allowance, then admit whole exact-vector objects that fit.
        let code_bytes = limits
            .values()
            .try_fold(
                set.dir.size + set.pages.as_ref().map_or(0, |p| p.size),
                |sum, n| sum.checked_add(*n),
            )
            .ok_or_else(|| io::Error::other("ANN stage size overflow"))?;
        let code_reservation = code_bytes.saturating_add(32 << 20);
        if code_reservation < stage._lease.bytes {
            let released = stage._lease.bytes - code_reservation;
            self.state.lock().expect("ANN staging lock").used -= released;
            stage._lease.bytes = code_reservation;
        }
        // Hydration reads follow the vector rerank on every query, and are far smaller than
        // the vectors: reserve them first. They are fetched into a pack after this stage
        // is published, so queries never wait for them.
        let mut comp_lease = self.empty_lease();
        let companions = self.plan_companions(
            scope,
            extras,
            &|e| limits.contains_key(&e.file.key),
            &mut comp_lease,
        );
        let prefixes = limits.clone();
        let mut extras: Vec<_> = exact
            .into_iter()
            .filter_map(|(key, len)| {
                let delta = len.saturating_sub(limits.get(&key).copied().unwrap_or(0));
                if delta == 0 {
                    return None;
                }
                let extra = delta
                    .div_ceil(16)
                    .checked_mul(15)?
                    .checked_add(delta.div_ceil(BLOCK as u64).checked_mul(22)?)?
                    .checked_add(if limits.contains_key(&key) {
                        0
                    } else {
                        key.len() as u64 + 1024
                    })?;
                Some((extra, key, len))
            })
            .collect();
        // Objects already written locally to their full length cost no download: admit them
        // first. Without a prestage this is the plain smallest-first order.
        let local = |key: &str, len: u64| pre.as_ref().is_some_and(|p| p.covers(key, len));
        extras.sort_unstable_by(|a, b| (!local(&a.1, a.2), a).cmp(&(!local(&b.1, b.2), b)));
        for (extra, key, len) in extras {
            let count = limits.len() + usize::from(!limits.contains_key(&key));
            if count
                <= stage
                    ._lease
                    .files
                    .saturating_sub(1 + usize::from(set.pages.is_some()))
                && self.extend(&mut stage._lease, extra)
            {
                limits.insert(key, len);
            }
        }
        stage.files.insert(set.dir.key.clone(), directory);
        if let Some(file) = &set.pages {
            let bytes = self
                .download(store, file, file.size, &stage.path.join("pages"))
                .await?;
            stage.files.insert(file.key.clone(), bytes);
        }
        let sources_to_copy = limits
            .into_iter()
            .map(|(key, len)| {
                let source = refs
                    .get(key.as_str())
                    .ok_or_else(|| io::Error::other("ANN stage source missing"))?;
                if len > source.size {
                    return Err(io::Error::other("ANN prefix exceeds object"));
                }
                Ok(((*source).clone(), 0, len))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut sources_to_copy = sources_to_copy;
        if let Some(pre) = &pre {
            let reused = pre
                .adopt(&mut sources_to_copy)
                .await
                .map_err(|e| io::Error::other(format!("prestage: {e}")))?;
            stage.files.extend(reused);
        }
        stage.files.extend(
            self.download_many(store, &sources_to_copy, &stage.path, true)
                .await?,
        );
        let sources: HashMap<_, _> = std::iter::once(&set.dir)
            .chain(set.pages.iter())
            .chain(set.slabs.iter().flat_map(|s| s.files.values()))
            .map(|f| (f.key.as_str(), f))
            .collect();
        let identities: Vec<_> = stage
            .files
            .iter()
            .map(|(key, file)| {
                (
                    file.source.clone().unwrap_or_else(|| key.clone()),
                    file.identity.clone(),
                )
            })
            .collect();
        futures::stream::iter(identities.into_iter().map(|(key, identity)| {
            let store = store.clone();
            async move {
                let current = store
                    .object_identity(&key)
                    .await
                    .map_err(io::Error::other)?;
                if identity.as_ref() != Some(&current) {
                    return Err(io::Error::other("ANN source changed during publication"));
                }
                Ok(())
            }
        }))
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await?;
        let mut files = BTreeMap::new();
        let mut keys: Vec<_> = stage.files.keys().collect();
        keys.sort_unstable();
        for (n, key) in keys.into_iter().enumerate() {
            let f = &stage.files[key];
            let identity = f
                .identity
                .clone()
                .ok_or_else(|| io::Error::other("missing ANN identity"))?;
            if identity.etag.is_none() && identity.version.is_none() {
                return Err(io::Error::other("backend has no immutable object identity"));
            }
            let _ = sources
                .get(f.source.as_deref().unwrap_or(key.as_str()))
                .ok_or_else(|| io::Error::other("unknown staged object"))?;
            let name = format!("object-{n}");
            // Existing builder names are deliberately unrelated to final record names.
            let target = stage.path.join(&name);
            let old = stage.path.join(if key == &set.dir.key {
                "directory".to_owned()
            } else if set.pages.as_ref().is_some_and(|p| &p.key == key) {
                "pages".to_owned()
            } else {
                format!("source-{}", digest(key.as_bytes()))
            });
            fs::rename(old, target)?;
            files.insert(
                key.clone(),
                FileRecord {
                    name,
                    offset: 0,
                    base: f.base,
                    len: f.len,
                    crc: f.crc.clone(),
                    encoded: f.encoded.clone(),
                    identity,
                    source: f.source.clone(),
                },
            );
        }
        let ns = set
            .dir
            .key
            .strip_prefix("ns/")
            .and_then(|s| s.split('/').next())
            .ok_or_else(|| io::Error::other("invalid staged namespace"))?;
        let hint_prefix = format!("{scope}ns/{ns}/");
        let hints = self
            .state
            .lock()
            .expect("ANN staging lock")
            .hints
            .iter()
            .filter(|(key, h)| {
                key.starts_with(&hint_prefix)
                    && h.object
                        .as_ref()
                        .is_none_or(|o| stage.files.contains_key(o))
            })
            .map(|(k, h)| (k.clone(), (h.object.clone(), h.len)))
            .collect();
        let mut record = Record {
            scope: scope.to_owned(),
            set: set.clone(),
            hints,
            files,
            companions: false,
            checkpoint: self.known_checkpoint(scope, ns),
        };
        // Exceptional blocks may invalidate the normalized-vector size estimate. Use
        // the actual completion size, and retain every code prefix if optional tails
        // cannot be charged. No published stage exceeds its active-reader lease.
        let mut actual: u64 = stage.files.values().map(StagedFile::stored_len).sum();
        let mut optional: Vec<_> = prefixes.keys().cloned().collect();
        optional.sort();
        loop {
            let metadata = serde_json::to_vec(&record).map_err(io::Error::other)?.len() as u64 + 32;
            let needed = actual.saturating_add(metadata);
            let extra = needed.saturating_sub(stage._lease.bytes);
            if needed <= stage._lease.bytes || self.extend(&mut stage._lease, extra) {
                break;
            }
            let key = optional
                .pop()
                .ok_or_else(|| io::Error::other("ANN stage exceeds reservation"))?;
            let len = prefixes[&key];
            let file = stage.files.get_mut(&key).expect("source");
            if file.len <= len {
                continue;
            }
            let raw = file.read(std::slice::from_ref(&(0..len)))?.remove(0);
            let previous = file.stored_len();
            file.file.write_all_at(&raw, 0)?;
            file.file.set_len(len)?;
            file.file.sync_all()?;
            file.len = len;
            file.encoded = None;
            file.crc = raw.chunks(BLOCK).map(crc32fast::hash).collect();
            actual = actual - previous + len;
            let saved = record.files.get_mut(&key).expect("record");
            saved.len = len;
            saved.encoded = None;
            saved.crc = file.crc.clone();
        }
        if crate::ann::trace_stages() {
            let staged_rows: u64 = dir
                .entries()
                .iter()
                .filter(|e| {
                    stage
                        .files
                        .get(&dir.segments()[e.segment].key)
                        .is_some_and(|f| f.len >= e.vectors_range().end)
                })
                .map(|e| u64::from(e.rows))
                .sum();
            eprintln!(
                "STAGED_VECTORS rows={staged_rows} total_rows={}",
                set.rows()
            );
        }
        let descriptors = stage.files.len();
        self.seal(&mut stage, &record, actual, descriptors)?;
        let job = (!companions.is_empty()).then_some((comp_lease, companions));
        Ok((stage, job))
    }
    /// Write the completion record, publish the stage directory under its digest, and
    /// shrink the reservation to the bytes and descriptors actually used.
    fn seal(
        &self,
        stage: &mut Stage,
        record: &Record,
        payload: u64,
        descriptors: usize,
    ) -> io::Result<()> {
        let raw = serde_json::to_vec(record).map_err(io::Error::other)?;
        let actual = payload
            .checked_add(raw.len() as u64 + 32)
            .filter(|n| *n <= stage._lease.bytes)
            .ok_or_else(|| io::Error::other("ANN completion exceeds reservation"))?;
        let mut complete = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(stage.path.join("completing"))?;
        complete.write_all(&Sha256::digest(&raw))?;
        complete.write_all(&raw)?;
        complete.sync_all()?;
        fs::rename(stage.path.join("completing"), stage.path.join("complete"))?;
        File::open(&stage.path)?.sync_all()?;
        let named = self.path.join(digest(&raw));
        fs::rename(&stage.path, &named)?;
        stage.path = named;
        File::open(&self.path)?.sync_all()?;
        stage.durable = true;
        let released = stage._lease.bytes - actual;
        let mut state = self.state.lock().expect("ANN staging lock");
        state.used -= released;
        state.files_used -= stage._lease.files - descriptors;
        drop(state);
        stage._lease.bytes = actual;
        stage._lease.files = descriptors;
        metrics::histogram!("blowfish_ann_staging_bytes").record(actual as f64);
        Ok(())
    }
    /// Fetch a tier's companions with bounded parallel GETs, each carrying the identity of
    /// the object it was served from, and append them to one local pack: one descriptor,
    /// one `fsync`, and no HEAD requests. Returns `None` when nothing qualifies.
    async fn build_companions(
        self: &Arc<Self>,
        store: &Storage,
        scope: &str,
        set: &SlabSet,
        lease: Lease,
        extras: Vec<Extra>,
    ) -> io::Result<Option<Stage>> {
        let path = self.path.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir(&path)?;
        let mut stage = Stage {
            path: path.clone(),
            files: HashMap::new(),
            bad: AtomicBool::new(false),
            _lease: lease,
            durable: false,
        };
        let pack = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path.join(PACK))?,
        );
        let mut fetched = futures::stream::iter(extras.into_iter().map(|extra| {
            let store = store.clone();
            async move { fetch_companion(&store, extra).await }
        }))
        .buffer_unordered(COMPANION_FETCHES);
        let (mut buffer, mut written) = (Vec::<u8>::new(), 0u64);
        let mut files = HashMap::new();
        while let Some(item) = fetched.try_next().await? {
            let Some(item) = item else { continue };
            files.insert(
                item.key,
                StagedFile {
                    file: pack.clone(),
                    offset: written + buffer.len() as u64,
                    base: item.base,
                    len: item.bytes.len() as u64,
                    crc: item.crc,
                    encoded: None,
                    identity: Some(item.identity),
                    source: item.source,
                },
            );
            buffer.extend_from_slice(&item.bytes);
            if buffer.len() >= PACK_FLUSH {
                written = flush_pack(&pack, &mut buffer, written).await?;
            }
        }
        drop(fetched);
        if files.is_empty() {
            return Ok(None);
        }
        written = flush_pack(&pack, &mut buffer, written).await?;
        let handle = pack.clone();
        tokio::task::spawn_blocking(move || {
            handle.set_len(written)?;
            handle.sync_all()
        })
        .await
        .map_err(io::Error::other)??;
        let records = files
            .iter()
            .map(|(key, f)| {
                Ok((
                    key.clone(),
                    FileRecord {
                        name: PACK.to_owned(),
                        offset: f.offset,
                        base: f.base,
                        len: f.len,
                        crc: f.crc.clone(),
                        encoded: f.encoded.clone(),
                        identity: f
                            .identity
                            .clone()
                            .ok_or_else(|| io::Error::other("missing ANN identity"))?,
                        source: f.source.clone(),
                    },
                ))
            })
            .collect::<io::Result<BTreeMap<_, _>>>()?;
        let record = Record {
            scope: scope.to_owned(),
            set: set.clone(),
            hints: BTreeMap::new(),
            files: records,
            companions: true,
            checkpoint: set
                .dir
                .key
                .strip_prefix("ns/")
                .and_then(|s| s.split('/').next())
                .and_then(|ns| self.known_checkpoint(scope, ns)),
        };
        stage.files = files;
        self.seal(&mut stage, &record, written, 1)?;
        Ok(Some(stage))
    }

    /// One range pool spans every slab. A slot includes its positional write and CRC,
    /// bounding payload memory to DOWNLOADS * PIECE without per-slab queues of bytes.
    async fn download_many(
        &self,
        store: &Storage,
        sources: &[(SegmentFile, u64, u64)],
        path: &Path,
        compress: bool,
    ) -> io::Result<HashMap<String, StagedFile>> {
        let mut files = HashMap::new();
        let mut jobs = Vec::new();
        // HEADs and file creation hold no payload memory.
        // Own descriptors before constructing futures, keeping the spawned build Send.
        let sources = sources.to_vec();
        let mut opened = futures::stream::iter(sources.into_iter().map(|(source, base, len)| {
            let (store, path) = (store.clone(), path.to_owned());
            async move {
                let identity = store
                    .object_identity(&source.key)
                    .await
                    .map_err(io::Error::other)?;
                let file = tokio::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path.join(format!("source-{}", digest(source.key.as_bytes()))))
                    .await?;
                if !compress {
                    file.set_len(len).await?;
                }
                Ok::<_, io::Error>((source, base, len, identity, file.into_std().await))
            }
        }))
        .buffer_unordered(32);
        while let Some((source, base, len, identity, file)) = opened.try_next().await? {
            let file = Arc::new(file);
            let reader = store
                .range_reader_with_length(&source.key, source.size, true)
                .map_err(io::Error::other)?
                .bypass_cache();
            for start in (0..len).step_by(PIECE as usize) {
                jobs.push((
                    source.key.clone(),
                    reader.clone(),
                    file.clone(),
                    base,
                    compress && len <= u64::from(u32::MAX),
                    start..(start + PIECE).min(len),
                ));
            }
            files.insert(
                source.key.clone(),
                StagedFile {
                    // One descriptor per source: the staged file shares the download handle.
                    file: file.clone(),
                    offset: 0,
                    base,
                    len,
                    crc: vec![0; len.div_ceil(BLOCK as u64) as usize],
                    encoded: (compress && len <= u64::from(u32::MAX)).then(Vec::new),
                    identity: Some(identity),
                    source: None,
                },
            );
        }
        let mut reads = futures::stream::iter(jobs.into_iter().map(
            |(key, reader, file, base, compress, range)| async move {
                let bytes = reader
                    .read_direct(base + range.start..base + range.end)
                    .await
                    .map_err(io::Error::other)?;
                tokio::task::spawn_blocking(move || {
                    let mut data = Vec::with_capacity(bytes.len());
                    let mut ends = Vec::new();
                    let mut crc = Vec::new();
                    for raw in bytes.chunks(BLOCK) {
                        let block = if compress {
                            codec::encode(raw)
                        } else {
                            raw.to_vec()
                        };
                        crc.push(crc32fast::hash(&block));
                        data.extend(block);
                        ends.push(data.len() as u32);
                    }
                    (key, file, range.start, data, ends, crc)
                })
                .await
                .map_err(io::Error::other)
            },
        ))
        // Preserve per-file order while overlapping bounded GETs and encoding work.
        .buffered(DOWNLOADS);
        while let Some((key, file, start, data, ends, crc)) = reads.try_next().await? {
            let target = files.get_mut(&key).expect("opened stage source");
            let offset = if let Some(index) = &mut target.encoded {
                let offset = index.last().copied().unwrap_or(0);
                for end in ends {
                    index.push(
                        offset
                            .checked_add(end)
                            .ok_or_else(|| io::Error::other("local exponent index overflow"))?,
                    );
                }
                u64::from(offset)
            } else {
                start
            };
            tokio::task::spawn_blocking(move || file.write_all_at(&data, offset))
                .await
                .map_err(io::Error::other)??;
            let first = start as usize / BLOCK;
            target.crc[first..first + crc.len()].copy_from_slice(&crc);
        }
        // Routing and code objects usually contain no compressible blocks. Keep their
        // existing zero-copy read path, and omit an index that would save no bytes.
        for file in files.values_mut() {
            if file.stored_len() == file.len {
                file.encoded = None;
            }
        }
        // No range task remains when files are synced and their identities rechecked.
        let syncing = files
            .iter()
            .map(|(key, file)| {
                (
                    key.clone(),
                    file.identity.clone(),
                    file.file.clone(),
                    file.stored_len(),
                )
            })
            .collect::<Vec<_>>();
        futures::stream::iter(syncing.into_iter().map(|(key, identity, handle, len)| {
            let store = store.clone();
            async move {
                tokio::task::spawn_blocking(move || {
                    // XFS can retain speculative allocation beyond EOF on an open
                    // streamed file. Even truncating to the current length releases it,
                    // keeping the physical footprint within the stage's reservation.
                    handle.set_len(len)?;
                    handle.sync_all()
                })
                .await
                .map_err(io::Error::other)??;
                let after = store
                    .object_identity(&key)
                    .await
                    .map_err(io::Error::other)?;
                if identity.as_ref() != Some(&after) {
                    return Err(io::Error::other("ANN source changed during download"));
                }
                Ok(())
            }
        }))
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await?;
        Ok(files)
    }
    async fn download(
        &self,
        store: &Storage,
        source: &SegmentFile,
        len: u64,
        path: &Path,
    ) -> io::Result<StagedFile> {
        use tokio::io::AsyncWriteExt;
        let before = store
            .object_identity(&source.key)
            .await
            .map_err(io::Error::other)?;
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
        file.set_len(len).await?;
        file.sync_all().await?;
        if before
            != store
                .object_identity(&source.key)
                .await
                .map_err(io::Error::other)?
        {
            return Err(io::Error::other("ANN source changed during download"));
        }
        Ok(StagedFile {
            file: Arc::new(file.into_std().await),
            offset: 0,
            base: 0,
            len,
            crc,
            encoded: None,
            identity: Some(before),
            source: None,
        })
    }
}

/// One companion, fetched whole and checksummed, ready to append to a pack.
struct Fetched {
    key: String,
    bytes: Bytes,
    crc: Vec<u32>,
    identity: ObjectIdentity,
    base: u64,
    source: Option<String>,
}
/// Fetch one companion with a single GET, taking its identity from that response. An ID map is
/// derived from the whole ID index; `None` means that segment has non-numeric IDs.
async fn fetch_companion(store: &Storage, extra: Extra) -> io::Result<Option<Fetched>> {
    let reader = store
        .range_reader_with_length(&extra.file.key, extra.file.size, true)
        .map_err(io::Error::other)?
        .bypass_cache();
    let (base, range) = match extra.kind {
        ExtraKind::Tail(base) => (base, base..extra.file.size),
        ExtraKind::NumericIds(_) => (0, 0..extra.file.size),
    };
    let (bytes, identity) = reader
        .read_direct_identified(range)
        .await
        .map_err(io::Error::other)?;
    if identity.etag.is_none() && identity.version.is_none() {
        return Err(io::Error::other("backend has no immutable object identity"));
    }
    let key = extra.key();
    match extra.kind {
        ExtraKind::Tail(_) => Ok(Some(Fetched {
            crc: bytes.chunks(BLOCK).map(crc32fast::hash).collect(),
            key,
            bytes,
            identity,
            base,
            source: None,
        })),
        ExtraKind::NumericIds(rows) => {
            let (store, source) = (store.clone(), extra.file.clone());
            let packed = tokio::task::spawn_blocking(move || {
                debug_assert_eq!(bytes.len() as u64, source.size);
                crate::segment::packed_numeric_ids(&store, &source, rows, bytes)
                    .map_err(|e| io::Error::other(format!("{e:?}")))
                    .map(|packed| {
                        packed.map(|p| {
                            let crc = p.chunks(BLOCK).map(crc32fast::hash).collect();
                            (Bytes::from(p), crc)
                        })
                    })
            })
            .await
            .map_err(io::Error::other)??;
            Ok(packed.map(|(bytes, crc)| Fetched {
                key,
                bytes,
                crc,
                identity,
                base: 0,
                source: Some(extra.file.key),
            }))
        }
    }
}
/// Write buffered pack bytes at `at`, returning the offset after them.
async fn flush_pack(file: &Arc<File>, buffer: &mut Vec<u8>, at: u64) -> io::Result<u64> {
    let (chunk, file) = (std::mem::take(buffer), file.clone());
    let end = at + chunk.len() as u64;
    tokio::task::spawn_blocking(move || file.write_all_at(&chunk, at))
        .await
        .map_err(io::Error::other)??;
    Ok(end)
}

// ---- Prestage: slab bytes written locally while a recluster uploads them ----

/// One slab object's stage bytes, written beside its upload.
struct Written {
    file: Arc<File>,
    /// Logical bytes covered: the codes' prefix, or through the exact vectors.
    len: u64,
    crc: Vec<u32>,
    encoded: Option<Vec<u32>>,
    identity: ObjectIdentity,
}
/// What a [`Prestage`] shares with the manager's registry.
struct PreInner {
    manager: Arc<Manager>,
    scope: String,
    dir: PathBuf,
    files: Mutex<HashMap<String, Written>>,
    /// Charges the written bytes against the staging cap until a query's stage takes over.
    lease: Mutex<Option<Lease>>,
    /// Set once the manifest commit that publishes the tier has happened.
    active: AtomicBool,
    /// Set when a stage owns the directory, which then decides its fate.
    taken: AtomicBool,
    /// Bytes per row a stage's codes will need, to keep room for the slabs still to come.
    code_row_bytes: u64,
}
impl Drop for PreInner {
    fn drop(&mut self) {
        if !self.taken.load(Ordering::Acquire) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}
impl PreInner {
    /// Whether the object is stored locally through at least `len` bytes.
    fn covers(&self, key: &str, len: u64) -> bool {
        self.files
            .lock()
            .expect("prestage files")
            .get(key)
            .is_some_and(|w| w.len >= len)
    }
    /// Take the files a stage needs (`wanted` is each object's key and prefix length), as
    /// staged files cut to exactly those lengths. Entries taken leave `wanted`; every other
    /// file is deleted, so the directory holds only what the stage will own.
    async fn adopt(
        &self,
        wanted: &mut Vec<(bf_wal::SegmentFile, u64, u64)>,
    ) -> io::Result<HashMap<String, StagedFile>> {
        let mut written = std::mem::take(&mut *self.files.lock().expect("prestage files"));
        let dir = self.dir.clone();
        let mut take = Vec::new();
        wanted.retain(|(source, _, len)| match written.remove(&source.key) {
            Some(w) if w.len >= *len => {
                take.push((source.key.clone(), *len, w));
                false
            }
            Some(w) => {
                drop(w.file);
                let _ = fs::remove_file(dir.join(source_name(&source.key)));
                true
            }
            None => true,
        });
        for key in written.keys() {
            let _ = fs::remove_file(dir.join(source_name(key)));
        }
        tokio::task::spawn_blocking(move || {
            let mut out = HashMap::new();
            for (key, len, w) in take {
                let mut staged = StagedFile {
                    file: w.file,
                    offset: 0,
                    base: 0,
                    len: w.len,
                    crc: w.crc,
                    encoded: w.encoded,
                    identity: Some(w.identity),
                    source: None,
                };
                if staged.len > len {
                    let blocks = len.div_ceil(BLOCK as u64) as usize;
                    // A cut within a packed block must decode the old block before
                    // encoding its shorter replacement, and checksumming stored bytes.
                    let tail = if !len.is_multiple_of(BLOCK as u64) {
                        let start = len / BLOCK as u64 * BLOCK as u64;
                        let raw = staged.read(std::slice::from_ref(&(start..len)))?.remove(0);
                        Some((start, raw))
                    } else {
                        None
                    };
                    staged.crc.truncate(blocks);
                    if let Some((start, raw)) = tail {
                        let offset = staged.physical(&(start..len)).start;
                        let data = if staged.encoded.is_some() {
                            codec::encode(&raw)
                        } else {
                            raw.to_vec()
                        };
                        staged.file.write_all_at(&data, offset)?;
                        *staged.crc.last_mut().expect("a partial block") = crc32fast::hash(&data);
                        if let Some(ends) = &mut staged.encoded {
                            ends[blocks - 1] = (offset + data.len() as u64) as u32;
                        }
                    }
                    if let Some(ends) = &mut staged.encoded {
                        ends.truncate(blocks);
                    }
                    staged.len = len;
                    staged.file.set_len(staged.stored_len())?;
                    staged.file.sync_all()?;
                }
                if staged.stored_len() == staged.len {
                    staged.encoded = None;
                }
                out.insert(key, staged);
            }
            Ok(out)
        })
        .await
        .map_err(io::Error::other)?
    }
}
fn source_name(key: &str) -> String {
    format!("source-{}", digest(key.as_bytes()))
}

impl PrestageWriter {
    /// Upload `bytes` as `key` with `upload` while writing its stage bytes beside it: through
    /// `code_len` (the codes), and through `vector_len` as well when the cap has room left
    /// for the `rows_after` rows of codes still to come. Local trouble never fails the upload.
    pub(crate) async fn upload<E>(
        &self,
        store: &Storage,
        key: &str,
        bytes: Bytes,
        (code_len, vector_len): (u64, u64),
        rows_after: u64,
        upload: impl Future<Output = Result<bf_wal::SegmentFile, E>>,
    ) -> Result<bf_wal::SegmentFile, E> {
        let pre = self.0.clone();
        let (path, name) = (pre.dir.clone(), source_name(key));
        let local = async {
            let pre = pre.clone();
            tokio::task::spawn_blocking(move || {
                let code_len = code_len.min(bytes.len() as u64);
                let vector_len = vector_len.min(bytes.len() as u64);
                let len = {
                    let mut guard = pre.lease.lock().expect("prestage lease");
                    guard.as_mut().and_then(|lease| {
                        pre.admit(lease, &bytes, code_len, vector_len, rows_after)
                    })
                };
                let Some(len) = len else { return Ok(None) };
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path.join(name))?;
                let compress = len <= u64::from(u32::MAX);
                let mut ends = Vec::new();
                let mut crc = Vec::new();
                let mut stored = 0u64;
                for raw in bytes[..len as usize].chunks(BLOCK) {
                    let data = if compress {
                        codec::encode(raw)
                    } else {
                        raw.to_vec()
                    };
                    file.write_all_at(&data, stored)?;
                    crc.push(crc32fast::hash(&data));
                    stored += data.len() as u64;
                    if compress {
                        ends.push(stored as u32);
                    }
                }
                // Release XFS speculative allocation beyond EOF before the final fsync,
                // including while the descriptor remains open for the adopting reader.
                file.set_len(stored)?;
                file.sync_all()?;
                let encoded = (stored < len).then_some(ends);
                Ok::<_, io::Error>(Some((Arc::new(file), crc, encoded, len)))
            })
            .await
            .ok()?
            .map_err(|error| tracing::debug!(%error, "prestage write failed"))
            .ok()?
        };
        let (uploaded, local) = futures::future::join(upload, local).await;
        let file = uploaded?;
        if let Some((handle, crc, encoded, len)) = local
            && let Ok(identity) = store.object_identity(key).await
            && (identity.etag.is_some() || identity.version.is_some())
        {
            pre.files.lock().expect("prestage files").insert(
                key.to_owned(),
                Written {
                    file: handle,
                    len,
                    crc,
                    encoded,
                    identity,
                },
            );
        }
        Ok(file)
    }
}
/// A recluster's handle on the slab bytes it stages locally for the node that serves queries.
/// The bytes are inert until [`Prestage::activate`] follows the manifest commit, and are
/// discarded if the handle is dropped first (a failed commit, an error, or a shutdown).
pub(crate) struct Prestage(Arc<PreInner>);
/// The part of a [`Prestage`] a slab upload task holds.
#[derive(Clone)]
pub(crate) struct PrestageWriter(Arc<PreInner>);
impl Prestage {
    /// A cloneable writer for upload tasks.
    pub(crate) fn writer(&self) -> PrestageWriter {
        PrestageWriter(self.0.clone())
    }
    /// Start staging for a build over vectors of `dims` dimensions, or `None` when this
    /// node has no staging area, or `BF_PRESTAGE=0` switches the feature off.
    pub(crate) fn begin(dims: usize) -> Option<Self> {
        if std::env::var("BF_PRESTAGE").is_ok_and(|v| v == "0") {
            return None;
        }
        let context = read_cache::staging_context()?;
        let manager = context.cache.staging.clone()?;
        if !context.admit {
            return None;
        }
        Self::open(manager, context.cache.key(""), dims)
    }
    fn open(manager: Arc<Manager>, scope: String, dims: usize) -> Option<Self> {
        if manager.stop.stopping() {
            return None;
        }
        let dir = manager
            .path
            .join(format!("prestage-{}", uuid::Uuid::new_v4()));
        fs::DirBuilder::new().mode(0o700).create(&dir).ok()?;
        let lease = manager.empty_lease();
        Some(Self(Arc::new(PreInner {
            scope,
            manager,
            dir,
            files: Mutex::default(),
            lease: Mutex::new(Some(lease)),
            active: AtomicBool::new(false),
            taken: AtomicBool::new(false),
            code_row_bytes: (dims.next_power_of_two() / 8 + 48) as u64,
        })))
    }
    /// Register the finished tier. Its bytes stay inert until [`Prestage::activate`].
    pub(crate) fn publish(&self, set: &SlabSet) {
        let key = stage_key(&self.0.scope, &set.dir.key, false);
        self.0
            .manager
            .state
            .lock()
            .expect("ANN staging lock")
            .prestaged
            .insert(key, self.0.clone());
    }
    /// The tier's manifest commit happened: its first query may adopt the bytes.
    pub(crate) fn activate(self) {
        // The registry holds the bytes from here on; dropping this handle no longer discards.
        self.0.active.store(true, Ordering::Release);
    }
}
impl Drop for Prestage {
    fn drop(&mut self) {
        if self.0.active.load(Ordering::Acquire) {
            return;
        }
        // Not committed: unregister, so no query adopts it, and let the last reference delete it.
        let removed = {
            let mut state = self.0.manager.state.lock().expect("ANN staging lock");
            let key = state
                .prestaged
                .iter()
                .find(|(_, p)| Arc::ptr_eq(p, &self.0))
                .map(|(k, _)| k.clone());
            key.and_then(|k| state.prestaged.remove(&k))
        };
        drop(removed);
    }
}
impl PreInner {
    /// Charge the bytes of one slab object to the lease and say how long a prefix to write.
    fn admit(
        &self,
        lease: &mut Lease,
        bytes: &[u8],
        code_len: u64,
        vector_len: u64,
        rows_after: u64,
    ) -> Option<u64> {
        // Charge actual stored bytes and both per-block CRC and physical-end metadata.
        // Exceptional blocks stay raw, so they cannot overrun the lease's estimate.
        let cost = |len: u64| {
            let stored = if len <= u64::from(u32::MAX) {
                bytes[..len as usize]
                    .chunks(BLOCK)
                    .map(codec::encoded_len)
                    .sum::<usize>() as u64
            } else {
                len
            };
            stored + len.div_ceil(BLOCK as u64) * 22
        };
        let m = &self.manager;
        if !m.extend_files(lease, 1) {
            return None;
        }
        if !m.extend(lease, cost(code_len)) {
            return None;
        }
        let extra = cost(vector_len).saturating_sub(cost(code_len));
        // Keep room for the codes of the slabs still to come, plus the stage's metadata.
        let reserve = rows_after
            .saturating_mul(self.code_row_bytes)
            .saturating_add(64 << 20);
        if vector_len > code_len
            && lease.bytes.saturating_add(extra).saturating_add(reserve) <= m.capacity
            && m.extend(lease, extra)
        {
            return Some(vector_len);
        }
        Some(code_len)
    }
}

pub(crate) fn observe(
    store: &Storage,
    set: &SlabSet,
    dims: usize,
    leaves: u64,
    attribute: &str,
    extras: &dyn Fn() -> Vec<Extra>,
) {
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
    manager.observe(store, &context.cache.key(""), set, dims, leaves, extras);
}
/// The hydration parts of a slab set's covered segments that a stage should hold: each
/// column directory (the tail a reader fetches first) and each ID index.
pub(crate) fn hydration_extras(set: &SlabSet, segments: &[bf_wal::Segment]) -> Vec<Extra> {
    let by_id: HashMap<&str, &bf_wal::Segment> =
        segments.iter().map(|s| (s.id.as_str(), s)).collect();
    let mut extras = Vec::new();
    for cover in &set.covered {
        let Some(segment) = by_id.get(cover.segment.as_str()) else {
            continue;
        };
        if let Some(file) = segment.files.get("docs") {
            // The same range `segment::directory` reads.
            let tail = file.metadata_len.max(65536).min(file.size);
            if tail > 0 {
                extras.push(Extra {
                    file: file.clone(),
                    kind: ExtraKind::Tail(file.size - tail),
                });
            }
        }
        if let Some(file) = segment.files.get("ids")
            && segment.row_count > 0
        {
            extras.push(Extra {
                file: file.clone(),
                kind: ExtraKind::NumericIds(segment.row_count),
            });
        }
    }
    extras
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
    let Some(Ok(aligned)) = file.local(ranges).map(|local| file.aligned(&local)) else {
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
/// Metadata-only coverage check for dispatching local I/O. Reads still verify every CRC,
/// and retain the ordinary object-store fallback if a stage is retired meanwhile.
pub(crate) fn covers(key: &str, range: Range<u64>) -> bool {
    let Some(context) = read_cache::staging_context().filter(|c| c.admit) else {
        return false;
    };
    let Some(stage) = context
        .cache
        .staging
        .as_ref()
        .and_then(|m| m.file(&context.cache.key(key)))
    else {
        return false;
    };
    stage
        .files
        .get(key)
        .is_some_and(|f| f.local(std::slice::from_ref(&range)).is_some())
}
/// The IDs of these rows of an ID index, from a stage's derived row-to-ID map. `None` when no
/// verified map covers them, and the caller reads the object instead.
pub(crate) fn staged_row_ids(ids_key: &str, rows: &roaring::RoaringBitmap) -> Option<Vec<u64>> {
    let ranges: Vec<Range<u64>> = rows
        .iter()
        .map(|row| u64::from(row) * 8..u64::from(row) * 8 + 8)
        .collect();
    let bytes = read(&rowid_key(ids_key), &ranges)?;
    bytes
        .into_iter()
        .map(|b| Some(u64::from_le_bytes(b.as_ref().try_into().ok()?)))
        .collect()
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
    fn encoded_ranges_preserve_odd_offsets_raw_blocks_and_checksum_boundaries() {
        let work = scratch();
        let temporary = tempfile::NamedTempFile::new_in(work.path()).unwrap();
        let mut raw: Vec<u8> = (0..BLOCK * 5 + 123)
            .map(|i| ((i * 37 % 251) as u8) & 0xbf)
            .collect();
        raw[BLOCK + 1] = 0x7c; // A block with an infinity exponent stays raw.
        let mut data = Vec::new();
        let mut ends = Vec::new();
        let mut crc = Vec::new();
        for block in raw.chunks(BLOCK) {
            let encoded = codec::encode(block);
            crc.push(crc32fast::hash(&encoded));
            data.extend(encoded);
            ends.push(data.len() as u32);
        }
        codec::validate(&ends, raw.len() as u64).unwrap();
        temporary.as_file().write_all_at(&data, 0).unwrap();
        let staged = StagedFile {
            file: Arc::new(temporary.as_file().try_clone().unwrap()),
            offset: 0,
            base: 71,
            len: raw.len() as u64,
            crc,
            encoded: Some(ends),
            identity: None,
            source: None,
        };
        assert!(staged.stored_len() < staged.len);
        for offset in [0, 1, BLOCK - 1, BLOCK, BLOCK * 2 - 3, BLOCK * 5 + 122] {
            for size in [0, 1, 2, 2048, BLOCK * 2] {
                let end = (offset + size).min(raw.len());
                let ranges = [71 + offset as u64..71 + end as u64, 71..73];
                let got = staged.read(&ranges).unwrap();
                assert_eq!(&got[0][..], &raw[offset..end]);
                assert_eq!(&got[1][..], &raw[..2]);
            }
        }
        // Corruption outside the requested subrange still invalidates the entire block.
        let last = staged.encoded.as_ref().unwrap()[0] as u64 - 1;
        staged
            .file
            .write_all_at(&[data[last as usize] ^ 1], last)
            .unwrap();
        assert!(staged.read(std::slice::from_ref(&(71..72))).is_err());
        staged.file.set_len(staged.stored_len() - 1).unwrap();
        assert!(
            staged
                .read(std::slice::from_ref(
                    &(71 + staged.len - 1..71 + staged.len)
                ))
                .is_err()
        );
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
            file: Arc::new(temporary.as_file().try_clone().unwrap()),
            offset: 0,
            base: 0,
            len: data.len() as u64,
            crc: data.chunks(BLOCK).map(crc32fast::hash).collect(),
            encoded: None,
            identity: None,
            source: None,
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
        fixture_dtype(salt, bf_vector::VectorType::F32).await
    }

    async fn fixture_dtype(
        salt: u32,
        dtype: bf_vector::VectorType,
    ) -> (Storage, SlabSet, Vec<u8>, u64) {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let builder = IndexBuilder::new(32, Metric::EuclideanSquared)
            .unwrap()
            .vector_type(dtype)
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

    async fn publish(store: &Storage, set: &SlabSet, deleted: bool, generation: u64) {
        let manifest = bf_wal::Manifest {
            format_version: bf_wal::MANIFEST_FORMAT_VERSION,
            schema: Default::default(),
            distance_metric: Default::default(),
            id_type: "uint".parse().unwrap(),
            created_at: chrono::Utc::now(),
            indexed_through: 0,
            indexer_epoch: 0,
            deleted,
            segments: vec![],
            stats: Default::default(),
            text_stats: None,
            ann_dirs: Default::default(),
            ann_tiers: if deleted {
                BTreeMap::new()
            } else {
                BTreeMap::from([("vector".into(), vec![set.clone()])])
            },
            lineage: vec![],
            updated_at: None,
            retired: vec![],
            checkpoints: vec![],
            pruned_through: 0,
            bulk_load_at: None,
            root: false,
        };
        bf_wal::Wal::new(store.clone(), "test")
            .unwrap()
            .publish_manifest(generation, &manifest)
            .await
            .unwrap();
    }

    async fn completed(work: &Path, store: &Storage, set: &SlabSet) -> (PathBuf, u64) {
        publish(store, set, false, 1).await;
        let manager = Manager::open(work, Some(1 << 20), Arc::default()).unwrap();
        manager.observe(store, "", set, 32, 16, &Vec::new);
        let stage = ready(&manager, "", set).await;
        let path = stage.path.clone();
        let bytes = manager.state.lock().unwrap().used;
        drop(stage);
        drop(manager);
        assert!(path.join("complete").exists());
        (path, bytes)
    }

    #[tokio::test]
    async fn f16_stages_charge_physical_bytes_and_adopt_encoded_vectors() {
        let work = scratch();
        let (store, set, ann, codes) = fixture_dtype(31, bf_vector::VectorType::F16).await;
        let (path, charged) = completed(work.path(), &store, &set).await;
        let record = read_record(&path).unwrap();
        let source = &set.slabs[0].files["annv2:vector"];
        let file = &record.files[&source.key];
        assert!(file.encoded.is_some());
        assert!(file.stored_len() < file.len);
        assert_eq!(
            charged,
            record
                .files
                .values()
                .map(FileRecord::stored_len)
                .sum::<u64>()
                + fs::metadata(path.join("complete")).unwrap().len()
        );
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let stats = Arc::new(bf_storage::BackendStats::default());
        stats.scope("adopt", manager.recover(&store)).await;
        assert_eq!(manager.state.lock().unwrap().used, charged);
        assert!(stats.snapshot().iter().all(|m| m.operation != "get_range"));
        let ranges = [codes - 1..codes + 65, file.len - 64..file.len];
        let got = manager.read(&source.key, &source.key, &ranges).unwrap();
        for (got, range) in got.iter().zip(&ranges) {
            assert_eq!(&got[..], &ann[range.start as usize..range.end as usize]);
        }
    }

    #[tokio::test]
    async fn checkpoint_metadata_is_saved_and_refreshed_during_restart_adoption() {
        let work = scratch();
        let (store, set, _, _) = fixture(13).await;
        publish(&store, &set, false, 1).await;
        let wal = bf_wal::Wal::new(store.clone(), "test").unwrap();
        let (_, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
        manifest.indexed_through = 7;
        wal.publish_manifest(2, &manifest).await.unwrap();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let checkpoint = Checkpoint {
            generation: 2,
            indexed_through: 7,
        };
        manager.note_checkpoint("", "test", Some(checkpoint));
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
        let stage = ready(&manager, "", &set).await;
        assert_eq!(
            read_record(&stage.path).unwrap().checkpoint,
            Some(checkpoint)
        );
        assert_eq!(manager.checkpoint("", "test"), Some(checkpoint));
        drop(stage);
        drop(manager);
        // A maintenance generation can keep exactly the same tier and WAL checkpoint.
        wal.publish_manifest(3, &manifest).await.unwrap();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        assert_eq!(
            manager.checkpoint("", "test"),
            Some(Checkpoint {
                generation: 3,
                ..checkpoint
            })
        );
    }

    #[tokio::test]
    async fn restart_adopts_without_payload_gets_and_keeps_capacity_stable() {
        let work = scratch();
        let (store, set, ann, end) = fixture(11).await;
        let end = end + 128; // Include exact-vector bytes in every restart check.
        let (path, bytes) = completed(work.path(), &store, &set).await;
        for _ in 0..3 {
            let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
            let stats = Arc::new(bf_storage::BackendStats::default());
            let recovery = manager.recover(&store);
            assert!(
                std::mem::size_of_val(&recovery) <= 32,
                "startup recovery must not enlarge every query's stack"
            );
            stats.scope("adopt", recovery).await;
            assert_eq!(manager.state.lock().unwrap().used, bytes);
            assert_eq!(manager.state.lock().unwrap().ready.len(), 1);
            let key = &set.slabs[0].files["annv2:vector"].key;
            let data = manager
                .read(key, key, std::slice::from_ref(&(0..end)))
                .unwrap();
            assert_eq!(&data[0][..], &ann[..end as usize]);
            assert!(
                stats.snapshot().iter().all(|m| m.operation != "get_range"),
                "adoption reads only the manifest and HEAD metadata"
            );
            assert!(bytes <= manager.capacity);
            drop(manager);
            assert!(path.exists());
        }
    }

    #[tokio::test]
    async fn adoption_preserves_tenant_scope_and_removes_retired_tiers() {
        let work = scratch();
        let (store, set, ann, end) = fixture(17).await;
        let tenant = store.scoped("tenant");
        for file in std::iter::once(&set.dir).chain(set.slabs.iter().flat_map(|s| s.files.values()))
        {
            if tenant.head(&file.key).await.is_err() {
                tenant
                    .create_unique(&file.key, store.get(&file.key).await.unwrap())
                    .await
                    .unwrap();
            }
        }
        publish(&tenant, &set, false, 1).await;
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.observe(&tenant, "tenant\0", &set, 32, 16, &Vec::new);
        let stage = ready(&manager, "tenant\0", &set).await;
        let path = stage.path.clone();
        drop(stage);
        drop(manager);
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        let key = &set.slabs[0].files["annv2:vector"].key;
        assert_eq!(
            &manager
                .read(
                    &format!("tenant\0{key}"),
                    key,
                    std::slice::from_ref(&(0..end))
                )
                .unwrap()[0][..],
            &ann[..end as usize]
        );
        assert!(
            manager
                .read(key, key, std::slice::from_ref(&(0..37)))
                .is_none()
        );
        manager.retain_namespace("other\0", "test", &BTreeMap::new());
        assert!(path.exists());
        manager.retain_namespace("tenant\0", "test", &BTreeMap::new());
        assert!(!path.exists());
        assert_eq!(manager.state.lock().unwrap().used, 0);
    }

    #[tokio::test]
    async fn corrupt_payload_after_restart_is_never_returned() {
        let work = scratch();
        let (store, set, ann, _) = fixture(12).await;
        let (path, _) = completed(work.path(), &store, &set).await;
        let key = &set.slabs[0].files["annv2:vector"].key;
        let record = read_record(&path).unwrap();
        OpenOptions::new()
            .write(true)
            .open(path.join(&record.files[key].name))
            .unwrap()
            .write_all_at(&[ann[0] ^ 0xff], 0)
            .unwrap();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        assert!(
            manager
                .read(key, key, std::slice::from_ref(&(0..37)))
                .is_none()
        );
        drop(manager);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn same_length_wrong_object_is_rejected_by_etag() {
        let work = scratch();
        let (store, set, mut ann, _) = fixture(13).await;
        let (path, _) = completed(work.path(), &store, &set).await;
        let key = &set.slabs[0].files["annv2:vector"].key;
        ann[40] ^= 0xff;
        // Preserve both the immutable key and its manifest size to exercise the ETag check.
        store.delete(key).await.unwrap();
        store.create_unique(key, ann).await.unwrap();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        assert!(manager.state.lock().unwrap().ready.is_empty());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn partial_files_and_damaged_completion_records_are_rejected() {
        for damage_record in [false, true] {
            let work = scratch();
            let (store, set, _, _) = fixture(14).await;
            let (path, _) = completed(work.path(), &store, &set).await;
            let record = read_record(&path).unwrap();
            let target = if damage_record {
                path.join("complete")
            } else {
                path.join(&record.files.values().next().unwrap().name)
            };
            OpenOptions::new()
                .write(true)
                .open(target)
                .unwrap()
                .set_len(31)
                .unwrap();
            let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
            manager.recover(&store).await;
            assert!(manager.state.lock().unwrap().ready.is_empty());
            assert!(!path.exists());
        }
    }

    #[tokio::test]
    async fn crash_before_final_directory_publication_leaves_nothing_adoptable() {
        let work = scratch();
        let (store, set, _, _) = fixture(19).await;
        let (path, _) = completed(work.path(), &store, &set).await;
        let partial = path.parent().unwrap().join("interrupted-publication");
        fs::rename(path, &partial).unwrap();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        assert!(manager.state.lock().unwrap().ready.is_empty());
        assert!(!partial.exists());
    }

    #[tokio::test]
    async fn restart_removes_deleted_or_unreferenced_namespace_stages() {
        let work = scratch();
        let (store, set, _, _) = fixture(15).await;
        let (path, _) = completed(work.path(), &store, &set).await;
        publish(&store, &set, true, 2).await;
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        assert!(manager.state.lock().unwrap().ready.is_empty());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn wal_tombstone_removes_stage_before_a_deleted_manifest_is_published() {
        let work = scratch();
        let (store, set, _, _) = fixture(20).await;
        let (path, _) = completed(work.path(), &store, &set).await;
        let mut entry = bf_wal::WalEntry::new(vec![]);
        entry.tombstone = true;
        bf_wal::Wal::new(store.clone(), "test")
            .unwrap()
            .append(1, &entry)
            .await
            .unwrap();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        assert!(manager.state.lock().unwrap().ready.is_empty());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn namespace_cleanup_waits_for_readers_and_smaller_caps_remove_stages() {
        let work = scratch();
        let (store, set, _, _) = fixture(16).await;
        let (path, bytes) = completed(work.path(), &store, &set).await;
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        let stage = ready(&manager, "", &set).await;
        manager.remove_namespace("", "test");
        assert!(path.exists());
        assert!(manager.state.lock().unwrap().files.is_empty());
        drop(stage);
        assert!(!path.exists());
        assert_eq!(manager.state.lock().unwrap().used, 0);
        drop(manager);
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
        let stage = ready(&manager, "", &set).await;
        let path = stage.path.clone();
        drop(stage);
        drop(manager);
        let manager = Manager::open(work.path(), Some(bytes - 1), Arc::default()).unwrap();
        assert!(!path.exists());
        assert!(manager.candidates.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn local_io_coverage_rejects_partial_retired_and_differently_scoped_files() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(128 << 10), Arc::default()).unwrap();
        let (store, set, ann, end) = fixture(1).await;
        manager.observe(&store, "left\0", &set, 32, 16, &Vec::new);
        let stage = ready(&manager, "left\0", &set).await;
        let key = &set.slabs[0].files["annv2:vector"].key;
        let mut cache = read_cache::DecodedCache::new(1 << 20, 1).scoped("left");
        cache.staging = Some(manager.clone());
        let context = read_cache::ReadContext::new(Arc::new(cache));
        let stats = Arc::new(bf_storage::BackendStats::default());
        stats
            .scope(
                "coverage",
                read_cache::scope(context.clone(), async {
                    assert!(covers(key, 0..end));
                    assert!(
                        !covers(key, 0..ann.len() as u64),
                        "code prefixes cannot enable local vector dispatch"
                    );
                    assert!(!covers("missing", 0..1));
                }),
            )
            .await;
        assert!(
            stats.snapshot().is_empty(),
            "dispatch planning must issue no I/O"
        );
        let mut other = read_cache::DecodedCache::new(1 << 20, 1).scoped("right");
        other.staging = Some(manager);
        read_cache::scope(read_cache::ReadContext::new(Arc::new(other)), async {
            assert!(!covers(key, 0..end));
        })
        .await;
        stage.bad.store(true, Ordering::Relaxed);
        read_cache::scope(context, async {
            assert!(!covers(key, 0..end));
        })
        .await;
    }

    #[tokio::test]
    async fn verified_prefixes_preserve_ranges_and_corruption_falls_back_to_storage() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(128 << 10), Arc::default()).unwrap();
        let (store, set, ann, end) = fixture(1).await;
        manager.observe(&store, "left\0", &set, 32, 16, &Vec::new);
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
    async fn pooled_downloads_preserve_piece_boundaries_and_every_checksum() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(64 << 20), Arc::default()).unwrap();
        let (store, _, _, _) = fixture(1).await;
        let mut sources = Vec::new();
        for n in 0..3 {
            let data: Vec<u8> = (0..PIECE as usize + BLOCK + 37)
                .map(|i| ((i * 31 + n) % 251) as u8)
                .collect();
            let key = format!("pool-{n}");
            store.create_unique(&key, data.clone()).await.unwrap();
            sources.push((file(&key, data.len() as u64, 0), 0, data.len() as u64));
        }
        let files = manager
            .download_many(&store, &sources, work.path(), false)
            .await
            .unwrap();
        for (n, (source, _, len)) in sources.iter().enumerate() {
            let file = &files[&source.key];
            let ranges = [0..37, PIECE - 17..PIECE + 29, len - 37..*len];
            for (bytes, range) in file.read(&ranges).unwrap().iter().zip(ranges) {
                let expected: Vec<u8> = (range.start..range.end)
                    .map(|i| ((i * 31 + n as u64) % 251) as u8)
                    .collect();
                assert_eq!(&bytes[..], expected);
            }
            assert_eq!(file.crc.len(), len.div_ceil(BLOCK as u64) as usize);
            assert_eq!(
                file.identity,
                Some(store.object_identity(&source.key).await.unwrap())
            );
        }
    }

    #[tokio::test]
    async fn a_staged_tail_serves_object_offsets_and_nothing_before_them() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(64 << 20), Arc::default()).unwrap();
        let (store, _, _, _) = fixture(1).await;
        let data: Vec<u8> = (0..PIECE as usize + BLOCK * 3 + 41)
            .map(|i| (i * 13 % 251) as u8)
            .collect();
        store
            .create_unique("tail-source", data.clone())
            .await
            .unwrap();
        let base = data.len() as u64 - (BLOCK as u64 * 2 + 41);
        let sources = [(
            file("tail-source", data.len() as u64, 0),
            base,
            data.len() as u64 - base,
        )];
        let files = manager
            .download_many(&store, &sources, work.path(), false)
            .await
            .unwrap();
        let tail = &files["tail-source"];
        let end = data.len() as u64;
        let ranges = [base..base + 5, end - 9..end, base + 100..base + 100];
        let got = tail.read(&ranges).unwrap();
        for (bytes, range) in got.iter().zip(&ranges) {
            assert_eq!(&bytes[..], &data[range.start as usize..range.end as usize]);
        }
        assert!(
            tail.local(std::slice::from_ref(&(base - 1..base + 4)))
                .is_none()
        );
        assert!(
            tail.local(std::slice::from_ref(&(end - 1..end + 1)))
                .is_none()
        );
        assert!(tail.read(std::slice::from_ref(&(0..8))).is_err());
    }

    #[tokio::test]
    async fn derived_row_ids_answer_from_the_stage_and_skip_string_ids() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(64 << 20), Arc::default()).unwrap();
        let (store, set, _, _) = fixture(1).await;
        let numeric: Vec<bf_types::Id> = (0..5000u64)
            .map(|n| bf_types::Id::Uint(n * 7 + 3))
            .collect();
        let text: Vec<bf_types::Id> = (0..10u32)
            .map(|n| bf_types::Id::String(format!("id-{n}")))
            .collect();
        let a = crate::segment::put_ids(&store, "ids-a".into(), numeric.clone())
            .await
            .unwrap();
        let b = crate::segment::put_ids(&store, "ids-b".into(), text)
            .await
            .unwrap();
        let maps = [
            Extra {
                file: a.clone(),
                kind: ExtraKind::NumericIds(5000),
            },
            Extra {
                file: b,
                kind: ExtraKind::NumericIds(10),
            },
        ];
        let mut lease = manager.empty_lease();
        assert!(manager.extend_files(&mut lease, 1) && manager.extend(&mut lease, 1 << 20));
        let stage = manager
            .build_companions(&store, "", &set, lease, maps.to_vec())
            .await
            .unwrap()
            .unwrap();
        let files = &stage.files;
        assert_eq!(files.len(), 1);
        let staged = &files[&rowid_key("ids-a")];
        assert_eq!(staged.source.as_deref(), Some("ids-a"));
        assert_eq!(staged.len, 5000 * 8);
        let rows = [0u64, 1, 2047, 2048, 4999];
        let ranges: Vec<_> = rows.iter().map(|r| r * 8..r * 8 + 8).collect();
        for (bytes, row) in staged.read(&ranges).unwrap().iter().zip(rows) {
            let want = match &numeric[row as usize] {
                bf_types::Id::Uint(n) => *n,
                _ => unreachable!(),
            };
            assert_eq!(u64::from_le_bytes(bytes.as_ref().try_into().unwrap()), want);
        }
        assert!(
            staged
                .read(std::slice::from_ref(&(5000 * 8..5000 * 8 + 8)))
                .is_err()
        );
    }

    #[tokio::test]
    async fn named_id_and_default_hydration_read_only_staged_ids() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(64 << 20), Arc::default()).unwrap();
        let (store, set, _, _) = fixture(46).await;
        let ids = crate::segment::put_ids(
            &store,
            "numeric-ids".into(),
            (0..2048).map(|n| bf_types::Id::Uint(n * 7 + 3)).collect(),
        )
        .await
        .unwrap();
        let extras = vec![Extra {
            file: ids.clone(),
            kind: ExtraKind::NumericIds(2048),
        }];
        manager.observe(&store, "", &set, 32, 16, &|| extras.clone());
        let _main = ready(&manager, "", &set).await;
        let _pack = companion_stage(&manager, &set).await;
        let mut segment = set.slabs[0].clone();
        // No docs object exists: any attempt to hydrate document bytes would fail.
        segment.files = BTreeMap::from([("ids".into(), ids)]);
        let mut cache = read_cache::DecodedCache::new(1 << 20, 1);
        cache.staging = Some(manager);
        let context = read_cache::ReadContext::new(Arc::new(cache));
        let rows = roaring::RoaringBitmap::from_iter([0, 1, 2047]);
        let stats = Arc::new(bf_storage::BackendStats::default());
        let (default, named) = stats
            .scope(
                "hydrate",
                read_cache::scope(context, async {
                    let default =
                        crate::segment::hydrated_rows(&store, &segment, &rows, &Default::default())
                            .await
                            .unwrap();
                    let named = crate::segment::hydrated_rows(
                        &store,
                        &segment,
                        &rows,
                        &std::collections::BTreeSet::from(["id".into()]),
                    )
                    .await
                    .unwrap();
                    (default, named)
                }),
            )
            .await;
        assert_eq!(default, named);
        assert_eq!(named[&2047]["id"], serde_json::json!(2047 * 7 + 3));
        assert!(
            stats.snapshot().is_empty(),
            "ID hydration makes no backend requests"
        );
    }

    async fn companion_stage(manager: &Arc<Manager>, set: &SlabSet) -> Arc<Stage> {
        let key = stage_key("", &set.dir.key, true);
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
    async fn companions_pack_into_one_file_after_the_code_stage_and_survive_restart() {
        let work = scratch();
        let (store, set, _, _) = fixture(31).await;
        publish(&store, &set, false, 1).await;
        let numeric: Vec<bf_types::Id> = (0..5000u64).map(bf_types::Id::Uint).collect();
        let a = crate::segment::put_ids(&store, "ids-a".into(), numeric)
            .await
            .unwrap();
        let tail = a.size - 100;
        let extras = vec![
            Extra {
                file: a.clone(),
                kind: ExtraKind::Tail(tail),
            },
            Extra {
                file: a.clone(),
                kind: ExtraKind::NumericIds(5000),
            },
        ];
        let manager = Manager::open(work.path(), Some(64 << 20), Arc::default()).unwrap();
        manager.observe(&store, "", &set, 32, 16, &|| extras.clone());
        let main = ready(&manager, "", &set).await;
        let pack = companion_stage(&manager, &set).await;
        assert_eq!(pack.files.len(), 2);
        let (docs, map) = (&pack.files["ids-a"], &pack.files["ids-a#rowid"]);
        assert!(Arc::ptr_eq(&docs.file, &map.file));
        assert_ne!(docs.offset, map.offset);
        assert_eq!(
            manager.state.lock().unwrap().files_used,
            main.files.len() + 1
        );
        let raw = store.get("ids-a").await.unwrap();
        let want = raw[tail as usize + 10..tail as usize + 20].to_vec();
        let range = tail + 10..tail + 20;
        let got = manager.read("stage:ids-a", "ids-a", std::slice::from_ref(&range));
        // The scoped lookup key is the scope followed by the object name.
        assert!(got.is_none());
        let got = manager
            .read("ids-a", "ids-a", std::slice::from_ref(&range))
            .unwrap();
        assert_eq!(&got[0][..], &want[..]);
        let row = manager
            .read(
                "ids-a#rowid",
                "ids-a#rowid",
                std::slice::from_ref(&(8 * 7..8 * 8)),
            )
            .unwrap();
        assert_eq!(u64::from_le_bytes(row[0].as_ref().try_into().unwrap()), 7);
        // Everything is settled: another observation starts nothing.
        manager.observe(&store, "", &set, 32, 16, &|| extras.clone());
        assert!(manager.state.lock().unwrap().pending.is_empty());
        drop((main, pack, got, row));
        drop(manager);

        // A restart adopts both stages and then finds nothing left to stage.
        let manager = Manager::open(work.path(), Some(64 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        assert!(
            manager
                .read("ids-a#rowid", "ids-a#rowid", std::slice::from_ref(&(0..8)))
                .is_some()
        );
        manager.observe(&store, "", &set, 32, 16, &|| extras.clone());
        assert!(manager.state.lock().unwrap().pending.is_empty());
        assert!(
            manager
                .read("ids-a", "ids-a", std::slice::from_ref(&range))
                .is_some()
        );
        drop(manager);

        // Losing only the pack leaves the code stage adoptable, and the pack is rebuilt.
        for entry in fs::read_dir(work.path().join("ann-staging-v3/test")).unwrap() {
            let dir = entry.unwrap().path();
            if dir.join(PACK).exists() {
                fs::remove_dir_all(dir).unwrap();
            }
        }
        let manager = Manager::open(work.path(), Some(64 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        assert!(
            manager
                .read("ids-a#rowid", "ids-a#rowid", std::slice::from_ref(&(0..8)))
                .is_none()
        );
        manager.observe(&store, "", &set, 32, 16, &|| extras.clone());
        companion_stage(&manager, &set).await;
        assert!(
            manager
                .read("ids-a#rowid", "ids-a#rowid", std::slice::from_ref(&(0..8)))
                .is_some()
        );
    }

    #[tokio::test]
    async fn exact_vectors_use_verified_sparse_reads_and_corruption_falls_back() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let (store, set, ann, codes_end) = fixture(22).await;
        manager.observe(&store, "left\0", &set, 32, 16, &Vec::new);
        let stage = ready(&manager, "left\0", &set).await;
        let source = &set.slabs[0].files["annv2:vector"];
        let dir_raw = store.get(&set.dir.key).await.unwrap();
        let dir = bf_vector::upgrade::directory(&dir_raw).unwrap();
        let vector_end = dir
            .entries()
            .iter()
            .map(|e| e.vectors_range().end)
            .max()
            .unwrap();
        assert!(vector_end > codes_end);
        assert_eq!(stage.files[&source.key].len, vector_end);
        let ranges = vec![codes_end..codes_end + 127, vector_end - 64..vector_end];
        let mut cache = read_cache::DecodedCache::new(1 << 20, 1).scoped("left");
        cache.staging = Some(manager.clone());
        let context = read_cache::ReadContext::new(Arc::new(cache));
        let stats = Arc::new(bf_storage::BackendStats::default());
        let actual = stats
            .scope(
                "exact-local",
                read_cache::scope(
                    context.clone(),
                    crate::segment::ObjectReader::new(&store, source)
                        .read_batch_sparse(ranges.clone()),
                ),
            )
            .await
            .unwrap();
        for (data, range) in actual.iter().zip(&ranges) {
            assert_eq!(&data[..], &ann[range.start as usize..range.end as usize]);
        }
        assert!(stats.snapshot().iter().all(|m| m.operation != "get_range"));
        stage.files[&source.key]
            .file
            .write_all_at(&[ann[codes_end as usize] ^ 0xff], codes_end)
            .unwrap();
        let actual = read_cache::scope(
            context,
            crate::segment::ObjectReader::new(&store, source).read_batch_sparse(ranges.clone()),
        )
        .await
        .unwrap();
        assert_eq!(
            &actual[0][..],
            &ann[codes_end as usize..codes_end as usize + 127]
        );
        assert!(stage.bad.load(Ordering::Relaxed));
        assert!(manager.state.lock().unwrap().used <= manager.capacity);
    }

    #[tokio::test]
    async fn capacity_keeps_all_codes_and_adopts_only_fitting_exact_objects() {
        let work = scratch();
        let (store, mut set, ann, codes_end) = fixture(23).await;
        let first = set.slabs[0].files["annv2:vector"].clone();
        let second_key = "ns/test/slabs/s2/ann.bfa";
        store.create_unique(second_key, ann.clone()).await.unwrap();
        let mut second = set.slabs[0].clone();
        second.id = "s2".into();
        second.files.get_mut("annv2:vector").unwrap().key = second_key.into();
        let raw = Directory::build(&[
            (
                "s".into(),
                first.key.clone(),
                first.key.clone(),
                Fragment::decode(&ann[..first.metadata_len as usize]).unwrap(),
            ),
            (
                "s2".into(),
                second_key.into(),
                second_key.into(),
                Fragment::decode(&ann[..first.metadata_len as usize]).unwrap(),
            ),
        ])
        .unwrap()
        .unwrap();
        let dir = Directory::decode(&raw).unwrap();
        let vectors_end = dir
            .entries()
            .iter()
            .map(|e| e.vectors_range().end)
            .max()
            .unwrap();
        set.dir = file(
            "ns/test/segments/pair-directory",
            raw.len() as u64,
            raw.len() as u64,
        );
        store.create_unique(&set.dir.key, raw).await.unwrap();
        set.slabs.push(second);
        let mut cover = set.covered[0].clone();
        cover.segment = "docs2".into();
        set.covered.push(cover);
        let estimate = set.rows() * 52 + 32 * 256 + set.dir.size;
        let delta = vectors_end - codes_end;
        let capacity = estimate + delta + delta.div_ceil(BLOCK as u64) * 11 + 512;
        publish(&store, &set, false, 1).await;
        for restarting in [false, true] {
            let manager = Manager::open(work.path(), Some(capacity), Arc::default()).unwrap();
            if restarting {
                manager.recover(&store).await;
            } else {
                manager.observe(&store, "", &set, 32, 32, &Vec::new);
            }
            let stage = ready(&manager, "", &set).await;
            let mut exact = 0;
            for key in [&first.key[..], second_key] {
                let code = manager
                    .read(key, key, std::slice::from_ref(&(0..codes_end)))
                    .unwrap();
                assert_eq!(&code[0][..], &ann[..codes_end as usize]);
                if let Some(data) = manager.read(
                    key,
                    key,
                    std::slice::from_ref(&(vectors_end - 64..vectors_end)),
                ) {
                    exact += 1;
                    assert_eq!(
                        &data[0][..],
                        &ann[vectors_end as usize - 64..vectors_end as usize]
                    );
                }
            }
            assert_eq!(
                exact, 1,
                "one complete exact object fits beside both code prefixes"
            );
            assert!(manager.state.lock().unwrap().used <= capacity);
            drop(stage);
            drop(manager);
        }
    }

    #[tokio::test]
    async fn optional_vectors_do_not_evict_existing_code_leases() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(256 << 10), Arc::default()).unwrap();
        let mut first = manager.reserve(128 << 10, 1).unwrap();
        let second = manager.reserve(64 << 10, 1).unwrap();
        assert!(!manager.extend(&mut first, 128 << 10));
        assert_eq!(first.bytes, 128 << 10);
        assert_eq!(manager.state.lock().unwrap().used, 192 << 10);
        drop(second);
        assert!(manager.extend(&mut first, 64 << 10));
        assert_eq!(manager.state.lock().unwrap().used, 192 << 10);
        drop(first);
        assert_eq!(manager.state.lock().unwrap().used, 0);
    }

    #[tokio::test]
    async fn deletion_during_preparation_cannot_publish_a_stage() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let (store, set, _, _) = fixture(18).await;
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
        manager.remove_namespace("", "test");
        tokio::time::timeout(Duration::from_secs(10), async {
            while !manager.state.lock().unwrap().pending.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let state = manager.state.lock().unwrap();
        assert!(state.ready.is_empty());
        assert_eq!(state.used, 0);
    }

    #[tokio::test]
    async fn generations_and_storage_scopes_never_alias() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let (left, a, ann_a, end_a) = fixture(1).await;
        let (right, b, ann_b, end_b) = fixture(2).await;
        manager.observe(&left, "left\0", &a, 32, 16, &Vec::new);
        let _a = ready(&manager, "left\0", &a).await;
        manager.observe(&right, "right\0", &b, 32, 16, &Vec::new);
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
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
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
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
        assert!(manager.state.lock().unwrap().pending.is_empty());
    }

    #[tokio::test]
    async fn eviction_removes_lookup_before_releasing_an_active_reader() {
        let work = scratch();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let (store, set, _, _) = fixture(7).await;
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
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
    fn backend_lock_is_exclusive_and_partial_stages_are_removed() {
        let work = scratch();
        let first = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        assert!(Manager::open(work.path(), Some(1 << 20), Arc::default()).is_err());
        let partial = first.path.join("interrupted");
        fs::create_dir(&partial).unwrap();
        fs::write(partial.join("directory"), b"unfinished").unwrap();
        drop(first);
        let second = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        assert!(!partial.exists());
        assert!(second.path.exists());
    }

    #[test]
    fn backend_changes_remove_stopped_cache_areas_and_preserve_live_ones() {
        let work = scratch();
        let left = Manager::open_named(work.path(), "left", Some(1 << 20), Arc::default()).unwrap();
        let left_path = left.path.clone();
        fs::write(left.path.join("retained-data"), b"retained").unwrap();
        let right =
            Manager::open_named(work.path(), "right", Some(1 << 20), Arc::default()).unwrap();
        assert!(left_path.exists(), "a live backend keeps its area");
        drop(left);
        drop(right);
        let right =
            Manager::open_named(work.path(), "right", Some(1 << 20), Arc::default()).unwrap();
        assert!(
            !left_path.exists(),
            "a stopped previous backend cannot accumulate another allowance"
        );
        assert!(right.path.exists());
    }

    #[test]
    fn upgrades_remove_orphaned_legacy_stages_and_keep_live_owners() {
        let work = scratch();
        for (version, prefix) in [("v1", "owner-"), ("v2", "backend-")] {
            let base = work.path().join(format!("ann-staging-{version}"));
            let live = base.join(format!("{prefix}live"));
            let dead = base.join(format!("{prefix}dead"));
            fs::create_dir_all(&live).unwrap();
            fs::create_dir_all(&dead).unwrap();
            let guard = File::create(live.join(".lock")).unwrap();
            guard.lock().unwrap();
            File::create(dead.join(".lock")).unwrap();
            let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
            assert!(live.exists());
            assert!(!dead.exists());
            drop(manager);
            drop(guard);
            sweep_legacy(work.path()).unwrap();
            assert!(!live.exists());
        }
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
                // Force final reranks across multiple streamed batches and slab sources.
                ann_rerank_factor: 1,
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
        let request: bf_types::QueryRequest = serde_json::from_value(serde_json::json!({"rank_by":["vector","ANN",vector(17,5)],"top_k":2500,"include_attributes":["id"]})).unwrap();
        let configured = engine.query_configured("test", request.clone());
        assert!(
            std::mem::size_of_val(&configured) <= 32,
            "nested recall queries must keep a bounded future"
        );
        drop(configured);
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
            .query_with_diagnostics("test", request.clone())
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
        let mut default_request = request;
        default_request.include_attributes = None;
        engine.drop_caches().await;
        let (default, _) = engine
            .query_with_diagnostics("test", default_request.clone())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&after.result.rows).unwrap(),
            serde_json::to_value(&default.result.rows).unwrap()
        );
        let warm = engine.query("test", default_request).await.unwrap();
        assert_eq!(
            serde_json::to_value(&default.result.rows).unwrap(),
            serde_json::to_value(&warm.result.rows).unwrap()
        );
        assert!(diagnostics.cache.disk_bytes > 0);
        assert_eq!(after.performance.cache_temperature, "warm");
        assert_eq!(
            after.performance.cache_hit_ratio,
            diagnostics.cache.cache_hit_ratio()
        );
        let stage_path = manager
            .state
            .lock()
            .unwrap()
            .ready
            .values()
            .next()
            .unwrap()
            .1
            .path
            .clone();
        let peer = crate::Engine::with_config(
            engine.0.store.clone(),
            crate::EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                decoded_cache_bytes: 16 << 20,
                ..Default::default()
            },
        );
        peer.delete_namespace("test").await.unwrap();
        assert!(
            engine
                .query(
                    "test",
                    serde_json::from_value(serde_json::json!({
                        "rank_by":["id","asc"], "top_k":1
                    }))
                    .unwrap()
                )
                .await
                .is_err()
        );
        assert!(manager.state.lock().unwrap().ready.is_empty());
        assert!(
            !stage_path.exists(),
            "a remote WAL tombstone also removes local stages"
        );
        assert!(peer.shutdown(Duration::from_secs(5)).await);
        assert!(engine.shutdown(Duration::from_secs(5)).await);
    }

    /// Upload-time staging of one fixture slab, as a recluster does it. The object already
    /// sits in the store, so the "upload" is the file it was given.
    async fn prestaged(
        manager: &Arc<Manager>,
        store: &Storage,
        set: &SlabSet,
        ann: &[u8],
        through_vectors: bool,
    ) -> Prestage {
        let file = set.slabs[0].files["annv2:vector"].clone();
        let lens = crate::recluster::stage_lengths(ann, file.metadata_len);
        assert!(lens.0 > 0 && lens.1 > lens.0, "{lens:?}");
        let pre = Prestage::open(manager.clone(), String::new(), 32).unwrap();
        let wanted = if through_vectors {
            lens
        } else {
            (lens.0, lens.0)
        };
        let done = file.clone();
        let got = pre
            .writer()
            .upload(
                store,
                &file.key,
                Bytes::from(ann.to_vec()),
                wanted,
                0,
                async move { Ok::<_, ()>(done) },
            )
            .await
            .unwrap();
        assert_eq!(got.key, file.key);
        pre
    }

    #[tokio::test]
    async fn prestaged_bytes_wait_for_the_commit_then_become_the_stage_without_a_download() {
        let (store, set, ann, _) = fixture_dtype(41, bf_vector::VectorType::F16).await;
        publish(&store, &set, false, 1).await;
        let key = set.slabs[0].files["annv2:vector"].key.clone();
        let control = scratch();
        let manager = Manager::open(control.path(), Some(512 << 20), Arc::default()).unwrap();
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
        let want = ready(&manager, "", &set).await;

        let work = scratch();
        let manager = Manager::open(work.path(), Some(512 << 20), Arc::default()).unwrap();
        let pre = prestaged(&manager, &store, &set, &ann, true).await;
        let local = {
            let files = pre.0.files.lock().unwrap();
            let written = &files[&key];
            let ends = written
                .encoded
                .as_ref()
                .expect("prestaged f16 bytes are encoded");
            codec::validate(ends, written.len).unwrap();
            assert!(u64::from(*ends.last().unwrap()) < written.len);
            assert_eq!(
                written.file.metadata().unwrap().len(),
                u64::from(*ends.last().unwrap())
            );
            written.file.clone()
        };
        assert!(manager.state.lock().unwrap().used > 0, "the cap is charged");
        pre.publish(&set);
        // Until the commit is confirmed, no query stages this tier at all.
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
        {
            let state = manager.state.lock().unwrap();
            assert!(state.pending.is_empty() && state.ready.is_empty());
        }
        pre.activate();
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
        let got = ready(&manager, "", &set).await;
        assert!(
            Arc::ptr_eq(&got.files[&key].file, &local),
            "the slab was not fetched again"
        );
        assert_eq!(got.files.len(), want.files.len());
        for (name, f) in &want.files {
            let g = &got.files[name];
            assert_eq!(
                (g.len, &g.crc, &g.encoded, &g.identity),
                (f.len, &f.crc, &f.encoded, &f.identity)
            );
            let all = std::slice::from_ref(&(0..f.len)).to_vec();
            assert_eq!(g.read(&all).unwrap(), f.read(&all).unwrap());
        }
        assert!(manager.state.lock().unwrap().prestaged.is_empty());
        let used = manager.state.lock().unwrap().used;
        assert!(used <= manager.capacity);
        // Another observation starts nothing, and the stage survives a restart.
        let path = got.path.clone();
        drop((got, manager));
        assert!(path.join("complete").exists());
        let manager = Manager::open(work.path(), Some(512 << 20), Arc::default()).unwrap();
        manager.recover(&store).await;
        assert!(
            manager
                .read(&key, &key, std::slice::from_ref(&(0..64)))
                .is_some()
        );
    }

    #[tokio::test]
    async fn a_failed_commit_discards_prestaged_bytes_and_the_cap() {
        let (store, set, ann, _) = fixture(42).await;
        publish(&store, &set, false, 1).await;
        let work = scratch();
        let manager = Manager::open(work.path(), Some(512 << 20), Arc::default()).unwrap();
        let pre = prestaged(&manager, &store, &set, &ann, true).await;
        let dir = pre.0.dir.clone();
        assert!(dir.is_dir());
        pre.publish(&set);
        drop(pre);
        assert!(!dir.exists(), "nothing of the failed tier stays on disk");
        {
            let state = manager.state.lock().unwrap();
            assert!(state.prestaged.is_empty());
            assert_eq!((state.used, state.files_used), (0, 0));
        }
        // The tier still stages the ordinary way if it is published after all.
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
        ready(&manager, "", &set).await;
    }

    #[tokio::test]
    async fn a_restart_before_the_first_query_leaves_no_prestage_and_the_tier_stages_normally() {
        let (store, set, ann, _) = fixture(43).await;
        publish(&store, &set, false, 1).await;
        let work = scratch();
        let manager = Manager::open(work.path(), Some(512 << 20), Arc::default()).unwrap();
        let pre = prestaged(&manager, &store, &set, &ann, true).await;
        let dir = pre.0.dir.clone();
        pre.publish(&set);
        pre.activate();
        // The process dies here: the manager's registry is gone, but the files are still on disk.
        let leaked = manager
            .state
            .lock()
            .unwrap()
            .prestaged
            .drain()
            .collect::<Vec<_>>();
        for (_, p) in &leaked {
            p.taken.store(true, Ordering::Release);
        }
        drop((leaked, manager));
        assert!(dir.is_dir());
        let manager = Manager::open(work.path(), Some(512 << 20), Arc::default()).unwrap();
        assert!(
            !dir.exists(),
            "an uncommitted-looking directory is never adopted"
        );
        manager.recover(&store).await;
        assert!(manager.state.lock().unwrap().ready.is_empty());
        manager.observe(&store, "", &set, 32, 16, &Vec::new);
        let stage = ready(&manager, "", &set).await;
        let key = &set.slabs[0].files["annv2:vector"].key;
        assert!(stage.files.contains_key(key));
    }

    #[tokio::test]
    async fn prestaging_respects_the_cap_and_a_tight_one_leaves_the_rest_to_the_download() {
        let (store, set, ann, end) = fixture(44).await;
        publish(&store, &set, false, 1).await;
        let work = scratch();
        // Room for the codes, but not for the exact vectors beside them.
        let manager = Manager::open(work.path(), Some(end + (64 << 10)), Arc::default()).unwrap();
        let pre = prestaged(&manager, &store, &set, &ann, true).await;
        let key = &set.slabs[0].files["annv2:vector"].key;
        assert_eq!(pre.0.files.lock().unwrap()[key].len, end);
        assert!(manager.state.lock().unwrap().used <= manager.capacity);
        drop(pre);
        // No room at all: nothing is written, and the upload is unaffected.
        let tiny_dir = scratch();
        let tiny = Manager::open(tiny_dir.path(), Some(1024), Arc::default()).unwrap();
        let pre = prestaged(&tiny, &store, &set, &ann, true).await;
        assert!(pre.0.files.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn prestage_admits_all_vectors_when_only_the_encoded_bytes_fit() {
        let (store, set, ann, _) = fixture_dtype(47, bf_vector::VectorType::F16).await;
        let file = &set.slabs[0].files["annv2:vector"];
        let (_, len) = crate::recluster::stage_lengths(&ann, file.metadata_len);
        let metadata = len.div_ceil(BLOCK as u64) * 22;
        let stored = ann[..len as usize]
            .chunks(BLOCK)
            .map(codec::encoded_len)
            .sum::<usize>() as u64;
        assert!(stored < len);
        let work = scratch();
        // The reserve protects future codes and metadata. Beside it, only the
        // encoded payload fits; charging raw vector bytes would reject the tail.
        let capacity = (64 << 20) + stored + metadata;
        let manager = Manager::open(work.path(), Some(capacity), Arc::default()).unwrap();
        let pre = prestaged(&manager, &store, &set, &ann, true).await;
        let files = pre.0.files.lock().unwrap();
        let written = &files[&file.key];
        assert_eq!(written.len, len);
        assert_eq!(written.file.metadata().unwrap().len(), stored);
        assert_eq!(manager.state.lock().unwrap().used, stored + metadata);
    }

    #[tokio::test]
    async fn a_prefix_cut_from_a_longer_prestage_keeps_every_checksum_true() {
        let (store, set, ann, end) = fixture_dtype(45, bf_vector::VectorType::F16).await;
        for len in [
            end - 1,
            end + BLOCK as u64 + 16,
            end + BLOCK as u64 + 17,
            (end / BLOCK as u64 + 1) * BLOCK as u64,
            0,
        ] {
            let work = scratch();
            let manager = Manager::open(work.path(), Some(512 << 20), Arc::default()).unwrap();
            let pre = prestaged(&manager, &store, &set, &ann, true).await;
            let file = set.slabs[0].files["annv2:vector"].clone();
            // Cover code cuts, packed vector cuts, block boundaries, and empty prefixes.
            assert!(
                len < crate::recluster::stage_lengths(
                    &ann,
                    set.slabs[0].files["annv2:vector"].metadata_len
                )
                .1
            );
            let mut wanted = vec![(file.clone(), 0, len)];
            let taken = pre.0.adopt(&mut wanted).await.unwrap();
            assert!(wanted.is_empty());
            let f = &taken[&file.key];
            assert_eq!(f.len, len);
            let all = f.read(std::slice::from_ref(&(0..len))).unwrap();
            assert_eq!(&all[0][..], &ann[..len as usize]);
            assert_eq!(
                fs::metadata(pre.0.dir.join(source_name(&file.key)))
                    .unwrap()
                    .len(),
                f.stored_len()
            );
            // An object stored shorter than wanted is left to the download, and its file removed.
            let short = scratch();
            let manager = Manager::open(short.path(), Some(512 << 20), Arc::default()).unwrap();
            let pre = prestaged(&manager, &store, &set, &ann, false).await;
            let mut wanted = vec![(file.clone(), 0, end + 1)];
            assert!(pre.0.adopt(&mut wanted).await.unwrap().is_empty());
            assert_eq!(wanted.len(), 1);
            assert!(!pre.0.dir.join(source_name(&file.key)).exists());
        }
    }
}

#[cfg(test)]
#[path = "local_ann_cold_wal_tests.rs"]
mod cold_wal_tests;
