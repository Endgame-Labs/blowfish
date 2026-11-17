//! Immutable object range caching and per-query accounting.
use crate::{
    Error, HEADER, MAGIC, Storage,
    disk::{Disk, digest, read_block},
};
use bytes::{Bytes, BytesMut};
use futures::{
    FutureExt, StreamExt, TryStreamExt,
    future::{BoxFuture, Shared},
    stream,
};
use moka::sync::Cache;
use object_store::ObjectStoreExt;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
    path::PathBuf,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

/// Cache budgets and I/O limits. Disk storage is disabled unless `disk_dir` is set.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    /// Resident block payload budget in bytes. Zero disables memory admission.
    pub memory_bytes: u64,
    /// Disk budget in bytes, including each block's 40-byte checksum header.
    pub disk_bytes: u64,
    /// Parent directory for exclusively locked, store-specific disk caches.
    pub disk_dir: Option<PathBuf>,
    /// Stable identity of the bucket/backend. `Storage::open` supplies one automatically.
    /// Custom backends must supply a distinct identity when disk caching is enabled.
    pub namespace: Option<String>,
    /// Logical block size in bytes (default 1 MiB).
    pub block_size: usize,
    /// Maximum blocks coalesced into one backend GET (default 8).
    pub max_coalesce_blocks: usize,
    /// Missing blocks separated by at most this many unrequested blocks share one GET; the
    /// gap bytes are fetched and discarded. Trades bytes for fewer requests (default 0).
    pub coalesce_gap_blocks: usize,
    /// Maximum simultaneous cache-fill GETs across all readers (default 64).
    pub fetch_concurrency: usize,
    /// Maximum background warming tasks; excess hints are rejected (default 4).
    pub prefetch_concurrency: usize,
    /// Extra-request budget for hedged ranged reads, as a fraction of primary requests
    /// (0 disables hedging; default 0.1). `BF_HEDGE_BUDGET` overrides it.
    pub hedge_budget: f64,
}
impl Default for CacheConfig {
    fn default() -> Self {
        // Respect Linux container limits as well as host RAM. Portable fallback: 1 GiB.
        let host = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|s| {
                s.lines().find_map(|l| {
                    l.strip_prefix("MemTotal:")?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
            })
            .map(|kb| kb.saturating_mul(1024))
            .unwrap_or(4 << 30);
        let limit = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(host);
        Self {
            memory_bytes: (host.min(limit) / 4).min(8 << 30),
            disk_bytes: 64 << 30,
            disk_dir: None,
            namespace: None,
            block_size: 1 << 20,
            max_coalesce_blocks: 8,
            coalesce_gap_blocks: 0,
            fetch_concurrency: 512,
            prefetch_concurrency: 4,
            hedge_budget: 0.1,
        }
    }
}

/// Share one accumulator across the parallel reads belonging to a query.
/// Counts requested logical bytes, including repeated and overlapping ranges.
#[derive(Debug, Default)]
pub struct CacheStats {
    memory: AtomicU64,
    disk: AtomicU64,
    misses: AtomicU64,
}
/// A consistent-enough snapshot for query performance reporting after reads finish.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStatsSnapshot {
    /// Requested bytes served from memory.
    pub memory_bytes: u64,
    /// Requested bytes served from disk.
    pub disk_bytes: u64,
    /// Requested bytes requiring an object-store fetch, including single-flight waiters.
    pub missed_bytes: u64,
}
impl CacheStats {
    /// Snapshot the counters. Call after all query reads finish for final statistics.
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            memory_bytes: self.memory.load(Ordering::Relaxed),
            disk_bytes: self.disk.load(Ordering::Relaxed),
            missed_bytes: self.misses.load(Ordering::Relaxed),
        }
    }
    /// Fraction of requested bytes supplied by either cache tier; no reads means 1.0.
    pub fn cache_hit_ratio(&self) -> f64 {
        self.snapshot().cache_hit_ratio()
    }
    /// `hot` for all-memory reads, `warm` for disk or mixed hits, and `cold` for all misses.
    pub fn temperature(&self) -> &'static str {
        self.snapshot().temperature()
    }
    /// Account for logical bytes supplied by a caller's decoded in-memory cache.
    /// Use the encoded size of the cached metadata, not its resident allocation size.
    pub fn record_memory_hit(&self, bytes: u64) {
        self.record(Tier::Memory, bytes);
    }
    fn record(&self, tier: Tier, bytes: u64) {
        self.record_many(tier, bytes, 1);
    }
    fn record_many(&self, tier: Tier, bytes: u64, blocks: u64) {
        if blocks == 0 {
            return;
        }
        let counter = match tier {
            Tier::Memory => &self.memory,
            Tier::Disk => &self.disk,
            Tier::Miss => &self.misses,
        };
        counter.fetch_add(bytes, Ordering::Relaxed);
        match tier {
            Tier::Memory => {
                metrics::counter!("blowfish_cache_hits_total", "tier" => "memory").increment(blocks)
            }
            Tier::Disk => {
                metrics::counter!("blowfish_cache_hits_total", "tier" => "disk").increment(blocks)
            }
            Tier::Miss => metrics::counter!("blowfish_cache_misses_total").increment(blocks),
        }
    }
}
impl CacheStatsSnapshot {
    /// Byte-weighted hit ratio across memory and disk.
    pub fn cache_hit_ratio(self) -> f64 {
        let hit = self.memory_bytes as f64 + self.disk_bytes as f64;
        let total = hit + self.missed_bytes as f64;
        if total == 0.0 { 1.0 } else { hit / total }
    }
    /// Wire-compatible cache temperature. These thresholds are blowfish's policy.
    pub fn temperature(self) -> &'static str {
        if self.missed_bytes == 0 && self.disk_bytes == 0 {
            "hot"
        } else if self.memory_bytes == 0 && self.disk_bytes == 0 {
            "cold"
        } else {
            "warm"
        }
    }
}
#[derive(Clone, Copy)]
enum Tier {
    Memory,
    Disk,
    Miss,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BlockKey {
    path: Arc<str>,
    index: u64,
    offset: u64,
    len: u64,
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct Layout {
    len: u64,
    offset: u64,
}
type Fetch = Shared<BoxFuture<'static, Result<Bytes, Arc<Error>>>>;
struct Flight(Fetch);
#[derive(Default)]
struct Planning {
    flights: HashMap<BlockKey, Weak<Flight>>,
}

pub(crate) struct RangeCache {
    pub(crate) config: CacheConfig,
    memory: Cache<BlockKey, Bytes>,
    disk: Option<Arc<Mutex<Disk>>>,
    layouts: Cache<String, Layout>,
    layout_locks: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
    planning: AsyncMutex<Planning>,
    fetches: Semaphore,
    prefetches: Arc<Semaphore>,
    pub(crate) hedge: crate::hedge::Hedger,
    /// Disk writes started behind reads and not yet finished.
    spills: Arc<Spills>,
}
#[derive(Default)]
struct Spills {
    pending: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
}
impl RangeCache {
    /// Empty the memory and disk tiers and the object-length cache (bench cold-cache runs).
    pub(crate) fn drop_all(&self) {
        self.memory.invalidate_all();
        self.memory.run_pending_tasks();
        self.layouts.invalidate_all();
        if let Some(disk) = &self.disk {
            disk.lock().expect("disk cache lock").clear();
        }
    }
    pub(crate) async fn open(config: CacheConfig) -> Result<Self, Error> {
        if config.block_size == 0
            || config.block_size > u32::MAX as usize
            || config.fetch_concurrency == 0
            || config.prefetch_concurrency == 0
            || config.fetch_concurrency > Semaphore::MAX_PERMITS
            || config.prefetch_concurrency > Semaphore::MAX_PERMITS
            || config.max_coalesce_blocks == 0
            || config
                .block_size
                .checked_mul(config.max_coalesce_blocks)
                .is_none()
        {
            return Err(Error::Config("invalid cache block size or concurrency"));
        }
        let disk = if let Some(dir) = config.disk_dir.clone().filter(|_| config.disk_bytes > 0) {
            let namespace = config.namespace.clone().ok_or(Error::Config(
                "disk cache requires a stable backend namespace",
            ))?;
            let budget = config.disk_bytes;
            Some(Arc::new(Mutex::new(
                tokio::task::spawn_blocking(move || Disk::open(&dir, &namespace, budget))
                    .await
                    .map_err(|_| Error::Config("disk cache initialization task failed"))??,
            )))
        } else {
            None
        };
        let memory = Cache::builder()
            .max_capacity(config.memory_bytes)
            .weigher(|_: &BlockKey, value: &Bytes| value.len() as u32)
            .eviction_listener(|_, value: Bytes, cause| {
                metrics::gauge!("blowfish_cache_bytes", "tier" => "memory")
                    .decrement(value.len() as f64);
                if cause == moka::notification::RemovalCause::Size {
                    metrics::counter!("blowfish_cache_evictions_total", "tier" => "memory")
                        .increment(1);
                }
            })
            .build();
        Ok(Self {
            fetches: Semaphore::new(config.fetch_concurrency),
            prefetches: Arc::new(Semaphore::new(config.prefetch_concurrency)),
            hedge: crate::hedge::Hedger::new(
                std::env::var("BF_HEDGE_BUDGET")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(config.hedge_budget),
            ),
            spills: Arc::default(),
            config,
            memory,
            disk,
            layouts: Cache::new(16_384),
            layout_locks: Mutex::new(HashMap::new()),
            planning: AsyncMutex::new(Planning::default()),
        })
    }
    /// Drop every cached block, object layout, and disk block. In-flight reads are unaffected.
    pub(crate) fn clear(&self) {
        self.memory.invalidate_all();
        self.memory.run_pending_tasks();
        self.layouts.invalidate_all();
        if let Some(disk) = &self.disk {
            disk.lock().unwrap().clear();
        }
    }
    fn disk_key(&self, key: &BlockKey) -> String {
        digest(format!(
            "{}:{}:{}:{}:{}:{}",
            self.config.block_size,
            key.offset,
            key.len,
            key.index,
            key.path.len(),
            key.path
        ))
    }
    /// Look up blocks in memory, then on disk, with no cache-wide lock held. Disk reads are
    /// split across blocking workers and run outside the disk index lock.
    async fn lookup_many(&self, wanted: &[(BlockKey, usize)]) -> Vec<Option<(Bytes, Tier)>> {
        let mut found: Vec<Option<(Bytes, Tier)>> = wanted
            .iter()
            .map(|(key, _)| self.memory.get(key).map(|b| (b, Tier::Memory)))
            .collect();
        let Some(disk) = self.disk.clone() else {
            return found;
        };
        let missing: Vec<usize> = (0..found.len()).filter(|i| found[*i].is_none()).collect();
        if missing.is_empty() {
            return found;
        }
        let jobs: Vec<_> = missing
            .iter()
            .map(|&i| (i, self.disk_key(&wanted[i].0), wanted[i].1))
            .collect();
        let chunk = jobs.len().div_ceil(8).max(64);
        let mut tasks = Vec::new();
        for part in jobs.chunks(chunk) {
            let part = part.to_vec();
            let disk = disk.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                let paths: Vec<_> = {
                    let disk = disk.lock().unwrap();
                    part.iter().map(|(_, name, _)| disk.locate(name)).collect()
                };
                let mut out = Vec::with_capacity(part.len());
                let mut verdicts = Vec::new();
                for ((i, name, expected), path) in part.into_iter().zip(paths) {
                    let Some(path) = path else { continue };
                    let bytes = read_block(&path, expected);
                    verdicts.push((name, bytes.is_some()));
                    if let Some(bytes) = bytes {
                        out.push((i, bytes));
                    }
                }
                let mut disk = disk.lock().unwrap();
                for (name, ok) in verdicts {
                    disk.finish_read(&name, ok);
                }
                out
            }));
        }
        for task in tasks {
            for (i, bytes) in task.await.unwrap_or_default() {
                self.insert_memory(wanted[i].0.clone(), bytes.clone());
                found[i] = Some((bytes, Tier::Disk));
            }
        }
        found
    }
    /// Wait until every disk write started behind a read has finished.
    pub(crate) async fn flush(&self) {
        loop {
            let idle = self.spills.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self
                .spills
                .pending
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                return;
            }
            idle.await;
        }
    }
    fn insert_memory(&self, key: BlockKey, value: Bytes) {
        if value.len() as u64 > self.config.memory_bytes || self.config.memory_bytes == 0 {
            return;
        }
        // The planner and single-flight reservations guarantee one insertion per block.
        metrics::gauge!("blowfish_cache_bytes", "tier" => "memory").increment(value.len() as f64);
        self.memory.insert(key, value);
    }
}
impl Drop for RangeCache {
    fn drop(&mut self) {
        self.memory.invalidate_all();
        self.memory.run_pending_tasks();
    }
}

/// A reader over one immutable object's logical bytes (portable retry headers are hidden).
#[derive(Clone)]
pub struct RangeReader {
    storage: Storage,
    key: String,
    path: Arc<str>,
    layout: Layout,
}
impl RangeReader {
    /// Logical object length, excluding the retry header.
    pub fn len(&self) -> u64 {
        self.layout.len
    }
    /// Whether the object has no logical bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Read one exact half-open range.
    pub async fn read(&self, range: Range<u64>) -> Result<Bytes, Error> {
        self.read_with_stats(range, &CacheStats::default()).await
    }
    /// Read one range and charge the provided query accumulator.
    pub async fn read_with_stats(
        &self,
        range: Range<u64>,
        stats: &CacheStats,
    ) -> Result<Bytes, Error> {
        Ok(self.read_batch_with_stats(&[range], stats).await?.remove(0))
    }
    /// Read ranges in input order, coalescing missing blocks across the entire batch.
    pub async fn read_batch(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>, Error> {
        self.read_batch_with_stats(ranges, &CacheStats::default())
            .await
    }
    /// Read a batch with query accounting and a shared, store-wide I/O concurrency limit.
    pub async fn read_batch_with_stats(
        &self,
        ranges: &[Range<u64>],
        stats: &CacheStats,
    ) -> Result<Vec<Bytes>, Error> {
        let cache = &self.storage.0.cache;
        let size = cache.config.block_size as u64;
        let mut requested = BTreeMap::<u64, u64>::new();
        for range in ranges {
            if range.start > range.end || range.end > self.len() {
                return Err(Error::Config("invalid object range"));
            }
            if range.is_empty() {
                continue;
            }
            for index in range.start / size..=(range.end - 1) / size {
                *requested.entry(index).or_default() +=
                    range.end.min((index * size).saturating_add(size))
                        - range.start.max(index * size);
            }
        }
        // Cache lookups (memory, then disk) run without the planning lock.
        let wanted: Vec<_> = requested
            .keys()
            .map(|&index| {
                let expected = (self.len() - index * size).min(size) as usize;
                (self.block_key(index), expected)
            })
            .collect();
        let looked_up = cache.lookup_many(&wanted).await;
        let mut blocks = BTreeMap::<u64, Bytes>::new();
        let mut unresolved = Vec::new();
        let (mut memory, mut disk, mut memory_blocks, mut disk_blocks) = (0, 0, 0, 0);
        for ((index, weight), found) in requested.iter().zip(looked_up) {
            match found {
                Some((bytes, Tier::Memory)) => {
                    memory += weight;
                    memory_blocks += 1;
                    blocks.insert(*index, bytes);
                }
                Some((bytes, _)) => {
                    disk += weight;
                    disk_blocks += 1;
                    blocks.insert(*index, bytes);
                }
                None => unresolved.push((*index, *weight)),
            }
        }
        stats.record_many(Tier::Memory, memory, memory_blocks);
        stats.record_many(Tier::Disk, disk, disk_blocks);
        // Serialize only the reservation of missing blocks, not network or disk I/O. The
        // re-check under the lock closes races with fetches that finished since the lookup.
        let mut planning = cache.planning.lock().await;
        planning.flights.retain(|_, weak| weak.strong_count() > 0);
        let mut pending = Vec::<(u64, Arc<Flight>)>::new();
        let mut missing = Vec::new();
        for (index, weight) in unresolved {
            let key = self.block_key(index);
            if let Some(bytes) = cache.memory.get(&key) {
                stats.record(Tier::Memory, weight);
                blocks.insert(index, bytes);
                continue;
            }
            stats.record(Tier::Miss, weight);
            if let Some(flight) = planning.flights.get(&key).and_then(Weak::upgrade) {
                pending.push((index, flight));
            } else {
                missing.push(index);
            }
        }
        let mut start = 0;
        while start < missing.len() {
            let mut end = start + 1;
            let gap = cache.config.coalesce_gap_blocks as u64;
            while end < missing.len()
                && (missing[end] - missing[start]) < cache.config.max_coalesce_blocks as u64
                && missing[end] <= missing[end - 1] + 1 + gap
            {
                end += 1;
            }
            let indices = missing[start..end].to_vec();
            let reader = self.clone();
            let first = indices[0];
            let last = *indices.last().unwrap();
            let group = async move {
                let cache = &reader.storage.0.cache;
                let _permit = cache
                    .fetches
                    .acquire()
                    .await
                    .expect("cache semaphore stays open");
                let from = first * size;
                let to = (last * size).saturating_add(size).min(reader.len());
                let bytes = cache
                    .hedge
                    .get_range(
                        &reader.storage.0.backend,
                        &reader.storage.path(&reader.key),
                        from + reader.layout.offset..to + reader.layout.offset,
                    )
                    .await
                    .map_err(|e| Arc::new(Error::from(e)))?;
                if bytes.len() as u64 != to - from {
                    return Err(Arc::new(Error::Config("short object-store range response")));
                }
                let mut values = Vec::with_capacity(indices.len());
                // At most MAX_SPILLS disk writes wait at once: on a cold cache the disk is slower
                // than the network, and an unbounded backlog would hold every fetched block in
                // memory. A skipped write only means that block is fetched again later.
                const MAX_SPILLS: usize = 16;
                let spilling = cache.disk.is_some()
                    && cache
                        .spills
                        .pending
                        .load(std::sync::atomic::Ordering::SeqCst)
                        < MAX_SPILLS;
                let mut spill = Vec::new();
                for index in indices {
                    let begin = ((index - first) * size) as usize;
                    let end = bytes.len().min(begin + size as usize);
                    // Independent allocations keep a single cached block from pinning a whole GET.
                    let value = Bytes::copy_from_slice(&bytes[begin..end]);
                    let key = reader.block_key(index);
                    if spilling {
                        spill.push((cache.disk_key(&key), value.clone()));
                    }
                    cache.insert_memory(key, value.clone());
                    values.push(value);
                }
                // The disk tier is written behind the read: a block-by-block write that the
                // read awaited cost about a millisecond per 4 KB block, which made a cold 2 MB
                // read take half a second after the network transfer had finished.
                if let Some(disk) = cache.disk.clone().filter(|_| spilling) {
                    let spills = cache.spills.clone();
                    spills
                        .pending
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tokio::task::spawn_blocking(move || {
                        {
                            let mut disk = disk.lock().unwrap();
                            for (name, value) in spill {
                                // A full or damaged cache never makes source-of-truth reads fail.
                                let _ = disk.insert(&name, &value);
                            }
                        }
                        spills
                            .pending
                            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        spills.idle.notify_waiters();
                    });
                }
                // Apply pending evictions once per coalesced fetch, so memory stays within
                // budget without paying for maintenance on every block.
                cache.memory.run_pending_tasks();
                Ok::<_, Arc<Error>>(Arc::new(values))
            }
            .boxed()
            .shared();
            for (position, index) in missing[start..end].iter().copied().enumerate() {
                let fetch = group.clone();
                let flight = Arc::new(Flight(
                    async move { Ok(fetch.await?[position].clone()) }
                        .boxed()
                        .shared(),
                ));
                planning
                    .flights
                    .insert(self.block_key(index), Arc::downgrade(&flight));
                pending.push((index, flight));
            }
            start = end;
        }
        drop(planning);
        let fetched: Vec<_> = stream::iter(pending)
            .map(|(index, flight)| async move {
                let bytes = flight.0.clone().await.map_err(Error::from_shared)?;
                Ok::<_, Error>((index, bytes))
            })
            // Poll enough block waiters to start all permitted coalesced GETs. The
            // semaphore still limits actual I/O, including shared and overlapping groups.
            .buffer_unordered(
                cache
                    .config
                    .fetch_concurrency
                    .saturating_mul(cache.config.max_coalesce_blocks),
            )
            .try_collect()
            .await?;
        blocks.extend(fetched);
        ranges
            .iter()
            .map(|range| {
                if range.is_empty() {
                    return Ok(Bytes::new());
                }
                let first = range.start / size;
                let last = (range.end - 1) / size;
                if first == last {
                    return Ok(blocks[&first].slice(
                        (range.start % size) as usize..((range.end - first * size) as usize),
                    ));
                }
                let len = usize::try_from(range.end - range.start)
                    .map_err(|_| Error::Config("range exceeds address space"))?;
                let mut out = BytesMut::with_capacity(len);
                for index in first..=last {
                    let begin = range.start.saturating_sub(index * size) as usize;
                    let end = (range.end - index * size).min(blocks[&index].len() as u64) as usize;
                    out.extend_from_slice(&blocks[&index][begin..end]);
                }
                Ok(out.freeze())
            })
            .collect()
    }
    fn block_key(&self, index: u64) -> BlockKey {
        BlockKey {
            path: self.path.clone(),
            index,
            offset: self.layout.offset,
            len: self.layout.len,
        }
    }
}
impl Storage {
    /// Open an immutable object, caching positive HEAD and header-layout results.
    /// Missing objects are never cached, so new WAL slots become visible immediately.
    pub async fn range_reader(&self, key: &str) -> Result<RangeReader, Error> {
        let cache = &self.0.cache;
        let ckey = self.cache_key(key);
        let lock = {
            let mut locks = cache.layout_locks.lock().unwrap();
            locks.retain(|_, weak| weak.strong_count() > 0);
            match locks.get(&ckey).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(AsyncMutex::new(()));
                    locks.insert(ckey.clone(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        let _guard = lock.lock().await;
        let layout = if let Some(layout) = cache.layouts.get(&ckey) {
            layout
        } else {
            let path = self.path(key);
            let _permit = cache
                .fetches
                .acquire()
                .await
                .expect("cache semaphore stays open");
            let meta = self.0.backend.head(&path).await?;
            let offset = if meta.size >= HEADER as u64 {
                let prefix = self.0.backend.get_range(&path, 0..HEADER as u64).await?;
                if prefix.len() != HEADER {
                    return Err(Error::Config("short object-store header response"));
                }
                if prefix.starts_with(MAGIC) {
                    HEADER as u64
                } else {
                    0
                }
            } else {
                0
            };
            let layout = Layout {
                len: meta.size - offset,
                offset,
            };
            cache.layouts.insert(ckey.clone(), layout);
            layout
        };
        Ok(RangeReader {
            storage: self.clone(),
            key: key.to_owned(),
            path: self.path(key).to_string().into(),
            layout,
        })
    }
    /// Open an immutable object without HEAD or header probing. The caller supplies the
    /// logical length and whether the physical object has blowfish's portable retry header.
    pub fn range_reader_with_length(
        &self,
        key: &str,
        len: u64,
        has_header: bool,
    ) -> Result<RangeReader, Error> {
        let offset = if has_header { HEADER as u64 } else { 0 };
        len.checked_add(offset)
            .ok_or(Error::Config("object length overflow"))?;
        Ok(RangeReader {
            storage: self.clone(),
            key: key.to_owned(),
            path: self.path(key).to_string().into(),
            layout: Layout { len, offset },
        })
    }
    /// Cached whole-object read for immutable objects, sharing the range cache and single-flight.
    /// Use `get` for mutable system objects. Prefer range reads for large index objects.
    pub async fn get_cached(&self, key: &str) -> Result<Bytes, Error> {
        self.get_cached_with_stats(key, &CacheStats::default())
            .await
    }
    /// Cached whole-object read charged to a query accumulator.
    pub async fn get_cached_with_stats(
        &self,
        key: &str,
        stats: &CacheStats,
    ) -> Result<Bytes, Error> {
        let reader = self.range_reader(key).await?;
        reader.read_with_stats(0..reader.len(), stats).await
    }
    /// Schedule a background warming hint. Excess concurrent hints return `PrefetchBusy`.
    /// The returned handle reports errors; dropping it leaves the warming task running.
    pub fn prefetch(
        &self,
        key: &str,
        ranges: Vec<Range<u64>>,
    ) -> Result<tokio::task::JoinHandle<Result<(), Error>>, Error> {
        let permit = self
            .0
            .cache
            .prefetches
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::PrefetchBusy)?;
        let storage = self.clone();
        let key = key.to_owned();
        Ok(tokio::spawn(async move {
            let _permit = permit;
            let reader = storage.range_reader(&key).await?;
            // Process bounded chunks without retaining the whole warmed object in memory.
            let size = storage.0.cache.config.block_size as u64;
            let chunk = size * storage.0.cache.config.max_coalesce_blocks as u64;
            for range in &ranges {
                if range.start > range.end || range.end > reader.len() {
                    return Err(Error::Config("invalid object range"));
                }
            }
            for range in ranges {
                let mut pos = range.start;
                while pos < range.end {
                    let end = pos.saturating_add(chunk).min(range.end);
                    reader.read(pos..end).await?;
                    pos = end;
                }
            }
            Ok(())
        }))
    }
}
