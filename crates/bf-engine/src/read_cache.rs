//! Engine-owned decoded metadata LRU and query-local range accounting.
use bf_storage::CacheStats;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

type Key = (TypeId, String);
struct Entry {
    value: Arc<dyn Any + Send + Sync>,
    bytes: u64,
    used: u64,
}
#[derive(Default)]
struct Entries {
    values: HashMap<Key, Entry>,
    bytes: u64,
    clock: u64,
}
/// Immutable keys include the object generation. In-flight users retain their own Arc.
pub(crate) struct DecodedCache {
    budget: u64,
    entries: Arc<Mutex<Entries>>,
    /// Keys are relative to a storage view, so each view prefixes its own scope.
    scope: String,
    pub workers: Arc<Semaphore>,
    /// Decoded directory cluster blobs (center, row set, checksums), by ANN object and cluster.
    /// A warm query probes hundreds of clusters; decoding each blob again cost about as much
    /// CPU as scanning its codes. Its own concurrent LRU, because these entries are small and
    /// many (the general LRU's eviction scans every entry).
    clusters: ClusterCache,
    /// Decoded document column blocks (256 rows each), for hydrating results.
    columns: Arc<PredicateColumns>,
    /// Decoded resident WAL vectors; charged to the same total budget as immutable metadata.
    pub(crate) tail_vectors: crate::tail::VectorCache,
    /// Immutable WAL and shadow-row term counts, charged to this cache's total budget.
    pub(crate) text_terms: Arc<ClockCache<Arc<crate::text_tail::Analysis>, crate::text_tail::Key>>,
}
/// foldhash, seeded per process (the keys are object keys and cluster numbers).
type FastHash = foldhash::fast::RandomState;
/// An ANN object's key and a cluster number in it.
type ClusterKey = (Arc<str>, u32);
pub(crate) type ClusterCache = Arc<ClockCache<bf_vector::DecodedCluster>>;

/// A byte-bounded cache of small, many, immutable values for hot paths: sharded mutexes over
/// hash maps with CLOCK (second-chance) eviction. A lookup is one uncontended lock, a hash
/// probe, and a clone; moka's per-read bookkeeping cost several times that under load.
pub(crate) struct ClockCache<V, K = ClusterKey> {
    shards: Vec<Mutex<ClockShard<V, K>>>,
    weigh: fn(&V) -> u64,
}
struct ClockShard<V, K> {
    map: HashMap<K, (V, u64, bool), FastHash>,
    ring: std::collections::VecDeque<K>,
    bytes: u64,
    cap: u64,
}
impl<V: Clone, K: Clone + std::hash::Hash + Eq> ClockCache<V, K> {
    const SHARDS: usize = 64;
    pub fn new(capacity: u64, weigh: fn(&V) -> u64) -> Self {
        let cap = capacity / Self::SHARDS as u64;
        Self {
            shards: (0..Self::SHARDS)
                .map(|_| {
                    Mutex::new(ClockShard {
                        map: HashMap::default(),
                        ring: Default::default(),
                        bytes: 0,
                        cap,
                    })
                })
                .collect(),
            weigh,
        }
    }
    fn shard(&self, key: &K) -> &Mutex<ClockShard<V, K>> {
        use std::hash::BuildHasher;
        // foldhash, seeded per process: SipHash on each of a warm query's hundreds of cluster
        // lookups cost about 2% of CPU.
        static SEED: std::sync::OnceLock<foldhash::fast::RandomState> = std::sync::OnceLock::new();
        let h = SEED.get_or_init(Default::default).hash_one(key);
        &self.shards[h as usize % Self::SHARDS]
    }
    pub fn get(&self, key: &K) -> Option<V> {
        let mut shard = self.shard(key).lock().expect("clock cache lock");
        let (value, _, hot) = shard.map.get_mut(key)?;
        *hot = true;
        Some(value.clone())
    }
    pub fn insert(&self, key: K, value: V) {
        let mut shard = self.shard(&key).lock().expect("clock cache lock");
        let retired = self.insert_locked(&mut shard, key, value);
        drop(shard);
        drop(retired);
    }
    /// Initialize a missing immutable value once, under its shard lock. Initializers must
    /// be short, and must not call this cache recursively. Retired values drop after unlock.
    pub fn get_or_try_insert<E>(
        &self,
        key: K,
        init: impl FnOnce() -> Result<V, E>,
    ) -> Result<V, E> {
        let mut shard = self.shard(&key).lock().expect("clock cache lock");
        if let Some((value, _, hot)) = shard.map.get_mut(&key) {
            *hot = true;
            return Ok(value.clone());
        }
        let value = init()?;
        let retired = self.insert_locked(&mut shard, key, value.clone());
        drop(shard);
        drop(retired);
        Ok(value)
    }
    fn insert_locked(&self, shard: &mut ClockShard<V, K>, key: K, value: V) -> Vec<V> {
        let bytes = (self.weigh)(&value);
        let mut retired = Vec::new();
        if bytes > shard.cap {
            retired.push(value);
            return retired;
        }
        if let Some((old, b, _)) = shard.map.insert(key.clone(), (value, bytes, false)) {
            shard.bytes -= b;
            retired.push(old);
        } else {
            shard.ring.push_back(key);
        }
        shard.bytes += bytes;
        // Second chance: a value read since the hand last passed it goes round once more.
        while shard.bytes > shard.cap {
            let Some(k) = shard.ring.pop_front() else {
                break;
            };
            match shard.map.get_mut(&k) {
                Some((_, _, hot)) if *hot => {
                    *hot = false;
                    shard.ring.push_back(k);
                }
                Some(_) => {
                    let (old, b, _) = shard.map.remove(&k).expect("present");
                    shard.bytes -= b;
                    retired.push(old);
                }
                None => {}
            }
        }
        retired
    }
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut shard = shard.lock().expect("clock cache lock");
            shard.map.clear();
            shard.ring.clear();
            shard.bytes = 0;
        }
    }
    #[cfg(test)]
    pub fn resident_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.lock().expect("clock cache lock").bytes)
            .sum()
    }
}
type Column = Arc<Vec<Option<serde_json::Value>>>;
type Predicate = Arc<bf_vector::RoaringBitmap>;

/// One bounded pool, with protected predicate admission and columns borrowing every unused byte.
/// Only column/predicate lookups take this lock; the hot cluster cache stays independently sharded.
struct PredicateColumns {
    budget: u64,
    protected: u64,
    entries: Mutex<PredicateColumnEntries>,
}
struct PredicateColumnEntries {
    columns: ClockShard<Column, String>,
    predicates: ClockShard<Predicate, String>,
}
impl<V: Clone> ClockShard<V, String> {
    fn empty(cap: u64) -> Self {
        Self {
            map: HashMap::default(),
            ring: Default::default(),
            bytes: 0,
            cap,
        }
    }
    fn lookup(&mut self, key: &str) -> Option<V> {
        let (value, _, hot) = self.map.get_mut(key)?;
        *hot = true;
        Some(value.clone())
    }
    fn trim(&mut self, capacity: u64) -> Vec<V> {
        self.cap = capacity;
        let mut retired = Vec::new();
        while self.bytes > self.cap {
            let key = self
                .ring
                .pop_front()
                .expect("resident entries have clock slots");
            match self.map.get_mut(&key) {
                Some((_, _, hot)) if *hot => {
                    *hot = false;
                    self.ring.push_back(key);
                }
                Some(_) => {
                    let (value, bytes, _) = self.map.remove(&key).expect("present");
                    self.bytes -= bytes;
                    retired.push(value);
                }
                None => {}
            }
        }
        retired
    }
    fn put(&mut self, key: String, value: V, bytes: u64) -> Vec<V> {
        if bytes > self.cap {
            return vec![value];
        }
        let mut retired = Vec::new();
        if let Some((old, bytes, _)) = self.map.insert(key.clone(), (value, bytes, false)) {
            self.bytes -= bytes;
            retired.push(old);
        } else {
            self.ring.push_back(key);
        }
        self.bytes += bytes;
        retired.extend(self.trim(self.cap));
        retired
    }
}
impl PredicateColumns {
    fn new(budget: u64, protected: u64) -> Self {
        Self {
            budget,
            protected,
            entries: Mutex::new(PredicateColumnEntries {
                columns: ClockShard::empty(budget),
                predicates: ClockShard::empty(protected),
            }),
        }
    }
    fn column(&self, key: &str) -> Option<Column> {
        self.entries
            .lock()
            .expect("column cache lock")
            .columns
            .lookup(key)
    }
    fn insert_column(&self, key: String, value: Column) {
        // The hash map and clock ring each own a key string.
        let bytes = (key.len() as u64)
            .saturating_mul(2)
            .saturating_add(64)
            .saturating_add((value.len() as u64).saturating_mul(48));
        let mut entries = self.entries.lock().expect("column cache lock");
        // Predicate insertion/eviction updates this ceiling while holding the same lock.
        let retired = entries.columns.put(key, value, bytes);
        drop(entries);
        drop(retired);
    }
    fn predicate(&self, key: &str) -> Option<Predicate> {
        self.entries
            .lock()
            .expect("column cache lock")
            .predicates
            .lookup(key)
    }
    fn insert_predicate(&self, key: String, value: Predicate) {
        let bytes = (key.len() as u64)
            .saturating_mul(2)
            .saturating_add(128)
            .saturating_add((value.serialized_size() as u64).saturating_mul(2));
        let mut entries = self.entries.lock().expect("column cache lock");
        let predicates = entries.predicates.put(key, value, bytes);
        let capacity = self.budget - entries.predicates.bytes;
        let columns = entries.columns.trim(capacity);
        drop(entries);
        // Large value destructors run outside the admission lock.
        drop((predicates, columns));
    }
    fn clear(&self) {
        let mut entries = self.entries.lock().expect("column cache lock");
        let previous = std::mem::replace(
            &mut *entries,
            PredicateColumnEntries {
                columns: ClockShard::empty(self.budget),
                predicates: ClockShard::empty(self.protected),
            },
        );
        drop(entries);
        drop(previous);
    }
}
impl DecodedCache {
    /// `budget` bytes in all: three eighths for the general LRU, a quarter for clusters,
    /// a sixth for resident WAL vectors, an eighth for text counts, and a twelfth shared by columns and predicates.
    /// Predicates protect up to a quarter of that pool; columns borrow unused bytes.
    pub fn new(budget: u64, workers: usize) -> Self {
        let (clusters, columns, vectors, text) = (budget / 4, budget / 12, budget / 6, budget / 8);
        Self {
            budget: budget - clusters - columns - vectors - text,
            entries: Arc::new(Mutex::new(Entries::default())),
            scope: String::new(),
            workers: Arc::new(Semaphore::new(workers.max(1))),
            clusters: Arc::new(ClockCache::new(
                clusters,
                |v: &bf_vector::DecodedCluster| v.resident_bytes(),
            )),
            columns: Arc::new(PredicateColumns::new(columns, budget / 48)),
            tail_vectors: crate::tail::VectorCache::new(vectors),
            text_terms: Arc::new(ClockCache::new(
                text,
                |value: &Arc<crate::text_tail::Analysis>| value.bytes(),
            )),
        }
    }
    /// A decoded column block (`key` is relative to this view, like every other key).
    pub fn column(&self, key: &str) -> Option<Arc<Vec<Option<serde_json::Value>>>> {
        self.columns.column(&self.key(key))
    }
    /// Keep a decoded column block.
    pub fn insert_column(&self, key: &str, value: Arc<Vec<Option<serde_json::Value>>>) {
        self.columns.insert_column(self.key(key), value);
    }
    fn predicate(&self, key: &str) -> Option<Arc<bf_vector::RoaringBitmap>> {
        self.columns.predicate(&self.key(key))
    }
    fn insert_predicate(&self, key: &str, value: Arc<bf_vector::RoaringBitmap>) {
        self.columns.insert_predicate(self.key(key), value);
    }
    /// Decoded clusters of the ANN object `object` (keys are scoped like every other entry).
    pub fn clusters(&self, object: &str) -> (ClusterCache, Arc<str>) {
        (self.clusters.clone(), self.key(object).into())
    }
    /// A view sharing this cache's byte budget and workers, with keys kept separate.
    pub fn scoped(&self, scope: &str) -> Self {
        Self {
            budget: self.budget,
            entries: self.entries.clone(),
            scope: scope.to_owned(),
            workers: self.workers.clone(),
            clusters: self.clusters.clone(),
            columns: self.columns.clone(),
            // Document allocation identities are process-wide, independent of storage scope.
            tail_vectors: self.tail_vectors.clone(),
            text_terms: self.text_terms.clone(),
        }
    }
    /// Forget every decoded entry (bench cold-cache runs).
    pub fn clear(&self) {
        let mut e = self.entries.lock().expect("decoded cache lock");
        e.values.clear();
        e.bytes = 0;
        self.clusters.clear();
        self.columns.clear();
        self.tail_vectors.clear();
        self.text_terms.clear();
    }
    pub(crate) fn key(&self, key: &str) -> String {
        if self.scope.is_empty() {
            key.to_owned()
        } else {
            format!("{}\0{key}", self.scope)
        }
    }
    pub(crate) fn get<T: Any + Send + Sync>(&self, key: &str) -> Option<Arc<T>> {
        let key = self.key(key);
        let mut entries = self.entries.lock().expect("decoded cache lock");
        entries.clock += 1;
        let used = entries.clock;
        let entry = entries.values.get_mut(&(TypeId::of::<T>(), key))?;
        entry.used = used;
        entry.value.clone().downcast().ok()
    }
    fn insert<T: Any + Send + Sync>(&self, key: &str, bytes: u64, value: Arc<T>) {
        self.install(key, bytes, value, false);
    }
    fn install<T: Any + Send + Sync>(
        &self,
        key: &str,
        bytes: u64,
        value: Arc<T>,
        once: bool,
    ) -> Arc<T> {
        if bytes > self.budget || self.budget == 0 {
            return value;
        }
        let mut retired = Vec::new();
        let key = self.key(key);
        let mut entries = self.entries.lock().expect("decoded cache lock");
        let key = (TypeId::of::<T>(), key);
        if once && entries.values.contains_key(&key) {
            entries.clock += 1;
            let used = entries.clock;
            let entry = entries.values.get_mut(&key).expect("present cache entry");
            entry.used = used;
            return entry.value.clone().downcast().expect("cache type key");
        }
        if let Some(old) = entries.values.remove(&key) {
            entries.bytes -= old.bytes;
            retired.push(old);
        }
        while entries.bytes > self.budget - bytes {
            let oldest = entries
                .values
                .iter()
                .min_by_key(|(_, v)| v.used)
                .map(|(k, _)| k.clone())
                .expect("nonempty over-budget cache");
            let old = entries.values.remove(&oldest).expect("LRU entry");
            entries.bytes -= old.bytes;
            retired.push(old);
        }
        entries.clock += 1;
        let used = entries.clock;
        entries.bytes += bytes;
        entries.values.insert(
            key,
            Entry {
                value: value.clone(),
                bytes,
                used,
            },
        );
        drop(entries);
        // Large posting directories can own millions of allocations. Their
        // destruction must not serialize unrelated cache lookups or insertions.
        drop(retired);
        value
    }
}
#[derive(Clone)]
pub(crate) struct ReadContext {
    pub cache: Arc<DecodedCache>,
    pub stats: Arc<CacheStats>,
    pub backend: Arc<bf_storage::BackendStats>,
    /// Whether segment reads in this context admit what they fetch into the block caches.
    /// Background one-pass scans (the recall sampler's exhaustive search) set it to false, so
    /// they cannot evict the blocks that queries reuse.
    pub admit: bool,
    pub ann_rows: Arc<std::sync::atomic::AtomicU64>,
    pub document_scan_rows: Arc<std::sync::atomic::AtomicU64>,
}
impl ReadContext {
    /// A context with fresh statistics that admits its reads.
    pub fn new(cache: Arc<DecodedCache>) -> Self {
        Self {
            cache,
            stats: Arc::default(),
            backend: Arc::default(),
            admit: true,
            ann_rows: Arc::default(),
            document_scan_rows: Arc::default(),
        }
    }
}
tokio::task_local! {
    static CURRENT: ReadContext;
}
pub(crate) fn context() -> Option<ReadContext> {
    CURRENT.try_with(Clone::clone).ok()
}
pub(crate) async fn scope<T>(context: ReadContext, future: impl Future<Output = T>) -> T {
    let backend = context.backend.clone();
    backend
        .scope("search", CURRENT.scope(context, future))
        .await
}
/// False inside a context whose reads must not fill the block caches.
pub(crate) fn admitting() -> bool {
    CURRENT.try_with(|c| c.admit).unwrap_or(true)
}
pub(crate) fn stats() -> Arc<CacheStats> {
    CURRENT.try_with(|c| c.stats.clone()).unwrap_or_default()
}
pub(crate) fn get<T: Any + Send + Sync>(key: &str, logical_bytes: u64) -> Option<Arc<T>> {
    let context = context()?;
    let value = context.cache.get(key)?;
    context.stats.record_memory_hit(logical_bytes);
    Some(value)
}
pub(crate) fn insert<T: Any + Send + Sync>(key: &str, bytes: u64, value: T) -> Arc<T> {
    let value = Arc::new(value);
    if let Some(context) = context() {
        context.cache.insert(key, bytes, value.clone());
    }
    value
}

/// The current query's general decoded-cache budget (zero outside a query context).
pub(crate) fn capacity() -> u64 {
    context().map_or(0, |c| c.cache.budget)
}
/// Reserve a complete resident view atomically. Concurrent cold queries share one winner,
/// and the rejected candidate is dropped after releasing the cache lock.
pub(crate) fn insert_once<T: Any + Send + Sync>(key: &str, bytes: u64, value: T) -> Option<Arc<T>> {
    let context = context()?;
    if bytes > context.cache.budget || context.cache.budget == 0 {
        return None;
    }
    Some(context.cache.install(key, bytes, Arc::new(value), true))
}

/// A generation-fenced predicate, isolated from large dictionary admission in the general LRU.
pub(crate) fn predicate(key: &str) -> Option<Arc<bf_vector::RoaringBitmap>> {
    let context = context()?;
    let value = context.cache.predicate(key)?;
    context
        .stats
        .record_memory_hit(value.serialized_size() as u64);
    Some(value)
}
pub(crate) fn insert_predicate(
    key: &str,
    value: bf_vector::RoaringBitmap,
) -> Arc<bf_vector::RoaringBitmap> {
    let value = Arc::new(value);
    if let Some(context) = context() {
        context.cache.insert_predicate(key, value.clone());
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn atomic_resident_admission_shares_one_bounded_generation() {
        let cache = Arc::new(DecodedCache::new(192, 1));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8u64)
            .map(|i| {
                let (cache, barrier) = (cache.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    cache.install("generation", 64, Arc::new(i), true)
                })
            })
            .collect();
        let pinned: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(pinned.iter().all(|value| Arc::ptr_eq(value, &pinned[0])));
        assert_eq!(cache.entries.lock().unwrap().bytes, 64);
        assert_eq!(cache.entries.lock().unwrap().values.len(), 1);
        let other = cache
            .scoped("other")
            .install("generation", 64, Arc::new(99u64), true);
        assert_eq!(*other, 99);
        assert!(!Arc::ptr_eq(&other, &pinned[0]));
        cache.clear();
        assert!(cache.get::<u64>("generation").is_none());
        assert!(pinned.iter().all(|value| Arc::ptr_eq(value, &pinned[0])));
    }

    #[test]
    fn clock_initializes_once_across_threads_and_retries_errors() {
        let cache = Arc::new(ClockCache::<u64, u64>::new(64 * 1024, |_| 8));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let calls = calls.clone();
            threads.push(std::thread::spawn(move || {
                assert_eq!(
                    cache.get_or_try_insert(42, || {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok::<_, ()>(7)
                    }),
                    Ok(7)
                );
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            cache.get_or_try_insert(43, || Err::<u64, _>("retry")),
            Err("retry")
        );
        assert_eq!(cache.get(&43), None);
        assert_eq!(cache.get_or_try_insert(43, || Ok::<_, ()>(9)), Ok(9));
    }
    #[test]
    fn byte_budget_lru_type_separation_and_pinned_users() {
        let cache = DecodedCache::new(24, 1);
        let pinned = Arc::new(1u64);
        cache.insert("a", 4, pinned.clone());
        cache.insert("b", 4, Arc::new(2u64));
        assert_eq!(*cache.get::<u64>("a").unwrap(), 1);
        assert!(cache.get::<u32>("a").is_none());
        cache.insert("c", 4, Arc::new(3u64));
        assert!(cache.get::<u64>("b").is_none());
        cache.insert("oversize", 11, Arc::new(4u64));
        assert!(cache.get::<u64>("oversize").is_none());
        cache.insert("a", 8, Arc::new(5u64));
        assert!(cache.get::<u64>("c").is_none());
        assert_eq!(*pinned, 1);
        assert_eq!(cache.entries.lock().unwrap().bytes, 8);
    }

    #[test]
    fn clock_cache_bounds_bytes_and_gives_read_entries_a_second_chance() {
        // 64 shards of 100 bytes; every key below lands in one shard or another, so test the
        // invariants per shard through the public API: bytes stay bounded, and an entry read
        // since the last sweep survives the next eviction in its shard.
        let cache = ClockCache::<u64>::new(64 * 100, |_| 40);
        let key = |i: u32| (Arc::<str>::from("obj"), i);
        for i in 0..1000 {
            cache.insert(key(i), i as u64);
        }
        for shard in &cache.shards {
            let s = shard.lock().unwrap();
            assert!(s.bytes <= 100 && s.map.len() <= 2 && s.ring.len() == s.map.len());
        }
        // Find two keys in one shard, read the older, insert a third: the read one stays.
        let survivors: Vec<u32> = (0..1000)
            .filter(|i| cache.get(&key(*i)).is_some())
            .collect();
        let same = |a: u32, b: u32| std::ptr::eq(cache.shard(&key(a)), cache.shard(&key(b)));
        let (a, b) = survivors
            .iter()
            .flat_map(|&a| survivors.iter().map(move |&b| (a, b)))
            .find(|&(a, b)| a != b && same(a, b))
            .expect("a shard with two entries");
        let shard = cache.shard(&key(a));
        let older = *shard.lock().unwrap().ring.front().map(|(_, i)| i).unwrap();
        let newer = if older == a { b } else { a };
        for (_, _, hot) in shard.lock().unwrap().map.values_mut() {
            *hot = false;
        }
        assert_eq!(cache.get(&key(older)), Some(older as u64));
        let third = (1000..).find(|i| same(*i, a)).unwrap();
        cache.insert(key(third), 7);
        assert!(cache.get(&key(older)).is_some());
        assert!(cache.get(&key(newer)).is_none());
        assert_eq!(cache.get(&key(third)), Some(7));
        cache.clear();
        assert!(cache.get(&key(third)).is_none());
    }

    #[test]
    fn retired_metadata_drops_do_not_block_cache_hits() {
        use std::{sync::mpsc, thread, time::Duration};

        struct SlowDrop {
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }
        impl Drop for SlowDrop {
            fn drop(&mut self) {
                let _ = self.entered.send(());
                let _ = self.release.lock().unwrap().recv();
            }
        }

        let cache = Arc::new(DecodedCache::new(1, 1));
        let (entered, dropping) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        cache.insert(
            "old",
            1,
            Arc::new(SlowDrop {
                entered,
                release: Mutex::new(resume),
            }),
        );
        let writer = cache.clone();
        let writer = thread::spawn(move || writer.insert("new", 1, Arc::new(9u64)));
        dropping.recv_timeout(Duration::from_secs(5)).unwrap();
        let (done, result) = mpsc::channel();
        let reader = thread::spawn(move || {
            done.send(cache.get::<u64>("new").map(|value| *value))
                .unwrap();
        });
        let observed = result.recv_timeout(Duration::from_secs(5));
        // Release the destructor before asserting, so a regression cannot leave
        // either thread stuck while the test unwinds.
        release.send(()).unwrap();
        writer.join().unwrap();
        reader.join().unwrap();
        assert!(matches!(observed, Ok(Some(9))));
    }
}

#[cfg(test)]
mod predicate_tests {
    use super::*;
    #[test]
    fn columns_borrow_unused_predicate_bytes_and_return_them_under_pressure() {
        let pool = PredicateColumns::new(16_384, 4096);
        let column = Arc::new(vec![None; 100]);
        for i in 0..3 {
            pool.insert_column(format!("column-{i}"), column.clone());
        }
        // All three fit the shared pool, although they exceed its old fixed column portion.
        assert!((0..3).all(|i| pool.column(&format!("column-{i}")).is_some()));
        let predicate: Predicate = Arc::new((0..100).map(|i| i * 65_537).collect());
        pool.insert_predicate("predicate".into(), predicate.clone());
        assert_eq!(pool.predicate("predicate"), Some(predicate));
        {
            let entries = pool.entries.lock().unwrap();
            assert_eq!(entries.columns.map.len(), 2);
            assert!(entries.columns.bytes + entries.predicates.bytes <= pool.budget);
        }
        // Replacing a large predicate with a small one immediately returns its unused bytes.
        pool.insert_predicate("predicate".into(), Arc::new((1..5).collect()));
        pool.insert_column("new-column".into(), column.clone());
        assert_eq!(pool.entries.lock().unwrap().columns.map.len(), 3);
        pool.insert_predicate(
            "oversized".into(),
            Arc::new((0..1000).map(|i| i * 65_537).collect()),
        );
        assert!(pool.predicate("oversized").is_none());
        assert_eq!(pool.entries.lock().unwrap().columns.map.len(), 3);
        pool.clear();
        let entries = pool.entries.lock().unwrap();
        assert_eq!(entries.columns.cap, pool.budget);
        assert_eq!(entries.columns.bytes + entries.predicates.bytes, 0);
    }

    #[test]
    fn both_clock_and_hash_map_key_copies_are_charged_before_admission() {
        let pool = PredicateColumns::new(4096, 1024);
        let key = "key".repeat(200);
        pool.insert_predicate(key.clone(), Arc::new((0..1).collect()));
        assert!(pool.predicate(&key).is_none());
        pool.insert_column(key.clone(), Arc::new(vec![None; 64]));
        assert!(pool.column(&key).is_none());
    }

    #[test]
    fn concurrent_predicate_and_column_admission_obeys_the_shared_byte_limit() {
        let pool = Arc::new(PredicateColumns::new(65_536, 16_384));
        std::thread::scope(|scope| {
            for thread in 0..8 {
                let pool = pool.clone();
                scope.spawn(move || {
                    for i in 0..100 {
                        let key = format!("{thread}-{i}");
                        pool.insert_column(key.clone(), Arc::new(vec![None; 100]));
                        pool.insert_predicate(key.clone(), Arc::new((0..100).collect()));
                        let _ = (pool.column(&key), pool.predicate(&key));
                        let entries = pool.entries.lock().unwrap();
                        assert!(entries.columns.bytes + entries.predicates.bytes <= pool.budget);
                        assert!(entries.predicates.bytes <= pool.protected);
                    }
                });
            }
        });
    }

    #[test]
    fn predicate_budget_survives_dictionary_churn_and_obeys_scope_and_clear() {
        let cache = DecodedCache::new(48 * 64 * 1024, 1);
        let rows = Arc::new((0..100).collect::<bf_vector::RoaringBitmap>());
        cache.insert_predicate("generation", rows.clone());
        for i in 0..4 {
            cache.insert(&format!("dictionary-{i}"), cache.budget, Arc::new(i));
        }
        assert_eq!(cache.predicate("generation").unwrap(), rows);
        let other = cache.scoped("other");
        assert!(other.predicate("generation").is_none());
        other.insert_predicate("generation", Arc::new((200..300).collect()));
        assert_ne!(other.predicate("generation").unwrap(), rows);
        let oversized = Arc::new((0..10_000).map(|r| r * 65_537).collect());
        cache.insert_predicate("oversized", oversized);
        assert!(cache.predicate("oversized").is_none());
        assert!(cache.columns.entries.lock().unwrap().predicates.bytes <= cache.columns.protected);
        cache.clear();
        assert!(cache.predicate("generation").is_none());
        assert!(other.predicate("generation").is_none());
        assert_eq!(rows.len(), 100);
    }
}
