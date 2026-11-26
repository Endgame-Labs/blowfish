//! A generation-scoped compiled tier, populated entirely by ordinary query reads.
use crate::read_cache;
use bf_vector::{CompiledTier, Directory, ResidentQueryScratch, ValidatedVector, VectorRecorder};
use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU32, AtomicUsize, Ordering},
    },
};

/// Counter and gauge handles for one tier's exact-vector cache, resolved once so the hot path
/// pays an atomic add, never a registry lookup.
struct Stats {
    hit: metrics::Counter,
    miss: metrics::Counter,
    inserted: metrics::Counter,
    evicted: metrics::Counter,
    decline_not_ready: metrics::Counter,
    decline_leaf: metrics::Counter,
    decline_exact_row: metrics::Counter,
    entries: metrics::Gauge,
    bytes: metrics::Gauge,
}
impl Stats {
    fn new(capacity: u64) -> Self {
        metrics::gauge!("blowfish_warm_exact_capacity_bytes").set(capacity as f64);
        Self {
            hit: metrics::counter!("blowfish_warm_exact_lookups_total", "result" => "hit"),
            miss: metrics::counter!("blowfish_warm_exact_lookups_total", "result" => "miss"),
            inserted: metrics::counter!("blowfish_warm_exact_inserted_total"),
            evicted: metrics::counter!("blowfish_warm_exact_evicted_total"),
            decline_not_ready: metrics::counter!("blowfish_warm_declined_total", "reason" => "not_ready"),
            decline_leaf: metrics::counter!("blowfish_warm_declined_total", "reason" => "leaf_missing"),
            decline_exact_row: metrics::counter!("blowfish_warm_declined_total", "reason" => "exact_row_missing"),
            entries: metrics::gauge!("blowfish_warm_exact_entries"),
            bytes: metrics::gauge!("blowfish_warm_exact_bytes"),
        }
    }
}

/// A query is a bulk reader when it asks for at least this many times the results that the
/// tier's recent queries typically ask for.
const BULK_MEDIAN_MULTIPLE: usize = 4;
/// A query is also a bulk reader when its shortlist (about `k * SHORTLIST_FACTOR` exact rows)
/// would fill more than one part in this many of the exact-vector budget.
const BULK_SHARE_DENOMINATOR: u64 = 512;
/// Rows reranked per requested result, as a planning estimate for the share rule.
const SHORTLIST_FACTOR: u64 = 8;
/// Recent `k` values kept per tier for the running median.
const RECENT_K: usize = 64;

/// The exact-vector cache of a sparse tier: one byte budget, two classes of reader.
///
/// Ordinary queries record into `vectors` and may take any of the budget. Bulk queries (see
/// [`WarmTier::recorder`]) record into `bulk` only while the two together are under the budget,
/// so a pass of large queries fills free space and never evicts what ordinary queries keep
/// resident; when an ordinary insert needs room, it is taken from `bulk` first. Either class
/// therefore uses the whole budget when it is the only one, as a single cache would, and the
/// reservation is the same as for one cache.
pub(crate) struct WarmTier {
    pub codes: CompiledTier,
    vectors: moka::sync::Cache<u32, ValidatedVector>,
    /// `None` when every row fits, where one cache serves both kinds of query.
    bulk: Option<Bulk>,
    stats: Arc<Stats>,
    ready: AtomicBool,
    pub rows: usize,
    #[cfg(test)]
    hits: std::sync::atomic::AtomicUsize,
}
struct Bulk {
    cache: moka::sync::Cache<u32, ValidatedVector>,
    /// Weighted bytes resident in each cache, kept from inserts and the eviction listeners.
    ordinary_bytes: Arc<AtomicI64>,
    bulk_bytes: Arc<AtomicI64>,
    budget: i64,
    /// Average bytes of one cache entry, for the share rule.
    entry_bytes: u64,
    recent: [AtomicU32; RECENT_K],
    seq: AtomicUsize,
    /// One thread at a time frees bulk rows.
    trimming: AtomicBool,
}
pub(crate) struct Found {
    pub winners: Vec<(f32, usize, u32)>,
    pub scanned_rows: usize,
}
struct Seen(AtomicU8);
thread_local! {
    static SCRATCH: RefCell<ResidentQueryScratch> = RefCell::new(ResidentQueryScratch::default());
}
/// Only a bounded, unfiltered, already compiled ANN tier may spend CPU on an HTTP reactor.
/// The normal planner still proves masks, coverage, and exact-vector residency; a miss yields
/// its existing future to shared I/O, and unsupported plans retain the fixed worker pool.
pub(crate) fn inline_plan(
    cache: &read_cache::DecodedCache,
    state: &crate::NamespaceState,
    request: &bf_types::QueryRequest,
    config: &crate::EngineConfig,
    max_rows: usize,
) -> bool {
    let Some(bf_types::RankBy::Vector {
        attribute,
        exact: false,
        vector,
        ..
    }) = &request.rank_by
    else {
        return false;
    };
    if vector.len() > 1024
        || config.ann_nprobe > 64
        || config.ann_rerank_factor > 32
        || request.compute_attributes.is_some()
        || request.limit.is_some()
        || request.filters.is_some()
        || request.queries.is_some()
        || request.aggregate_by.is_some()
        || request.group_by.is_some()
        || request.rerank_by.is_some()
        || request.total().unwrap_or(10).saturating_add(request.offset) > max_rows
        || state.tail_bytes != 0
        || !state.docs.is_empty()
        || state
            .segments
            .iter()
            .any(|segment| segment.live_generation != 0)
    {
        return false;
    }
    let Some(tiers) = state.ann_tiers.get(attribute) else {
        return false;
    };
    tiers.len() == 1
        && cache
            .get::<WarmTier>(&tiers[0].dir.key)
            .is_some_and(|warm| {
                warm.ready.load(Ordering::Acquire)
                    && warm.rows as u64
                        == state
                            .segments
                            .iter()
                            .map(|segment| u64::from(segment.row_count))
                            .sum::<u64>()
            })
}

impl WarmTier {
    /// A first query uses only the normal cold path. A second query can reserve a compiled
    /// view if its complete code working set and exact-vector cache fit the decoded budget.
    pub fn for_tier(key: &str, directory: Arc<Directory>) -> Option<Arc<Self>> {
        if let Some(warm) = read_cache::get::<Self>(key, 0) {
            return Some(warm);
        }
        // Eligibility can race eviction. Rebuilding metadata for a large tier belongs
        // to shared I/O; a reactor miss reaches the planner's existing yield instead.
        if crate::query_worker::is_reactor() {
            return None;
        }
        if read_cache::capacity() == 0 {
            return None;
        }
        let seen = read_cache::get::<Seen>(key, 0)
            .or_else(|| read_cache::insert_once(key, 64, Seen(AtomicU8::new(0))))?;
        if seen.0.load(Ordering::Relaxed) == 2
            || seen
                .0
                .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            return None;
        }
        // Paged directories list a query-specific subset of leaves, so they retain their
        // normal reader. A compiled view always describes the complete immutable tier.
        let listed: usize = directory.entries().iter().map(|e| e.rows as usize).sum();
        let total: usize = directory.segment_totals().iter().map(|(n, _)| n).sum();
        let code_reservation = CompiledTier::reservation(&directory);
        // ValidatedVector::resident_bytes charges its payload, descriptor, and 128 bytes
        // per cache entry. Physical vector ranges conservatively cover every row payload.
        let all_vectors = directory.entries().iter().fold(0u64, |bytes, entry| {
            let range = entry.vectors_range();
            bytes
                .saturating_add(range.end - range.start)
                .saturating_add(
                    u64::from(entry.rows)
                        .saturating_mul((std::mem::size_of::<ValidatedVector>() + 128) as u64),
                )
        });
        // A code-sized sparse cache can thrash even on a fully memory-resident small
        // namespace. Retain every validated row when the entire tier fits the budget;
        // large tiers keep the bounded sparse policy and ordinary miss fallback.
        let vector_budget =
            if code_reservation.saturating_add(all_vectors) <= read_cache::capacity() {
                all_vectors.max(CompiledTier::code_bytes(&directory))
            } else {
                CompiledTier::code_bytes(&directory)
            };
        let sparse = vector_budget < all_vectors;
        let reservation = code_reservation.saturating_add(vector_budget);
        if listed != total || total <= 2048 || reservation > read_cache::capacity() {
            seen.0.store(2, Ordering::Relaxed);
            return None;
        }
        let codes = match CompiledTier::new(directory) {
            Ok(codes) => codes,
            Err(_) => {
                seen.0.store(2, Ordering::Relaxed);
                return None;
            }
        };
        let stats = Arc::new(Stats::new(vector_budget));
        let (ordinary_bytes, bulk_bytes) =
            (Arc::new(AtomicI64::new(0)), Arc::new(AtomicI64::new(0)));
        let cache = |resident: Arc<AtomicI64>| {
            let (evicted, gone) = (stats.evicted.clone(), resident.clone());
            moka::sync::Cache::builder()
                .max_capacity(vector_budget)
                .weigher(|_: &u32, v: &ValidatedVector| weight(v))
                .eviction_listener(move |_, v: ValidatedVector, cause| {
                    gone.fetch_sub(i64::from(weight(&v)), Ordering::Relaxed);
                    if matches!(cause, moka::notification::RemovalCause::Size) {
                        evicted.increment(1);
                    }
                })
                .build()
        };
        let vectors = cache(ordinary_bytes.clone());
        let bulk = sparse.then(|| Bulk {
            cache: cache(bulk_bytes.clone()),
            ordinary_bytes,
            bulk_bytes,
            budget: vector_budget.min(i64::MAX as u64) as i64,
            entry_bytes: (all_vectors / total.max(1) as u64).max(1),
            recent: std::array::from_fn(|_| AtomicU32::new(0)),
            seq: AtomicUsize::new(0),
            trimming: AtomicBool::new(false),
        });
        read_cache::insert_once(
            key,
            reservation,
            Self {
                codes,
                vectors,
                bulk,
                stats,
                ready: AtomicBool::new(false),
                rows: total,
                #[cfg(test)]
                hits: std::sync::atomic::AtomicUsize::new(0),
            },
        )
    }

    /// The complete scoring pipeline runs on the query's compute worker. Scratch is released
    /// before hydration can yield; in-flight queries retain this generation's immutable Arc.
    pub fn search(
        &self,
        vector: &[f32],
        nprobe: usize,
        rerank_factor: usize,
        k: usize,
    ) -> bf_vector::Result<Option<Found>> {
        if !self.ready.load(Ordering::Acquire) {
            self.stats.decline_not_ready.increment(1);
            return Ok(None);
        }
        SCRATCH.with(|scratch| {
            let mut scratch = scratch.borrow_mut();
            let (hit, miss) = (std::cell::Cell::new(0u64), std::cell::Cell::new(0u64));
            let found = self.codes.try_search(
                vector,
                nprobe,
                rerank_factor,
                k,
                |row| {
                    let found = self.get(row);
                    if found.is_some() {
                        hit.set(hit.get() + 1);
                    } else {
                        miss.set(miss.get() + 1);
                    }
                    found
                },
                &mut scratch,
            )?;
            let (hit, miss) = (hit.get(), miss.get());
            self.stats.hit.increment(hit);
            self.stats.miss.increment(miss);
            let bulk = self.bulk.iter().map(|b| &b.cache);
            let (entries, bytes) = std::iter::once(&self.vectors)
                .chain(bulk)
                .fold((0, 0), |(n, b), c| {
                    (n + c.entry_count(), b + c.weighted_size())
                });
            self.stats.entries.set(entries as f64);
            self.stats.bytes.set(bytes as f64);
            Ok(match found {
                Some(found) => {
                    #[cfg(test)]
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    Some(Found {
                        winners: found.hits.to_vec(),
                        scanned_rows: found.scanned_rows,
                    })
                }
                None => {
                    // The exact lookup is the only step that counts misses, and it ends the
                    // search at the first one.
                    if miss > 0 {
                        self.stats.decline_exact_row.increment(1);
                    } else {
                        self.stats.decline_leaf.increment(1);
                    }
                    None
                }
            })
        })
    }
}
fn weight(v: &ValidatedVector) -> u32 {
    v.resident_bytes().min(u64::from(u32::MAX)) as u32
}
impl Bulk {
    /// Notes `k` among the recent queries and says whether this one is a bulk reader: it asks
    /// for far more than the tier's recent queries typically do, or its shortlist is a large
    /// share of the budget. A workload that is uniform at any `k` below the share rule is
    /// never bulk.
    fn classify(&self, k: usize) -> bool {
        let slot = self.seq.fetch_add(1, Ordering::Relaxed) % RECENT_K;
        self.recent[slot].store(k.min(u32::MAX as usize) as u32, Ordering::Relaxed);
        let mut seen: Vec<u32> = self
            .recent
            .iter()
            .map(|k| k.load(Ordering::Relaxed))
            .filter(|&k| k > 0)
            .collect();
        let median = if seen.is_empty() {
            k
        } else {
            let mid = seen.len() / 2;
            *seen.select_nth_unstable(mid).1 as usize
        };
        let shortlist = (k as u64).saturating_mul(SHORTLIST_FACTOR);
        k >= median.saturating_mul(BULK_MEDIAN_MULTIPLE).max(2)
            || shortlist.saturating_mul(self.entry_bytes)
                > self.budget as u64 / BULK_SHARE_DENOMINATOR
    }
    fn used(&self) -> i64 {
        self.ordinary_bytes.load(Ordering::Relaxed) + self.bulk_bytes.load(Ordering::Relaxed)
    }
    /// Frees bulk rows until the two caches fit the budget again, with a little slack so that
    /// the next ordinary inserts do not each pay for a pass.
    fn trim(&self) {
        if self.used() <= self.budget
            || self
                .trimming
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        // The listeners settle the counters when the cache next runs its maintenance, so
        // count what this pass frees locally and run that maintenance once at the end.
        let mut need = self.used() - (self.budget - self.budget / 64);
        for (row, v) in self.cache.iter() {
            if need <= 0 {
                break;
            }
            self.cache.invalidate(&*row);
            need -= i64::from(weight(&v));
        }
        self.cache.run_pending_tasks();
        self.trimming.store(false, Ordering::Release);
    }
}
impl WarmTier {
    fn get(&self, row: u32) -> Option<ValidatedVector> {
        self.vectors
            .get(&row)
            .or_else(|| self.bulk.as_ref()?.cache.get(&row))
    }
    /// The recorder for a query that asked for `k` results. Rows from a bulk reader (see
    /// [`WarmTier`]) go to the bulk cache, which only takes free budget.
    pub fn recorder(&self, k: usize) -> Recorder<'_> {
        Recorder {
            tier: self,
            bulk: self.bulk.as_ref().is_some_and(|b| b.classify(k)),
        }
    }
}
/// Records one query's validated exact rows into its tier. See [`WarmTier::recorder`].
pub(crate) struct Recorder<'a> {
    tier: &'a WarmTier,
    bulk: bool,
}
impl VectorRecorder for Recorder<'_> {
    fn record(&self, vector: ValidatedVector) {
        let tier = self.tier;
        match tier.bulk.as_ref() {
            Some(b) if self.bulk => {
                if b.used() + i64::from(weight(&vector)) <= b.budget {
                    b.bulk_bytes
                        .fetch_add(i64::from(weight(&vector)), Ordering::Relaxed);
                    tier.stats.inserted.increment(1);
                    b.cache.insert(vector.row(), vector);
                }
            }
            other => {
                tier.stats.inserted.increment(1);
                if let Some(b) = other {
                    b.ordinary_bytes
                        .fetch_add(i64::from(weight(&vector)), Ordering::Relaxed);
                }
                tier.vectors.insert(vector.row(), vector);
                if let Some(b) = other {
                    b.trim();
                }
            }
        }
        tier.ready.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bulk_state(budget: i64, entry_bytes: u64) -> Bulk {
        let cache = || moka::sync::Cache::builder().build();
        Bulk {
            cache: cache(),
            ordinary_bytes: Arc::new(AtomicI64::new(0)),
            bulk_bytes: Arc::new(AtomicI64::new(0)),
            budget,
            entry_bytes,
            recent: std::array::from_fn(|_| AtomicU32::new(0)),
            seq: AtomicUsize::new(0),
            trimming: AtomicBool::new(false),
        }
    }

    #[test]
    fn a_uniform_workload_at_any_k_is_never_bulk() {
        for k in [1, 10, 20, 40] {
            let state = bulk_state(400 << 20, 2200);
            assert!((0..200).all(|_| !state.classify(k)), "k = {k}");
        }
    }

    #[test]
    fn a_pass_of_very_large_queries_is_bulk_by_share_alone() {
        // 100 results is a 800-row shortlist, 1.7 MB of a 400 MB budget.
        let state = bulk_state(400 << 20, 2200);
        assert!((0..50).all(|_| state.classify(100)));
    }

    #[test]
    fn a_rare_large_query_in_an_ordinary_stream_is_bulk() {
        // A budget so large that the share rule never fires leaves the median rule alone.
        let state = bulk_state(i64::MAX, 1);
        let bulk: Vec<bool> = (0..100)
            .map(|i| state.classify(if i % 10 == 9 { 100 } else { 10 }))
            .collect();
        assert!(
            bulk.iter()
                .enumerate()
                .all(|(i, &b)| b == (i % 10 == 9 && i > 0))
        );
    }

    use crate::{Engine, EngineConfig, ReclusterOptions};
    use bf_storage::{Storage, StorageConfig};
    use bf_types::{Document, QueryRequest, WriteRequest};
    use serde_json::{Value, json};

    fn vector(id: usize) -> Vec<f32> {
        (0..32)
            .map(|d| {
                if id.is_multiple_of(200) {
                    0.
                } else {
                    ((id * 7919 + d * 104729) as f64).sin() as f32
                }
            })
            .collect()
    }
    fn engine(store: &Storage, decoded_cache_bytes: u64) -> Engine {
        Engine::with_config(
            store.clone(),
            EngineConfig {
                indexing: false,
                recall_sample_rate: 0.,
                decoded_cache_bytes,
                ann_nprobe: 8,
                ..Default::default()
            },
        )
    }
    async fn query(engine: &Engine, body: Value) -> Vec<Document> {
        // Keep the combined residency and eviction fixture's debug future off the test stack.
        Box::pin(engine.query(
            "compiled",
            serde_json::from_value::<QueryRequest>(body).unwrap(),
        ))
        .await
        .unwrap()
        .result
        .rows
        .unwrap()
    }
    async fn current(engine: &Engine, key: &str) -> Option<Arc<WarmTier>> {
        read_cache::scope(
            read_cache::ReadContext::new(engine.0.read_cache.clone()),
            async { read_cache::get(key, 0) },
        )
        .await
    }

    async fn reactor_miss_never_rebuilds(key: String, directory: Arc<Directory>) {
        let cache = Arc::new(read_cache::DecodedCache::new(64 << 20, 2));
        let context = read_cache::ReadContext::new(cache.clone());
        let shared = tokio::runtime::Handle::current();
        let (reactor_key, reactor_directory) = (key.clone(), directory.clone());
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(crate::on_query_reactor(
                    shared,
                    read_cache::scope(context, async move {
                        // Model eviction of the large tier while its small admission marker stays.
                        read_cache::insert_once(&reactor_key, 64, Seen(AtomicU8::new(1))).unwrap();
                        assert!(WarmTier::for_tier(&reactor_key, reactor_directory).is_none());
                        assert!(read_cache::get::<WarmTier>(&reactor_key, 0).is_none());
                    }),
                ));
        })
        .await
        .unwrap();
        read_cache::scope(read_cache::ReadContext::new(cache), async move {
            // The shared path can still rebuild that same admissible generation.
            assert!(WarmTier::for_tier(&key, directory).is_some());
        })
        .await;
    }

    // Keep the build future on the heap so the driver has room for compaction and queries.
    fn fixture(
        cluster_rows: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = (Storage, Engine)>>> {
        Box::pin(async move {
            let store = Storage::open(&StorageConfig {
                url: "memory://".into(),
                ..Default::default()
            })
            .await
            .unwrap();
            let writer = engine(&store, 64 << 20);
            for batch in 0..2 {
                let docs: Vec<_> = (batch * 3000..(batch + 1) * 3000)
                    .map(|id| {
                        json!({
                            "id":format!("row-{:05}", 6000-id), "vector":vector(id), "tag":id % 4,
                        })
                    })
                    .collect();
                writer.write("compiled", serde_json::from_value::<WriteRequest>(json!({
                "distance_metric":"cosine_distance", "schema":{"vector":{"type":"[32]f16"}}, "upsert_rows":docs,
            })).unwrap()).await.unwrap();
                writer.index_namespace("compiled", true).await.unwrap();
            }
            let scratch = tempfile::tempdir().unwrap();
            writer
                .recluster(
                    "compiled",
                    &ReclusterOptions {
                        attribute: "vector".into(),
                        cells: 1,
                        cluster_rows,
                        max_slab_rows: 10000,
                        scratch: scratch.path().to_owned(),
                    },
                )
                .await
                .unwrap();
            (store, writer)
        })
    }

    #[test]
    fn compiled_queries_preserve_snapshots_masks_projections_and_cache_eviction() {
        // The synthetic write, build, and compaction futures share one test driver. Keep
        // their stack separate from the runtime workers, which retain their normal stacks.
        std::thread::Builder::new()
            .stack_size(8 << 20)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        for cluster_rows in [60, 640] {
                            compiled_case(cluster_rows).await;
                        }
                    });
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn compiled_case(cluster_rows: usize) {
        let (store, writer) = fixture(cluster_rows).await;
        let wal = bf_wal::Wal::new(store.clone(), "compiled").unwrap();
        let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
        let key = &manifest.ann_tiers["vector"][0].dir.key;
        let normal = engine(&store, 0);
        let warm = engine(&store, 64 << 20);
        let request = |id| json!({"rank_by":["vector","ANN",vector(id)],"top_k":10,"include_attributes":["id","tag"]});
        let first = query(&warm, request(17)).await;
        assert_eq!(first, query(&normal, request(17)).await);
        assert!(
            current(&warm, key).await.is_none(),
            "a first cold query must not reserve a tier"
        );
        for id in [17, 2111, 5999, 0] {
            let want = query(&normal, request(id)).await;
            for _ in 0..4 {
                assert_eq!(query(&warm, request(id)).await, want);
            }
        }
        let compiled = current(&warm, key).await.expect("hot tier admitted");
        let state = warm.handle("compiled").await.unwrap().state.load();
        let mut eligible: QueryRequest = serde_json::from_value(request(17)).unwrap();
        assert!(inline_plan(
            &warm.0.read_cache,
            &state,
            &eligible,
            &warm.0.config,
            10
        ));
        eligible.top_k = Some(11);
        assert!(!inline_plan(
            &warm.0.read_cache,
            &state,
            &eligible,
            &warm.0.config,
            10
        ));
        eligible.top_k = Some(10);
        let mut expensive = warm.0.config.clone();
        expensive.ann_nprobe = 65;
        assert!(!inline_plan(
            &warm.0.read_cache,
            &state,
            &eligible,
            &expensive,
            10
        ));
        eligible.filters = Some(serde_json::from_value(json!(["tag", "Eq", 0])).unwrap());
        assert!(!inline_plan(
            &warm.0.read_cache,
            &state,
            &eligible,
            &warm.0.config,
            10
        ));
        assert!(
            compiled.hits.load(Ordering::Relaxed) > 0,
            "exercise the real compiled branch"
        );
        reactor_miss_never_rebuilds(
            key.clone(),
            crate::ann_dir::load(&store, &manifest.ann_tiers["vector"][0].dir, None)
                .await
                .unwrap(),
        )
        .await;
        // Large enough for the rejection marker in the general pool, far too small for the tier.
        let tiny = engine(&store, 256);
        for _ in 0..3 {
            assert_eq!(
                query(&tiny, request(2111)).await,
                query(&normal, request(2111)).await
            );
        }
        assert!(
            current(&tiny, key).await.is_none(),
            "oversized tiers are never retained"
        );
        let rejected = read_cache::scope(
            read_cache::ReadContext::new(tiny.0.read_cache.clone()),
            async { read_cache::get::<Seen>(key, 0).expect("rejection marker retained") },
        )
        .await;
        assert_eq!(rejected.0.load(Ordering::Relaxed), 2);
        for _ in 0..8 {
            assert_eq!(
                query(&tiny, request(2111)).await,
                query(&normal, request(2111)).await
            );
            let marker = read_cache::scope(
                read_cache::ReadContext::new(tiny.0.read_cache.clone()),
                async { read_cache::get::<Seen>(key, 0).expect("rejection marker retained") },
            )
            .await;
            assert!(Arc::ptr_eq(&rejected, &marker));
            assert_eq!(marker.0.load(Ordering::Relaxed), 2);
            assert!(current(&tiny, key).await.is_none());
        }
        // A varied small working set must stay compiled when all exact rows fit. A
        // code-sized vector cache loses these rows between queries, despite ample budget.
        // Keep zero-vector/all-tie queries in the correctness checks above: their
        // deliberately oversized winner set uses the ordinary path, even when resident.
        let requests: Vec<_> = (1..=32).map(|i| request(i * 193 % 6000)).collect();
        for body in &requests {
            assert_eq!(
                query(&warm, body.clone()).await,
                query(&normal, body.clone()).await
            );
        }
        compiled.vectors.run_pending_tasks();
        let before = compiled.hits.load(Ordering::Relaxed);
        for body in requests {
            query(&warm, body).await;
        }
        assert_eq!(
            compiled.hits.load(Ordering::Relaxed) - before,
            32,
            "a resident exact working set must not fall back after warming"
        );
        // Pure compaction changes placement, but leaves every tier row live. Reuse the
        // compiled generation through lineage, including after caching its full-mask proof.
        let merger = Engine::with_config(
            store.clone(),
            EngineConfig {
                indexing: false,
                recall_sample_rate: 0.,
                compaction_segments: 2,
                max_segment_rows: 10_000,
                ..Default::default()
            },
        );
        assert!(
            Box::pin(merger.index_namespace("compiled", true))
                .await
                .unwrap()
        );
        let (_, merged) = wal.latest_manifest().await.unwrap().unwrap();
        assert!(!merged.lineage.is_empty());
        assert_eq!(merged.ann_tiers["vector"][0].dir.key, *key);
        warm.0.namespaces.lock().await.clear();
        normal.0.namespaces.lock().await.clear();
        let before_merge = compiled.hits.load(Ordering::Relaxed);
        for id in [17, 2111, 5999, 17] {
            assert_eq!(
                query(&warm, request(id)).await,
                query(&normal, request(id)).await
            );
        }
        assert_eq!(
            compiled.hits.load(Ordering::Relaxed) - before_merge,
            4,
            "full live masks must retain the compiled tier after compaction"
        );
        // A concurrent reader cannot return an old speculative result over an unindexed write.
        writer.write("compiled", serde_json::from_value::<WriteRequest>(json!({
            "deletes":["row-05983"], "upsert_rows":[{"id":"new","vector":vector(17),"tag":1}],
        })).unwrap()).await.unwrap();
        let got = query(&warm, request(17)).await;
        assert_eq!(got, query(&normal, request(17)).await);
        assert!(got.iter().all(|row| row["id"] != "row-05983"));
        assert!(got.iter().any(|row| row["id"] == "new"));
        // Indexed masks and uncovered segments use the existing query plan.
        writer.index_namespace("compiled", true).await.unwrap();
        warm.0.namespaces.lock().await.clear();
        normal.0.namespaces.lock().await.clear();
        let before_mask = compiled.hits.load(Ordering::Relaxed);
        assert_eq!(
            query(&warm, request(17)).await,
            query(&normal, request(17)).await
        );
        assert_eq!(
            compiled.hits.load(Ordering::Relaxed),
            before_mask,
            "masked tier bypasses the fully live compiled plan"
        );
        let filtered = json!({"rank_by":["vector","ANN",vector(17)],"top_k":20,"filters":["tag","Eq",1],"include_attributes":["tag"]});
        assert_eq!(
            query(&warm, filtered.clone()).await,
            query(&normal, filtered).await
        );
        let large = json!({"rank_by":["vector","ANN",vector(0)],"top_k":1000});
        assert_eq!(
            query(&warm, large.clone()).await,
            query(&normal, large).await
        );
        warm.0.read_cache.clear();
        assert!(current(&warm, key).await.is_none());
        assert_eq!(
            query(&warm, request(17)).await,
            query(&normal, request(17)).await
        );
    }
}

#[cfg(test)]
#[path = "compiled_benchmark.rs"]
mod benchmark;
