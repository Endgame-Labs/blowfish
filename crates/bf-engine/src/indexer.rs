//! Epoch-fenced index publication, tiered compaction, and grace-period reclamation.
use crate::{Engine, NamespaceState, catch_up, log_error, segment, snapshot, storage_error};
use bf_types::ApiError;
use bf_vector::RoaringBitmap;
use bf_wal::{IndexerLease, Manifest, NamespaceStats, RetiredObject, Wal};
use chrono::Utc;
use std::{collections::BTreeMap, time::Duration};

/// Node-local indexing and query tuning. No option changes the durable write ordering.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Byte budget for decoded immutable metadata and resident WAL vectors (charged at resident size).
    /// Zero disables admission. In-flight queries can pin evicted entries until completion.
    pub decoded_cache_bytes: u64,
    /// Maximum concurrent segment scoring tasks across all queries on this engine.
    pub search_concurrency: usize,
    /// Maximum admitted query drivers, including plans waiting on I/O or a CPU worker.
    pub query_concurrency: usize,
    /// Disable indexing to keep a WAL-only differential reference node.
    pub indexing: bool,
    /// Minimum distinct tail mutations before a background flush.
    pub index_rows: usize,
    /// Minimum logical WAL bytes before a background flush.
    pub index_bytes: u64,
    /// Flush small tails after this age, and estimate the rate of arriving writes within it.
    pub index_age: Duration,
    /// While writes are still arriving, keep accumulating the tail up to this many distinct
    /// rows (or `bulk_flush_age`) before building a segment, so a bulk load builds a few large
    /// segments instead of many small ones that compaction would then rewrite. Ignored when
    /// `index_age` is under a second.
    pub bulk_flush_rows: usize,
    /// Longest a tail keeps accumulating under `bulk_flush_rows`.
    pub bulk_flush_age: Duration,
    /// Rows a second a tail must arrive at for its flush to count as a bulk load's (see
    /// `BULK_LOAD_ROWS_PER_SEC`; tests that load on a busy machine lower it).
    pub bulk_load_rows_per_sec: f64,
    /// Logical tail bytes at which a bulk load stops accumulating and flushes (`BF_BULK_FLUSH_BYTES`).
    /// Under `max_unindexed_bytes`, so writes keep arriving while the flush builds.
    pub bulk_flush_bytes: u64,
    /// While writes are arriving, compaction waits until a namespace holds this many segments
    /// (`BF_DEFER_COMPACTION_SEGMENTS`). Each row is then built once during a load instead of about
    /// twice, and the merges run when the writes pause.
    pub defer_compaction_segments: usize,
    /// Background poll interval.
    pub index_interval: Duration,
    /// Merge this many segments in one logarithmic size tier.
    pub compaction_segments: usize,
    /// Maximum rows in a newly built segment (a single document is indivisible).
    pub max_segment_rows: usize,
    /// Maximum source-document bytes per flush segment, and persisted input bytes
    /// per compaction. Index overhead can exceed this source budget.
    pub max_segment_bytes: u64,
    /// A retired object remains readable for at least this long.
    pub retention: Duration,
    /// Advisory lease duration; correctness relies solely on manifest fencing.
    pub lease_duration: Duration,
    /// Reject writes once unindexed logical WAL bytes reach this limit.
    pub max_unindexed_bytes: u64,
    /// WAL groups whose payloads upload concurrently before their slots commit in order
    /// (1 disables pipelining). `BF_WAL_PIPELINE`.
    pub wal_pipeline: usize,
    /// Eventual reads include at most this many logical WAL bytes.
    pub eventual_tail_bytes: u64,
    /// Segments built concurrently when one flush or compaction yields several. Up to two
    /// overwritten-row statistics scans share this limit and the existing aggregate row bound.
    pub build_concurrency: usize,
    /// IVF leaf probes for ANN queries.
    pub ann_nprobe: usize,
    /// Whether `ann_nprobe` was set by the operator (flag, TOML, or environment). An explicit
    /// probe count is used as given; the default one is scaled up for independently clustered
    /// segments (see `ann_dir::independent_spread`).
    pub ann_nprobe_explicit: bool,
    /// Exact rerank budget as a multiple of the requested k.
    pub ann_rerank_factor: usize,
    /// A filtered ANN query with at most this many allowed rows fetches and ranks exactly
    /// those rows' vectors instead of scanning quantized codes.
    pub exact_fetch_rows: usize,
    /// Fixed legacy behavior used only by the internal recall regression test.
    #[cfg(test)]
    pub(crate) legacy_three_filter_test: bool,
    /// Frozen inverse-selectivity planner, used only by per-query recall comparisons.
    #[cfg(test)]
    pub(crate) baseline_filter_test: bool,
    /// Fraction of ANN requests checked against exhaustive ground truth.
    pub recall_sample_rate: f64,
    /// Least time between two sampled recall comparisons (and only one runs at a time).
    /// `BF_RECALL_SAMPLE_INTERVAL_MS`.
    pub recall_sample_interval: Duration,
    /// Embedding and reranking models. Without them, `Embed` and model `rerank_by` return a 400.
    pub models: Option<std::sync::Arc<bf_models::Models>>,
}
fn memory_limit_bytes() -> u64 {
    bf_alloc::memory_limit_bytes()
}
/// A quarter of the memory this process may use, within 512 MiB to 4 GiB. Entries are charged at
/// their resident size, so this bounds real memory.
fn default_decoded_cache_bytes() -> u64 {
    (memory_limit_bytes() / 4).clamp(512 << 20, 4 << 30)
}
/// Rows in one built or compacted segment that a machine of this size can hold in memory. A
/// compaction of 1,024-dimension vectors needs about 30 KB per row while it runs (measured with the
/// jemalloc heap profiler), so the cap is memory / 80,000 rows within 25,000 to 200,000.
fn default_max_segment_rows() -> usize {
    ((memory_limit_bytes() / 80_000) as usize).clamp(25_000, 200_000)
}
/// Backpressure by bytes: an eighth of memory (448 MiB on a 3.5 GiB limit), within 128 MiB to 2 GB. A
/// bulk flush then builds segments as large as `max_segment_rows` allows instead of a trickle of
/// small ones.
fn default_max_unindexed_bytes() -> u64 {
    (memory_limit_bytes() / 8).clamp(128 << 20, 2_000_000_000)
}
/// A bulk load flushes at three quarters of the unindexed limit.
fn bulk_flush_bytes_for(max_unindexed: u64) -> u64 {
    max_unindexed / 4 * 3
}
impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            decoded_cache_bytes: default_decoded_cache_bytes(),
            search_concurrency: 8,
            // Queue another core's worth of work while drivers await I/O or HTTP progress.
            // Keep the existing minimum: one permit per core reduced four-core throughput.
            query_concurrency: std::thread::available_parallelism()
                .map_or(8, |n| n.get().saturating_mul(2).max(8)),
            indexing: true,
            index_rows: 1000,
            index_bytes: 8 * 1024 * 1024,
            index_age: Duration::from_secs(5),
            bulk_flush_rows: default_max_segment_rows().min(100_000),
            bulk_flush_age: Duration::from_secs(60),
            bulk_load_rows_per_sec: BULK_LOAD_ROWS_PER_SEC,
            bulk_flush_bytes: bulk_flush_bytes_for(default_max_unindexed_bytes()),
            defer_compaction_segments: 1024,
            index_interval: Duration::from_millis(250),
            compaction_segments: 4,
            max_segment_rows: default_max_segment_rows(),
            max_segment_bytes: 2 << 30,
            retention: Duration::from_secs(3600),
            lease_duration: Duration::from_secs(30),
            max_unindexed_bytes: default_max_unindexed_bytes(),
            wal_pipeline: 8,
            eventual_tail_bytes: 128 * 1024 * 1024,
            // Two builds at once only where memory allows (each holds a whole segment).
            build_concurrency: if memory_limit_bytes() < (8 << 30) {
                1
            } else {
                2
            },
            ann_nprobe: 64,
            ann_nprobe_explicit: false,
            ann_rerank_factor: 32,
            exact_fetch_rows: 8192,
            #[cfg(test)]
            legacy_three_filter_test: false,
            #[cfg(test)]
            baseline_filter_test: false,
            recall_sample_rate: 0.01,
            recall_sample_interval: Duration::from_secs(60),
            models: None,
        }
    }
}
impl EngineConfig {
    /// Environment overrides let existing HTTP entrypoints exercise indexing unchanged.
    /// Invalid or zero threshold values retain defaults.
    /// `BF_DECODED_CACHE_BYTES` accepts zero to disable decoded metadata admission.
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Some(bytes) = std::env::var("BF_DECODED_CACHE_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
        {
            c.decoded_cache_bytes = bytes;
        }
        let number = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
        };
        if let Some(v) = number("BF_INDEX_ROWS") {
            c.index_rows = v as usize;
        }
        if let Some(v) = number("BF_INDEX_BYTES") {
            c.index_bytes = v;
        }
        if let Some(v) = number("BF_MAX_SEGMENT_ROWS") {
            c.max_segment_rows = v as usize;
        }
        if let Some(v) = number("BF_MAX_SEGMENT_BYTES") {
            c.max_segment_bytes = v;
        }
        if let Some(v) = number("BF_INDEX_AGE_MS") {
            c.index_age = Duration::from_millis(v);
        }
        if let Some(v) = number("BF_BULK_FLUSH_ROWS") {
            c.bulk_flush_rows = v as usize;
        }
        if let Some(v) = number("BF_BULK_FLUSH_AGE_MS") {
            c.bulk_flush_age = Duration::from_millis(v);
        }
        if let Some(v) = number("BF_INDEX_INTERVAL_MS") {
            c.index_interval = Duration::from_millis(v);
        }
        if let Some(v) = number("BF_MAX_UNINDEXED_BYTES") {
            c.max_unindexed_bytes = v;
            c.bulk_flush_bytes = bulk_flush_bytes_for(v);
        }
        if let Some(v) = number("BF_BULK_FLUSH_BYTES") {
            c.bulk_flush_bytes = v;
        }
        if let Some(v) = number("BF_DEFER_COMPACTION_SEGMENTS") {
            c.defer_compaction_segments = v as usize;
        }
        if let Some(v) = number("BF_WAL_PIPELINE") {
            c.wal_pipeline = (v as usize).max(1);
        }
        if let Some(v) = number("BF_RETENTION_SECS") {
            c.retention = Duration::from_secs(v);
        }
        if let Some(v) = number("BF_QUERY_CONCURRENCY") {
            c.query_concurrency = v as usize;
        }
        if let Some(v) = number("BF_SEARCH_CONCURRENCY") {
            c.search_concurrency = v as usize;
        }
        if let Some(v) = number("BF_RECALL_SAMPLE_INTERVAL_MS") {
            c.recall_sample_interval = Duration::from_millis(v);
        }
        if let Some(v) = number("BF_ANN_NPROBE") {
            c.ann_nprobe = v as usize;
            c.ann_nprobe_explicit = true;
        }
        if let Some(v) = number("BF_BUILD_CONCURRENCY") {
            c.build_concurrency = v as usize;
        }
        if let Some(v) = number("BF_ANN_RERANK") {
            c.ann_rerank_factor = v as usize;
        }
        if let Some(v) = number("BF_EXACT_FETCH") {
            c.exact_fetch_rows = v as usize;
        }

        c
    }
}
pub(crate) fn start(engine: &Engine) {
    if !engine.0.config.indexing || engine.0.store.is_read_only() {
        return;
    }
    let weak = std::sync::Arc::downgrade(&engine.0);
    let interval = engine.0.config.index_interval.max(Duration::from_millis(1));
    let stop = engine.0.stop.clone();
    // Shutdown drops the loop at its next await, mid-pass if need be: a pass publishes only
    // through a fenced manifest write, so a dropped one leaves orphans for GC, as a crash does.
    crate::query_worker::shared_runtime()
        .spawn(async move { stop.clone().run(index_loop(weak, interval, stop)).await });
}
async fn index_loop(
    weak: std::sync::Weak<crate::Inner>,
    interval: Duration,
    stop: std::sync::Arc<crate::stop::Stop>,
) {
    {
        let mut last_gc = std::time::Instant::now();
        let mut discover = true;
        // Namespaces whose older-layout vector indexes have been upgraded (or never had any) since
        // start, and the one upgrade that may run at a time, in its own task so it does not hold
        // up indexing (see `reindex`).
        let upgraded = std::sync::Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::<
            String,
        >::new()));
        let upgrading = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        loop {
            tokio::time::sleep(interval).await;
            let Some(inner) = weak.upgrade() else { break };
            let engine = Engine(inner);
            let gc_due = last_gc.elapsed()
                >= engine
                    .0
                    .config
                    .retention
                    .div_f64(2.0)
                    .clamp(Duration::from_secs(1), Duration::from_secs(60));
            if discover || gc_due {
                if let Ok(objects) = engine.0.store.list("ns/").await {
                    let names: std::collections::BTreeSet<_> = objects
                        .iter()
                        .filter_map(|o| o.key.strip_prefix("ns/").and_then(|s| s.split('/').next()))
                        .collect();
                    for name in names {
                        let _ = engine.handle(name).await;
                    }
                }
                discover = false;
            }
            let names: Vec<_> = engine.0.namespaces.lock().await.keys().cloned().collect();
            for ns in names {
                if let Err(e) = engine.index_namespace(&ns, false).await {
                    tracing::warn!(error=%e, "background indexing deferred");
                }
                engine.consider_recluster(&ns).await;
                let pending = !upgraded.lock().is_ok_and(|u| u.contains(&ns));
                if pending && !upgrading.swap(true, std::sync::atomic::Ordering::AcqRel) {
                    let (engine, ns, upgraded, upgrading) = (
                        engine.clone(),
                        ns.clone(),
                        upgraded.clone(),
                        upgrading.clone(),
                    );
                    let stop = stop.clone();
                    tokio::spawn(async move {
                        // A namespace with no checkpoint yet has no old indexes: new ones are fast-scan.
                        let Some(result) = stop.run(engine.reindex(&ns, &Default::default())).await
                        else {
                            // Shutting down: a later start resumes the upgrade.
                            return;
                        };
                        match result {
                            Ok(_) => {
                                if let Ok(mut u) = upgraded.lock() {
                                    u.insert(ns);
                                }
                            }
                            Err(e) if e.status == 404 => {
                                if let Ok(mut u) = upgraded.lock() {
                                    u.insert(ns);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error=%e, "vector index upgrade deferred");
                                tokio::time::sleep(Duration::from_secs(30)).await;
                            }
                        }
                        upgrading.store(false, std::sync::atomic::Ordering::Release);
                    });
                }
                if gc_due && let Err(e) = engine.collect_garbage(&ns).await {
                    tracing::warn!(error=%e,"background garbage collection deferred");
                }
            }
            if gc_due {
                last_gc = std::time::Instant::now();
            }
        }
    }
}
/// Segments holding rows some recluster tier covers, directly or through lineage. A flush keeps
/// them listed even once every row is deleted.
pub(crate) fn covered_segments(manifest: &Manifest) -> std::collections::BTreeSet<String> {
    crate::lineage::covered(&manifest.segments, &manifest.ann_tiers, &manifest.lineage)
}
/// A recluster tier holds rows of the segments it covers: once a covered segment is gone with no
/// lineage to where its rows went (a schema rebuild, or deletion of the namespace), it can never
/// answer again, so retire it. Lineage no remaining tier refers to is dropped too.
pub(crate) fn retire_stale_tiers(manifest: &mut Manifest) {
    let mut stale = Vec::new();
    for (attr, tiers) in &manifest.ann_tiers {
        for (n, set) in tiers.iter().enumerate() {
            if !crate::lineage::reachable(&manifest.segments, &manifest.lineage, set) {
                stale.push((attr.clone(), n));
            }
        }
        // A delta is built against its base's cells and never answers without it.
        if stale.iter().any(|(a, n)| a == attr && *n == 0) {
            stale.retain(|(a, _)| a != attr);
            stale.extend((0..tiers.len()).map(|n| (attr.clone(), n)));
        }
    }
    for (attr, n) in stale.into_iter().rev() {
        let tiers = manifest.ann_tiers.get_mut(&attr).expect("listed above");
        let set = tiers.remove(n);
        tracing::warn!(
            attribute = attr.as_str(),
            tier = n,
            "recluster: tier retired, its covered segments are gone"
        );
        metrics::counter!("blowfish_recluster_tiers_stale_total").increment(1);
        if tiers.is_empty() {
            manifest.ann_tiers.remove(&attr);
        }
        retire_slabs(manifest, &set);
    }
    crate::lineage::prune(manifest);
}
pub(crate) fn retire_slabs(manifest: &mut Manifest, set: &bf_wal::SlabSet) {
    // Objects another remaining tier still uses (a delta's base, or cells shared) stay.
    let kept: std::collections::BTreeSet<String> = manifest
        .ann_tiers
        .values()
        .flatten()
        .flat_map(crate::auto_recluster::tier_keys)
        .collect();
    for key in crate::auto_recluster::tier_keys(set) {
        if !kept.contains(&key) {
            retire(manifest, key);
        }
    }
}
pub(crate) fn retire(manifest: &mut Manifest, key: String) {
    if !manifest.retired.iter().any(|r| r.key == key) {
        manifest.retired.push(RetiredObject {
            key,
            retired_at: Utc::now(),
        });
    }
}
/// Move per-term text statistics out of the manifest into their own object, so only BM25 queries
/// (and the indexer) read them.
async fn externalize_text_stats(
    store: &bf_storage::Storage,
    ns: &str,
    manifest: &mut Manifest,
) -> Result<(), ApiError> {
    if manifest.stats.text.is_empty() {
        return Ok(());
    }
    let text = std::mem::take(&mut manifest.stats.text);
    let (bytes, index) = crate::text::encode_text_stats(&text)?;
    let mut file = segment::put_file(
        store,
        format!("ns/{ns}/segments/text-stats-{}.bfs", uuid::Uuid::new_v4()),
        bytes,
        0,
    )
    .await?;
    file.stats_index = Some(index);
    manifest.text_stats = Some(file);
    Ok(())
}
fn adjust<D: std::borrow::Borrow<bf_types::Document>>(
    stats: &mut NamespaceStats,
    docs: &BTreeMap<bf_types::Id, D>,
    add: bool,
    schema: &bf_types::Schema,
) {
    for doc in docs.values() {
        crate::text::adjust(stats, std::borrow::Borrow::borrow(doc), schema, add);
    }
    let rows = docs.len() as u64;
    let bytes: u64 = docs
        .values()
        .map(|d| {
            serde_json::to_vec(std::borrow::Borrow::borrow(d))
                .expect("document serializes")
                .len() as u64
        })
        .sum();
    if add {
        stats.rows += rows;
        stats.logical_bytes += bytes;
    } else {
        stats.rows = stats.rows.saturating_sub(rows);
        stats.logical_bytes = stats.logical_bytes.saturating_sub(bytes);
    }
}
impl Engine {
    /// Flush a namespace's current tail and compact one eligible tier.
    /// `force` bypasses local freshness, size, age, and advisory lease checks,
    /// but never epoch fencing.
    /// Returns false when there was no work or another indexer won the publication.
    pub async fn index_namespace(&self, ns: &str, force: bool) -> Result<bool, ApiError> {
        let context = crate::read_cache::ReadContext::new(self.0.read_cache.clone());
        crate::read_cache::scope(context, self.index_namespace_inner(ns, force)).await
    }
    async fn index_namespace_inner(&self, ns: &str, force: bool) -> Result<bool, ApiError> {
        let t0 = std::time::Instant::now();
        let handle = self.handle(ns).await?;
        let bulk = |state: &NamespaceState| {
            crate::auto_recluster::loading_with_backpressure(
                state,
                Utc::now(),
                std::time::Instant::now(),
                &handle.backpressure,
                self.0.config.max_unindexed_bytes,
            )
        };
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        // A built recluster tier waiting to be installed is work too, as are the segments a build
        // covers (compaction leaves them alone).
        let protected = self.0.jobs.protected(ns);
        let installing = self.0.jobs.has_pending(ns);
        // Object existence is advisory until this pass claims its publication epoch. Probe
        // outside the namespace lock, so an idle survey never stalls queries or commits.
        let (tiers, probe_due) = {
            let state = handle.state.load();
            (
                state.ann_tiers.clone(),
                force || state.refreshed.elapsed() >= self.0.config.index_age,
            )
        };
        let missing_map = if probe_due {
            self.0.store.cached_bucket_format().map_err(storage_error)?;
            crate::slab_rows::missing(
                &self.0.store,
                &tiers,
                handle
                    .row_map_probe
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            )
            .await?
        } else {
            None
        };
        let (state, row_maps) = {
            let mut state = handle.state.lock().await;
            if state.root_only {
                // Queries keep the compact manifest copy; a build must publish the segments' text
                // headers, so take the full manifest first.
                catch_up(&wal, &mut state, &self.0.store).await?;
            }
            // Local commits and queued compaction are already known. Poll remote
            // writers at the configured maximum tail age while idle, rather than
            // holding this lock across two object-store calls on every tick.
            if !force
                && missing_map.is_none()
                && state.seq > 0
                && state.seq == state.indexed_through
                && state.refreshed.elapsed() < self.0.config.index_age
                && tier(&state, &self.0.config, force, &protected, bulk(&state)).is_empty()
                && !(needs_text_upgrade(&state)
                    || (crate::slab_rows::needs(&state.ann_tiers)
                        || crate::vector_locator::needs_retirement(&state, &protected)))
                && !installing
            {
                return Ok(false);
            }
            self.0.store.cached_bucket_format().map_err(storage_error)?;
            catch_up(&wal, &mut state, &self.0.store).await?;
            // Decide whether there is work before cloning: the tail can hold hundreds of MiB,
            // and a clone under this lock stalls every concurrent write.
            if !state.exists {
                return Ok(false);
            }
            let row_maps = crate::slab_rows::needs(&state.ann_tiers) || missing_map.is_some();
            let will_rebuild = state.segments.iter().any(|seg| {
                state
                    .schema
                    .keys()
                    .chain(seg.schema.keys())
                    .any(|name| !segment::seg_text_matches(seg, &state.schema, name))
                    || seg.schema.iter().any(|(name, old)| {
                        state.schema.get(name).is_none_or(|new| {
                            old.filterable != new.filterable
                                || old.fuzzy != new.fuzzy
                                || old.regex != new.regex
                                || old.glob != new.glob
                        })
                    })
            });
            let will_compact = tier(&state, &self.0.config, force, &protected, bulk(&state));
            let upgrade = needs_text_upgrade(&state)
                || (row_maps || crate::vector_locator::needs_retirement(&state, &protected))
                || installing;
            if state.seq == state.indexed_through
                && will_compact.is_empty()
                && !will_rebuild
                && !upgrade
            {
                return Ok(false);
            }
            let tail_age = Utc::now()
                .signed_duration_since(state.tail_since.unwrap_or(state.updated_at))
                .to_std()
                .unwrap_or_default();
            if !force
                && !will_rebuild
                && !upgrade
                && will_compact.is_empty()
                && ((state.shadowed.len() < self.0.config.index_rows
                    && state.tail_bytes < self.0.config.index_bytes
                    && tail_age < self.0.config.index_age)
                    || bulk_wait(&state, &self.0.config, tail_age))
            {
                return Ok(false);
            }
            (state.clone(), row_maps)
        };
        if !state.exists {
            return Ok(false);
        }
        crate::ann::stage(t0, "idx lock+clone");
        // Past every early return: this pass builds or compacts, so freed pages should leave quickly.
        let _busy = bf_alloc::busy();
        // A recluster build waits between slabs while this runs.
        let _indexing = crate::auto_recluster::indexing();
        let mut state = state;
        let rebuild = state.segments.iter().any(|seg| {
            state
                .schema
                .keys()
                .chain(seg.schema.keys())
                .any(|name| !segment::seg_text_matches(seg, &state.schema, name))
                || seg.schema.iter().any(|(name, old)| {
                    state.schema.get(name).is_none_or(|new| {
                        old.filterable != new.filterable
                            || old.fuzzy != new.fuzzy
                            || old.regex != new.regex
                            || old.glob != new.glob
                    })
                })
        });
        // A chunked statistics object is updated by streaming it (see `text::merge_text_stats`),
        // so the per-term map never enters memory. A rebuild, which recomputes every statistic,
        // and the legacy whole-object format load it completely.
        let mut external = state
            .text_stats
            .as_ref()
            .is_some_and(|f| f.stats_index.is_some())
            && !rebuild;
        if !external {
            crate::text::hydrate(&self.0.store, &mut state).await?;
        }
        crate::ann::stage(t0, "idx hydrate");
        let compact = tier(&state, &self.0.config, force, &protected, bulk(&state));
        let upgrade = needs_text_upgrade(&state)
            || (row_maps || crate::vector_locator::needs_retirement(&state, &protected))
            || installing;
        if state.seq == state.indexed_through && compact.is_empty() && !rebuild && !upgrade {
            return Ok(false);
        }
        let old = wal.latest_manifest().await.map_err(log_error)?;
        let old_sequence = old.as_ref().map_or(0, |(s, _)| *s);
        if old_sequence != state.manifest_seq {
            return Ok(false);
        }
        let age = Utc::now()
            .signed_duration_since(state.tail_since.unwrap_or(state.updated_at))
            .to_std()
            .unwrap_or_default();
        if !force
            && !rebuild
            && !upgrade
            && compact.is_empty()
            && ((state.shadowed.len() < self.0.config.index_rows
                && state.tail_bytes < self.0.config.index_bytes
                && age < self.0.config.index_age)
                || bulk_wait(&state, &self.0.config, age))
        {
            return Ok(false);
        }
        let mut manifest = old.map(|(_, m)| m).unwrap_or_else(|| Manifest {
            format_version: bf_wal::MANIFEST_FORMAT_VERSION,
            schema: state.schema.clone(),
            distance_metric: state.metric,
            id_type: state.schema["id"].data_type.clone().expect("id schema"),
            created_at: state.created_at,
            indexed_through: 0,
            indexer_epoch: 0,
            deleted: false,
            segments: state.segments.to_vec(),
            stats: Default::default(),
            text_stats: None,
            ann_dirs: Default::default(),
            ann_tiers: (*state.ann_tiers).clone(),
            lineage: (*state.lineage).clone(),
            updated_at: None,
            retired: vec![],
            checkpoints: vec![],
            pruned_through: 0,
            bulk_load_at: None,
            root: false,
        });
        if !force
            && let Some(lease) = wal
                .indexer_lease(manifest.indexer_epoch)
                .await
                .map_err(log_error)?
            && lease.owner != self.0.owner
            && lease.expires_at > Utc::now()
        {
            return Ok(false);
        }
        let epoch = manifest.indexer_epoch;
        manifest.indexer_epoch += 1;
        let claim = match wal.publish_fenced(old_sequence, epoch, &manifest).await {
            Ok(s) => s,
            Err(bf_wal::Error::Fenced) => return Ok(false),
            Err(e) => return Err(log_error(e)),
        };
        wal.create_lease(
            manifest.indexer_epoch,
            &IndexerLease {
                owner: self.0.owner,
                expires_at: Utc::now()
                    + chrono::Duration::from_std(self.0.config.lease_duration)
                        .unwrap_or(chrono::Duration::seconds(30)),
            },
        )
        .await
        .map_err(log_error)?;
        // Include the claim's retirement records in the subsequent publication.
        manifest = wal
            .latest_manifest()
            .await
            .map_err(log_error)?
            .ok_or_else(|| ApiError::new(503, "manifest disappeared"))?
            .1;
        if manifest.indexer_epoch != epoch + 1 {
            return Ok(false);
        }
        crate::ann::stage(t0, "idx claim");
        let store = &self.0.store;
        manifest.stats = (*state.stats).clone();
        // The previous text statistics object is replaced by this publication.
        let previous_text = manifest.text_stats.take();
        // With `external` statistics, changes accumulate here and merge into the object at the end.
        let (mut added_stats, mut removed_stats) =
            (NamespaceStats::default(), NamespaceStats::default());
        // A tombstone followed by recreation discards every old segment and its statistics.
        if state.segments.is_empty() && !manifest.segments.is_empty() {
            for seg in manifest.segments.clone() {
                for file in seg.files.values().chain(std::iter::once(&seg.live)) {
                    retire(&mut manifest, file.key.clone());
                }
            }
            manifest.segments.clear();
            manifest.stats = NamespaceStats::default();
            external = false;
        }
        let mut merged_text: Option<bf_wal::SegmentFile> = None;
        let segments = if rebuild {
            let mut rebuilt = vec![];
            manifest.stats = NamespaceStats::default();
            for seg in &state.segments {
                let live = snapshot::allowed(store, &state, seg, None).await?;
                let mut docs =
                    crate::vector_locator::scope(&state, segment::documents(store, seg, &live))
                        .await?;
                for doc in docs.values_mut() {
                    doc.retain(|name, _| state.schema.contains_key(name));
                }
                adjust(&mut manifest.stats, &docs, true, &state.schema);
                if !docs.is_empty() {
                    rebuilt.extend(
                        build_segments(
                            store,
                            ns,
                            &state.schema,
                            state.metric,
                            &docs,
                            BTreeMap::new(),
                            seg.wal_start,
                            seg.wal_end,
                            &Default::default(),
                            &self.0.config,
                        )
                        .await?,
                    );
                }
                for file in seg.files.values().chain(std::iter::once(&seg.live)) {
                    retire(&mut manifest, file.key.clone());
                }
            }
            if !state.docs.is_empty() {
                adjust(&mut manifest.stats, &state.docs, true, &state.schema);
                rebuilt.extend(
                    build_segments(
                        store,
                        ns,
                        &state.schema,
                        state.metric,
                        &state.docs,
                        BTreeMap::new(),
                        state.indexed_through + 1,
                        state.seq,
                        &Default::default(),
                        &self.0.config,
                    )
                    .await?,
                );
            }
            rebuilt
        } else {
            let mut segments = vec![];
            crate::ann::stage(t0, "idx pre-live");
            let mut covered = covered_segments(&manifest);
            covered.extend(protected.iter().cloned());
            let (masks, removed) = crate::vector_locator::scope(
                &state,
                crate::statistics::removals(store, &state, self.0.config.build_concurrency),
            )
            .await?;
            if external {
                removed_stats = removed;
            } else {
                crate::statistics::merge(&mut manifest.stats, removed, false);
            }
            for (mut seg, (live, keep)) in state.segments.iter().cloned().zip(masks) {
                let deleted = &live - &keep;
                if !deleted.is_empty() {
                    retire(&mut manifest, seg.live.key.clone());
                    // A covered segment stays listed, with no live rows, while its tier does:
                    // the tier still holds its rows, and queries mask them out.
                    if keep.is_empty() && !covered.contains(&seg.id) {
                        for file in seg.files.values() {
                            retire(&mut manifest, file.key.clone());
                        }
                        continue;
                    }
                    segment::set_live(store, ns, &mut seg, &keep).await?;
                }
                segments.push(seg);
            }
            crate::ann::stage(t0, "idx live");
            // Compaction only replaces selected disjoint live collections. New tail rows
            // are already masked out of old segments, so the merged result has unique IDs.
            let mut compact_docs: BTreeMap<bf_types::Id, std::sync::Arc<bf_types::Document>> =
                BTreeMap::new();
            // Vector attributes travel as packed stored-precision bytes, not documents.
            let strip: std::collections::BTreeSet<String> = state
                .schema
                .iter()
                .filter(|(_, c)| segment::dense_type(c).is_some() && c.filterable != Some(true))
                .map(|(name, _)| name.clone())
                .collect();
            // Delegate an attribute only when its tier owns every row of every input.
            // A partially covered merge still builds its own ANN.
            let sources = crate::vector_locator::Sources::new(&state);
            let delegated: std::collections::BTreeSet<String> = strip
                .iter()
                .filter(|attr| {
                    state
                        .segments
                        .iter()
                        .filter(|seg| compact.contains(&seg.id))
                        .all(|seg| sources.covers(seg, attr))
                })
                .cloned()
                .collect();
            let mut compact_vectors: BTreeMap<String, Vec<segment::PackedPart>> = BTreeMap::new();
            let mut start = u64::MAX;
            let mut end = 0;
            // Merged segments whose rows a tier covers: their surviving rows and IDs, for lineage.
            let lineage_sources = covered_segments(&manifest);
            let mut tracked: Vec<(String, RoaringBitmap, std::sync::Arc<segment::IdIndex>)> =
                Vec::new();
            if !compact.is_empty() {
                let mut remaining = vec![];
                for seg in segments {
                    if compact.contains(&seg.id) {
                        let live = segment::live(store, &seg).await?;
                        if lineage_sources.contains(&seg.id) {
                            tracked.push((
                                seg.id.clone(),
                                live.clone(),
                                segment::ids(store, &seg).await?,
                            ));
                        }
                        let (docs, vectors) = crate::vector_locator::scope(
                            &state,
                            segment::compaction_rows(store, &seg, &live, &strip, &delegated),
                        )
                        .await?;
                        compact_docs.extend(crate::shared(docs));
                        for (name, part) in vectors {
                            compact_vectors.entry(name).or_default().push(part);
                        }
                        start = start.min(seg.wal_start);
                        end = end.max(seg.wal_end);
                        for file in seg.files.values().chain(std::iter::once(&seg.live)) {
                            retire(&mut manifest, file.key.clone());
                        }
                    } else {
                        remaining.push(seg);
                    }
                }
                segments = remaining;
                crate::ann::stage(t0, "idx compact_read");
            }
            let flushing = state.seq > state.indexed_through && !state.docs.is_empty();
            // The flush segments, the compaction output, and the tail's text statistics are
            // independent: build them at the same time.
            let stats_job = flushing.then(|| {
                let docs = state.docs.clone();
                let schema = state.schema.clone();
                let mut stats = if external {
                    NamespaceStats::default()
                } else {
                    std::mem::take(&mut manifest.stats)
                };
                tokio::task::spawn_blocking(move || {
                    adjust(&mut stats, &docs, true, &schema);
                    stats
                })
            });
            let flush = async {
                if flushing {
                    build_segments(
                        store,
                        ns,
                        &state.schema,
                        state.metric,
                        &state.docs,
                        BTreeMap::new(),
                        state.indexed_through + 1,
                        state.seq,
                        &Default::default(),
                        &self.0.config,
                    )
                    .await
                } else {
                    Ok(vec![])
                }
            };
            let compact_packed =
                segment::merge_packed(&state.schema, &compact_docs, compact_vectors)?;
            let merged_lineage: Vec<crate::lineage::Merged> = if tracked.is_empty() {
                Vec::new()
            } else {
                let order: Vec<&bf_types::Id> = compact_docs.keys().collect();
                tracked
                    .iter()
                    .map(|(id, live, ids)| {
                        let rows = live
                            .iter()
                            .map(|row| {
                                let id = ids.get(row as usize).ok_or_else(segment::corrupt)?;
                                let at =
                                    order.binary_search(&id).map_err(|_| segment::corrupt())?;
                                Ok((row, at as u32))
                            })
                            .collect::<Result<Vec<_>, ApiError>>()?;
                        Ok(crate::lineage::Merged {
                            segment: id.clone(),
                            rows,
                        })
                    })
                    .collect::<Result<_, ApiError>>()?
            };
            let compacted = async {
                if compact_docs.is_empty() {
                    Ok(vec![])
                } else {
                    build_segments(
                        store,
                        ns,
                        &state.schema,
                        state.metric,
                        &compact_docs,
                        compact_packed,
                        start,
                        end,
                        &delegated,
                        &self.0.config,
                    )
                    .await
                }
            };
            // The statistics merge needs only the tail's statistics and the previous object, not
            // the built segments, so it runs beside the builds (it is mostly object-store
            // transfer, and the builds leave cores idle while they wait on uploads).
            let removed_text = std::mem::take(&mut removed_stats.text);
            let previous_for_merge = previous_text.clone();
            let text_merge = async {
                let mut tail_stats = match stats_job {
                    Some(job) => Some(job.await.map_err(segment::index_error)?),
                    None => None,
                };
                if !external {
                    return Ok::<_, ApiError>((tail_stats, None));
                }
                let added_text = tail_stats
                    .as_mut()
                    .map(|s| std::mem::take(&mut s.text))
                    .unwrap_or_default();
                if crate::text::unchanged_stats(&added_text, &removed_text) {
                    return Ok((tail_stats, None));
                }
                let previous = previous_for_merge
                    .as_ref()
                    .expect("external statistics object");
                let old = segment::read_whole_uncached(store, previous).await?;
                let index = previous.stats_index.clone().expect("chunked statistics");
                let (bytes, index) = tokio::task::spawn_blocking(move || {
                    crate::text::merge_text_stats(&old, &index, &added_text, &removed_text)
                })
                .await
                .map_err(segment::index_error)??;
                let mut file = segment::put_file(
                    store,
                    format!("ns/{ns}/segments/text-stats-{}.bfs", uuid::Uuid::new_v4()),
                    bytes,
                    0,
                )
                .await?;
                file.stats_index = Some(index);
                Ok((tail_stats, Some(file)))
            };
            let (flushed, compacted, (tail_stats, merged)) =
                futures::future::try_join3(flush, compacted, text_merge).await?;
            crate::ann::stage(t0, "idx builds");
            if let Some(stats) = tail_stats {
                if external {
                    added_stats = stats;
                } else {
                    manifest.stats = stats;
                }
            }
            merged_text = merged;
            if !tracked.is_empty() {
                crate::lineage::record(store, ns, &mut manifest, &merged_lineage, &compacted)
                    .await?;
            }
            segments.extend(flushed);
            segments.extend(compacted);
            segments
        };
        manifest.deleted = false;
        manifest.segments = segments;
        let old_vectors = crate::vector_locator::Sources::new(&state);
        retire_stale_tiers(&mut manifest);
        crate::vector_locator::restore_uncovered(store, ns, &mut manifest, &old_vectors).await?;
        // Rewrite bounded batches under this publication's epoch claim. A failed or fenced
        // pass leaves only unreferenced objects for GC; it never deletes uncertain uploads.
        let old = crate::text_upgrade::upgrade(store, ns, &mut manifest.segments).await?;
        for key in old {
            retire(&mut manifest, key);
        }
        // A finished recluster tier joins this publication if its segments are still here.
        let installed = self.0.jobs.install(ns, &mut manifest);
        if installed.is_some() {
            // A new base covers the merged segments' targets itself: their lineage goes.
            crate::lineage::prune(&mut manifest);
        }
        crate::vector_locator::restore_uncovered(store, ns, &mut manifest, &old_vectors).await?;
        crate::slab_rows::ensure(store, ns, &mut manifest, 1, missing_map.as_deref()).await?;
        crate::vector_locator::retire_covered(&mut manifest, &protected);
        // Rebuild each vector attribute's ANN directories only when their segment list changed.
        let attributes: std::collections::BTreeSet<String> = manifest
            .segments
            .iter()
            .flat_map(|s| s.files.keys())
            .filter_map(|k| k.strip_prefix("annv2:").map(str::to_string))
            .collect();
        let previous_dirs = std::mem::take(&mut manifest.ann_dirs);
        for attr in attributes {
            // The two layouts are independent objects: write them at the same time.
            let sources = manifest
                .segments
                .iter()
                .filter(|s| crate::upgrade::files(s, &attr).is_some())
                .cloned()
                .collect::<Vec<_>>();
            let layouts = [crate::ann_dir::Layout::Segments];
            let built = futures::future::try_join_all(layouts.iter().map(|layout| {
                crate::ann_dir::build(
                    store,
                    ns,
                    &sources,
                    &attr,
                    *layout,
                    previous_dirs.get(&layout.dir_key(&attr)),
                )
            }))
            .await?;
            for (layout, file) in layouts.iter().zip(built) {
                if let Some(file) = file {
                    manifest.ann_dirs.insert(layout.dir_key(&attr), file);
                }
            }
        }
        for (role, old) in &previous_dirs {
            if !manifest.ann_dirs.contains_key(role) {
                retire(&mut manifest, crate::ann_dir::hint_key(ns, role));
            }
            if !manifest.ann_dirs.values().any(|f| f.key == old.key) {
                retire(&mut manifest, old.key.clone());
            }
        }
        crate::ann::stage(t0, "idx dirs");
        manifest.schema = state.schema.clone();
        manifest.distance_metric = state.metric;
        manifest.id_type = state.schema["id"].data_type.clone().expect("id schema");
        manifest.created_at = state.created_at;
        manifest.updated_at = Some(state.updated_at);
        manifest.indexed_through = state.seq;
        if external && merged_text.is_none() {
            // Nothing changed in the text statistics (a compaction, say): keep the object.
            manifest.stats.rows = manifest
                .stats
                .rows
                .saturating_add(added_stats.rows)
                .saturating_sub(removed_stats.rows);
            manifest.stats.logical_bytes = manifest
                .stats
                .logical_bytes
                .saturating_add(added_stats.logical_bytes)
                .saturating_sub(removed_stats.logical_bytes);
            manifest.text_stats = previous_text.clone();
        } else if external {
            manifest.stats.rows = manifest
                .stats
                .rows
                .saturating_add(added_stats.rows)
                .saturating_sub(removed_stats.rows);
            manifest.stats.logical_bytes = manifest
                .stats
                .logical_bytes
                .saturating_add(added_stats.logical_bytes)
                .saturating_sub(removed_stats.logical_bytes);
            manifest.text_stats = Some(merged_text.take().expect("merged statistics object"));
        } else {
            externalize_text_stats(store, ns, &mut manifest).await?;
        }
        if let Some(previous) = previous_text
            && manifest
                .text_stats
                .as_ref()
                .is_none_or(|f| f.key != previous.key)
        {
            retire(&mut manifest, previous.key);
        }
        crate::ann::stage(t0, "idx text_stats");
        // A load-sized flush, written at a load's rate: the recluster trigger holds builds while
        // loads continue, across restarts too. A steady stream of writes is not a load: its rows
        // go into delta tiers as they come.
        if state.docs.len() >= self.0.config.bulk_flush_rows.max(4) / 4
            && state.docs.len() as f64 / age.as_secs_f64().max(1.0)
                >= self.0.config.bulk_load_rows_per_sec
        {
            manifest.bulk_load_at = Some(Utc::now());
        }
        let generation = match wal
            .publish_fenced(claim, manifest.indexer_epoch, &manifest)
            .await
        {
            Ok(generation) => generation,
            Err(bf_wal::Error::Fenced) => return Ok(false),
            Err(e) => return Err(log_error(e)),
        };
        if let Some(installed) = installed {
            installed.published();
        }
        crate::ann::stage(t0, "idx publish");
        crate::drain::record(state.tail_bytes, t0.elapsed());
        let mut current = handle.state.lock().await;
        // This node wrote the manifest and holds the tail in memory, so adopt the checkpoint
        // directly. Re-reading the tail from the object store would download every unindexed
        // document again on every publication, with the state locked against writes.
        if !current.fast_forward(&manifest, generation, state.seq) {
            catch_up(&wal, &mut current, &self.0.store).await?;
        }
        crate::ann::stage(t0, "idx done");
        Ok(true)
    }
    /// Reclaim retired files, checkpointed WAL payloads, and abandoned uploads after retention.
    /// Permanent WAL slot records continue to fence every stale writer.
    pub async fn collect_garbage(&self, ns: &str) -> Result<usize, ApiError> {
        self.0
            .store
            .check_bucket_format()
            .await
            .map_err(storage_error)?;
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        let grace = chrono::Duration::from_std(self.0.config.retention)
            .unwrap_or(chrono::Duration::hours(1));
        let cutoff = Utc::now() - grace;
        // This also runs before the first checkpoint, including after a process crashed while
        // uploading its first groups. Slot fences make it independent of the manifest epoch.
        let orphans = wal
            .collect_orphan_payloads(cutoff)
            .await
            .map_err(log_error)?;
        let Some((sequence, mut manifest)) = wal.latest_manifest().await.map_err(log_error)? else {
            return Ok(orphans);
        };
        // Start from the resident state when this node has one: a fresh replay would download
        // every unindexed WAL payload again on each collection.
        let mut checkpoint = match self.0.namespaces.lock().await.get(ns).cloned() {
            Some(handle) => handle.state.lock().await.clone(),
            None => NamespaceState::default(),
        };
        catch_up(&wal, &mut checkpoint, &self.0.store).await?;
        if checkpoint.manifest_seq != sequence {
            return Ok(orphans);
        }
        let deleting = !checkpoint.exists && !manifest.deleted;
        if deleting {
            for seg in manifest.segments.clone() {
                for file in seg.files.values().chain(std::iter::once(&seg.live)) {
                    retire(&mut manifest, file.key.clone());
                }
            }
            manifest.segments.clear();
            manifest.stats = NamespaceStats::default();
            if let Some(file) = manifest.text_stats.take() {
                retire(&mut manifest, file.key);
            }
            for file in std::mem::take(&mut manifest.ann_dirs).into_values() {
                retire(&mut manifest, file.key);
            }
            for set in std::mem::take(&mut manifest.ann_tiers)
                .into_values()
                .flatten()
            {
                retire_slabs(&mut manifest, &set);
            }
            crate::lineage::prune(&mut manifest);
            manifest.deleted = true;
            manifest.indexed_through = checkpoint.seq;
        }
        let mut protected = std::collections::BTreeSet::new();
        protected.extend(manifest.text_stats.iter().map(|f| f.key.clone()));
        protected.extend(manifest.ann_dirs.values().map(|f| f.key.clone()));
        protected.extend(
            manifest
                .ann_dirs
                .keys()
                .map(|k| crate::ann_dir::hint_key(ns, k.trim_start_matches("ann:"))),
        );
        for (attr, set) in manifest
            .ann_tiers
            .iter()
            .flat_map(|(attr, tiers)| tiers.iter().map(move |set| (attr, set)))
        {
            protected.extend(crate::auto_recluster::tier_keys(set));
            protected.insert(crate::ann_dir::hint_key(ns, &format!("ann3:{attr}")));
            protected.insert(crate::ann_dir::hint_key(
                ns,
                &crate::ann_dir::paged_hint_name(attr),
            ));
        }
        protected.extend(crate::lineage::keys(&manifest).cloned());
        // Objects of recluster builds still running (their records are fresh).
        let building = crate::auto_recluster::building_prefixes(&self.0.store, ns).await?;
        for seg in &manifest.segments {
            protected.extend(
                seg.files
                    .values()
                    .chain(std::iter::once(&seg.live))
                    .map(|f| f.key.clone()),
            );
        }
        for retired in &manifest.retired {
            if retired.retired_at > cutoff {
                protected.insert(retired.key.clone());
            }
        }
        let slot = self
            .0
            .store
            .get(&format!("ns/{ns}/manifest/{sequence:020}.json"))
            .await
            .map_err(storage_error)?;
        if let Some(key) = bf_wal::slot_payload_key(&slot) {
            protected.insert(key);
        }
        // Take the candidate inventory BEFORE claiming a newer epoch. This fences
        // paused builders of these files, without sweeping a newer builder's files.
        let mut candidates = std::collections::BTreeSet::new();
        for prefix in [
            format!("ns/{ns}/segments/"),
            format!("ns/{ns}/slabs/"),
            format!("ns/{ns}/manifest-data/"),
            format!("ns/{ns}/indexer/"),
        ] {
            for object in self.0.store.list(&prefix).await.map_err(storage_error)? {
                if object.last_modified <= cutoff
                    && !protected.contains(&object.key)
                    && !building.iter().any(|p| object.key.starts_with(p.as_str()))
                {
                    candidates.insert(object.key);
                }
            }
        }
        let through = manifest
            .checkpoints
            .iter()
            .filter(|(_, at)| *at <= cutoff)
            .map(|(seq, _)| *seq)
            .max()
            .unwrap_or(0);
        if candidates.is_empty() && through == 0 && !deleting {
            return Ok(orphans);
        }
        let epoch = manifest.indexer_epoch;
        manifest.indexer_epoch += 1;
        let claim = match wal.publish_fenced(sequence, epoch, &manifest).await {
            Ok(s) => s,
            Err(bf_wal::Error::Fenced) => return Ok(orphans),
            Err(e) => return Err(log_error(e)),
        };
        // Reread durable pins AFTER the epoch claim. A new copier either pinned
        // before this read, or observes the new epoch and retries its snapshot.
        let shared = crate::branch::protected(&self.0.store).await?;
        let fresh_builds = crate::auto_recluster::building_prefixes(&self.0.store, ns).await?;
        candidates.retain(|key| {
            !shared.contains(key) && !fresh_builds.iter().any(|p| key.starts_with(p))
        });
        // The winner owns no reusable file names. Repeating deletes after a crash is safe.
        // Deletes are independent round trips; run them concurrently so reclaiming
        // thousands of retired objects does not stall indexing for minutes.
        let keys: Vec<&String> = candidates.iter().collect();
        for group in keys.chunks(32) {
            let mut deletes = Vec::with_capacity(group.len());
            for key in group {
                deletes.push(self.0.store.delete(key));
            }
            futures::future::try_join_all(deletes)
                .await
                .map_err(storage_error)?;
        }
        if through > 0 {
            wal.prune_payloads_from(manifest.pruned_through.min(through), through)
                .await
                .map_err(log_error)?;
        }
        let Some((latest, mut manifest)) = wal.latest_manifest().await.map_err(log_error)? else {
            return Ok(candidates.len() + orphans);
        };
        if latest != claim {
            return Ok(candidates.len() + orphans);
        }
        manifest
            .retired
            .retain(|file| !candidates.contains(&file.key));
        manifest.checkpoints.retain(|(seq, _)| *seq > through);
        manifest.pruned_through = manifest.pruned_through.max(through);
        match wal
            .publish_fenced(claim, manifest.indexer_epoch, &manifest)
            .await
        {
            Ok(_) | Err(bf_wal::Error::Fenced) => {}
            Err(e) => return Err(log_error(e)),
        }
        Ok(candidates.len() + orphans)
    }
}
/// A flush of a tail written at least this fast is a bulk load's (NQ loads at about 10,000 rows a
/// second; the continuous-write test writes 1,000).
const BULK_LOAD_ROWS_PER_SEC: f64 = 2_000.0;
/// Give arriving writes one normal flush-age window to establish their rate, then accumulate
/// only bulk loads. A steady stream below the bulk rate must not leave a minute of exact scans.
fn bulk_wait(state: &NamespaceState, config: &EngineConfig, tail_age: Duration) -> bool {
    config.index_age >= Duration::from_secs(1)
        && (tail_age < config.index_age
            || state.docs.len() as f64 / tail_age.as_secs_f64().max(1.0)
                >= config.bulk_load_rows_per_sec)
        && state.shadowed.len() < config.bulk_flush_rows
        && tail_age < config.bulk_flush_age
        && state.tail_bytes < config.bulk_flush_bytes.min(config.max_unindexed_bytes)
        && Utc::now()
            .signed_duration_since(state.updated_at)
            .to_std()
            .is_ok_and(|since| since < Duration::from_secs(1))
}
fn needs_text_upgrade(state: &NamespaceState) -> bool {
    state.segments.iter().any(|seg| {
        crate::text_keys::needs(seg)
            || seg
                .files
                .iter()
                .any(|(key, file)| key.starts_with("fts:") && segment::text_needs_upgrade(file))
    })
}

/// The segments to merge next, if any. Segments in `protected` (a recluster build covering them is
/// running or waiting to be installed) are left alone. A merge never mixes coverage classes (the
/// tiers covering a segment's rows): a tier-covered segment merges only with segments covered by
/// the same tiers, and lineage records where its rows went (see `lineage.rs`).
pub(crate) fn tier(
    state: &NamespaceState,
    config: &EngineConfig,
    force: bool,
    protected: &std::collections::BTreeSet<String>,
    bulk: bool,
) -> Vec<String> {
    if !force
        && (bulk || state.shadowed.len() >= config.index_rows)
        && state.segments.len() < config.defer_compaction_segments
    {
        return vec![];
    }
    let classes = crate::lineage::classes(&state.segments, &state.ann_tiers, &state.lineage);
    let mut by_class: BTreeMap<Vec<&str>, Vec<&bf_wal::Segment>> = BTreeMap::new();
    for seg in state.segments.iter().filter(|s| !protected.contains(&s.id)) {
        let class = classes
            .get(&seg.id)
            .map(|c| c.iter().map(String::as_str).collect())
            .unwrap_or_default();
        by_class.entry(class).or_default().push(seg);
    }
    by_class
        .into_values()
        .map(|candidates| pick(&candidates, config))
        .find(|ids| !ids.is_empty())
        .unwrap_or_default()
}

/// The merge [`tier`] picks among `candidates` (all of one coverage class).
fn pick(candidates: &[&bf_wal::Segment], config: &EngineConfig) -> Vec<String> {
    let count = config.compaction_segments.max(2);
    let mut tiers: BTreeMap<u32, (Vec<String>, u64, u64)> = BTreeMap::new();
    for seg in candidates.iter().copied() {
        let bucket = seg.row_count.max(1).ilog2();
        let group = tiers.entry(bucket).or_default();
        let bytes = seg.files.values().map(|file| file.size).sum::<u64>();
        if group.1.saturating_add(seg.row_count as u64) > config.max_segment_rows.max(1) as u64
            || group.2.saturating_add(bytes) > config.max_segment_bytes.max(1)
        {
            // The group cannot grow to `count` members without passing the caps: merge what it
            // holds (two or more) rather than leave a tier of half-full segments forever, so a
            // bulk load ends with a few evenly sized segments.
            if group.0.len() >= 2 {
                return group.0.clone();
            }
            continue;
        }
        group.0.push(seg.id.clone());
        group.1 += seg.row_count as u64;
        group.2 += bytes;
        if group.0.len() == count {
            return group.0.clone();
        }
    }
    // No tier is full. Merge the smallest sizeable segments (at least an eighth of the row cap,
    // so trickles of small writes do not rewrite big segments) that fit under the caps together,
    // whatever their tier: the odd-sized tail of a bulk load still ends up in a few large
    // segments. Segments past half the cap never qualify, so this stops by itself.
    let floor = (config.max_segment_rows.max(1) / 8) as u32;
    let mut by_size: Vec<_> = candidates
        .iter()
        .copied()
        .filter(|seg| seg.row_count >= floor)
        .collect();
    by_size.sort_by_key(|seg| (seg.row_count, seg.id.clone()));
    let (mut ids, mut rows, mut bytes) = (Vec::new(), 0u64, 0u64);
    for seg in by_size {
        let size = seg.files.values().map(|file| file.size).sum::<u64>();
        if rows.saturating_add(seg.row_count as u64) > config.max_segment_rows.max(1) as u64
            || bytes.saturating_add(size) > config.max_segment_bytes.max(1)
            || ids.len() == count
        {
            break;
        }
        ids.push(seg.id.clone());
        rows += seg.row_count as u64;
        bytes += size;
    }
    if ids.len() >= 2 { ids } else { vec![] }
}

// A segment build owns only one bounded document batch at a time. The WAL overlay
// is separately bounded by ingest backpressure. One oversized document stays atomic.
#[allow(clippy::too_many_arguments)]
async fn build_segments<D: std::borrow::Borrow<bf_types::Document> + Clone>(
    store: &bf_storage::Storage,
    ns: &str,
    schema: &bf_types::Schema,
    metric: bf_types::DistanceMetric,
    docs: &BTreeMap<bf_types::Id, D>,
    mut vectors: BTreeMap<String, segment::PackedColumn>,
    start: u64,
    end: u64,
    delegated: &std::collections::BTreeSet<String>,
    config: &EngineConfig,
) -> Result<Vec<bf_wal::Segment>, ApiError> {
    let mut batches = vec![];
    let mut batch = BTreeMap::new();
    let mut bytes = 0u64;
    for (id, doc) in docs {
        let size = serde_json::to_vec(std::borrow::Borrow::borrow(doc))
            .map_err(segment::index_error)?
            .len() as u64;
        if !batch.is_empty()
            && (batch.len() >= config.max_segment_rows.max(1)
                || bytes.saturating_add(size) > config.max_segment_bytes.max(1))
        {
            batches.push(std::mem::take(&mut batch));
            bytes = 0;
        }
        batch.insert(id.clone(), doc.clone());
        bytes = bytes.saturating_add(size);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    // Hand each batch its share of the packed vectors (they cover `docs` in order).
    let mut batch_vectors: Vec<BTreeMap<String, segment::PackedColumn>> =
        (0..batches.len()).map(|_| BTreeMap::new()).collect();
    for (name, column) in &mut vectors {
        let width = column.dims * column.dtype.width();
        let mut base = 0u32;
        for (b, batch) in batches.iter().enumerate() {
            let rows = batch.len() as u32;
            let n = column.rows.partition_point(|r| *r < base + rows);
            let rest_rows = column.rows.split_off(n);
            let rest_data = column.data.split_off(n * width);
            let rows_here = std::mem::replace(&mut column.rows, rest_rows)
                .into_iter()
                .map(|r| r - base)
                .collect();
            let data_here = std::mem::replace(&mut column.data, rest_data);
            batch_vectors[b].insert(
                name.clone(),
                segment::PackedColumn {
                    dims: column.dims,
                    dtype: column.dtype,
                    rows: rows_here,
                    data: data_here,
                },
            );
            base += rows;
        }
    }
    drop(vectors);
    // Segments are independent: build a few at once, keeping their order.
    let mut out = Vec::with_capacity(batches.len());
    let mut batch_vectors = batch_vectors.into_iter();
    for group in batches.chunks(config.build_concurrency.max(1)) {
        let mut builds = Vec::with_capacity(group.len());
        for batch in group {
            let vectors = batch_vectors.next().unwrap_or_default();
            builds.push(segment::build_with_delegation(
                store, ns, schema, metric, batch, vectors, start, end, delegated,
            ));
        }
        let started = std::time::Instant::now();
        let built = futures::future::try_join_all(builds).await?;
        let rows: u32 = built.iter().map(|s| s.row_count).sum();
        metrics::counter!("blowfish_segment_build_millis_total")
            .increment((started.elapsed().as_secs_f64() * 1000.0) as u64);
        metrics::counter!("blowfish_segment_build_rows_total").increment(rows as u64);
        out.extend(built);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steady_writes_flush_at_normal_age_but_bulk_loads_keep_accumulating() {
        let config = EngineConfig {
            bulk_flush_rows: 100_000,
            bulk_flush_bytes: 1_000_000_000,
            ..Default::default()
        };
        for rate in [100, 300, 1000, 3000] {
            let mut state = NamespaceState {
                updated_at: Utc::now(),
                ..Default::default()
            };
            for id in 0..rate * 6 {
                state
                    .docs
                    .insert(bf_types::Id::Uint(id), std::sync::Arc::default());
                state.shadowed.insert(bf_types::Id::Uint(id));
            }
            assert_eq!(
                bulk_wait(&state, &config, Duration::from_secs(6)),
                rate >= 2000,
                "rate={rate}"
            );
            assert!(bulk_wait(&state, &config, Duration::from_secs(1)));
            assert!(!bulk_wait(&state, &config, config.bulk_flush_age));
            state.updated_at = Utc::now() - chrono::Duration::seconds(2);
            assert!(!bulk_wait(&state, &config, Duration::from_secs(6)));
            state.updated_at = Utc::now();
            state.tail_bytes = config.bulk_flush_bytes;
            assert!(!bulk_wait(&state, &config, Duration::from_secs(6)));
        }
    }

    fn state_with(rows: &[u32]) -> NamespaceState {
        let mut state = NamespaceState::default();
        for (i, row_count) in rows.iter().enumerate() {
            state.segments.push(
                serde_json::from_value(serde_json::json!({
                    "id": format!("s{i}"), "wal_start": 0, "wal_end": 0, "row_count": row_count,
                    "files": {}, "live": {"key": "k", "size": 0, "metadata_len": 0},
                    "live_generation": 0
                }))
                .unwrap(),
            );
        }
        state
    }

    #[test]
    fn compaction_waits_through_advertised_retries_but_honors_force_and_segment_cap() {
        let config = EngineConfig {
            max_segment_rows: 200_000,
            defer_compaction_segments: 8,
            ..Default::default()
        };
        let mut state = state_with(&[1000; 4]);
        let now = Utc::now();
        let clock = std::time::Instant::now();
        state.updated_at = now - chrono::Duration::seconds(30);
        state.bulk_load_at = Some(state.updated_at);
        state.seq = 9;
        state.indexed_through = 9;
        let pressure = crate::auto_recluster::LoadBackpressure::default();
        pressure.record(state.created_at, 20_001);
        let active = |state: &NamespaceState, seconds| {
            crate::auto_recluster::loading_with_backpressure(
                state,
                now + chrono::Duration::seconds(seconds),
                clock + Duration::from_secs(seconds as u64),
                &pressure,
                u64::MAX,
            )
        };
        // Indexing has caught up, but clients obeying Retry-After have not resumed yet.
        assert!(active(&state, 10));
        assert!(
            tier(
                &state,
                &config,
                false,
                &Default::default(),
                active(&state, 10)
            )
            .is_empty()
        );
        assert_eq!(
            tier(&state, &config, true, &Default::default(), true).len(),
            4
        );
        assert!(!active(&state, 24));
        assert_eq!(
            tier(
                &state,
                &config,
                false,
                &Default::default(),
                active(&state, 24)
            )
            .len(),
            4
        );
        // The existing cap prevents a long load from growing the segment list without bound.
        let capped = state_with(&[1000; 8]);
        let protected = (0..4).map(|i| format!("s{i}")).collect();
        assert_eq!(
            tier(&capped, &config, false, &protected, true),
            vec!["s4", "s5", "s6", "s7"]
        );
    }

    #[test]
    fn compaction_merges_pairs_that_cannot_grow_to_a_full_group() {
        // The default cap is sized from host memory; pin it so the test doesn't depend on the machine.
        let config = EngineConfig {
            max_segment_rows: 200_000,
            ..EngineConfig::default()
        };
        // Four segments of 100K rows would pass the 200K row cap as a group of four, so the
        // first two merge instead of staying as four half-full segments forever.
        let state = state_with(&[100_000, 100_000, 100_000, 100_000]);
        assert_eq!(
            tier(&state, &config, false, &Default::default(), false),
            vec!["s0", "s1"]
        );
        // A lone segment, or segments already at the cap, stay as they are.
        assert!(
            tier(
                &state_with(&[100_000]),
                &config,
                false,
                &Default::default(),
                false
            )
            .is_empty()
        );
        assert!(
            tier(
                &state_with(&[190_000, 190_000]),
                &config,
                false,
                &Default::default(),
                false,
            )
            .is_empty()
        );
        // Small segments still merge four at a time.
        let small = state_with(&[1000, 1100, 1200, 1300, 1400]);
        assert_eq!(
            tier(&small, &config, false, &Default::default(), false).len(),
            4
        );
        // Odd sizes in different tiers merge while their total fits under the cap.
        let odd = state_with(&[70_000, 90_000, 150_000]);
        assert_eq!(
            tier(&odd, &config, false, &Default::default(), false),
            vec!["s0", "s1"]
        );
        // Small ones are left to their own tiers.
        assert!(
            tier(
                &state_with(&[300, 50_000]),
                &config,
                false,
                &Default::default(),
                false,
            )
            .is_empty()
        );
        assert!(
            tier(
                &state_with(&[110_000, 150_000]),
                &config,
                false,
                &Default::default(),
                false,
            )
            .is_empty()
        );
    }
}
