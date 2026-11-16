//! Epoch-fenced index publication, tiered compaction, and grace-period reclamation.
use crate::{Engine, NamespaceState, catch_up, log_error, segment, snapshot, storage_error};
use bf_types::ApiError;
use bf_wal::{IndexerLease, Manifest, NamespaceStats, RetiredObject, Wal};
use chrono::Utc;
use std::{collections::BTreeMap, time::Duration};

/// Node-local indexing and query tuning. No option changes the durable write ordering.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Byte budget for decoded immutable segment metadata (charged at resident size).
    /// Zero disables admission. In-flight queries can pin evicted entries until completion.
    pub decoded_cache_bytes: u64,
    /// Maximum concurrent segment scoring tasks across all queries on this engine.
    pub search_concurrency: usize,
    /// Maximum concurrent query drivers; each runs outside the Tokio reactor.
    pub query_concurrency: usize,
    /// Disable indexing to keep a WAL-only differential reference node.
    pub indexing: bool,
    /// Minimum distinct tail mutations before a background flush.
    pub index_rows: usize,
    /// Minimum logical WAL bytes before a background flush.
    pub index_bytes: u64,
    /// Flush small tails after this age.
    pub index_age: Duration,
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
    /// Eventual reads include at most this many logical WAL bytes.
    pub eventual_tail_bytes: u64,
    /// Segments built concurrently when one flush or compaction yields several.
    pub build_concurrency: usize,
    /// IVF leaf probes for ANN queries.
    pub ann_nprobe: usize,
    /// Exact rerank budget as a multiple of the requested k.
    pub ann_rerank_factor: usize,
    /// A filtered ANN query with at most this many allowed rows fetches and ranks exactly
    /// those rows' vectors instead of scanning quantized codes.
    pub exact_fetch_rows: usize,
    /// A filtered ANN query that scans codes probes up to this many times as many clusters as
    /// an unfiltered one, in proportion to how few rows the filter leaves (recall on filtered
    /// sets needs a deeper probe than the allowed fraction alone suggests).
    pub filtered_probe_cap: usize,
    /// Fraction of ANN requests checked against exhaustive ground truth.
    pub recall_sample_rate: f64,
}
/// An eighth of the memory this process may use (host RAM or the cgroup limit), within
/// 512 MiB to 4 GiB. Entries are charged at their resident size, so this bounds real memory.
fn default_decoded_cache_bytes() -> u64 {
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
        .map_or(4 << 30, |kb| kb.saturating_mul(1024));
    let limit = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(host);
    (host.min(limit) / 8).clamp(512 << 20, 4 << 30)
}
impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            decoded_cache_bytes: default_decoded_cache_bytes(),
            search_concurrency: 8,
            query_concurrency: 8,
            indexing: true,
            index_rows: 1000,
            index_bytes: 8 * 1024 * 1024,
            index_age: Duration::from_secs(5),
            index_interval: Duration::from_millis(250),
            compaction_segments: 4,
            max_segment_rows: 200_000,
            max_segment_bytes: 2 << 30,
            retention: Duration::from_secs(3600),
            lease_duration: Duration::from_secs(30),
            max_unindexed_bytes: 2_000_000_000,
            eventual_tail_bytes: 128 * 1024 * 1024,
            build_concurrency: 2,
            ann_nprobe: 64,
            ann_rerank_factor: 32,
            exact_fetch_rows: 8192,
            filtered_probe_cap: 3,
            recall_sample_rate: 0.01,
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
        if let Some(v) = number("BF_INDEX_INTERVAL_MS") {
            c.index_interval = Duration::from_millis(v);
        }
        if let Some(v) = number("BF_MAX_UNINDEXED_BYTES") {
            c.max_unindexed_bytes = v;
        }
        if let Some(v) = number("BF_RETENTION_SECS") {
            c.retention = Duration::from_secs(v);
        }
        if let Some(v) = number("BF_ANN_NPROBE") {
            c.ann_nprobe = v as usize;
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
        if let Some(v) = number("BF_FILTER_PROBE_CAP") {
            c.filtered_probe_cap = v as usize;
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
    tokio::spawn(async move {
        let mut last_gc = std::time::Instant::now();
        let mut discover = true;
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
                if gc_due && let Err(e) = engine.collect_garbage(&ns).await {
                    tracing::warn!(error=%e,"background garbage collection deferred");
                }
            }
            if gc_due {
                last_gc = std::time::Instant::now();
            }
        }
    });
}
fn retire(manifest: &mut Manifest, key: String) {
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
    let bytes = crate::text::encode_text_stats(&text)?;
    manifest.text_stats = Some(
        segment::put_file(
            store,
            format!("ns/{ns}/segments/text-stats-{}.bfs", uuid::Uuid::new_v4()),
            bytes,
            0,
        )
        .await?,
    );
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
        let context = crate::read_cache::ReadContext {
            cache: self.0.read_cache.clone(),
            stats: Default::default(),
            backend: Default::default(),
        };
        crate::read_cache::scope(context, self.index_namespace_inner(ns, force)).await
    }
    async fn index_namespace_inner(&self, ns: &str, force: bool) -> Result<bool, ApiError> {
        let t0 = std::time::Instant::now();
        let handle = self.handle(ns).await?;
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        let state = {
            let mut state = handle.state.lock().await;
            // Local commits and queued compaction are already known. Poll remote
            // writers at the configured maximum tail age while idle, rather than
            // holding this lock across two object-store calls on every tick.
            if !force
                && state.seq > 0
                && state.seq == state.indexed_through
                && state.refreshed.elapsed() < self.0.config.index_age
                && tier(&state, &self.0.config).is_empty()
            {
                return Ok(false);
            }
            catch_up(&wal, &mut state, &self.0.store).await?;
            // Decide whether there is work before cloning: the tail can hold hundreds of MiB,
            // and a clone under this lock stalls every concurrent write.
            if !state.exists {
                return Ok(false);
            }
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
            let will_compact = tier(&state, &self.0.config);
            if state.seq == state.indexed_through && will_compact.is_empty() && !will_rebuild {
                return Ok(false);
            }
            let tail_age = Utc::now()
                .signed_duration_since(state.tail_since.unwrap_or(state.updated_at))
                .to_std()
                .unwrap_or_default();
            if !force
                && !will_rebuild
                && will_compact.is_empty()
                && state.shadowed.len() < self.0.config.index_rows
                && state.tail_bytes < self.0.config.index_bytes
                && tail_age < self.0.config.index_age
            {
                return Ok(false);
            }
            state.clone()
        };
        if !state.exists {
            return Ok(false);
        }
        crate::ann::stage(t0, "idx lock+clone");
        // The indexer rewrites complete statistics, so it needs the out-of-line text part.
        let mut state = state;
        crate::text::hydrate(&self.0.store, &mut state).await?;
        crate::ann::stage(t0, "idx hydrate");
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
        let compact = tier(&state, &self.0.config);
        if state.seq == state.indexed_through && compact.is_empty() && !rebuild {
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
            && compact.is_empty()
            && state.shadowed.len() < self.0.config.index_rows
            && state.tail_bytes < self.0.config.index_bytes
            && age < self.0.config.index_age
        {
            return Ok(false);
        }
        let mut manifest = old.map(|(_, m)| m).unwrap_or_else(|| Manifest {
            format_version: 1,
            schema: state.schema.clone(),
            distance_metric: state.metric,
            id_type: state.schema["id"].data_type.clone().expect("id schema"),
            created_at: state.created_at,
            indexed_through: 0,
            indexer_epoch: 0,
            deleted: false,
            segments: vec![],
            stats: Default::default(),
            text_stats: None,
            ann_dirs: Default::default(),
            updated_at: None,
            retired: vec![],
            checkpoints: vec![],
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
        // A tombstone followed by recreation discards every old segment and its statistics.
        if state.segments.is_empty() && !manifest.segments.is_empty() {
            for seg in manifest.segments.clone() {
                for file in seg.files.values().chain(std::iter::once(&seg.live)) {
                    retire(&mut manifest, file.key.clone());
                }
            }
            manifest.segments.clear();
            manifest.stats = NamespaceStats::default();
        }
        let segments = if rebuild {
            let mut rebuilt = vec![];
            manifest.stats = NamespaceStats::default();
            for seg in &state.segments {
                let live = snapshot::allowed(store, &state, seg, None).await?;
                let mut docs = segment::documents(store, seg, &live).await?;
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
                            seg.wal_start,
                            seg.wal_end,
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
                        state.indexed_through + 1,
                        state.seq,
                        &self.0.config,
                    )
                    .await?,
                );
            }
            rebuilt
        } else {
            let mut segments = vec![];
            crate::ann::stage(t0, "idx pre-live");
            for mut seg in state.segments.clone() {
                let live = segment::live(store, &seg).await?;
                let keep = snapshot::allowed(store, &state, &seg, None).await?;
                let deleted = &live - &keep;
                if !deleted.is_empty() {
                    let removed = segment::documents(store, &seg, &deleted).await?;
                    adjust(&mut manifest.stats, &removed, false, &state.schema);
                    retire(&mut manifest, seg.live.key.clone());
                    if keep.is_empty() {
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
            let mut compact_docs = BTreeMap::new();
            let mut start = u64::MAX;
            let mut end = 0;
            if !compact.is_empty() {
                let mut remaining = vec![];
                for seg in segments {
                    if compact.contains(&seg.id) {
                        let live = segment::live(store, &seg).await?;
                        compact_docs.extend(segment::documents(store, &seg, &live).await?);
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
                let mut stats = std::mem::take(&mut manifest.stats);
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
                        state.indexed_through + 1,
                        state.seq,
                        &self.0.config,
                    )
                    .await
                } else {
                    Ok(vec![])
                }
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
                        start,
                        end,
                        &self.0.config,
                    )
                    .await
                }
            };
            let (flushed, compacted) = futures::future::try_join(flush, compacted).await?;
            crate::ann::stage(t0, "idx builds");
            if let Some(job) = stats_job {
                manifest.stats = job.await.map_err(segment::index_error)?;
            }
            segments.extend(flushed);
            segments.extend(compacted);
            segments
        };
        manifest.deleted = false;
        manifest.segments = segments;
        // Rebuild each vector attribute's ANN directory only when its segment list changed.
        let attributes: std::collections::BTreeSet<String> = manifest
            .segments
            .iter()
            .flat_map(|s| s.files.keys())
            .filter(|k| k.starts_with("ann:"))
            .cloned()
            .collect();
        let previous_dirs = std::mem::take(&mut manifest.ann_dirs);
        for key in attributes {
            let built =
                crate::ann_dir::build(store, ns, &manifest.segments, &key, previous_dirs.get(&key))
                    .await?;
            if let Some(file) = built {
                manifest.ann_dirs.insert(key, file);
            }
        }
        for old in previous_dirs.values() {
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
        if let Some(previous) = previous_text {
            retire(&mut manifest, previous.key);
        }
        externalize_text_stats(store, ns, &mut manifest).await?;
        crate::ann::stage(t0, "idx text_stats");
        match wal
            .publish_fenced(claim, manifest.indexer_epoch, &manifest)
            .await
        {
            Ok(_) => {}
            Err(bf_wal::Error::Fenced) => return Ok(false),
            Err(e) => return Err(log_error(e)),
        }
        crate::ann::stage(t0, "idx publish");
        let mut current = handle.state.lock().await;
        catch_up(&wal, &mut current, &self.0.store).await?;
        crate::ann::stage(t0, "idx done");
        Ok(true)
    }
    /// Reclaim retired files and checkpointed WAL payloads after the retention window.
    /// Permanent WAL slot records continue to fence every stale writer.
    pub async fn collect_garbage(&self, ns: &str) -> Result<usize, ApiError> {
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        let Some((sequence, mut manifest)) = wal.latest_manifest().await.map_err(log_error)? else {
            return Ok(0);
        };
        let grace = chrono::Duration::from_std(self.0.config.retention)
            .unwrap_or(chrono::Duration::hours(1));
        let cutoff = Utc::now() - grace;
        let mut checkpoint = NamespaceState::default();
        catch_up(&wal, &mut checkpoint, &self.0.store).await?;
        if checkpoint.manifest_seq != sequence {
            return Ok(0);
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
            manifest.deleted = true;
            manifest.indexed_through = checkpoint.seq;
        }
        let mut protected = std::collections::BTreeSet::new();
        protected.extend(manifest.text_stats.iter().map(|f| f.key.clone()));
        protected.extend(manifest.ann_dirs.values().map(|f| f.key.clone()));
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
            format!("ns/{ns}/manifest-data/"),
            format!("ns/{ns}/indexer/"),
        ] {
            for object in self.0.store.list(&prefix).await.map_err(storage_error)? {
                if object.last_modified <= cutoff && !protected.contains(&object.key) {
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
            return Ok(0);
        }
        let epoch = manifest.indexer_epoch;
        manifest.indexer_epoch += 1;
        let claim = match wal.publish_fenced(sequence, epoch, &manifest).await {
            Ok(s) => s,
            Err(bf_wal::Error::Fenced) => return Ok(0),
            Err(e) => return Err(log_error(e)),
        };
        // Reread durable pins AFTER the epoch claim. A new copier either pinned
        // before this read, or observes the new epoch and retries its snapshot.
        let shared = crate::branch::protected(&self.0.store).await?;
        candidates.retain(|key| !shared.contains(key));
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
            wal.prune_payloads(through).await.map_err(log_error)?;
        }
        let Some((latest, mut manifest)) = wal.latest_manifest().await.map_err(log_error)? else {
            return Ok(candidates.len());
        };
        if latest != claim {
            return Ok(candidates.len());
        }
        manifest
            .retired
            .retain(|file| !candidates.contains(&file.key));
        manifest.checkpoints.retain(|(seq, _)| *seq > through);
        match wal
            .publish_fenced(claim, manifest.indexer_epoch, &manifest)
            .await
        {
            Ok(_) | Err(bf_wal::Error::Fenced) => {}
            Err(e) => return Err(log_error(e)),
        }
        Ok(candidates.len())
    }
}
fn tier(state: &NamespaceState, config: &EngineConfig) -> Vec<String> {
    let count = config.compaction_segments.max(2);
    let mut tiers: BTreeMap<u32, (Vec<String>, u64, u64)> = BTreeMap::new();
    for seg in &state.segments {
        let bucket = seg.row_count.max(1).ilog2();
        let group = tiers.entry(bucket).or_default();
        let bytes = seg.files.values().map(|file| file.size).sum::<u64>();
        if group.1.saturating_add(seg.row_count as u64) > config.max_segment_rows.max(1) as u64
            || group.2.saturating_add(bytes) > config.max_segment_bytes.max(1)
        {
            continue;
        }
        group.0.push(seg.id.clone());
        group.1 += seg.row_count as u64;
        group.2 += bytes;
        if group.0.len() == count {
            return group.0.clone();
        }
    }
    vec![]
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
    start: u64,
    end: u64,
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
    // Segments are independent: build a few at once, keeping their order.
    let mut out = Vec::with_capacity(batches.len());
    for group in batches.chunks(config.build_concurrency.max(1)) {
        let mut builds = Vec::with_capacity(group.len());
        for batch in group {
            builds.push(segment::build(store, ns, schema, metric, batch, start, end));
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
