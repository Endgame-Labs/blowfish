//! Epoch-fenced index publication, tiered compaction, and grace-period reclamation.
use crate::{Engine, NamespaceState, catch_up, log_error, segment, snapshot, storage_error};
use bf_types::ApiError;
use bf_wal::{IndexerLease, Manifest, NamespaceStats, RetiredObject, Wal};
use chrono::Utc;
use std::{collections::BTreeMap, time::Duration};

/// Node-local indexing and query tuning. No option changes the durable write ordering.
#[derive(Clone, Debug)]
pub struct EngineConfig {
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
    /// A retired object remains readable for at least this long.
    pub retention: Duration,
    /// Advisory lease duration; correctness relies solely on manifest fencing.
    pub lease_duration: Duration,
    /// Reject writes once unindexed logical WAL bytes reach this limit.
    pub max_unindexed_bytes: u64,
    /// Eventual reads include at most this many logical WAL bytes.
    pub eventual_tail_bytes: u64,
    /// IVF leaf probes for ANN queries.
    pub ann_nprobe: usize,
    /// Exact rerank budget as a multiple of the requested k.
    pub ann_rerank_factor: usize,
    /// Fraction of ANN requests checked against exhaustive ground truth.
    pub recall_sample_rate: f64,
}
impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            indexing: true,
            index_rows: 1000,
            index_bytes: 8 * 1024 * 1024,
            index_age: Duration::from_secs(5),
            index_interval: Duration::from_millis(250),
            compaction_segments: 4,
            retention: Duration::from_secs(3600),
            lease_duration: Duration::from_secs(30),
            max_unindexed_bytes: 2_000_000_000,
            eventual_tail_bytes: 128 * 1024 * 1024,
            ann_nprobe: 128,
            ann_rerank_factor: 64,
            recall_sample_rate: 0.01,
        }
    }
}
impl EngineConfig {
    /// Environment overrides let existing HTTP entrypoints exercise indexing unchanged.
    /// Invalid or zero threshold values retain defaults.
    pub fn from_env() -> Self {
        let mut c = Self::default();
        let number = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
        };
        if let Some(v) = number("BF_INDEX_ROWS") {
            c.index_rows = v as usize;
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
        if let Some(v) = number("BF_ANN_NPROBE") {
            c.ann_nprobe = v as usize;
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
        loop {
            tokio::time::sleep(interval).await;
            let Some(inner) = weak.upgrade() else { break };
            let engine = Engine(inner);
            let names: Vec<_> = engine.0.namespaces.lock().await.keys().cloned().collect();
            for ns in names {
                if let Err(e) = engine.index_namespace(&ns, false).await {
                    tracing::warn!(error=%e, "background indexing deferred");
                }
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
fn adjust(
    stats: &mut NamespaceStats,
    docs: &BTreeMap<bf_types::Id, bf_types::Document>,
    add: bool,
) {
    let rows = docs.len() as u64;
    let bytes: u64 = docs
        .values()
        .map(|d| serde_json::to_vec(d).expect("document serializes").len() as u64)
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
    /// `force` bypasses size, age, and advisory lease checks, but never epoch fencing.
    /// Returns false when there was no work or another indexer won the publication.
    pub async fn index_namespace(&self, ns: &str, force: bool) -> Result<bool, ApiError> {
        let handle = self.handle(ns).await?;
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        let state = {
            let mut state = handle.state.lock().await;
            catch_up(&wal, &mut state).await?;
            state.clone()
        };
        if !state.exists {
            return Ok(false);
        }
        let compact = tier(&state, self.0.config.compaction_segments.max(2));
        if state.seq == state.indexed_through && compact.is_empty() {
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
            segments: vec![],
            stats: Default::default(),
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
        let store = &self.0.store;
        manifest.stats = state.stats.clone();
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
        let mut segments = vec![];
        for mut seg in state.segments.clone() {
            let live = segment::live(store, &seg).await?;
            let keep = snapshot::allowed(store, &state, &seg, None).await?;
            let deleted = &live - &keep;
            if !deleted.is_empty() {
                let removed = segment::documents(store, &seg, &deleted).await?;
                adjust(&mut manifest.stats, &removed, false);
                retire(&mut manifest, seg.live.key.clone());
                segment::set_live(store, &mut seg, &keep).await?;
            }
            segments.push(seg);
        }
        if state.seq > state.indexed_through && !state.docs.is_empty() {
            segments.push(
                segment::build(
                    store,
                    ns,
                    &state.schema,
                    &state.docs,
                    state.indexed_through + 1,
                    state.seq,
                )
                .await?,
            );
            adjust(&mut manifest.stats, &state.docs, true);
        }
        // Compaction only replaces selected disjoint live collections. New tail rows
        // are already masked out of old segments, so the merged result has unique IDs.
        if !compact.is_empty() {
            let mut docs = BTreeMap::new();
            let mut start = u64::MAX;
            let mut end = 0;
            let mut remaining = vec![];
            for seg in segments {
                if compact.contains(&seg.id) {
                    let live = segment::live(store, &seg).await?;
                    docs.extend(segment::documents(store, &seg, &live).await?);
                    start = start.min(seg.wal_start);
                    end = end.max(seg.wal_end);
                    for file in seg.files.values().chain(std::iter::once(&seg.live)) {
                        retire(&mut manifest, file.key.clone());
                    }
                } else {
                    remaining.push(seg);
                }
            }
            if !docs.is_empty() {
                remaining.push(segment::build(store, ns, &state.schema, &docs, start, end).await?);
            }
            segments = remaining;
        }
        manifest.segments = segments;
        manifest.schema = state.schema.clone();
        manifest.distance_metric = state.metric;
        manifest.id_type = state.schema["id"].data_type.clone().expect("id schema");
        manifest.created_at = state.created_at;
        manifest.updated_at = Some(state.updated_at);
        manifest.indexed_through = state.seq;
        match wal
            .publish_fenced(claim, manifest.indexer_epoch, &manifest)
            .await
        {
            Ok(_) => {}
            Err(bf_wal::Error::Fenced) => return Ok(false),
            Err(e) => return Err(log_error(e)),
        }
        let mut current = handle.state.lock().await;
        catch_up(&wal, &mut current).await?;
        Ok(true)
    }
    /// Reclaim retired files and checkpointed WAL payloads after the retention window.
    /// Permanent WAL slot records continue to fence every stale writer.
    pub async fn collect_garbage(&self, ns: &str) -> Result<usize, ApiError> {
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        let Some((_, manifest)) = wal.latest_manifest().await.map_err(log_error)? else {
            return Ok(0);
        };
        let grace = chrono::Duration::from_std(self.0.config.retention)
            .unwrap_or(chrono::Duration::hours(1));
        let cutoff = Utc::now() - grace;
        let mut deleted = 0;
        for file in &manifest.retired {
            if file.retired_at <= cutoff {
                self.0
                    .store
                    .delete(&file.key)
                    .await
                    .map_err(storage_error)?;
                deleted += 1;
            }
        }
        // Publication time, not the last indexed write time, starts the WAL grace period.
        // Use a retained manifest old enough to be safe; newer checkpoints may still
        // be needed by an in-flight reader of their predecessors.
        let through = manifest
            .checkpoints
            .iter()
            .filter(|(_, at)| *at <= cutoff)
            .map(|(seq, _)| *seq)
            .max()
            .unwrap_or(0);
        if through > 0 {
            wal.prune_payloads(through).await.map_err(log_error)?;
        }
        Ok(deleted)
    }
}
fn tier(state: &NamespaceState, count: usize) -> Vec<String> {
    let mut tiers: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for seg in &state.segments {
        let bucket = seg.row_count.max(1).ilog2();
        let group = tiers.entry(bucket).or_default();
        group.push(seg.id.clone());
        if group.len() == count {
            return group.clone();
        }
    }
    vec![]
}
