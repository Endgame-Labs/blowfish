//! The automatic recluster (`docs/design/auto-recluster.md`): when a namespace gets a new tier,
//! how the build runs beside indexing and queries, and how the indexer installs it.
//!
//! - **What.** A vector attribute has at most two tiers: a base, and a delta over rows written
//!   since, built against the base's coarse cells. Rows in neither are in uncovered segments.
//! - **When.** After each publication the background indexer evaluates the trigger from manifest
//!   numbers only ([`decide`]). A namespace with no tier gets a base once its layout would gain
//!   from it. With a base, the rows outside the tiers get a delta (rebuilt with them) once they
//!   reach a small share of the base, and the delta merges into a new base once it reaches half
//!   of it. A tier in an older format is rebuilt. While a bulk load runs, builds wait.
//! - **How.** One build per process, on half the cores, waiting between slabs while the indexer
//!   flushes or compacts. It reserves memory from the engine's budget and checks the scratch disk
//!   first, and defers (counting the reason) when either is short.
//! - **Safely.** The build reads a manifest snapshot and claims nothing. A create-only build record,
//!   rewritten every ten minutes until the tier is installed or dropped, keeps GC away from its
//!   objects. The finished tier waits in memory until the indexer's next pass, which checks that
//!   the segments it covers are still live (and, for a delta, that its base is still installed)
//!   and installs it in its own fenced publication; then the records are deleted. Compaction
//!   leaves the segments of a running or waiting build alone, so the check only fails when
//!   another node changed them.
use crate::{Engine, log_error, recluster, storage_error};
use bf_types::ApiError;
use bf_wal::{Manifest, SlabSet, Wal};
use std::{
    collections::{BTreeSet, HashMap},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

/// Below this many rows a namespace is scanned well enough as loaded.
const MIN_ROWS: u64 = 100_000;
/// A first base is built once it would scan this many times fewer rows.
const BASE_GAIN: f64 = 1.25;
/// Rows outside the tiers get a delta once they reach this share of the base's rows, and this
/// share of the delta's (so a growing delta is rebuilt a bounded number of times).
const DELTA_SHARE: f64 = 0.04;
const DELTA_GROWTH: f64 = 0.25;
/// The delta (with the rows outside it) merges into a new base at this share of the base, and
/// the tiers are rebuilt once deletes and updates mask this share of their rows.
const MERGE_SHARE: f64 = 0.5;
const MASKED_SHARE: f64 = 0.2;
/// A merge retrains the coarse cells once the rows have grown this many times past the rows
/// they were trained over.
const COARSE_GROWTH: u64 = 4;
/// A namespace whose indexer flushed a bulk-load-sized tail this recently is still loading.
const BULK_WINDOW: Duration = Duration::from_secs(300);
/// How soon a deferred evaluation runs again, though nothing was published since.
const RETRY_DEFERRED: Duration = Duration::from_secs(60);
/// How soon a failed build is tried again.
const RETRY_FAILED: Duration = Duration::from_secs(600);
/// Builder memory per row of a slab (1,024 dimensions in f16: the packed rows, the blob being
/// built, and the previous slab's blob still uploading), and the slab size bounds.
const SLAB_ROW_BYTES: u64 = 12_000;
const MIN_SLAB_ROWS: usize = 50_000;
const MAX_SLAB_ROWS: usize = 700_000;
/// What a segment build or compaction needs per row while it runs (see `default_max_segment_rows`).
const SEGMENT_ROW_BYTES: u64 = 30_000;
/// The spill file needs this much more free disk than its size.
const DISK_MARGIN: f64 = 1.1;
/// A build record is rewritten this often; GC honors one for twice as long.
pub(crate) const RECORD_INTERVAL: Duration = Duration::from_secs(600);
/// A finished tier the indexer could not install within this long is dropped.
const INSTALL_TIMEOUT: Duration = Duration::from_secs(3600);

/// Whether a build is running in this process (one at a time, across scoped engines).
static RUNNING: AtomicBool = AtomicBool::new(false);
/// Index passes doing work right now, across the process. Builds yield to them.
static INDEXING: AtomicUsize = AtomicUsize::new(0);

/// Held by an index pass while it builds or compacts.
pub(crate) struct Indexing(());
impl Drop for Indexing {
    fn drop(&mut self) {
        INDEXING.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(crate) fn indexing() -> Indexing {
    INDEXING.fetch_add(1, Ordering::AcqRel);
    Indexing(())
}

/// What the trigger decided for a namespace.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Decision {
    /// Nothing to gain.
    Nothing,
    /// A build would help, but waits for this reason.
    Wait(&'static str),
    /// Build a tier for this attribute, for this reason (`delta` builds a delta tier; every
    /// other reason a new base over all rows).
    Build(String, &'static str),
}

/// Which tier a build makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A base over every row, replacing the attribute's tiers.
    Base,
    /// A delta over the current delta's rows and the rows outside the tiers, against the base's
    /// coarse cells.
    Delta,
}
impl Kind {
    fn of(reason: &str) -> Self {
        if reason == "delta" {
            Self::Delta
        } else {
            Self::Base
        }
    }
}

/// The layout numbers the trigger reads, for one vector attribute.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Layout {
    pub covered_rows: u64,
    pub base_rows: u64,
    pub delta_rows: u64,
    pub uncovered_rows: u64,
    pub uncovered_segments: usize,
    /// Rows the tiers hold that were deleted or updated since (estimated from the namespace's
    /// live rows: what is not live outside the tiers).
    pub masked_rows: u64,
    /// Rows a default query scans now over rows it would scan after a rebuild (1 when there is
    /// nothing to gain, infinity for a tier in an older format).
    pub gain: f64,
}

/// The trigger's numbers for `attr` (see the design's gain estimate). For uncovered segments a
/// default query probes `min(S^0.3, 4)` times the clusters, each holding about `sqrt(rows)` of a
/// segment's rows; after a rebuild it probes leaves of `CLUSTER_ROWS`.
pub(crate) fn layout(
    segments: &[bf_wal::Segment],
    lineage: &[bf_wal::Lineage],
    tiers: Option<&Vec<SlabSet>>,
    attr: &str,
) -> Layout {
    let tiers = tiers.map(Vec::as_slice).unwrap_or_default();
    // Segments whose rows the tiers hold, themselves or merged into them.
    let covered = crate::lineage::covered(
        segments,
        &std::collections::BTreeMap::from([(attr.to_string(), tiers.to_vec())]),
        lineage,
    );
    let key = format!("annv2:{attr}");
    let uncovered: Vec<u64> = segments
        .iter()
        .filter(|s| s.files.contains_key(&key) && !covered.contains(&s.id))
        .map(|s| u64::from(s.row_count))
        .filter(|n| *n > 0)
        .collect();
    let covered_rows: u64 = tiers.iter().map(SlabSet::rows).sum();
    let uncovered_rows: u64 = uncovered.iter().sum();
    let mut out = Layout {
        covered_rows,
        base_rows: tiers.first().map_or(0, SlabSet::rows),
        delta_rows: tiers.get(1).map_or(0, SlabSet::rows),
        uncovered_rows,
        uncovered_segments: uncovered.len(),
        masked_rows: 0,
        gain: 1.0,
    };
    if tiers.iter().any(|t| t.format < bf_wal::TIER_FORMAT_VERSION) {
        out.gain = f64::INFINITY;
        return out;
    }
    let rows = covered_rows + uncovered_rows;
    if rows < MIN_ROWS || (tiers.is_empty() && uncovered.len() < 2) || uncovered_rows == 0 {
        return out;
    }
    let (sum, squares, roots) = uncovered.iter().fold((0f64, 0f64, 0f64), |(s, q, r), n| {
        let n = *n as f64;
        (s + n, q + n * n, r + n.sqrt())
    });
    let spread = (sum * sum / squares).max(1.0).powf(0.3).min(4.0);
    let per_cluster = sum / roots.max(1.0);
    let leaf = recluster::CLUSTER_ROWS as f64;
    // Rows scanned per probe now (tiers' leaves, plus the uncovered part's coarser clusters), over
    // rows scanned per probe after.
    let tier_part = if tiers.is_empty() { 0.0 } else { leaf };
    out.gain = (tier_part + spread * per_cluster) / leaf;
    out
}

/// The trigger, from manifest numbers only. `live_rows` is the namespace's live row count (when
/// known), `bulk` says a bulk load flushed recently, and `compacting` that a merge is pending (a
/// build would cover segments about to disappear).
pub(crate) fn decide(
    segments: &[bf_wal::Segment],
    tiers: &std::collections::BTreeMap<String, Vec<SlabSet>>,
    lineage: &[bf_wal::Lineage],
    live_rows: Option<u64>,
    bulk: bool,
    compacting: bool,
) -> (Decision, Option<Layout>) {
    // Vector attributes every segment has a current ANN object for (an older layout is upgraded
    // first, by `reindex`).
    let attrs: BTreeSet<String> = segments
        .iter()
        .flat_map(|s| s.files.keys())
        .filter_map(|k| k.strip_prefix("annv2:").map(str::to_string))
        .filter(|a| {
            segments
                .iter()
                .all(|s| s.files.contains_key(&format!("annv2:{a}")))
        })
        .collect();
    for attr in attrs {
        let tiered = tiers.get(&attr).filter(|t| !t.is_empty());
        let mut l = layout(segments, lineage, tiered, &attr);
        if let Some(live) = live_rows {
            l.masked_rows = l
                .covered_rows
                .saturating_sub(live.saturating_sub(l.uncovered_rows));
        }
        let why = match tiered {
            _ if l.gain.is_infinite() => "upgrade",
            None if l.gain >= BASE_GAIN => "base",
            None => continue,
            Some(_) => match next_tier(&l) {
                Some(why) => why,
                None => continue,
            },
        };
        // A load-then-query namespace builds once its load ends, plus doubling builds while the
        // load outgrows what is covered.
        if bulk && l.uncovered_rows <= l.covered_rows && why != "upgrade" {
            return (Decision::Wait("bulk_load"), Some(l));
        }
        if compacting {
            return (Decision::Wait("compaction"), Some(l));
        }
        return (Decision::Build(attr, why), Some(l));
    }
    (Decision::Nothing, None)
}

/// With a base installed: a merge once the delta and the rows outside it reach half the base, a
/// delta once the rows outside the tiers reach [`DELTA_SHARE`] of the base and
/// [`DELTA_GROWTH`] of the delta, a new base once [`MASKED_SHARE`] of the tiers' rows are
/// masked, and otherwise nothing.
fn next_tier(l: &Layout) -> Option<&'static str> {
    let (base, delta, outside) = (
        l.base_rows as f64,
        l.delta_rows as f64,
        l.uncovered_rows as f64,
    );
    if l.uncovered_rows == 0 && l.delta_rows == 0 && l.masked_rows == 0 {
        return None;
    }
    if delta + outside >= MERGE_SHARE * base {
        return Some("merge");
    }
    // A delta first: it takes seconds and covers the new rows, where a masked rebuild takes as
    // long as a base build, and steady updates would otherwise leave their rows outside the
    // tiers for all of it.
    if l.uncovered_rows > 0 && outside >= (DELTA_SHARE * base).max(DELTA_GROWTH * delta) {
        return Some("delta");
    }
    (l.masked_rows as f64 >= MASKED_SHARE * l.covered_rows as f64).then_some("masked")
}

/// The tier a foreground run builds whatever the thresholds say: a base when there is none or a
/// merge is due, else a delta when rows are outside the tiers, else a new base.
fn forced_kind(
    segments: &[bf_wal::Segment],
    lineage: &[bf_wal::Lineage],
    tiers: Option<&Vec<SlabSet>>,
    attr: &str,
) -> Kind {
    let Some(tiers) = tiers.filter(|t| !t.is_empty()) else {
        return Kind::Base;
    };
    let l = layout(segments, lineage, Some(tiers), attr);
    if l.gain.is_infinite() || l.uncovered_rows == 0 || next_tier(&l) == Some("merge") {
        Kind::Base
    } else {
        Kind::Delta
    }
}

/// What a build reads and makes: its kind, the segments whose rows it covers (in manifest order),
/// the coarse cells it reuses, and for a delta the directory key of the base it was built against.
pub(crate) struct Target {
    pub kind: Kind,
    pub segments: Vec<bf_wal::Segment>,
    pub coarse: Option<(bf_wal::SegmentFile, u64)>,
    pub base_dir: Option<String>,
}

/// The build `kind` makes for `attr` over `manifest`.
fn target(manifest: &Manifest, attr: &str, kind: Kind) -> Target {
    let tiers = manifest
        .ann_tiers
        .get(attr)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let base = tiers
        .first()
        .filter(|b| b.format == bf_wal::TIER_FORMAT_VERSION);
    let all: u64 = manifest
        .segments
        .iter()
        .map(|s| u64::from(s.row_count))
        .sum();
    match (kind, base) {
        (Kind::Delta, Some(base)) if base.coarse.is_some() => {
            // Rows the base holds, in segments it covers or merged ones' targets.
            let in_base = crate::lineage::covered(
                &manifest.segments,
                &std::collections::BTreeMap::from([(attr.to_string(), vec![base.clone()])]),
                &manifest.lineage,
            );
            let key = format!("annv2:{attr}");
            Target {
                kind,
                segments: manifest
                    .segments
                    .iter()
                    .filter(|s| s.files.contains_key(&key) && !in_base.contains(&s.id))
                    .cloned()
                    .collect(),
                coarse: base.coarse.clone().map(|f| (f, base.coarse_rows)),
                base_dir: Some(base.dir.key.clone()),
            }
        }
        // A delta needs a current base with its cells; anything else is a new base.
        _ => Target {
            kind: Kind::Base,
            segments: manifest.segments.clone(),
            coarse: base
                .and_then(|b| b.coarse.clone().map(|f| (f, b.coarse_rows)))
                .filter(|(_, rows)| all <= rows.saturating_mul(COARSE_GROWTH)),
            base_dir: None,
        },
    }
}

/// A finished tier waiting for the indexer's next publication.
pub(crate) struct Pending {
    attr: String,
    set: SlabSet,
    kind: Kind,
    /// For a delta, the directory key of the base it was built against.
    base_dir: Option<String>,
    built_at: Instant,
    outcome: tokio::sync::watch::Sender<Option<bool>>,
}

/// Per-engine job bookkeeping, keyed by namespace.
#[derive(Default)]
pub(crate) struct Jobs(std::sync::Mutex<JobsState>);
#[derive(Default)]
struct JobsState {
    /// Segments a running build covers.
    building: HashMap<String, BTreeSet<String>>,
    pending: HashMap<String, Pending>,
    /// The manifest generation each namespace's trigger last evaluated, and when to evaluate it
    /// again anyway (after a deferral or a failure; never while it stands).
    evaluated: HashMap<String, (u64, Option<Instant>)>,
}

impl Jobs {
    fn lock(&self) -> std::sync::MutexGuard<'_, JobsState> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Segments compaction must leave alone because a build covers them (running or waiting).
    pub(crate) fn protected(&self, ns: &str) -> BTreeSet<String> {
        let state = self.lock();
        let mut out = state.building.get(ns).cloned().unwrap_or_default();
        if let Some(p) = state.pending.get(ns) {
            out.extend(p.set.covered.iter().map(|c| c.segment.clone()));
        }
        out
    }
    pub(crate) fn has_pending(&self, ns: &str) -> bool {
        self.lock().pending.contains_key(ns)
    }
    /// Whether `ns`'s trigger should be evaluated at manifest `generation`: once a generation,
    /// unless a retry has come due. Marks it evaluated.
    fn evaluate(&self, ns: &str, generation: u64) -> bool {
        let mut state = self.lock();
        if let Some((seen, retry)) = state.evaluated.get(ns)
            && *seen == generation
            && retry.is_none_or(|at| Instant::now() < at)
        {
            return false;
        }
        state.evaluated.insert(ns.to_string(), (generation, None));
        true
    }
    /// Evaluate `ns` again after `wait`, even if nothing is published meanwhile: a deferral (a bulk
    /// load, compaction, memory, or disk) or a failure must not stand until the next write.
    fn retry_later(&self, ns: &str, wait: Duration) {
        if let Some(entry) = self.lock().evaluated.get_mut(ns) {
            entry.1 = Some(Instant::now() + wait);
        }
    }
    /// Take the namespace's waiting tier for installation in `manifest`. Returns a guard that puts
    /// it back unless [`Installing::published`] is called, or `None` when there is none (or it
    /// was dropped: its covered segments changed, its base was replaced, or it waited too long).
    pub(crate) fn install<'a>(
        &'a self,
        ns: &str,
        manifest: &mut Manifest,
    ) -> Option<Installing<'a>> {
        let pending = self.lock().pending.remove(ns)?;
        if pending.built_at.elapsed() > INSTALL_TIMEOUT {
            discard(ns, &pending, "timeout");
            return None;
        }
        let live: HashMap<&str, u32> = manifest
            .segments
            .iter()
            .map(|s| (s.id.as_str(), s.row_count))
            .collect();
        if !pending
            .set
            .covered
            .iter()
            .all(|c| live.get(c.segment.as_str()) == Some(&c.rows))
        {
            discard(ns, &pending, "coverage");
            return None;
        }
        let current = manifest
            .ann_tiers
            .get(&pending.attr)
            .cloned()
            .unwrap_or_default();
        // An earlier publication may already hold this tier (its reply was lost): nothing to retire.
        let (installed, replaced): (Vec<SlabSet>, Vec<SlabSet>) = match pending.kind {
            Kind::Base => (vec![pending.set.clone()], current),
            Kind::Delta => {
                let Some(base) = current
                    .first()
                    .filter(|b| Some(&b.dir.key) == pending.base_dir.as_ref())
                else {
                    discard(ns, &pending, "superseded");
                    return None;
                };
                (
                    vec![base.clone(), pending.set.clone()],
                    current.into_iter().skip(1).collect(),
                )
            }
        };
        // Objects the new tiers still use (a base built with its predecessor's cells) stay.
        let kept: BTreeSet<String> = installed.iter().flat_map(tier_keys).collect();
        for old in replaced
            .iter()
            .filter(|old| old.dir.key != pending.set.dir.key)
        {
            for key in tier_keys(old) {
                if !kept.contains(&key) {
                    crate::indexer::retire(manifest, key);
                }
            }
        }
        manifest.ann_tiers.insert(pending.attr.clone(), installed);
        Some(Installing {
            jobs: self,
            ns: ns.to_string(),
            pending: Some(pending),
        })
    }
    /// Hand a finished tier to the indexer; the receiver says whether it was installed.
    fn stage(
        &self,
        ns: &str,
        attr: &str,
        set: SlabSet,
        target: &Target,
    ) -> tokio::sync::watch::Receiver<Option<bool>> {
        let (outcome, rx) = tokio::sync::watch::channel(None);
        let mut state = self.lock();
        state.building.remove(ns);
        if let Some(old) = state.pending.insert(
            ns.to_string(),
            Pending {
                attr: attr.to_string(),
                set,
                kind: target.kind,
                base_dir: target.base_dir.clone(),
                built_at: Instant::now(),
                outcome,
            },
        ) {
            discard(ns, &old, "superseded");
        }
        rx
    }
}

/// Every object a tier references: its slabs, its directories, and its coarse cells.
pub(crate) fn tier_keys(set: &SlabSet) -> Vec<String> {
    let mut out: Vec<String> = set
        .slabs
        .iter()
        .flat_map(|s| s.files.values().map(|f| f.key.clone()))
        .collect();
    out.push(set.dir.key.clone());
    out.extend(set.pages.iter().map(|f| f.key.clone()));
    out.extend(set.coarse.iter().map(|f| f.key.clone()));
    out
}

fn discard(ns: &str, pending: &Pending, reason: &'static str) {
    tracing::warn!(namespace = ns, reason, "recluster: built tier discarded");
    metrics::counter!("blowfish_recluster_discarded_total", "reason" => reason).increment(1);
    let _ = pending.outcome.send(Some(false));
}

/// A tier being installed by an index pass.
pub(crate) struct Installing<'a> {
    jobs: &'a Jobs,
    ns: String,
    pending: Option<Pending>,
}
impl Installing<'_> {
    /// The publication that installed the tier succeeded.
    pub(crate) fn published(mut self) {
        if let Some(p) = self.pending.take() {
            let _ = p.outcome.send(Some(true));
        }
    }
}
impl Drop for Installing<'_> {
    fn drop(&mut self) {
        // The publication failed or was fenced: the next pass tries again.
        if let Some(p) = self.pending.take() {
            let mut state = self.jobs.lock();
            if !state.pending.contains_key(&self.ns) {
                state.pending.insert(self.ns.clone(), p);
            }
        }
    }
}

/// A running build: its identity, its build record, and its claim on its covered segments.
pub(crate) struct Job {
    id: String,
    slab_prefix: String,
    record: Option<tokio::task::JoinHandle<()>>,
    jobs: Option<(std::sync::Arc<crate::Inner>, String)>,
}
impl Job {
    /// The ID prefix of this build's slabs (`ns/<ns>/slabs/<prefix>...` holds its objects).
    pub(crate) fn slab_prefix(&self) -> &str {
        &self.slab_prefix
    }
    /// Wait while an index pass is flushing or compacting.
    pub(crate) async fn pause(&self) {
        while INDEXING.load(Ordering::Acquire) > 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    /// Stop refreshing the build record and delete it: the tier was installed (the manifest now
    /// protects its objects) or dropped (they are orphans for GC).
    async fn finish(mut self, store: &bf_storage::Storage, ns: &str) {
        if let Some(record) = self.record.take() {
            record.abort();
            let _ = record.await;
        }
        let records = format!("ns/{ns}/indexer/build-{}-", self.id);
        match store.list(&format!("ns/{ns}/indexer/")).await {
            Ok(objects) => {
                for object in objects.iter().filter(|o| o.key.starts_with(&records)) {
                    if let Err(e) = store.delete(&object.key).await {
                        tracing::warn!(error = %e, "recluster: build record not deleted");
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "recluster: build records not listed"),
        }
    }
}
impl Drop for Job {
    fn drop(&mut self) {
        if let Some(record) = self.record.take() {
            record.abort();
        }
        if let Some((inner, ns)) = self.jobs.take() {
            inner.jobs.lock().building.remove(&ns);
        }
    }
}

/// The build record's key and body. GC keeps objects under `prefix` while a record for it is
/// younger than twice [`RECORD_INTERVAL`].
fn record_key(ns: &str, id: &str, n: u64) -> String {
    format!("ns/{ns}/indexer/build-{id}-{n:06}.json")
}
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct BuildRecord {
    pub prefix: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
}

/// Object-key prefixes of builds whose records are fresh, and those records: GC must keep them.
pub(crate) async fn building_prefixes(
    store: &bf_storage::Storage,
    ns: &str,
) -> Result<Vec<String>, ApiError> {
    let fresh = chrono::Utc::now()
        - chrono::Duration::from_std(RECORD_INTERVAL * 2).unwrap_or(chrono::Duration::minutes(20));
    let mut out = Vec::new();
    // Listings match whole path segments, so list the directory and pick the records out.
    let records = format!("ns/{ns}/indexer/build-");
    for object in store
        .list(&format!("ns/{ns}/indexer/"))
        .await
        .map_err(storage_error)?
    {
        if !object.key.starts_with(&records) || object.last_modified <= fresh {
            continue;
        }
        // An unreadable record protects nothing; GC then treats its objects as orphans.
        if let Ok(bytes) = store.get(&object.key).await
            && let Ok(record) = serde_json::from_slice::<BuildRecord>(&bytes)
            && record.prefix.starts_with(&format!("ns/{ns}/slabs/"))
        {
            out.push(record.prefix);
            // The record itself, too, whatever the retention window.
            out.push(object.key);
        }
    }
    Ok(out)
}

/// Memory a slab build may use: the budget after the block cache, the decoded cache, the
/// unindexed limit, and a segment build if one is running, as rows of a slab; `None` below the floor.
fn slab_rows(engine: &Engine) -> Option<usize> {
    let config = &engine.0.config;
    let (cache, _) = engine.0.store.cache_limits();
    let in_flight = if INDEXING.load(Ordering::Acquire) > 0 {
        config.max_segment_rows as u64 * SEGMENT_ROW_BYTES
    } else {
        0
    };
    let available = bf_alloc::memory_limit_bytes()
        .saturating_sub(cache)
        .saturating_sub(config.decoded_cache_bytes)
        .saturating_sub(config.max_unindexed_bytes)
        .saturating_sub(in_flight);
    let rows = (available / SLAB_ROW_BYTES) as usize;
    (rows >= MIN_SLAB_ROWS).then_some(rows.min(MAX_SLAB_ROWS))
}

/// Where the spill file goes: the disk cache's directory when there is one, else the system's
/// temporary directory.
fn scratch_dir(engine: &Engine) -> std::path::PathBuf {
    let (_, disk) = engine.0.store.cache_limits();
    disk.unwrap_or_else(std::env::temp_dir)
        .join("blowfish-recluster")
}

/// Free bytes on the file system holding `path` (or its nearest existing parent).
fn free_bytes(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let mut at = path;
    while !at.exists() {
        at = at.parent()?;
    }
    let c = std::ffi::CString::new(at.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path and `stat` a writable statvfs.
    if unsafe { libc::statvfs(c.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    Some(stat.f_bavail as u64 * stat.f_frsize as u64)
}

/// Bytes of the spill file a build over `segments` writes (every row's vector at stored precision).
fn spill_bytes(manifest: &Manifest, segments: &[bf_wal::Segment], attr: &str) -> u64 {
    let rows: u64 = segments.iter().map(|s| u64::from(s.row_count)).sum();
    let dims = manifest
        .schema
        .get(attr)
        .and_then(crate::segment::dense_type)
        .map_or(1024 * 2, |(dims, dtype)| dims * dtype.width());
    rows * dims as u64
}

impl Engine {
    /// Evaluate the trigger for `ns` after an index pass, and start a build in the background if
    /// it says so. Costs nothing when nothing was published since the last evaluation.
    pub(crate) async fn consider_recluster(&self, ns: &str) {
        if RUNNING.load(Ordering::Acquire) || self.0.jobs.has_pending(ns) {
            return;
        }
        let Some(handle) = self.0.namespaces.lock().await.get(ns).cloned() else {
            return;
        };
        let protected = self.0.jobs.protected(ns);
        let (generation, decision, numbers) = {
            let state = handle.state.lock().await;
            if !state.exists || state.manifest_seq == 0 || state.root_only {
                return;
            }
            // A merge the indexer would run now. While a long write tail defers compaction (as
            // under steady writes), waiting for it would leave the new rows outside the tiers.
            let compacting =
                !crate::indexer::tier(&state, &self.0.config, false, &protected).is_empty();
            let bulk = state.bulk_load_at.is_some_and(|at| {
                (chrono::Utc::now() - at)
                    .to_std()
                    .is_ok_and(|since| since < BULK_WINDOW)
                    || at > chrono::Utc::now()
            });
            let (decision, numbers) = decide(
                &state.segments,
                &state.ann_tiers,
                &state.lineage,
                Some(state.stats.rows),
                bulk,
                compacting,
            );
            (state.manifest_seq, decision, numbers)
        };
        if !self.0.jobs.evaluate(ns, generation) {
            return;
        }
        let (attr, why) = match decision {
            Decision::Nothing => return,
            Decision::Wait(reason) => {
                defer(ns, reason, numbers);
                self.0.jobs.retry_later(ns, RETRY_DEFERRED);
                return;
            }
            Decision::Build(attr, why) => (attr, why),
        };
        if RUNNING.swap(true, Ordering::AcqRel) {
            // Another namespace's build started meanwhile: evaluate again after it.
            self.0.jobs.lock().evaluated.remove(ns);
            return;
        }
        let (engine, ns) = (self.clone(), ns.to_string());
        tokio::spawn(async move {
            let _running = Running;
            tracing::info!(
                namespace = ns.as_str(),
                attribute = attr.as_str(),
                reason = why,
                numbers = ?numbers,
                "recluster: starting a build"
            );
            let stop = engine.0.stop.clone();
            let Some(result) = stop.run(engine.background_build(&ns, &attr, why)).await else {
                // Its record stays until it is stale; the next start builds again.
                tracing::info!(
                    namespace = ns.as_str(),
                    "recluster: build stopped for shutdown"
                );
                return;
            };
            match result {
                Ok(Some(report)) => tracing::info!(
                    namespace = ns.as_str(),
                    rows = report.rows,
                    slabs = report.slabs,
                    seconds = report.seconds,
                    "recluster: tier installed"
                ),
                // Deferred (memory, disk, or another lease holder), or discarded.
                Ok(None) => engine.0.jobs.retry_later(&ns, RETRY_DEFERRED),
                Err(e) => {
                    engine.0.jobs.retry_later(&ns, RETRY_FAILED);
                    metrics::counter!("blowfish_recluster_builds_total", "outcome" => "failed")
                        .increment(1);
                    tracing::warn!(namespace = ns.as_str(), error = %e, "recluster: build failed");
                }
            }
        });
    }

    async fn background_build(
        &self,
        ns: &str,
        attr: &str,
        why: &'static str,
    ) -> Result<Option<recluster::ReclusterReport>, ApiError> {
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        let Some((_, manifest)) = wal.latest_manifest().await.map_err(log_error)? else {
            return Ok(None);
        };
        // Only the lease holder builds.
        if let Some(lease) = wal
            .indexer_lease(manifest.indexer_epoch)
            .await
            .map_err(log_error)?
            && lease.owner != self.0.owner
            && lease.expires_at > chrono::Utc::now()
        {
            return Ok(None);
        }
        let target = target(&manifest, attr, Kind::of(why));
        if target.segments.is_empty() {
            return Ok(None);
        }
        let Some(max_slab_rows) = slab_rows(self) else {
            defer(ns, "memory", None);
            return Ok(None);
        };
        let scratch = scratch_dir(self);
        let need = (spill_bytes(&manifest, &target.segments, attr) as f64 * DISK_MARGIN) as u64;
        if free_bytes(&scratch).is_none_or(|free| free < need) {
            defer(ns, "disk", None);
            return Ok(None);
        }
        let opts = recluster::ReclusterOptions {
            attribute: attr.to_string(),
            cells: recluster::CELLS,
            cluster_rows: recluster::CLUSTER_ROWS,
            max_slab_rows,
            scratch,
        };
        tracing::info!(
            namespace = ns,
            reason = why,
            kind = ?target.kind,
            segments = target.segments.len(),
            reuses_cells = target.coarse.is_some(),
            max_slab_rows,
            "recluster: building"
        );
        self.build_and_install(ns, &manifest, &target, &opts, false)
            .await
            .map(Some)
    }

    /// The job: build a tier for `target` over `manifest`, then wait until an index pass installs
    /// it. `drive` runs those index passes from here (a foreground run, or an engine whose
    /// background indexing is off); otherwise the background indexer runs them.
    pub(crate) async fn build_and_install(
        &self,
        ns: &str,
        manifest: &Manifest,
        target: &Target,
        opts: &recluster::ReclusterOptions,
        drive: bool,
    ) -> Result<recluster::ReclusterReport, ApiError> {
        let started = Instant::now();
        metrics::gauge!("blowfish_recluster_running").set(1.0);
        let _gauge = scopeguard(|| metrics::gauge!("blowfish_recluster_running").set(0.0));
        let job = self.start_job(ns, &target.segments).await?;
        let built = self.build_tier(ns, manifest, target, opts, &job).await?;
        let mut rx = self.0.jobs.stage(ns, &opts.attribute, built.set, target);
        let deadline = Instant::now() + INSTALL_TIMEOUT;
        let mut failures = 0;
        let installed = loop {
            if let Some(outcome) = *rx.borrow() {
                break outcome;
            }
            if Instant::now() > deadline {
                break false;
            }
            if drive {
                if let Err(e) = self.index_namespace(ns, true).await {
                    failures += 1;
                    tracing::warn!(error = %e, "recluster: install pass deferred");
                    if failures >= 20 {
                        // The tier stays staged, and its record keeps being refreshed until the
                        // job is dropped: a later index pass may still install it.
                        return Err(e);
                    }
                }
                if rx.borrow().is_none() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            } else {
                let _ = tokio::time::timeout(Duration::from_secs(5), rx.changed()).await;
            }
        };
        // Installed, the manifest protects the tier's objects; dropped, they are orphans.
        job.finish(&self.0.store, ns).await;
        let seconds = started.elapsed().as_secs_f64();
        metrics::histogram!("blowfish_recluster_build_seconds").record(seconds);
        if !installed {
            metrics::counter!("blowfish_recluster_builds_total", "outcome" => "discarded")
                .increment(1);
            return Err(ApiError::new(
                409,
                "the built tier was not installed: its segments or base changed meanwhile",
            ));
        }
        metrics::counter!("blowfish_recluster_builds_total", "outcome" => "installed").increment(1);
        metrics::counter!("blowfish_recluster_rows_total").increment(built.report.rows);
        Ok(recluster::ReclusterReport {
            seconds,
            ..built.report
        })
    }

    /// Register a build: its covered segments (compaction leaves them alone) and its first build
    /// record (written before any object is uploaded), refreshed every [`RECORD_INTERVAL`].
    async fn start_job(&self, ns: &str, segments: &[bf_wal::Segment]) -> Result<Job, ApiError> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let slab_prefix = format!("slab-{id}-");
        let record = BuildRecord {
            prefix: format!("ns/{ns}/slabs/{slab_prefix}"),
            started_at: chrono::Utc::now(),
        };
        let body = serde_json::to_vec(&record).map_err(crate::segment::index_error)?;
        let store = self.0.store.clone();
        store
            .create(&record_key(ns, &id, 0), body.clone())
            .await
            .map_err(storage_error)?;
        self.0.jobs.lock().building.insert(
            ns.to_string(),
            segments.iter().map(|s| s.id.clone()).collect(),
        );
        let (key_ns, key_id) = (ns.to_string(), id.clone());
        let refresh = tokio::spawn(async move {
            let mut n = 0u64;
            loop {
                tokio::time::sleep(RECORD_INTERVAL).await;
                n += 1;
                if let Err(e) = store
                    .create(&record_key(&key_ns, &key_id, n), body.clone())
                    .await
                {
                    tracing::warn!(error = %e, "recluster: build record not refreshed");
                }
            }
        });
        Ok(Job {
            id,
            slab_prefix,
            record: Some(refresh),
            jobs: Some((self.0.clone(), ns.to_string())),
        })
    }

    /// The foreground form of the job (see [`Engine::recluster`]).
    pub(crate) async fn run_recluster_job(
        &self,
        ns: &str,
        opts: &recluster::ReclusterOptions,
    ) -> Result<recluster::ReclusterReport, ApiError> {
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        let (_, manifest) = wal
            .latest_manifest()
            .await
            .map_err(log_error)?
            .ok_or_else(|| ApiError::new(404, "namespace has no checkpoint"))?;
        while RUNNING.swap(true, Ordering::AcqRel) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let _running = Running;
        let kind = forced_kind(
            &manifest.segments,
            &manifest.lineage,
            manifest.ann_tiers.get(&opts.attribute),
            &opts.attribute,
        );
        let target = target(&manifest, &opts.attribute, kind);
        self.0
            .stop
            .clone()
            .run(self.build_and_install(ns, &manifest, &target, opts, true))
            .await
            .unwrap_or_else(|| Err(ApiError::new(503, "the server is shutting down")))
    }
}

/// Count and log a deferred build.
fn defer(ns: &str, reason: &'static str, numbers: Option<Layout>) {
    metrics::counter!("blowfish_recluster_deferred_total", "reason" => reason).increment(1);
    tracing::info!(namespace = ns, reason, numbers = ?numbers, "recluster: deferred");
}

/// Clears [`RUNNING`] when a build ends, however it ends.
struct Running;
impl Drop for Running {
    fn drop(&mut self) {
        RUNNING.store(false, Ordering::Release);
    }
}

/// Run `f` on drop.
fn scopeguard(f: impl FnOnce()) -> impl Drop {
    struct Guard<F: FnOnce()>(Option<F>);
    impl<F: FnOnce()> Drop for Guard<F> {
        fn drop(&mut self) {
            if let Some(f) = self.0.take() {
                f();
            }
        }
    }
    Guard(Some(f))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generation_is_evaluated_once_unless_a_deferral_comes_due() {
        let jobs = Jobs::default();
        assert!(jobs.evaluate("n", 7));
        assert!(!jobs.evaluate("n", 7));
        // A deferral (say, a bulk load) evaluates the same generation again once it comes due.
        jobs.retry_later("n", Duration::from_secs(3600));
        assert!(!jobs.evaluate("n", 7));
        jobs.retry_later("n", Duration::ZERO);
        assert!(jobs.evaluate("n", 7));
        assert!(!jobs.evaluate("n", 7));
        // A new publication is always evaluated.
        assert!(jobs.evaluate("n", 8));
    }

    fn segment(id: &str, rows: u32) -> bf_wal::Segment {
        serde_json::from_value(serde_json::json!({
            "id": id, "wal_start": 0, "wal_end": 0, "row_count": rows,
            "files": {
                "annv2:v": {"key": format!("{id}/a"), "size": 1, "metadata_len": 0}
            },
            "live": {"key": "k", "size": 0, "metadata_len": 0},
            "live_generation": 0
        }))
        .unwrap()
    }
    fn tier(covered: &[(&str, u32)], format: u32) -> SlabSet {
        let mut base = 0;
        let covered: Vec<_> = covered
            .iter()
            .map(|(id, rows)| {
                let c = serde_json::json!({"segment": id, "base": base, "rows": rows});
                base += rows;
                c
            })
            .collect();
        let slab = segment("slab", base);
        serde_json::from_value(serde_json::json!({
            "covered": covered,
            "slabs": [slab],
            "dir": {"key": "d", "size": 1, "metadata_len": 0},
            "format": format
        }))
        .unwrap()
    }

    #[test]
    fn small_or_single_segment_namespaces_never_build() {
        let none = Default::default();
        let small = [segment("a", 30_000), segment("b", 30_000)];
        assert_eq!(
            decide(&small, &none, &[], None, false, false).0,
            Decision::Nothing
        );
        let single = [segment("a", 500_000)];
        assert_eq!(
            decide(&single, &none, &[], None, false, false).0,
            Decision::Nothing
        );
    }

    #[test]
    fn a_loaded_namespace_gets_a_base_once_compaction_and_the_load_settle() {
        let none = Default::default();
        let loaded: Vec<_> = (0..25)
            .map(|i| segment(&format!("s{i}"), 100_000))
            .collect();
        let (decision, numbers) = decide(&loaded, &none, &[], None, false, false);
        assert_eq!(decision, Decision::Build("v".into(), "base"));
        // 25 segments of 100K: 25^0.3 more probes of about 316-row clusters, against 200-row leaves.
        let gain = numbers.unwrap().gain;
        assert!((gain - 2.6 * 316.0 / 200.0).abs() < 0.2, "{gain}");
        assert_eq!(
            decide(&loaded, &none, &[], None, false, true).0,
            Decision::Wait("compaction")
        );
        // No base yet, so uncovered rows exceed covered ones: a load does not hold the first one.
        assert_eq!(
            decide(&loaded, &none, &[], None, true, false).0,
            Decision::Build("v".into(), "base")
        );
    }

    #[test]
    fn rows_outside_a_base_get_a_delta_that_merges_at_half_the_base() {
        let mut segments: Vec<_> = (0..4).map(|i| segment(&format!("s{i}"), 100_000)).collect();
        let covered: Vec<(&str, u32)> = vec![
            ("s0", 100_000),
            ("s1", 100_000),
            ("s2", 100_000),
            ("s3", 100_000),
        ];
        let base = tier(&covered, 1);
        let mut tiers = std::collections::BTreeMap::new();
        tiers.insert("v".to_string(), vec![base.clone()]);
        assert_eq!(
            decide(&segments, &tiers, &[], None, false, false).0,
            Decision::Nothing
        );
        // 2.5% of the base outside it: not yet.
        segments.push(segment("n0", 10_000));
        let (decision, numbers) = decide(&segments, &tiers, &[], None, false, false);
        assert_eq!(decision, Decision::Nothing);
        assert_eq!(numbers, None);
        // 5%: a delta.
        segments.push(segment("n1", 10_000));
        assert_eq!(
            decide(&segments, &tiers, &[], None, false, false).0,
            Decision::Build("v".into(), "delta")
        );
        // While a load runs, it waits until the load outgrows what is covered.
        assert_eq!(
            decide(&segments, &tiers, &[], None, true, false).0,
            Decision::Wait("bulk_load")
        );
        // With a 20K delta installed, the next one waits for 4% of the base again (the delta's
        // quarter is less).
        tiers.insert(
            "v".to_string(),
            vec![base.clone(), tier(&[("n0", 10_000), ("n1", 10_000)], 1)],
        );
        assert_eq!(
            decide(&segments, &tiers, &[], None, false, false).0,
            Decision::Nothing
        );
        segments.push(segment("n2", 16_000));
        assert_eq!(
            decide(&segments, &tiers, &[], None, false, false).0,
            Decision::Build("v".into(), "delta")
        );
        // A large delta is rebuilt only once a quarter of it is outside.
        tiers.insert(
            "v".to_string(),
            vec![base.clone(), tier(&[("n0", 100_000), ("n1", 60_000)], 1)],
        );
        segments.truncate(4);
        segments.extend([segment("n0", 100_000), segment("n1", 60_000)]);
        segments.push(segment("n2", 30_000));
        assert_eq!(
            decide(&segments, &tiers, &[], None, false, false).0,
            Decision::Nothing
        );
        segments.push(segment("n3", 10_000));
        let (decision, numbers) = decide(&segments, &tiers, &[], None, false, false);
        assert_eq!(decision, Decision::Build("v".into(), "merge"));
        let numbers = numbers.unwrap();
        assert_eq!(
            (
                numbers.base_rows,
                numbers.delta_rows,
                numbers.uncovered_rows
            ),
            (400_000, 160_000, 40_000)
        );
        // An older format rebuilds at once, load or not.
        tiers.insert("v".to_string(), vec![tier(&covered, 0)]);
        segments.truncate(4);
        assert_eq!(
            decide(&segments, &tiers, &[], None, true, false).0,
            Decision::Build("v".into(), "upgrade")
        );
    }

    #[test]
    fn a_base_whose_rows_are_a_fifth_masked_is_rebuilt() {
        let segments: Vec<_> = (0..4).map(|i| segment(&format!("s{i}"), 100_000)).collect();
        let mut tiers = std::collections::BTreeMap::new();
        tiers.insert(
            "v".to_string(),
            vec![tier(
                &[
                    ("s0", 100_000),
                    ("s1", 100_000),
                    ("s2", 100_000),
                    ("s3", 100_000),
                ],
                1,
            )],
        );
        let at = |live| decide(&segments, &tiers, &[], Some(live), false, false);
        assert_eq!(at(400_000).0, Decision::Nothing);
        assert_eq!(at(330_000).0, Decision::Nothing);
        let (decision, numbers) = at(320_000);
        assert_eq!(decision, Decision::Build("v".into(), "masked"));
        assert_eq!(numbers.unwrap().masked_rows, 80_000);
        // Rows outside the tier are live rows the tier does not mask.
        let mut more = segments.clone();
        more.push(segment("n", 10_000));
        let (decision, numbers) = decide(&more, &tiers, &[], Some(340_000), false, false);
        assert_eq!(decision, Decision::Nothing);
        assert_eq!(numbers, None);
        // With both due, the delta goes first (steady updates mask the base as fast as they
        // write rows outside it).
        more.push(segment("m", 10_000));
        let (decision, numbers) = decide(&more, &tiers, &[], Some(330_000), false, false);
        assert_eq!(decision, Decision::Build("v".into(), "delta"));
        assert_eq!(numbers.unwrap().masked_rows, 90_000);
    }

    #[test]
    fn a_foreground_run_builds_a_delta_beside_a_base_unless_a_merge_is_due() {
        let mut segments: Vec<_> = (0..4).map(|i| segment(&format!("s{i}"), 100_000)).collect();
        let base = tier(
            &[
                ("s0", 100_000),
                ("s1", 100_000),
                ("s2", 100_000),
                ("s3", 100_000),
            ],
            1,
        );
        assert_eq!(forced_kind(&segments, &[], None, "v"), Kind::Base);
        let tiers = vec![base];
        assert_eq!(forced_kind(&segments, &[], Some(&tiers), "v"), Kind::Base);
        segments.push(segment("n", 1_000));
        assert_eq!(forced_kind(&segments, &[], Some(&tiers), "v"), Kind::Delta);
        segments.push(segment("m", 300_000));
        assert_eq!(forced_kind(&segments, &[], Some(&tiers), "v"), Kind::Base);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queries_keep_using_the_tier_after_new_segments() {
        use std::sync::atomic::Ordering::Relaxed;
        let store = bf_storage::Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let engine = Engine::with_config(
            store,
            crate::EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                ..Default::default()
            },
        );
        let vector = |id: usize| -> Vec<f32> {
            (0..32)
                .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
                .collect()
        };
        let write = |from: usize, to: usize| {
            let docs: Vec<_> = (from..to)
                .map(|n| serde_json::json!({"id": n, "vector": vector(n)}))
                .collect();
            serde_json::from_value::<bf_types::WriteRequest>(serde_json::json!({
                "distance_metric": "euclidean_squared", "upsert_rows": docs
            }))
            .unwrap()
        };
        for batch in 0..2 {
            engine
                .write("t", write(batch * 2000, batch * 2000 + 2000))
                .await
                .unwrap();
            engine.index_namespace("t", true).await.unwrap();
        }
        let scratch = std::env::temp_dir().join(format!("bf-tier-{}", std::process::id()));
        engine
            .recluster(
                "t",
                &recluster::ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 2,
                    cluster_rows: 50,
                    max_slab_rows: 5000,
                    scratch: scratch.clone(),
                },
            )
            .await
            .unwrap();
        let _ = std::fs::remove_dir_all(&scratch);
        engine.write("t", write(4000, 4200)).await.unwrap();
        engine.index_namespace("t", true).await.unwrap();
        let query = |q: usize| {
            serde_json::from_value::<bf_types::QueryRequest>(
                serde_json::json!({"rank_by": ["vector", "ANN", vector(q)], "top_k": 5}),
            )
            .unwrap()
        };
        for q in [17usize, 3017, 4100] {
            let before = crate::ann::TIER_ANSWERS.load(Relaxed);
            let rows = engine
                .query("t", query(q))
                .await
                .unwrap()
                .result
                .rows
                .unwrap();
            assert_eq!(rows[0]["id"], serde_json::json!(q));
            assert!(crate::ann::TIER_ANSWERS.load(Relaxed) > before);
        }
    }
}
