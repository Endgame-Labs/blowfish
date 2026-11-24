//! Feature-gated G3 prototype: build a disposable graph from immutable tier objects.
//!
//! Default builds do not compile this module. Promotion is explicit in the experiment; the
//! automatic heat policy, eviction policy, and optional persistence are a separate shipping step.
use crate::{Engine, NamespaceState, lineage::Placement, segment};
use bf_storage::Storage;
use bf_types::{ApiError, DistanceMetric, Document, Id};
use bf_vector::{
    Index, RoaringBitmap, VectorType,
    hot_graph::{HotBuildParams, HotGraph, HotScorer, HotScratch},
};
use bf_wal::{SlabSet, Wal};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

/// Admission and query settings for the experimental accelerator.
#[derive(Clone, Copy, Debug)]
pub struct HotGraphOptions {
    /// Exact-built graph and query scoring representation.
    pub build: HotBuildParams,
    /// Query beam, calibrated by the offline NQ study.
    pub query_ef: usize,
    /// Exact rerank cap; requests larger than this keep using IVF.
    pub rerank: usize,
    /// Hard admission cap on the estimated build working set, including packed input.
    pub build_budget_bytes: u64,
    /// Drop construction vectors, and rerank through the existing sparse range cache.
    pub sparse_rerank: bool,
}
impl Default for HotGraphOptions {
    fn default() -> Self {
        Self {
            build: HotBuildParams::default(),
            query_ef: 160,
            rerank: 30,
            build_budget_bytes: 512 * 1024 * 1024,
            sparse_rerank: true,
        }
    }
}
/// Result of building and checking the source tier again before installation.
#[derive(Clone, Debug)]
pub struct HotGraphReport {
    /// Rows copied from immutable source objects.
    pub rows: usize,
    /// Admission estimate, including construction overhead and input buffering.
    pub estimated_build_bytes: u64,
    /// Retained allocation capacities; allocator metadata and worker scratch are additional.
    pub resident_bytes: u64,
    /// Source reads, validation, construction, and encoding wall time.
    pub seconds: f64,
    /// False when a concurrent tier replacement made the finished accelerator stale.
    pub installed: bool,
}
#[derive(Clone)]
struct Entry {
    graph: Arc<HotGraph>,
    options: HotGraphOptions,
    sources: Arc<Vec<Index<segment::ObjectReader>>>,
    locations: Arc<Vec<(u32, [u32; 3])>>,
}
// Deliberately one process-wide entry in this prototype. Immutable directory keys identify its
// source. A production cache must scope entries by backend, enforce a shared budget, and evict by heat.
static READY: LazyLock<Mutex<BTreeMap<String, Arc<Entry>>>> = LazyLock::new(Default::default);
static ANSWERS: AtomicU64 = AtomicU64::new(0);
thread_local! { static SCRATCH:RefCell<Option<(usize,HotScratch)>>=const { RefCell::new(None) }; }

/// Graph candidate plans completed, for benchmark verification.
pub fn answered_queries() -> u64 {
    ANSWERS.load(Ordering::Relaxed)
}
/// Evict the prototype cache. Active queries retain their immutable snapshot until they finish.
pub fn evict() {
    READY.lock().unwrap_or_else(|e| e.into_inner()).clear();
}
pub(crate) fn available(key: &str) -> bool {
    READY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(key)
}

impl Engine {
    /// Build the first recluster tier's RAM accelerator, without publishing any stored object.
    ///
    /// Reads bypass the block cache. Unsupported metrics, precision, insufficient budget, and
    /// a missing tier return `None`, leaving the ordinary query plan available throughout.
    /// This prototype admits one graph per process, rather than implementing an automatic LRU.
    pub async fn build_hot_graph(
        &self,
        ns: &str,
        attribute: &str,
        options: HotGraphOptions,
    ) -> Result<Option<HotGraphReport>, ApiError> {
        if options.query_ef == 0 || options.rerank == 0 {
            return Err(ApiError::bad("invalid experimental graph query budget"));
        }
        let start = Instant::now();
        let wal = Wal::new(self.0.store.clone(), ns).map_err(segment::index_error)?;
        let Some((_, manifest)) = wal.latest_manifest().await.map_err(segment::index_error)? else {
            return Ok(None);
        };
        if manifest.deleted || manifest.distance_metric != DistanceMetric::CosineDistance {
            return Ok(None);
        }
        let Some((dims, VectorType::F16)) =
            manifest.schema.get(attribute).and_then(segment::dense_type)
        else {
            return Ok(None);
        };
        let Some(set) = manifest
            .ann_tiers
            .get(attribute)
            .and_then(|sets| sets.first())
            .cloned()
        else {
            return Ok(None);
        };
        // Query-only NQ comparison on a host too small for the monolithic builder. The graph
        // was built from corpus/document IDs 0..N; translate those IDs into this fixture's tier
        // coordinates. This is a trusted benchmark import, not an accelerator persistence API.
        if let Some(root) = std::env::var_os("G3_NQ_GRAPH_IMPORT") {
            let count = set.rows() as usize;
            let estimated = count as u64 * (dims.next_power_of_two() + 768) as u64;
            if !options.sparse_rerank || estimated > options.build_budget_bytes {
                return Ok(None);
            }
            let at =
                crate::lineage::place(&self.0.store, &manifest.segments, &manifest.lineage, &set)
                    .await?
                    .ok_or_else(segment::corrupt)?;
            let mut indexes = Vec::new();
            for segment in &manifest.segments {
                indexes.push(segment::ids(&self.0.store, segment).await?);
            }
            let mut rows = vec![u32::MAX; count];
            for cover in &set.covered {
                for local in 0..cover.rows {
                    let global = cover.base + local;
                    let (part, row) = at.real(&set.covered, global).ok_or_else(segment::corrupt)?;
                    let Id::Uint(id) = indexes[part][row as usize] else {
                        return Err(ApiError::bad("NQ import requires integer corpus IDs"));
                    };
                    let target = rows.get_mut(id as usize).ok_or_else(segment::corrupt)?;
                    if *target != u32::MAX {
                        return Err(segment::corrupt());
                    }
                    *target = global;
                }
            }
            if rows.contains(&u32::MAX) {
                return Err(segment::corrupt());
            }
            let root = std::path::PathBuf::from(root);
            let graph =
                tokio::task::spawn_blocking(move || HotGraph::load_experiment(dims, rows, &root))
                    .await
                    .map_err(segment::index_error)?
                    .map_err(segment::index_error)?;
            let mut sources = Vec::new();
            let mut locations = Vec::with_capacity(count);
            for (slab, s) in set.slabs.iter().enumerate() {
                let (file, _) = crate::upgrade::files(s, attribute).ok_or_else(segment::corrupt)?;
                let source = Index::open_with_metadata_len(
                    segment::ObjectReader::new(&self.0.store, file),
                    file.metadata_len,
                )
                .await
                .map_err(segment::index_error)?;
                locations.extend(
                    source
                        .hot_graph_locations()
                        .into_iter()
                        .map(|(row, c, o)| (row, [slab as u32, c, o])),
                );
                sources.push(source);
            }
            locations.sort_unstable_by_key(|x| x.0);
            let resident_bytes = graph.resident_bytes()
                + sources.iter().map(Index::resident_bytes).sum::<u64>()
                + (locations.capacity() * std::mem::size_of::<(u32, [u32; 3])>()) as u64;
            let current = wal.latest_manifest().await.map_err(segment::index_error)?;
            let installed = current.as_ref().is_some_and(|(_, m)| {
                !m.deleted
                    && m.ann_tiers
                        .get(attribute)
                        .is_some_and(|sets| sets.iter().any(|s| s.dir.key == set.dir.key))
            });
            if installed {
                let mut ready = READY.lock().unwrap_or_else(|e| e.into_inner());
                ready.clear();
                ready.insert(
                    set.dir.key,
                    Arc::new(Entry {
                        graph: Arc::new(graph),
                        options,
                        sources: Arc::new(sources),
                        locations: Arc::new(locations),
                    }),
                );
            }
            return Ok(Some(HotGraphReport {
                rows: count,
                estimated_build_bytes: estimated,
                resident_bytes,
                seconds: start.elapsed().as_secs_f64(),
                installed,
            }));
        }
        let count = set
            .slabs
            .iter()
            .map(|s| s.row_count as usize)
            .sum::<usize>();
        let max_object = set
            .slabs
            .iter()
            .filter_map(|s| crate::upgrade::files(s, attribute))
            .map(|f| f.0.size)
            .max()
            .unwrap_or(0);
        let code_bytes = if options.build.scorer == HotScorer::EightBit {
            dims.next_power_of_two()
        } else {
            0
        };
        let retained = READY
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|e| e.graph.resident_bytes())
            .sum::<u64>();
        let estimated = (count as u64)
            .saturating_mul((2 * dims + 4096 + code_bytes) as u64)
            .saturating_add(max_object.saturating_mul(2))
            .saturating_add(retained.saturating_mul(5) / 4);
        if count == 0 || estimated > options.build_budget_bytes {
            return Ok(None);
        }
        let mut rows = Vec::with_capacity(count);
        let mut packed = Vec::with_capacity(count * dims * 2);
        let mut locations = Vec::new();
        let mut sources = Vec::new();
        for (slab_id, slab) in set.slabs.iter().enumerate() {
            let Some((file, _)) = crate::upgrade::files(slab, attribute) else {
                return Ok(None);
            };
            // Plain get bypasses the range cache, so a build does not evict foreground code pages.
            let bytes = self
                .0
                .store
                .get(&file.key)
                .await
                .map_err(segment::index_error)?;
            let index = Index::open(bytes).await.map_err(segment::index_error)?;
            if index.vector_type() != VectorType::F16
                || index.dims() != dims
                || index.metric() != bf_vector::Metric::CosineDistance
            {
                return Ok(None);
            }
            for c in 0..index.cluster_count() {
                let (ids, vectors) = index
                    .read_cluster_rows(c)
                    .await
                    .map_err(segment::index_error)?;
                if vectors.len() != ids.len() * dims * 2 {
                    return Err(segment::corrupt());
                }
                if options.sparse_rerank {
                    locations.extend(
                        ids.iter().enumerate().map(|(ordinal, row)| {
                            (*row, [slab_id as u32, c as u32, ordinal as u32])
                        }),
                    );
                }
                rows.extend(ids);
                packed.extend_from_slice(&vectors);
            }
            if options.sparse_rerank {
                sources.push(
                    Index::open_with_metadata_len(
                        segment::ObjectReader::new(&self.0.store, file),
                        file.metadata_len,
                    )
                    .await
                    .map_err(segment::index_error)?,
                );
            }
        }
        if rows.len() != count {
            return Err(segment::corrupt());
        }
        let mut graph =
            tokio::task::spawn_blocking(move || HotGraph::build(dims, rows, packed, options.build))
                .await
                .map_err(segment::index_error)?
                .map_err(segment::index_error)?;
        if options.sparse_rerank && !graph.release_exact_vectors() {
            return Err(ApiError::bad(
                "sparse prototype requires eight-bit navigation",
            ));
        }
        locations.sort_unstable_by_key(|x| x.0);
        let resident_bytes = graph.resident_bytes()
            + sources.iter().map(Index::resident_bytes).sum::<u64>()
            + (locations.capacity() * std::mem::size_of::<(u32, [u32; 3])>()) as u64;
        let current = wal.latest_manifest().await.map_err(segment::index_error)?;
        let installed = current.as_ref().is_some_and(|(_, m)| {
            !m.deleted
                && m.ann_tiers
                    .get(attribute)
                    .is_some_and(|sets| sets.iter().any(|s| s.dir.key == set.dir.key))
        });
        if installed {
            let mut ready = READY.lock().unwrap_or_else(|e| e.into_inner());
            ready.clear();
            ready.insert(
                set.dir.key,
                Arc::new(Entry {
                    graph: Arc::new(graph),
                    options,
                    sources: Arc::new(sources),
                    locations: Arc::new(locations),
                }),
            );
        }
        Ok(Some(HotGraphReport {
            rows: count,
            estimated_build_bytes: estimated,
            resident_bytes,
            seconds: start.elapsed().as_secs_f64(),
            installed,
        }))
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn search(
    store: &Storage,
    state: &NamespaceState,
    set: &SlabSet,
    at: &Placement,
    vector: &[f32],
    mask: Option<Arc<RoaringBitmap>>,
    k: usize,
    columns: &BTreeSet<String>,
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    let entry = READY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&set.dir.key)
        .cloned();
    let Some(entry) = entry else { return Ok(None) };
    if k == 0
        || k > entry.options.rerank
        || vector.len() != entry.graph.dims()
        || vector.iter().all(|x| *x == 0.)
    {
        return Ok(None);
    }
    let vector = vector.to_vec();
    let worker_vector = vector.clone();
    let mask_for_worker = mask.clone();
    let worker_entry = entry.clone();
    let hits = tokio::task::spawn_blocking(move || {
        let entry = worker_entry;
        SCRATCH.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.as_ref().is_none_or(|(n, _)| *n != entry.graph.len()) {
                *slot = Some((entry.graph.len(), entry.graph.scratch()));
            }
            if entry.options.sparse_rerank {
                entry
                    .graph
                    .candidates(
                        &worker_vector,
                        entry.options.query_ef,
                        entry.options.rerank,
                        mask_for_worker.as_deref(),
                        &mut slot.as_mut().unwrap().1,
                    )
                    .map(|rows| rows.into_iter().map(|r| (r, 0.)).collect())
            } else {
                entry.graph.search(
                    &worker_vector,
                    k,
                    entry.options.query_ef,
                    entry.options.rerank,
                    mask_for_worker.as_deref(),
                    &mut slot.as_mut().unwrap().1,
                )
            }
        })
    })
    .await
    .map_err(segment::index_error)?
    .map_err(segment::index_error)?;
    let hits = if entry.options.sparse_rerank {
        let mut by_source: BTreeMap<usize, Vec<(u32, u32)>> = BTreeMap::new();
        for (row, _) in hits {
            let p = entry
                .locations
                .binary_search_by_key(&row, |x| x.0)
                .map_err(|_| segment::corrupt())?;
            let [slab, cluster, ordinal] = entry.locations[p].1;
            by_source
                .entry(slab as usize)
                .or_default()
                .push((cluster, ordinal));
        }
        let mut exact =
            futures::future::try_join_all(by_source.into_iter().map(|(slab, locations)| {
                let source = &entry.sources[slab];
                let vector = &vector;
                async move {
                    source
                        .rerank_candidates(vector, k, locations)
                        .await
                        .map_err(segment::index_error)
                }
            }))
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        exact.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        exact.truncate(k);
        exact
    } else {
        hits
    };
    if hits.len() < k {
        return Ok(None);
    }
    let mut by_segment: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
    let mut distances = BTreeMap::new();
    for (row, distance) in hits {
        if mask.as_ref().is_some_and(|m| !m.contains(row)) {
            continue;
        }
        let Some((i, local)) = at.real(&set.covered, row) else {
            continue;
        };
        by_segment.entry(i).or_default().insert(local);
        distances.insert((i, local), distance);
    }
    let mut scores = BTreeMap::new();
    let mut documents = BTreeMap::new();
    for (i, rows) in by_segment {
        for (row, doc) in
            segment::projected_rows(store, &state.segments[i], &rows, Some(columns)).await?
        {
            let id: Id =
                serde_json::from_value(doc.get("id").ok_or_else(segment::corrupt)?.clone())
                    .map_err(segment::index_error)?;
            let distance = *distances.get(&(i, row)).ok_or_else(segment::corrupt)?;
            scores.insert(id.clone(), distance as f64);
            documents.insert(id, doc);
        }
    }
    ANSWERS.fetch_add(1, Ordering::Relaxed);
    Ok(Some((scores, documents)))
}

/// Change only the prototype's query budget, retaining its immutable graph and source vectors.
/// Active queries finish with the previous budget. This is a benchmark control, not a server setting.
pub fn query_budget(ef: usize, rerank: usize) -> bool {
    if ef == 0 || rerank == 0 {
        return false;
    }
    let mut ready = READY.lock().unwrap_or_else(|e| e.into_inner());
    for entry in ready.values_mut() {
        let entry = Arc::make_mut(entry);
        entry.options.query_ef = ef;
        entry.options.rerank = rerank;
    }
    !ready.is_empty()
}
