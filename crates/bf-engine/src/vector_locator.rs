//! Stored-precision vectors addressed through immutable tier coverage and reverse lineage.
use crate::{NamespaceState, lineage, read_cache, segment};
use bf_storage::Storage;
use bf_types::ApiError;
use bf_vector::RoaringBitmap;
use bf_wal::{Lineage, Segment, SlabSet};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) mod inputs;

/// Snapshot-owned sources: readers never resolve a row against a newer publication.
#[derive(Clone)]
pub(crate) struct Sources {
    tiers: Arc<BTreeMap<String, Vec<SlabSet>>>,
    lineage: Arc<Vec<Lineage>>,
}
impl Sources {
    /// Whether a tier owns every document row of this segment, including sparse vectors.
    pub fn covers(&self, seg: &Segment, attr: &str) -> bool {
        let Some(sets) = self.tiers.get(attr) else {
            return false;
        };
        let mut rows = 0u64;
        for cover in sets.iter().flat_map(|s| &s.covered) {
            if cover.segment == seg.id {
                rows += u64::from(cover.rows);
            } else if let Some(entry) = self.lineage.iter().find(|l| l.source == cover.segment) {
                rows += entry
                    .parts
                    .iter()
                    .filter(|p| p.target == seg.id)
                    .map(|p| u64::from(p.rows))
                    .sum::<u64>();
            }
        }
        rows == u64::from(seg.row_count) && rows > 0
    }

    pub fn new(state: &NamespaceState) -> Self {
        Self {
            tiers: state.ann_tiers.clone(),
            lineage: state.lineage.clone(),
        }
    }

    pub fn from_parts(tiers: BTreeMap<String, Vec<SlabSet>>, lineage: Vec<Lineage>) -> Self {
        Self {
            tiers: Arc::new(tiers),
            lineage: Arc::new(lineage),
        }
    }
    pub fn from_manifest(manifest: &bf_wal::Manifest) -> Self {
        Self {
            tiers: Arc::new(manifest.ann_tiers.clone()),
            lineage: Arc::new(manifest.lineage.clone()),
        }
    }

    /// Resolve requested segment rows through coverage, reverse lineage, and the resident
    /// ascending slab row lists. Slabs address vectors by global row; their list ordinal is
    /// the slab-local row. Missing vectors remain absent, including sparse columns.
    pub async fn read(
        &self,
        store: &Storage,
        seg: &Segment,
        rows: &RoaringBitmap,
        attr: &str,
        bypass: bool,
    ) -> Result<(Vec<u32>, Vec<u8>), ApiError> {
        let sets = self
            .tiers
            .get(attr)
            .ok_or_else(|| segment::index_error("vector source has no covering tier"))?;
        let (dims, dtype) = seg
            .schema
            .get(attr)
            .and_then(segment::dense_type)
            .ok_or_else(segment::corrupt)?;
        let width = dims * dtype.width();
        let mut found: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        let mut addressed = RoaringBitmap::new();
        for set in sets {
            let mut global_to_row = BTreeMap::new();
            for cover in &set.covered {
                if cover.segment == seg.id {
                    for row in rows {
                        if row >= cover.rows {
                            return Err(segment::corrupt());
                        }
                        global_to_row.insert(
                            cover.base.checked_add(row).ok_or_else(segment::corrupt)?,
                            row,
                        );
                        addressed.insert(row);
                    }
                } else if let Some(entry) = self.lineage.iter().find(|l| l.source == cover.segment)
                {
                    for part in entry.parts.iter().filter(|part| part.target == seg.id) {
                        let map = lineage::load(store, part).await?;
                        for row in rows & &map.to {
                            let source = map.back(row).ok_or_else(segment::corrupt)?;
                            if source >= cover.rows {
                                return Err(segment::corrupt());
                            }
                            global_to_row.insert(
                                cover
                                    .base
                                    .checked_add(source)
                                    .ok_or_else(segment::corrupt)?,
                                row,
                            );
                            addressed.insert(row);
                        }
                    }
                }
            }
            if global_to_row.is_empty() {
                continue;
            }
            for slab in &set.slabs {
                let member = crate::slab_rows::load(store, slab, attr).await?;
                let member = member.ok_or_else(segment::corrupt)?;
                let resident = member.row_ids().ok_or_else(segment::corrupt)?;
                let wanted: RoaringBitmap = global_to_row
                    .keys()
                    .filter(|r| resident.binary_search(r).is_ok())
                    .copied()
                    .collect();
                if wanted.is_empty() {
                    continue;
                }
                // Slab schemas are empty in the manifest; the covered segment supplies shape.
                let mut source = slab.clone();
                source.schema = seg.schema.clone();
                let (present, packed) =
                    segment::read_own_ann_packed(store, &source, &wanted, attr, bypass).await?;
                if packed.len() != present.len() * width {
                    return Err(segment::corrupt());
                }
                for (global, bytes) in present.into_iter().zip(packed.chunks_exact(width)) {
                    let row = *global_to_row.get(&global).ok_or_else(segment::corrupt)?;
                    if found.insert(row, bytes.to_vec()).is_some() {
                        return Err(segment::index_error("vector row has multiple tier sources"));
                    }
                }
            }
        }
        if rows - &addressed != RoaringBitmap::new() {
            return Err(segment::index_error("vector rows are not fully covered"));
        }
        let mut present = Vec::with_capacity(found.len());
        let mut packed = Vec::with_capacity(found.len() * width);
        for (row, bytes) in found {
            present.push(row);
            packed.extend(bytes);
        }
        Ok((present, packed))
    }
}

#[cfg(test)]
#[path = "vector_locator_tests.rs"]
mod tests;

/// Bind immutable vector sources to every read worker through its existing read context.
/// Maintenance and reference callers without a cache still get the same source snapshot.
pub(crate) fn scope<'a, T: Send + 'a>(
    state: &NamespaceState,
    future: impl std::future::Future<Output = T> + Send + 'a,
) -> futures::future::BoxFuture<'a, T> {
    scope_sources(Sources::new(state), future)
}
pub(crate) fn scope_manifest<'a, T: Send + 'a>(
    manifest: &bf_wal::Manifest,
    future: impl std::future::Future<Output = T> + Send + 'a,
) -> futures::future::BoxFuture<'a, T> {
    scope_sources(Sources::from_manifest(manifest), future)
}
fn scope_sources<'a, T: Send + 'a>(
    sources: Sources,
    future: impl std::future::Future<Output = T> + Send + 'a,
) -> futures::future::BoxFuture<'a, T> {
    if sources.tiers.is_empty() {
        return Box::pin(future);
    }
    let sources = Arc::new(sources);
    // The caller may construct this future before entering its read scope. Resolve the
    // cache, graph admission, and accounting on first poll, inside that caller's scope.
    Box::pin(async move {
        let mut context = read_cache::context().unwrap_or_else(|| {
            read_cache::ReadContext::new(Arc::new(read_cache::DecodedCache::new(0, 1)))
        });
        context.vectors = Some(sources);
        read_cache::scope(context, future).await
    })
}

/// Decode stored precision without changing a coordinate before the exact distance kernel.
pub(crate) fn decode(dtype: bf_vector::VectorType, bytes: &[u8]) -> Vec<f32> {
    match dtype {
        bf_vector::VectorType::I8 => bytes.iter().map(|x| *x as i8 as f32).collect(),
        bf_vector::VectorType::F16 => bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|x| half::f16::from_le_bytes(*x).to_f32())
            .collect(),
        bf_vector::VectorType::F32 => bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|x| f32::from_le_bytes(*x))
            .collect(),
    }
}

/// Bounded exact search through the locator, using the same f32 metric as ANN reranking.
pub(crate) async fn exact(
    store: &Storage,
    seg: &Segment,
    allowed: &RoaringBitmap,
    attr: &str,
    query: &[f32],
    k: usize,
    metric: bf_types::DistanceMetric,
) -> Result<Vec<(u32, f32)>, ApiError> {
    let (dims, dtype) = seg
        .schema
        .get(attr)
        .and_then(segment::dense_type)
        .ok_or_else(segment::corrupt)?;
    let width = dims * dtype.width();
    let metric = segment::vector_metric(metric);
    let mut rows = allowed.iter();
    let mut hits = Vec::new();
    loop {
        let batch: RoaringBitmap = rows.by_ref().take(4096).collect();
        if batch.is_empty() {
            break;
        }
        let (present, packed) = segment::read_packed(store, seg, &batch, attr).await?;
        for (row, bytes) in present.into_iter().zip(packed.chunks_exact(width)) {
            let distance = metric
                .distance(query, &decode(dtype, bytes))
                .map_err(segment::index_error)?;
            hits.push((row, distance));
        }
        hits.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        hits.truncate(k);
    }
    Ok(hits)
}

/// Recreate independent ANN ownership before the publication drops the last covering tier.
/// Uploads precede the manifest CAS; failures leave the old tier authoritative.
pub(crate) async fn restore_uncovered(
    store: &Storage,
    ns: &str,
    manifest: &mut bf_wal::Manifest,
    old: &Sources,
) -> Result<(), ApiError> {
    let remaining = Sources::from_manifest(manifest);
    for seg in &mut manifest.segments {
        for (attr, cfg) in &seg.schema {
            let Some((dims, dtype)) = segment::dense_type(cfg) else {
                continue;
            };
            if crate::upgrade::files(seg, attr).is_some() || remaining.covers(seg, attr) {
                continue;
            }
            if !old.covers(seg, attr) {
                // Legacy document-only segments have never delegated vector ownership. Their
                // v1 document column stays authoritative until the automatic rewrite builds ANN.
                if segment::upgrade::needs_upgrade(store, seg).await? {
                    continue;
                }
                return Err(segment::index_error(
                    "segment lost its authoritative vector source",
                ));
            }
            let live = segment::live(store, seg).await?;
            let mut rows = Vec::new();
            let mut packed = Vec::new();
            let mut scan = live.iter();
            loop {
                let chunk: RoaringBitmap = scan.by_ref().take(4096).collect();
                if chunk.is_empty() {
                    break;
                }
                let (present, data) = old.read(store, seg, &chunk, attr, true).await?;
                rows.extend(present);
                packed.extend(data);
            }
            let metric = segment::vector_metric(manifest.distance_metric);
            let bytes = tokio::task::spawn_blocking(move || {
                segment::ann_builder(dims, metric, dtype, rows.len())?.build_packed(rows, packed)
            })
            .await
            .map_err(segment::index_error)?
            .map_err(segment::index_error)?;
            let (bytes, metadata_len) =
                bf_vector::Fragment::build_ann(&bytes).map_err(segment::index_error)?;
            let file = segment::put_bytes(
                store,
                format!(
                    "ns/{ns}/segments/{}/ann.restored.{}.bfa",
                    seg.id,
                    uuid::Uuid::new_v4()
                ),
                bytes.into(),
                metadata_len,
            )
            .await?;
            seg.files.insert(format!("annv2:{attr}"), file);
        }
    }
    Ok(())
}

/// Both legacy fragments and current containers are redundant once a current tier owns them.
fn ann_attribute(role: &str) -> Option<&str> {
    ["annv2:", "ann:", "annfrag:", "annfrag2:"]
        .into_iter()
        .find_map(|prefix| role.strip_prefix(prefix))
}

/// Eligible segment copies only; an in-flight builder keeps its source objects until install.
pub(crate) fn needs_retirement(
    state: &NamespaceState,
    protected: &std::collections::BTreeSet<String>,
) -> bool {
    let sources = Sources::new(state);
    state.segments.iter().any(|seg| {
        !protected.contains(&seg.id)
            && seg
                .files
                .keys()
                .filter_map(|k| ann_attribute(k))
                .any(|attr| {
                    sources.covers(seg, attr)
                        && state
                            .ann_tiers
                            .get(attr)
                            .is_some_and(|sets| crate::slab_rows::ready(sets, attr))
                })
    })
}
/// Remove redundant vector descriptors and enqueue their immutable objects in this same publication.
pub(crate) fn retire_covered(
    manifest: &mut bf_wal::Manifest,
    protected: &std::collections::BTreeSet<String>,
) {
    let sources = Sources::from_manifest(manifest);
    let mut retired = Vec::new();
    for seg in &mut manifest.segments {
        if protected.contains(&seg.id) {
            continue;
        }
        let roles: Vec<_> = seg
            .files
            .keys()
            .filter(|role| {
                ann_attribute(role).is_some_and(|attr| {
                    sources.covers(seg, attr)
                        && manifest
                            .ann_tiers
                            .get(attr)
                            .is_some_and(|sets| crate::slab_rows::ready(sets, attr))
                })
            })
            .cloned()
            .collect();
        for role in roles {
            retired.push(seg.files.remove(&role).expect("selected above").key);
        }
    }
    for key in retired {
        crate::indexer::retire(manifest, key);
    }
}
