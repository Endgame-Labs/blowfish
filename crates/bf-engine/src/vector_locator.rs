//! Stored-precision vectors addressed through immutable recluster tiers and compaction lineage.
//! Sources belong to a snapshot: readers never discover a newer manifest partway through a read.
use crate::{NamespaceState, lineage, read_cache, segment};
use bf_storage::Storage;
use bf_types::ApiError;
use bf_vector::RoaringBitmap;
use bf_wal::{Lineage, Manifest, Segment, SegmentFile, SlabSet};
use futures::{StreamExt, TryStreamExt};
use std::{collections::BTreeMap, sync::Arc};

/// The vector sources pinned by one namespace snapshot or index publication.
#[derive(Clone, Copy)]
pub(crate) struct Source<'a> {
    pub segments: &'a [Segment],
    pub tiers: &'a BTreeMap<String, Vec<SlabSet>>,
    pub lineage: &'a [Lineage],
}
impl<'a> From<&'a NamespaceState> for Source<'a> {
    fn from(state: &'a NamespaceState) -> Self {
        Self {
            segments: &state.segments,
            tiers: &state.ann_tiers,
            lineage: &state.lineage,
        }
    }
}
impl<'a> From<&'a Manifest> for Source<'a> {
    fn from(manifest: &'a Manifest) -> Self {
        Self {
            segments: &manifest.segments,
            tiers: &manifest.ann_tiers,
            lineage: &manifest.lineage,
        }
    }
}
impl Source<'_> {
    /// Whether a current tier holds this segment's vectors, directly or through lineage.
    pub fn covers(self, segment: &Segment, attr: &str) -> bool {
        self.tiers.get(attr).is_some_and(|tiers| {
            tiers.iter().any(|set| {
                !set.slabs.is_empty()
                    && set
                        .slabs
                        .iter()
                        .all(|s| s.files.contains_key(&format!("annv2:{attr}")))
                    && lineage::reachable(self.segments, self.lineage, set)
                    && set.covered.iter().any(|c| {
                        c.segment == segment.id
                            || self.lineage.iter().any(|l| {
                                l.source == c.segment
                                    && l.parts.iter().any(|p| p.target == segment.id)
                            })
                    })
            })
        })
    }

    /// Read ascending segment rows from their covering tier. Slabs retain global row IDs;
    /// binary search in each resident row list finds membership without reading vector bytes.
    /// Missing vectors (nullable columns, or rows deleted before the tier build) are omitted.
    pub async fn read(
        self,
        store: &Storage,
        segment: &Segment,
        wanted: &RoaringBitmap,
        attr: &str,
        bypass: bool,
    ) -> Result<(Vec<u32>, Vec<u8>), ApiError> {
        let (dims, dtype) = segment
            .schema
            .get(attr)
            .and_then(segment::dense_type)
            .ok_or_else(segment::corrupt)?;
        let width = dims * dtype.width();
        let position = self
            .segments
            .iter()
            .position(|s| s.id == segment.id)
            .ok_or_else(segment::corrupt)?;
        let mut found: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        for set in self.tiers.get(attr).into_iter().flatten() {
            if set.slabs.is_empty()
                || set
                    .slabs
                    .iter()
                    .any(|s| !s.files.contains_key(&format!("annv2:{attr}")))
            {
                continue;
            }
            let Some(at) = lineage::place(store, self.segments, self.lineage, set).await? else {
                continue;
            };
            let mut globals = BTreeMap::new();
            for (c, place) in set.covered.iter().zip(&at.places) {
                match place {
                    lineage::Place::Here(i) if *i == position => {
                        for row in wanted {
                            if row < c.rows {
                                globals.insert(c.base + row, row);
                            }
                        }
                    }
                    lineage::Place::Moved(parts) => {
                        for (i, map) in parts.iter().filter(|(i, _)| *i == position) {
                            let _ = i;
                            for row in wanted {
                                if let Some(old) = map.back(row) {
                                    if old >= c.rows {
                                        return Err(segment::corrupt());
                                    }
                                    globals.insert(
                                        c.base.checked_add(old).ok_or_else(segment::corrupt)?,
                                        row,
                                    );
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            if globals.is_empty() {
                continue;
            }
            for slab in &set.slabs {
                let file = slab
                    .files
                    .get(&format!("annv2:{attr}"))
                    .ok_or_else(segment::corrupt)?;
                let rows = rows(store, slab, set, attr).await?;
                let selected: RoaringBitmap = globals
                    .keys()
                    .filter(|g| rows.binary_search(g).is_ok())
                    .copied()
                    .collect();
                if selected.is_empty() {
                    continue;
                }
                let dir = segment::projection_directory(store, slab, file, bypass).await?;
                if dir.dims() != dims || dir.vector_row_bytes() != width {
                    return Err(segment::corrupt());
                }
                let (present, bytes) =
                    segment::read_ann_file(store, slab, file, &selected, attr, bypass).await?;
                if present.len() != selected.len() as usize || bytes.len() != present.len() * width
                {
                    return Err(segment::corrupt());
                }
                for (global, raw) in present.into_iter().zip(bytes.chunks_exact(width)) {
                    let local = *globals.get(&global).ok_or_else(segment::corrupt)?;
                    if found.insert(local, raw.to_vec()).is_some() {
                        return Err(segment::corrupt());
                    }
                }
            }
        }
        let (rows, bytes): (Vec<_>, Vec<_>) = found.into_iter().unzip();
        Ok((rows, bytes.into_iter().flatten().collect()))
    }
}

/// Ascending global rows, with a CRC covering the raw u32 list. This is metadata, never vectors.
pub(crate) async fn write_rows(
    store: &Storage,
    key: String,
    rows: &[u32],
) -> Result<SegmentFile, ApiError> {
    if rows.windows(2).any(|r| r[0] >= r[1]) {
        return Err(segment::corrupt());
    }
    let mut bytes: Vec<u8> = rows.iter().flat_map(|r| r.to_le_bytes()).collect();
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    segment::put_file(store, key, bytes, 0).await
}

/// Resident slab membership costs four bytes per row, independent of vector dimensions.
pub(crate) async fn rows(
    store: &Storage,
    slab: &Segment,
    set: &SlabSet,
    attr: &str,
) -> Result<Arc<Vec<u32>>, ApiError> {
    let (ann, _) = crate::upgrade::files(slab, attr).ok_or_else(segment::corrupt)?;
    let key = format!("vector-rows:{}", ann.key);
    if let Some(rows) = read_cache::get(&key, u64::from(slab.row_count) * 4) {
        return Ok(rows);
    }
    let rows = if let Some(file) = slab.files.get("vector_rows") {
        let data = segment::ObjectReader::new(store, file)
            .read(0..file.size)
            .await
            .map_err(segment::index_error)?;
        let end = data.len().checked_sub(4).ok_or_else(segment::corrupt)?;
        if end % 4 != 0
            || crc32fast::hash(&data[..end])
                != u32::from_le_bytes(data[end..].try_into().map_err(segment::index_error)?)
        {
            return Err(segment::corrupt());
        }
        data[..end]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|r| u32::from_le_bytes(*r))
            .collect::<Vec<_>>()
    } else {
        // One-time upgrade for tiers published before D4. Their index API exposes row sets
        // together with their payloads; new builds persist the small row list directly.
        let reader = segment::ObjectReader::new(store, ann).bypass_cache();
        let index = bf_vector::Index::open_with_metadata_len(reader, ann.metadata_len)
            .await
            .map_err(segment::index_error)?;
        let mut rows = Vec::with_capacity(slab.row_count as usize);
        let leaves =
            futures::stream::iter((0..index.cluster_count()).map(|c| index.read_cluster_rows(c)))
                .buffer_unordered(16);
        futures::pin_mut!(leaves);
        while let Some((part, _)) = leaves.try_next().await.map_err(segment::index_error)? {
            rows.extend(part);
        }
        rows.sort_unstable();
        rows
    };
    if rows.len() != slab.row_count as usize
        || rows.windows(2).any(|r| r[0] >= r[1])
        || rows.iter().any(|r| {
            !set.covered
                .iter()
                .any(|c| *r >= c.base && *r - c.base < c.rows)
        })
    {
        return Err(segment::corrupt());
    }
    Ok(read_cache::insert(&key, rows.len() as u64 * 4, rows))
}

/// Add row lists to older tiers before removing any segment's vector source.
pub(crate) async fn prepare(store: &Storage, manifest: &mut Manifest) -> Result<(), ApiError> {
    for (attr, tiers) in &mut manifest.ann_tiers {
        for set in tiers {
            if set.slabs.is_empty()
                || set
                    .slabs
                    .iter()
                    .any(|s| !s.files.contains_key(&format!("annv2:{attr}")))
            {
                continue;
            }
            for i in 0..set.slabs.len() {
                if set.slabs[i].files.contains_key("vector_rows") {
                    continue;
                }
                let rows = rows(store, &set.slabs[i], set, attr).await?;
                let ann = &set.slabs[i].files[&format!("annv2:{attr}")];
                let file = write_rows(
                    store,
                    format!("{}.{}.rows", ann.key, uuid::Uuid::new_v4().simple()),
                    &rows,
                )
                .await?;
                set.slabs[i].files.insert("vector_rows".into(), file);
            }
        }
    }
    Ok(())
}

/// Remove redundant current ANN references only after all covering metadata is durable. The
/// caller publishes this manifest under its epoch fence; GC observes the normal retention window.
pub(crate) async fn retire_covered(
    store: &Storage,
    manifest: &mut Manifest,
) -> Result<usize, ApiError> {
    prepare(store, manifest).await?;
    let mut retire = Vec::new();
    for (attr, tiers) in &manifest.ann_tiers {
        let mut covered: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
        for set in tiers {
            if set.slabs.is_empty()
                || set.slabs.iter().any(|s| {
                    !s.files.contains_key(&format!("annv2:{attr}"))
                        || !s.files.contains_key("vector_rows")
                })
            {
                continue;
            }
            let Some(at) =
                lineage::place(store, &manifest.segments, &manifest.lineage, set).await?
            else {
                continue;
            };
            for (c, place) in set.covered.iter().zip(&at.places) {
                match place {
                    lineage::Place::Here(i) => {
                        covered.entry(*i).or_default().insert_range(0..c.rows);
                    }
                    lineage::Place::Moved(parts) => {
                        for (i, map) in parts {
                            *covered.entry(*i).or_default() |= &map.to;
                        }
                    }
                }
            }
        }
        for (i, rows) in covered {
            let seg = &manifest.segments[i];
            let role = format!("annv2:{attr}");
            if seg.files.contains_key(&role) && (segment::live(store, seg).await? - rows).is_empty()
            {
                retire.push((i, role));
            }
        }
    }
    let count = retire.len();
    for (i, role) in retire {
        let file = manifest.segments[i]
            .files
            .remove(&role)
            .expect("listed reference");
        crate::indexer::retire(manifest, file.key);
    }
    Ok(count)
}

/// The inexpensive trigger for an index pass whose only work is vector retirement.
pub(crate) fn needs_retirement(state: &NamespaceState) -> bool {
    let source = Source::from(state);
    state.segments.iter().any(|s| {
        s.files.keys().any(|k| {
            k.strip_prefix("annv2:")
                .is_some_and(|a| source.covers(s, a))
        })
    })
}

/// Recreate a segment ANN object before a publication drops its last covering tier.
pub(crate) async fn restore(
    store: &Storage,
    ns: &str,
    source: Source<'_>,
    segment: &Segment,
    attr: &str,
    metric: bf_types::DistanceMetric,
) -> Result<SegmentFile, ApiError> {
    let (dims, dtype) = segment
        .schema
        .get(attr)
        .and_then(segment::dense_type)
        .ok_or_else(segment::corrupt)?;
    let (rows, bytes) = source
        .read(
            store,
            segment,
            &(0..segment.row_count).collect(),
            attr,
            true,
        )
        .await?;
    let bytes = tokio::task::spawn_blocking(move || {
        let blob = bf_vector::IndexBuilder::new(dims, segment::vector_metric(metric))
            .map_err(segment::index_error)?
            .vector_type(dtype)
            .build_packed(rows, bytes)
            .map_err(segment::index_error)?;
        bf_vector::Fragment::build_ann(&blob).map_err(segment::index_error)
    })
    .await
    .map_err(segment::index_error)??;
    segment::put_file(
        store,
        format!(
            "ns/{ns}/segments/{}/ann.{}.bfa",
            segment.id,
            uuid::Uuid::new_v4().simple()
        ),
        bytes.0,
        bytes.1,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Engine, EngineConfig, ReclusterOptions};
    use serde_json::json;

    #[tokio::test]
    async fn nullable_columns_and_stored_precisions_keep_exact_bytes() {
        let store = Storage::from_backend(
            Arc::new(object_store::memory::InMemory::new()),
            Default::default(),
            false,
            false,
        )
        .await
        .unwrap();
        for dtype in ["f16", "f32", "i8"] {
            let schema = serde_json::from_value(
                json!({"id":{"type":"uint"},"v":{"type":format!("[3]{dtype}")}}),
            )
            .unwrap();
            let docs = (0..20)
                .map(|id| {
                    let mut doc = bf_types::Document::from_iter([("id".into(), json!(id))]);
                    if id % 3 != 0 {
                        doc.insert("v".into(), json!([id, -2, 3]));
                    }
                    (bf_types::Id::Uint(id), doc)
                })
                .collect::<BTreeMap<_, _>>();
            let segment = segment::build_with(
                &store,
                "nullable",
                &schema,
                bf_types::DistanceMetric::EuclideanSquared,
                &docs,
                Default::default(),
                0,
                1,
            )
            .await
            .unwrap();
            let wanted = (0..segment.row_count).collect();
            let own = segment::read_packed(&store, &segment, &wanted, "v")
                .await
                .unwrap();
            let mut slab = segment.clone();
            slab.row_count = own.0.len() as u32;
            let file = write_rows(&store, format!("rows-{dtype}"), &own.0)
                .await
                .unwrap();
            slab.files.insert("vector_rows".into(), file.clone());
            let set = SlabSet {
                covered: vec![bf_wal::SlabCover {
                    segment: segment.id.clone(),
                    base: 0,
                    rows: segment.row_count,
                    live_generation: 0,
                }],
                slabs: vec![slab],
                dir: file,
                pages: None,
                format: bf_wal::TIER_FORMAT_VERSION,
                coarse: None,
                coarse_rows: 0,
            };
            let segments = [segment.clone()];
            let tiers = BTreeMap::from([("v".into(), vec![set])]);
            let source = Source {
                segments: &segments,
                tiers: &tiers,
                lineage: &[],
            };
            assert_eq!(
                source
                    .read(&store, &segment, &wanted, "v", true)
                    .await
                    .unwrap(),
                own,
                "{dtype}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn locator_matches_own_ann_before_retirement_and_after_lineage() {
        let store = Storage::from_backend(
            Arc::new(object_store::memory::InMemory::new()),
            Default::default(),
            false,
            false,
        )
        .await
        .unwrap();
        let engine = Engine::with_config(
            store.clone(),
            EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                ..Default::default()
            },
        );
        for batch in 0..2 {
            let docs: Vec<_> = (0..180)
                .map(|n| {
                    let id = batch * 180 + n;
                    json!({"id":id,"v":[id as f32, 0.25, -0.5]})
                })
                .collect();
            engine.write("loc", serde_json::from_value(json!({"distance_metric":"euclidean_squared", "schema":{"v":{"type":"[3]f16"}},"upsert_rows":docs})).unwrap()).await.unwrap();
            engine.index_namespace("loc", true).await.unwrap();
        }
        std::fs::create_dir_all(".scratch/d4-locator").unwrap();
        let scratch = tempfile::tempdir_in(".scratch/d4-locator").unwrap();
        let originals_before = bf_wal::Wal::new(store.clone(), "loc")
            .unwrap()
            .latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1
            .segments;
        engine
            .recluster(
                "loc",
                &ReclusterOptions {
                    attribute: "v".into(),
                    cells: 3,
                    cluster_rows: 20,
                    max_slab_rows: 100,
                    scratch: scratch.path().into(),
                },
            )
            .await
            .unwrap();
        let (_, mut manifest) = bf_wal::Wal::new(store.clone(), "loc")
            .unwrap()
            .latest_manifest()
            .await
            .unwrap()
            .unwrap();
        prepare(&store, &mut manifest).await.unwrap();
        for seg in &manifest.segments {
            let wanted = (0..seg.row_count).filter(|r| r % 3 != 0).collect();
            let own_seg = originals_before.iter().find(|s| s.id == seg.id).unwrap();
            let own = segment::read_packed(&store, own_seg, &wanted, "v")
                .await
                .unwrap();
            assert_eq!(
                Source::from(&manifest)
                    .read(&store, seg, &wanted, "v", true)
                    .await
                    .unwrap(),
                own
            );
        }
        // A stale tier must restore every surviving segment before its sole vector source
        // disappears, even when a different covered segment is no longer reachable.
        let snapshot = manifest.clone();
        let mut dropping = manifest.clone();
        dropping.segments.truncate(1);
        crate::indexer::retire_stale_tiers(&store, "loc", &mut dropping, (&snapshot).into())
            .await
            .unwrap();
        assert!(dropping.ann_tiers.is_empty());
        let survived = &dropping.segments[0];
        assert!(survived.files.contains_key("annv2:v"));
        let wanted = (0..survived.row_count).collect();
        let restored = segment::read_packed(&store, survived, &wanted, "v")
            .await
            .unwrap();
        let old = originals_before
            .iter()
            .find(|s| s.id == survived.id)
            .unwrap();
        assert_eq!(
            restored,
            segment::read_packed(&store, old, &wanted, "v")
                .await
                .unwrap()
        );
        let mut broken = snapshot.clone();
        broken.segments.truncate(1);
        let unreachable = broken.clone();
        assert!(
            crate::indexer::retire_stale_tiers(&store, "loc", &mut broken, (&unreachable).into())
                .await
                .is_err()
        );
        assert!(
            !broken.ann_tiers.is_empty(),
            "an unresolvable source must prevent tier deletion"
        );
        // A target with interleaved source rows must map back by rank/select, rather than
        // adding its current row to a source's old base.
        let mut wrong_shape = originals_before[0].clone();
        wrong_shape.schema.get_mut("v").unwrap().data_type =
            Some(bf_types::AttributeType::Vector {
                dimensions: 2,
                f16: true,
            });
        assert!(
            segment::read_packed(&store, &wrong_shape, &[0].into_iter().collect(), "v")
                .await
                .is_err()
        );
        let originals = originals_before;
        let mut target = originals[0].clone();
        target.id = "merged".into();
        target.row_count = originals.iter().map(|s| s.row_count).sum();
        let merged: Vec<_> = originals
            .iter()
            .enumerate()
            .map(|(i, s)| lineage::Merged {
                segment: s.id.clone(),
                rows: (0..s.row_count).map(|r| (r, r * 2 + i as u32)).collect(),
            })
            .collect();
        // record requires ID-preserving, ascending pairs; these interleaved pairs satisfy it.
        lineage::record(&store, "loc", &mut manifest, &merged, &[target.clone()])
            .await
            .unwrap();
        manifest.segments = vec![target.clone()];
        let wanted: RoaringBitmap = (0..target.row_count).collect();
        let (rows, data) = Source::from(&manifest)
            .read(&store, &target, &wanted, "v", true)
            .await
            .unwrap();
        assert_eq!(rows.len(), target.row_count as usize);
        for (r, raw) in rows.into_iter().zip(data.as_chunks::<6>().0) {
            let source = &originals[(r % 2) as usize];
            let expected =
                segment::read_packed(&store, source, &[r / 2].into_iter().collect(), "v")
                    .await
                    .unwrap();
            assert_eq!(raw.as_slice(), expected.1);
        }
        drop(scratch);
        std::fs::remove_dir(".scratch/d4-locator").unwrap();
    }
}
