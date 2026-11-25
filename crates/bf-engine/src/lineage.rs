//! Lineage (round R4 of `docs/design/auto-recluster.md`): where the rows of tier-covered segments
//! went when compaction merged them.
//!
//! A recluster tier addresses rows by global row number (`SlabCover::base` plus the row within its
//! covered segment), and its slabs are immutable. When compaction merges covered segments, the
//! manifest records, per covered source segment, the segments that hold its surviving rows now and
//! a map object for each: the source rows placed there and the target rows they became. Segments
//! are in ID order, so both lists are ascending and aligned: `rank` in one and `select` in the
//! other map a row either way. A merge of a target composes its maps, so a tier row is always one
//! hop from where it lives.
//!
//! Compaction never mixes coverage classes (the tiers covering a segment's rows, directly or
//! through lineage), so a segment is either fully covered by its class's tiers or not covered.
use crate::segment;
use bf_storage::Storage;
use bf_types::ApiError;
use bf_vector::RoaringBitmap;
use bf_wal::{Lineage, LineagePart, Manifest, Segment, SlabSet};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

/// A part's map object: `from[k]` (source row) became `to[k]` (target row).
#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Map {
    pub from: RoaringBitmap,
    pub to: RoaringBitmap,
}
impl Map {
    /// The target row a source row became, if it was placed.
    pub fn forward(&self, row: u32) -> Option<u32> {
        if !self.from.contains(row) {
            return None;
        }
        self.to.select((self.from.rank(row) - 1) as u32)
    }
    /// The source row a target row came from, if it came from this source.
    #[allow(dead_code)] // Used once segments drop their own ANN objects (D4).
    pub fn back(&self, row: u32) -> Option<u32> {
        if !self.to.contains(row) {
            return None;
        }
        self.from.select((self.to.rank(row) - 1) as u32)
    }
    /// `(source row, target row)` pairs in order.
    pub fn pairs(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.from.iter().zip(self.to.iter())
    }
}

/// Load a part's map (cached by its immutable key).
pub(crate) async fn load(store: &Storage, part: &LineagePart) -> Result<Arc<Map>, ApiError> {
    let map: Arc<Map> = segment::get_value(store, &part.map).await?;
    if map.from.len() != map.to.len() || map.from.len() != u64::from(part.rows) {
        return Err(segment::corrupt());
    }
    Ok(map)
}

/// The tier keys (directory object keys) covering each segment's rows, directly or through
/// lineage. Segments no tier covers are absent.
pub(crate) fn classes(
    segments: &[Segment],
    tiers: &BTreeMap<String, Vec<SlabSet>>,
    lineage: &[Lineage],
) -> HashMap<String, BTreeSet<String>> {
    let present: BTreeSet<&str> = segments.iter().map(|s| s.id.as_str()).collect();
    let moved: HashMap<&str, &Lineage> = lineage.iter().map(|l| (l.source.as_str(), l)).collect();
    let mut out: HashMap<String, BTreeSet<String>> = HashMap::new();
    for set in tiers.values().flatten() {
        for c in &set.covered {
            if present.contains(c.segment.as_str()) {
                out.entry(c.segment.clone())
                    .or_default()
                    .insert(set.dir.key.clone());
            } else if let Some(entry) = moved.get(c.segment.as_str()) {
                for part in &entry.parts {
                    out.entry(part.target.clone())
                        .or_default()
                        .insert(set.dir.key.clone());
                }
            }
        }
    }
    out
}

/// Segments whose rows some tier covers, directly or through lineage.
pub(crate) fn covered(
    segments: &[Segment],
    tiers: &BTreeMap<String, Vec<SlabSet>>,
    lineage: &[Lineage],
) -> BTreeSet<String> {
    classes(segments, tiers, lineage).into_keys().collect()
}

/// Where one covered segment's rows are now.
#[derive(Clone)]
pub(crate) enum Place {
    /// Still in its own segment, at this position among the live segments.
    Here(usize),
    /// Merged away: in these segments (positions), through their maps.
    Moved(Vec<(usize, Arc<Map>)>),
}

/// Where each covered segment of a tier is now, in `SlabSet::covered` order.
#[derive(Clone)]
pub(crate) struct Placement {
    pub places: Vec<Place>,
    /// Map object keys, in order (the tier's mask depends on them).
    pub maps: Vec<String>,
}

/// Find a covered segment among `segments`, from position `from` on first: segments keep their
/// relative order across publications.
fn find(segments: &[Segment], id: &str, from: usize) -> Option<usize> {
    segments[from.min(segments.len())..]
        .iter()
        .position(|s| s.id == id)
        .map(|p| p + from)
        .or_else(|| segments.iter().position(|s| s.id == id))
}

/// Whether every covered segment of `set` is live (with its row count) or reachable by lineage
/// whose targets are all live: the tier can answer.
pub(crate) fn reachable(segments: &[Segment], lineage: &[Lineage], set: &SlabSet) -> bool {
    let mut j = 0;
    set.covered
        .iter()
        .all(|c| match find(segments, &c.segment, j) {
            Some(at) => {
                j = at + 1;
                segments[at].row_count == c.rows
            }
            None => lineage
                .iter()
                .find(|l| l.source == c.segment)
                .is_some_and(|l| {
                    l.parts
                        .iter()
                        .all(|p| segments.iter().any(|s| s.id == p.target))
                }),
        })
}

/// Resolve where `set`'s covered rows are, loading the lineage maps it needs; `None` when some
/// covered segment is neither live nor reachable by lineage (the tier can no longer answer).
pub(crate) async fn place(
    store: &Storage,
    segments: &[Segment],
    lineage: &[Lineage],
    set: &SlabSet,
) -> Result<Option<Placement>, ApiError> {
    let mut places = Vec::with_capacity(set.covered.len());
    let mut wanted: Vec<(usize, &LineagePart)> = Vec::new();
    let mut j = 0;
    for (p, c) in set.covered.iter().enumerate() {
        if let Some(at) = find(segments, &c.segment, j) {
            if segments[at].row_count != c.rows {
                return Ok(None);
            }
            j = at + 1;
            places.push(Place::Here(at));
            continue;
        }
        let Some(entry) = lineage.iter().find(|l| l.source == c.segment) else {
            return Ok(None);
        };
        for part in &entry.parts {
            wanted.push((p, part));
        }
        places.push(Place::Moved(Vec::new()));
    }
    let loaded =
        futures::future::try_join_all(wanted.iter().map(|(_, part)| load(store, part))).await?;
    let mut maps = Vec::with_capacity(wanted.len());
    for ((p, part), map) in wanted.into_iter().zip(loaded) {
        let Some(at) = segments.iter().position(|s| s.id == part.target) else {
            return Ok(None);
        };
        if let Place::Moved(parts) = &mut places[p] {
            parts.push((at, map));
        }
        maps.push(part.map.key.clone());
    }
    Ok(Some(Placement { places, maps }))
}

/// The segment holding each of `set`'s covered runs, when every run is still live and unchanged
/// (same rows, same live-row generation) and the runs account for every segment. Then no lineage
/// map or live mask is needed, so the answer needs no I/O. `None` means "use [`place`]".
pub(crate) fn direct(segments: &[Segment], set: &SlabSet) -> Option<Vec<usize>> {
    if set.covered.len() != segments.len() {
        return None;
    }
    set.covered
        .iter()
        .zip(segments)
        .enumerate()
        .map(|(at, (cover, segment))| {
            (segment.id == cover.segment
                && segment.row_count == cover.rows
                && segment.live_generation == cover.live_generation)
                .then_some(at)
        })
        .collect()
}

impl Placement {
    /// The live segment and row holding global row `row` of a tier with these covered segments,
    /// or `None` when the row was deleted before a merge.
    pub fn real(&self, covered: &[bf_wal::SlabCover], row: u32) -> Option<(usize, u32)> {
        let p = covered.partition_point(|c| c.base <= row).checked_sub(1)?;
        let local = row - covered[p].base;
        match &self.places[p] {
            Place::Here(at) => Some((*at, local)),
            Place::Moved(parts) => parts
                .iter()
                .find_map(|(at, map)| map.forward(local).map(|r| (*at, r))),
        }
    }
    /// Positions of every segment holding some of the tier's rows.
    pub fn positions(&self) -> impl Iterator<Item = usize> + '_ {
        self.places.iter().flat_map(|place| match place {
            Place::Here(at) => vec![*at],
            Place::Moved(parts) => parts.iter().map(|(at, _)| *at).collect(),
        })
    }
}

/// What compaction learned about one merged source segment: its rows that survived, and the
/// position of each in the merged (ID) order, both ascending.
pub(crate) struct Merged {
    pub segment: String,
    pub rows: Vec<(u32, u32)>,
}

/// Record lineage for a compaction that merged `merged` into `outputs` (in order: output `b`
/// holds merged positions from the sum of the earlier outputs' row counts on). Sources a tier
/// covers directly get an entry; parts that targeted a merged source are composed through it.
/// Writes the new map objects and retires the replaced ones.
pub(crate) async fn record(
    store: &Storage,
    ns: &str,
    manifest: &mut Manifest,
    merged: &[Merged],
    outputs: &[Segment],
) -> Result<(), ApiError> {
    let covered: BTreeSet<&str> = manifest
        .ann_tiers
        .values()
        .flatten()
        .flat_map(|t| t.covered.iter().map(|c| c.segment.as_str()))
        .collect();
    let starts: Vec<u32> = outputs
        .iter()
        .scan(0u32, |at, s| {
            let start = *at;
            *at += s.row_count;
            Some(start)
        })
        .collect();
    // A merged position's output and row there.
    let locate = |position: u32| -> (usize, u32) {
        let b = starts.partition_point(|s| *s <= position) - 1;
        (b, position - starts[b])
    };
    let by_source: HashMap<&str, &Merged> =
        merged.iter().map(|m| (m.segment.as_str(), m)).collect();
    // New entries' parts, as (source row, merged position) pairs per source.
    let mut placed: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
    for m in merged {
        if covered.contains(m.segment.as_str()) {
            placed.insert(m.segment.clone(), m.rows.clone());
        }
    }
    let mut retired = Vec::new();
    // Parts of composed entries whose targets this merge left alone: they stay as they are.
    let mut kept: BTreeMap<String, Vec<LineagePart>> = BTreeMap::new();
    for entry in &manifest.lineage {
        let through: Vec<&LineagePart> = entry
            .parts
            .iter()
            .filter(|p| by_source.contains_key(p.target.as_str()))
            .collect();
        if through.is_empty() {
            continue;
        }
        kept.insert(
            entry.source.clone(),
            entry
                .parts
                .iter()
                .filter(|p| !by_source.contains_key(p.target.as_str()))
                .cloned()
                .collect(),
        );
        let mut pairs = Vec::new();
        for part in through {
            let map = load(store, part).await?;
            let rows = &by_source[part.target.as_str()].rows;
            for (source_row, target_row) in map.pairs() {
                if let Ok(k) = rows.binary_search_by_key(&target_row, |(r, _)| *r) {
                    pairs.push((source_row, rows[k].1));
                }
            }
            retired.push(part.map.key.clone());
        }
        placed
            .entry(entry.source.clone())
            .or_default()
            .extend(pairs);
    }
    if placed.is_empty() {
        return Ok(());
    }
    let mut entries = Vec::with_capacity(placed.len());
    for (source, mut pairs) in placed {
        pairs.sort_unstable();
        let mut maps: BTreeMap<usize, Map> = BTreeMap::new();
        for (source_row, position) in pairs {
            let (b, row) = locate(position);
            let map = maps.entry(b).or_default();
            map.from.insert(source_row);
            map.to.insert(row);
        }
        let mut parts = kept.remove(&source).unwrap_or_default();
        for (b, map) in maps {
            let target = &outputs[b].id;
            let rows = map.from.len() as u32;
            let key = format!(
                "ns/{ns}/segments/{target}/lineage-{source}-{}.bfm",
                uuid::Uuid::new_v4().simple()
            );
            let file = segment::put_value(store, key, &map).await?;
            parts.push(LineagePart {
                target: target.clone(),
                rows,
                map: file,
            });
        }
        entries.push(Lineage { source, parts });
    }
    tracing::info!(
        namespace = ns,
        sources = merged.len(),
        entries = entries.len(),
        targets = outputs.len(),
        "compaction: merged tier-covered segments, lineage recorded"
    );
    metrics::counter!("blowfish_lineage_merges_total").increment(1);
    // Composed entries replace their old selves.
    let replaced: BTreeSet<String> = entries.iter().map(|e| e.source.clone()).collect();
    manifest.lineage.retain(|l| !replaced.contains(&l.source));
    manifest.lineage.extend(entries);
    for key in retired {
        crate::indexer::retire(manifest, key);
    }
    Ok(())
}

/// Drop lineage no tier refers to any more (its source is covered by no tier), and parts whose
/// target was dropped with no rows left, retiring their maps.
pub(crate) fn prune(manifest: &mut Manifest) {
    let covered: BTreeSet<String> = manifest
        .ann_tiers
        .values()
        .flatten()
        .flat_map(|t| t.covered.iter().map(|c| c.segment.clone()))
        .collect();
    let mut retired = Vec::new();
    manifest.lineage.retain(|l| {
        let keep = covered.contains(&l.source);
        if !keep {
            retired.extend(l.parts.iter().map(|p| p.map.key.clone()));
        }
        keep
    });
    for key in retired {
        crate::indexer::retire(manifest, key);
    }
}

/// Every map object the manifest references (GC keeps them).
pub(crate) fn keys(manifest: &Manifest) -> impl Iterator<Item = &String> {
    manifest
        .lineage
        .iter()
        .flat_map(|l| l.parts.iter().map(|p| &p.map.key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_go_both_ways_by_rank_and_select() {
        let map = Map {
            from: [1u32, 4, 5, 9].into_iter().collect(),
            to: [0u32, 2, 7, 8].into_iter().collect(),
        };
        assert_eq!(map.forward(1), Some(0));
        assert_eq!(map.forward(5), Some(7));
        assert_eq!(map.forward(9), Some(8));
        assert_eq!(map.forward(2), None);
        assert_eq!(map.back(2), Some(4));
        assert_eq!(map.back(1), None);
        assert_eq!(
            map.pairs().collect::<Vec<_>>(),
            [(1, 0), (4, 2), (5, 7), (9, 8)]
        );
    }

    fn segment(id: &str, rows: u32) -> Segment {
        serde_json::from_value(serde_json::json!({
            "id": id, "wal_start": 0, "wal_end": 0, "row_count": rows, "files": {},
            "live": {"key": "", "size": 0, "metadata_len": 0}, "live_generation": 0
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn a_merge_of_one_target_keeps_the_parts_in_the_others() {
        let store = Storage::from_backend(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            object_store::path::Path::default(),
            false,
            false,
        )
        .await
        .unwrap();
        let mut manifest = Manifest {
            format_version: bf_wal::MANIFEST_FORMAT_VERSION,
            schema: Default::default(),
            distance_metric: Default::default(),
            id_type: bf_types::AttributeType::Scalar(bf_types::ScalarType::Uint),
            created_at: Default::default(),
            indexed_through: 0,
            indexer_epoch: 0,
            deleted: false,
            segments: vec![],
            stats: Default::default(),
            text_stats: None,
            ann_dirs: Default::default(),
            ann_tiers: Default::default(),
            lineage: Default::default(),
            updated_at: None,
            retired: vec![],
            checkpoints: vec![],
            pruned_through: 0,
            bulk_load_at: None,
            root: false,
        };
        // Source `s` (4 rows) was merged into `t1` (rows 0 and 2 there) and `t2` (rows 1 and 3).
        let mut parts = Vec::new();
        for (target, from, to) in [("t1", [0u32, 2], [1u32, 3]), ("t2", [1, 3], [0, 1])] {
            let map = Map {
                from: from.into_iter().collect(),
                to: to.into_iter().collect(),
            };
            let file = segment::put_value(&store, format!("m-{target}"), &map)
                .await
                .unwrap();
            parts.push(LineagePart {
                target: target.into(),
                rows: 2,
                map: file,
            });
        }
        manifest.lineage.push(Lineage {
            source: "s".into(),
            parts,
        });
        // Now `t1` (4 rows) merges alone into `u`, at positions 4 to 7.
        let merged = [Merged {
            segment: "t1".into(),
            rows: (0..4).map(|r| (r, r + 4)).collect(),
        }];
        record(&store, "n", &mut manifest, &merged, &[segment("u", 8)])
            .await
            .unwrap();
        assert_eq!(manifest.lineage.len(), 1);
        let parts = &manifest.lineage[0].parts;
        let targets: Vec<&str> = parts.iter().map(|p| p.target.as_str()).collect();
        assert_eq!(targets, ["t2", "u"]);
        assert_eq!(parts[0].map.key, "m-t2");
        let map = load(&store, &parts[1]).await.unwrap();
        assert_eq!(map.pairs().collect::<Vec<_>>(), [(0, 5), (2, 7)]);
    }
}
