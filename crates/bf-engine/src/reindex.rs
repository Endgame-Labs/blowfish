//! ANN object upgrade (`blowfish reindex`): merge each older index and its fragments into one
//! object, upgrading row-major codes to fast scan. The indexer runs it once per namespace.
//!
//! Index objects are immutable, so a rewrite writes new objects under new keys and then swaps
//! them into the manifest in one fenced publication. Nothing else changes: documents, the WAL,
//! the full-text and filter indexes, and every cluster center, row membership, and full-precision
//! vector stay as they are, and nothing is retrained (see `bf_vector::convert_layout`).
//!
//! The job has three phases per batch of segments (and then per batch of recluster slabs):
//!
//! 1. **Convert** (no lock, no claim). One index at a time: read the object, rewrite its codes,
//!    merge its payloads, and upload one object under a fresh key. A crash here leaves unreferenced
//!    objects and an unchanged manifest.
//! 2. **Claim**. Publish the latest manifest with the epoch advanced by one. This fences an
//!    indexer on another node that read the old manifest: its publication fails and it retries
//!    from the new one, exactly as it does for a recluster. The claim lasts only as long as the
//!    next step.
//! 3. **Swap and publish**. Re-read the manifest, and for each converted index that is still the
//!    one the manifest names (a compaction may have replaced its segment), point the segment at the
//!    new objects, retire the old ones for normal garbage collection, rebuild the ANN directories,
//!    and publish against the claim. A conflict repeats 2 and 3 from the newest manifest; the
//!    converted objects stay valid. Writes that arrived meanwhile are in the WAL, which a
//!    publication does not touch, and `indexed_through` is carried over unchanged.
//!
//! A batch's swap is durable on its own, so a run interrupted between batches keeps what it
//! published, and running it again skips every single ANN object.
use crate::{Engine, ann_dir, log_error, segment};
use bf_types::ApiError;
use bf_vector::CodeLayout;
use bf_wal::{Manifest, Segment, SegmentFile, SlabSet, Wal};
use std::collections::BTreeSet;

/// How to run an upgrade.
#[derive(Clone, Debug)]
pub struct ReindexOptions {
    /// Report what would change, and write nothing.
    pub dry_run: bool,
    /// Publish after this many bytes of converted index objects (each publication rebuilds the
    /// ANN directories, so it is not done per segment).
    pub batch_bytes: u64,
    /// Publish after this many converted indexes.
    pub batch_indexes: usize,
}
impl Default for ReindexOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            batch_bytes: 1 << 30,
            batch_indexes: 32,
        }
    }
}

/// Index counts and bytes in the current object format and each legacy code layout.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LayoutCounts {
    pub row_major: (usize, u64),
    pub fast_scan: (usize, u64),
    /// Current single ANN objects (including flat indexes).
    pub single_object: (usize, u64),
    /// Legacy flat indexes and pre-compact code layouts.
    pub other: (usize, u64),
}
impl LayoutCounts {
    fn add(&mut self, layout: CodeLayout, bytes: u64, current: bool) {
        let slot = if current {
            &mut self.single_object
        } else {
            match layout {
                CodeLayout::RowMajor => &mut self.row_major,
                CodeLayout::FastScan => &mut self.fast_scan,
                CodeLayout::Other => &mut self.other,
            }
        };
        slot.0 += 1;
        slot.1 += bytes;
    }
}

/// What a reindex did.
#[derive(Clone, Debug, Default)]
pub struct ReindexReport {
    /// Per-segment indexes before the run.
    pub segments: LayoutCounts,
    /// A recluster's slabs before the run.
    pub slabs: LayoutCounts,
    /// Indexes rewritten and published (segments, then slabs).
    pub converted_segments: usize,
    pub converted_slabs: usize,
    /// Bytes of index objects written.
    pub bytes_written: u64,
    /// Converted indexes dropped because a concurrent publication replaced their segment.
    pub superseded: usize,
    pub publications: usize,
    pub seconds: f64,
}

/// One index rewritten and uploaded but not yet in the manifest.
struct Converted {
    /// Segment or slab id.
    id: String,
    attr: String,
    /// The `ann:` object the conversion was made from.
    old_ann: String,
    /// New files by file-map key, and the old ones they replace.
    new: Vec<(String, SegmentFile)>,
    old: Vec<SegmentFile>,
    bytes: u64,
}

enum Batch {
    Segments(Vec<Converted>),
    Slabs(String, Vec<Converted>),
}

impl Engine {
    /// Rewrite legacy vector indexes of `ns` as single ANN objects. Safe beside a running server on the
    /// same bucket (see the module notes). Prints progress to standard error.
    pub async fn reindex(
        &self,
        ns: &str,
        opts: &ReindexOptions,
    ) -> Result<ReindexReport, ApiError> {
        let _busy = bf_alloc::busy();
        let context = crate::read_cache::ReadContext::new(self.0.read_cache.clone());
        crate::read_cache::scope(context, self.reindex_inner(ns, opts)).await
    }

    /// Every namespace in the bucket (by its objects' prefixes), sorted.
    pub async fn namespace_names(&self) -> Result<Vec<String>, ApiError> {
        let mut names = BTreeSet::new();
        for obj in self
            .0
            .store
            .list("ns/")
            .await
            .map_err(|e| ApiError::new(503, format!("listing failed: {e}")))?
        {
            if let Some(ns) = obj
                .key
                .strip_prefix("ns/")
                .and_then(|p| p.split('/').next())
            {
                names.insert(ns.to_owned());
            }
        }
        Ok(names.into_iter().collect())
    }

    async fn reindex_inner(
        &self,
        ns: &str,
        opts: &ReindexOptions,
    ) -> Result<ReindexReport, ApiError> {
        let t0 = std::time::Instant::now();
        let store = &self.0.store;
        let wal = Wal::new(store.clone(), ns).map_err(log_error)?;
        let Some((_, manifest)) = wal.latest_manifest().await.map_err(log_error)? else {
            return Err(ApiError::new(404, "namespace has no checkpoint"));
        };
        let attrs = attributes(&manifest);
        let mut report = ReindexReport::default();
        // Survey: one small ranged read per index.
        let mut todo: Vec<(String, String, SegmentFile)> = Vec::new();
        for seg in &manifest.segments {
            for attr in &attrs {
                if let Some(file) = seg
                    .files
                    .get(&format!("annv2:{attr}"))
                    .or_else(|| seg.files.get(&format!("ann:{attr}")))
                {
                    let layout = self.layout_of(file).await?;
                    report.segments.add(
                        layout,
                        file.size,
                        seg.files.contains_key(&format!("annv2:{attr}")),
                    );
                    if seg.files.contains_key(&format!("ann:{attr}")) {
                        todo.push((seg.id.clone(), attr.clone(), file.clone()));
                    }
                }
            }
        }
        let mut slab_todo: Vec<(String, Vec<(Segment, SegmentFile)>)> = Vec::new();
        {
            for attr in &attrs {
                if let Some(set) = manifest.ann_slabs.get(attr) {
                    let mut list = Vec::new();
                    for slab in &set.slabs {
                        if let Some(file) = slab
                            .files
                            .get(&format!("annv2:{attr}"))
                            .or_else(|| slab.files.get(&format!("ann:{attr}")))
                        {
                            let layout = self.layout_of(file).await?;
                            report.slabs.add(
                                layout,
                                file.size,
                                slab.files.contains_key(&format!("annv2:{attr}")),
                            );
                            if slab.files.contains_key(&format!("ann:{attr}")) {
                                list.push((slab.clone(), file.clone()));
                            }
                        }
                    }
                    slab_todo.push((attr.clone(), list));
                }
            }
        }
        if !todo.is_empty() || slab_todo.iter().any(|(_, l)| !l.is_empty()) {
            tracing::info!(
                "reindex {ns}: {} segment indexes to rewrite of {}, {} slabs of {}",
                todo.len(),
                report.segments.row_major.0
                    + report.segments.fast_scan.0
                    + report.segments.other.0
                    + report.segments.single_object.0,
                slab_todo.iter().map(|(_, l)| l.len()).sum::<usize>(),
                report.slabs.row_major.0
                    + report.slabs.fast_scan.0
                    + report.slabs.other.0
                    + report.slabs.single_object.0,
            );
        }
        if opts.dry_run {
            report.seconds = t0.elapsed().as_secs_f64();
            return Ok(report);
        }

        // Segments.
        let total = todo.len();
        let mut batch: Vec<Converted> = Vec::new();
        let mut batch_bytes = 0u64;
        for (n, (id, attr, file)) in todo.into_iter().enumerate() {
            let seg = manifest
                .segments
                .iter()
                .find(|s| s.id == id)
                .expect("surveyed segment");
            let converted = self.convert_segment_index(ns, seg, &attr, &file).await?;
            report.bytes_written += converted.bytes;
            batch_bytes += converted.bytes;
            batch.push(converted);
            tracing::info!(
                "reindex {ns}: segment index {}/{total} converted at {:.0}s",
                n + 1,
                t0.elapsed().as_secs_f64()
            );
            if batch_bytes >= opts.batch_bytes || batch.len() >= opts.batch_indexes {
                self.publish_batch(
                    &wal,
                    ns,
                    Batch::Segments(std::mem::take(&mut batch)),
                    &mut report,
                )
                .await?;
                batch_bytes = 0;
            }
        }
        if !batch.is_empty() {
            self.publish_batch(&wal, ns, Batch::Segments(batch), &mut report)
                .await?;
        }

        // Slabs.
        for (attr, list) in slab_todo {
            let total = list.len();
            let mut batch: Vec<Converted> = Vec::new();
            let mut batch_bytes = 0u64;
            for (n, (slab, file)) in list.into_iter().enumerate() {
                let converted = self.convert_slab(ns, &slab, &attr, &file).await?;
                report.bytes_written += converted.bytes;
                batch_bytes += converted.bytes;
                batch.push(converted);
                tracing::info!(
                    "reindex {ns}: slab {}/{total} of {attr} converted at {:.0}s",
                    n + 1,
                    t0.elapsed().as_secs_f64()
                );
                if batch_bytes >= opts.batch_bytes || batch.len() >= opts.batch_indexes {
                    self.publish_batch(
                        &wal,
                        ns,
                        Batch::Slabs(attr.clone(), std::mem::take(&mut batch)),
                        &mut report,
                    )
                    .await?;
                    batch_bytes = 0;
                }
            }
            if !batch.is_empty() {
                self.publish_batch(&wal, ns, Batch::Slabs(attr.clone(), batch), &mut report)
                    .await?;
            }
        }
        report.seconds = t0.elapsed().as_secs_f64();
        Ok(report)
    }

    async fn layout_of(&self, file: &SegmentFile) -> Result<CodeLayout, ApiError> {
        let head = self
            .0
            .store
            .get_range(&file.key, 0..file.size.min(64))
            .await
            .map_err(|e| ApiError::new(503, format!("index unreadable: {e}")))?;
        bf_vector::code_layout(&head).map_err(segment::index_error)
    }

    /// Copy an old index into one ANN object, upgrading its codes if necessary.
    async fn convert_segment_index(
        &self,
        ns: &str,
        seg: &Segment,
        attr: &str,
        file: &SegmentFile,
    ) -> Result<Converted, ApiError> {
        let blob = segment::read_whole_uncached(&self.0.store, file).await?;
        let (blob, meta) =
            tokio::task::spawn_blocking(move || bf_vector::Fragment::build_ann(&blob))
                .await
                .map_err(segment::index_error)?
                .map_err(segment::index_error)?;
        let bytes = blob.len() as u64;
        let kind = if seg.id.starts_with("slab-") {
            "slabs"
        } else {
            "segments"
        };
        let key = format!("ns/{ns}/{kind}/{}/ann.{}.bfa", seg.id, uuid::Uuid::new_v4());
        let new = segment::put_file(&self.0.store, key, blob, meta).await?;
        let old = [
            format!("ann:{attr}"),
            format!("annfrag:{attr}"),
            format!("annfrag2:{attr}"),
        ]
        .iter()
        .filter_map(|k| seg.files.get(k).cloned())
        .collect();
        Ok(Converted {
            id: seg.id.clone(),
            attr: attr.to_string(),
            old_ann: file.key.clone(),
            new: vec![(format!("annv2:{attr}"), new)],
            old,
            bytes,
        })
    }

    async fn convert_slab(
        &self,
        ns: &str,
        slab: &Segment,
        attr: &str,
        file: &SegmentFile,
    ) -> Result<Converted, ApiError> {
        self.convert_segment_index(ns, slab, attr, file).await
    }

    /// Claim, swap the batch into the newest manifest, and publish; repeat on a conflict.
    async fn publish_batch(
        &self,
        wal: &Wal,
        ns: &str,
        batch: Batch,
        report: &mut ReindexReport,
    ) -> Result<(), ApiError> {
        let result = self.publish_batch_inner(wal, ns, &batch, report).await;
        if !matches!(result, Ok(true)) {
            // Nothing references the converted objects of a batch that was not (or only partly)
            // swapped in; `inner` has already deleted the superseded ones on success.
            if result.is_err() {
                for c in batch.converted() {
                    for (_, f) in &c.new {
                        let _ = self.0.store.delete(&f.key).await;
                    }
                }
            }
        }
        result.map(|_| ())
    }

    async fn publish_batch_inner(
        &self,
        wal: &Wal,
        ns: &str,
        batch: &Batch,
        report: &mut ReindexReport,
    ) -> Result<bool, ApiError> {
        let store = &self.0.store;
        for attempt in 0..12u32 {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(100 * u64::from(attempt)))
                    .await;
            }
            let (sequence, mut manifest) = wal
                .latest_manifest()
                .await
                .map_err(log_error)?
                .ok_or_else(|| ApiError::new(404, "namespace has no checkpoint"))?;
            let epoch = manifest.indexer_epoch;
            manifest.indexer_epoch += 1;
            let claim = match wal.publish_fenced(sequence, epoch, &manifest).await {
                Ok(s) => s,
                Err(bf_wal::Error::Fenced) => continue,
                Err(e) => return Err(log_error(e)),
            };
            let mut manifest = wal
                .latest_manifest()
                .await
                .map_err(log_error)?
                .ok_or_else(|| ApiError::new(503, "manifest disappeared"))?
                .1;
            if manifest.indexer_epoch != epoch + 1 {
                continue;
            }
            let (applied, superseded) = match batch {
                Batch::Segments(list) => apply_segments(store, ns, &mut manifest, list).await?,
                Batch::Slabs(attr, list) => self.apply_slabs(ns, &mut manifest, attr, list).await?,
            };
            if applied == 0 {
                // Everything was replaced by someone else; the claim published nothing new.
                for c in batch.converted() {
                    for (_, f) in &c.new {
                        let _ = store.delete(&f.key).await;
                    }
                }
                report.superseded += superseded.len();
                return Ok(true);
            }
            match wal
                .publish_fenced(claim, manifest.indexer_epoch, &manifest)
                .await
            {
                Ok(_) => {
                    match batch {
                        Batch::Segments(_) => report.converted_segments += applied,
                        Batch::Slabs(..) => report.converted_slabs += applied,
                    }
                    report.publications += 1;
                    report.superseded += superseded.len();
                    for c in batch
                        .converted()
                        .filter(|c| superseded.contains(&(c.id.clone(), c.attr.clone())))
                    {
                        for (_, f) in &c.new {
                            let _ = store.delete(&f.key).await;
                        }
                    }
                    return Ok(true);
                }
                Err(bf_wal::Error::Fenced) => continue,
                Err(e) => return Err(log_error(e)),
            }
        }
        Err(ApiError::new(
            409,
            "reindex could not publish: the namespace kept changing; run it again",
        ))
    }

    async fn apply_slabs(
        &self,
        ns: &str,
        manifest: &mut Manifest,
        attr: &str,
        list: &[Converted],
    ) -> Result<(usize, BTreeSet<(String, String)>), ApiError> {
        let store = &self.0.store;
        let Some(set) = manifest.ann_slabs.get(attr).cloned() else {
            return Ok((
                0,
                list.iter()
                    .map(|c| (c.id.clone(), c.attr.clone()))
                    .collect(),
            ));
        };
        let mut slabs = set.slabs.clone();
        let (mut applied, mut superseded) = (0usize, BTreeSet::new());
        let mut retired = Vec::new();
        for c in list {
            match slabs.iter_mut().find(|s| s.id == c.id) {
                Some(slab)
                    if slab
                        .files
                        .get(&format!("ann:{attr}"))
                        .is_some_and(|f| f.key == c.old_ann) =>
                {
                    for prefix in ["ann:", "annfrag:", "annfrag2:"] {
                        slab.files.remove(&format!("{prefix}{}", c.attr));
                    }
                    for (key, file) in &c.new {
                        slab.files.insert(key.clone(), file.clone());
                    }
                    retired.extend(c.old.iter().map(|f| f.key.clone()));
                    applied += 1;
                }
                _ => {
                    superseded.insert((c.id.clone(), c.attr.clone()));
                }
            }
        }
        if applied == 0 {
            return Ok((0, superseded));
        }
        // The directory over the slabs, as a recluster writes it.
        let Some(dir) =
            ann_dir::build(store, ns, &slabs, attr, ann_dir::Layout::Slabs, None).await?
        else {
            // Keep the old set protected if a concurrent replacement cannot supply a directory.
            // Ordinary partial batches build a directory covering both old and current slabs.
            for key in retired {
                crate::indexer::retire(manifest, key);
            }
            manifest
                .ann_slabs
                .insert(attr.to_string(), SlabSet { slabs, ..set });
            return Ok((applied, superseded));
        };
        let paged_dir = ann_dir::load(store, &dir, None).await?;
        let pages = ann_dir::build_paged(store, ns, attr, paged_dir.clone(), &slabs).await?;
        if pages.is_none() {
            let bytes = segment::ObjectReader::new(store, &dir)
                .bypass_cache()
                .read(0..dir.size)
                .await
                .map_err(segment::index_error)?;
            ann_dir::write_hint(store, ns, ann_dir::Layout::Slabs, attr, &dir.key, &bytes).await;
        }
        for key in retired {
            crate::indexer::retire(manifest, key);
        }
        crate::indexer::retire(manifest, set.dir.key.clone());
        if let Some(old) = &set.pages {
            crate::indexer::retire(manifest, old.key.clone());
        }
        manifest.ann_slabs.insert(
            attr.to_string(),
            SlabSet {
                covered: set.covered,
                slabs,
                dir,
                pages,
            },
        );
        Ok((applied, superseded))
    }
}

impl Batch {
    fn converted(&self) -> impl Iterator<Item = &Converted> {
        match self {
            Batch::Segments(l) | Batch::Slabs(_, l) => l.iter(),
        }
    }
}

/// The vector attributes to process.
fn attributes(manifest: &Manifest) -> Vec<String> {
    manifest
        .segments
        .iter()
        .flat_map(|s| s.files.keys())
        .filter_map(|k| {
            k.strip_prefix("ann:")
                .or_else(|| k.strip_prefix("annv2:"))
                .map(str::to_string)
        })
        .chain(manifest.ann_slabs.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Point each still-current segment at its converted objects and rebuild the ANN directories
/// (as the indexer does after a segment list changes). Returns how many were swapped in and the
/// ids whose segment had meanwhile been replaced or already rewritten.
async fn apply_segments(
    store: &bf_storage::Storage,
    ns: &str,
    manifest: &mut Manifest,
    list: &[Converted],
) -> Result<(usize, BTreeSet<(String, String)>), ApiError> {
    let (mut applied, mut superseded) = (0usize, BTreeSet::new());
    for c in list {
        let current = manifest
            .segments
            .iter_mut()
            .find(|s| s.id == c.id)
            .filter(|s| {
                s.files
                    .get(&format!("ann:{}", c.attr))
                    .is_some_and(|f| f.key == c.old_ann)
            });
        let Some(seg) = current else {
            superseded.insert((c.id.clone(), c.attr.clone()));
            continue;
        };
        for prefix in ["ann:", "annfrag:", "annfrag2:"] {
            seg.files.remove(&format!("{prefix}{}", c.attr));
        }
        for (key, file) in &c.new {
            seg.files.insert(key.clone(), file.clone());
        }
        for f in &c.old {
            crate::indexer::retire(manifest, f.key.clone());
        }
        applied += 1;
    }
    if applied == 0 {
        return Ok((0, superseded));
    }
    let attributes: BTreeSet<String> = manifest
        .segments
        .iter()
        .flat_map(|s| s.files.keys())
        .filter_map(|k| {
            k.strip_prefix("annv2:")
                .or_else(|| k.strip_prefix("ann:"))
                .map(str::to_string)
        })
        .collect();
    let previous_dirs = std::mem::take(&mut manifest.ann_dirs);
    for attr in attributes {
        let layouts = [ann_dir::Layout::Segments];
        let built = futures::future::try_join_all(layouts.iter().map(|layout| {
            ann_dir::build(
                store,
                ns,
                &manifest.segments,
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
    for old in previous_dirs.values() {
        if !manifest.ann_dirs.values().any(|f| f.key == old.key) {
            crate::indexer::retire(manifest, old.key.clone());
        }
    }
    Ok((applied, superseded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bf_storage::{Storage, StorageConfig};
    use serde_json::json;

    #[tokio::test]
    async fn superseded_attribute_does_not_discard_another_attribute_of_the_same_segment() {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let engine = Engine::with_config(
            store.clone(),
            crate::EngineConfig {
                indexing: false,
                ..Default::default()
            },
        );
        engine
            .write(
                "t",
                serde_json::from_value(json!({
                    "distance_metric":"euclidean_squared",
                    "schema":{"a":{"type":"[2]f32","ann":true},"b":{"type":"[2]f32","ann":true}},
                    "upsert_rows":[{"id":1,"a":[1.,2.],"b":[3.,4.]}]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("t", true).await.unwrap();
        let wal = Wal::new(store.clone(), "t").unwrap();
        let (_, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
        let seg = &mut manifest.segments[0];
        let a = seg.files.remove("annv2:a").unwrap();
        let b = seg.files.remove("annv2:b").unwrap();
        seg.files.insert("ann:a".into(), a.clone());
        seg.files.insert("ann:b".into(), b.clone());
        let list = vec![
            Converted {
                id: seg.id.clone(),
                attr: "a".into(),
                old_ann: a.key.clone(),
                new: vec![("annv2:a".into(), a.clone())],
                old: vec![],
                bytes: a.size,
            },
            Converted {
                id: seg.id.clone(),
                attr: "b".into(),
                old_ann: "already-replaced".into(),
                new: vec![("annv2:b".into(), b)],
                old: vec![],
                bytes: 0,
            },
        ];
        let (applied, superseded) = apply_segments(&store, "t", &mut manifest, &list)
            .await
            .unwrap();
        assert_eq!(applied, 1);
        assert!(manifest.segments[0].files.contains_key("annv2:a"));
        assert!(manifest.segments[0].files.contains_key("ann:b"));
        assert_eq!(
            superseded,
            BTreeSet::from([(list[1].id.clone(), "b".into())])
        );
        assert!(!superseded.contains(&(list[0].id.clone(), "a".into())));
    }
    #[tokio::test]
    async fn successive_upgrade_batches_without_a_directory_refresh_resident_file_keys() {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let engine = Engine::with_config(
            store.clone(),
            crate::EngineConfig {
                indexing: false,
                ..Default::default()
            },
        );
        engine
            .write(
                "t",
                serde_json::from_value(json!({
                    "distance_metric":"euclidean_squared",
                    "schema":{"a":{"type":"[2]f32","ann":true}},
                    "upsert_rows":[{"id":1,"a":[1.,2.]}]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("t", true).await.unwrap();
        let wal = Wal::new(store.clone(), "t").unwrap();
        for batch in 0..2 {
            let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
            let file = manifest.segments[0].files.get_mut("annv2:a").unwrap();
            let blob = store.get(&file.key).await.unwrap();
            let replacement = format!("ns/t/segments/rewrite-{batch}.bfa");
            store.create_unique(&replacement, blob).await.unwrap();
            file.key = replacement.clone();
            manifest.ann_dirs.clear();
            wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
                .await
                .unwrap();
            let handle = engine.handle("t").await.unwrap();
            {
                let mut state = handle.state.lock().await;
                crate::snapshot::catch_up(&wal, &mut state, None, &store)
                    .await
                    .unwrap();
            }
            let result = engine
                .query(
                    "t",
                    serde_json::from_value(json!({"rank_by":["a","ANN",[1.,2.]],"top_k":1}))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(result.result.rows.unwrap()[0]["id"], 1);
            assert_eq!(
                handle.state.lock().await.segments[0].files["annv2:a"].key,
                replacement
            );
        }
    }
}

#[cfg(test)]
#[path = "reindex_migration_tests.rs"]
mod migration_tests;
