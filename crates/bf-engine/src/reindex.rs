//! Layout upgrade (`blowfish reindex`): rewrite a namespace's older row-major vector indexes in
//! the fast-scan code layout. The background indexer runs it by itself, once per namespace.
//!
//! Index objects are immutable, so a rewrite writes new objects under new keys and then swaps
//! them into the manifest in one fenced publication. Nothing else changes: documents, the WAL,
//! the full-text and filter indexes, and every cluster center, row membership, and full-precision
//! vector stay as they are, and nothing is retrained (see `bf_vector::convert_layout`).
//!
//! The job has three phases per batch of segments (and then per batch of recluster slabs):
//!
//! 1. **Convert** (no lock, no claim). One index at a time: read the object, rewrite its codes,
//!    rebuild its fragments, and upload all of it under fresh keys. A crash here leaves unreferenced
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
//! published, and running it again skips every index already in the fast-scan layout.
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

/// How many index objects, and how many bytes, are in each layout.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LayoutCounts {
    pub row_major: (usize, u64),
    pub fast_scan: (usize, u64),
    /// Flat indexes and formats too old to convert.
    pub other: (usize, u64),
}
impl LayoutCounts {
    fn add(&mut self, layout: CodeLayout, bytes: u64) {
        let slot = match layout {
            CodeLayout::RowMajor => &mut self.row_major,
            CodeLayout::FastScan => &mut self.fast_scan,
            CodeLayout::Other => &mut self.other,
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
    /// Rewrite the row-major vector indexes of `ns` in the fast-scan layout. Safe beside a running server on the
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
                if let Some(file) = seg.files.get(&format!("ann:{attr}")) {
                    let layout = self.layout_of(file).await?;
                    report.segments.add(layout, file.size);
                    if layout == CodeLayout::RowMajor {
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
                        if let Some(file) = slab.files.get(&format!("ann:{attr}")) {
                            let layout = self.layout_of(file).await?;
                            report.slabs.add(layout, file.size);
                            if layout == CodeLayout::RowMajor {
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
                report.segments.row_major.0 + report.segments.fast_scan.0 + report.segments.other.0,
                slab_todo.iter().map(|(_, l)| l.len()).sum::<usize>(),
                report.slabs.row_major.0 + report.slabs.fast_scan.0 + report.slabs.other.0,
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

    /// Rewrite one object's codes (on a blocking thread), or `None` when it needs nothing.
    async fn convert_object(&self, file: &SegmentFile) -> Result<Option<Vec<u8>>, ApiError> {
        let blob = segment::read_whole_uncached(&self.0.store, file).await?;
        tokio::task::spawn_blocking(move || bf_vector::upgrade_layout(&blob))
            .await
            .map_err(segment::index_error)?
            .map_err(segment::index_error)
    }

    async fn convert_segment_index(
        &self,
        ns: &str,
        seg: &Segment,
        attr: &str,
        file: &SegmentFile,
    ) -> Result<Converted, ApiError> {
        let store = &self.0.store;
        let blob = self
            .convert_object(file)
            .await?
            .ok_or_else(|| ApiError::new(500, "index needs no conversion"))?;
        let prefix = format!("ns/{ns}/segments/{}", seg.id);
        let meta = bf_vector::metadata_len(&blob).map_err(segment::index_error)?;
        let (want_plain, want_blocks) = (
            seg.files.contains_key(&format!("annfrag:{attr}")),
            seg.files.contains_key(&format!("annfrag2:{attr}")),
        );
        let (blob, plain, blocks) = tokio::task::spawn_blocking(move || {
            let plain = want_plain
                .then(|| bf_vector::Fragment::build(&blob))
                .transpose()?;
            let blocks = want_blocks
                .then(|| bf_vector::Fragment::build_blocks(&blob))
                .transpose()?;
            Ok::<_, bf_vector::Error>((blob, plain, blocks))
        })
        .await
        .map_err(segment::index_error)?
        .map_err(segment::index_error)?;
        let mut bytes = blob.len() as u64;
        let mut old = vec![file.clone()];
        let mut new = vec![(
            format!("ann:{attr}"),
            segment::put_file(
                store,
                format!("{prefix}/ann.{}.bfv", uuid::Uuid::new_v4()),
                blob,
                meta,
            )
            .await?,
        )];
        for (key, ext, built) in [
            (format!("annfrag:{attr}"), "bff", plain),
            (format!("annfrag2:{attr}"), "bfb", blocks),
        ] {
            let Some((data, meta)) = built else { continue };
            bytes += data.len() as u64;
            old.extend(seg.files.get(&key).cloned());
            let object = format!("{prefix}/ann.{}.{ext}", uuid::Uuid::new_v4());
            new.push((key, segment::put_file(store, object, data, meta).await?));
        }
        Ok(Converted {
            id: seg.id.clone(),
            attr: attr.to_string(),
            old_ann: file.key.clone(),
            new,
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
        let store = &self.0.store;
        let id = slab.id.as_str();
        let blob = self
            .convert_object(file)
            .await?
            .ok_or_else(|| ApiError::new(500, "index needs no conversion"))?;
        let meta = bf_vector::metadata_len(&blob).map_err(segment::index_error)?;
        let (frag, frag_meta) =
            bf_vector::Fragment::build_blocks(&blob).map_err(segment::index_error)?;
        let prefix = format!("ns/{ns}/slabs/{id}");
        let bytes = (blob.len() + frag.len()) as u64;
        let ann = segment::put_file(
            store,
            format!("{prefix}/ann.{}.bfv", uuid::Uuid::new_v4()),
            blob,
            meta,
        )
        .await?;
        let frag = segment::put_file(
            store,
            format!("{prefix}/ann2.{}.bfb", uuid::Uuid::new_v4()),
            frag,
            frag_meta,
        )
        .await?;
        let old = slab.files.values().cloned().collect();
        Ok(Converted {
            id: id.to_string(),
            attr: attr.to_string(),
            old_ann: file.key.clone(),
            new: vec![
                (format!("ann:{attr}"), ann),
                (format!("annfrag2:{attr}"), frag),
            ],
            old,
            bytes,
        })
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
                    for c in batch.converted().filter(|c| superseded.contains(&c.id)) {
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
    ) -> Result<(usize, BTreeSet<String>), ApiError> {
        let store = &self.0.store;
        let Some(set) = manifest.ann_slabs.get(attr).cloned() else {
            return Ok((0, list.iter().map(|c| c.id.clone()).collect()));
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
                    for (key, file) in &c.new {
                        slab.files.insert(key.clone(), file.clone());
                    }
                    retired.extend(c.old.iter().map(|f| f.key.clone()));
                    applied += 1;
                }
                _ => {
                    superseded.insert(c.id.clone());
                }
            }
        }
        if applied == 0 {
            return Ok((0, superseded));
        }
        // The directory over the slabs, as a recluster writes it.
        let dir = ann_dir::build(store, ns, &slabs, attr, ann_dir::Layout::Slabs, None)
            .await?
            .ok_or_else(|| ApiError::new(500, "slab directory not built"))?;
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
        .filter_map(|k| k.strip_prefix("ann:").map(str::to_string))
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
) -> Result<(usize, BTreeSet<String>), ApiError> {
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
            superseded.insert(c.id.clone());
            continue;
        };
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
        .filter_map(|k| k.strip_prefix("ann:").map(str::to_string))
        .collect();
    let previous_dirs = std::mem::take(&mut manifest.ann_dirs);
    for attr in attributes {
        let layouts = [ann_dir::Layout::Plain, ann_dir::Layout::Blocks];
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
