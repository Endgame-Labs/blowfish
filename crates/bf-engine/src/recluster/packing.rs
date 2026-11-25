//! RR6: spatially pack independently trained source leaves for the first tier.
use super::*;

impl Engine {
    pub(super) async fn pack_first_tier(
        &self,
        ns: &str,
        target: &Target,
        opts: &ReclusterOptions,
        covered: Vec<SlabCover>,
        job: &Job,
    ) -> Result<Built, ApiError> {
        let start = std::time::Instant::now();
        let store = &self.0.store;
        let attr = &opts.attribute;
        let key = format!("annv2:{attr}");
        let _opening = Stage::new("pack_open");
        let indexes: Vec<_> =
            futures::stream::iter(target.segments.clone().into_iter().map(|seg: Segment| {
                let key = key.clone();
                async move {
                    let index = segment_index(store, &seg, &key).await?;
                    let live = if seg.live_generation == 0 {
                        None
                    } else {
                        Some(segment::live(store, &seg).await?)
                    };
                    Ok::<_, ApiError>((index, live))
                }
            }))
            .buffered(6)
            .try_collect()
            .await?;
        drop(_opening);
        let mut leaves = Vec::new();
        let mut centers = Vec::new();
        for (s, (index, _)) in indexes.iter().enumerate() {
            for (cid, center) in index
                .packing_centers()
                .map_err(segment::index_error)?
                .into_iter()
                .enumerate()
            {
                if index.cluster_rows(cid) > 0 {
                    leaves.push((s, cid, index.cluster_rows(cid)));
                    centers.push(center);
                }
            }
        }
        if centers.is_empty() {
            return Err(ApiError::new(400, "no source leaves"));
        }
        let cells = opts.cells.max(1).min(centers.len());
        let max = opts.max_slab_rows.max(1);
        let (coarse, plans) = on_pool(move || {
            let _planning = Stage::new("pack_plan");
            let refs: Vec<_> = centers.iter().map(Vec::as_slice).collect();
            let coarse = math::kmeans(&refs, cells, 8, 7);
            let packed = math::Centers::new(coarse.iter().map(Vec::as_slice));
            let assignment = packed.nearest(&refs);
            let mut groups = vec![Vec::new(); cells];
            for (leaf, c) in leaves.into_iter().zip(assignment) {
                groups[c].push(leaf);
            }
            let mut plans = Vec::new();
            for group in groups {
                let mut rows = 0;
                let mut slab = Vec::new();
                for leaf in group {
                    if rows + leaf.2 > max && !slab.is_empty() {
                        plans.push(std::mem::take(&mut slab));
                        rows = 0;
                    }
                    rows += leaf.2;
                    slab.push(leaf);
                }
                if !slab.is_empty() {
                    plans.push(slab);
                }
            }
            (coarse, plans)
        })
        .await?;
        let mut coarse_bytes = Vec::new();
        for x in coarse.iter().flatten() {
            coarse_bytes.extend(x.to_le_bytes());
        }
        let coarse_file = segment::put_file(
            store,
            format!("ns/{ns}/slabs/{}coarse.bin", job.slab_prefix()),
            coarse_bytes,
            0,
        )
        .await?;
        let mut slabs = Vec::new();
        let mut rows = 0;
        let mut clusters = 0;
        for (n, plan) in plans.into_iter().enumerate() {
            job.pause().await;
            if self.0.stop.stopping() {
                return Err(shutting_down());
            }
            let _reading = Stage::new("pack_read");
            let leaves: Vec<_> = futures::stream::iter(plan.into_iter().map(|(s, cid, _)| {
                let (index, live) = &indexes[s];
                let base = covered[s].base;
                async move {
                    index
                        .packing_leaf(cid, base, live.as_ref())
                        .await
                        .map_err(segment::index_error)
                }
            }))
            .buffered(32)
            .try_collect()
            .await?;
            drop(_reading);
            let rows_n = leaves.iter().map(|l| l.len() as u32).sum::<u32>();
            if rows_n == 0 {
                continue;
            }
            clusters += leaves.iter().filter(|l| !l.is_empty()).count();
            rows += u64::from(rows_n);
            let (blob, meta) = on_pool(move || {
                let _encoding = Stage::new("pack_encode");
                bf_vector::pack_leaves(leaves).map_err(segment::index_error)
            })
            .await??;
            let id = format!("{}{n:05}", job.slab_prefix());
            let _upload = Stage::new("pack_upload");
            let ann = segment::put_file(
                store,
                format!("ns/{ns}/slabs/{id}/ann.{}.bfa", uuid::Uuid::new_v4()),
                blob,
                meta,
            )
            .await?;
            slabs.push(Segment {
                id,
                wal_start: 0,
                wal_end: 0,
                row_count: rows_n,
                schema: Default::default(),
                files: std::collections::BTreeMap::from([(format!("annv2:{attr}"), ann)]),
                live: bf_wal::SegmentFile {
                    key: String::new(),
                    size: 0,
                    metadata_len: 0,
                    stats_index: None,
                    text_header: None,
                },
                live_generation: 0,
            });
        }
        let (dir, pages) = slab_directories(store, ns, attr, &slabs, true).await?;
        let report = ReclusterReport {
            rows,
            cluster_rows: 0,
            slabs: slabs.len(),
            clusters,
            directory_bytes: dir.size,
            seconds: start.elapsed().as_secs_f64(),
        };
        Ok(Built {
            set: SlabSet {
                covered,
                slabs,
                dir,
                pages,
                format: bf_wal::TIER_FORMAT_VERSION,
                coarse: Some(coarse_file),
                coarse_rows: rows,
            },
            report,
        })
    }
}
