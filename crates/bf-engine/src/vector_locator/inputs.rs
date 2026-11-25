//! Streaming authoritative vector sources for a recluster rebuild.
use super::*;
use bf_wal::Segment;

/// One authoritative ANN object consumed by a rebuild. Its stored rows are translated into
/// the new build's global numbering, omitting deleted rows and segments outside the target.
pub(crate) struct Input {
    pub file: bf_wal::SegmentFile,
    address: Address,
}
#[derive(Clone)]
struct TargetRows {
    base: u32,
    rows: u32,
    live: Option<Arc<RoaringBitmap>>,
}
enum Address {
    Segment(TargetRows),
    Tier {
        set: Arc<SlabSet>,
        placement: Arc<lineage::Placement>,
        targets: Arc<Vec<Option<TargetRows>>>,
    },
}
impl TargetRows {
    fn global(&self, row: u32) -> Result<Option<u32>, ApiError> {
        if row >= self.rows {
            return Err(segment::corrupt());
        }
        if self.live.as_ref().is_some_and(|l| !l.contains(row)) {
            return Ok(None);
        }
        self.base
            .checked_add(row)
            .map(Some)
            .ok_or_else(segment::corrupt)
    }
}
impl Input {
    pub fn global(&self, row: u32) -> Result<Option<u32>, ApiError> {
        match &self.address {
            Address::Segment(target) => target.global(row),
            Address::Tier {
                set,
                placement,
                targets,
            } => {
                let p = set
                    .covered
                    .partition_point(|c| c.base <= row)
                    .checked_sub(1)
                    .ok_or_else(segment::corrupt)?;
                if row - set.covered[p].base >= set.covered[p].rows {
                    return Err(segment::corrupt());
                }
                let Some((at, local)) = placement.real(&set.covered, row) else {
                    return Ok(None);
                };
                match targets.get(at).and_then(Option::as_ref) {
                    Some(target) => target.global(local),
                    None => Ok(None),
                }
            }
        }
    }
}

/// Plan one streaming read of each predecessor slab, plus uncovered segment indexes. A base
/// rebuild never falls back to an already retired segment object or reads a tier once per segment.
pub(crate) async fn inputs(
    store: &Storage,
    manifest: &bf_wal::Manifest,
    selected: &[Segment],
    attr: &str,
) -> Result<Vec<Input>, ApiError> {
    let source = Sources::from_manifest(manifest);
    let mut targets = vec![None; manifest.segments.len()];
    let mut base = 0u64;
    let mut out = Vec::new();
    for seg in selected {
        let at = manifest
            .segments
            .iter()
            .position(|s| s.id == seg.id && s.row_count == seg.row_count)
            .ok_or_else(segment::corrupt)?;
        let target = TargetRows {
            base: u32::try_from(base)
                .map_err(|_| segment::index_error("too many recluster rows"))?,
            rows: seg.row_count,
            live: if seg.live_generation == 0 {
                None
            } else {
                Some(Arc::new(segment::live(store, seg).await?))
            },
        };
        base += u64::from(seg.row_count);
        if source.covers(seg, attr) {
            targets[at] = Some(target);
        } else {
            let file = crate::upgrade::files(seg, attr)
                .ok_or_else(|| segment::index_error("recluster source has no vector owner"))?
                .0
                .clone();
            out.push(Input {
                file,
                address: Address::Segment(target),
            });
        }
    }
    if targets.iter().all(Option::is_none) {
        return Ok(out);
    }
    let targets = Arc::new(targets);
    if let Some(sets) = manifest.ann_tiers.get(attr) {
        for set in sets {
            let Some(placement) =
                lineage::place(store, &manifest.segments, &manifest.lineage, set).await?
            else {
                return Err(segment::index_error("unreachable recluster vector tier"));
            };
            if !placement.positions().any(|i| targets[i].is_some()) {
                continue;
            }
            let placement = Arc::new(placement);
            let set = Arc::new(set.clone());
            for slab in &set.slabs {
                let file = crate::upgrade::files(slab, attr)
                    .ok_or_else(segment::corrupt)?
                    .0
                    .clone();
                out.push(Input {
                    file,
                    address: Address::Tier {
                        set: set.clone(),
                        placement: placement.clone(),
                        targets: targets.clone(),
                    },
                });
            }
        }
    }
    Ok(out)
}
