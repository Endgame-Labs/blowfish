//! One-pass, unaligned reads of cold filtered leaf runs. Every code block still goes
//! through the ordinary checksum, mask, scan, and exact-rerank validation.
use super::{BLOB_MERGE_MAX, Reader};
use crate::{read_cache, segment};
use bf_types::ApiError;
use bf_vector::{DirEntry, DirSegment};
use bytes::Bytes;
use std::{ops::Range, sync::Arc};

/// Bound response buffers through scoring, independently of the store's fetch limit.
pub(super) const IN_FLIGHT_BYTES: usize = 64 << 20;
const RUN_GAP: u64 = 16 << 10;

pub(super) async fn reserve(
    entries: &[&DirEntry],
    budget: Arc<tokio::sync::Semaphore>,
) -> Result<tokio::sync::OwnedSemaphorePermit, ApiError> {
    let range = extent(entries)?;
    budget
        .acquire_many_owned((range.end - range.start).min(IN_FLIGHT_BYTES as u64) as u32)
        .await
        .map_err(segment::index_error)
}

fn extent(entries: &[&DirEntry]) -> Result<Range<u64>, ApiError> {
    let start = entries.iter().map(|e| e.blob_range().start).min();
    let end = entries.iter().map(|e| e.codes_range().end).max();
    match start.zip(end) {
        Some((start, end)) if start <= end => Ok(start..end),
        _ => Err(segment::index_error("invalid direct leaf batch")),
    }
}

/// Current objects keep each blob immediately before its codes. Older split objects
/// retain the cache-backed reader, including its transitional metadata handling.
pub(super) fn supports(segment: &DirSegment) -> bool {
    !segment.flat && segment.codes_in_fragment && segment.key == segment.fragment
}

pub(super) fn batches(entries: &[DirEntry], chosen: &[usize]) -> Vec<Vec<usize>> {
    plan(
        chosen
            .iter()
            .map(|&n| {
                (
                    n,
                    entries[n].blob_range().start..entries[n].codes_range().end,
                )
            })
            .collect(),
    )
}

/// Keep nearby selected leaves together in file order, skipping large unselected gaps.
/// One unusually large leaf stays whole, as it does in the ordinary merged reader.
fn plan(mut leaves: Vec<(usize, Range<u64>)>) -> Vec<Vec<usize>> {
    leaves.sort_unstable_by_key(|(_, r)| r.start);
    let mut batches: Vec<Vec<usize>> = Vec::new();
    let mut range: Range<u64> = 0..0;
    for (id, next) in leaves {
        if !batches.is_empty()
            && next.start <= range.end.saturating_add(RUN_GAP)
            && next.end.max(range.end) - range.start <= BLOB_MERGE_MAX
        {
            range.end = range.end.max(next.end);
            batches.last_mut().expect("nonempty batches").push(id);
        } else {
            range = next;
            batches.push(vec![id]);
        }
    }
    batches
}

/// Consume one bounded run as soon as it arrives. Borrow selected payloads from its
/// response buffer, and discard gaps without aligning, copying, or seeding cache blocks.
pub(super) async fn read(
    entries: &[&DirEntry],
    reader: &Reader,
    cached: &[bool],
) -> Result<(Vec<Bytes>, Vec<Bytes>), ApiError> {
    if entries.is_empty() || entries.len() != cached.len() {
        return Err(segment::index_error("invalid direct leaf batch"));
    }
    let Range { start, end } = extent(entries)?;
    let source = reader
        .store
        .range_reader_with_length(&reader.file.key, reader.file.size, true)
        .map_err(segment::index_error)?;
    let bytes = source
        .read_direct_with_stats(start..end, &read_cache::stats())
        .await
        .map_err(segment::index_error)?;
    let slice = |range: Range<u64>| {
        let offset = range.start.checked_sub(start);
        let limit = range.end.checked_sub(start);
        match offset.zip(limit) {
            Some((offset, limit)) if offset <= limit && limit <= bytes.len() as u64 => {
                Ok(bytes.slice(offset as usize..limit as usize))
            }
            _ => Err(segment::index_error("leaf outside direct run")),
        }
    };
    let blobs = entries
        .iter()
        .zip(cached)
        .filter(|(_, cached)| !**cached)
        .map(|(entry, _)| slice(entry.blob_range()))
        .collect::<Result<_, _>>()?;
    let codes = entries
        .iter()
        .map(|entry| slice(entry.codes_range()))
        .collect::<Result<_, _>>()?;
    if crate::ann::trace_stages() {
        eprintln!(
            "DIRECT_SCAN clusters={} bytes={}",
            entries.len(),
            bytes.len()
        );
    }
    Ok((blobs, codes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_preserve_every_leaf_and_skip_large_gaps_without_alignment() {
        let far = 80_003 + RUN_GAP + 1;
        let leaves = vec![(3, far..far + 39_994), (1, 3..40_001), (2, 40_001..80_003)];
        assert_eq!(plan(leaves), vec![vec![1, 2], vec![3]]);
        assert!(plan(Vec::new()).is_empty());
        let large = BLOB_MERGE_MAX + 17;
        assert_eq!(
            plan(vec![(1, 7..large), (2, large..large + 19)]),
            vec![vec![1], vec![2]]
        );
        assert_eq!(
            plan(vec![(1, 3..7), (2, 7 + RUN_GAP..11 + RUN_GAP)]),
            vec![vec![1, 2]]
        );
    }

    #[test]
    fn split_and_flat_layouts_keep_the_ordinary_reader() {
        let mut segment = DirSegment {
            id: "slab".into(),
            key: "index".into(),
            fragment: "index".into(),
            flat: false,
            clusters: 10,
            rows: 1000,
            membership: None,
            codes_in_fragment: true,
            files: None,
        };
        assert!(supports(&segment));
        segment.fragment = "older-fragment".into();
        assert!(!supports(&segment));
        segment.fragment = segment.key.clone();
        segment.codes_in_fragment = false;
        assert!(!supports(&segment));
        segment.codes_in_fragment = true;
        segment.flat = true;
        assert!(!supports(&segment));
    }
}
