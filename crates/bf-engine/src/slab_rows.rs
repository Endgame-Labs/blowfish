//! Resident global row and cluster membership, without vector or code payloads.
use crate::{read_cache, segment};
use bf_storage::Storage;
use bf_types::ApiError;
use bf_vector::Membership;
use bf_wal::{Manifest, Segment, SegmentFile};
use std::sync::Arc;

pub(crate) fn role(attr: &str) -> String {
    format!("annrows:{attr}")
}
fn key(ann: &SegmentFile) -> String {
    format!("{}.rows-v1.bfr", ann.key)
}
fn current<'a>(slab: &'a Segment, attr: &str) -> Option<&'a SegmentFile> {
    let ann = slab.files.get(&format!("annv2:{attr}"))?;
    slab.files
        .get(&role(attr))
        .filter(|file| file.key == key(ann))
}

/// Load the immutable resident map tied to this exact ANN generation.
pub(crate) async fn load(
    store: &Storage,
    slab: &Segment,
    attr: &str,
) -> Result<Option<Arc<Membership>>, ApiError> {
    let Some(file) = current(slab, attr) else {
        return Ok(None);
    };
    if let Some(member) = read_cache::get(&file.key, file.size) {
        return Ok(Some(member));
    }
    let bytes = segment::read_whole(store, file).await?;
    let member = Membership::decode_sparse(&bytes).map_err(segment::index_error)?;
    if member
        .row_ids()
        .is_none_or(|r| r.len() != slab.row_count as usize)
    {
        return Err(segment::corrupt());
    }
    Ok(Some(read_cache::insert(
        &file.key,
        member.resident_bytes(),
        member,
    )))
}

/// A map is deterministic for its immutable ANN object; concurrent builders write the same bytes.
pub(crate) async fn write(
    store: &Storage,
    ann: &SegmentFile,
    member: &Membership,
) -> Result<SegmentFile, ApiError> {
    let bytes = member.encode_sparse().map_err(segment::index_error)?;
    let len = bytes.len() as u64;
    let file = SegmentFile {
        key: key(ann),
        size: len,
        metadata_len: len,
        stats_index: None,
        text_header: None,
    };
    let bytes = bytes::Bytes::from(bytes);
    if store
        .create_unique(&file.key, bytes.clone())
        .await
        .map_err(crate::storage_error)?
        == bf_storage::CreateResult::Conflict
        && store.get(&file.key).await.map_err(crate::storage_error)? != bytes
    {
        return Err(segment::index_error("conflicting resident slab row map"));
    }
    Ok(file)
}

/// Current containers whose sparse resident metadata has not yet been backfilled.
pub(crate) fn needs(tiers: &std::collections::BTreeMap<String, Vec<bf_wal::SlabSet>>) -> bool {
    tiers.iter().any(|(attr, sets)| {
        sets.iter()
            .flat_map(|s| &s.slabs)
            .any(|s| s.files.contains_key(&format!("annv2:{attr}")) && current(s, attr).is_none())
    })
}

/// Backfill a bounded number of old slabs, preserving every center, code, and exact vector.
/// The caller publishes descriptors and retirement records together under its manifest fence.
pub(crate) async fn ensure(
    store: &Storage,
    manifest: &mut Manifest,
    limit: usize,
) -> Result<usize, ApiError> {
    let mut count = 0;
    let mut retired = Vec::new();
    'sets: for (attr, sets) in &mut manifest.ann_tiers {
        for slab in sets.iter_mut().flat_map(|s| &mut s.slabs) {
            if count >= limit {
                break 'sets;
            }
            let Some(ann) = slab.files.get(&format!("annv2:{attr}")) else {
                continue;
            };
            if current(slab, attr).is_some() {
                continue;
            }
            let index = bf_vector::Index::open_with_metadata_len(
                segment::ObjectReader::new(store, ann).bypass_cache(),
                ann.metadata_len,
            )
            .await
            .map_err(segment::index_error)?;
            let file = write(
                store,
                ann,
                &index.membership().map_err(segment::index_error)?,
            )
            .await?;
            if let Some(old) = slab.files.insert(role(attr), file) {
                retired.push(old.key);
            }
            count += 1;
        }
    }
    for old in retired {
        crate::indexer::retire(manifest, old);
    }
    Ok(count)
}
