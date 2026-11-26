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
// Keep maps beside the immutable ANN generation, but in the writing namespace. Inherited
// ANN objects belong to a source namespace whose collector cannot see our publication.
fn key(ns: &str, ann: &SegmentFile) -> Result<String, ApiError> {
    let (_, relative) = ann
        .key
        .strip_prefix("ns/")
        .and_then(|s| s.split_once('/'))
        .ok_or_else(segment::corrupt)?;
    Ok(format!("ns/{ns}/{relative}.rows-v1.bfr"))
}
fn current<'a>(slab: &'a Segment, attr: &str) -> Option<&'a SegmentFile> {
    let ann = crate::upgrade::files(slab, attr)?.0;
    slab.files.get(&role(attr)).filter(|file| {
        file.key
            .strip_prefix("ns/")
            .and_then(|s| s.split_once('/'))
            .is_some_and(|(ns, _)| key(ns, ann).is_ok_and(|key| file.key == key))
    })
}

/// Load the immutable map for this ANN generation. Unpublished or collected maps are
/// reconstructed from ANN metadata until the next fenced backfill publishes them. Readers
/// never repair storage themselves: inherited sources can belong to another namespace.
pub(crate) async fn load(
    store: &Storage,
    slab: &Segment,
    attr: &str,
) -> Result<Option<Arc<Membership>>, ApiError> {
    let Some(file) = current(slab, attr) else {
        // Independent segments use their dense fragment membership; only slabs need global IDs.
        return if slab.files.contains_key("docs") {
            Ok(None)
        } else {
            rebuild(store, slab, attr, "unpublished").await.map(Some)
        };
    };
    if let Some(member) = read_cache::get(&file.key, file.size) {
        return Ok(Some(member));
    }
    let bytes = match segment::ObjectReader::new(store, file)
        .read(0..file.size)
        .await
    {
        Ok(bytes) => bytes,
        Err(error)
            if matches!(
                error
                    .get_ref()
                    .and_then(|e| e.downcast_ref::<bf_storage::Error>()),
                Some(bf_storage::Error::NotFound)
            ) =>
        {
            return rebuild(store, slab, attr, "missing").await.map(Some);
        }
        Err(error) => return Err(segment::index_error(error)),
    };
    let member = crate::ann_dir::on_worker(async move {
        Membership::decode_sparse(&bytes).map_err(segment::index_error)
    })
    .await?;
    validate(slab, &member)?;
    Ok(Some(read_cache::insert(
        &file.key,
        member.resident_bytes(),
        member,
    )))
}

fn validate(slab: &Segment, member: &Membership) -> Result<(), ApiError> {
    if member
        .row_ids()
        .is_none_or(|r| r.len() != slab.row_count as usize)
    {
        return Err(segment::corrupt());
    }
    Ok(())
}
async fn rebuild(
    store: &Storage,
    slab: &Segment,
    attr: &str,
    reason: &'static str,
) -> Result<Arc<Membership>, ApiError> {
    let ann = crate::upgrade::files(slab, attr)
        .ok_or_else(segment::corrupt)?
        .0;
    let cache_key = format!("rebuilt-slab-rows:{}", ann.key);
    if let Some(member) = read_cache::get(&cache_key, 0) {
        return Ok(member);
    }
    let index = bf_vector::Index::open_with_metadata_len(
        segment::ObjectReader::new(store, ann).bypass_cache(),
        ann.metadata_len,
    )
    .await
    .map_err(segment::index_error)?;
    let member = index.membership().map_err(segment::index_error)?;
    validate(slab, &member)?;
    metrics::counter!("blowfish_ann_row_map_rebuilds_total", "reason" => reason).increment(1);
    Ok(read_cache::insert(
        &cache_key,
        member.resident_bytes(),
        member,
    ))
}

/// Concurrent builders in one namespace write identical bytes for one immutable ANN object.
pub(crate) async fn write(
    store: &Storage,
    ns: &str,
    ann: &SegmentFile,
    member: &Membership,
) -> Result<SegmentFile, ApiError> {
    let bytes = member.encode_sparse().map_err(segment::index_error)?;
    let len = bytes.len() as u64;
    let file = SegmentFile {
        key: key(ns, ann)?,
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

/// Every supported slab format backfills into the same sparse resident metadata.
pub(crate) fn needs(tiers: &std::collections::BTreeMap<String, Vec<bf_wal::SlabSet>>) -> bool {
    tiers.iter().any(|(attr, sets)| {
        sets.iter()
            .flat_map(|s| &s.slabs)
            .any(|s| crate::upgrade::files(s, attr).is_some() && current(s, attr).is_none())
    })
}

/// Check one rotating map per refreshed index pass, outside the namespace lock. Queries
/// reconstruct missing maps immediately; this bounded survey schedules their durable repair.
pub(crate) async fn missing(
    store: &Storage,
    tiers: &std::collections::BTreeMap<String, Vec<bf_wal::SlabSet>>,
    probe: usize,
) -> Result<Option<String>, ApiError> {
    let files: Vec<_> = tiers
        .iter()
        .flat_map(|(attr, sets)| {
            sets.iter()
                .flat_map(|s| &s.slabs)
                .filter_map(move |s| current(s, attr))
        })
        .collect();
    if files.is_empty() {
        return Ok(None);
    }
    let file = files[probe % files.len()];
    match store.head(&file.key).await {
        Ok(_) => Ok(None),
        Err(bf_storage::Error::NotFound) => Ok(Some(file.key.clone())),
        Err(error) => Err(crate::storage_error(error)),
    }
}

/// Backfill a bounded number of slabs, preserving every center, code, and exact vector.
/// The caller publishes descriptors and retirement records together under its manifest fence.
pub(crate) async fn ensure(
    store: &Storage,
    ns: &str,
    manifest: &mut Manifest,
    limit: usize,
    missing: Option<&str>,
) -> Result<usize, ApiError> {
    let mut count = 0;
    let mut retired = Vec::new();
    'sets: for (attr, sets) in &mut manifest.ann_tiers {
        for slab in sets.iter_mut().flat_map(|s| &mut s.slabs) {
            if count >= limit {
                break 'sets;
            }
            let Some((ann, _)) = crate::upgrade::files(slab, attr) else {
                continue;
            };
            if let Some(file) = current(slab, attr)
                && missing != Some(file.key.as_str())
            {
                continue;
            }
            let member = rebuild(store, slab, attr, "backfill").await?;
            let file = write(store, ns, ann, &member).await?;
            if let Some(old) = slab.files.insert(role(attr), file)
                && old.key != slab.files[&role(attr)].key
            {
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

/// Retirement requires current sparse maps on every authoritative slab.
pub(crate) fn ready(sets: &[bf_wal::SlabSet], attr: &str) -> bool {
    !sets.is_empty()
        && sets.iter().all(|set| {
            !set.slabs.is_empty() && set.slabs.iter().all(|s| current(s, attr).is_some())
        })
}
