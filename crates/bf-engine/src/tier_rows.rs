//! One immutable global-row location object per tier, published with its manifest.
use crate::{ann_dir, read_cache, segment};
use bf_storage::{Error, Storage};
use bf_types::ApiError;
use bf_vector::{Directory, RoaringBitmap, RowLocations};
use bf_wal::{Manifest, SegmentFile, SlabSet};
use sha2::{Digest, Sha256};
use std::sync::Arc;

fn role(attr: &str) -> String {
    format!("annloc:{attr}")
}
fn key(ns: &str, set: &SlabSet) -> Result<String, ApiError> {
    let (_, relative) = set
        .dir
        .key
        .strip_prefix("ns/")
        .and_then(|s| s.split_once('/'))
        .ok_or_else(segment::corrupt)?;
    let mut hash = Sha256::new();
    for slab in &set.slabs {
        for (role, file) in &slab.files {
            if role.starts_with("ann:") || role.starts_with("annv2:") || role.starts_with("annfrag")
            {
                hash.update((file.key.len() as u64).to_le_bytes());
                hash.update(file.key.as_bytes());
                hash.update(file.size.to_le_bytes());
            }
        }
        hash.update(slab.row_count.to_le_bytes());
    }
    let generation: String = hash.finalize()[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok(format!("ns/{ns}/{relative}.locations-v1-{generation}.bfl"))
}
fn file<'a>(set: &'a SlabSet, attr: &str) -> Option<&'a SegmentFile> {
    let file = set.slabs.first()?.files.get(&role(attr))?;
    let (ns, _) = file.key.strip_prefix("ns/")?.split_once('/')?;
    (key(ns, set).ok()? == file.key).then_some(file)
}
pub(crate) fn has(set: &SlabSet, attr: &str) -> bool {
    file(set, attr).is_some()
}
fn eligible(set: &SlabSet) -> bool {
    !set.slabs.is_empty()
        && set
            .covered
            .iter()
            .map(|c| u64::from(c.base) + u64::from(c.rows))
            .max()
            .unwrap_or(0)
            <= set.rows().saturating_mul(2)
}
pub(crate) fn needs(ns: &str, tiers: &std::collections::BTreeMap<String, Vec<SlabSet>>) -> bool {
    tiers.iter().any(|(attr, sets)| {
        sets.iter().any(|set| {
            eligible(set)
                && file(set, attr).is_none_or(|file| key(ns, set).is_ok_and(|key| key != file.key))
        })
    })
}

/// Create before publication. A failed manifest CAS leaves only an unreferenced object.
pub(crate) async fn build(
    store: &Storage,
    ns: &str,
    attr: &str,
    set: &mut SlabSet,
) -> Result<bool, ApiError> {
    if !eligible(set) {
        return Ok(false);
    }
    let members = futures::future::try_join_all(
        set.slabs
            .iter()
            .map(|s| crate::slab_rows::load(store, s, attr)),
    )
    .await?;
    let bytes = tokio::task::spawn_blocking(move || {
        let members: Vec<_> = members
            .iter()
            .map(|m| m.as_deref().ok_or_else(segment::corrupt))
            .collect::<Result<_, _>>()?;
        RowLocations::encode(&members).map_err(segment::index_error)
    })
    .await
    .map_err(segment::index_error)??;
    let file = SegmentFile {
        key: key(ns, set)?,
        size: bytes.len() as u64,
        metadata_len: 32,
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
        return Err(segment::index_error("conflicting tier row locations"));
    }
    set.slabs[0].files.insert(role(attr), file);
    Ok(true)
}

pub(crate) async fn ensure(
    store: &Storage,
    ns: &str,
    manifest: &mut Manifest,
    missing: Option<&str>,
) -> Result<(), ApiError> {
    let mut retired = None;
    'tiers: for (attr, sets) in &mut manifest.ann_tiers {
        for set in sets {
            if !eligible(set) {
                continue;
            }
            if file(set, attr).is_some_and(|f| {
                missing != Some(f.key.as_str()) && key(ns, set).is_ok_and(|key| key == f.key)
            }) {
                continue;
            }
            let old = set
                .slabs
                .first()
                .and_then(|s| s.files.get(&role(attr)))
                .map(|f| f.key.clone());
            if build(store, ns, attr, set).await? {
                retired = old.filter(|old| file(set, attr).is_some_and(|f| f.key != *old));
            }
            break 'tiers;
        }
    }
    if let Some(old) = retired {
        crate::indexer::retire(manifest, old);
    }
    Ok(())
}
pub(crate) async fn missing(
    store: &Storage,
    tiers: &std::collections::BTreeMap<String, Vec<SlabSet>>,
    probe: usize,
) -> Result<Option<String>, ApiError> {
    let files: Vec<_> = tiers
        .iter()
        .flat_map(|(a, sets)| sets.iter().filter_map(|s| file(s, a)))
        .collect();
    if files.is_empty() {
        return Ok(None);
    }
    let file = files[probe % files.len()];
    match store.head(&file.key).await {
        Ok(_) => Ok(None),
        Err(Error::NotFound) => Ok(Some(file.key.clone())),
        Err(e) => Err(crate::storage_error(e)),
    }
}

async fn header(
    store: &Storage,
    file: &SegmentFile,
) -> Result<Option<Arc<RowLocations>>, ApiError> {
    if let Some(h) = read_cache::get(&file.key, 32) {
        return Ok(Some(h));
    }
    let reader = store
        .range_reader_with_length(&file.key, file.size, true)
        .map_err(crate::storage_error)?
        .sparse(8192);
    let bytes = match reader.read_with_stats(0..32, &read_cache::stats()).await {
        Ok(bytes) => bytes,
        Err(Error::NotFound) => return Ok(None),
        Err(e) => return Err(crate::storage_error(e)),
    };
    let h = RowLocations::decode(&bytes, file.size).map_err(segment::index_error)?;
    Ok(Some(read_cache::insert(&file.key, 32, h)))
}

async fn pages(
    store: &Storage,
    file: &SegmentFile,
    header: &RowLocations,
    allowed: &RoaringBitmap,
) -> Result<Option<Vec<(u64, bytes::Bytes)>>, ApiError> {
    let pages = header.ranges(allowed);
    let ranges: Vec<_> = pages.iter().map(|(_, r)| r.clone()).collect();
    let mut reader = store
        .range_reader_with_length(&file.key, file.size, true)
        .map_err(crate::storage_error)?;
    if ranges.iter().map(|r| r.end - r.start).sum::<u64>() <= 1 << 20 {
        reader = reader.sparse(8192);
    }
    let bytes = match reader
        .read_batch_with_stats(&ranges, &read_cache::stats())
        .await
    {
        Ok(bytes) => bytes,
        Err(Error::NotFound) => return Ok(None),
        Err(e) => return Err(crate::storage_error(e)),
    };
    Ok(Some(
        pages.into_iter().map(|(id, _)| id).zip(bytes).collect(),
    ))
}

/// Prefetch partial predicates as their source segments finish. No row decoding is needed.
pub(crate) async fn warm(
    store: &Storage,
    set: &SlabSet,
    attr: &str,
    allowed: &RoaringBitmap,
) -> Result<bool, ApiError> {
    let Some(file) = file(set, attr) else {
        return Ok(false);
    };
    if allowed.is_empty() {
        return Ok(true);
    }
    if store.cache_limits().0 == 0 {
        return Ok(true);
    }
    let Some(h) = header(store, file).await? else {
        return Ok(false);
    };
    // Once completed predicates already need a quarter of the table, overlap one
    // coalesced read with the remaining predicates. Bound speculative metadata bytes
    // by four times known aligned demand, and only do this when the raw cache can retain it.
    let reader = store
        .range_reader_with_length(&file.key, file.size, true)
        .map_err(crate::storage_error)?;
    let ranges = h.ranges(allowed);
    let (mut blocks, mut previous) = (0u64, 0u64);
    for (_, r) in &ranges {
        let first = r.start / reader.block_size();
        let end = r.end.div_ceil(reader.block_size());
        blocks += end.saturating_sub(first.max(previous));
        previous = previous.max(end);
    }
    let needed = blocks.saturating_mul(reader.block_size()).min(file.size);
    if needed.saturating_mul(4) >= file.size && store.cache_limits().0 >= file.size {
        if reader.is_memory_resident(0..file.size) {
            return Ok(true);
        }
        let ranges: Vec<_> = (0..file.size)
            .step_by(64 << 10)
            .map(|start| start..(start + (64 << 10)).min(file.size))
            .collect();
        return match reader
            .read_batch_with_stats(&ranges, &read_cache::stats())
            .await
        {
            Ok(_) => Ok(true),
            Err(Error::NotFound) => Ok(false),
            Err(e) => Err(crate::storage_error(e)),
        };
    }
    Ok(pages(store, file, &h, allowed).await?.is_some())
}

struct Projection {
    _predicate: Arc<RoaringBitmap>,
    members: read_cache::Memberships,
}
/// Completed predicate projections need no additional raw metadata warming.
pub(crate) fn cached(
    set: &SlabSet,
    attr: &str,
    allowed: &Arc<RoaringBitmap>,
) -> Option<read_cache::Memberships> {
    let file = file(set, attr)?;
    let key = format!("tier-locations:{}:{:p}", file.key, Arc::as_ptr(allowed));
    read_cache::get::<Projection>(&key, 0).map(|p| p.members.clone())
}
/// Read only selected global rows. Cached projections retain the predicate owning their key.
pub(crate) async fn load(
    store: &Storage,
    set: &SlabSet,
    attr: &str,
    dir: &Directory,
    allowed: &Arc<RoaringBitmap>,
) -> Result<Option<read_cache::Memberships>, ApiError> {
    if let Some(found) = cached(set, attr, allowed) {
        return Ok(Some(found));
    }
    let Some(members) = project(store, set, attr, dir, allowed).await? else {
        return Ok(None);
    };
    Ok(Some(seed(set, attr, allowed.clone(), members)))
}

pub(crate) async fn project(
    store: &Storage,
    set: &SlabSet,
    attr: &str,
    dir: &Directory,
    allowed: &Arc<RoaringBitmap>,
) -> Result<Option<read_cache::Memberships>, ApiError> {
    let Some(file) = file(set, attr) else {
        return Ok(None);
    };
    let started = std::time::Instant::now();
    let Some(h) = header(store, file).await? else {
        return Ok(None);
    };
    let Some(pages) = pages(store, file, &h, allowed).await? else {
        return Ok(None);
    };
    crate::ann::stage(started, "locations read");
    let counts: Vec<_> = dir.segments().iter().map(|s| s.clusters).collect();
    let rows: Vec<_> = set.slabs.iter().map(|s| u64::from(s.row_count)).collect();
    let global = allowed.clone();
    let members = ann_dir::on_worker(async move {
        h.project(&global, &pages, &counts, &rows)
            .map(|members| {
                members
                    .into_iter()
                    .map(|m| Some(Arc::new(m)))
                    .collect::<Vec<_>>()
            })
            .map_err(segment::index_error)
    })
    .await?;
    crate::ann::stage(started, "locations project");
    Ok(Some(members))
}

pub(crate) fn seed(
    set: &SlabSet,
    attr: &str,
    allowed: Arc<RoaringBitmap>,
    members: read_cache::Memberships,
) -> read_cache::Memberships {
    let file = file(set, attr).expect("projected tier locations");
    let key = format!("tier-locations:{}:{:p}", file.key, Arc::as_ptr(&allowed));
    let bytes = members.iter().flatten().map(|m| m.resident_bytes()).sum();
    let found = read_cache::insert(
        &key,
        bytes,
        Projection {
            _predicate: allowed,
            members,
        },
    );
    found.members.clone()
}

pub(crate) async fn merge(
    parts: Vec<read_cache::Memberships>,
) -> Result<read_cache::Memberships, ApiError> {
    if parts.len() == 1 {
        return Ok(parts.into_iter().next().unwrap());
    }
    if parts.is_empty() {
        return Err(segment::corrupt());
    }
    let parallel = crate::fan_out();
    ann_dir::on_worker(async move {
        let merge = |i: usize| {
            let members = parts
                .iter()
                .map(|p| p[i].as_deref().ok_or_else(segment::corrupt))
                .collect::<Result<Vec<_>, _>>()?;
            bf_vector::Membership::merge_locations(&members)
                .map(|m| Some(Arc::new(m)))
                .map_err(segment::index_error)
        };
        if parallel {
            use rayon::prelude::*;
            (0..parts[0].len()).into_par_iter().map(merge).collect()
        } else {
            (0..parts[0].len()).map(merge).collect()
        }
    })
    .await
}
