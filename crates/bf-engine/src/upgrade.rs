//! Transitional ANN readers, used only until the automatic object rewrite finishes.
use crate::{read_cache, segment};
use bf_storage::Storage;
use bf_types::ApiError;
use bf_vector::{Directory, Fragment, Membership, PartialIndex};
use bf_wal::{Segment, SegmentFile};
use std::sync::Arc;

/// Resolve the immutable index and fragment while a namespace contains older objects.
pub(crate) fn files<'a>(
    seg: &'a Segment,
    attr: &str,
) -> Option<(&'a SegmentFile, &'a SegmentFile)> {
    if let Some(file) = seg.files.get(&format!("annv2:{attr}")) {
        return Some((file, file));
    }
    let index = seg.files.get(&format!("ann:{attr}"))?;
    let frag = seg
        .files
        .get(&format!("annfrag2:{attr}"))
        .or_else(|| seg.files.get(&format!("annfrag:{attr}")))
        .unwrap_or(index);
    Some((index, frag))
}

pub(crate) fn covers(d: &bf_vector::DirSegment, seg: &Segment, attr: &str) -> bool {
    let Some((index, _)) = files(seg, attr) else {
        return false;
    };
    d.id == seg.id
        && d.key == index.key
        && ["annv2:", "annfrag2:", "annfrag:", "ann:"].iter().any(|p| {
            seg.files
                .get(&format!("{p}{attr}"))
                .is_some_and(|f| f.key == d.fragment)
        })
}

pub(crate) async fn fragment(
    store: &Storage,
    seg: &Segment,
    attr: &str,
) -> Result<(String, Fragment), ApiError> {
    let (index_file, file) =
        files(seg, attr).ok_or_else(|| segment::index_error("missing ANN source"))?;
    if index_file.key != file.key || seg.files.contains_key(&format!("annv2:{attr}")) {
        let bytes = segment::ObjectReader::new(store, file)
            .read(0..file.metadata_len)
            .await
            .map_err(segment::index_error)?;
        match bf_vector::upgrade::fragment(&bytes) {
            Ok(fragment) => return Ok((file.key.clone(), fragment)),
            Err(error) if !seg.files.contains_key(&format!("ann:{attr}")) => {
                return Err(segment::index_error(error));
            }
            // Earlier fragment versions are redundant: the original index still has every
            // center, row set, and checksum needed by the current directory planner.
            Err(_) => {}
        }
    }
    let index = segment::vector_index(store, index_file).await?;
    let fragment =
        bf_vector::upgrade::fragment_from_index(&*index).map_err(segment::index_error)?;
    Ok((index_file.key.clone(), fragment))
}

/// Assemble resident metadata before the first background publication, without writing storage.
pub(crate) async fn directory(
    store: &Storage,
    segments: &[Segment],
    attr: &str,
) -> Result<Arc<Directory>, ApiError> {
    let sources = segments
        .iter()
        .map(|s| files(s, attr))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| segment::index_error("missing ANN source"))?;
    let identity = serde_json::to_string(
        &sources
            .iter()
            .zip(segments)
            .map(|((a, b), s)| (&s.id, &a.key, &b.key))
            .collect::<Vec<_>>(),
    )
    .map_err(segment::index_error)?;
    let key = format!("upgrade-directory:{identity}");
    if let Some(dir) = read_cache::get(&key, 0) {
        return Ok(dir);
    }
    let fragments =
        futures::future::try_join_all(segments.iter().map(|s| fragment(store, s, attr))).await?;
    let sources = segments
        .iter()
        .zip(sources)
        .zip(fragments)
        .map(|((s, (a, _)), (fragment_key, f))| (s.id.clone(), a.key.clone(), fragment_key, f))
        .collect::<Vec<_>>();
    let bytes = Directory::build(&sources)
        .map_err(segment::index_error)?
        .ok_or_else(|| segment::index_error("inconsistent ANN vector spaces"))?;
    let dir = Directory::decode(&bytes).map_err(segment::index_error)?;
    Ok(read_cache::insert(&key, dir.resident_bytes(), dir))
}

pub(crate) async fn partial(
    store: &Storage,
    dir: &Directory,
    i: usize,
    file: &SegmentFile,
) -> Result<PartialIndex<segment::ObjectReader>, ApiError> {
    let reader = segment::ObjectReader::new(store, file);
    if file.key == dir.segments()[i].fragment && dir.segments()[i].codes_in_fragment {
        return dir.partial_index(i, reader).map_err(segment::index_error);
    }
    let header = reader.read(0..48).await.map_err(segment::index_error)?;
    bf_vector::upgrade::partial_index(dir, i, reader, &header).map_err(segment::index_error)
}

pub(crate) async fn membership(
    store: &Storage,
    seg: &Segment,
    attr: &str,
) -> Result<Option<Arc<Membership>>, ApiError> {
    let Some(index_file) = seg.files.get(&format!("ann:{attr}")) else {
        return Ok(None);
    };
    let file = seg.files.get(&format!("annfrag:{attr}"));
    let key = format!("upgrade-member:{}", file.unwrap_or(index_file).key);
    if let Some(m) = read_cache::get(&key, 0) {
        return Ok(Some(m));
    }
    if let Some(file) = file {
        let bytes = segment::ObjectReader::new(store, file)
            .read(0..file.metadata_len)
            .await
            .map_err(segment::index_error)?;
        if let Ok(fragment) = bf_vector::upgrade::fragment(&bytes) {
            let bytes =
                Directory::build(&[(seg.id.clone(), file.key.clone(), file.key.clone(), fragment)])
                    .map_err(segment::index_error)?
                    .ok_or_else(|| segment::index_error("missing ANN membership"))?;
            let dir = Directory::decode(&bytes).map_err(segment::index_error)?;
            let d = &dir.segments()[0];
            if let Some(range) = d.membership.clone() {
                let bytes = segment::ObjectReader::new(store, file)
                    .read(range)
                    .await
                    .map_err(segment::index_error)?;
                let m =
                    Membership::decode(&bytes, d.rows, d.clusters).map_err(segment::index_error)?;
                return Ok(Some(read_cache::insert(&key, m.resident_bytes(), m)));
            }
        }
    }
    let index = segment::vector_index(store, index_file).await?;
    Ok(bf_vector::upgrade::membership(&*index)
        .map(|m| read_cache::insert(&key, m.resident_bytes(), m)))
}

/// Old plain fragments keep their codes in the index. Current objects and old block fragments
/// use the same merged payload reads in the directory planner.
pub(crate) async fn read_clusters(
    entries: &[&bf_vector::DirEntry],
    exact: bool,
    fragment: &segment::ObjectReader,
    partial: &PartialIndex<segment::ObjectReader>,
    cached: Option<&[bool]>,
) -> Result<(Vec<bytes::Bytes>, Vec<bytes::Bytes>), ApiError> {
    if entries.iter().all(|e| e.blob_range().is_empty()) {
        let index = segment::vector_index(&fragment.store, &fragment.file).await?;
        let wanted = entries.iter().map(|e| e.cluster).collect::<Vec<_>>();
        let blobs = bf_vector::upgrade::cluster_blobs(
            &*index,
            &entries
                .iter()
                .enumerate()
                .filter(|(i, _)| !cached.is_some_and(|c| c[*i]))
                .map(|(_, e)| e.cluster)
                .collect::<Vec<_>>(),
        )
        .map_err(segment::index_error)?;
        let codes = if exact {
            Vec::new()
        } else {
            index
                .read_codes(&wanted)
                .await
                .map_err(segment::index_error)?
        };
        return Ok((blobs, codes));
    }
    let blobs = bf_vector::read_merged(
        fragment,
        entries
            .iter()
            .enumerate()
            .filter(|(i, _)| !cached.is_some_and(|c| c[*i]))
            .map(|(_, e)| e.blob_range())
            .collect(),
        bf_vector::read_gap(64 << 10),
        8 << 20,
    );
    let codes = async {
        if exact {
            Ok(Vec::new())
        } else {
            partial
                .index()
                .read_codes(&entries.iter().map(|e| e.cluster).collect::<Vec<_>>())
                .await
        }
    };
    let (blobs, codes) = tokio::try_join!(blobs, codes).map_err(segment::index_error)?;
    Ok((blobs, codes))
}

pub(crate) fn directory_file<'a>(
    dirs: &'a std::collections::BTreeMap<String, SegmentFile>,
    attr: &str,
) -> Option<(&'a SegmentFile, String)> {
    ["annv2:", "ann2:", "ann:"].into_iter().find_map(|prefix| {
        let key = format!("{prefix}{attr}");
        dirs.get(&key).map(|file| (file, key))
    })
}

/// Reconstruct a speculative paged hint's file map without losing the old index/fragment pair.
pub(crate) fn hint_files(
    s: &bf_vector::DirSegment,
    attr: &str,
    index: SegmentFile,
    fragment: SegmentFile,
) -> std::collections::BTreeMap<String, SegmentFile> {
    if s.key == s.fragment && s.codes_in_fragment {
        return [(format!("annv2:{attr}"), index)].into();
    }
    let prefix = if s.codes_in_fragment {
        "annfrag2"
    } else {
        "annfrag"
    };
    [
        (format!("ann:{attr}"), index),
        (format!("{prefix}:{attr}"), fragment),
    ]
    .into()
}

/// Reconstruct sparse slab membership only until the resident-map backfill publishes.
pub(crate) async fn slab_membership(
    store: &Storage,
    file: &SegmentFile,
) -> Result<Arc<Membership>, ApiError> {
    let key = format!("upgrade-sparse-member:{}", file.key);
    if let Some(member) = read_cache::get(&key, 0) {
        return Ok(member);
    }
    let member = segment::vector_index(store, file)
        .await?
        .membership()
        .map_err(segment::index_error)?;
    Ok(read_cache::insert(&key, member.resident_bytes(), member))
}
