//! Compact, optional row-to-ID maps for ID-only text results. Numeric strides and
//! string prefixes compress before admission. Large maps record a ranged-read marker,
//! so they neither add cold overfetch nor get rebuilt on every indexer pass.
use crate::{read_cache, segment};
use bf_storage::Storage;
use bf_types::{ApiError, Id};
use bf_wal::{Segment, SegmentFile};
use std::sync::Arc;

pub(crate) const ROLE: &str = "text_keys";
const MAGIC: &[u8; 8] = b"BFTKEY01";
const MAX_BYTES: usize = 128 << 10;

pub(crate) enum Keys {
    /// IDs, and whether they are strictly ascending (checked once, when decoded).
    Numeric(Vec<u64>, bool),
    Strings(Vec<String>, bool),
}
impl Keys {
    /// Rows are in strictly ascending ID order, which is the order that breaks score ties.
    pub(crate) fn ascending(&self) -> bool {
        match self {
            Self::Numeric(_, ascending) | Self::Strings(_, ascending) => *ascending,
        }
    }
    pub(crate) fn id(&self, row: u32) -> Result<Id, ApiError> {
        match self {
            Self::Numeric(ids, _) => ids.get(row as usize).copied().map(Id::Uint),
            Self::Strings(ids, _) => ids.get(row as usize).cloned().map(Id::String),
        }
        .ok_or_else(segment::corrupt)
    }
    fn resident(&self) -> u64 {
        (64 + match self {
            Self::Numeric(ids, _) => ids.capacity() * 8,
            Self::Strings(ids, _) => {
                ids.capacity() * std::mem::size_of::<String>()
                    + ids.iter().map(String::capacity).sum::<usize>()
            }
        }) as u64
    }
}
fn take<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], ApiError> {
    let value = bytes
        .get(..N)
        .ok_or_else(segment::corrupt)?
        .try_into()
        .unwrap();
    *bytes = &bytes[N..];
    Ok(value)
}
fn finish(mut body: Vec<u8>) -> Vec<u8> {
    body.extend(crc32fast::hash(&body).to_le_bytes());
    body
}
fn encode(ids: &[Id]) -> Result<(Vec<u8>, bool), ApiError> {
    let count = u32::try_from(ids.len()).map_err(segment::index_error)?;
    let mut header = MAGIC.to_vec();
    header.extend(count.to_le_bytes());
    let mut body = header.clone();
    match ids.first() {
        Some(Id::String(_)) => {
            body.push(1);
            let mut previous: &[u8] = &[];
            for id in ids {
                let Id::String(id) = id else {
                    return Err(segment::corrupt());
                };
                let id = id.as_bytes();
                if id.len() > 64 {
                    return Err(segment::corrupt());
                }
                let prefix = previous.iter().zip(id).take_while(|(a, b)| a == b).count();
                body.push(prefix as u8);
                body.push((id.len() - prefix) as u8);
                body.extend_from_slice(&id[prefix..]);
                previous = id;
                if body.len() + 4 > MAX_BYTES {
                    header.push(2);
                    return Ok((finish(header), false));
                }
            }
        }
        _ => {
            body.push(0);
            if let Some(Id::Uint(first)) = ids.first() {
                body.extend(first.to_le_bytes());
                let mut i = 1;
                while i < ids.len() {
                    let (Id::Uint(a), Id::Uint(b)) = (&ids[i - 1], &ids[i]) else {
                        return Err(segment::corrupt());
                    };
                    let delta = b.wrapping_sub(*a);
                    let mut end = i + 1;
                    while end < ids.len() {
                        let (Id::Uint(a), Id::Uint(b)) = (&ids[end - 1], &ids[end]) else {
                            return Err(segment::corrupt());
                        };
                        if b.wrapping_sub(*a) != delta {
                            break;
                        }
                        end += 1;
                    }
                    body.extend(((end - i) as u32).to_le_bytes());
                    body.extend(delta.to_le_bytes());
                    i = end;
                    if body.len() + 4 > MAX_BYTES {
                        header.push(2);
                        return Ok((finish(header), false));
                    }
                }
            }
        }
    }
    Ok((finish(body), true))
}
fn decode(bytes: &[u8], rows: u32) -> Result<Option<Keys>, ApiError> {
    if bytes.len() < 17 || bytes.len() > MAX_BYTES || &bytes[..8] != MAGIC {
        return Err(segment::corrupt());
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if crc32fast::hash(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err(segment::corrupt());
    }
    let mut body = &body[8..];
    if u32::from_le_bytes(take(&mut body)?) != rows {
        return Err(segment::corrupt());
    }
    let mode = take::<1>(&mut body)?[0];
    let keys = match mode {
        0 => {
            let mut ids = Vec::new();
            ids.try_reserve_exact(rows as usize)
                .map_err(segment::index_error)?;
            if rows > 0 {
                ids.push(u64::from_le_bytes(take(&mut body)?));
            }
            while ids.len() < rows as usize {
                let count = u32::from_le_bytes(take(&mut body)?) as usize;
                let delta = u64::from_le_bytes(take(&mut body)?);
                if count == 0 || count > rows as usize - ids.len() {
                    return Err(segment::corrupt());
                }
                for _ in 0..count {
                    ids.push(ids.last().unwrap().wrapping_add(delta));
                }
            }
            let ascending = ids.windows(2).all(|w| w[0] < w[1]);
            Keys::Numeric(ids, ascending)
        }
        1 => {
            let mut ids: Vec<String> = Vec::new();
            ids.try_reserve_exact(rows as usize)
                .map_err(segment::index_error)?;
            for _ in 0..rows {
                let prefix = take::<1>(&mut body)?[0] as usize;
                let suffix = take::<1>(&mut body)?[0] as usize;
                let previous = ids.last().map_or(&[][..], |s| s.as_bytes());
                if prefix > previous.len() || prefix + suffix > 64 {
                    return Err(segment::corrupt());
                }
                let mut id = previous[..prefix].to_vec();
                id.extend_from_slice(body.get(..suffix).ok_or_else(segment::corrupt)?);
                body = &body[suffix..];
                ids.push(String::from_utf8(id).map_err(segment::index_error)?);
            }
            let ascending = ids.windows(2).all(|w| w[0] < w[1]);
            Keys::Strings(ids, ascending)
        }
        2 if body.is_empty() => return Ok(None),
        _ => return Err(segment::corrupt()),
    };
    if !body.is_empty() {
        return Err(segment::corrupt());
    }
    Ok(Some(keys))
}
pub(crate) fn needs(seg: &Segment) -> bool {
    seg.files.keys().any(|key| key.starts_with("fts:")) && !seg.files.contains_key(ROLE)
}
pub(crate) fn inline(seg: &Segment) -> bool {
    seg.files
        .get(ROLE)
        .is_some_and(|f| f.metadata_len > 0 && f.size <= MAX_BYTES as u64)
}
pub(crate) async fn put(
    store: &Storage,
    prefix: &str,
    ids: &[Id],
) -> Result<SegmentFile, ApiError> {
    let (bytes, inline) = encode(ids)?;
    let metadata = if inline { bytes.len() as u64 } else { 0 };
    segment::put_file(
        store,
        format!("{prefix}/text-keys.{}.bfk", uuid::Uuid::new_v4()),
        bytes,
        metadata,
    )
    .await
}
pub(crate) async fn backfill(
    store: &Storage,
    ns: &str,
    seg: &mut Segment,
) -> Result<bool, ApiError> {
    if !needs(seg) {
        return Ok(false);
    }
    let ids = segment::ids(store, seg).await?;
    let file = put(store, &format!("ns/{ns}/segments/{}", seg.id), &ids).await?;
    seg.files.insert(ROLE.into(), file);
    Ok(true)
}
pub(crate) async fn load(store: &Storage, seg: &Segment) -> Result<Arc<Keys>, ApiError> {
    let file = seg.files.get(ROLE).ok_or_else(segment::corrupt)?;
    let key = format!("textkeys:{}", file.key);
    if let Some(keys) = read_cache::get(&key, file.size) {
        return Ok(keys);
    }
    let bytes = segment::read_whole(store, file).await?;
    let keys = decode(&bytes, seg.row_count)?.ok_or_else(segment::corrupt)?;
    Ok(read_cache::insert(&key, keys.resident(), keys))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strides_prefixes_and_wrapping_values_round_trip() {
        for ids in [
            vec![],
            (0..10000).map(|i| Id::Uint(1000000000 + i)).collect(),
            vec![Id::Uint(0), Id::Uint(u64::MAX), Id::Uint(1), Id::Uint(1)],
            vec![
                Id::String("café-one".into()),
                Id::String("café-two".into()),
                Id::String("東京".into()),
            ],
        ] {
            let (bytes, inline) = encode(&ids).unwrap();
            assert!(inline);
            let decoded = decode(&bytes, ids.len() as u32).unwrap().unwrap();
            assert_eq!(decoded.ascending(), ids.windows(2).all(|w| w[0] < w[1]));
            for (row, id) in ids.iter().enumerate() {
                assert_eq!(&decoded.id(row as u32).unwrap(), id);
            }
        }
    }
    #[test]
    fn large_maps_record_a_ranged_marker() {
        let ids: Vec<_> = (0..20000u64)
            .map(|i| Id::Uint(i.wrapping_mul(i).wrapping_mul(7919)))
            .collect();
        let (bytes, inline) = encode(&ids).unwrap();
        assert!(!inline);
        assert_eq!(bytes.len(), 17);
        assert!(decode(&bytes, ids.len() as u32).unwrap().is_none());
    }
    #[test]
    fn corrupt_truncated_and_wrong_row_counts_fail_closed() {
        let (bytes, _) = encode(&[Id::Uint(42), Id::Uint(43)]).unwrap();
        for len in 0..bytes.len() {
            assert!(decode(&bytes[..len], 2).is_err());
        }
        let mut bad = bytes.clone();
        bad[15] ^= 1;
        assert!(decode(&bad, 2).is_err());
        assert!(decode(&bytes, 3).is_err());
    }
}
