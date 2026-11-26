//! A dense, global-row-indexed transpose of immutable slab memberships.
use crate::{Membership, Result, RoaringBitmap, format::bad};
use bytes::Bytes;
use rayon::prelude::*;
type LocationRows = Vec<Vec<(u32, u32, u32)>>;
use std::ops::Range;

const HEADER: usize = 32;
const PAGE_ROWS: u64 = 1024;

/// Header of a tier's checksummed row-location object. Records use six bytes when
/// slab, cluster, and ordinal fit u16, and twelve bytes otherwise. Missing rows have
/// an all-ones slab address. Each page has its own checksum, allowing selective reads.
#[derive(Clone, Debug)]
pub struct RowLocations {
    rows: u64,
    slabs: usize,
    width: usize,
}

impl RowLocations {
    /// Transpose complete slab memberships. Sparse address spaces are rejected rather
    /// than allocating by an arbitrarily large global ID. Tier addresses are contiguous.
    pub fn encode(members: &[&Membership]) -> Result<Vec<u8>> {
        let count: usize = members.iter().map(|m| m.cluster_of.len()).sum();
        let rows = members
            .iter()
            .filter_map(|m| m.row_ids.as_ref()?.last().copied())
            .max()
            .map_or(0, |r| u64::from(r) + 1);
        if rows > (count as u64).saturating_mul(2) {
            return Err(bad("sparse tier address space"));
        }
        let wide = members.len() >= u16::MAX as usize
            || members.iter().any(|m| {
                m.clusters > u16::MAX as usize || m.ordinal_of.iter().any(|&o| o > u16::MAX as u32)
            });
        let header = Self {
            rows,
            slabs: members.len(),
            width: if wide { 12 } else { 6 },
        };
        let len = usize::try_from(header.len()).map_err(|_| bad("row locations too large"))?;
        let mut out = vec![255; len];
        out[..8].copy_from_slice(b"BFLOC001");
        out[8..16].copy_from_slice(&rows.to_le_bytes());
        out[16..20].copy_from_slice(
            &u32::try_from(members.len())
                .map_err(|_| bad("too many slabs"))?
                .to_le_bytes(),
        );
        out[20..24].copy_from_slice(&(header.width as u32).to_le_bytes());
        out[24..28].copy_from_slice(&(PAGE_ROWS as u32).to_le_bytes());
        let crc = crc32fast::hash(&out[..28]);
        out[28..32].copy_from_slice(&crc.to_le_bytes());
        for (slab, member) in members.iter().enumerate() {
            let ids = member
                .row_ids
                .as_ref()
                .ok_or_else(|| bad("expected sparse slab"))?;
            let mut next = vec![0u32; member.clusters.min(ids.len())];
            let mut overflow = std::collections::BTreeMap::new();
            for ((&row, &cluster), &ordinal) in
                ids.iter().zip(&member.cluster_of).zip(&member.ordinal_of)
            {
                let expected = match next.get_mut(cluster as usize) {
                    Some(next) => next,
                    None => overflow.entry(cluster).or_default(),
                };
                if ordinal != *expected {
                    return Err(bad("incomplete slab membership"));
                }
                *expected += 1;
                let page = u64::from(row) / PAGE_ROWS;
                let at = header.page(page).start as usize
                    + row as usize % PAGE_ROWS as usize * header.width;
                let record = &mut out[at..at + header.width];
                if record.iter().any(|b| *b != 255) {
                    return Err(bad("duplicate tier row location"));
                }
                for (part, value) in
                    record
                        .chunks_exact_mut(header.width / 3)
                        .zip([slab as u32, cluster, ordinal])
                {
                    if wide {
                        part.copy_from_slice(&value.to_le_bytes());
                    } else {
                        part.copy_from_slice(&(value as u16).to_le_bytes());
                    }
                }
            }
        }
        for page in 0..header.pages() {
            let range = header.page(page);
            let end = range.end as usize - 4;
            let crc = crc32fast::hash(&out[range.start as usize..end]);
            out[end..end + 4].copy_from_slice(&crc.to_le_bytes());
        }
        Ok(out)
    }

    /// Decode the fixed header, validating its checksum, geometry, and physical length.
    pub fn decode(bytes: &[u8], len: u64) -> Result<Self> {
        if bytes.len() != HEADER
            || &bytes[..8] != b"BFLOC001"
            || crc32fast::hash(&bytes[..28])
                != u32::from_le_bytes(bytes[28..32].try_into().unwrap())
        {
            return Err(bad("invalid row-location header"));
        }
        let header = Self {
            rows: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            slabs: u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize,
            width: u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize,
        };
        if !matches!(header.width, 6 | 12)
            || header.rows > u64::from(u32::MAX) + 1
            || u32::from_le_bytes(bytes[24..28].try_into().unwrap()) != PAGE_ROWS as u32
            || header.len() != len
            || (header.width == 6 && header.slabs >= u16::MAX as usize)
        {
            return Err(bad("invalid row-location geometry"));
        }
        Ok(header)
    }

    /// Total encoded bytes, including independent page checksums.
    pub fn len(&self) -> u64 {
        HEADER as u64 + self.rows * self.width as u64 + self.pages() * 4
    }
    /// Whether the tier contains no row addresses.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
    fn pages(&self) -> u64 {
        self.rows.div_ceil(PAGE_ROWS)
    }
    fn page(&self, page: u64) -> Range<u64> {
        let start = HEADER as u64 + page * (PAGE_ROWS * self.width as u64 + 4);
        start..start + (self.rows - page * PAGE_ROWS).min(PAGE_ROWS) * self.width as u64 + 4
    }
    /// Unique pages needed by the selected global rows, in file order.
    pub fn ranges(&self, allowed: &RoaringBitmap) -> Vec<(u64, Range<u64>)> {
        let mut pages = Vec::new();
        for row in allowed.iter().take_while(|r| u64::from(*r) < self.rows) {
            let page = u64::from(row) / PAGE_ROWS;
            if pages.last().is_none_or(|(last, _)| *last != page) {
                pages.push((page, self.page(page)));
            }
        }
        pages
    }

    /// Project only selected rows into slab memberships, retaining each row's original
    /// cluster ordinal. Every requested page must be present and independently verified.
    pub fn project(
        &self,
        allowed: &RoaringBitmap,
        pages: &[(u64, Bytes)],
        clusters: &[usize],
        slab_rows: &[u64],
    ) -> Result<Vec<Membership>> {
        if clusters.len() != self.slabs || slab_rows.len() != self.slabs {
            return Err(bad("row locations do not cover tier"));
        }
        let wanted = self.ranges(allowed);
        if wanted.len() != pages.len() {
            return Err(bad("incomplete row-location pages"));
        }
        let parallel = pages.len() > 256 && allowed.len() > 8192;
        let rows = if parallel {
            let groups: Vec<_> = wanted
                .par_chunks(256)
                .zip(pages.par_chunks(256))
                .map(|(wanted, pages)| {
                    self.project_pages(allowed, wanted, pages, clusters, slab_rows)
                })
                .collect::<Result<_>>()?;
            let mut rows = vec![Vec::new(); self.slabs];
            for group in groups {
                for (rows, mut part) in rows.iter_mut().zip(group) {
                    rows.append(&mut part);
                }
            }
            rows
        } else {
            self.project_pages(allowed, &wanted, pages, clusters, slab_rows)?
        };
        if parallel {
            rows.into_par_iter()
                .zip(clusters.par_iter())
                .map(|(r, &c)| Membership::from_locations(r, c))
                .collect()
        } else {
            rows.into_iter()
                .zip(clusters)
                .map(|(r, &c)| Membership::from_locations(r, c))
                .collect()
        }
    }

    fn project_pages(
        &self,
        allowed: &RoaringBitmap,
        wanted: &[(u64, Range<u64>)],
        pages: &[(u64, Bytes)],
        clusters: &[usize],
        slab_rows: &[u64],
    ) -> Result<LocationRows> {
        let mut rows = vec![Vec::new(); self.slabs];
        for ((page, range), (got, bytes)) in wanted.iter().cloned().zip(pages) {
            if page != *got || bytes.len() as u64 != range.end - range.start || bytes.len() < 4 {
                return Err(bad("invalid row-location page"));
            }
            let (body, tail) = bytes.split_at(bytes.len() - 4);
            if crc32fast::hash(body) != u32::from_le_bytes(tail.try_into().unwrap()) {
                return Err(bad("row-location checksum mismatch"));
            }
            let base = page * PAGE_ROWS;
            for row in allowed.range(
                base as u32..=u32::try_from((base + PAGE_ROWS - 1).min(self.rows - 1)).unwrap(),
            ) {
                let at = (u64::from(row) - base) as usize * self.width;
                let record = &body[at..at + self.width];
                let mut values = record.chunks_exact(self.width / 3).map(|p| {
                    if self.width == 6 {
                        u32::from(u16::from_le_bytes(p.try_into().unwrap()))
                    } else {
                        u32::from_le_bytes(p.try_into().unwrap())
                    }
                });
                let slab = values.next().unwrap();
                if slab
                    == if self.width == 6 {
                        u16::MAX as u32
                    } else {
                        u32::MAX
                    }
                {
                    continue;
                }
                let cluster = values.next().unwrap();
                let ordinal = values.next().unwrap();
                if slab as usize >= self.slabs
                    || cluster as usize >= clusters[slab as usize]
                    || u64::from(ordinal) >= slab_rows[slab as usize]
                {
                    return Err(bad("row location outside slab"));
                }
                rows[slab as usize].push((row, cluster, ordinal));
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selective_pages_preserve_ordinals_holes_and_wide_addresses() {
        for clusters in [3, 65_536] {
            let a = Membership::from_sparse_rows(
                (0..=2048)
                    .step_by(2)
                    .map(|r| (r, if r == 2048 { 2 } else { 1 })),
                clusters,
            )
            .unwrap();
            let b = Membership::from_sparse_rows(
                (1..2048).step_by(2).filter(|r| *r != 3).map(|r| (r, 0)),
                1,
            )
            .unwrap();
            let bytes = RowLocations::encode(&[&a, &b]).unwrap();
            let h = RowLocations::decode(&bytes[..HEADER], bytes.len() as u64).unwrap();
            let allowed: RoaringBitmap = [2, 3, 2047, 2048, u32::MAX].into();
            let ranges = h.ranges(&allowed);
            assert_eq!(h.ranges(&[2].into()).len(), 1);
            let pages: Vec<_> = ranges
                .iter()
                .map(|(p, r)| {
                    (
                        *p,
                        Bytes::copy_from_slice(&bytes[r.start as usize..r.end as usize]),
                    )
                })
                .collect();
            let projected = h
                .project(&allowed, &pages, &[clusters, 1], &[1025, 1023])
                .unwrap();
            assert_eq!(
                projected[0].locations(&allowed, |_| true),
                [(1, 1, 2), (2, 0, 2048)]
            );
            assert_eq!(
                projected[1].locations(&allowed, |_| true),
                [(0, 1022, 2047)]
            );
            assert!(
                h.project(&allowed, &pages[..1], &[clusters, 1], &[1025, 1023])
                    .is_err()
            );
            let mut corrupt = pages;
            let mut bytes = corrupt[0].1.to_vec();
            bytes[0] ^= 1;
            corrupt[0].1 = bytes.into();
            assert!(
                h.project(&allowed, &corrupt, &[clusters, 1], &[1025, 1023])
                    .is_err()
            );
        }
    }

    #[test]
    fn wide_ordinals_and_malformed_headers_are_checked() {
        let member = Membership::from_sparse_rows((0..=65_536).map(|r| (r, 0)), 1).unwrap();
        let bytes = RowLocations::encode(&[&member]).unwrap();
        let h = RowLocations::decode(&bytes[..HEADER], bytes.len() as u64).unwrap();
        assert_eq!(h.width, 12);
        let allowed: RoaringBitmap = [65_536].into();
        let pages: Vec<_> = h
            .ranges(&allowed)
            .into_iter()
            .map(|(id, r)| {
                (
                    id,
                    Bytes::copy_from_slice(&bytes[r.start as usize..r.end as usize]),
                )
            })
            .collect();
        assert_eq!(
            h.project(&allowed, &pages, &[1], &[65_537]).unwrap()[0].locations(&allowed, |_| true),
            [(0, 65_536, 65_536)]
        );
        assert!(RowLocations::encode(&[&member, &member]).is_err());
        let partial = Membership::from_locations([(0, 0, 1)], 1).unwrap();
        assert!(RowLocations::encode(&[&partial]).is_err());
        let sparse = Membership::from_sparse_rows([(u32::MAX, 0)], 1).unwrap();
        assert!(RowLocations::encode(&[&sparse]).is_err());
        let mut header = bytes[..HEADER].to_vec();
        header[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        let crc = crc32fast::hash(&header[..28]);
        header[28..].copy_from_slice(&crc.to_le_bytes());
        assert!(RowLocations::decode(&header, bytes.len() as u64).is_err());
        assert!(RowLocations::decode(&bytes[..31], bytes.len() as u64).is_err());
    }

    #[test]
    fn parallel_projection_keeps_slab_order_and_original_ordinals() {
        let members: Vec<_> = (0..3)
            .map(|slab| {
                Membership::from_sparse_rows(
                    (0..1_000_000u32)
                        .filter(|r| r % 3 == slab)
                        .map(|r| (r, r / 64 % 8)),
                    8,
                )
                .unwrap()
            })
            .collect();
        let refs: Vec<_> = members.iter().collect();
        let bytes = RowLocations::encode(&refs).unwrap();
        let h = RowLocations::decode(&bytes[..HEADER], bytes.len() as u64).unwrap();
        let allowed = (0..1_000_000u32).step_by(7).collect();
        let pages: Vec<_> = h
            .ranges(&allowed)
            .into_iter()
            .map(|(id, r)| {
                (
                    id,
                    Bytes::copy_from_slice(&bytes[r.start as usize..r.end as usize]),
                )
            })
            .collect();
        let rows: Vec<_> = members
            .iter()
            .map(|m| m.row_ids.as_ref().unwrap().len() as u64)
            .collect();
        let projected = h.project(&allowed, &pages, &[8; 3], &rows).unwrap();
        for (a, b) in members.iter().zip(projected) {
            assert_eq!(
                a.locations(&allowed, |_| true),
                b.locations(&allowed, |_| true)
            );
        }
    }
}
