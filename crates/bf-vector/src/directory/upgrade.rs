//! Transitional readers for old fragments, directories, and code headers.
//! They feed the current directory planner until the background rewrite replaces each object.
use super::*;

fn section(p: &mut Parser<'_>, segment: usize, blocks: bool) -> Result<Section> {
    let shape = get_shape(p)?;
    let padded = shape.dims.next_power_of_two();
    let clusters = p.u32()? as usize;
    let n = p.u32()? as usize;
    let rows = p.u32()?;
    let membership = p.u64()?..p.u64()?;
    let codes_in_fragment = if blocks {
        match p.u32()? {
            0 => false,
            1 => true,
            _ => return Err(bad("invalid directory section flags")),
        }
    } else {
        false
    };
    if n > clusters || n > p.remaining() || membership.start > membership.end {
        return Err(bad("invalid directory section"));
    }
    let centroid = Arc::new(p.vector(padded)?);
    let entries = (0..n)
        .map(|_| {
            let cluster = p.u32()? as usize;
            let rows = p.u32()?;
            let codes = p.u64()?..p.u64()?;
            let vectors = p.u64()?..p.u64()?;
            let codes_crc = p.u32()?;
            let vectors_crc = p.u32()?;
            let start = p.u64()?;
            let blob = start..start + p.u32()? as u64;
            let norm = f32::from_le_bytes(p.take(4)?.try_into().unwrap());
            let alpha = f32::from_le_bytes(p.take(4)?.try_into().unwrap());
            if cluster >= clusters || !norm.is_finite() || !(alpha > 0. && alpha <= 1.) {
                return Err(bad("directory entry outside segment"));
            }
            Ok(DirEntry {
                segment,
                cluster,
                rows,
                codes,
                vectors,
                codes_crc,
                vectors_crc,
                blob,
                norm,
                alpha,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let bits = p.take(n * (padded / 8).max(1))?.to_vec();
    Ok(Section {
        shape,
        clusters,
        centroid,
        entries,
        bits,
        rows,
        membership,
        codes_in_fragment,
        stubs: Default::default(),
    })
}

/// Decode current resident metadata or an older fragment without reading payloads.
pub fn fragment(bytes: &[u8]) -> Result<Fragment> {
    if bytes.starts_with(ANN_MAGIC) {
        return Fragment::decode(bytes);
    }
    if bytes.len() < 12 {
        return Err(bad("truncated legacy fragment"));
    }
    let (body, tail) = bytes.split_at(bytes.len() - 4);
    check_crc(body, u32::from_le_bytes(tail.try_into().unwrap()))?;
    let mut p = Parser::new(body);
    let magic = p.take(8)?;
    let blocks = magic == b"BFFRG003";
    if !blocks && magic != b"BFFRG002" {
        return Err(bad("bad legacy fragment magic"));
    }
    let section = section(&mut p, 0, blocks)?;
    if p.remaining() != 0 {
        return Err(bad("inconsistent legacy fragment length"));
    }
    Ok(Fragment(Arc::new(section)))
}

/// Decode an old plain directory into the current in-memory directory representation.
pub fn directory(bytes: &[u8]) -> Result<Directory> {
    if !bytes.starts_with(b"BFDIR003") {
        return Directory::decode(bytes);
    }
    if bytes.len() < 16 {
        return Err(bad("truncated legacy directory"));
    }
    let (body, tail) = bytes.split_at(bytes.len() - 4);
    check_crc(body, u32::from_le_bytes(tail.try_into().unwrap()))?;
    let mut p = Parser::new(body);
    p.take(8)?;
    let n = p.u32()? as usize;
    if n > p.remaining() {
        return Err(bad("invalid legacy directory"));
    }
    let mut sources = Vec::with_capacity(n);
    for i in 0..n {
        sources.push((
            text(&mut p)?,
            text(&mut p)?,
            text(&mut p)?,
            Fragment(Arc::new(section(&mut p, i, false)?)),
        ));
    }
    if p.remaining() != 0 {
        return Err(bad("inconsistent legacy directory length"));
    }
    let current =
        Directory::build(&sources)?.ok_or_else(|| bad("inconsistent legacy vector spaces"))?;
    Directory::decode(&current)
}

/// Apply the original code flags to a directory partial index during conversion.
/// The current directory encodes vector ranges, while old headers identify how codes scan.
pub fn partial_index<R: RangeRead>(
    dir: &Directory,
    segment: usize,
    reader: R,
    header: &[u8],
) -> Result<PartialIndex<R>> {
    if header.len() < 48 || &header[..8] != b"BFVIDX01" {
        return Err(bad("bad legacy index header"));
    }
    let mut partial = dir.partial_index(segment, reader)?;
    partial.0.metadata_mut().flags =
        u32::from_le_bytes(header[44..48].try_into().unwrap()) | crate::format::FLAG_ROTATED;
    Ok(partial)
}

/// Create an old fragment for migration tests; never enabled in release builds.
#[cfg(feature = "legacy-fixtures")]
pub fn legacy_fragment(index_blob: &[u8], blocks_mode: bool) -> Result<(Vec<u8>, u64)> {
    let index = Index::<Bytes>::metadata_only(index_blob, Bytes::new())?;
    let m = index.metadata();
    let shape = Shape {
        dims: m.dims,
        metric: m.metric,
        dtype: m.dtype,
        seed: m.seed,
        flat: m.flat,
    };
    let padded = m.dims.next_power_of_two();
    let live: Vec<usize> = (0..m.clusters.len())
        .filter(|c| !m.clusters[*c].rows.is_empty())
        .collect();
    let centroid = Arc::new(crate::format::mean_center(
        live.iter().map(|c| &*m.clusters[*c].center),
        padded,
    ));
    let root = (padded as f64).sqrt();
    let mut bits = Vec::with_capacity(live.len() * bit_bytes(padded));
    let mut entries = Vec::with_capacity(live.len());
    let mut blobs = Vec::new();
    let resident_len = 8
        + 24
        + 8
        + 20
        + padded * 4
        + live.len() * (ENTRY_BYTES + bit_bytes(padded))
        + 4
        + if blocks_mode { 4 } else { 0 };
    // Blocks are laid out in `order`; entries stay in cluster order.
    let order = if blocks_mode {
        tour(&live, |c| &*m.clusters[c].center)
    } else {
        live.clone()
    };
    let mut placed: Vec<Option<(u64, usize, u64)>> = vec![None; m.clusters.len()];
    for &cid in &order {
        let c = &m.clusters[cid];
        let blob = index.cluster_blob(cid);
        let start = (resident_len + blobs.len()) as u64;
        let codes_len = if blocks_mode {
            c.codes.end - c.codes.start
        } else {
            0
        };
        placed[cid] = Some((start, blob.len(), codes_len));
        blobs.extend(blob);
        if blocks_mode {
            let codes = index_blob
                .get(c.codes.start as usize..c.codes.end as usize)
                .ok_or_else(|| bad("cluster codes outside index"))?;
            blobs.extend_from_slice(codes);
        }
    }
    for &cid in &live {
        let c = &m.clusters[cid];
        let r: Vec<f64> = c
            .center
            .iter()
            .zip(centroid.iter())
            .map(|(x, g)| (*x - *g) as f64)
            .collect();
        let norm = r.iter().map(|x| x * x).sum::<f64>().sqrt();
        let mut code = vec![0u8; bit_bytes(padded)];
        for (i, x) in r.iter().enumerate() {
            if *x >= 0. {
                code[i / 8] |= 1 << (i % 8);
            }
        }
        bits.extend(code);
        let alpha = if norm == 0. {
            1.
        } else {
            r.iter().map(|x| x.abs()).sum::<f64>() / (norm * root)
        };
        let (start, blob_len, codes_len) = placed[cid].expect("every live cluster is placed");
        let blob_end = start + blob_len as u64;
        entries.push(DirEntry {
            segment: 0,
            cluster: cid,
            rows: c.rows.len() as u32,
            codes: if blocks_mode {
                blob_end..blob_end + codes_len
            } else {
                c.codes.clone()
            },
            vectors: c.vectors.clone(),
            codes_crc: c.codes_crc,
            vectors_crc: c.vectors_crc,
            blob: start..blob_end,
            norm: norm as f32,
            alpha: alpha.clamp(1e-6, 1.) as f32,
        });
    }
    // The row-to-cluster map: with it, a filtered query knows how many allowed rows each
    // cluster holds, and where each one's vector is, without reading cluster blobs.
    let mut member = Vec::new();
    if !m.flat && !blocks_mode {
        let wide = m.clusters.len() > u16::MAX as usize;
        let mut map = vec![0u32; m.count as usize];
        for (cid, c) in m.clusters.iter().enumerate() {
            for row in c.rows.iter() {
                *map.get_mut(row as usize)
                    .ok_or_else(|| bad("row outside segment"))? = cid as u32;
            }
        }
        for cid in map {
            if wide {
                member.extend(cid.to_le_bytes());
            } else {
                member.extend((cid as u16).to_le_bytes());
            }
        }
        let crc = crc32fast::hash(&member);
        put_u32(&mut member, crc);
    }
    let member_start = (resident_len + blobs.len()) as u64;
    let section = Section {
        shape,
        clusters: m.clusters.len(),
        centroid,
        entries,
        bits,
        rows: m.count,
        membership: member_start..member_start + member.len() as u64,
        codes_in_fragment: blocks_mode,
        stubs: Default::default(),
    };
    let mut out = if blocks_mode {
        b"BFFRG003".to_vec()
    } else {
        b"BFFRG002".to_vec()
    };
    section.put(&mut out);
    if !blocks_mode {
        out.drain(60..64);
    }
    let crc = crc32fast::hash(&out);
    put_u32(&mut out, crc);
    debug_assert_eq!(out.len(), resident_len);
    out.extend(blobs);
    out.extend(member);
    Ok((out, resident_len as u64))
}

/// Resident directory section for an index predating separate fragments. No vectors or codes
/// are read: the original reader supplies probed cluster blobs from its resident metadata.
pub fn fragment_from_index<R: RangeRead>(index: &Index<R>) -> Result<Fragment> {
    let m = index.metadata();
    let shape = Shape {
        dims: m.dims,
        metric: m.metric,
        dtype: m.dtype,
        seed: m.seed,
        flat: m.flat,
    };
    let padded = m.dims.next_power_of_two();
    let live = (0..m.clusters.len())
        .filter(|&c| !m.clusters[c].rows.is_empty())
        .collect::<Vec<_>>();
    let centroid = Arc::new(crate::format::mean_center(
        live.iter().map(|&c| &*m.clusters[c].center),
        padded,
    ));
    let mut entries = Vec::new();
    let mut bits = Vec::new();
    for cid in live {
        let c = &m.clusters[cid];
        let r = c
            .center
            .iter()
            .zip(centroid.iter())
            .map(|(x, g)| f64::from(*x - *g))
            .collect::<Vec<_>>();
        let norm = r.iter().map(|x| x * x).sum::<f64>().sqrt();
        let mut code = vec![0; bit_bytes(padded)];
        for (i, x) in r.iter().enumerate() {
            if *x >= 0. {
                code[i / 8] |= 1 << (i % 8);
            }
        }
        bits.extend(code);
        let alpha = if norm == 0. {
            1.
        } else {
            r.iter().map(|x| x.abs()).sum::<f64>() / (norm * (padded as f64).sqrt())
        };
        entries.push(DirEntry {
            segment: 0,
            cluster: cid,
            rows: c.rows.len() as u32,
            codes: c.codes.clone(),
            vectors: c.vectors.clone(),
            codes_crc: c.codes_crc,
            vectors_crc: c.vectors_crc,
            blob: 0..0,
            norm: norm as f32,
            alpha: alpha.clamp(1e-6, 1.) as f32,
        });
    }
    Ok(Fragment(Arc::new(Section {
        shape,
        clusters: m.clusters.len(),
        centroid,
        entries,
        bits,
        rows: m.count,
        membership: 0..0,
        codes_in_fragment: false,
        stubs: Default::default(),
    })))
}

/// Materialize only the requested leaf blobs from an older index's resident metadata.
pub fn cluster_blobs<R: RangeRead>(index: &Index<R>, clusters: &[usize]) -> Result<Vec<Bytes>> {
    clusters
        .iter()
        .map(|&c| {
            if c >= index.cluster_count() {
                return Err(bad("legacy cluster outside index"));
            }
            Ok(Bytes::from(index.cluster_blob(c)))
        })
        .collect()
}

/// Reconstruct dense segment membership for an index older than the membership fragments.
/// Sparse slab rows do not allocate a namespace-sized map.
pub fn membership<R: RangeRead>(index: &Index<R>) -> Option<Membership> {
    let m = index.metadata();
    if m.flat {
        return None;
    }
    let mut cluster_of = vec![0; m.count as usize];
    let mut ordinal_of = vec![0; m.count as usize];
    for (cid, c) in m.clusters.iter().enumerate() {
        for (ordinal, row) in c.rows.iter().enumerate() {
            *cluster_of.get_mut(row as usize)? = cid as u32;
            ordinal_of[row as usize] = ordinal as u32;
        }
    }
    Some(Membership {
        cluster_of,
        ordinal_of,
    })
}

#[cfg(all(test, feature = "legacy-fixtures"))]
mod tests {
    use super::*;

    #[test]
    fn plain_directories_normalize_and_page_without_changing_their_sources() {
        let blob = crate::IndexBuilder::new(16, Metric::EuclideanSquared)
            .unwrap()
            .build(
                &(0..100)
                    .map(|r| {
                        (
                            r,
                            (0..16)
                                .map(|d| ((r * 7919 + d * 104729) as f64).sin() as f32)
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let (bytes, meta) = legacy_fragment(&blob, false).unwrap();
        let f = fragment(&bytes[..meta as usize]).unwrap();
        let mut old = b"BFDIR003".to_vec();
        put_u32(&mut old, 1);
        for value in ["segment", "old.bfv", "old.bff"] {
            put_str(&mut old, value);
        }
        let at = old.len();
        f.0.put(&mut old);
        old.drain(at + 52..at + 56);
        let crc = crc32fast::hash(&old);
        put_u32(&mut old, crc);
        let dir = directory(&old).unwrap();
        assert_eq!(dir.segments()[0].key, "old.bfv");
        assert_eq!(dir.segments()[0].fragment, "old.bff");
        assert!(!dir.segments()[0].codes_in_fragment);
        let current =
            Directory::build(&[("segment".into(), "old.bfv".into(), "old.bff".into(), f)])
                .unwrap()
                .unwrap();
        let current = Directory::decode(&current).unwrap();
        assert_eq!(
            dir.center_distances_on(&[0.5; 16], false).unwrap(),
            current.center_distances_on(&[0.5; 16], false).unwrap()
        );
        let (pages, head) = dir
            .build_paged(
                8,
                7,
                &[[
                    (blob.len() as u64, crate::metadata_len(&blob).unwrap()),
                    (bytes.len() as u64, meta),
                ]],
            )
            .unwrap();
        let header = PagedDirectory::decode(&pages[..head as usize]).unwrap();
        assert!(!header.segments()[0].codes_in_fragment);
        old[12] ^= 1;
        assert!(directory(&old).is_err());
    }
}
