use std::{io::Cursor, ops::Range};

use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::{
    Error, Metric, RaBitQ, Result,
    cluster::{IndexBuilder, VectorType},
};

pub(crate) const FOOTER_SIZE: u64 = 64;
const MAGIC: &[u8; 8] = b"BFVFOOT1";
/// Current `.bfv` file format version.
pub const FORMAT_VERSION: u32 = 1;

pub(crate) struct Cluster {
    pub center: Vec<f32>,
    pub parent: u32,
    pub rows: RoaringBitmap,
    pub codes: Range<u64>,
    pub vectors: Range<u64>,
    pub codes_crc: u32,
    pub vectors_crc: u32,
    pub row_crcs: Vec<u32>,
}

pub(crate) struct Metadata {
    pub dims: usize,
    pub count: u32,
    pub metric: Metric,
    pub dtype: VectorType,
    pub seed: u64,
    pub flat: bool,
    pub parents: Vec<Vec<f32>>,
    pub clusters: Vec<Cluster>,
}

struct Block {
    cluster: Cluster,
    codes: Vec<u8>,
    vectors: Vec<u8>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_blob(
    b: &IndexBuilder,
    stored: &[(u32, Vec<f32>)],
    training: &[Vec<f32>],
    centers: Vec<Vec<f32>>,
    parents: Vec<Vec<f32>>,
    parent_ids: Vec<u32>,
    assignments: Vec<usize>,
    flat: bool,
) -> Result<Vec<u8>> {
    let rq = RaBitQ::new(b.dims, b.params.seed)?;
    let mut groups = vec![Vec::new(); centers.len()];
    for (i, c) in assignments.into_iter().enumerate() {
        groups[c].push(i);
    }
    let blocks: Vec<Block> = groups
        .into_par_iter()
        .enumerate()
        .map(|(c, group)| {
            let mut codes = Vec::new();
            let mut vectors = Vec::new();
            let mut row_crcs = Vec::with_capacity(group.len());
            let mut rows = RoaringBitmap::new();
            for i in group {
                rows.insert(stored[i].0);
                let start = vectors.len();
                for x in &stored[i].1 {
                    match b.dtype {
                        VectorType::F32 => vectors.extend(x.to_le_bytes()),
                        VectorType::I8 => vectors.push(*x as i8 as u8),
                        VectorType::F16 => vectors.extend(half::f16::from_f32(*x).to_le_bytes()),
                    }
                }
                row_crcs.push(crc32fast::hash(&vectors[start..]));
                if !flat {
                    let code = rq.encode_unchecked(&training[i], &centers[c]);
                    codes.extend(code.norm.to_le_bytes());
                    codes.extend(code.alpha.to_le_bytes());
                    codes.extend(u64::from(training[i].iter().all(|x| *x == 0.)).to_le_bytes());
                    codes.extend(code.bits);
                }
            }
            Block {
                cluster: Cluster {
                    center: centers[c].clone(),
                    parent: parent_ids[c],
                    rows,
                    codes: 0..codes.len() as u64,
                    vectors: 0..vectors.len() as u64,
                    codes_crc: crc32fast::hash(&codes),
                    vectors_crc: crc32fast::hash(&vectors),
                    row_crcs,
                },
                codes,
                vectors,
            }
        })
        .collect();
    let (clusters, payloads): (Vec<_>, Vec<_>) = blocks
        .into_iter()
        .map(|b| (b.cluster, (b.codes, b.vectors)))
        .unzip();
    let mut metadata = Metadata {
        dims: b.dims,
        count: stored.len() as u32,
        metric: b.metric,
        dtype: b.dtype,
        seed: b.params.seed,
        flat,
        parents,
        clusters,
    };
    let meta_len = encode_metadata(&metadata)?.len() as u64;
    let mut offset = meta_len;
    for cluster in &mut metadata.clusters {
        let codes_len = cluster.codes.end;
        let vectors_len = cluster.vectors.end;
        cluster.codes = offset..offset + codes_len;
        offset += codes_len;
        cluster.vectors = offset..offset + vectors_len;
        offset += vectors_len;
    }
    let mut blob = encode_metadata(&metadata)?;
    let meta_crc = crc32fast::hash(&blob);
    for (codes, vectors) in payloads {
        blob.extend(codes);
        blob.extend(vectors);
    }
    let mut footer = vec![0u8; FOOTER_SIZE as usize];
    footer[..8].copy_from_slice(MAGIC);
    footer[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    footer[16..24].copy_from_slice(&meta_len.to_le_bytes());
    footer[24..32].copy_from_slice(&(offset + FOOTER_SIZE).to_le_bytes());
    footer[32..36].copy_from_slice(&meta_crc.to_le_bytes());
    let checksum = crc32fast::hash(&footer[..60]);
    footer[60..].copy_from_slice(&checksum.to_le_bytes());
    blob.extend(footer);
    Ok(blob)
}

fn encode_metadata(m: &Metadata) -> Result<Vec<u8>> {
    let mut out = b"BFVIDX01".to_vec();
    put_u32(&mut out, m.dims as u32);
    put_u32(&mut out, m.count);
    put_u32(
        &mut out,
        if m.metric == Metric::CosineDistance {
            0
        } else {
            1
        },
    );
    put_u32(
        &mut out,
        match m.dtype {
            VectorType::F32 => 0,
            VectorType::F16 => 1,
            VectorType::I8 => 2,
        },
    );
    out.extend(m.seed.to_le_bytes());
    put_u32(&mut out, u32::from(m.flat));
    put_u32(&mut out, m.clusters.len() as u32);
    put_u32(&mut out, m.parents.len() as u32);
    put_u32(&mut out, 0);
    for p in &m.parents {
        for x in p {
            out.extend(x.to_le_bytes());
        }
    }
    for c in &m.clusters {
        for x in &c.center {
            out.extend(x.to_le_bytes());
        }
        put_u32(&mut out, c.parent);
        put_u32(&mut out, c.rows.len() as u32);
        for v in [c.codes.start, c.codes.end, c.vectors.start, c.vectors.end] {
            out.extend(v.to_le_bytes());
        }
        put_u32(&mut out, c.codes_crc);
        put_u32(&mut out, c.vectors_crc);
        put_u32(&mut out, c.rows.serialized_size() as u32);
        c.rows.serialize_into(&mut out)?;
        for crc in &c.row_crcs {
            put_u32(&mut out, *crc);
        }
    }
    Ok(out)
}

fn put_u32(out: &mut Vec<u8>, x: u32) {
    out.extend(x.to_le_bytes());
}

pub(crate) fn bad(message: &str) -> Error {
    Error::InvalidFormat(message.into())
}

/// Read the metadata length from a complete in-memory blob. Store this number
/// alongside the object key to open a cold index in one batched read round.
pub fn metadata_len(blob: &[u8]) -> Result<u64> {
    if blob.len() < FOOTER_SIZE as usize {
        return Err(bad("missing footer"));
    }
    Ok(decode_footer(
        &blob[blob.len() - FOOTER_SIZE as usize..],
        blob.len() as u64,
    )?
    .0)
}

pub(crate) fn decode_footer(bytes: &[u8], len: u64) -> Result<(u64, u32)> {
    if bytes.len() != FOOTER_SIZE as usize || &bytes[..8] != MAGIC {
        return Err(bad("bad footer magic or size"));
    }
    let mut p = Parser::new(&bytes[8..]);
    if p.u32()? != FORMAT_VERSION {
        return Err(bad("unsupported format version"));
    }
    if p.u32()? != 0 {
        return Err(bad("nonzero reserved footer field"));
    }
    let meta_len = p.u64()?;
    if p.u64()? != len || meta_len < 48 || meta_len > len.saturating_sub(FOOTER_SIZE) {
        return Err(bad("invalid footer offsets"));
    }
    let crc = p.u32()?;
    if bytes[36..60].iter().any(|b| *b != 0) {
        return Err(bad("nonzero reserved footer bytes"));
    }
    check_crc(
        &bytes[..60],
        u32::from_le_bytes(bytes[60..64].try_into().unwrap()),
    )?;
    Ok((meta_len, crc))
}

pub(crate) fn decode_metadata(bytes: &[u8], crc: u32, payload_end: u64) -> Result<Metadata> {
    check_crc(bytes, crc)?;
    let mut p = Parser::new(bytes);
    if p.take(8)? != b"BFVIDX01" {
        return Err(bad("bad metadata magic"));
    }
    let dims = p.u32()? as usize;
    if dims == 0 || dims > 65_536 {
        return Err(bad("invalid dimensions"));
    }
    let count = p.u32()?;
    let metric = match p.u32()? {
        0 => Metric::CosineDistance,
        1 => Metric::EuclideanSquared,
        _ => return Err(bad("invalid metric")),
    };
    let dtype = match p.u32()? {
        0 => VectorType::F32,
        1 => VectorType::F16,
        2 => VectorType::I8,
        _ => return Err(bad("invalid vector type")),
    };
    let seed = p.u64()?;
    let flat = match p.u32()? {
        0 => false,
        1 => true,
        _ => return Err(bad("invalid flat flag")),
    };
    let nclusters = p.u32()? as usize;
    let nparents = p.u32()? as usize;
    if nclusters == 0
        || nclusters > 65_536
        || nparents > nclusters
        || (flat && (nclusters != 1 || nparents != 0))
        || p.u32()? != 0
    {
        return Err(bad("invalid centroid directory"));
    }
    // Check minimum size before allocating based on untrusted counts.
    let min_size = nclusters
        .checked_mul(dims * 4 + 52)
        .and_then(|x| x.checked_add(nparents * dims * 4))
        .ok_or_else(|| bad("metadata size overflow"))?;
    if min_size > p.remaining() {
        return Err(bad("truncated centroid directory"));
    }
    let parents = (0..nparents)
        .map(|_| p.vector(dims))
        .collect::<Result<Vec<_>>>()?;
    let mut clusters = Vec::with_capacity(nclusters);
    let mut all_rows = RoaringBitmap::new();
    let mut offset = bytes.len() as u64;
    for _ in 0..nclusters {
        let center = p.vector(dims)?;
        let parent = p.u32()?;
        if (nparents == 0 && parent != u32::MAX) || (nparents > 0 && parent as usize >= nparents) {
            return Err(bad("invalid parent centroid"));
        }
        let n = p.u32()? as usize;
        let codes = p.u64()?..p.u64()?;
        let vectors = p.u64()?..p.u64()?;
        let codes_crc = p.u32()?;
        let vectors_crc = p.u32()?;
        let bitmap_len = p.u32()? as usize;
        let mut cursor = Cursor::new(p.take(bitmap_len)?);
        let rows = RoaringBitmap::deserialize_from(&mut cursor)
            .map_err(|_| bad("invalid membership bitmap"))?;
        if cursor.position() != bitmap_len as u64
            || rows.len() != n as u64
            || !all_rows.is_disjoint(&rows)
        {
            return Err(bad("invalid or duplicate row membership"));
        }
        all_rows |= &rows;
        if n > p.remaining() / 4 {
            return Err(bad("truncated vector checksums"));
        }
        let row_crcs = (0..n).map(|_| p.u32()).collect::<Result<Vec<_>>>()?;
        let code_len = if flat {
            0
        } else {
            n as u64 * (24 + dims.next_power_of_two().div_ceil(8)) as u64
        };
        let vector_len = n as u64 * dims as u64 * dtype.width() as u64;
        if codes.start != offset
            || codes.end.checked_sub(codes.start) != Some(code_len)
            || vectors.start != codes.end
            || vectors.end.checked_sub(vectors.start) != Some(vector_len)
            || vectors.end > payload_end
        {
            return Err(bad("invalid cluster ranges"));
        }
        offset = vectors.end;
        clusters.push(Cluster {
            center,
            parent,
            rows,
            codes,
            vectors,
            codes_crc,
            vectors_crc,
            row_crcs,
        });
    }
    if p.remaining() != 0 || offset != payload_end || all_rows.len() != count as u64 {
        return Err(bad("inconsistent index lengths or row count"));
    }
    Ok(Metadata {
        dims,
        count,
        metric,
        dtype,
        seed,
        flat,
        parents,
        clusters,
    })
}

pub(crate) fn check_crc(bytes: &[u8], crc: u32) -> Result<()> {
    if crc32fast::hash(bytes) != crc {
        return Err(bad("checksum mismatch"));
    }
    Ok(())
}

pub(crate) struct Parser<'a> {
    bytes: &'a [u8],
}
impl<'a> Parser<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    pub fn remaining(&self) -> usize {
        self.bytes.len()
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.bytes.len() {
            return Err(bad("truncated data"));
        }
        let (a, b) = self.bytes.split_at(n);
        self.bytes = b;
        Ok(a)
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn vector(&mut self, dims: usize) -> Result<Vec<f32>> {
        let bytes = self.take(dims * 4)?;
        let v: Vec<_> = bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|x| f32::from_le_bytes(*x))
            .collect();
        if v.iter().any(|x| !x.is_finite()) {
            return Err(bad("non-finite vector"));
        }
        Ok(v)
    }
}
