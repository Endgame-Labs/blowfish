use std::{io::Cursor, ops::Range, sync::Arc};

use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::{
    Error, Metric, RaBitQ, Result,
    cluster::{IndexBuilder, Packed, VectorType},
};

pub(crate) const FOOTER_SIZE: u64 = 64;
pub(crate) const ANN_FOOTER_MAGIC: &[u8; 8] = b"BFAFOOT2";
pub(crate) const MAGIC: &[u8; 8] = b"BFVFOOT1";
/// Current `.bfv` file format version.
pub const FORMAT_VERSION: u32 = 1;
/// Current single ANN object format version.
pub const ANN_FORMAT_VERSION: u32 = 2;
/// Metadata flag: centers and parents are stored rotated and zero-padded to the transform width.
pub(crate) const FLAG_ROTATED: u32 = 1;
/// Metadata flag: eight-byte code headers (f32 norm and alpha) and all codes stored before all vectors.
pub(crate) const FLAG_COMPACT: u32 = 2;
/// Metadata flag (with [`FLAG_COMPACT`]): each cluster's codes are in the blocked layout of
/// [`crate::fastscan`]. A reader tells the layouts apart by a code block's length too, which is
/// what a partial index over a directory does.
pub(crate) const FLAG_FASTSCAN: u32 = 4;

/// The mean of the non-empty clusters' rotated centers, accumulated in cluster order in `f32`.
/// A directory section's centroid and a fast-scan index's reference point are this same value,
/// so a partial index can rebuild the reference from the section.
pub(crate) fn mean_center<'a>(centers: impl Iterator<Item = &'a [f32]>, padded: usize) -> Vec<f32> {
    let mut mean = vec![0f32; padded];
    let mut n = 0usize;
    for c in centers {
        for (g, x) in mean.iter_mut().zip(c) {
            *g += *x;
        }
        n += 1;
    }
    for g in &mut mean {
        *g /= n.max(1) as f32;
    }
    mean
}

/// A leaf cluster's decoded payload description: center, rows, and checksums. Shared (one
/// reference count) by every index holding the cluster, including the per-query partial
/// indexes that install a cached decoded directory cluster (see [`crate::DecodedCluster`]).
#[derive(Debug, Default)]
pub(crate) struct ClusterData {
    pub center: Vec<f32>,
    pub rows: RoaringBitmap,
    pub row_crcs: Vec<u32>,
    /// `rows` in order, when a decoded directory cluster supplied them; scans and reranks
    /// then map ordinals to rows by index.
    pub ids: Option<Vec<u32>>,
    /// Set once the code block has matched `codes_crc`, so a cached cluster's immutable code
    /// block is verified once rather than on every scan.
    pub codes_checked: std::sync::atomic::AtomicBool,
}

impl ClusterData {
    /// Rows in the cluster: one checksum per row, so no walk over the bitmap's containers.
    pub fn len(&self) -> usize {
        self.row_crcs.len()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Cluster {
    /// `None` for a stub (a directory cluster not installed): reads see an empty payload.
    pub data: Option<Arc<ClusterData>>,
    pub parent: u32,
    pub codes: Range<u64>,
    pub vectors: Range<u64>,
    pub codes_crc: u32,
    pub vectors_crc: u32,
}

/// A normal index owns its complete table. A query's partial index shares immutable ranges
/// and owns only the few payloads it installs. Cluster IDs never change between the views.
pub(crate) enum Clusters {
    Dense(Vec<Cluster>),
    Partial {
        base: Arc<Vec<Cluster>>,
        installed: Vec<(usize, Cluster)>,
    },
}
impl From<Vec<Cluster>> for Clusters {
    fn from(value: Vec<Cluster>) -> Self {
        Self::Dense(value)
    }
}
impl Clusters {
    pub fn partial(base: Arc<Vec<Cluster>>) -> Self {
        Self::Partial {
            base,
            installed: Vec::new(),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Dense(rows) => rows.len(),
            Self::Partial { base, .. } => base.len(),
        }
    }
    pub fn get(&self, index: usize) -> Option<&Cluster> {
        match self {
            Self::Dense(rows) => rows.get(index),
            Self::Partial { base, installed } => {
                match installed.binary_search_by_key(&index, |(id, _)| *id) {
                    Ok(at) => Some(&installed[at].1),
                    Err(_) => base.get(index),
                }
            }
        }
    }
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Cluster> {
        // A fully opened index should keep contiguous iteration and lookup. Partial queries
        // normally install only a few percent of these slots.
        if matches!(self, Self::Partial { base, installed } if installed.len() >= base.len().div_ceil(2))
        {
            self.make_dense();
        }
        match self {
            Self::Dense(rows) => rows.get_mut(index),
            Self::Partial { base, installed } => {
                if index >= base.len() {
                    return None;
                }
                let at = match installed.binary_search_by_key(&index, |(id, _)| *id) {
                    Ok(at) => at,
                    Err(at) => {
                        installed.insert(at, (index, base[index].clone()));
                        at
                    }
                };
                Some(&mut installed[at].1)
            }
        }
    }
    pub fn iter(&self) -> ClusterIter<'_> {
        ClusterIter {
            table: self,
            indices: 0..self.len(),
        }
    }
    fn make_dense(&mut self) {
        if let Self::Partial { base, installed } = self {
            let mut rows = base.as_ref().clone();
            for (i, row) in installed.drain(..) {
                rows[i] = row;
            }
            *self = Self::Dense(rows);
        }
    }
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, Cluster> {
        self.make_dense();
        match self {
            Self::Dense(rows) => rows.iter_mut(),
            Self::Partial { .. } => unreachable!(),
        }
    }
}
impl std::ops::Index<usize> for Clusters {
    type Output = Cluster;
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("cluster index")
    }
}
pub(crate) struct ClusterIter<'a> {
    table: &'a Clusters,
    indices: std::ops::Range<usize>,
}
impl<'a> Iterator for ClusterIter<'a> {
    type Item = &'a Cluster;
    fn next(&mut self) -> Option<Self::Item> {
        self.indices.next().map(|i| &self.table[i])
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.indices.size_hint()
    }
}
impl DoubleEndedIterator for ClusterIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.indices.next_back().map(|i| &self.table[i])
    }
}
impl ExactSizeIterator for ClusterIter<'_> {}
impl<'a> IntoIterator for &'a Clusters {
    type Item = &'a Cluster;
    type IntoIter = ClusterIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a> IntoIterator for &'a mut Clusters {
    type Item = &'a mut Cluster;
    type IntoIter = std::slice::IterMut<'a, Cluster>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
static EMPTY_CLUSTER: std::sync::LazyLock<ClusterData> = std::sync::LazyLock::new(Default::default);
impl std::ops::Deref for Cluster {
    type Target = ClusterData;
    fn deref(&self) -> &ClusterData {
        self.data.as_deref().unwrap_or(&EMPTY_CLUSTER)
    }
}

pub(crate) struct Metadata {
    pub dims: usize,
    pub count: u32,
    pub metric: Metric,
    pub dtype: VectorType,
    pub seed: u64,
    pub flat: bool,
    pub flags: u32,
    pub parents: Vec<Vec<f32>>,
    pub clusters: Clusters,
}

struct Block {
    cluster: Cluster,
    codes: Vec<u8>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_blob(
    b: &IndexBuilder,
    rows: &Packed,
    centers: Vec<Vec<f32>>,
    parents: Vec<Vec<f32>>,
    parent_ids: Vec<u32>,
    assignments: Vec<usize>,
    flat: bool,
) -> Result<Vec<u8>> {
    let rq = RaBitQ::new(b.dims, b.params.seed)?;
    let mut groups: Vec<Vec<u32>> = vec![Vec::new(); centers.len()];
    for (i, c) in assignments.into_iter().enumerate() {
        groups[c].push(i as u32);
    }
    // Only an index with fewer than eight padded dimensions (no whole code byte per block row) keeps
    // the row-major layout; every other index is written in the fast-scan one.
    let fastscan = !flat && rq.padded.is_multiple_of(8);
    let rotated: Vec<Vec<f32>> = centers.par_iter().map(|c| rotated_center(&rq, c)).collect();
    let reference = mean_center(
        rotated
            .iter()
            .zip(&groups)
            .filter(|(_, g)| !g.is_empty())
            .map(|(c, _)| c.as_slice()),
        rq.padded,
    );
    let root = (rq.padded as f64).sqrt();
    // Only the quantized codes are materialized per cluster. The full-precision bytes are the
    // packed input rows verbatim, so they are checksummed here and copied once into the blob.
    let blocks: Vec<Block> = groups
        .par_iter()
        .enumerate()
        .map(|(c, group)| {
            let mut codes = Vec::new();
            let mut row_crcs = Vec::with_capacity(group.len());
            let mut vectors_crc = crc32fast::Hasher::new();
            let mut vectors_len = 0u64;
            let mut ids = RoaringBitmap::new();
            let mut fast_rows = Vec::new();
            for &i in group {
                let i = i as usize;
                ids.insert(rows.row_id(i));
                let raw = rows.raw(i);
                row_crcs.push(crc32fast::hash(raw));
                vectors_crc.update(raw);
                vectors_len += raw.len() as u64;
                if !flat {
                    let training = rows.prepared(i);
                    let code = rq.encode_unchecked(&training, &centers[c]);
                    let zero = training.iter().all(|x| *x == 0.);
                    let alpha = (code.alpha as f32).clamp(1e-6, 1.);
                    let signed = if zero { -alpha } else { alpha };
                    if fastscan {
                        // `<b, c - g> / sqrt(D)`: the part of the residual inner product that
                        // does not depend on the query (see `fastscan`).
                        let cb = rotated[c]
                            .iter()
                            .zip(&reference)
                            .enumerate()
                            .map(|(i, (c, g))| {
                                let x = *c as f64 - *g as f64;
                                if code.bits[i / 8] & (1 << (i % 8)) != 0 {
                                    x
                                } else {
                                    -x
                                }
                            })
                            .sum::<f64>()
                            / root;
                        fast_rows.push(crate::fastscan::RowCode {
                            bits: code.bits,
                            norm: code.norm as f32,
                            alpha,
                            cb: cb as f32,
                            zero,
                        });
                    } else {
                        codes.extend((code.norm as f32).to_le_bytes());
                        codes.extend(signed.to_le_bytes());
                        codes.extend(code.bits);
                    }
                }
            }
            if fastscan {
                codes = crate::fastscan::encode(&fast_rows, rq.padded);
            }
            Block {
                cluster: Cluster {
                    data: Some(Arc::new(ClusterData {
                        center: rotated[c].clone(),
                        rows: ids,
                        row_crcs,
                        ..Default::default()
                    })),
                    parent: parent_ids[c],
                    codes: 0..codes.len() as u64,
                    vectors: 0..vectors_len,
                    codes_crc: crc32fast::hash(&codes),
                    vectors_crc: vectors_crc.finalize(),
                },
                codes,
            }
        })
        .collect();
    let (clusters, all_codes): (Vec<_>, Vec<_>) =
        blocks.into_iter().map(|b| (b.cluster, b.codes)).unzip();
    let mut metadata = Metadata {
        dims: b.dims,
        count: rows.len() as u32,
        metric: b.metric,
        dtype: b.dtype,
        seed: b.params.seed,
        flat,
        flags: FLAG_ROTATED | FLAG_COMPACT | if fastscan { FLAG_FASTSCAN } else { 0 },
        parents: parents.iter().map(|p| rotated_center(&rq, p)).collect(),
        clusters: clusters.into(),
    };
    let meta_len = encode_metadata(&metadata)?.len() as u64;
    // All codes first (probed clusters sit close together, so reads coalesce), then all vectors.
    let mut offset = meta_len;
    for cluster in &mut metadata.clusters {
        let codes_len = cluster.codes.end;
        cluster.codes = offset..offset + codes_len;
        offset += codes_len;
    }
    for cluster in &mut metadata.clusters {
        let vectors_len = cluster.vectors.end;
        cluster.vectors = offset..offset + vectors_len;
        offset += vectors_len;
    }
    let mut blob = Vec::with_capacity(offset as usize + FOOTER_SIZE as usize);
    blob.extend(encode_metadata(&metadata)?);
    let meta_crc = crc32fast::hash(&blob);
    for codes in all_codes {
        blob.extend(codes);
    }
    for group in &groups {
        for &i in group {
            blob.extend_from_slice(rows.raw(i as usize));
        }
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

fn rotated_center(rq: &RaBitQ, c: &[f32]) -> Vec<f32> {
    rq.rotate(c).into_iter().map(|x| x as f32).collect()
}

pub(crate) fn encode_metadata(m: &Metadata) -> Result<Vec<u8>> {
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
    put_u32(&mut out, m.flags);
    for p in &m.parents {
        for x in p {
            out.extend(x.to_le_bytes());
        }
    }
    for c in &m.clusters {
        for x in c.center.iter() {
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
        for crc in c.row_crcs.iter() {
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
    if bytes.len() != FOOTER_SIZE as usize
        || (&bytes[..8] != MAGIC && &bytes[..8] != ANN_FOOTER_MAGIC)
    {
        return Err(bad("bad footer magic or size"));
    }
    let mut p = Parser::new(&bytes[8..]);
    if p.u32()?
        != if &bytes[..8] == ANN_FOOTER_MAGIC {
            ANN_FORMAT_VERSION
        } else {
            FORMAT_VERSION
        }
    {
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
    let flags = p.u32()?;
    if nclusters == 0
        || nclusters > 65_536
        || nparents > nclusters
        || (flat && (nclusters != 1 || nparents != 0))
        || flags & !(FLAG_ROTATED | FLAG_COMPACT | FLAG_FASTSCAN) != 0
        || (flags & FLAG_FASTSCAN != 0 && (flags & FLAG_COMPACT == 0 || flat))
    {
        return Err(bad("invalid centroid directory"));
    }
    let compact = flags & FLAG_COMPACT != 0;
    let center_dims = if flags & FLAG_ROTATED != 0 {
        dims.next_power_of_two()
    } else {
        dims
    };
    // Check minimum size before allocating based on untrusted counts.
    let min_size = nclusters
        .checked_mul(center_dims * 4 + 52)
        .and_then(|x| x.checked_add(nparents * center_dims * 4))
        .ok_or_else(|| bad("metadata size overflow"))?;
    if min_size > p.remaining() {
        return Err(bad("truncated centroid directory"));
    }
    let parents = (0..nparents)
        .map(|_| p.vector(center_dims))
        .collect::<Result<Vec<_>>>()?;
    let mut clusters = Vec::with_capacity(nclusters);
    let mut all_rows = RoaringBitmap::new();
    let meta_end = bytes.len() as u64;
    let mut total = 0u64;
    let mut offset = meta_end;
    for _ in 0..nclusters {
        let center = p.vector(center_dims)?;
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
        } else if flags & FLAG_FASTSCAN != 0 {
            crate::fastscan::block_len(n, dims.next_power_of_two()) as u64
        } else {
            n as u64
                * ((if compact { 8 } else { 24 }) + dims.next_power_of_two().div_ceil(8)) as u64
        };
        let vector_len = n as u64 * dims as u64 * dtype.width() as u64;
        let in_payload = |r: &Range<u64>| r.start >= meta_end && r.end <= payload_end;
        if codes.end.checked_sub(codes.start) != Some(code_len)
            || vectors.end.checked_sub(vectors.start) != Some(vector_len)
            || !in_payload(&codes)
            || !in_payload(&vectors)
            || (!compact && (codes.start != offset || vectors.start != codes.end))
        {
            return Err(bad("invalid cluster ranges"));
        }
        offset = vectors.end;
        total += code_len + vector_len;
        clusters.push(Cluster {
            data: Some(Arc::new(ClusterData {
                center,
                rows,
                row_crcs,
                ..Default::default()
            })),
            parent,
            codes,
            vectors,
            codes_crc,
            vectors_crc,
        });
    }
    if p.remaining() != 0
        || (!compact && offset != payload_end)
        || meta_end + total != payload_end
        || all_rows.len() != count as u64
    {
        return Err(bad("inconsistent index lengths or row count"));
    }
    Ok(Metadata {
        dims,
        count,
        metric,
        dtype,
        seed,
        flat,
        flags,
        parents,
        clusters: clusters.into(),
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
