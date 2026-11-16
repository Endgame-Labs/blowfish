//! Namespace-level ANN directory: one immutable object listing every leaf cluster of every
//! segment, so a query can rank clusters and address their bytes without opening any segment.
//!
//! Each segment contributes a *fragment* (an object written once, when the segment is built):
//! a resident part with one entry per non-empty leaf cluster, followed by a blob area with one
//! blob per cluster (exact rotated center, row set, row checksums) that a query reads only for
//! the clusters it probes. The directory is the concatenation of the fragments' resident parts,
//! so publishing a new segment set never reopens an existing segment: unchanged sections are
//! carried over from the previous directory, and only new fragments are read.
//!
//! A cluster's center is stored as a RaBitQ one-bit code of its offset from the segment's mean
//! center (`centroid`), with the offset's norm and the code's inner-product correction. That is
//! `padded / 8 + 8` bytes per cluster instead of `padded` for scalar quantization. The code only
//! ranks clusters; scanning uses the exact center from the blob.
use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use rayon::prelude::*;

use crate::{
    Metric, RaBitQ, RangeRead, Result, VectorType,
    cluster::prepare,
    format::{Parser, bad, check_crc},
    index::{Index, StubCluster},
};

const DIR_MAGIC: &[u8; 8] = b"BFDIR002";
const FRAG_MAGIC: &[u8; 8] = b"BFFRG001";

/// Vector-space parameters shared by every segment in a directory.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Shape {
    pub dims: usize,
    pub metric: Metric,
    pub dtype: VectorType,
    pub seed: u64,
    pub flat: bool,
}

/// One segment listed in a directory.
#[derive(Clone, Debug)]
pub struct DirSegment {
    /// Segment ID.
    pub id: String,
    /// Object key of the segment's `.bfv` index, to detect a stale directory.
    pub key: String,
    /// Object key of the segment's fragment (holds the cluster blobs).
    pub fragment: String,
    /// Whether the segment is a flat (exact, unclustered) index.
    pub flat: bool,
    /// Leaf cluster count in the segment.
    pub clusters: usize,
}

/// One leaf cluster.
#[derive(Clone, Debug)]
pub struct DirEntry {
    /// Index into [`Directory::segments`].
    pub segment: usize,
    /// Cluster ID local to the segment.
    pub cluster: usize,
    /// Rows in the cluster when the directory was built.
    pub rows: u32,
    codes: Range<u64>,
    vectors: Range<u64>,
    codes_crc: u32,
    vectors_crc: u32,
    blob: Range<u64>,
    norm: f32,
    alpha: f32,
}
impl DirEntry {
    /// Range of this cluster's blob inside its segment's fragment object.
    pub fn blob_range(&self) -> Range<u64> {
        self.blob.clone()
    }
    /// Range of this cluster's full-precision vectors inside its segment's index object.
    pub fn vectors_range(&self) -> Range<u64> {
        self.vectors.clone()
    }
}

/// One segment's clusters: what a fragment contributes to a directory.
#[derive(Debug)]
struct Section {
    shape: Shape,
    /// Cluster count of the segment, including empty clusters.
    clusters: usize,
    centroid: Vec<f32>,
    entries: Vec<DirEntry>,
    /// One-bit codes, `padded / 8` bytes per entry.
    bits: Vec<u8>,
}

/// A parsed directory. Cheap to share: entries and center codes stay resident.
pub struct Directory {
    shape: Shape,
    segments: Vec<DirSegment>,
    sections: Vec<Arc<Section>>,
    entries: Vec<DirEntry>,
    rq: RaBitQ,
    /// Bytes of the resident metadata part (the whole object).
    pub metadata_len: u64,
}

/// A parsed segment fragment, ready to be placed in a directory.
pub struct Fragment(Arc<Section>);

fn put_u32(out: &mut Vec<u8>, x: u32) {
    out.extend(x.to_le_bytes());
}
fn put_u64(out: &mut Vec<u8>, x: u64) {
    out.extend(x.to_le_bytes());
}
fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend(s.as_bytes());
}
fn text(p: &mut Parser<'_>) -> Result<String> {
    let n = p.u32()? as usize;
    String::from_utf8(p.take(n)?.to_vec()).map_err(|_| bad("invalid directory text"))
}
fn put_shape(out: &mut Vec<u8>, s: &Shape) {
    put_u32(out, s.dims as u32);
    put_u32(out, u32::from(s.metric != Metric::CosineDistance));
    put_u32(
        out,
        match s.dtype {
            VectorType::F32 => 0,
            VectorType::F16 => 1,
            VectorType::I8 => 2,
        },
    );
    put_u64(out, s.seed);
    put_u32(out, u32::from(s.flat));
}
fn get_shape(p: &mut Parser<'_>) -> Result<Shape> {
    let dims = p.u32()? as usize;
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
    let flat = p.u32()? != 0;
    if dims == 0 || dims > 65_536 {
        return Err(bad("invalid directory header"));
    }
    Ok(Shape {
        dims,
        metric,
        dtype,
        seed,
        flat,
    })
}

/// Bytes of one entry in the table: cluster, rows, two ranges, two checksums, blob start and
/// length, norm, and alpha.
const ENTRY_BYTES: usize = 4 + 4 + 8 * 4 + 4 + 4 + 8 + 4 + 4 + 4;

impl Section {
    fn put(&self, out: &mut Vec<u8>) {
        put_shape(out, &self.shape);
        put_u32(out, self.clusters as u32);
        put_u32(out, self.entries.len() as u32);
        for x in &self.centroid {
            out.extend(x.to_le_bytes());
        }
        for e in &self.entries {
            put_u32(out, e.cluster as u32);
            put_u32(out, e.rows);
            for v in [e.codes.start, e.codes.end, e.vectors.start, e.vectors.end] {
                put_u64(out, v);
            }
            put_u32(out, e.codes_crc);
            put_u32(out, e.vectors_crc);
            put_u64(out, e.blob.start);
            put_u32(out, (e.blob.end - e.blob.start) as u32);
            out.extend(e.norm.to_le_bytes());
            out.extend(e.alpha.to_le_bytes());
        }
        out.extend(&self.bits);
    }

    fn get(p: &mut Parser<'_>, segment: usize) -> Result<Self> {
        let shape = get_shape(p)?;
        let padded = shape.dims.next_power_of_two();
        let clusters = p.u32()? as usize;
        let n = p.u32()? as usize;
        if n > clusters || n > p.remaining() {
            return Err(bad("invalid directory section"));
        }
        let centroid = p.vector(padded)?;
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
        Ok(Self {
            shape,
            clusters,
            centroid,
            entries,
            bits,
        })
    }
}

fn bit_bytes(padded: usize) -> usize {
    padded.div_ceil(8)
}

impl Fragment {
    /// Build a segment's fragment from its complete `.bfv` bytes. Returns the fragment object
    /// bytes and the length of its resident part.
    pub fn build(index_blob: &[u8]) -> Result<(Vec<u8>, u64)> {
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
        let mut centroid = vec![0f32; padded];
        for &c in &live {
            for (g, x) in centroid.iter_mut().zip(&m.clusters[c].center) {
                *g += *x;
            }
        }
        for g in &mut centroid {
            *g /= live.len().max(1) as f32;
        }
        let root = (padded as f64).sqrt();
        let mut bits = Vec::with_capacity(live.len() * bit_bytes(padded));
        let mut entries = Vec::with_capacity(live.len());
        let mut blobs = Vec::new();
        let resident_len =
            8 + 24 + 8 + padded * 4 + live.len() * (ENTRY_BYTES + bit_bytes(padded)) + 4;
        for &cid in &live {
            let c = &m.clusters[cid];
            let r: Vec<f64> = c
                .center
                .iter()
                .zip(&centroid)
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
            let blob = index.cluster_blob(cid);
            let start = (resident_len + blobs.len()) as u64;
            entries.push(DirEntry {
                segment: 0,
                cluster: cid,
                rows: c.rows.len() as u32,
                codes: c.codes.clone(),
                vectors: c.vectors.clone(),
                codes_crc: c.codes_crc,
                vectors_crc: c.vectors_crc,
                blob: start..start + blob.len() as u64,
                norm: norm as f32,
                alpha: alpha.clamp(1e-6, 1.) as f32,
            });
            blobs.extend(blob);
        }
        let section = Section {
            shape,
            clusters: m.clusters.len(),
            centroid,
            entries,
            bits,
        };
        let mut out = FRAG_MAGIC.to_vec();
        section.put(&mut out);
        let crc = crc32fast::hash(&out);
        put_u32(&mut out, crc);
        debug_assert_eq!(out.len(), resident_len);
        out.extend(blobs);
        Ok((out, resident_len as u64))
    }

    /// Decode a fragment's resident part (the first `metadata_len` bytes of its object).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(bad("truncated fragment"));
        }
        let (body, tail) = bytes.split_at(bytes.len() - 4);
        check_crc(body, u32::from_le_bytes(tail.try_into().unwrap()))?;
        let mut p = Parser::new(body);
        if p.take(8)? != FRAG_MAGIC {
            return Err(bad("bad fragment magic"));
        }
        let section = Section::get(&mut p, 0)?;
        if p.remaining() != 0 {
            return Err(bad("inconsistent fragment length"));
        }
        Ok(Self(Arc::new(section)))
    }
}

impl Directory {
    /// Build a directory object over `(segment id, index key, fragment key, fragment)` for each
    /// segment. Segments carried over from a previous directory pass the same `Fragment`
    /// (see [`Directory::fragment_of`]). Returns the object bytes, or `None` when the segments
    /// do not share one vector space (they cannot be ranked together).
    pub fn build(sources: &[(String, String, String, Fragment)]) -> Result<Option<Vec<u8>>> {
        let Some((_, _, _, first)) = sources.first() else {
            return Ok(None);
        };
        let s = first.0.shape;
        if sources.iter().any(|(_, _, _, f)| {
            let t = f.0.shape;
            (t.dims, t.metric, t.dtype, t.seed) != (s.dims, s.metric, s.dtype, s.seed)
        }) {
            return Ok(None);
        }
        let mut out = DIR_MAGIC.to_vec();
        put_u32(&mut out, sources.len() as u32);
        for (id, key, frag, f) in sources {
            put_str(&mut out, id);
            put_str(&mut out, key);
            put_str(&mut out, frag);
            f.0.put(&mut out);
        }
        let crc = crc32fast::hash(&out);
        put_u32(&mut out, crc);
        Ok(Some(out))
    }

    /// The fragment of the directory's `segment`, to carry over into the next directory.
    pub fn fragment_of(&self, segment: usize) -> Fragment {
        Fragment(self.sections[segment].clone())
    }

    /// Decode a directory object (read whole; it is all resident metadata).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(bad("truncated directory"));
        }
        let (body, tail) = bytes.split_at(bytes.len() - 4);
        check_crc(body, u32::from_le_bytes(tail.try_into().unwrap()))?;
        let mut p = Parser::new(body);
        if p.take(8)? != DIR_MAGIC {
            return Err(bad("bad directory magic"));
        }
        let nsegs = p.u32()? as usize;
        if nsegs > p.remaining() {
            return Err(bad("invalid directory header"));
        }
        let mut segments = Vec::with_capacity(nsegs);
        let mut sections = Vec::with_capacity(nsegs);
        let mut entries = Vec::new();
        for s in 0..nsegs {
            let id = text(&mut p)?;
            let key = text(&mut p)?;
            let fragment = text(&mut p)?;
            let section = Section::get(&mut p, s)?;
            segments.push(DirSegment {
                id,
                key,
                fragment,
                flat: section.shape.flat,
                clusters: section.clusters,
            });
            entries.extend(section.entries.iter().cloned().map(|mut e| {
                e.segment = s;
                e
            }));
            sections.push(Arc::new(section));
        }
        if p.remaining() != 0 {
            return Err(bad("inconsistent directory length"));
        }
        let shape = sections
            .first()
            .map(|s| s.shape)
            .ok_or_else(|| bad("empty directory"))?;
        Ok(Self {
            shape: Shape {
                flat: false,
                ..shape
            },
            segments,
            sections,
            entries,
            rq: RaBitQ::new(shape.dims, shape.seed)?,
            metadata_len: bytes.len() as u64,
        })
    }

    /// Segments in the order entries refer to them.
    pub fn segments(&self) -> &[DirSegment] {
        &self.segments
    }
    /// Every non-empty leaf cluster.
    pub fn entries(&self) -> &[DirEntry] {
        &self.entries
    }
    /// Approximate resident bytes.
    pub fn resident_bytes(&self) -> u64 {
        let bits: usize = self
            .sections
            .iter()
            .map(|s| s.bits.len() + s.centroid.len() * 4)
            .sum();
        (bits
            + self.entries.len() * 2 * std::mem::size_of::<DirEntry>()
            + self.segments.len() * 320) as u64
    }
    /// Vector dimension.
    pub fn dims(&self) -> usize {
        self.shape.dims
    }
    /// Stored bytes per full-precision vector.
    pub fn vector_row_bytes(&self) -> usize {
        self.shape.dims * self.shape.dtype.width()
    }

    /// Estimated squared distance from the query to every entry's center, in entry order,
    /// from the one-bit codes. Comparable across segments because they share one rotation.
    pub fn center_distances(&self, query: &[f32]) -> Result<Vec<f32>> {
        crate::distance::validate(query, self.shape.dims)?;
        let q = self.rq.rotate(&prepare(query, self.shape.metric));
        let padded = q.len();
        let root = (padded as f64).sqrt() as f32;
        let per_segment: Vec<Vec<f32>> = self
            .sections
            .par_iter()
            .map(|s| {
                let qr: Vec<f32> = q
                    .iter()
                    .zip(&s.centroid)
                    .map(|(x, g)| *x as f32 - g)
                    .collect();
                let q2: f32 = qr.iter().map(|x| x * x).sum();
                // lut[j * 256 + v] = sum of qr over the bits set in v, for byte j.
                let width = bit_bytes(padded);
                let mut lut = vec![0f32; width * 256];
                for j in 0..width {
                    let t = &mut lut[j * 256..(j + 1) * 256];
                    for v in 1..256usize {
                        let bit = v.trailing_zeros() as usize;
                        t[v] = t[v & (v - 1)] + qr.get(j * 8 + bit).copied().unwrap_or(0.);
                    }
                }
                let total: f32 = qr.iter().sum();
                s.entries
                    .iter()
                    .enumerate()
                    .map(|(n, e)| {
                        let code = &s.bits[n * width..(n + 1) * width];
                        let set: f32 = code
                            .iter()
                            .enumerate()
                            .map(|(j, b)| lut[j * 256 + *b as usize])
                            .sum();
                        let dot = (2. * set - total) / root;
                        q2 + e.norm * e.norm - 2. * e.norm * dot / e.alpha
                    })
                    .collect()
            })
            .collect();
        Ok(per_segment.into_iter().flatten().collect())
    }

    /// A partial index for `segment`: code and vector ranges of every listed cluster are known,
    /// centers and row sets only after [`PartialIndex::install`].
    pub fn partial_index<R: RangeRead>(
        &self,
        segment: usize,
        reader: R,
    ) -> Result<PartialIndex<R>> {
        let seg = &self.segments[segment];
        let mut stubs: Vec<StubCluster> = (0..seg.clusters)
            .map(|_| StubCluster {
                codes: 0..0,
                vectors: 0..0,
                codes_crc: 0,
                vectors_crc: 0,
            })
            .collect();
        for e in &self.sections[segment].entries {
            stubs[e.cluster] = StubCluster {
                codes: e.codes.clone(),
                vectors: e.vectors.clone(),
                codes_crc: e.codes_crc,
                vectors_crc: e.vectors_crc,
            };
        }
        let shape = Shape {
            flat: seg.flat,
            ..self.shape
        };
        Ok(PartialIndex(Index::stub(reader, &shape, stubs)?))
    }
}

/// A segment index reconstructed from directory entries.
pub struct PartialIndex<R: RangeRead>(Index<R>);
impl<R: RangeRead> PartialIndex<R> {
    /// Install a probed cluster's blob, read from the directory object.
    pub fn install(&mut self, cluster: usize, blob: &Bytes) -> Result<()> {
        self.0.install_cluster(cluster, blob)
    }
    /// The index. Only clusters that were installed may be scanned or reranked.
    pub fn index(&self) -> &Index<R> {
        &self.0
    }
    /// Take the index.
    pub fn into_index(self) -> Index<R> {
        self.0
    }
}
