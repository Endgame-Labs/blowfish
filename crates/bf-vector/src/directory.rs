//! Namespace-level ANN directory: one immutable object listing every leaf cluster of every
//! segment, so a query can rank clusters and address their bytes without opening any segment.
//!
//! Each segment contributes its ANN object's resident *fragment*: one entry per non-empty
//! leaf cluster. The same object holds the membership map, checksummed leaf blobs (exact
//! rotated center, row set, and row checksums), codes, and vectors. Queries read payloads
//! only for the clusters they probe. The directory is the concatenation of the fragments' resident parts,
//! so publishing a new segment set never reopens an existing segment: unchanged sections are
//! carried over from the previous directory, and only new fragments are read.
//!
//! A cluster's center is stored as a RaBitQ one-bit code of its offset from the segment's mean
//! center (`centroid`), with the offset's norm and the code's inner-product correction. That is
//! `padded / 8 + 8` bytes per cluster instead of `padded` for scalar quantization. The code only
//! ranks clusters; scanning uses the exact center from the blob.
use std::{ops::Range, sync::Arc};

mod paged;
pub use paged::{Page, PagedDirectory};

use bytes::Bytes;
use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::{
    Metric, RaBitQ, RangeRead, Result, VectorType,
    cluster::prepare,
    format::{Parser, bad, check_crc},
    index::{Index, StubCluster},
};

pub(crate) const ANN_MAGIC: &[u8; 8] = b"BFANN002";

const DIR_MAGIC: &[u8; 8] = b"BFDIR004";

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
    /// Object key of the segment's `.bfa` index, to detect a stale directory.
    pub key: String,
    /// The same ANN object key, retained in the directory encoding.
    pub fragment: String,
    /// Whether the segment is a flat (exact, unclustered) index.
    pub flat: bool,
    /// Leaf cluster count in the segment.
    pub clusters: usize,
    /// Rows in the segment's index.
    pub rows: usize,
    /// Byte range of the segment's row-to-cluster map inside its fragment, if it has one.
    pub membership: Option<Range<u64>>,
    /// Whether each cluster's quantized codes sit in the fragment, right after the cluster's
    /// blob. Current ANN objects always set this; older directory readers also record it.
    pub codes_in_fragment: bool,
    /// `(size, metadata length)` of the segment's index object and of its fragment, when the
    /// directory records them (a paged directory does, so a reader can open the objects without
    /// the manifest).
    pub files: Option<[(u64, u64); 2]>,
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
    /// Range of this cluster's quantized codes in the ANN object.
    pub fn codes_range(&self) -> Range<u64> {
        self.codes.clone()
    }
}

/// One segment's clusters: what a fragment contributes to a directory.
#[derive(Debug)]
struct Section {
    shape: Shape,
    /// Cluster count of the segment, including empty clusters.
    clusters: usize,
    centroid: Arc<Vec<f32>>,
    entries: Vec<DirEntry>,
    /// One-bit codes, `padded / 8` bytes per entry.
    bits: Vec<u8>,
    /// Rows in the segment's index.
    rows: u32,
    /// Row-to-cluster map inside the fragment (empty when the segment is flat).
    membership: Range<u64>,
    /// Quantized codes live in the fragment, after each cluster's blob.
    codes_in_fragment: bool,
    /// Every cluster's ranges, by cluster ID, for [`Directory::partial_index`] (built once).
    stubs: std::sync::OnceLock<Vec<StubCluster>>,
}
impl Section {
    fn stubs(&self) -> &[StubCluster] {
        self.stubs.get_or_init(|| {
            let mut stubs = vec![
                StubCluster {
                    codes: 0..0,
                    vectors: 0..0,
                    codes_crc: 0,
                    vectors_crc: 0,
                };
                self.clusters
            ];
            for e in &self.entries {
                stubs[e.cluster] = StubCluster {
                    codes: e.codes.clone(),
                    vectors: e.vectors.clone(),
                    codes_crc: e.codes_crc,
                    vectors_crc: e.vectors_crc,
                };
            }
            stubs
        })
    }
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
    /// Per segment `(rows, clusters)` of the whole namespace, when `entries` lists only part of it
    /// (see [`PagedDirectory::assemble`]).
    totals: Option<Vec<(usize, usize)>>,
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
        put_u32(out, self.rows);
        put_u64(out, self.membership.start);
        put_u64(out, self.membership.end);
        put_u32(out, 1);
        for x in self.centroid.iter() {
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
        let rows = p.u32()?;
        let membership = p.u64()?..p.u64()?;
        if p.u32()? != 1 {
            return Err(bad("invalid directory section flags"));
        }
        let codes_in_fragment = true;
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
        Ok(Self {
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
}

/// Order `live` clusters along a greedy nearest-neighbor tour of their centers, starting at the
/// first. Clusters that a query probes together are near each other, so they land near each
/// other on the tour.
fn tour<'a>(live: &[usize], center: impl Fn(usize) -> &'a [f32] + Sync) -> Vec<usize> {
    let Some(&first) = live.first() else {
        return Vec::new();
    };
    let mut left: Vec<usize> = live[1..].to_vec();
    let mut order = vec![first];
    while !left.is_empty() {
        let last = center(*order.last().expect("tour is not empty"));
        let (at, _) = left
            .par_iter()
            .enumerate()
            .map(|(i, &c)| {
                let d: f32 = center(c)
                    .iter()
                    .zip(last)
                    .map(|(a, b)| (a - b) * (a - b))
                    .sum();
                (i, d)
            })
            .reduce(
                || (usize::MAX, f32::INFINITY),
                |a, b| {
                    if b.1 < a.1 || (b.1 == a.1 && b.0 < a.0) {
                        b
                    } else {
                        a
                    }
                },
            );
        order.push(left.swap_remove(at));
    }
    order
}

fn bit_bytes(padded: usize) -> usize {
    padded.div_ceil(8)
}

impl Fragment {
    /// Build one version-2 ANN object, with resident membership, leaf blobs and codes in tour
    /// order, and one vector region. Existing fast-scan codes and vectors are copied unchanged.
    pub fn build_ann(index_blob: &[u8]) -> Result<(Vec<u8>, u64)> {
        let upgraded = crate::upgrade_layout(index_blob)?;
        let index_blob = upgraded.as_deref().unwrap_or(index_blob);
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
        let resident_len =
            8 + 24 + 8 + 20 + padded * 4 + live.len() * (ENTRY_BYTES + bit_bytes(padded)) + 4 + 4;
        let dense = m
            .clusters
            .iter()
            .flat_map(|c| c.rows.iter())
            .all(|r| r < m.count);
        let map_len = if m.flat || !dense {
            0
        } else {
            m.count as usize
                * if m.clusters.len() > u16::MAX as usize {
                    4
                } else {
                    2
                }
                + 4
        };
        let resident_len = resident_len + map_len;
        // Blocks are laid out in `order`; entries stay in cluster order.
        let order = tour(&live, |c| &*m.clusters[c].center);
        let mut placed: Vec<Option<(u64, usize, u64)>> = vec![None; m.clusters.len()];
        for &cid in &order {
            let c = &m.clusters[cid];
            let blob = index.cluster_blob(cid);
            let start = (resident_len + blobs.len()) as u64;
            let codes_len = c.codes.end - c.codes.start;
            placed[cid] = Some((start, blob.len(), codes_len));
            blobs.extend(blob);
            {
                let codes = index_blob
                    .get(c.codes.start as usize..c.codes.end as usize)
                    .ok_or_else(|| bad("cluster codes outside index"))?;
                blobs.extend_from_slice(codes);
            }
        }
        let mut vector_ranges = vec![0..0; m.clusters.len()];
        let mut vector_len = 0;
        for &cid in &order {
            let c = &m.clusters[cid];
            let start = (resident_len + blobs.len() + vector_len) as u64;
            let bytes = index_blob
                .get(c.vectors.start as usize..c.vectors.end as usize)
                .ok_or_else(|| bad("cluster vectors outside index"))?;
            vector_ranges[cid] = start..start + bytes.len() as u64;
            vector_len += bytes.len();
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
                codes: blob_end..blob_end + codes_len,
                vectors: vector_ranges[cid].clone(),
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
        if !m.flat && dense {
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
        let member_start = (resident_len - map_len - 4) as u64;
        let section = Section {
            shape,
            clusters: m.clusters.len(),
            centroid,
            entries,
            bits,
            rows: m.count,
            membership: member_start..member_start + member.len() as u64,
            codes_in_fragment: true,
            stubs: Default::default(),
        };
        let mut out = Vec::with_capacity(
            resident_len + blobs.len() + vector_len + crate::format::FOOTER_SIZE as usize,
        );
        out.extend_from_slice(ANN_MAGIC);
        section.put(&mut out);
        out.extend(member);
        let crc = crc32fast::hash(&out);
        put_u32(&mut out, crc);
        debug_assert_eq!(out.len(), resident_len);
        out.extend(blobs);
        for &cid in &order {
            let range = &m.clusters[cid].vectors;
            out.extend_from_slice(&index_blob[range.start as usize..range.end as usize]);
        }
        let mut footer = vec![0u8; crate::format::FOOTER_SIZE as usize];
        footer[..8].copy_from_slice(crate::format::ANN_FOOTER_MAGIC);
        footer[8..12].copy_from_slice(&crate::format::ANN_FORMAT_VERSION.to_le_bytes());
        footer[16..24].copy_from_slice(&(resident_len as u64).to_le_bytes());
        footer[24..32]
            .copy_from_slice(&(out.len() as u64 + crate::format::FOOTER_SIZE).to_le_bytes());
        footer[32..36].copy_from_slice(&crc32fast::hash(&out[..resident_len]).to_le_bytes());
        let crc = crc32fast::hash(&footer[..60]);
        footer[60..].copy_from_slice(&crc.to_le_bytes());
        out.extend(footer);
        Ok((out, resident_len as u64))
    }

    pub(crate) fn validate(&self, resident: u64, payload_end: u64) -> Result<()> {
        let width = self.0.shape.dims as u64 * self.0.shape.dtype.width() as u64;
        for e in &self.0.entries {
            if e.blob.start < resident
                || e.blob.end != e.codes.start
                || e.codes.start > e.codes.end
                || e.codes.end > e.vectors.start
                || e.vectors.start > e.vectors.end
                || e.vectors.end > payload_end
                || e.vectors.end - e.vectors.start != u64::from(e.rows) * width
            {
                return Err(bad("invalid ANN payload ranges"));
            }
        }
        Ok(())
    }

    pub(crate) fn index<R: RangeRead>(&self, reader: R) -> Result<Index<R>> {
        Index::stub(
            reader,
            &self.0.shape,
            self.0.stubs(),
            self.0.centroid.clone(),
        )
    }
    pub(crate) fn blobs(&self) -> Vec<(usize, Range<u64>)> {
        self.0
            .entries
            .iter()
            .map(|e| (e.cluster, e.blob.clone()))
            .collect()
    }
    pub(crate) fn rows(&self) -> u32 {
        self.0.rows
    }
    /// Decode a fragment's resident part (the first `metadata_len` bytes of its object).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(bad("truncated fragment"));
        }
        let (body, tail) = bytes.split_at(bytes.len() - 4);
        check_crc(body, u32::from_le_bytes(tail.try_into().unwrap()))?;
        let mut p = Parser::new(body);
        if p.take(8)? != ANN_MAGIC {
            return Err(bad("bad ANN object magic"));
        }
        let section = Section::get(&mut p, 0)?;
        if !section.membership.is_empty() {
            if section.membership.start != (body.len() - p.remaining()) as u64 {
                return Err(bad("invalid resident membership offset"));
            }
            let member = p.take((section.membership.end - section.membership.start) as usize)?;
            Membership::decode(member, section.rows as usize, section.clusters)?;
        }
        if p.remaining() != 0 {
            return Err(bad("inconsistent fragment length"));
        }
        Ok(Self(Arc::new(section)))
    }
}

/// Estimated squared distance from the (rotated) query `q` to each entry of `s`, from the
/// section's one-bit codes. The residual query is quantized to bit planes once per section, so
/// each entry costs a few popcounts per 64 dimensions (512 at a time with AVX-512) rather than
/// a table lookup per byte.
fn section_distances(s: &Section, q: &[f64]) -> Vec<f32> {
    let mut out = Vec::with_capacity(s.entries.len());
    section_distances_into(s, q, &mut out);
    out
}
fn section_distances_into(s: &Section, q: &[f64], out: &mut Vec<f32>) {
    let padded = q.len();
    let plane = crate::rabitq::PlaneQuery::new(q, &s.centroid);
    let width = bit_bytes(padded);
    let start = out.len();
    out.resize(start + s.entries.len(), 0.);
    let distances = &mut out[start..];
    plane.dots(&s.bits, width, s.entries.len(), padded, |n, dot| {
        let e = &s.entries[n];
        let (norm, alpha) = (e.norm as f64, e.alpha as f64);
        distances[n] = (plane.norm2 + norm * norm - 2. * norm * dot / alpha) as f32;
    });
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
        if sources.iter().any(|(_, _, _, f)| !f.0.codes_in_fragment) {
            return Err(bad("upgrade legacy fragments before directory publication"));
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
                rows: section.rows as usize,
                membership: (!section.membership.is_empty()).then(|| section.membership.clone()),
                codes_in_fragment: section.codes_in_fragment,
                files: None,
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
        // Computed once here rather than by every query's plan.
        let mut totals = vec![(0usize, 0usize); segments.len()];
        for e in &entries {
            totals[e.segment].0 += e.rows as usize;
            totals[e.segment].1 += 1;
        }
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
            totals: Some(totals),
        })
    }

    /// Per segment, the rows and the non-empty clusters of the whole namespace. For a directory
    /// assembled from pages this is more than `entries()` lists.
    pub fn segment_totals(&self) -> Vec<(usize, usize)> {
        if let Some(t) = &self.totals {
            return t.clone();
        }
        let mut t = vec![(0usize, 0usize); self.segments.len()];
        for e in &self.entries {
            t[e.segment].0 += e.rows as usize;
            t[e.segment].1 += 1;
        }
        t
    }

    /// Segments in the order entries refer to them.
    pub fn segments(&self) -> &[DirSegment] {
        &self.segments
    }
    /// The entries of one segment, in `entries()` order.
    pub fn segment_entries(&self, segment: usize) -> &[DirEntry] {
        &self.sections[segment].entries
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
        self.center_distances_on(query, true)
    }
    /// The query normalized for the metric and rotated, for [`Self::center_distances_rotated`]
    /// and for scanning any of the directory's segments
    /// ([`crate::Index::scan_blocks_bounded_rotated`]).
    pub fn rotate_query(&self, query: &[f32]) -> Result<crate::RotatedQuery> {
        crate::distance::validate(query, self.shape.dims)?;
        Ok(crate::RotatedQuery::new(&self.rq, query, self.shape.metric))
    }
    /// [`Self::center_distances_on`] for a query from [`Self::rotate_query`].
    pub fn center_distances_rotated(
        &self,
        query: &crate::RotatedQuery,
        parallel: bool,
    ) -> Result<Vec<f32>> {
        let q = &query.rotated;
        if q.len() != self.rq.padded || query.seed != self.shape.seed {
            return Err(crate::Error::InvalidInput(
                "query rotated for another directory".into(),
            ));
        }
        let mut out = Vec::with_capacity(self.entries.len());
        if !parallel {
            for s in &self.sections {
                section_distances_into(s, q, &mut out);
            }
            return Ok(out);
        }
        let per_segment: Vec<Vec<f32>> = self
            .sections
            .par_iter()
            .map(|s| section_distances(s, q))
            .collect();
        for d in per_segment {
            out.extend_from_slice(&d);
        }
        Ok(out)
    }
    /// [`Self::center_distances`], on this thread alone when `parallel` is false (a busy
    /// server gains nothing from fanning one query's ranking out over the rayon pool).
    pub fn center_distances_on(&self, query: &[f32], parallel: bool) -> Result<Vec<f32>> {
        if !parallel {
            crate::distance::validate(query, self.shape.dims)?;
            let q = self.rq.rotate(&prepare(query, self.shape.metric));
            return Ok(self
                .sections
                .iter()
                .flat_map(|s| section_distances(s, &q))
                .collect());
        }
        crate::distance::validate(query, self.shape.dims)?;
        let q = self.rq.rotate(&prepare(query, self.shape.metric));
        let per_segment: Vec<Vec<f32>> = self
            .sections
            .par_iter()
            .map(|s| section_distances(s, &q))
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
        let stubs = self.sections[segment].stubs();
        let shape = Shape {
            flat: seg.flat,
            ..self.shape
        };
        Ok(PartialIndex(Index::stub(
            reader,
            &shape,
            stubs,
            self.sections[segment].centroid.clone(),
        )?))
    }
}

/// A segment index reconstructed from directory entries.
pub struct PartialIndex<R: RangeRead>(Index<R>);
impl<R: RangeRead> PartialIndex<R> {
    /// Install a probed cluster's blob, read from the directory object.
    pub fn install(&mut self, cluster: usize, blob: &Bytes) -> Result<()> {
        self.0.install_cluster(cluster, blob)
    }
    /// Decode a probed cluster's blob, to keep (see [`crate::DecodedCluster`]) and install.
    pub fn decode(&self, blob: &Bytes) -> Result<crate::DecodedCluster> {
        self.0.decode_cluster(blob)
    }
    /// Install a cluster decoded earlier, by this or another partial index of the same segment.
    pub fn install_decoded(
        &mut self,
        cluster: usize,
        decoded: &crate::DecodedCluster,
    ) -> Result<()> {
        self.0.install_decoded(cluster, decoded)
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

/// A segment's row-to-cluster map (decoded from the fragment's membership area).
pub struct Membership {
    cluster_of: Vec<u32>,
    /// Position of each row among its cluster's rows, in ascending row order.
    ordinal_of: Vec<u32>,
}
impl Membership {
    /// Decode the membership bytes of a segment with `rows` rows and `clusters` clusters.
    pub fn decode(bytes: &[u8], rows: usize, clusters: usize) -> Result<Self> {
        let width = if clusters > u16::MAX as usize { 4 } else { 2 };
        if bytes.len() != rows * width + 4 {
            return Err(bad("invalid membership length"));
        }
        let (body, tail) = bytes.split_at(rows * width);
        check_crc(body, u32::from_le_bytes(tail.try_into().unwrap()))?;
        let cluster_of: Vec<u32> = if width == 2 {
            body.as_chunks::<2>()
                .0
                .iter()
                .map(|c| u32::from(u16::from_le_bytes(*c)))
                .collect()
        } else {
            body.as_chunks::<4>()
                .0
                .iter()
                .map(|c| u32::from_le_bytes(*c))
                .collect()
        };
        if cluster_of.iter().any(|c| *c as usize >= clusters) {
            return Err(bad("membership cluster outside segment"));
        }
        let mut next = vec![0u32; clusters];
        let ordinal_of = cluster_of
            .iter()
            .map(|&c| {
                let n = &mut next[c as usize];
                *n += 1;
                *n - 1
            })
            .collect();
        Ok(Self {
            cluster_of,
            ordinal_of,
        })
    }
    /// Approximate resident bytes.
    pub fn resident_bytes(&self) -> u64 {
        self.cluster_of.len() as u64 * 8
    }
    /// Rows of `allowed` in each of the segment's `clusters` clusters.
    pub fn counts(&self, allowed: &RoaringBitmap, clusters: usize) -> Vec<u32> {
        let mut counts = vec![0u32; clusters];
        for row in allowed {
            if let Some(c) = self.cluster_of.get(row as usize) {
                counts[*c as usize] += 1;
            }
        }
        counts
    }
    /// `(cluster, ordinal within the cluster, row)` of every row of `allowed` whose cluster
    /// passes `keep`. A cluster's ordinals count its rows in ascending row order, the same
    /// order its vectors are stored in.
    pub fn locations(
        &self,
        allowed: &RoaringBitmap,
        keep: impl Fn(u32) -> bool,
    ) -> Vec<(u32, u32, u32)> {
        allowed
            .iter()
            .filter_map(|row| {
                let cluster = *self.cluster_of.get(row as usize)?;
                keep(cluster).then(|| (cluster, self.ordinal_of[row as usize], row))
            })
            .collect()
    }
}
impl<R: RangeRead> PartialIndex<R> {
    /// Exact top-`k` `(row, distance)` over rows given by location (see
    /// [`Membership::locations`]), reading only those vectors. Needs no installed clusters.
    pub async fn exact_at(
        &self,
        query: &[f32],
        k: usize,
        locations: Vec<(u32, u32, u32)>,
    ) -> Result<Vec<(u32, f32)>> {
        self.0.exact_at(query, k, locations).await
    }
}
