//! Namespace-level ANN directory: one immutable object listing every leaf cluster of every
//! segment, so a query can rank clusters and address their bytes without opening any segment.
//!
//! Layout: a metadata part (`metadata_len` bytes, read whole and kept resident), followed by a
//! blob area holding one blob per cluster (exact rotated center, row set, row checksums) that a
//! query reads only for the clusters it probes.
//!
//! The resident part stores each center scalar-quantized to `i8` with a per-cluster scale. It
//! only ranks clusters: scanning uses the exact center from the blob.
use std::ops::Range;

use bytes::Bytes;
use rayon::prelude::*;

use crate::{
    Metric, RaBitQ, RangeRead, Result, VectorType,
    cluster::prepare,
    format::{Parser, bad, check_crc},
    index::{Index, StubCluster},
};

const MAGIC: &[u8; 8] = b"BFDIR001";

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
    scale: f32,
}
impl DirEntry {
    /// Range of this cluster's blob inside the directory object.
    pub fn blob_range(&self) -> Range<u64> {
        self.blob.clone()
    }
}

/// A parsed directory. Cheap to share: entries and quantized centers stay resident.
pub struct Directory {
    shape: Shape,
    segments: Vec<DirSegment>,
    entries: Vec<DirEntry>,
    centers: Vec<i8>,
    rq: RaBitQ,
    /// Bytes of the resident metadata part.
    pub metadata_len: u64,
}

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

impl Directory {
    /// Build a directory over `(segment id, object key, index)` triples. Returns the object
    /// bytes and the length of the resident metadata part, or `None` when the segments do not
    /// share one vector space (they cannot be ranked together).
    pub fn build<R: RangeRead>(
        sources: &[(String, String, &Index<R>)],
    ) -> Result<Option<(Vec<u8>, u64)>> {
        let Some((_, _, first)) = sources.first() else {
            return Ok(None);
        };
        let m = first.metadata();
        let (dims, metric, dtype, seed) = (m.dims, m.metric, m.dtype, m.seed);
        let padded = dims.next_power_of_two();
        if sources.iter().any(|(_, _, i)| {
            let m = i.metadata();
            m.dims != dims || m.metric != metric || m.dtype != dtype || m.seed != seed
        }) {
            return Ok(None);
        }
        let entries_total: usize = sources
            .iter()
            .map(|(_, _, i)| {
                i.metadata()
                    .clusters
                    .iter()
                    .filter(|c| !c.rows.is_empty())
                    .count()
            })
            .sum();
        // Entry fields: seg, cluster, rows, 4 u64 ranges, 2 crc, blob start/len, scale.
        let mut head = b"BFDIR001".to_vec();
        for v in [
            dims as u32,
            u32::from(metric != Metric::CosineDistance),
            match dtype {
                VectorType::F32 => 0,
                VectorType::F16 => 1,
                VectorType::I8 => 2,
            },
        ] {
            put_u32(&mut head, v);
        }
        put_u64(&mut head, seed);
        put_u32(&mut head, sources.len() as u32);
        put_u32(&mut head, entries_total as u32);
        for (id, key, index) in sources {
            put_str(&mut head, id);
            put_str(&mut head, key);
            put_u32(&mut head, u32::from(index.metadata().flat));
            put_u32(&mut head, index.metadata().clusters.len() as u32);
        }
        let entry_bytes = 4 + 4 + 4 + 8 * 4 + 4 + 4 + 8 + 4 + 4;
        let meta_len = (head.len() + entries_total * (entry_bytes + padded) + 4) as u64;
        let mut table = Vec::with_capacity(entries_total * entry_bytes);
        let mut quant = Vec::with_capacity(entries_total * padded);
        let mut blobs = Vec::new();
        for (s, (_, _, index)) in sources.iter().enumerate() {
            let md = index.metadata();
            for (cid, c) in md.clusters.iter().enumerate() {
                if c.rows.is_empty() {
                    continue;
                }
                let blob = index.cluster_blob(cid);
                let start = meta_len + blobs.len() as u64;
                let max = c.center.iter().fold(0f32, |a, x| a.max(x.abs()));
                let scale = if max > 0. { max / 127. } else { 1. };
                quant.extend(
                    c.center
                        .iter()
                        .map(|x| (x / scale).round().clamp(-127., 127.) as i8),
                );
                put_u32(&mut table, s as u32);
                put_u32(&mut table, cid as u32);
                put_u32(&mut table, c.rows.len() as u32);
                for v in [c.codes.start, c.codes.end, c.vectors.start, c.vectors.end] {
                    put_u64(&mut table, v);
                }
                put_u32(&mut table, c.codes_crc);
                put_u32(&mut table, c.vectors_crc);
                put_u64(&mut table, start);
                put_u32(&mut table, blob.len() as u32);
                table.extend(scale.to_le_bytes());
                blobs.extend(blob);
            }
        }
        let mut out = head;
        out.extend(table);
        out.extend(quant.iter().map(|x| *x as u8));
        let crc = crc32fast::hash(&out);
        put_u32(&mut out, crc);
        debug_assert_eq!(out.len() as u64, meta_len);
        out.extend(blobs);
        Ok(Some((out, meta_len)))
    }

    /// Decode the resident metadata part (`bytes` is the first `metadata_len` bytes).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(bad("truncated directory"));
        }
        let (body, tail) = bytes.split_at(bytes.len() - 4);
        check_crc(body, u32::from_le_bytes(tail.try_into().unwrap()))?;
        let mut p = Parser::new(body);
        if p.take(8)? != MAGIC {
            return Err(bad("bad directory magic"));
        }
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
        let nsegs = p.u32()? as usize;
        let nentries = p.u32()? as usize;
        if dims == 0 || dims > 65_536 || nsegs > p.remaining() || nentries > p.remaining() {
            return Err(bad("invalid directory header"));
        }
        let padded = dims.next_power_of_two();
        let segments = (0..nsegs)
            .map(|_| {
                Ok(DirSegment {
                    id: text(&mut p)?,
                    key: text(&mut p)?,
                    flat: p.u32()? != 0,
                    clusters: p.u32()? as usize,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let entries = (0..nentries)
            .map(|_| {
                let segment = p.u32()? as usize;
                let cluster = p.u32()? as usize;
                let rows = p.u32()?;
                let codes = p.u64()?..p.u64()?;
                let vectors = p.u64()?..p.u64()?;
                let codes_crc = p.u32()?;
                let vectors_crc = p.u32()?;
                let start = p.u64()?;
                let blob = start..start + p.u32()? as u64;
                let scale = f32::from_le_bytes(p.take(4)?.try_into().unwrap());
                if segment >= segments.len() || cluster >= segments[segment].clusters {
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
                    scale,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if p.remaining() != nentries * padded {
            return Err(bad("inconsistent directory length"));
        }
        let centers: Vec<i8> = p
            .take(nentries * padded)?
            .iter()
            .map(|x| *x as i8)
            .collect();
        Ok(Self {
            shape: Shape {
                dims,
                metric,
                dtype,
                seed,
                flat: false,
            },
            segments,
            entries,
            centers,
            rq: RaBitQ::new(dims, seed)?,
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
        (self.centers.len() + self.entries.len() * 128 + self.segments.len() * 160) as u64
    }
    /// Vector dimension.
    pub fn dims(&self) -> usize {
        self.shape.dims
    }
    /// Stored bytes per full-precision vector.
    pub fn vector_row_bytes(&self) -> usize {
        self.shape.dims * self.shape.dtype.width()
    }

    /// Squared distance from the query to every entry's (quantized) center, in entry order.
    /// Comparable across segments because they share one rotation.
    pub fn center_distances(&self, query: &[f32]) -> Result<Vec<f32>> {
        crate::distance::validate(query, self.shape.dims)?;
        let q: Vec<f32> = self
            .rq
            .rotate(&prepare(query, self.shape.metric))
            .into_iter()
            .map(|x| x as f32)
            .collect();
        let padded = q.len();
        Ok(self
            .entries
            .par_iter()
            .zip(self.centers.par_chunks(padded))
            .map(|(e, c)| {
                let mut sum = 0f32;
                for (q, c) in q.iter().zip(c) {
                    let d = q - *c as f32 * e.scale;
                    sum += d * d;
                }
                sum
            })
            .collect())
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
        for e in self.entries.iter().filter(|e| e.segment == segment) {
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
