use std::ops::Range;

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::{
    Error, Metric, RaBitQ, Result, VectorType,
    cluster::prepare,
    distance::{sort_truncate, validate},
    format::{self, FOOTER_SIZE, Metadata, Parser, bad, check_crc},
    reader::RangeRead,
};

/// Search effort controls. More probes improve cluster coverage; more reranking
/// improves quantization recall. Neither is a universal recall guarantee.
#[derive(Clone, Copy, Debug)]
pub struct SearchParams {
    /// Minimum leaf clusters to probe. Capped at the available cluster count,
    /// and increased automatically under selective filters.
    pub nprobe: usize,
    /// Rerank at least `k * rerank_factor` estimated nearest candidates.
    pub rerank_factor: usize,
}
impl Default for SearchParams {
    fn default() -> Self {
        Self {
            nprobe: 128,
            rerank_factor: 64,
        }
    }
}

/// A parsed immutable index. Metadata and centroids stay resident; codes and
/// vectors are fetched through the reader for each search (usually cache-backed).
pub struct Index<R: RangeRead> {
    reader: R,
    metadata: Metadata,
    rq: RaBitQ,
}
impl<R: RangeRead> Index<R> {
    /// Open an index with two metadata reads: fixed footer, then directory.
    /// To combine those into one parallel batch, persist [`crate::metadata_len`]
    /// in the segment manifest, and use [`Self::open_with_metadata_len`].
    pub async fn open(reader: R) -> Result<Self> {
        let len = reader.len();
        if len < FOOTER_SIZE {
            return Err(bad("missing footer"));
        }
        let footer = read_one(&reader, len - FOOTER_SIZE..len).await?;
        let (meta_len, crc) = format::decode_footer(&footer, len)?;
        let bytes = read_one(&reader, 0..meta_len).await?;
        Self::from_metadata(reader, &bytes, crc, len)
    }
    /// Open footer and front metadata in one parallel range-read batch, using
    /// the metadata length recorded when building or publishing the segment.
    pub async fn open_with_metadata_len(reader: R, meta_len: u64) -> Result<Self> {
        let len = reader.len();
        if len < FOOTER_SIZE || meta_len > len - FOOTER_SIZE {
            return Err(bad("invalid metadata range"));
        }
        let ranges = vec![len - FOOTER_SIZE..len, 0..meta_len];
        let bytes = read_batch(&reader, ranges).await?;
        let (actual, crc) = format::decode_footer(&bytes[0], len)?;
        if actual != meta_len {
            return Err(bad("manifest metadata length disagrees with footer"));
        }
        Self::from_metadata(reader, &bytes[1], crc, len)
    }
    fn from_metadata(reader: R, bytes: &[u8], crc: u32, len: u64) -> Result<Self> {
        let metadata = format::decode_metadata(bytes, crc, len - FOOTER_SIZE)?;
        let rq = RaBitQ::new(metadata.dims, metadata.seed)?;
        Ok(Self {
            reader,
            metadata,
            rq,
        })
    }
    /// Vector dimension.
    pub fn dims(&self) -> usize {
        self.metadata.dims
    }
    /// Indexed row count.
    pub fn len(&self) -> usize {
        self.metadata.count as usize
    }
    /// Whether the index contains no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Full-precision column representation.
    pub fn vector_type(&self) -> VectorType {
        self.metadata.dtype
    }
    /// Distance metric used by the index.
    pub fn metric(&self) -> Metric {
        self.metadata.metric
    }
    /// Number of IVF leaf clusters (one in flat mode).
    pub fn cluster_count(&self) -> usize {
        self.metadata.clusters.len()
    }
    /// Number of top-level centroids; zero for a single-level index.
    pub fn parent_count(&self) -> usize {
        self.metadata.parents.len()
    }
    /// Whether searches use an exact flat scan.
    pub fn is_flat(&self) -> bool {
        self.metadata.flat
    }

    /// ANN top-k, sorted by exact stored-vector distance, then row ID.
    /// A filter is an allowlist of segment row IDs. Unknown IDs are ignored.
    /// Sets of at most 2,048 matching rows use exact range reads. Larger filtered
    /// searches skip empty clusters, and increase probing to collect candidates.
    pub async fn search(
        &self,
        query: &[f32],
        k: usize,
        params: SearchParams,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(u32, f32)>> {
        validate(query, self.dims())?;
        if params.nprobe == 0 || params.rerank_factor == 0 {
            return Err(Error::InvalidInput(
                "nprobe and rerank_factor must be positive".into(),
            ));
        }
        if k == 0 || self.is_empty() {
            return Ok(vec![]);
        }
        let counts: Vec<usize> = self
            .metadata
            .clusters
            .iter()
            .map(|c| filter.map_or(c.rows.len(), |f| c.rows.intersection_len(f)) as usize)
            .collect();
        let matching: usize = counts.iter().sum();
        if self.is_flat() || matching <= 2048 || matching <= k {
            return self.exact_search(query, k, filter).await;
        }
        let target = k.saturating_mul(params.rerank_factor).min(matching);
        let probes = params
            .nprobe
            .saturating_mul(self.len())
            .div_ceil(matching)
            .min(self.cluster_count());
        let q = prepare(query, self.metric());
        let order = self.cluster_order(&q, probes);
        let mut chosen = Vec::new();
        let mut candidates = 0;
        for c in order {
            if counts[c] == 0 {
                continue;
            }
            chosen.push(c);
            candidates += counts[c];
            if chosen.len() >= probes && candidates >= target {
                break;
            }
        }
        let ranges = chosen
            .iter()
            .map(|c| self.metadata.clusters[*c].codes.clone())
            .collect();
        let blocks = read_batch(&self.reader, ranges).await?;
        let q_zero = q.iter().all(|x| *x == 0.);
        let mut estimated = Vec::with_capacity(candidates);
        for (cid, bytes) in chosen.into_iter().zip(blocks) {
            let c = &self.metadata.clusters[cid];
            check_crc(&bytes, c.codes_crc)?;
            let rotated = self.rq.residual(&q, &c.center);
            let prepared = crate::rabitq::PreparedQuery::new(&rotated);
            let mut p = Parser::new(&bytes);
            for (ordinal, row) in c.rows.iter().enumerate() {
                let norm = p.f64()?;
                let alpha = p.f64()?;
                let zero = p.u64()?;
                let bits = p.take(self.rq.padded.div_ceil(8))?;
                if !norm.is_finite()
                    || norm < 0.
                    || !alpha.is_finite()
                    || alpha <= 0.
                    || alpha > 1.
                    || zero > 1
                {
                    return Err(bad("invalid quantization factors"));
                }
                if filter.is_some_and(|f| !f.contains(row)) {
                    continue;
                }
                let e = prepared.estimate(norm, alpha, bits, self.rq.padded);
                let distance = if self.metric() == Metric::CosineDistance {
                    if q_zero || zero == 1 {
                        if q_zero && zero == 1 { 0. } else { 1. }
                    } else {
                        (e.distance * 0.5).clamp(0., 2.)
                    }
                } else {
                    e.distance.max(0.)
                };
                estimated.push((row, distance as f32, cid, ordinal));
            }
        }
        let cmp = |a: &(u32, f32, usize, usize), b: &(u32, f32, usize, usize)| {
            a.1.total_cmp(&b.1).then(a.0.cmp(&b.0))
        };
        if estimated.len() > target {
            estimated.select_nth_unstable_by(target, cmp);
            estimated.truncate(target);
        }
        let locations = estimated.into_iter().map(|(_, _, c, i)| (c, i)).collect();
        self.rerank(query, k, locations).await
    }

    /// Exact top-k over stored vectors, using bitmap membership to fetch only
    /// matching rows. Adjacent row ranges coalesce; dense selections read the
    /// whole cluster when at least one third of its vectors are needed.
    pub async fn exact_search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<(u32, f32)>> {
        validate(query, self.dims())?;
        if k == 0 {
            return Ok(vec![]);
        }
        let mut locations = Vec::new();
        for (cid, c) in self.metadata.clusters.iter().enumerate() {
            if let Some(filter) = filter {
                for row in (&c.rows & filter).iter() {
                    locations.push((cid, c.rows.rank(row) as usize - 1));
                }
            } else {
                locations.extend((0..c.rows.len() as usize).map(|i| (cid, i)));
            }
        }
        self.rerank(query, k, locations).await
    }

    fn cluster_order(&self, q: &[f32], nprobe: usize) -> Vec<usize> {
        let mut parents: Vec<_> = (0..self.metadata.parents.len()).collect();
        parents.sort_unstable_by(|a, b| {
            Metric::EuclideanSquared
                .distance_unchecked(q, &self.metadata.parents[*a])
                .total_cmp(
                    &Metric::EuclideanSquared.distance_unchecked(q, &self.metadata.parents[*b]),
                )
                .then(a.cmp(b))
        });
        // Probe a wide parent beam, then sort its children. Remaining parents
        // remain available for adaptive widening under filters.
        let beam = if parents.is_empty() {
            0
        } else {
            ((nprobe.saturating_mul(4)).div_ceil(self.cluster_count().div_ceil(parents.len())))
                .max(4)
                .min(parents.len())
        };
        let mut preferred = vec![false; parents.len()];
        for p in parents.into_iter().take(beam) {
            preferred[p] = true;
        }
        let mut order: Vec<_> = (0..self.cluster_count()).collect();
        let distances: Vec<_> = self
            .metadata
            .clusters
            .iter()
            .map(|c| Metric::EuclideanSquared.distance_unchecked(q, &c.center))
            .collect();
        order.sort_unstable_by(|a, b| {
            let ca = &self.metadata.clusters[*a];
            let cb = &self.metadata.clusters[*b];
            let pa = preferred.is_empty() || preferred[ca.parent as usize];
            let pb = preferred.is_empty() || preferred[cb.parent as usize];
            pb.cmp(&pa)
                .then(distances[*a].total_cmp(&distances[*b]))
                .then(a.cmp(b))
        });
        order
    }

    async fn rerank(
        &self,
        query: &[f32],
        k: usize,
        mut locations: Vec<(usize, usize)>,
    ) -> Result<Vec<(u32, f32)>> {
        locations.sort_unstable();
        locations.dedup();
        let width = self.dims() * self.vector_type().width();
        let mut plans = Vec::<(usize, usize, usize, bool)>::new();
        let mut pos = 0;
        while pos < locations.len() {
            let cid = locations[pos].0;
            let mut end = pos + 1;
            while end < locations.len() && locations[end].0 == cid {
                end += 1;
            }
            let cluster = &self.metadata.clusters[cid];
            if (end - pos) * 3 >= cluster.rows.len() as usize {
                plans.push((cid, 0, cluster.rows.len() as usize, true));
            } else {
                for (_, i) in &locations[pos..end] {
                    if let Some(last) = plans.last_mut()
                        && last.0 == cid
                        && last.2 == *i
                    {
                        last.2 += 1;
                    } else {
                        plans.push((cid, *i, *i + 1, false));
                    }
                }
            }
            pos = end;
        }
        let ranges: Vec<_> = plans
            .iter()
            .map(|(c, start, end, _)| {
                let offset = self.metadata.clusters[*c].vectors.start;
                offset + (*start * width) as u64..offset + (*end * width) as u64
            })
            .collect();
        let blocks = read_batch(&self.reader, ranges).await?;
        let mut result = Vec::with_capacity(locations.len());
        let mut decoded = Vec::with_capacity(self.dims());
        for ((cid, start, end, whole), bytes) in plans.into_iter().zip(blocks) {
            let c = &self.metadata.clusters[cid];
            if whole {
                check_crc(&bytes, c.vectors_crc)?;
            }
            for i in start..end {
                if whole && locations.binary_search(&(cid, i)).is_err() {
                    continue;
                }
                let vbytes = &bytes[(i - start) * width..(i - start + 1) * width];
                check_crc(vbytes, c.row_crcs[i])?;
                decoded.clear();
                match self.vector_type() {
                    VectorType::I8 => decoded.extend(vbytes.iter().map(|x| *x as i8 as f32)),
                    VectorType::F32 => decoded.extend(
                        vbytes
                            .as_chunks::<4>()
                            .0
                            .iter()
                            .map(|x| f32::from_le_bytes(*x)),
                    ),
                    VectorType::F16 => decoded.extend(
                        vbytes
                            .as_chunks::<2>()
                            .0
                            .iter()
                            .map(|x| half::f16::from_le_bytes(*x).to_f32()),
                    ),
                }
                if decoded.iter().any(|x| !x.is_finite()) {
                    return Err(bad("non-finite stored vector"));
                }
                let row = c
                    .rows
                    .select(i as u32)
                    .ok_or_else(|| bad("invalid row ordinal"))?;
                result.push((row, self.metric().distance_unchecked(query, &decoded)));
            }
        }
        sort_truncate(&mut result, k);
        Ok(result)
    }
}

async fn read_one<R: RangeRead>(reader: &R, range: Range<u64>) -> Result<Bytes> {
    let n = range.end - range.start;
    let bytes = reader.read(range).await?;
    if bytes.len() as u64 != n {
        return Err(bad("short range read"));
    }
    Ok(bytes)
}
async fn read_batch<R: RangeRead>(reader: &R, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
    let bytes = reader.read_batch(ranges.clone()).await?;
    if bytes.len() != ranges.len()
        || bytes
            .iter()
            .zip(ranges)
            .any(|(b, r)| b.len() as u64 != r.end - r.start)
    {
        return Err(bad("short or incomplete batch read"));
    }
    Ok(bytes)
}
