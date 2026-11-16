use std::ops::Range;

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::{
    Error, Metric, RaBitQ, Result, VectorType,
    cluster::prepare,
    distance::{sort_truncate, validate},
    format::{self, FOOTER_SIZE, Metadata, bad, check_crc},
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

/// One row's quantized distance estimate, with the location needed to rerank it.
#[derive(Clone, Copy, Debug)]
pub struct Candidate {
    /// Segment-local row ID.
    pub row: u32,
    /// Estimated distance from the query.
    pub distance: f32,
    /// Statistical bound on the estimate's error (about 2 sigma): the exact distance is
    /// within `distance` plus or minus `error` almost always.
    pub error: f32,
    /// Leaf cluster holding the row.
    pub cluster: u32,
    /// Position within the cluster.
    pub ordinal: u32,
}

/// Code blocks closer than this are read together (the gap bytes are fetched and dropped).
const CODE_GAP: u64 = 64 << 10;
const CODE_MERGE_MAX: u64 = 8 << 20;
/// Candidate vectors closer than this are read together.
const VECTOR_GAP: u64 = 64 << 10;
const VECTOR_MERGE_MAX: u64 = 2 << 20;

pub(crate) struct StubCluster {
    pub codes: Range<u64>,
    pub vectors: Range<u64>,
    pub codes_crc: u32,
    pub vectors_crc: u32,
}

/// A parsed immutable index. Metadata and centroids stay resident; codes and
/// vectors are fetched through the reader for each search (usually cache-backed).
pub struct Index<R: RangeRead> {
    reader: R,
    metadata: Metadata,
    pub(crate) rq: RaBitQ,
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
        let mut metadata = format::decode_metadata(bytes, crc, len - FOOTER_SIZE)?;
        let rq = RaBitQ::new(metadata.dims, metadata.seed)?;
        if metadata.flags & format::FLAG_ROTATED == 0 {
            // Older indexes store raw centers. Rotating once at open lets every search
            // rotate the query once instead of once per probed cluster.
            let rot =
                |c: &Vec<f32>| -> Vec<f32> { rq.rotate(c).into_iter().map(|x| x as f32).collect() };
            metadata.parents = metadata.parents.iter().map(rot).collect();
            for c in &mut metadata.clusters {
                c.center = rot(&c.center);
            }
            metadata.flags |= format::FLAG_ROTATED;
        }
        Ok(Self {
            reader,
            metadata,
            rq,
        })
    }
    /// A partial index over a namespace directory: only the code and vector ranges are known
    /// until [`Self::install_cluster`] supplies each probed cluster's center and row set.
    pub(crate) fn stub(
        reader: R,
        shape: &crate::directory::Shape,
        stubs: Vec<StubCluster>,
    ) -> Result<Self> {
        let rq = RaBitQ::new(shape.dims, shape.seed)?;
        let clusters = stubs
            .into_iter()
            .map(|s| format::Cluster {
                center: Vec::new(),
                parent: u32::MAX,
                rows: RoaringBitmap::new(),
                codes: s.codes,
                vectors: s.vectors,
                codes_crc: s.codes_crc,
                vectors_crc: s.vectors_crc,
                row_crcs: Vec::new(),
            })
            .collect();
        let metadata = Metadata {
            dims: shape.dims,
            count: 0,
            metric: shape.metric,
            dtype: shape.dtype,
            seed: shape.seed,
            flat: shape.flat,
            flags: format::FLAG_ROTATED | format::FLAG_COMPACT,
            parents: Vec::new(),
            clusters,
        };
        Ok(Self {
            reader,
            metadata,
            rq,
        })
    }
    /// Install a directory cluster blob (rotated center, row set, row checksums).
    pub(crate) fn install_cluster(&mut self, cid: usize, blob: &[u8]) -> Result<()> {
        let padded = self.rq.padded;
        let mut p = format::Parser::new(blob);
        let center = p.vector(padded)?;
        let n = p.u32()? as usize;
        let len = p.u32()? as usize;
        let rows = RoaringBitmap::deserialize_from(std::io::Cursor::new(p.take(len)?))
            .map_err(|_| bad("invalid membership bitmap"))?;
        if rows.len() != n as u64 || n > p.remaining() / 4 {
            return Err(bad("invalid cluster blob"));
        }
        let row_crcs = (0..n).map(|_| p.u32()).collect::<Result<Vec<_>>>()?;
        let c = self
            .metadata
            .clusters
            .get_mut(cid)
            .ok_or_else(|| bad("cluster outside segment"))?;
        c.center = center;
        c.rows = rows;
        c.row_crcs = row_crcs;
        Ok(())
    }
    /// Cluster payload for the directory blob area.
    pub(crate) fn cluster_blob(&self, cid: usize) -> Vec<u8> {
        let c = &self.metadata.clusters[cid];
        let mut out = Vec::new();
        for x in &c.center {
            out.extend(x.to_le_bytes());
        }
        out.extend((c.rows.len() as u32).to_le_bytes());
        out.extend((c.rows.serialized_size() as u32).to_le_bytes());
        c.rows.serialize_into(&mut out).expect("vec write");
        for crc in &c.row_crcs {
            out.extend(crc.to_le_bytes());
        }
        out
    }
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    /// The query, normalized as the metric requires, rotated into the index's transform space.
    fn rotated_query(&self, query: &[f32]) -> Vec<f64> {
        self.rq.rotate(&prepare(query, self.metric()))
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
        let q: Vec<f32> = self
            .rotated_query(query)
            .into_iter()
            .map(|x| x as f32)
            .collect();
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
        let mut estimated = self.scan_clusters(query, &chosen, filter).await?;
        let cmp = |a: &Candidate, b: &Candidate| {
            a.distance.total_cmp(&b.distance).then(a.row.cmp(&b.row))
        };
        if estimated.len() > target {
            estimated.select_nth_unstable_by(target, cmp);
            estimated.truncate(target);
        }
        let locations = estimated
            .into_iter()
            .map(|c| (c.cluster as usize, c.ordinal as usize))
            .collect();
        self.rerank(query, k, locations).await
    }

    /// Number of rows per leaf cluster that pass `filter` (all rows when `None`).
    pub fn cluster_counts(&self, filter: Option<&RoaringBitmap>) -> Vec<u32> {
        self.metadata
            .clusters
            .iter()
            .map(|c| filter.map_or(c.rows.len(), |f| c.rows.intersection_len(f)) as u32)
            .collect()
    }

    /// Squared Euclidean distance from the query, normalized as the metric requires,
    /// to every leaf center. Distances are comparable across indexes built over one
    /// vector space, which lets callers rank clusters across segments.
    pub fn center_distances(&self, query: &[f32]) -> Result<Vec<f32>> {
        validate(query, self.dims())?;
        let q: Vec<f32> = self
            .rotated_query(query)
            .into_iter()
            .map(|x| x as f32)
            .collect();
        Ok(self
            .metadata
            .clusters
            .iter()
            .map(|c| Metric::EuclideanSquared.distance_unchecked(&q, &c.center))
            .collect())
    }

    /// Stored bytes scanned per row when probing a cluster's quantized codes.
    pub fn code_row_bytes(&self) -> usize {
        self.code_head() + self.rq.padded.div_ceil(8)
    }

    fn code_head(&self) -> usize {
        if self.metadata.flags & format::FLAG_COMPACT != 0 {
            8
        } else {
            24
        }
    }

    /// Stored bytes per full-precision vector.
    pub fn vector_row_bytes(&self) -> usize {
        self.dims() * self.vector_type().width()
    }

    /// Rows stored in one leaf cluster.
    pub fn cluster_rows(&self, cluster: usize) -> usize {
        self.metadata.clusters[cluster].rows.len() as usize
    }

    /// Estimate distances for every row (passing `filter`) in the given leaf clusters,
    /// from their quantized codes. Cluster code ranges are fetched in one batch.
    pub async fn scan_clusters(
        &self,
        query: &[f32],
        clusters: &[usize],
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<Candidate>> {
        validate(query, self.dims())?;
        let t_read = std::time::Instant::now();
        let blocks = self.read_coalesced(clusters).await?;
        let read_ms = t_read.elapsed().as_millis();
        self.scan_blocks_timed(query, clusters, blocks, filter, read_ms)
    }

    /// Read the quantized-code blocks of `clusters` (merged into few requests), in input order.
    pub async fn read_codes(&self, clusters: &[usize]) -> Result<Vec<Bytes>> {
        self.read_coalesced(clusters).await
    }

    /// Score code blocks already fetched with [`Self::read_codes`]. Every scanned cluster's
    /// center and row set must be present (always true for an opened index).
    pub fn scan_blocks(
        &self,
        query: &[f32],
        clusters: &[usize],
        blocks: Vec<Bytes>,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<Candidate>> {
        validate(query, self.dims())?;
        self.scan_blocks_timed(query, clusters, blocks, filter, 0)
    }

    fn scan_blocks_timed(
        &self,
        query: &[f32],
        clusters: &[usize],
        blocks: Vec<Bytes>,
        filter: Option<&RoaringBitmap>,
        read_ms: u128,
    ) -> Result<Vec<Candidate>> {
        let q = prepare(query, self.metric());
        let q_zero = q.iter().all(|x| *x == 0.);
        let q_rot = self.rq.rotate(&q);
        let t_cpu = std::time::Instant::now();
        let rows: usize = clusters
            .iter()
            .map(|c| self.metadata.clusters[*c].rows.len() as usize)
            .sum();
        let mut estimated = Vec::with_capacity(rows);
        let compact = self.metadata.flags & format::FLAG_COMPACT != 0;
        for (cid, bytes) in clusters.iter().copied().zip(blocks) {
            let c = &self.metadata.clusters[cid];
            check_crc(&bytes, c.codes_crc)?;
            let plane = crate::rabitq::PlaneQuery::new(&q_rot, &c.center);
            let mut rows = c.rows.iter();
            plane.scan(
                &bytes,
                c.rows.len() as usize,
                compact,
                self.rq.padded,
                &mut |ordinal, d, bound, zero| {
                    let row = rows.next().unwrap_or(u32::MAX);
                    if filter.is_some_and(|f| !f.contains(row)) {
                        return;
                    }
                    let (distance, error) = if self.metric() == Metric::CosineDistance {
                        if q_zero || zero {
                            (if q_zero && zero { 0. } else { 1. }, 0.)
                        } else {
                            ((d * 0.5).clamp(0., 2.), bound * 0.5)
                        }
                    } else {
                        (d.max(0.), bound)
                    };
                    estimated.push(Candidate {
                        row,
                        distance: distance as f32,
                        error: error as f32,
                        cluster: cid as u32,
                        ordinal: ordinal as u32,
                    });
                },
            )?;
        }
        if std::env::var_os("BF_TRACE_STAGES").is_some() {
            eprintln!(
                "SCAN clusters={} read={}ms cpu={}ms rows={}",
                clusters.len(),
                read_ms,
                t_cpu.elapsed().as_millis(),
                estimated.len()
            );
        }
        Ok(estimated)
    }

    /// Read each cluster's code block, merging nearby ranges into one request.
    async fn read_coalesced(&self, clusters: &[usize]) -> Result<Vec<Bytes>> {
        let ranges: Vec<_> = clusters
            .iter()
            .map(|c| self.metadata.clusters[*c].codes.clone())
            .collect();
        self.read_merged(ranges, CODE_GAP, CODE_MERGE_MAX).await
    }

    /// Read `ranges` (in any order), merging ranges whose gap is at most `gap` bytes into one
    /// read of at most `max` bytes. Returns one `Bytes` per input range, in input order.
    async fn read_merged(&self, ranges: Vec<Range<u64>>, gap: u64, max: u64) -> Result<Vec<Bytes>> {
        read_merged(&self.reader, ranges, gap, max).await
    }

    /// Fetch full-precision vectors for `(cluster, ordinal)` locations and return the
    /// exact top-k, sorted by distance then row ID.
    pub async fn rerank_candidates(
        &self,
        query: &[f32],
        k: usize,
        locations: Vec<(u32, u32)>,
    ) -> Result<Vec<(u32, f32)>> {
        validate(query, self.dims())?;
        let locations = locations
            .into_iter()
            .map(|(c, i)| (c as usize, i as usize))
            .collect();
        self.rerank(query, k, locations).await
    }

    /// Every `(cluster, ordinal)` location whose row passes `filter`.
    pub fn locations(&self, filter: Option<&RoaringBitmap>) -> Vec<(u32, u32)> {
        let mut out = Vec::new();
        for (cid, c) in self.metadata.clusters.iter().enumerate() {
            match filter {
                Some(f) => {
                    for row in (&c.rows & f).iter() {
                        out.push((cid as u32, c.rows.rank(row) as u32 - 1));
                    }
                }
                None => out.extend((0..c.rows.len() as u32).map(|i| (cid as u32, i))),
            }
        }
        out
    }

    /// Approximate resident bytes of the decoded metadata (centers, row sets, checksums).
    pub fn resident_bytes(&self) -> u64 {
        let m = &self.metadata;
        let mut total = (m.parents.len() * (m.dims * 4 + 48)) as u64;
        for c in &m.clusters {
            total += (c.center.len() * 4 + c.row_crcs.capacity() * 4 + 160) as u64
                + c.rows.serialized_size() as u64;
        }
        total + (self.rq.padded * 8 * 4) as u64
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
        let t_read = std::time::Instant::now();
        let nranges = ranges.len();
        let rbytes: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        let blocks = self
            .read_merged(ranges, VECTOR_GAP, VECTOR_MERGE_MAX)
            .await?;
        let read_ms = t_read.elapsed().as_millis();
        let t_cpu = std::time::Instant::now();
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
        if std::env::var_os("BF_TRACE_STAGES").is_some() {
            eprintln!(
                "RERANK locations={} ranges={} bytes={} read={}ms cpu={}ms",
                locations.len(),
                nranges,
                rbytes,
                read_ms,
                t_cpu.elapsed().as_millis()
            );
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

/// Read `ranges` (in any order) from `reader`, merging ranges whose gap is at most `gap` bytes
/// into one read of at most `max` bytes. Returns one `Bytes` per input range, in input order.
pub async fn read_merged<R: RangeRead>(
    reader: &R,
    ranges: Vec<Range<u64>>,
    gap: u64,
    max: u64,
) -> Result<Vec<Bytes>> {
    let mut order: Vec<usize> = (0..ranges.len()).collect();
    order.sort_unstable_by_key(|i| ranges[*i].start);
    let mut merged: Vec<Range<u64>> = Vec::new();
    let mut members: Vec<Vec<usize>> = Vec::new();
    for i in order {
        let r = ranges[i].clone();
        match merged.last_mut() {
            Some(last) if r.start <= last.end + gap && r.end.max(last.end) - last.start <= max => {
                last.end = last.end.max(r.end);
                members.last_mut().unwrap().push(i);
            }
            _ => {
                merged.push(r);
                members.push(vec![i]);
            }
        }
    }
    let starts: Vec<u64> = merged.iter().map(|r| r.start).collect();
    let bytes = read_batch(reader, merged).await?;
    let mut out = vec![Bytes::new(); ranges.len()];
    for ((bytes, start), group) in bytes.into_iter().zip(starts).zip(members) {
        for i in group {
            let r = &ranges[i];
            out[i] = bytes.slice((r.start - start) as usize..(r.end - start) as usize);
        }
    }
    Ok(out)
}
