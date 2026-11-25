use std::{ops::Range, sync::Arc};

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

/// Whether `BF_TRACE_STAGES` is set (read once).
fn trace_stages() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("BF_TRACE_STAGES").is_some())
}

/// Ranges closer than this many bytes are read together: the gap is fetched and dropped.
/// `BF_READ_GAP_KB` overrides every merge gap below (experiments and tuning).
pub fn read_gap(default: u64) -> u64 {
    static OVERRIDE: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    OVERRIDE
        .get_or_init(|| {
            std::env::var("BF_READ_GAP_KB")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb << 10)
        })
        .unwrap_or(default)
}
/// Code blocks closer than this are read together (the gap bytes are fetched and dropped).
const CODE_GAP: u64 = 64 << 10;
const CODE_MERGE_MAX: u64 = 8 << 20;
/// Candidate vectors closer than this are read together.
const VECTOR_GAP: u64 = 64 << 10;
const VECTOR_MERGE_MAX: u64 = 2 << 20;

/// A directory cluster blob, decoded: the rotated center, the row set, and the row checksums.
/// Cheap to clone (shared), so a caller can keep decoded clusters across queries and install
/// them into each query's partial index instead of decoding the blob again.
#[derive(Clone)]
pub struct DecodedCluster(pub(crate) Arc<format::ClusterData>);
impl DecodedCluster {
    /// Approximate resident bytes.
    pub fn resident_bytes(&self) -> u64 {
        (self.0.center.len() * 4 + self.0.row_crcs.len() * 8 + self.0.rows.serialized_size() + 128)
            as u64
    }
}

/// A query normalized for its metric and rotated into an index's transform space.
pub struct RotatedQuery {
    pub(crate) rotated: Vec<f64>,
    /// `rotated` in single precision.
    pub(crate) rotated32: Vec<f32>,
    /// The normalized query is all zero.
    pub(crate) zero: bool,
    pub(crate) seed: u64,
}
impl RotatedQuery {
    pub(crate) fn new(rq: &RaBitQ, query: &[f32], metric: Metric) -> Self {
        let q = prepare(query, metric);
        let rotated = rq.rotate(&q);
        Self {
            zero: q.iter().all(|x| *x == 0.),
            rotated32: rotated.iter().map(|x| *x as f32).collect(),
            rotated,
            seed: rq.seed,
        }
    }
}

/// An `f32` ordered by `total_cmp`, for heaps.
#[derive(PartialEq)]
struct OrdF32(f32);
impl Eq for OrdF32 {}
impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Reusable worker-local buffers for scanning immutable code blocks.
#[derive(Default)]
pub struct ScanScratch {
    uppers: std::collections::BinaryHeap<OrdF32>,
    final_uppers: Vec<f32>,
    estimated: Vec<Candidate>,
    fast: Option<crate::fastscan::FastQuery>,
    ordered: Vec<u32>,
    rescore: Vec<(Range<usize>, usize, usize, f32)>,
    selected: Vec<usize>,
    filter_bits: Vec<u64>,
}

// One lookup per row avoids repeatedly merging each leaf with the same sparse predicate.
// Bound scratch by the scan population; widely spaced external row IDs use the bitmap merge.
fn filter_bits<'a>(
    filter: &RoaringBitmap,
    rows: usize,
    scratch: &'a mut Vec<u64>,
) -> Option<&'a [u64]> {
    let words = filter.max().map_or(0, |row| row as usize / 64 + 1);
    if words > rows {
        return None;
    }
    scratch.clear();
    scratch.resize(words, 0);
    for row in filter {
        scratch[row as usize / 64] |= 1u64 << (row % 64);
    }
    Some(scratch)
}

/// A parsed immutable index. Metadata and centroids stay resident; codes and
/// vectors are fetched through the reader for each search (usually cache-backed).
pub struct Index<R: RangeRead> {
    reader: R,
    pub(crate) metadata: Metadata,
    pub(crate) rq: RaBitQ,
    /// The mean rotated center fast-scan codes are relative to (see [`crate::fastscan`]): for a
    /// fast-scan index, or for a partial index, the directory section's centroid.
    pub(crate) reference: Option<Arc<Vec<f32>>>,
    /// Cluster blobs come from a fragment written before v0.1.15, whose blobs carry no
    /// checksum (see [`Self::decode_cluster`]).
    legacy_blobs: bool,
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
        Self::open_metadata(reader, &bytes, crc, len).await
    }
    /// Open footer and front metadata in one parallel range-read batch, using
    /// the metadata length recorded when building or publishing the segment.
    pub async fn open_with_metadata_len(reader: R, meta_len: u64) -> Result<Self> {
        let len = reader.len();
        if len < FOOTER_SIZE || meta_len > len - FOOTER_SIZE {
            return Err(bad("invalid metadata range"));
        }
        let ranges = vec![len - FOOTER_SIZE..len, 0..meta_len];
        let bytes = read_batch(&reader, ranges, false).await?;
        let (actual, crc) = format::decode_footer(&bytes[0], len)?;
        if actual != meta_len {
            return Err(bad("manifest metadata length disagrees with footer"));
        }
        Self::open_metadata(reader, &bytes[1], crc, len).await
    }
    async fn open_metadata(reader: R, bytes: &[u8], crc: u32, len: u64) -> Result<Self> {
        if bytes.starts_with(crate::directory::ANN_MAGIC) {
            format::check_crc(bytes, crc)?;
            let fragment = crate::Fragment::decode(bytes)?;
            fragment.validate(bytes.len() as u64, len - FOOTER_SIZE)?;
            let mut index = fragment.index(reader)?;
            let blobs = fragment.blobs();
            if blobs
                .iter()
                .any(|(_, r)| r.start < bytes.len() as u64 || r.end > len - FOOTER_SIZE)
            {
                return Err(bad("cluster blob outside ANN object"));
            }
            let data = read_batch(
                &index.reader,
                blobs.iter().map(|(_, r)| r.clone()).collect(),
                false,
            )
            .await?;
            for ((cid, _), data) in blobs.into_iter().zip(data) {
                index.install_cluster(cid, &data)?;
            }
            index.metadata.count = fragment.rows();
            return Ok(index);
        }
        Self::from_metadata(reader, bytes, crc, len)
    }
    fn from_metadata(reader: R, bytes: &[u8], crc: u32, len: u64) -> Result<Self> {
        let mut metadata = format::decode_metadata(bytes, crc, len - FOOTER_SIZE)?;
        let rq = RaBitQ::new(metadata.dims, metadata.seed)?;
        if metadata.flags & format::FLAG_ROTATED == 0 {
            // Older indexes store raw centers. Rotating once at open lets every search
            // rotate the query once instead of once per probed cluster.
            let rot =
                |c: &[f32]| -> Vec<f32> { rq.rotate(c).into_iter().map(|x| x as f32).collect() };
            metadata.parents = metadata.parents.iter().map(|p| rot(p)).collect();
            for c in &mut metadata.clusters {
                let data = c
                    .data
                    .as_mut()
                    .and_then(Arc::get_mut)
                    .expect("fresh cluster");
                data.center = rot(&data.center);
            }
            metadata.flags |= format::FLAG_ROTATED;
        }
        let reference = (metadata.flags & format::FLAG_FASTSCAN != 0).then(|| {
            Arc::new(format::mean_center(
                metadata
                    .clusters
                    .iter()
                    .filter(|c| !c.rows.is_empty())
                    .map(|c| c.center.as_slice()),
                rq.padded,
            ))
        });
        Ok(Self {
            reader,
            metadata,
            rq,
            reference,
            legacy_blobs: false,
        })
    }
    /// Parse only the metadata of a complete in-memory index blob (no payload reads are possible).
    pub(crate) fn metadata_only(blob: &[u8], reader: R) -> Result<Self> {
        let len = blob.len() as u64;
        if len < FOOTER_SIZE {
            return Err(bad("missing footer"));
        }
        let (meta_len, crc) = format::decode_footer(&blob[(len - FOOTER_SIZE) as usize..], len)?;
        if blob.starts_with(crate::directory::ANN_MAGIC) {
            format::check_crc(&blob[..meta_len as usize], crc)?;
            let fragment = crate::Fragment::decode(&blob[..meta_len as usize])?;
            fragment.validate(meta_len, len - FOOTER_SIZE)?;
            let mut index = fragment.index(reader)?;
            for (cid, range) in fragment.blobs() {
                index.install_cluster(
                    cid,
                    blob.get(range.start as usize..range.end as usize)
                        .ok_or_else(|| bad("blob outside ANN object"))?,
                )?;
            }
            index.metadata.count = fragment.rows();
            return Ok(index);
        }
        Self::from_metadata(reader, &blob[..meta_len as usize], crc, len)
    }
    /// A partial index over a namespace directory: only the code and vector ranges are known
    /// until [`Self::install_cluster`] supplies each probed cluster's center and row set.
    pub(crate) fn stub(
        reader: R,
        shape: &crate::directory::Shape,
        stubs: Arc<Vec<format::Cluster>>,
        reference: Arc<Vec<f32>>,
    ) -> Result<Self> {
        let rq = RaBitQ::new(shape.dims, shape.seed)?;
        // The directory owns all immutable ranges; this query installs only selected payloads.
        let clusters = format::Clusters::partial(stubs);
        let metadata = Metadata {
            dims: shape.dims,
            count: 0,
            metric: shape.metric,
            dtype: shape.dtype,
            seed: shape.seed,
            flat: shape.flat,
            flags: format::FLAG_ROTATED
                | format::FLAG_COMPACT
                | if !shape.flat && rq.padded.is_multiple_of(8) {
                    format::FLAG_FASTSCAN
                } else {
                    0
                },
            parents: Vec::new(),
            clusters,
        };
        Ok(Self {
            reader,
            metadata,
            rq,
            reference: Some(reference),
            legacy_blobs: false,
        })
    }
    /// Install a directory cluster blob (rotated center, row set, row checksums).
    pub(crate) fn install_cluster(&mut self, cid: usize, blob: &[u8]) -> Result<()> {
        let decoded = self.decode_cluster(blob)?;
        self.install_decoded(cid, &decoded)
    }
    /// Decode a directory cluster blob without installing it.
    pub(crate) fn decode_cluster(&self, blob: &[u8]) -> Result<DecodedCluster> {
        let body = match blob.split_last_chunk::<4>() {
            Some((body, crc)) if crc32fast::hash(body) == u32::from_le_bytes(*crc) => body,
            // A pre-v0.1.15 block fragment's blob has no trailing checksum. Its parse below must
            // still consume it exactly. Blobs rebuilt from such an index do carry one.
            _ if self.legacy_blobs => blob,
            Some(_) => return Err(bad("checksum mismatch")),
            None => return Err(bad("truncated cluster blob")),
        };
        let padded = self.rq.padded;
        let mut p = format::Parser::new(body);
        let center = p.vector(padded)?;
        let n = p.u32()? as usize;
        let len = p.u32()? as usize;
        let rows = RoaringBitmap::deserialize_from(std::io::Cursor::new(p.take(len)?))
            .map_err(|_| bad("invalid membership bitmap"))?;
        if rows.len() != n as u64 || n > p.remaining() / 4 {
            return Err(bad("invalid cluster blob"));
        }
        let row_crcs = (0..n).map(|_| p.u32()).collect::<Result<Vec<_>>>()?;
        if p.remaining() != 0 {
            return Err(bad("unexpected cluster blob bytes"));
        }
        Ok(DecodedCluster(Arc::new(format::ClusterData {
            center,
            ids: Some(rows.iter().collect()),
            rows,
            row_crcs,
            codes_checked: Default::default(),
        })))
    }
    /// Install a cluster decoded by [`Self::decode_cluster`] (shared, not copied).
    pub(crate) fn install_decoded(&mut self, cid: usize, d: &DecodedCluster) -> Result<()> {
        if d.0.center.len() != self.rq.padded {
            return Err(bad("cluster center width"));
        }
        let c = self
            .metadata
            .clusters
            .get_mut(cid)
            .ok_or_else(|| bad("cluster outside segment"))?;
        c.data = Some(d.0.clone());
        Ok(())
    }
    /// Cluster payload for the directory blob area.
    pub(crate) fn cluster_blob(&self, cid: usize) -> Vec<u8> {
        let c = &self.metadata.clusters[cid];
        let mut out = Vec::new();
        for x in c.center.iter() {
            out.extend(x.to_le_bytes());
        }
        out.extend((c.rows.len() as u32).to_le_bytes());
        out.extend((c.rows.serialized_size() as u32).to_le_bytes());
        c.rows.serialize_into(&mut out).expect("vec write");
        for crc in c.row_crcs.iter() {
            out.extend(crc.to_le_bytes());
        }
        let crc = crc32fast::hash(&out);
        out.extend(crc.to_le_bytes());
        out
    }
    /// Accept cluster blobs without a checksum, as fragments before v0.1.15 wrote them.
    pub(crate) fn accept_legacy_blobs(&mut self) {
        self.legacy_blobs = true;
    }
    pub(crate) fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
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
            .map(|c| crate::distance::l2_squared_f32(&q, &c.center))
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

    /// Read the requested rows at their stored precision, sorted by row, with verified
    /// checksums. Missing rows are omitted. A partial index reads only its installed
    /// clusters; callers must install every cluster that can contain a requested row first.
    /// Sparse reads use batches of at most 1,024 vectors and sparse cache blocks. Bulk reads
    /// (including compaction) read whole leaves, at most 16 at a time, and restore row order.
    pub async fn read_packed_rows(&self, wanted: &RoaringBitmap) -> Result<(Vec<u32>, Vec<u8>)> {
        let mut present = RoaringBitmap::new();
        for c in &self.metadata.clusters {
            present |= &c.rows & wanted;
        }
        let rows: Vec<u32> = present.iter().collect();
        let width = self.vector_row_bytes();
        let mut out = vec![0; rows.len() * width];
        let mut locations = Vec::with_capacity(rows.len());
        let mut whole = Vec::new();
        for (cid, c) in self.metadata.clusters.iter().enumerate() {
            let selected = &c.rows & wanted;
            if !selected.is_empty() && selected.len() * 3 >= c.rows.len() {
                whole.push(cid);
                continue;
            }
            for row in selected {
                let ordinal = (c.rows.rank(row) - 1) as usize;
                let at = c.vectors.start + (ordinal * width) as u64;
                if at + width as u64 > c.vectors.end {
                    return Err(bad("vector ordinal outside cluster"));
                }
                locations.push((row, at, c.row_crcs[ordinal]));
            }
        }
        for batch in whole.chunks(16) {
            let leaves =
                futures::future::try_join_all(batch.iter().map(|cid| self.read_cluster_rows(*cid)))
                    .await?;
            for (leaf_rows, bytes) in leaves {
                for (row, raw) in leaf_rows.iter().zip(bytes.chunks_exact(width)) {
                    if let Ok(slot) = rows.binary_search(row) {
                        out[slot * width..(slot + 1) * width].copy_from_slice(raw);
                    }
                }
            }
        }
        for batch in locations.chunks(1024) {
            let bytes = self
                .read_vectors(
                    batch
                        .iter()
                        .map(|(_, at, _)| *at..*at + width as u64)
                        .collect(),
                )
                .await?;
            for ((row, _, crc), bytes) in batch.iter().zip(bytes) {
                check_crc(&bytes, *crc)?;
                let slot = rows.binary_search(row).expect("present row");
                out[slot * width..(slot + 1) * width].copy_from_slice(&bytes);
            }
        }
        Ok((rows, out))
    }

    /// Every row of one leaf cluster (ascending) and its raw full-precision vectors
    /// (`vector_row_bytes` each, in row order), read in one ranged read. Used by reclustering.
    pub async fn read_cluster_rows(&self, cluster: usize) -> Result<(Vec<u32>, Bytes)> {
        let c = &self.metadata.clusters[cluster];
        let rows: Vec<u32> = c.rows.iter().collect();
        if rows.is_empty() {
            return Ok((rows, Bytes::new()));
        }
        let bytes = read_one(&self.reader, c.vectors.clone()).await?;
        if bytes.len() != rows.len() * self.vector_row_bytes() {
            return Err(bad("cluster vector length disagrees with its rows"));
        }
        check_crc(&bytes, c.vectors_crc)?;
        Ok((rows, bytes))
    }

    /// Decode one stored vector (`vector_row_bytes` raw bytes) to `f32`.
    pub fn decode_stored(&self, raw: &[u8]) -> Vec<f32> {
        match self.vector_type() {
            VectorType::F32 => raw
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect(),
            VectorType::I8 => raw.iter().map(|b| *b as i8 as f32).collect(),
            VectorType::F16 => {
                let mut out = Vec::with_capacity(raw.len() / 2);
                extend_f16(raw, &mut out);
                out
            }
        }
    }

    /// Rows stored in one leaf cluster.
    pub fn cluster_rows(&self, cluster: usize) -> usize {
        self.metadata.clusters[cluster].len()
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
        let read_us = t_read.elapsed().as_micros();
        self.scan_blocks_timed(query, clusters, blocks, filter, read_us, None)
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
        self.scan_blocks_timed(query, clusters, blocks, filter, 0, None)
    }

    /// [`Self::scan_blocks`] for a caller that keeps only candidates whose lower bound
    /// (`distance - error`) is at most the `k`-th smallest upper bound (`distance + error`) of
    /// all candidates. A candidate whose lower bound already exceeds the `k`-th smallest upper
    /// bound among this call's own candidates can never pass that test, so it is not returned.
    pub fn scan_blocks_bounded(
        &self,
        query: &[f32],
        clusters: &[usize],
        blocks: Vec<Bytes>,
        filter: Option<&RoaringBitmap>,
        k: usize,
    ) -> Result<Vec<Candidate>> {
        validate(query, self.dims())?;
        self.scan_blocks_timed(query, clusters, blocks, filter, 0, (k > 0).then_some(k))
    }

    /// The query normalized for the metric and rotated into the transform space. Indexes of
    /// one vector space (one seed and dimension) share it, so a caller scanning several of them
    /// rotates once (see [`Self::scan_blocks_bounded_rotated`]).
    pub fn rotate_query(&self, query: &[f32]) -> Result<RotatedQuery> {
        validate(query, self.dims())?;
        Ok(RotatedQuery::new(&self.rq, query, self.metric()))
    }

    /// [`Self::scan_blocks_bounded`] with a query from [`Self::rotate_query`] or
    /// [`crate::Directory::rotate_query`] of the same vector space.
    /// `upper_bound` is infinity, or the k-th smallest candidate upper bound already found
    /// in another index for the same query, metric, and k. Candidates whose lower bounds
    /// exceed it cannot win globally.
    pub fn scan_blocks_bounded_rotated(
        &self,
        query: &RotatedQuery,
        clusters: &[usize],
        blocks: Vec<Bytes>,
        filter: Option<&RoaringBitmap>,
        k: usize,
        upper_bound: f32,
    ) -> Result<Vec<Candidate>> {
        if query.rotated.len() != self.rq.padded || query.seed != self.metadata.seed {
            return Err(Error::InvalidInput(
                "query rotated for another index".into(),
            ));
        }
        if upper_bound.is_nan() || upper_bound < 0. {
            return Err(Error::InvalidInput("invalid candidate upper bound".into()));
        }
        self.scan_rotated(
            query,
            clusters,
            blocks,
            filter,
            0,
            (k > 0).then_some((k, upper_bound)),
        )
    }

    fn scan_blocks_timed(
        &self,
        query: &[f32],
        clusters: &[usize],
        blocks: Vec<Bytes>,
        filter: Option<&RoaringBitmap>,
        read_us: u128,
        keep: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let q = RotatedQuery::new(&self.rq, query, self.metric());
        self.scan_rotated(
            &q,
            clusters,
            blocks,
            filter,
            read_us,
            keep.map(|k| (k, f32::INFINITY)),
        )
    }

    fn scan_rotated(
        &self,
        query: &RotatedQuery,
        clusters: &[usize],
        blocks: Vec<Bytes>,
        filter: Option<&RoaringBitmap>,
        read_us: u128,
        keep: Option<(usize, f32)>,
    ) -> Result<Vec<Candidate>> {
        let mut scratch = ScanScratch::default();
        self.scan_into(
            query,
            clusters,
            |position, _| &blocks[position],
            filter,
            read_us,
            keep,
            &mut scratch,
        )?;
        Ok(scratch.estimated)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_into<'a, 'b>(
        &self,
        query: &RotatedQuery,
        clusters: &[usize],
        block: impl Fn(usize, usize) -> &'b [u8],
        filter: Option<&RoaringBitmap>,
        read_us: u128,
        keep: Option<(usize, f32)>,
        scratch: &'a mut ScanScratch,
    ) -> Result<&'a [Candidate]> {
        self.scan_with(
            query,
            clusters,
            block,
            |cid| &self.metadata.clusters[cid],
            filter,
            read_us,
            keep,
            scratch,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_with<'a, 'b, 'c>(
        &self,
        query: &RotatedQuery,
        clusters: &[usize],
        block: impl Fn(usize, usize) -> &'b [u8],
        cluster: impl Fn(usize) -> &'c format::Cluster,
        filter: Option<&RoaringBitmap>,
        read_us: u128,
        keep: Option<(usize, f32)>,
        scratch: &'a mut ScanScratch,
    ) -> Result<&'a [Candidate]> {
        // The `k` smallest upper bounds so far, as a max-heap; `cut` is its largest once full.
        scratch.uppers.clear();
        scratch.estimated.clear();
        scratch.rescore.clear();
        let ScanScratch {
            uppers,
            final_uppers,
            estimated,
            fast,
            ordered,
            rescore,
            selected,
            filter_bits: bits,
        } = scratch;
        let (keep, upper_bound) = keep.map_or((None, f32::INFINITY), |(k, bound)| (Some(k), bound));
        let cut = std::cell::Cell::new(upper_bound);
        let q_zero = query.zero;
        let q_rot = &query.rotated;
        let t_cpu = std::time::Instant::now();
        let rows: usize = clusters.iter().map(|c| cluster(*c).len()).sum();
        estimated.reserve(rows);
        // A sparse scan pays scalar table lookups per allowed row. Dense and unfiltered
        // scans retain their block kernel, including its existing admission and cut order.
        let sparse_filter = filter.filter(|f| f.len().saturating_mul(16) <= rows as u64);
        let bits = sparse_filter.and_then(|filter| filter_bits(filter, rows, bits));
        let mut skipped_codes = 0usize;
        let compact = self.metadata.flags & format::FLAG_COMPACT != 0;
        let padded = self.rq.padded;
        let cosine = self.metric() == Metric::CosineDistance;
        let mut prepare_fast = true;
        let q32 = &query.rotated32;

        // Fast-scanned clusters' candidates (a range of `estimated`), to score again with the
        // fine tables once the cut is final.

        for (position, cid) in clusters.iter().copied().enumerate() {
            let bytes = block(position, cid);
            let c = cluster(cid);
            // A stub's (shared, empty) payload never records a check.
            let checked = c.data.as_deref().map(|d| &d.codes_checked);
            let n = c.len();
            let blocked = n > 0
                && compact
                && padded.is_multiple_of(8)
                && bytes.len() == crate::fastscan::block_len(n, padded);
            if !checked.is_some_and(|f| f.load(std::sync::atomic::Ordering::Relaxed)) {
                check_crc(bytes, c.codes_crc)?;
                // The fast scan trusts its factors once they are checked here.
                if blocked {
                    crate::fastscan::check_factors(bytes, n, padded)?;
                }
                if let Some(f) = checked {
                    f.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
            let ids: &[u32] = match c.ids.as_deref() {
                Some(ids) => ids,
                None => {
                    ordered.clear();
                    ordered.extend(c.rows.iter());
                    ordered
                }
            };
            let first = estimated.len();
            let mut accept = |ordinal: usize, distance: f32, error: f32| {
                if keep.is_some() && distance - error > cut.get() {
                    return;
                }
                let row = ids.get(ordinal).copied().unwrap_or(u32::MAX);
                if filter.is_some_and(|f| !f.contains(row)) {
                    return;
                }
                if let Some(k) = keep {
                    let upper = distance + error;
                    if uppers.len() < k {
                        uppers.push(OrdF32(upper));
                    } else if upper < cut.get() {
                        uppers.pop();
                        uppers.push(OrdF32(upper));
                    }
                    if uppers.len() == k {
                        cut.set(upper_bound.min(uppers.peek().map_or(f32::INFINITY, |u| u.0)));
                    }
                }
                estimated.push(Candidate {
                    row,
                    distance,
                    error,
                    cluster: cid as u32,
                    ordinal: ordinal as u32,
                });
            };
            // A zero query or row has a fixed cosine distance.
            let fixed = |zero: bool| -> (f32, f32) { (if q_zero && zero { 0. } else { 1. }, 0.) };
            if blocked {
                let reference = self
                    .reference
                    .as_deref()
                    .ok_or_else(|| bad("fast-scan codes without a reference center"))?;
                if prepare_fast {
                    match fast {
                        Some(fq) => fq.reset(q_rot, reference),
                        None => *fast = Some(crate::fastscan::FastQuery::new(q_rot, reference)),
                    }
                    prepare_fast = false;
                }
                let fq = fast.as_ref().expect("prepared fast query");
                let qc2 = crate::distance::l2_squared_f32(q32, &c.center);
                // A zero query has a fixed distance to every row: no cut applies.
                let open = std::cell::Cell::new(f32::INFINITY);
                let scan_cut = if cosine && q_zero { &open } else { &cut };
                selected.clear();
                if let Some(filter) = sparse_filter {
                    if let Some(bits) = bits {
                        selected.extend(ids.iter().enumerate().filter_map(|(ordinal, &row)| {
                            bits.get(row as usize / 64)
                                .is_some_and(|word| word & (1u64 << (row % 64)) != 0)
                                .then_some(ordinal)
                        }));
                    } else {
                        let allowed = &c.rows & filter;
                        selected.extend(allowed.iter().map(|row| (c.rows.rank(row) - 1) as usize));
                    }
                }
                if sparse_filter.is_some() && selected.len().saturating_mul(16) <= n {
                    skipped_codes += n - selected.len();
                    fq.scan_selected(
                        bytes,
                        n,
                        qc2,
                        cosine,
                        scan_cut,
                        selected,
                        &mut |ordinal, d, e, z| {
                            let (d, e) = if cosine && (q_zero || z) {
                                fixed(z)
                            } else {
                                (d, e)
                            };
                            accept(ordinal, d, e);
                        },
                    )?;
                } else {
                    fq.scan(
                        bytes,
                        n,
                        qc2,
                        cosine,
                        scan_cut,
                        &mut |start, mut rows, d, e, zero| {
                            while rows != 0 {
                                let j = rows.trailing_zeros() as usize;
                                rows &= rows - 1;
                                let z = zero & (1 << j) != 0;
                                let (d, e) = if cosine && (q_zero || z) {
                                    fixed(z)
                                } else {
                                    (d[j], e[j])
                                };
                                accept(start + j, d, e);
                            }
                        },
                    )?;
                }
                if keep.is_some() && estimated.len() > first && fq.rounding_matters(qc2) {
                    rescore.push((first..estimated.len(), position, n, qc2));
                }
                continue;
            }
            let mut consider = |ordinal: usize, d: f64, bound: f64, zero: bool| {
                let (distance, error) = if cosine {
                    if q_zero || zero {
                        let (d, e) = fixed(zero);
                        (f64::from(d), f64::from(e))
                    } else {
                        ((d * 0.5).clamp(0., 2.), bound * 0.5)
                    }
                } else {
                    (d.max(0.), bound)
                };
                accept(ordinal, distance as f32, error as f32);
            };
            let plane = crate::rabitq::PlaneQuery::new(q_rot, &c.center);
            plane.scan(bytes, n, compact, padded, &mut |ordinal, d, bound, zero| {
                consider(ordinal, d, bound, zero)
            })?;
        }
        // The coarse tables' rounding grows with the query's distance from the index's mean
        // center, so rows that can still make the top `keep` get the fine score: the ordering of
        // close rows (which decides prefetches and the rerank pool) and tighter bounds.
        if !prepare_fast && let Some(fq) = fast {
            let cut = cut.get();
            for (range, position, n, qc2) in rescore.iter() {
                let bytes = block(*position, clusters[*position]);
                for c in &mut estimated[range.clone()] {
                    if c.error == 0. || c.distance - c.error > cut {
                        continue;
                    }
                    (c.distance, c.error) = fq.rescore(bytes, *n, *qc2, cosine, c.ordinal as usize);
                }
            }
        }
        if let Some(k) = keep
            && k > 0
            && estimated.len() > k
        {
            // Early clusters used a looser cut. Apply this index's final bound before
            // returning candidates to the namespace-wide merge. Its k-th upper bound
            // cannot be smaller than the global one, so every possible winner remains.
            // Recompute after fine rescoring, which can change both distance and error.
            final_uppers.clear();
            final_uppers.extend(estimated.iter().map(|c| c.distance + c.error));
            final_uppers.select_nth_unstable_by(k - 1, f32::total_cmp);
            let threshold = upper_bound.min(final_uppers[k - 1]);
            estimated.retain(|c| c.distance - c.error <= threshold);
        }
        if trace_stages() {
            eprintln!(
                "SCAN clusters={} read={}us cpu={}us scanned={} evaluated={} candidates={}",
                clusters.len(),
                read_us,
                t_cpu.elapsed().as_micros(),
                rows,
                rows - skipped_codes,
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
        self.read_merged(ranges, read_gap(CODE_GAP), CODE_MERGE_MAX)
            .await
    }

    /// Read `ranges` (in any order), merging ranges whose gap is at most `gap` bytes into one
    /// read of at most `max` bytes. Returns one `Bytes` per input range, in input order.
    async fn read_merged(&self, ranges: Vec<Range<u64>>, gap: u64, max: u64) -> Result<Vec<Bytes>> {
        read_merged(&self.reader, ranges, gap, max).await
    }
    /// Full-precision vectors for a rerank: few rows scattered over the vector section.
    async fn read_vectors(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        merged(
            &self.reader,
            ranges,
            read_gap(VECTOR_GAP),
            VECTOR_MERGE_MAX,
            true,
        )
        .await
    }

    /// Fetch full-precision vectors for `(cluster, ordinal)` locations and return the
    /// exact top-k, sorted by distance then row ID.
    pub async fn rerank_candidates(
        &self,
        query: &[f32],
        k: usize,
        locations: Vec<(u32, u32)>,
    ) -> Result<Vec<(u32, f32)>> {
        self.rerank_candidates_recorded(query, k, locations, None)
            .await
    }

    /// [`Self::rerank_candidates`], recording validated immutable vector rows for a resident
    /// tier. Only payloads already fetched by the ordinary rerank are recorded.
    pub async fn rerank_candidates_recorded(
        &self,
        query: &[f32],
        k: usize,
        locations: Vec<(u32, u32)>,
        recorder: Option<&dyn crate::VectorRecorder>,
    ) -> Result<Vec<(u32, f32)>> {
        validate(query, self.dims())?;
        let locations = locations
            .into_iter()
            .map(|(c, i)| (c as usize, i as usize))
            .collect();
        self.rerank_recorded(query, k, locations, recorder).await
    }

    /// Resident membership for present global rows and their cluster-local vector ordinals.
    /// Partial indexes include only installed clusters, so callers needing complete membership
    /// must first open or install every cluster.
    pub fn membership(&self) -> Result<crate::Membership> {
        crate::Membership::from_sparse_rows(
            self.metadata
                .clusters
                .iter()
                .enumerate()
                .flat_map(|(cluster, c)| c.rows.iter().map(move |row| (row, cluster as u32))),
            self.cluster_count(),
        )
    }

    /// Present row IDs in ascending order. A partial index includes only installed leaves.
    /// Tier vector locators retain this four-byte-per-row address list without vector payloads.
    pub fn row_ids(&self) -> Vec<u32> {
        let mut rows = RoaringBitmap::new();
        for cluster in &self.metadata.clusters {
            rows |= &cluster.rows;
        }
        rows.iter().collect()
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
                None => out.extend((0..c.len() as u32).map(|i| (cid as u32, i))),
            }
        }
        out
    }

    /// Approximate resident bytes of the decoded metadata (centers, row sets, checksums).
    pub fn resident_bytes(&self) -> u64 {
        let m = &self.metadata;
        let mut total = (m.parents.len() * (m.dims * 4 + 48)) as u64;
        for c in &m.clusters {
            total += (c.center.len() * 4 + c.row_crcs.len() * 4 + 160) as u64
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
                locations.extend((0..c.len()).map(|i| (cid, i)));
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
            .map(|c| crate::distance::l2_squared_f32(q, &c.center))
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

    /// Exact top-k over rows given as `(cluster, ordinal, row)`; only those vectors are read.
    /// Row checksums are not available for a partial index, so vectors are not verified.
    pub(crate) async fn exact_at(
        &self,
        query: &[f32],
        k: usize,
        mut locations: Vec<(u32, u32, u32)>,
    ) -> Result<Vec<(u32, f32)>> {
        validate(query, self.dims())?;
        if k == 0 || locations.is_empty() {
            return Ok(vec![]);
        }
        locations.sort_unstable();
        let width = self.dims() * self.vector_type().width();
        let mut ranges: Vec<Range<u64>> = Vec::with_capacity(locations.len());
        for &(cid, ordinal, _) in &locations {
            let c = self
                .metadata
                .clusters
                .get(cid as usize)
                .ok_or_else(|| bad("cluster outside segment"))?;
            let at = c.vectors.start + u64::from(ordinal) * width as u64;
            if at + width as u64 > c.vectors.end {
                return Err(bad("vector ordinal outside cluster"));
            }
            match ranges.last_mut() {
                Some(last) if last.end == at => last.end += width as u64,
                _ => ranges.push(at..at + width as u64),
            }
        }
        let blocks = self.read_vectors(ranges.clone()).await?;
        let mut result = Vec::with_capacity(locations.len());
        let mut decoded = Vec::with_capacity(self.dims());
        let mut next = locations.iter();
        for (range, bytes) in ranges.iter().zip(blocks) {
            for i in 0..(range.end - range.start) as usize / width {
                let &(_, _, row) = next.next().ok_or_else(|| bad("location count mismatch"))?;
                let vbytes = &bytes[i * width..(i + 1) * width];
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
                    VectorType::F16 => extend_f16(vbytes, &mut decoded),
                }
                if decoded.iter().any(|x| !x.is_finite()) {
                    return Err(bad("non-finite stored vector"));
                }
                result.push((row, self.metric().distance_unchecked(query, &decoded)));
            }
        }
        sort_truncate(&mut result, k);
        Ok(result)
    }

    async fn rerank(
        &self,
        query: &[f32],
        k: usize,
        locations: Vec<(usize, usize)>,
    ) -> Result<Vec<(u32, f32)>> {
        self.rerank_recorded(query, k, locations, None).await
    }

    async fn rerank_recorded(
        &self,
        query: &[f32],
        k: usize,
        locations: Vec<(usize, usize)>,
        recorder: Option<&dyn crate::VectorRecorder>,
    ) -> Result<Vec<(u32, f32)>> {
        self.rerank_with_norms(query, k, locations, recorder, None, &[])
            .await
    }
    /// Exact cosine rerank with norms from the same immutable source rows. Validates the
    /// query shape, stored checksums, and finite values just like [`Self::rerank_candidates`].
    /// Passing a norm for a different vector violates this method's caller contract.
    pub async fn rerank_cosine_candidates(
        &self,
        query: &crate::CosineQuery<'_>,
        k: usize,
        mut candidates: Vec<(usize, usize, crate::CosineNorm)>,
    ) -> Result<Vec<(u32, f32)>> {
        validate(query.vector(), self.dims())?;
        if self.metric() != Metric::CosineDistance {
            return Err(bad("cached cosine norms require cosine distance"));
        }
        candidates.sort_unstable_by_key(|c| (c.0, c.1));
        let locations = candidates.iter().map(|c| (c.0, c.1)).collect::<Vec<_>>();
        // Match the public rerank's bounds validation before reading any candidate metadata.
        for &(cid, ordinal) in &locations {
            if self
                .metadata
                .clusters
                .get(cid)
                .is_none_or(|c| ordinal >= c.len() || c.data.is_none())
            {
                return Err(bad("invalid rerank location"));
            }
        }
        self.rerank_with_norms(query.vector(), k, locations, None, Some(query), &candidates)
            .await
    }
    async fn rerank_with_norms(
        &self,
        query: &[f32],
        k: usize,
        mut locations: Vec<(usize, usize)>,
        recorder: Option<&dyn crate::VectorRecorder>,
        prepared: Option<&crate::CosineQuery<'_>>,
        norms: &[(usize, usize, crate::CosineNorm)],
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
            if (end - pos) * 3 >= cluster.len() {
                plans.push((cid, 0, cluster.len(), true));
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
        let blocks = self.read_vectors(ranges).await?;
        let read_us = t_read.elapsed().as_micros();
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
                // A whole cluster's vectors were checked at once above.
                if !whole {
                    check_crc(vbytes, c.row_crcs[i])?;
                }
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
                    VectorType::F16 => extend_f16(vbytes, &mut decoded),
                }
                if decoded.iter().any(|x| !x.is_finite()) {
                    return Err(bad("non-finite stored vector"));
                }
                let row = match &c.ids {
                    Some(ids) => ids.get(i).copied(),
                    None => c.rows.select(i as u32),
                }
                .ok_or_else(|| bad("invalid row ordinal"))?;
                if let Some(recorder) = recorder {
                    recorder.record(crate::ValidatedVector::new(
                        row,
                        self.dims(),
                        self.vector_type(),
                        vbytes,
                    ));
                }
                let distance = if let Some(prepared) = prepared {
                    let p = norms
                        .binary_search_by_key(&(cid, i), |c| (c.0, c.1))
                        .expect("prepared rerank norm");
                    prepared.distance(&decoded, norms[p].2)
                } else {
                    self.metric().distance_unchecked(query, &decoded)
                };
                result.push((row, distance));
            }
        }
        if trace_stages() {
            eprintln!(
                "RERANK locations={} ranges={} bytes={} read={}us cpu={}us",
                locations.len(),
                nranges,
                rbytes,
                read_us,
                t_cpu.elapsed().as_micros()
            );
        }
        sort_truncate(&mut result, k);
        Ok(result)
    }
}

/// Append little-endian `f16` bytes to `out` as `f32`, converting a block at a time (with the
/// CPU's conversion instructions where it has them) instead of one value per call.
pub(crate) fn extend_f16(bytes: &[u8], out: &mut Vec<f32>) {
    use half::slice::HalfFloatSliceExt;
    let start = out.len();
    out.resize(start + bytes.len() / 2, 0.);
    let mut buf = [half::f16::ZERO; 256];
    for (chunk, dst) in bytes.chunks(512).zip(out[start..].chunks_mut(256)) {
        let values = &mut buf[..chunk.len() / 2];
        for (b, h) in chunk.as_chunks::<2>().0.iter().zip(values.iter_mut()) {
            *h = half::f16::from_le_bytes(*b);
        }
        values.convert_to_f32_slice(dst);
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
async fn read_batch<R: RangeRead>(
    reader: &R,
    ranges: Vec<Range<u64>>,
    sparse: bool,
) -> Result<Vec<Bytes>> {
    let bytes = if sparse {
        reader.read_batch_sparse(ranges.clone()).await?
    } else {
        reader.read_batch(ranges.clone()).await?
    };
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
    merged(reader, ranges, gap, max, false).await
}

async fn merged<R: RangeRead>(
    reader: &R,
    ranges: Vec<Range<u64>>,
    gap: u64,
    max: u64,
    sparse: bool,
) -> Result<Vec<Bytes>> {
    if !sparse && reader.caches_blocks() {
        return read_batch(reader, ranges, false).await;
    }
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
    let bytes = read_batch(reader, merged, sparse).await?;
    let mut out = vec![Bytes::new(); ranges.len()];
    for ((bytes, start), group) in bytes.into_iter().zip(starts).zip(members) {
        for i in group {
            let r = &ranges[i];
            out[i] = bytes.slice((r.start - start) as usize..(r.end - start) as usize);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod sparse_filter_tests {
    use super::*;

    #[test]
    fn scratch_membership_matches_bitmap_and_bounds_widely_spaced_rows() {
        let mut scratch = Vec::new();
        for allowed in [vec![0, 63, 64, 129], vec![255], vec![]] {
            let filter: RoaringBitmap = allowed.into_iter().collect();
            let bits = filter_bits(&filter, 4, &mut scratch).unwrap();
            for row in 0u32..300 {
                assert_eq!(
                    bits.get(row as usize / 64)
                        .is_some_and(|word| word & (1u64 << (row % 64)) != 0),
                    filter.contains(row)
                );
            }
        }
        let capacity = scratch.capacity();
        for row in [256, u32::MAX] {
            assert!(filter_bits(&[row].into(), 4, &mut scratch).is_none());
            assert_eq!(scratch.capacity(), capacity);
        }
        assert!(
            filter_bits(&RoaringBitmap::new(), 0, &mut scratch)
                .unwrap()
                .is_empty()
        );
    }
}
