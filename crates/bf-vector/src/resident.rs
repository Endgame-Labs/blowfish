//! Validated resident payloads and reusable synchronous query scratch.
use crate::{Bytes, Candidate, Directory, Index, RangeRead, Result, RotatedQuery, ScanScratch};

/// Immutable index payloads validated once on admission. Each arena owns exactly its bytes;
/// slices do not pin unrelated cache blocks or the original object.
pub struct ResidentIndex<R: RangeRead = Bytes> {
    pub(crate) index: Index<R>,
    pub(crate) codes: Vec<Bytes>,
    vectors: Option<Vec<Bytes>>,
    bytes: u64,
}
impl<R: RangeRead> Index<R> {
    /// Compile all installed clusters and their code blocks into a resident view.
    /// Optional full-vector blocks enable synchronous reranking. Inputs are in cluster order.
    /// Corrupt codes, vectors, or incomplete cluster metadata are rejected before publication.
    pub fn into_resident(
        self,
        codes: Vec<Bytes>,
        vectors: Option<Vec<Bytes>>,
    ) -> Result<ResidentIndex<R>> {
        let n = self.cluster_count();
        if codes.len() != n || vectors.as_ref().is_some_and(|v| v.len() != n) {
            return Err(crate::format::bad("resident cluster count mismatch"));
        }
        for (cid, bytes) in codes.iter().enumerate() {
            let c = &self.metadata.clusters[cid];
            if c.len() == 0 && bytes.is_empty() {
                continue;
            }
            crate::format::check_crc(bytes, c.codes_crc)?;
            crate::fastscan::check_factors(bytes, c.len(), self.rq.padded)?;
            if let Some(vectors) = &vectors {
                let bytes = &vectors[cid];
                if bytes.len() != c.len() * self.dims() * self.vector_type().width() {
                    return Err(crate::format::bad("resident vector length mismatch"));
                }
                crate::format::check_crc(bytes, c.vectors_crc)?;
                let mut decoded = Vec::with_capacity(self.dims());
                for row in bytes.chunks_exact(self.dims() * self.vector_type().width()) {
                    decode(self.vector_type(), row, &mut decoded);
                    if decoded.iter().any(|x| !x.is_finite()) {
                        return Err(crate::format::bad("non-finite resident vector"));
                    }
                }
            }
            let data = c
                .data
                .as_deref()
                .ok_or_else(|| crate::format::bad("missing resident cluster"))?;
            data.codes_checked
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let bytes = self.resident_bytes()
            + (codes.capacity() * std::mem::size_of::<Bytes>()) as u64
            + self
                .metadata
                .clusters
                .iter()
                .map(|c| c.ids.as_ref().map_or(0, |ids| ids.len() as u64 * 4))
                .sum::<u64>()
            + codes.iter().map(|b| b.len() as u64).sum::<u64>()
            + vectors.as_ref().map_or(0, |v| {
                (v.capacity() * std::mem::size_of::<Bytes>()) as u64
                    + v.iter().map(|b| b.len() as u64).sum::<u64>()
            });
        Ok(ResidentIndex {
            index: self,
            codes: pack(codes),
            vectors: vectors.map(pack),
            bytes,
        })
    }
}

fn pack(parts: Vec<Bytes>) -> Vec<Bytes> {
    let mut arena = Vec::with_capacity(parts.iter().map(Bytes::len).sum());
    for part in &parts {
        arena.extend_from_slice(part);
    }
    let arena = Bytes::from(arena);
    let mut offset = 0;
    parts
        .iter()
        .map(|part| {
            let out = arena.slice(offset..offset + part.len());
            offset += part.len();
            out
        })
        .collect()
}

fn decode(dtype: crate::VectorType, bytes: &[u8], out: &mut Vec<f32>) {
    out.clear();
    match dtype {
        crate::VectorType::F16 => crate::index::extend_f16(bytes, out),
        crate::VectorType::F32 => out.extend(
            bytes
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b)),
        ),
        crate::VectorType::I8 => out.extend(bytes.iter().map(|b| *b as i8 as f32)),
    }
}

impl<R: RangeRead> ResidentIndex<R> {
    /// Bytes charged to the decoded cache, including metadata and both arenas.
    pub fn resident_bytes(&self) -> u64 {
        self.bytes
    }

    /// Original index, for sparse or asynchronous reranking without resident full vectors.
    pub fn index(&self) -> &Index<R> {
        &self.index
    }

    /// Scan selected clusters on this thread using the ordinary bounded fast-scan algorithm.
    pub fn scan<'a>(
        &self,
        query: &RotatedQuery,
        clusters: &[usize],
        filter: Option<&crate::RoaringBitmap>,
        k: usize,
        scratch: &'a mut ScanScratch,
    ) -> Result<&'a [Candidate]> {
        if query.rotated.len() != self.index.rq.padded || query.seed != self.index.metadata.seed {
            return Err(crate::Error::InvalidInput(
                "query rotated for another index".into(),
            ));
        }
        self.index.scan_into(
            query,
            clusters,
            |_, cid| &self.codes[cid],
            filter,
            0,
            (k > 0).then_some(k),
            scratch,
        )
    }

    /// Score one candidate from the validated resident vector arena, retaining f32 query and
    /// distance semantics. The caller supplies a reusable decode buffer.
    pub fn distance(
        &self,
        query: &[f32],
        candidate: &Candidate,
        decoded: &mut Vec<f32>,
    ) -> Result<f32> {
        crate::distance::validate(query, self.index.dims())?;
        self.distance_validated(query, candidate, decoded)
    }

    fn distance_validated(
        &self,
        query: &[f32],
        candidate: &Candidate,
        decoded: &mut Vec<f32>,
    ) -> Result<f32> {
        let vectors = self
            .vectors
            .as_ref()
            .ok_or_else(|| crate::format::bad("vectors are not resident"))?;
        let cid = candidate.cluster as usize;
        let width = self.index.dims() * self.index.vector_type().width();
        let start = candidate.ordinal as usize * width;
        let bytes = vectors
            .get(cid)
            .and_then(|v| v.get(start..start + width))
            .ok_or_else(|| crate::format::bad("resident vector location outside cluster"))?;
        decode(self.index.vector_type(), bytes, decoded);
        Ok(self.index.metric().distance_unchecked(query, decoded))
    }
}

/// Scratch owned by one synchronous query worker. Buffers grow only when the corpus or
/// query limits grow; repeat queries retain all capacities.
pub struct ResidentQueryScratch {
    pub(crate) normalized: Vec<f32>,
    pub(crate) rotated: RotatedQuery,
    pub(crate) plane: crate::rabitq::PlaneQuery,
    pub(crate) distances: Vec<f32>,
    order: Vec<u64>,
    chosen: Vec<Vec<usize>>,
    scan: ScanScratch,
    candidates: Vec<(usize, Candidate)>,
    uppers: Vec<f32>,
    decoded: Vec<f32>,
    hits: Vec<(u32, f32)>,
}
impl Default for ResidentQueryScratch {
    fn default() -> Self {
        Self {
            normalized: Vec::new(),
            rotated: RotatedQuery {
                rotated: Vec::new(),
                rotated32: Vec::new(),
                zero: false,
                seed: 0,
            },
            plane: Default::default(),
            distances: Vec::new(),
            order: Vec::new(),
            chosen: Vec::new(),
            scan: Default::default(),
            candidates: Vec::new(),
            uppers: Vec::new(),
            decoded: Vec::new(),
            hits: Vec::new(),
        }
    }
}
impl Directory {
    /// Identical unfiltered, fully live slab-tier probe plan, bounded scans, and reranking on
    /// the calling thread. The slabs share global row IDs. No I/O or runtime is used.
    /// `nprobe` has the engine's square-root scaling, and `rerank_factor` caps exact work.
    #[allow(clippy::too_many_arguments)]
    pub fn search_resident<'a, R: RangeRead>(
        &self,
        indexes: &[ResidentIndex<R>],
        query: &[f32],
        nprobe: usize,
        rerank_factor: usize,
        k: usize,
        scratch: &'a mut ResidentQueryScratch,
    ) -> Result<&'a [(u32, f32)]> {
        if indexes.len() != self.segments().len() || self.segments().iter().any(|s| s.flat) {
            return Err(crate::Error::InvalidInput(
                "resident search requires every non-flat slab".into(),
            ));
        }
        self.prepare_resident(query, scratch)?;
        let s = scratch;
        s.hits.clear();
        if k == 0 {
            return Ok(&s.hits);
        }
        let entries = self.entries();
        let rows: usize = entries.iter().map(|e| e.rows as usize).sum();
        let probes =
            ((nprobe as f64 * (entries.len() as f64 / (rows as f64).sqrt()).max(1.)).ceil()
                as usize)
                .min(entries.len());
        let target = k.saturating_mul(rerank_factor).min(rows);
        s.order.clear();
        s.order.extend(
            entries
                .iter()
                .enumerate()
                .filter(|(_, e)| e.rows > 0)
                .map(|(i, _)| {
                    let bits = s.distances[i].to_bits();
                    let key = if bits >> 31 == 1 {
                        !bits
                    } else {
                        bits | 1 << 31
                    };
                    (u64::from(key) << 32) | i as u64
                }),
        );
        let mut sorted = if s.order.len() > probes.saturating_mul(8).max(256) {
            let head = probes * 2 + 64;
            s.order.select_nth_unstable(head);
            s.order[..head].sort_unstable();
            head
        } else {
            s.order.sort_unstable();
            s.order.len()
        };
        s.chosen.resize_with(indexes.len(), Vec::new);
        for group in &mut s.chosen {
            group.clear();
        }
        let (mut count, mut picked_rows) = (0usize, 0usize);
        for at in 0..s.order.len() {
            if at == sorted {
                s.order[at..].sort_unstable();
                sorted = s.order.len();
            }
            let entry = (s.order[at] & u64::from(u32::MAX)) as usize;
            let e = &entries[entry];
            s.chosen[e.segment].push(entry);
            count += 1;
            picked_rows += e.rows as usize;
            if count >= probes && picked_rows >= target {
                break;
            }
        }
        s.candidates.clear();
        for (i, group) in s.chosen.iter_mut().enumerate() {
            if group.is_empty() {
                continue;
            }
            // The server scans each slab in directory order, with its own running cutoff.
            group.sort_unstable();
            for entry in group.iter_mut() {
                *entry = entries[*entry].cluster;
            }
            s.candidates.extend(
                indexes[i]
                    .scan(&s.rotated, group, None, k, &mut s.scan)?
                    .iter()
                    .map(|c| (i, *c)),
            );
        }
        if s.candidates.len() > k {
            s.uppers.clear();
            s.uppers
                .extend(s.candidates.iter().map(|(_, c)| c.distance + c.error));
            s.uppers.select_nth_unstable_by(k - 1, f32::total_cmp);
            let edge = s.uppers[k - 1];
            s.candidates.retain(|(_, c)| c.distance - c.error <= edge);
        }
        if s.candidates.len() > target {
            s.candidates.select_nth_unstable_by(target, |a, b| {
                a.1.distance
                    .total_cmp(&b.1.distance)
                    .then(a.0.cmp(&b.0))
                    .then(a.1.row.cmp(&b.1.row))
            });
            s.candidates.truncate(target);
        }
        for (i, candidate) in &s.candidates {
            s.hits.push((
                candidate.row,
                indexes[*i].distance_validated(query, candidate, &mut s.decoded)?,
            ));
        }
        s.hits
            .sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        s.hits.truncate(k);
        Ok(&s.hits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reused_rotation_and_center_ranking_are_bit_identical() {
        for dims in [17, 64, 128] {
            for metric in [
                crate::Metric::EuclideanSquared,
                crate::Metric::CosineDistance,
            ] {
                let rows: Vec<_> = (0..200)
                    .map(|i| {
                        (
                            i as u32,
                            (0..dims)
                                .map(|j| ((i * 73 + j * 19) % 101) as f32 / 10.)
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect();
                let blob = crate::IndexBuilder::new(dims, metric)
                    .unwrap()
                    .params(crate::BuildParams {
                        flat_threshold: 10,
                        ..Default::default()
                    })
                    .build(&rows)
                    .unwrap();
                let (ann, metadata) = crate::Fragment::build_ann(&blob).unwrap();
                let fragment = crate::Fragment::decode(&ann[..metadata as usize]).unwrap();
                let dir = Directory::decode(
                    &Directory::build(&[("s".into(), "k".into(), "k".into(), fragment)])
                        .unwrap()
                        .unwrap(),
                )
                .unwrap();
                let mut scratch = ResidentQueryScratch::default();
                for query in rows.iter().take(5).map(|r| &r.1).chain([&vec![0.; dims]]) {
                    let expected = dir.center_distances_on(query, false).unwrap();
                    dir.prepare_resident(query, &mut scratch).unwrap();
                    assert_eq!(expected, scratch.distances);
                    let rotated = dir.rotate_query(query).unwrap();
                    assert_eq!(rotated.rotated, scratch.rotated.rotated);
                    assert_eq!(rotated.rotated32, scratch.rotated.rotated32);
                    assert_eq!(rotated.zero, scratch.rotated.zero);
                }
            }
        }
    }
}
