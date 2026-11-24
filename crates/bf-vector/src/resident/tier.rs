//! Incrementally admitted code rows and validated sparse exact-vector rows.
use super::{ResidentPopulation, ResidentQueryScratch, decode, prune};
use crate::{Bytes, DecodedCluster, Directory, Index, Result, VectorType, format};
use std::sync::{Arc, OnceLock};

/// A vector row whose checksum and finite values were checked by the ordinary rerank.
/// Cloning shares only this row's bytes, never the surrounding cache block.
#[derive(Clone)]
pub struct ValidatedVector {
    row: u32,
    dims: usize,
    dtype: VectorType,
    bytes: Bytes,
}
impl ValidatedVector {
    pub(crate) fn new(row: u32, dims: usize, dtype: VectorType, bytes: &[u8]) -> Self {
        Self {
            row,
            dims,
            dtype,
            bytes: Bytes::copy_from_slice(bytes),
        }
    }
    /// Tier-global row number, scoped to the immutable tier being searched.
    pub fn row(&self) -> u32 {
        self.row
    }
    /// Payload, descriptor, and conservative cache-entry overhead, for byte-bounded admission.
    pub fn resident_bytes(&self) -> u64 {
        (self.bytes.len() + std::mem::size_of::<Self>() + 128) as u64
    }
}

/// Receives validated exact rows already fetched by a normal rerank. Implementations must
/// bound retained bytes and scope row numbers to the immutable tier generation.
pub trait VectorRecorder: Send + Sync {
    /// Record one row. This never asks the caller to fetch additional vectors.
    fn record(&self, vector: ValidatedVector);
}

struct Leaf {
    cluster: format::Cluster,
    codes: Bytes,
}
struct Slab {
    index: Index<Bytes>,
    leaves: Vec<OnceLock<Leaf>>,
    rows: Vec<u32>,
}

/// Exact winners and scan accounting from a synchronous compiled query.
pub struct CompiledHits<'a> {
    /// `(distance, slab index, tier-global row)`, including ties at the k-th distance.
    pub hits: &'a [(f32, usize, u32)],
    /// Rows in the selected leaves.
    pub scanned_rows: usize,
}

/// A generation-scoped compiled view of a fully live, unfiltered slab tier. Immutable leaf
/// payloads are admitted once from reads the ordinary query path already performed. Missing
/// leaves or exact rows make a query fall back without issuing I/O here.
pub struct CompiledTier {
    directory: Arc<Directory>,
    slabs: Vec<Slab>,
    population: ResidentPopulation,
}
impl CompiledTier {
    /// Maximum resident bytes for code payloads and metadata, before constructing a view.
    /// The caller reserves this amount in its decoded cache before admitting any leaf.
    pub fn reservation(directory: &Directory) -> u64 {
        let leaves: usize = directory.segments().iter().map(|s| s.clusters).sum();
        let rows: u64 = directory.entries().iter().map(|e| u64::from(e.rows)).sum();
        Self::code_bytes(directory)
            + directory.resident_bytes()
            + directory.entries().len() as u64 * std::mem::size_of::<usize>() as u64
            + rows * 16
            + leaves as u64 * (directory.dims().next_power_of_two() as u64 * 4 + 512)
            + directory.segments().len() as u64 * directory.dims().next_power_of_two() as u64 * 32
    }
    /// Encoded bytes for every leaf. Also a useful budget for a bounded sparse-vector cache.
    pub fn code_bytes(directory: &Directory) -> u64 {
        directory
            .entries()
            .iter()
            .map(|e| e.codes_range().end - e.codes_range().start)
            .sum()
    }
    /// Allocate metadata slots only. Flat or legacy non-resident-code layouts are unsupported;
    /// callers keep using their ordinary reader for those layouts.
    pub fn new(directory: Arc<Directory>) -> Result<Self> {
        if directory
            .segments()
            .iter()
            .any(|s| s.flat || !s.codes_in_fragment)
        {
            return Err(format::bad(
                "compiled tier requires non-flat resident-code slabs",
            ));
        }
        let mut slabs = Vec::with_capacity(directory.segments().len());
        for (i, segment) in directory.segments().iter().enumerate() {
            let mut rows = vec![0; segment.clusters];
            for e in directory.segment_entries(i) {
                rows[e.cluster] = e.rows;
            }
            slabs.push(Slab {
                index: directory.partial_index(i, Bytes::new())?.into_index(),
                leaves: (0..segment.clusters).map(|_| OnceLock::new()).collect(),
                rows,
            });
        }
        let population = ResidentPopulation::new(&directory);
        Ok(Self {
            directory,
            slabs,
            population,
        })
    }
    /// Admit a decoded leaf and its immutable code bytes. Racing admissions converge on one
    /// validated payload. The source block is copied so it cannot pin unrelated cache data.
    pub fn admit(
        &self,
        segment: usize,
        cluster: usize,
        decoded: &DecodedCluster,
        codes: &Bytes,
    ) -> Result<()> {
        let slab = self
            .slabs
            .get(segment)
            .ok_or_else(|| format::bad("compiled slab outside tier"))?;
        let slot = slab
            .leaves
            .get(cluster)
            .ok_or_else(|| format::bad("compiled leaf outside slab"))?;
        if slot.get().is_some() {
            return Ok(());
        }
        let stub = &slab.index.metadata.clusters[cluster];
        let data = &decoded.0;
        if data.len() != slab.rows[cluster] as usize
            || data.center.len() != slab.index.rq.padded
            || data.ids.as_ref().is_none_or(|ids| ids.len() != data.len())
        {
            return Err(format::bad("compiled leaf metadata mismatch"));
        }
        format::check_crc(codes, stub.codes_crc)?;
        crate::fastscan::check_factors(codes, data.len(), slab.index.rq.padded)?;
        let leaf = Leaf {
            cluster: format::Cluster {
                data: Some(data.clone()),
                parent: u32::MAX,
                codes: stub.codes.clone(),
                vectors: stub.vectors.clone(),
                codes_crc: stub.codes_crc,
                vectors_crc: stub.vectors_crc,
            },
            codes: Bytes::copy_from_slice(codes),
        };
        data.codes_checked
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = slot.set(leaf);
        Ok(())
    }
    /// Run one identical unfiltered ANN plan to completion on this worker. Returns `None`
    /// if any probed leaf or shortlisted exact row is absent. Returned winners include all
    /// ties at the global k-th distance, after the original per-slab exact top-k truncation.
    /// Scan accounting reports the rows in the selected leaves.
    #[allow(clippy::too_many_arguments)]
    pub fn try_search<'a>(
        &self,
        query: &[f32],
        nprobe: usize,
        rerank_factor: usize,
        k: usize,
        get_vector: impl Fn(u32) -> Option<ValidatedVector>,
        scratch: &'a mut ResidentQueryScratch,
    ) -> Result<Option<CompiledHits<'a>>> {
        scratch.winners.clear();
        if k == 0 {
            return Ok(Some(CompiledHits {
                hits: &scratch.winners,
                scanned_rows: 0,
            }));
        }
        let target = self.directory.resident_plan(
            query,
            nprobe,
            rerank_factor,
            k,
            Some(&self.population),
            scratch,
        )?;
        let s = scratch;
        if s.chosen.iter().enumerate().any(|(i, cs)| {
            cs.iter()
                .any(|&cid| self.slabs[i].leaves[cid].get().is_none())
        }) {
            return Ok(None);
        }
        s.candidates.clear();
        let mut upper_bound = f32::INFINITY;
        for (i, group) in s.chosen.iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let slab = &self.slabs[i];
            let leaf = |cid: usize| slab.leaves[cid].get().expect("checked admitted leaf");
            let found = slab.index.scan_with(
                &s.rotated,
                group,
                |_, cid| &leaf(cid).codes,
                |cid| &leaf(cid).cluster,
                None,
                0,
                Some((k, upper_bound)),
                &mut s.scan,
            )?;
            // Publish only bounds supported by k witnesses from this query. Scans run
            // sequentially here, so a local value shares the cut without an atomic.
            if found.len() >= k {
                s.uppers.clear();
                s.uppers.extend(found.iter().map(|c| c.distance + c.error));
                s.uppers.select_nth_unstable_by(k - 1, f32::total_cmp);
                upper_bound = upper_bound.min(s.uppers[k - 1]);
            }
            s.candidates.extend(found.iter().map(|c| (i, *c)));
        }
        prune(s, k, target);
        for (i, candidate) in &s.candidates {
            let Some(vector) = get_vector(candidate.row) else {
                return Ok(None);
            };
            let index = &self.slabs[*i].index;
            if vector.row != candidate.row
                || vector.dims != index.dims()
                || vector.dtype != index.vector_type()
            {
                return Ok(None);
            }
            decode(vector.dtype, &vector.bytes, &mut s.decoded);
            s.winners.push((
                index.metric().distance_unchecked(query, &s.decoded),
                *i,
                candidate.row,
            ));
        }
        s.winners
            .sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.total_cmp(&b.0)).then(a.2.cmp(&b.2)));
        let (mut previous, mut count) = (usize::MAX, 0usize);
        s.winners.retain(|w| {
            if w.1 != previous {
                previous = w.1;
                count = 0;
            }
            count += 1;
            count <= k
        });
        s.winners
            .sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
        let mut keep = k.min(s.winners.len());
        if keep > 0 {
            let edge = s.winners[keep - 1].0;
            while keep < s.winners.len() && s.winners[keep].0 == edge {
                keep += 1;
            }
        }
        s.winners.truncate(keep);
        Ok(Some(CompiledHits {
            hits: &s.winners,
            scanned_rows: s.scanned,
        }))
    }
}
