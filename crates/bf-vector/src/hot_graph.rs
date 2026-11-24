//! Experimental RAM-only graph for immutable hot tiers. No stored format is added.
//!
//! This is a query-path prototype, not the production index selector. The builder deliberately
//! retains the f16 corpus, so memory accounting must include vectors, codes, links, and scratch.
#[allow(dead_code)]
#[path = "../examples/g3/data.rs"]
mod data;
#[allow(dead_code)]
#[path = "../examples/g3/graph.rs"]
mod graph;
#[allow(dead_code)]
#[path = "../examples/g3/quant.rs"]
mod quant;
use crate::{Error, Result, RoaringBitmap, rabitq};
use half::f16;
use std::sync::Arc;

/// Scorer used while walking the same exact-built row graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HotScorer {
    /// Native f16 distances, followed by f32-query exact rerank for response distances.
    F16,
    /// Rotated eight-bit scalar codes, followed by exact f16 rerank.
    EightBit,
}
/// Construction parameters for the disposable prototype.
#[derive(Clone, Copy, Debug)]
pub struct HotBuildParams {
    /// Upper-layer degree; the base layer has at most twice this many links.
    pub degree: usize,
    /// HNSW construction beam.
    pub construction_ef: usize,
    /// Scoring representation retained for queries.
    pub scorer: HotScorer,
}
impl Default for HotBuildParams {
    fn default() -> Self {
        Self {
            degree: 16,
            construction_ef: 100,
            scorer: HotScorer::EightBit,
        }
    }
}
/// Immutable RAM accelerator, with dense graph nodes mapped to original tier row numbers.
pub struct HotGraph {
    vectors: Arc<data::Vectors>,
    graph: graph::Graph,
    codes: Option<quant::Codes>,
    rows: Vec<u32>,
}
/// Reusable per-worker visited bytes and heaps.
pub struct HotScratch(graph::Search);
impl HotGraph {
    /// Read the existing G3 scratch encodings for a query-only, cross-machine experiment.
    /// This is not a persisted index format or a production cache loader.
    pub fn load_experiment(dims: usize, rows: Vec<u32>, root: &std::path::Path) -> Result<Self> {
        if dims == 0 || dims > 65_536 || rows.is_empty() {
            return Err(Error::InvalidInput("invalid G3 experiment shape".into()));
        }
        let mut sorted = rows.clone();
        sorted.sort_unstable();
        if sorted.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::InvalidInput("duplicate G3 experiment row".into()));
        }
        let graph = graph::Graph::read(&root.join("rows.graph"))
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        let codes = quant::Codes::read(&root.join("rows.graph.codes8"))
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        if graph.levels.len() != rows.len()
            || codes.scales.len() != rows.len()
            || codes.dims != dims.next_power_of_two()
            || dims == 0
        {
            return Err(Error::InvalidInput(
                "incompatible G3 experiment artifacts".into(),
            ));
        }
        Ok(Self {
            vectors: Arc::new(data::Vectors {
                dims,
                values: vec![],
                inv_norms: vec![],
            }),
            graph,
            codes: Some(codes),
            rows,
        })
    }
    /// Write only the already-defined, private G3 scratch graph and eight-bit code encodings.
    /// Callers own source-identity checking, row mapping, cleanup, and the query-only import.
    pub fn save_experiment(&self, root: &std::path::Path) -> Result<()> {
        let codes = self
            .codes
            .as_ref()
            .ok_or_else(|| Error::InvalidInput("eight-bit codes required".into()))?;
        self.graph
            .write(&root.join("rows.graph"))
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        codes
            .write(&root.join("rows.graph.codes8"))
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        Ok(())
    }
    /// Build from little-endian packed f16 rows read from an immutable tier.
    ///
    /// `rows[i]` is the tier row number of packed vector `i`. This first prototype supports
    /// finite, nonzero cosine vectors only; callers keep IVF serving on unsupported inputs.
    pub fn build(
        dims: usize,
        rows: Vec<u32>,
        packed: Vec<u8>,
        params: HotBuildParams,
    ) -> Result<Self> {
        if dims == 0
            || dims > 65_536
            || rows.is_empty()
            || rows.len() > u32::MAX as usize
            || rows.len().checked_mul(dims).and_then(|n| n.checked_mul(2)) != Some(packed.len())
            || !(2..=64).contains(&params.degree)
            || params.construction_ef < params.degree
        {
            return Err(Error::InvalidInput(
                "invalid hot graph shape or construction parameters".into(),
            ));
        }
        let mut sorted = rows.clone();
        sorted.sort_unstable();
        if sorted.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::InvalidInput("duplicate hot graph row".into()));
        }
        drop(sorted);
        for row in packed.chunks_exact(dims * 2) {
            let values = row.as_chunks::<2>().0;
            if values
                .iter()
                .any(|b| !f16::from_bits(u16::from_le_bytes(*b)).is_finite())
                || values.iter().all(|b| u16::from_le_bytes(*b) & 0x7fff == 0)
            {
                return Err(Error::InvalidInput(
                    "hot graph prototype requires finite, nonzero rows".into(),
                ));
            }
        }
        let vectors = Arc::new(data::Vectors::from_matrix(data::Matrix {
            rows: rows.len(),
            cols: dims,
            bytes: packed,
        }));
        let graph = graph::Graph::build(vectors.clone(), params.degree, params.construction_ef);
        let codes = (params.scorer == HotScorer::EightBit).then(|| quant::Codes::build(&vectors));
        Ok(Self {
            vectors,
            graph,
            codes,
            rows,
        })
    }
    /// Number of indexed tier rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    /// Whether the accelerator has no indexed rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    /// Query dimension.
    pub fn dims(&self) -> usize {
        self.vectors.dims
    }
    /// Retained allocation capacities, excluding allocator metadata and worker scratch.
    pub fn resident_bytes(&self) -> u64 {
        let links = self
            .graph
            .levels
            .iter()
            .map(|levels| {
                levels.capacity() * std::mem::size_of::<Vec<u32>>()
                    + levels
                        .iter()
                        .map(|level| level.capacity() * 4)
                        .sum::<usize>()
            })
            .sum::<usize>();
        let codes = self
            .codes
            .as_ref()
            .map_or(0, |c| c.values.capacity() + c.scales.capacity() * 4);
        (self.vectors.values.capacity() * 2
            + self.vectors.inv_norms.capacity() * 4
            + self.rows.capacity() * 4
            + self.graph.levels.capacity() * std::mem::size_of::<Vec<Vec<u32>>>()
            + links
            + codes) as u64
    }
    /// Allocate one reusable visited array and heap pair.
    pub fn scratch(&self) -> HotScratch {
        HotScratch(graph::Search::new(self.len()))
    }
    /// Release exact vectors after eight-bit construction. Exact rerank then belongs to the caller.
    /// Returns false for an f16 navigation graph, which still needs its source vectors.
    pub fn release_exact_vectors(&mut self) -> bool {
        if self.codes.is_none() {
            return false;
        }
        self.vectors = Arc::new(data::Vectors {
            dims: self.dims(),
            values: Vec::new(),
            inv_norms: Vec::new(),
        });
        true
    }
    /// Approximate candidates in original tier row coordinates, before exact storage rerank.
    /// Masked nodes remain navigable, and do not occupy the returned candidate budget.
    pub fn candidates(
        &self,
        query: &[f32],
        ef: usize,
        count: usize,
        live: Option<&RoaringBitmap>,
        scratch: &mut HotScratch,
    ) -> Result<Vec<u32>> {
        crate::distance::validate(query, self.dims())?;
        if ef == 0 || count == 0 || query.iter().all(|x| *x == 0.) {
            return Err(Error::InvalidInput(
                "unsupported hot graph candidate budget".into(),
            ));
        }
        let q = data::normalized(query);
        let ef = ef.max(count).min(self.len());
        let (mut hits, _) = if let Some(c) = &self.codes {
            let (q8, scale) = c.prepare(&q);
            self.graph
                .search(ef, &mut scratch.0, |id| c.score(&q8, scale, id))
        } else {
            let half: Vec<_> = q.iter().map(|x| f16::from_f32(*x)).collect();
            self.graph
                .search(ef, &mut scratch.0, |id| self.vectors.query(&half, id))
        };
        hits.retain(|h| live.is_none_or(|mask| mask.contains(self.rows[h.id as usize])));
        hits.truncate(count);
        Ok(hits.into_iter().map(|h| self.rows[h.id as usize]).collect())
    }
    /// Return exact cosine scores for reached, currently live tier rows.
    ///
    /// Masked nodes remain navigable. They are removed before selecting the exact rerank pool.
    /// The caller falls back to IVF if masking leaves fewer than the requested number of hits.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        rerank: usize,
        live: Option<&RoaringBitmap>,
        scratch: &mut HotScratch,
    ) -> Result<Vec<(u32, f32)>> {
        crate::distance::validate(query, self.dims())?;
        if self.vectors.values.is_empty() {
            return Err(Error::InvalidInput(
                "exact vectors were released; rerank candidates through storage".into(),
            ));
        }
        if k == 0 || rerank < k || ef == 0 || query.iter().all(|x| *x == 0.) {
            return Err(Error::InvalidInput(
                "unsupported hot graph query budget or zero query".into(),
            ));
        }
        let q = data::normalized(query);
        let ef = ef.max(rerank).max(k).min(self.len());
        let (mut hits, _) = if let Some(c) = &self.codes {
            let (q8, scale) = c.prepare(&q);
            self.graph
                .search(ef, &mut scratch.0, |id| c.score(&q8, scale, id))
        } else {
            let half: Vec<_> = q.iter().map(|x| f16::from_f32(*x)).collect();
            self.graph
                .search(ef, &mut scratch.0, |id| self.vectors.query(&half, id))
        };
        hits.retain(|h| live.is_none_or(|mask| mask.contains(self.rows[h.id as usize])));
        hits.truncate(rerank);
        for hit in &mut hits {
            hit.distance = self.vectors.rerank(&q, hit.id).clamp(0., 2.);
        }
        hits.sort_unstable();
        hits.truncate(k);
        Ok(hits
            .into_iter()
            .map(|h| (self.rows[h.id as usize], h.distance))
            .collect())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn global_rows_and_live_masks_survive_exact_rerank() {
        let dims = 16;
        let rows: Vec<_> = (0..160).map(|i| 1000 + i * 3).collect();
        let mut state = 7u64;
        let source: Vec<Vec<f32>> = (0..rows.len())
            .map(|_| {
                (0..dims)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        ((state >> 40) as f32 / 16_777_216.) * 2. - 1.
                    })
                    .collect()
            })
            .collect();
        let packed = source
            .iter()
            .flatten()
            .flat_map(|x| f16::from_f32(*x).to_bits().to_le_bytes())
            .collect();
        let mut g = HotGraph::build(dims, rows.clone(), packed, HotBuildParams::default()).unwrap();
        let mut scratch = g.scratch();
        let live: RoaringBitmap = rows.iter().copied().filter(|id| *id != rows[7]).collect();
        let hits = g
            .search(&source[7], 10, 160, 80, Some(&live), &mut scratch)
            .unwrap();
        assert_eq!(hits.len(), 10);
        assert!(
            hits.iter()
                .all(|(id, d)| live.contains(*id) && d.is_finite())
        );
        let packed: Vec<_> = source
            .iter()
            .map(|v| {
                v.iter()
                    .map(|x| f16::from_f32(*x).to_f32())
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut exact: Vec<_> = rows
            .iter()
            .zip(packed)
            .filter(|(id, _)| live.contains(**id))
            .map(|(id, v)| {
                (
                    *id,
                    crate::Metric::CosineDistance
                        .distance(&source[7], &v)
                        .unwrap(),
                )
            })
            .collect();
        exact.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        assert_eq!(
            hits.iter().map(|h| h.0).collect::<Vec<_>>(),
            exact.iter().take(10).map(|h| h.0).collect::<Vec<_>>()
        );
        let candidates = g
            .candidates(&source[7], 160, 30, Some(&live), &mut scratch)
            .unwrap();
        let bytes = g.resident_bytes();
        assert!(g.release_exact_vectors());
        assert!(g.resident_bytes() < bytes);
        assert_eq!(
            g.candidates(&source[7], 160, 30, Some(&live), &mut scratch)
                .unwrap(),
            candidates
        );
        assert!(
            g.search(&source[7], 10, 160, 30, None, &mut scratch)
                .is_err()
        );
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../.scratch")
            .join(format!(
                "g3-vector-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
        std::fs::create_dir_all(&root).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        g.save_experiment(&root).unwrap();
        let loaded = HotGraph::load_experiment(dims, rows, &root).unwrap();
        assert_eq!(
            loaded
                .candidates(&source[7], 160, 30, Some(&live), &mut loaded.scratch())
                .unwrap(),
            candidates
        );
    }
    #[test]
    fn unsupported_rows_fail_before_construction() {
        assert!(HotGraph::build(2, vec![0], vec![0; 4], HotBuildParams::default()).is_err());
        assert!(HotGraph::build(2, vec![0, 0], vec![0; 8], HotBuildParams::default()).is_err());
    }
}
