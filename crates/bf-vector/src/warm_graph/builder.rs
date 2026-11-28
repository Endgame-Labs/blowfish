use super::{HotGraph, check, quant, vamana};
use crate::{Error, Result, rabitq::RaBitQ};
use half::f16;
use numkong::Dot;
/// Coarse regions whose medoids seed a walk, and the fewest nodes a graph needs to have them.
const REGIONS: usize = 256;
const MIN_SEEDED: usize = 4_096;
/// Streaming SQ8 collector for a disposable, whole-tier row graph.
/// Final code arrays are filled directly under the caller's memory reservation. Run collection
/// and `finish` on a bounded CPU pool. Original f16 rows are never retained or reread.
pub struct HotGraphBuilder {
    codes: quant::Codes,
    transform: RaBitQ,
    dims: usize,
    count: usize,
    rows: Vec<u32>,
    sum: Vec<f64>,
    first: Vec<f32>,
    norms: Vec<crate::CosineNorm>,
}
impl HotGraphBuilder {
    /// Reserve final SQ8 arrays without creating temporary files. At most `count` rows may
    /// be appended, in immutable source-leaf order. The caller reserves the complete build.
    pub fn new(dims: usize, count: usize) -> Result<Self> {
        if HotGraph::reservation(count, dims, 1).is_none() {
            return Err(Error::InvalidInput("unsupported graph source shape".into()));
        }
        Ok(Self {
            codes: quant::Codes {
                dims: dims.next_power_of_two(),
                values: Vec::with_capacity(count * dims.next_power_of_two()),
                scales: Vec::with_capacity(count),
                dot: quant::dot_kernel(),
            },
            transform: RaBitQ::new(dims, 42)?,
            dims,
            count,
            rows: Vec::with_capacity(count),
            sum: vec![0.; dims],
            first: Vec::new(),
            norms: Vec::with_capacity(count),
        })
    }
    /// Append little-endian f16 rows. Immutable tier coordinates are preserved explicitly;
    /// graph node IDs follow arrival order. Nonfinite and zero vectors are rejected.
    /// Cancellation is checked for each row. Drop the builder after a cancellation error.
    pub fn append(
        &mut self,
        rows: &[u32],
        packed: &[u8],
        cancelled: &(impl Fn() -> bool + Sync),
    ) -> Result<()> {
        if self.rows.len().saturating_add(rows.len()) > self.count
            || rows.len().checked_mul(self.dims * 2) != Some(packed.len())
        {
            return Err(Error::InvalidInput("invalid graph source rows".into()));
        }
        // Validate the entire append before changing its logical state.
        for row in packed.chunks_exact(self.dims * 2) {
            check(cancelled)?;
            let norm = row
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| f16::from_le_bytes(*b).to_f64().powi(2))
                .sum::<f64>();
            if !norm.is_finite() || norm == 0. {
                return Err(Error::InvalidInput(
                    "graph requires finite, nonzero rows".into(),
                ));
            }
        }
        for row in packed.chunks_exact(self.dims * 2) {
            check(cancelled)?;
            let values = row
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| f16::from_le_bytes(*b))
                .collect::<Vec<_>>();
            // SAFETY: both transparent half wrappers have identical layout and alignment.
            let half = unsafe {
                std::slice::from_raw_parts(values.as_ptr().cast::<numkong::f16>(), values.len())
            };
            let original = values.iter().map(|x| x.to_f32()).collect::<Vec<_>>();
            self.norms.push(crate::CosineNorm::new(&original)?);
            let inv = 1. / numkong::f16::dot(half, half).unwrap().sqrt();
            let normalized = if inv.is_finite() && inv > 0. {
                values.iter().map(|x| x.to_f32() * inv).collect::<Vec<_>>()
            } else {
                let norm = values
                    .iter()
                    .map(|x| x.to_f64().powi(2))
                    .sum::<f64>()
                    .sqrt();
                values
                    .iter()
                    .map(|x| (x.to_f64() / norm) as f32)
                    .collect::<Vec<_>>()
            };
            if self.first.is_empty() {
                self.first.clone_from(&normalized);
            }
            for (sum, value) in self.sum.iter_mut().zip(&normalized) {
                *sum += f64::from(*value);
            }
            let (values, scale) = quant::encode(&self.transform.rotate(&normalized));
            self.codes.scales.push(scale);
            self.codes.values.extend(values);
        }
        self.rows.extend_from_slice(rows);
        Ok(())
    }
    /// Build one monolithic R32/L64 SQ8 Vamana graph, with two passes and bounded adjacency.
    /// Construction checks cancellation between source rows, graph points, and bounded phases.
    pub fn finish(mut self, cancelled: &(impl Fn() -> bool + Sync)) -> Result<HotGraph> {
        check(cancelled)?;
        if self.rows.len() != self.count
            || self.codes.values.len() != self.count * self.codes.dims
            || self.codes.scales.len() != self.count
            || self.norms.len() != self.count
        {
            return Err(Error::InvalidInput("incomplete graph source".into()));
        }
        let mut sorted = self.rows.clone();
        sorted.sort_unstable();
        if sorted.windows(2).any(|p| p[0] == p[1]) {
            return Err(Error::InvalidInput("duplicate tier row".into()));
        }
        drop(sorted);
        let codes = self.codes;
        let norm = self.sum.iter().map(|x| x * x).sum::<f64>().sqrt();
        let center = if norm > 0. {
            self.sum.iter().map(|x| (x / norm) as f32).collect()
        } else {
            self.first.clone()
        };
        let (query, scale) = codes.prepare(&center);
        let mut best = (0, f32::INFINITY);
        for id in 0..self.count as u32 {
            check(cancelled)?;
            let distance = codes.score(&query, scale, id);
            if distance < best.1 {
                best = (id, distance);
            }
        }
        let bits = super::binary::Bits::new(&codes, cancelled)?;
        let nodes = vamana::build(&codes, &bits, best.0, cancelled)?;
        // The global medoid first, then the coarse regions' medoids, for seeding a walk near its query.
        let mut entries = vec![best.0];
        if self.count >= MIN_SEEDED {
            entries.extend(
                bits.region_medoids(REGIONS.min(self.count / 16), cancelled)?
                    .into_iter()
                    .filter(|&id| id != best.0),
            );
        }
        let entry_bits = bits.gather(&entries);
        let mut links = Vec::with_capacity(self.count);
        let mut degrees = Vec::with_capacity(self.count);
        for (id, neighbors) in nodes.into_iter().enumerate() {
            check(cancelled)?;
            let neighbors = neighbors.into_inner();
            let mut row = [0; 34];
            let mut count = neighbors.as_slice().len();
            row[..count].copy_from_slice(neighbors.as_slice());
            for other in [(id + 1) % self.count, (id + self.count - 1) % self.count] {
                if other != id && !row[..count].contains(&(other as u32)) {
                    row[count] = other as u32;
                    count += 1;
                }
            }
            links.push(row);
            degrees.push(count as u8);
        }
        let rows = std::mem::take(&mut self.rows);
        // A dense row-to-node table when tier row numbers are within 4x the node count.
        let top = rows.iter().max().map_or(0, |m| *m as usize + 1);
        let mut nodes = Vec::new();
        if top <= self.count.saturating_mul(4) {
            nodes = vec![u32::MAX; top];
            for (node, row) in rows.iter().enumerate() {
                nodes[*row as usize] = node as u32;
            }
        }
        Ok(HotGraph {
            dims: self.dims,
            rows,
            nodes,
            links,
            degrees,
            entries,
            entry_bits,
            codes,
            bits,
            norms: std::mem::take(&mut self.norms),
        })
    }
}
