//! One-bit screening codes that stay resident for filtered scans. Each row keeps the signs of
//! its rotated, mean-centered coordinates (the same codes graph construction steers with) plus
//! one metadata word: the row's mean absolute centered coordinate and its popcount. A query is
//! rotated and quantized to a few bit planes, so a row's score is a handful of AND and popcount
//! passes over 128 bytes instead of an SQ8 dot product over 1,024.
use super::{binary::mean, quant::Codes};
use rayon::prelude::*;

/// Coordinates kept per row: every rotated coordinate is exchangeable, so a wide code drops its
/// last 64 coordinates and the metadata word completes a 128-byte row at 1,024 dimensions.
fn coordinates(dims: usize) -> usize {
    if dims >= 512 { dims - 64 } else { dims }
}
pub(super) struct Screen {
    words: Vec<u64>,
    /// Words of sign bits per row; the row's metadata word follows them.
    per: usize,
}
/// A query quantized to bit planes over the screen's coordinates.
pub(super) struct ScreenQuery {
    planes: Vec<Vec<u64>>,
    lo: f32,
    step: f32,
    /// `lo * coordinates + step * sum(levels)`: the quantized query's sum.
    total: f32,
}
impl Screen {
    pub(super) fn new(
        codes: &Codes,
        cancelled: &(impl Fn() -> bool + Sync),
    ) -> crate::Result<Self> {
        let dims = codes.dims;
        let coords = coordinates(dims);
        let per = coords.div_ceil(64);
        let n = codes.scales.len();
        let mean = mean(codes, cancelled)?;
        let mut words = vec![0u64; n * (per + 1)];
        words
            .par_chunks_mut(per + 1)
            .zip(codes.values.par_chunks(dims))
            .zip(codes.scales.par_iter())
            .for_each(|((out, row), &scale)| {
                if cancelled() {
                    return;
                }
                let mut magnitude = 0f32;
                for (i, (&x, &m)) in row.iter().zip(&mean).take(coords).enumerate() {
                    let centered = f32::from(x) * scale - m;
                    magnitude += centered.abs();
                    if centered > 0. {
                        out[i / 64] |= 1 << (i % 64);
                    }
                }
                let ones = out[..per].iter().map(|w| w.count_ones()).sum::<u32>();
                out[per] = u64::from((magnitude / coords as f32).to_bits()) | u64::from(ones) << 32;
            });
        super::check(cancelled)?;
        Ok(Self { words, per })
    }
    /// Bytes retained.
    pub(super) fn bytes(&self) -> usize {
        self.words.capacity() * 8
    }
    /// Quantize a rotated, normalized query to `bits` planes (1 to 8) clipped at `clip` standard
    /// deviations of its coordinates.
    pub(super) fn query(&self, rotated: &[f32], bits: usize, clip: f32) -> ScreenQuery {
        let coords = self.per * 64;
        let coords = coords.min(rotated.len());
        let used = &rotated[..coords];
        let deviation = (used.iter().map(|x| x * x).sum::<f32>() / used.len() as f32).sqrt();
        let top = clip * deviation;
        let levels = ((1u32 << bits) - 1) as f32;
        let step = 2. * top / levels;
        let mut planes = vec![vec![0u64; self.per]; bits];
        let mut sum = 0f32;
        for (i, x) in used.iter().enumerate() {
            let level = ((x.clamp(-top, top) + top) / step).round();
            sum += level;
            for (p, plane) in planes.iter_mut().enumerate() {
                if (level as u32 >> p) & 1 == 1 {
                    plane[i / 64] |= 1 << (i % 64);
                }
            }
        }
        ScreenQuery {
            planes,
            lo: -top,
            step,
            total: -top * coords as f32 + step * sum,
        }
    }
    /// Distance-like score (lower is nearer): minus the estimated dot product with the
    /// centered row, which differs from the cosine distance by a per-query constant.
    pub(super) fn score(&self, q: &ScreenQuery, id: u32) -> f32 {
        let start = id as usize * (self.per + 1);
        let row = &self.words[start..start + self.per + 1];
        let bits = &row[..self.per];
        let meta = row[self.per];
        let magnitude = f32::from_bits(meta as u32);
        let ones = (meta >> 32) as f32;
        let mut weighted = 0u32;
        for (p, plane) in q.planes.iter().enumerate() {
            let count = bits
                .iter()
                .zip(plane)
                .map(|(a, b)| (a & b).count_ones())
                .sum::<u32>();
            weighted += count << p;
        }
        let signed = 2. * (q.lo * ones + q.step * weighted as f32) - q.total;
        -magnitude * signed
    }
    pub(super) fn prefetch(&self, id: u32) {
        let start = id as usize * (self.per + 1);
        super::quant::prefetch_lines(self.words[start..].as_ptr().cast(), (self.per + 1) * 8);
    }
}
