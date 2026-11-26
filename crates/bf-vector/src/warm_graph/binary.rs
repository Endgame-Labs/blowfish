//! One-bit navigation codes for graph construction. Each row keeps the signs of its rotated,
//! mean-centered coordinates. Hamming distance orders neighbors well enough to steer a beam,
//! and it reads 128 bytes per row instead of 1,024; construction scores only the nodes it
//! expands, and those the pruner needs, with the exact SQ8 kernel.
use super::quant::Codes;
use rayon::prelude::*;

/// Mean of the dequantized rows. Checks cancellation between row blocks.
pub(super) fn mean(
    codes: &Codes,
    cancelled: &(impl Fn() -> bool + Sync),
) -> crate::Result<Vec<f32>> {
    let dims = codes.dims;
    let n = codes.scales.len();
    let sums = codes
        .values
        .par_chunks(dims * 4096)
        .zip(codes.scales.par_chunks(4096))
        .map(|(rows, scales)| {
            let mut sum = vec![0f64; dims];
            if cancelled() {
                return sum;
            }
            for (row, &scale) in rows.chunks_exact(dims).zip(scales) {
                for (s, &x) in sum.iter_mut().zip(row) {
                    *s += f64::from(x) * f64::from(scale);
                }
            }
            sum
        })
        .reduce(
            || vec![0f64; dims],
            |mut a, b| {
                a.iter_mut().zip(b).for_each(|(a, b)| *a += b);
                a
            },
        );
    super::check(cancelled)?;
    Ok(sums.iter().map(|s| (s / n as f64) as f32).collect())
}

pub(super) struct Bits {
    words: Vec<u64>,
    per: usize,
}
impl Bits {
    /// Sign codes for every row of `codes`, centered on the mean of the dequantized rows.
    /// Checks cancellation between row blocks; a cancelled build returns an error.
    pub(super) fn new(
        codes: &Codes,
        cancelled: &(impl Fn() -> bool + Sync),
    ) -> crate::Result<Self> {
        let dims = codes.dims;
        let per = dims.div_ceil(64);
        let n = codes.scales.len();
        let mean = mean(codes, cancelled)?;
        let mut words = vec![0u64; n * per];
        words
            .par_chunks_mut(per)
            .zip(codes.values.par_chunks(dims))
            .zip(codes.scales.par_iter())
            .for_each(|((out, row), &scale)| {
                if cancelled() {
                    return;
                }
                for (i, (&x, &m)) in row.iter().zip(&mean).enumerate() {
                    if f32::from(x) * scale > m {
                        out[i / 64] |= 1 << (i % 64);
                    }
                }
            });
        super::check(cancelled)?;
        Ok(Self { words, per })
    }
    pub(super) fn row(&self, id: u32) -> &[u64] {
        let start = id as usize * self.per;
        &self.words[start..start + self.per]
    }
    pub(super) fn distance(&self, a: &[u64], id: u32) -> f32 {
        a.iter()
            .zip(self.row(id))
            .map(|(a, b)| (a ^ b).count_ones())
            .sum::<u32>() as f32
    }
    pub(super) fn prefetch(&self, id: u32) {
        let row = self.row(id);
        super::quant::prefetch_lines(row.as_ptr().cast(), row.len() * 8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hamming_distance_tracks_cosine_on_centered_codes() {
        let dims = 256;
        let n = 64;
        let mut seed = 99u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % 2001) as f32 / 1000. - 1.
        };
        // Row 0 is the anchor, row 1 a small perturbation of it, and the rest are independent.
        let anchor = (0..dims).map(|_| next() + 0.5).collect::<Vec<_>>();
        let mut rows = vec![anchor.clone()];
        rows.push(anchor.iter().map(|x| x + next() * 0.2).collect());
        for _ in 2..n {
            rows.push((0..dims).map(|_| next() + 0.5).collect());
        }
        let mut codes = Codes {
            dims,
            values: Vec::new(),
            scales: Vec::new(),
            dot: super::super::quant::dot_kernel(),
        };
        for row in &rows {
            let (code, scale) =
                super::super::quant::encode(&row.iter().map(|x| f64::from(*x)).collect::<Vec<_>>());
            codes.values.extend(code);
            codes.scales.push(scale);
        }
        let bits = Bits::new(&codes, &|| false).unwrap();
        let near = bits.distance(bits.row(0), 1);
        assert!(bits.distance(bits.row(0), 0) == 0.);
        for id in 2..n as u32 {
            assert!(near < bits.distance(bits.row(0), id), "row {id}");
        }
        assert!(Bits::new(&codes, &|| true).is_err());
    }
}
