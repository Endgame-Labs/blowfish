//! Residual one-bit quantization with RaBitQ's per-vector inner-product correction.
//!
//! Reference: https://arxiv.org/abs/2405.12497 (Section 3.2).
//! We use three seeded signed Hadamard transforms, rather than a dense Haar
//! rotation. Consequently, we expose a deterministic Cauchy–Schwarz bound,
//! rather than claiming the paper's Haar-specific probabilistic guarantee.

/// An approximate squared distance and a conservative absolute error bound.
#[derive(Clone, Copy, Debug)]
pub struct DistanceEstimate {
    /// Corrected squared Euclidean distance estimate.
    pub distance: f64,
    /// Deterministic absolute error bound (including a floating-point margin).
    pub error_bound: f64,
}

/// A one-bit residual code and its per-vector correction factors.
#[derive(Clone, Debug)]
pub struct RaBitQCode {
    /// Sign bits in transformed space, least significant bit first.
    pub bits: Vec<u8>,
    /// Length of the original residual.
    pub norm: f64,
    /// Inner product of the normalized residual and its unit sign code.
    pub alpha: f64,
}

/// Deterministic fast orthogonal transform, padded to a power of two.
#[derive(Clone, Debug)]
pub struct RaBitQ {
    pub(crate) dims: usize,
    pub(crate) padded: usize,
    signs: Vec<Vec<f64>>,
}

impl RaBitQ {
    /// Create a seeded transform. Supports dimensions 1 through 65,536.
    pub fn new(dims: usize, seed: u64) -> crate::Result<Self> {
        if dims == 0 || dims > 65_536 {
            return Err(crate::Error::InvalidInput(
                "dimensions must be in 1..=65536".into(),
            ));
        }
        let padded = dims.next_power_of_two();
        let mut rng = Rng(seed);
        let signs = (0..3)
            .map(|_| {
                (0..padded)
                    .map(|_| if rng.next() & 1 == 0 { -1. } else { 1. })
                    .collect()
            })
            .collect();
        Ok(Self {
            dims,
            padded,
            signs,
        })
    }

    /// Quantize `vector - centroid`, retaining the norm and inner-product factor.
    pub fn encode(&self, vector: &[f32], centroid: &[f32]) -> crate::Result<RaBitQCode> {
        crate::distance::validate(vector, self.dims)?;
        crate::distance::validate(centroid, self.dims)?;
        Ok(self.encode_unchecked(vector, centroid))
    }

    pub(crate) fn encode_unchecked(&self, v: &[f32], c: &[f32]) -> RaBitQCode {
        let rotated = self.residual(v, c);
        let norm = rotated.iter().map(|x| x * x).sum::<f64>().sqrt();
        let mut bits = vec![0u8; self.padded.div_ceil(8)];
        for (i, x) in rotated.iter().enumerate() {
            if *x >= 0. {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        let alpha = if norm == 0. {
            1.
        } else {
            rotated.iter().map(|x| x.abs()).sum::<f64>() / (norm * (self.padded as f64).sqrt())
        };
        RaBitQCode {
            bits,
            norm,
            alpha: alpha.clamp(f64::MIN_POSITIVE, 1.),
        }
    }

    /// Estimate squared Euclidean distance to a query, with a deterministic bound.
    pub fn estimate(
        &self,
        code: &RaBitQCode,
        query: &[f32],
        centroid: &[f32],
    ) -> crate::Result<DistanceEstimate> {
        crate::distance::validate(query, self.dims)?;
        crate::distance::validate(centroid, self.dims)?;
        if code.bits.len() != self.padded.div_ceil(8)
            || !code.norm.is_finite()
            || code.norm < 0.
            || !code.alpha.is_finite()
            || code.alpha <= 0.
            || code.alpha > 1.
        {
            return Err(crate::Error::InvalidInput("invalid RaBitQ code".into()));
        }
        Ok(self.estimate_rotated(code, &self.residual(query, centroid)))
    }

    pub(crate) fn residual(&self, v: &[f32], c: &[f32]) -> Vec<f64> {
        let mut result = vec![0.; self.padded];
        for i in 0..self.dims {
            result[i] = v[i] as f64 - c[i] as f64;
        }
        for signs in &self.signs {
            for (x, sign) in result.iter_mut().zip(signs) {
                *x *= sign;
            }
            let mut stride = 1;
            while stride < self.padded {
                for chunk in result.chunks_exact_mut(2 * stride) {
                    let (a, b) = chunk.split_at_mut(stride);
                    for (a, b) in a.iter_mut().zip(b) {
                        let sum = *a + *b;
                        *b = *a - *b;
                        *a = sum;
                    }
                }
                stride *= 2;
            }
            let scale = 1. / (self.padded as f64).sqrt();
            for x in &mut result {
                *x *= scale;
            }
        }
        result
    }

    pub(crate) fn estimate_rotated(&self, code: &RaBitQCode, q: &[f64]) -> DistanceEstimate {
        // Byte lookup tables allow summing eight signed coordinates per code byte.
        let dot = q
            .iter()
            .enumerate()
            .map(|(i, q)| {
                if code.bits[i / 8] & (1 << (i % 8)) != 0 {
                    *q
                } else {
                    -*q
                }
            })
            .sum::<f64>()
            / (self.padded as f64).sqrt();
        let q2 = q.iter().map(|x| x * x).sum::<f64>();
        let distance = code.norm * code.norm + q2 - 2. * code.norm * dot / code.alpha;
        let perpendicular = q2.sqrt();
        let angular_error = (1. - code.alpha * code.alpha).max(0.).sqrt() / code.alpha;
        let margin = 1e-10 * (code.norm * code.norm + q2 + distance.abs() + 1.);
        DistanceEstimate {
            distance,
            error_bound: 2. * code.norm * angular_error * perpendicular + margin,
        }
    }
}

// SplitMix64: a fixed algorithm keeps builds reproducible across dependency versions.
pub(crate) struct Rng(pub u64);
impl Rng {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #[test]
        fn bound_contains_truth(values in prop::collection::vec((-10f32..10.,-10f32..10.,-10f32..10.), 1..257)) {
            let v: Vec<_> = values.iter().map(|x|x.0).collect();
            let q: Vec<_> = values.iter().map(|x|x.1).collect();
            let c: Vec<_> = values.iter().map(|x|x.2).collect();
            let rq = RaBitQ::new(v.len(), 17).unwrap();
            let code = rq.encode(&v,&c).unwrap();
            let e = rq.estimate(&code,&q,&c).unwrap();
            let truth: f64 = v.iter().zip(&q).map(|(a,b)|(*a as f64-*b as f64).powi(2)).sum();
            prop_assert!((e.distance-truth).abs() <= e.error_bound);
        }
    }
    #[test]
    fn zero_residual_and_padding() {
        let rq = RaBitQ::new(3, 0).unwrap();
        let code = rq.encode(&[2., 3., 4.], &[2., 3., 4.]).unwrap();
        let e = rq.estimate(&code, &[1., 2., 3.], &[2., 3., 4.]).unwrap();
        assert!((e.distance - 3.).abs() < 1e-12);
        assert!(
            rq.estimate(
                &RaBitQCode {
                    bits: vec![],
                    ..code
                },
                &[0.; 3],
                &[0.; 3]
            )
            .is_err()
        );
    }
}
