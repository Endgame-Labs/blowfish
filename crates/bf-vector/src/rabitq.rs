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
        self.transform(result)
    }

    /// Rotate a vector (zero-padded to the transform width). The transform is linear and
    /// orthogonal, so `rotate(v) - rotate(c) == residual(v, c)` and distances are preserved.
    pub(crate) fn rotate(&self, v: &[f32]) -> Vec<f64> {
        let mut result = vec![0.; self.padded];
        for i in 0..self.dims {
            result[i] = v[i] as f64;
        }
        self.transform(result)
    }

    fn transform(&self, mut result: Vec<f64>) -> Vec<f64> {
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
        // The public reference path keeps all query coordinates at full precision.
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
        let query_norm = q2.sqrt();
        let angular_error = (1. - code.alpha * code.alpha).max(0.).sqrt() / code.alpha;
        let margin = 1e-10 * (code.norm * code.norm + q2 + distance.abs() + 1.);
        DistanceEstimate {
            distance,
            error_bound: 2. * code.norm * angular_error * query_norm + margin,
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

/// Multiples of the typical inner-product error that a candidate's distance bound covers.
pub(crate) const ERROR_CONFIDENCE: f64 = 3.0;

/// Bits used to quantize each coordinate of the rotated residual query.
pub(crate) const QUERY_BITS: usize = 8;

/// A residual query quantized to `QUERY_BITS` bit planes, so that a one-bit code's inner
/// product costs a few popcounts per 64 dimensions instead of a table lookup per byte.
pub(crate) struct PlaneQuery {
    /// `planes[j * words + w]` holds bit `j` of the quantized coordinates `64w..64w+64`.
    planes: Vec<u64>,
    words: usize,
    delta: f64,
    vmin: f64,
    sum: f64,
    /// Squared norm of the rotated residual query.
    pub(crate) norm2: f64,
}
impl PlaneQuery {
    /// `q` and `c` are rotated, padded to the same length.
    pub(crate) fn new(q: &[f64], c: &[f32]) -> Self {
        let words = q.len().div_ceil(64);
        let mut vmin = f64::INFINITY;
        let mut vmax = f64::NEG_INFINITY;
        let mut norm2 = 0.;
        let mut sum = 0.;
        let r: Vec<f64> = q.iter().zip(c).map(|(q, c)| *q - *c as f64).collect();
        for x in &r {
            vmin = vmin.min(*x);
            vmax = vmax.max(*x);
            norm2 += x * x;
            sum += x;
        }
        let levels = ((1usize << QUERY_BITS) - 1) as f64;
        let delta = if vmax > vmin {
            (vmax - vmin) / levels
        } else {
            0.
        };
        let mut planes = vec![0u64; QUERY_BITS * words];
        if delta > 0. {
            for (i, x) in r.iter().enumerate() {
                let v = ((x - vmin) / delta).round().clamp(0., levels) as u64;
                for j in 0..QUERY_BITS {
                    planes[j * words + i / 64] |= ((v >> j) & 1) << (i % 64);
                }
            }
        }
        Self {
            planes,
            words,
            delta,
            vmin,
            sum,
            norm2,
        }
    }

    /// Inner product of the residual query with the code's +-1 vector, divided by sqrt(padded).
    #[inline(always)]
    fn signed_dot(&self, bits: &[u8], padded: usize) -> f64 {
        let mut weighted = 0u64;
        let mut ones = 0u64;
        let full = bits.len() / 8;
        for w in 0..self.words {
            let word = if w < full {
                u64::from_le_bytes(bits[w * 8..w * 8 + 8].try_into().unwrap())
            } else {
                let mut buf = [0u8; 8];
                let rest = &bits[(w * 8).min(bits.len())..];
                buf[..rest.len()].copy_from_slice(rest);
                u64::from_le_bytes(buf)
            };
            ones += word.count_ones() as u64;
            for j in 0..QUERY_BITS {
                weighted += ((word & self.planes[j * self.words + w]).count_ones() as u64) << j;
            }
        }
        (2. * (self.delta * weighted as f64 + self.vmin * ones as f64) - self.sum)
            / (padded as f64).sqrt()
    }

    /// Estimated squared distance (plain, without the error bound).
    #[inline(always)]
    pub(crate) fn distance(&self, norm: f64, alpha: f64, bits: &[u8], padded: usize) -> f64 {
        let dot = self.signed_dot(bits, padded);
        norm * norm + self.norm2 - 2. * norm * dot / alpha
    }

    /// Score `rows` consecutive codes of `stride` bytes each (header of `head` bytes).
    /// Calls `emit(ordinal, distance, zero_flag)`.
    pub(crate) fn scan(
        &self,
        bytes: &[u8],
        rows: usize,
        compact: bool,
        padded: usize,
        emit: &mut dyn FnMut(usize, f64, f64, bool),
    ) -> crate::Result<()> {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("popcnt") {
                // SAFETY: the CPU feature was detected at runtime.
                return unsafe { self.scan_popcnt(bytes, rows, compact, padded, emit) };
            }
        }
        self.scan_impl(bytes, rows, compact, padded, emit)
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "popcnt")]
    unsafe fn scan_popcnt(
        &self,
        bytes: &[u8],
        rows: usize,
        compact: bool,
        padded: usize,
        emit: &mut dyn FnMut(usize, f64, f64, bool),
    ) -> crate::Result<()> {
        self.scan_impl(bytes, rows, compact, padded, emit)
    }

    #[inline(always)]
    fn scan_impl(
        &self,
        bytes: &[u8],
        rows: usize,
        compact: bool,
        padded: usize,
        emit: &mut dyn FnMut(usize, f64, f64, bool),
    ) -> crate::Result<()> {
        let code = padded.div_ceil(8);
        let head = if compact { 8 } else { 24 };
        let stride = head + code;
        if bytes.len() != rows * stride {
            return Err(crate::format::bad("code block length mismatch"));
        }
        for (ordinal, row) in bytes.chunks_exact(stride).enumerate() {
            let (norm, alpha, zero) = if compact {
                let norm = f32::from_le_bytes(row[0..4].try_into().unwrap()) as f64;
                // A negative alpha marks an all-zero input vector.
                let alpha = f32::from_le_bytes(row[4..8].try_into().unwrap()) as f64;
                (norm, alpha.abs(), alpha < 0.)
            } else {
                let norm = f64::from_le_bytes(row[0..8].try_into().unwrap());
                let alpha = f64::from_le_bytes(row[8..16].try_into().unwrap());
                let zero = u64::from_le_bytes(row[16..24].try_into().unwrap());
                if zero > 1 {
                    return Err(crate::format::bad("invalid quantization factors"));
                }
                (norm, alpha, zero == 1)
            };
            if !norm.is_finite() || norm < 0. || !alpha.is_finite() || alpha <= 0. || alpha > 1. {
                return Err(crate::format::bad("invalid quantization factors"));
            }
            // Statistical error bound of the distance estimate: the inner-product error of a
            // one-bit code is about sqrt(1 - a^2) / (a * sqrt(D - 1)) per unit of norms.
            let spread = ((1. - alpha * alpha).max(0.)).sqrt()
                / (alpha * ((padded - 1).max(1) as f64).sqrt());
            let bound = ERROR_CONFIDENCE * 2. * norm * self.norm2.sqrt() * spread;
            emit(
                ordinal,
                self.distance(norm, alpha, &row[head..], padded),
                bound,
                zero,
            );
        }
        Ok(())
    }
}

/// Query-local lookup tables; eight signs are accumulated per byte.
#[cfg(test)]
pub(crate) struct PreparedQuery {
    tables: Vec<[f64; 256]>,
    norm2: f64,
}
#[cfg(test)]
impl PreparedQuery {
    pub(crate) fn new(q: &[f64]) -> Self {
        let tables = q
            .chunks(8)
            .map(|chunk| {
                let mut table = [0.; 256];
                table[0] = -chunk.iter().sum::<f64>();
                for bits in 1usize..256 {
                    let bit = bits.trailing_zeros() as usize;
                    table[bits] =
                        table[bits & (bits - 1)] + 2. * chunk.get(bit).copied().unwrap_or(0.);
                }
                table
            })
            .collect();
        Self {
            tables,
            norm2: q.iter().map(|x| x * x).sum(),
        }
    }
    pub(crate) fn estimate(
        &self,
        norm: f64,
        alpha: f64,
        bits: &[u8],
        padded: usize,
    ) -> DistanceEstimate {
        // Independent accumulators avoid a long dependent floating-point sum.
        let (tables, tail) = self.tables.as_chunks::<4>();
        let (codes, code_tail) = bits.as_chunks::<4>();
        let mut sums = [0.; 4];
        for (t, b) in tables.iter().zip(codes) {
            sums[0] += t[0][b[0] as usize];
            sums[1] += t[1][b[1] as usize];
            sums[2] += t[2][b[2] as usize];
            sums[3] += t[3][b[3] as usize];
        }
        let dot = (sums.iter().sum::<f64>()
            + tail
                .iter()
                .zip(code_tail)
                .map(|(t, b)| t[*b as usize])
                .sum::<f64>())
            / (padded as f64).sqrt();
        let distance = norm * norm + self.norm2 - 2. * norm * dot / alpha;
        let margin = 1e-10 * (norm * norm + self.norm2 + distance.abs() + 1.);
        DistanceEstimate {
            distance,
            error_bound: 2. * norm * (1. - alpha * alpha).max(0.).sqrt() / alpha
                * self.norm2.sqrt()
                + margin,
        }
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
            let fast = PreparedQuery::new(&rq.residual(&q,&c)).estimate(code.norm,code.alpha,&code.bits,rq.padded);
            prop_assert!((fast.distance-e.distance).abs() < 1e-8*e.distance.abs().max(1.));
            let truth: f64 = v.iter().zip(&q).map(|(a,b)|(*a as f64-*b as f64).powi(2)).sum();
            prop_assert!((e.distance-truth).abs() <= e.error_bound);
        }
    }
    #[test]
    fn plane_estimate_tracks_reference() {
        let dims = 1024;
        let mut rng = Rng(7);
        let mut sample = |n: usize| -> Vec<f32> {
            (0..n)
                .map(|_| (rng.next() % 2000) as f32 / 1000. - 1.)
                .collect()
        };
        let (v, q, c) = (sample(dims), sample(dims), sample(dims));
        let rq = RaBitQ::new(dims, 3).unwrap();
        let code = rq.encode(&v, &c).unwrap();
        let reference = rq.estimate(&code, &q, &c).unwrap().distance;
        let c_rot: Vec<f32> = rq.rotate(&c).into_iter().map(|x| x as f32).collect();
        let plane = PlaneQuery::new(&rq.rotate(&q), &c_rot);
        let fast = plane.distance(code.norm, code.alpha, &code.bits, rq.padded);
        assert!(
            (fast - reference).abs() < 2e-2 * reference.abs(),
            "{fast} vs {reference}"
        );
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
