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
    pub(crate) seed: u64,
    signs: std::sync::Arc<Vec<Vec<f64>>>,
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
        // Every partial index a query builds needs the same few transforms: keep them.
        type Signs = std::sync::Arc<Vec<Vec<f64>>>;
        static KNOWN: std::sync::Mutex<Vec<(usize, u64, Signs)>> =
            std::sync::Mutex::new(Vec::new());
        let mut known = KNOWN.lock().expect("transform cache lock");
        let signs = match known.iter().find(|(d, s, _)| *d == dims && *s == seed) {
            Some((_, _, signs)) => signs.clone(),
            None => {
                let mut rng = Rng(seed);
                let signs: Signs = std::sync::Arc::new(
                    (0..3)
                        .map(|_| {
                            (0..padded)
                                .map(|_| if rng.next() & 1 == 0 { -1. } else { 1. })
                                .collect()
                        })
                        .collect(),
                );
                // A handful of (dimension, seed) pairs in practice; bound it regardless.
                if known.len() >= 64 {
                    known.remove(0);
                }
                known.push((dims, seed, signs.clone()));
                signs
            }
        };
        drop(known);
        Ok(Self {
            dims,
            padded,
            seed,
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

    pub(crate) fn rotate_into(&self, v: &[f32], result: &mut Vec<f64>) {
        result.clear();
        result.extend(v.iter().map(|x| f64::from(*x)));
        result.resize(self.padded, 0.);
        self.transform_in_place(result);
    }

    fn transform(&self, mut result: Vec<f64>) -> Vec<f64> {
        self.transform_in_place(&mut result);
        result
    }

    fn transform_in_place(&self, result: &mut [f64]) {
        for signs in self.signs.iter() {
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
            for x in result.iter_mut() {
                *x *= scale;
            }
        }
    }

    pub(crate) fn inverse_in_place(&self, result: &mut [f64]) {
        for signs in self.signs.iter().rev() {
            let mut stride = 1;
            while stride < self.padded {
                for block in result.chunks_exact_mut(2 * stride) {
                    let (left, right) = block.split_at_mut(stride);
                    for (a, b) in left.iter_mut().zip(right) {
                        let sum = *a + *b;
                        *b = *a - *b;
                        *a = sum;
                    }
                }
                stride *= 2;
            }
            let scale = 1. / (self.padded as f64).sqrt();
            for (x, sign) in result.iter_mut().zip(signs) {
                *x *= scale * sign;
            }
        }
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
pub(crate) const ERROR_CONFIDENCE: f64 = 1.9;

/// Bits used to quantize each coordinate of the rotated residual query.
pub(crate) const QUERY_BITS: usize = 8;

/// A residual query quantized to `QUERY_BITS` bit planes, so that a one-bit code's inner
/// product costs a few popcounts per 64 dimensions instead of a table lookup per byte.
#[derive(Default)]
pub(crate) struct PlaneQuery {
    quantized: Vec<u8>,
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
        let mut out = Self::default();
        out.reset(q, c);
        out
    }

    pub(crate) fn reset(&mut self, q: &[f64], c: &[f32]) {
        #[cfg(target_arch = "x86_64")]
        if avx512() && q.len().is_multiple_of(64) && c.len() == q.len() {
            // SAFETY: the features were detected; lengths are checked above.
            unsafe { self.reset_avx512(q, c) };
            return;
        }
        self.reset_scalar(q, c);
    }

    /// [`Self::new`] eight coordinates at a time. Sums are accumulated in another order than
    /// the scalar loop, so `norm2` and `sum` can differ from it in the last bits.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512bw")]
    unsafe fn reset_avx512(&mut self, q: &[f64], c: &[f32]) {
        use std::arch::x86_64::*;
        let n = q.len();
        let words = n / 64;
        let residual = |i: usize| {
            // SAFETY: `i + 8 <= n` for both slices.
            unsafe {
                _mm512_sub_pd(
                    _mm512_loadu_pd(q.as_ptr().add(i)),
                    _mm512_cvtps_pd(_mm256_loadu_ps(c.as_ptr().add(i))),
                )
            }
        };
        let mut vmin = _mm512_set1_pd(f64::INFINITY);
        let mut vmax = _mm512_set1_pd(f64::NEG_INFINITY);
        let mut norm2 = _mm512_setzero_pd();
        let mut sum = _mm512_setzero_pd();
        for i in (0..n).step_by(8) {
            let x = residual(i);
            vmin = _mm512_min_pd(vmin, x);
            vmax = _mm512_max_pd(vmax, x);
            norm2 = _mm512_fmadd_pd(x, x, norm2);
            sum = _mm512_add_pd(sum, x);
        }
        let (lo, hi) = (_mm512_reduce_min_pd(vmin), _mm512_reduce_max_pd(vmax));
        let (norm2, sum) = (_mm512_reduce_add_pd(norm2), _mm512_reduce_add_pd(sum));
        let levels = ((1usize << QUERY_BITS) - 1) as f64;
        let delta = if hi > lo { (hi - lo) / levels } else { 0. };
        self.planes.clear();
        self.planes.resize(QUERY_BITS * words, 0);
        let planes = &mut self.planes;
        if delta > 0. {
            self.quantized.resize(n, 0);
            let quantized = &mut self.quantized;
            let (base, step, top, half) = (
                _mm512_set1_pd(lo),
                _mm512_set1_pd(delta),
                _mm512_set1_pd(levels),
                _mm512_set1_pd(0.5),
            );
            for i in (0..n).step_by(8) {
                let level = _mm512_add_pd(
                    _mm512_min_pd(_mm512_div_pd(_mm512_sub_pd(residual(i), base), step), top),
                    half,
                );
                // Truncation of a non-negative value: the same rounding as the scalar path.
                let bytes =
                    _mm512_cvtepi32_epi8(_mm512_castsi256_si512(_mm512_cvttpd_epi32(level)));
                // SAFETY: `quantized` holds `n` bytes and `i + 8 <= n`.
                unsafe {
                    _mm_storel_epi64(quantized.as_mut_ptr().add(i).cast(), bytes);
                }
            }
            // SAFETY: the features were detected; `quantized` holds `words * 64` bytes.
            unsafe { transpose_avx512(quantized, words, planes) };
        }
        self.words = words;
        self.delta = delta;
        self.vmin = lo;
        self.sum = sum;
        self.norm2 = norm2;
    }

    #[cfg(all(test, target_arch = "x86_64"))]
    fn new_scalar(q: &[f64], c: &[f32]) -> Self {
        let mut out = Self::default();
        out.reset_scalar(q, c);
        out
    }

    fn reset_scalar(&mut self, q: &[f64], c: &[f32]) {
        let words = q.len().div_ceil(64);
        let mut vmin = f64::INFINITY;
        let mut vmax = f64::NEG_INFINITY;
        let mut norm2 = 0.;
        let mut sum = 0.;
        for (q, c) in q.iter().zip(c) {
            let x = *q - *c as f64;
            vmin = vmin.min(x);
            vmax = vmax.max(x);
            norm2 += x * x;
            sum += x;
        }
        let levels = ((1usize << QUERY_BITS) - 1) as f64;
        let delta = if vmax > vmin {
            (vmax - vmin) / levels
        } else {
            0.
        };
        self.planes.clear();
        self.planes.resize(QUERY_BITS * words, 0);
        let planes = &mut self.planes;
        if delta > 0. {
            // Quantize into bytes first (a loop the compiler vectorizes), then transpose each
            // 64-value block into one word per bit plane in registers.
            self.quantized.clear();
            self.quantized.resize(words * 64, 0);
            let levels_q = &mut self.quantized;
            for ((slot, q), c) in levels_q.iter_mut().zip(q).zip(c) {
                let x = *q - *c as f64;
                // `(x - vmin) / delta` is never negative, so adding a half and truncating rounds
                // half away from zero exactly as `round` does, without the libm call (and the
                // loop vectorizes). The cast saturates.
                *slot = (((x - vmin) / delta).min(levels) + 0.5) as u8;
            }
            for (w, block) in levels_q.as_chunks::<64>().0.iter().enumerate() {
                let mut acc = [0u64; QUERY_BITS];
                for (k, &v) in block.iter().enumerate() {
                    for (j, a) in acc.iter_mut().enumerate() {
                        *a |= (((v >> j) & 1) as u64) << k;
                    }
                }
                for (j, a) in acc.into_iter().enumerate() {
                    planes[j * words + w] = a;
                }
            }
        }
        self.words = words;
        self.delta = delta;
        self.vmin = vmin;
        self.sum = sum;
        self.norm2 = norm2;
    }

    /// [`Self::signed_dot`], with the AVX-512 kernel where the CPU has it.
    pub(crate) fn dot(&self, bits: &[u8], padded: usize) -> f64 {
        #[cfg(target_arch = "x86_64")]
        if avx512() && self.words.is_multiple_of(8) && bits.len() >= self.words * 8 {
            // SAFETY: the features were detected, and `bits` holds `words * 8` bytes.
            return unsafe { self.signed_dot_avx512(bits, padded) };
        }
        #[cfg(target_arch = "aarch64")]
        if self.words.is_multiple_of(2) && bits.len() >= self.words * 8 {
            return self.signed_dot_neon(bits, padded);
        }
        self.signed_dot(bits, padded)
    }

    /// [`Self::dot`] of each of `rows` codes packed `width` bytes apart in `bits`, passed to `f`
    /// with its row number (the same values, bit for bit). With AVX-512 and 512 or 1,024 padded
    /// dimensions the query's planes stay in registers across rows.
    pub(crate) fn dots(
        &self,
        bits: &[u8],
        width: usize,
        rows: usize,
        padded: usize,
        mut f: impl FnMut(usize, f64),
    ) {
        assert!(bits.len() >= rows * width);
        #[cfg(target_arch = "x86_64")]
        if avx512() && self.words.is_multiple_of(8) && width == self.words * 8 {
            // SAFETY: the features were detected, and each row holds `words * 8` bytes.
            return unsafe { self.dots_avx512(bits, rows, padded, &mut f) };
        }
        for n in 0..rows {
            f(n, self.dot(&bits[n * width..(n + 1) * width], padded));
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512bw,avx512vpopcntdq")]
    unsafe fn dots_avx512(
        &self,
        bits: &[u8],
        rows: usize,
        padded: usize,
        f: &mut impl FnMut(usize, f64),
    ) {
        use std::arch::x86_64::*;
        #[target_feature(enable = "avx512f,avx512bw,avx512vpopcntdq")]
        #[inline]
        fn rows_with<const C: usize>(
            q: &PlaneQuery,
            bits: &[u8],
            rows: usize,
            padded: usize,
            f: &mut impl FnMut(usize, f64),
        ) {
            debug_assert!(q.words == C * 8 && bits.len() >= rows * C * 64);
            // SAFETY (every load): `planes` holds `QUERY_BITS * words` words, and `bits` holds
            // `rows` codes of `words * 8 = C * 64` bytes; the caller enabled the features.
            let planes: [[__m512i; C]; QUERY_BITS] = std::array::from_fn(|j| {
                std::array::from_fn(|c| unsafe {
                    _mm512_loadu_si512(q.planes.as_ptr().add(j * q.words + c * 8).cast())
                })
            });
            for n in 0..rows {
                let mut weighted = _mm512_setzero_si512();
                let mut ones = _mm512_setzero_si512();
                for c in 0..C {
                    let at = (n * C + c) * 64;
                    let code = unsafe { _mm512_loadu_si512(bits.as_ptr().add(at).cast()) };
                    ones = _mm512_add_epi64(ones, _mm512_popcnt_epi64(code));
                    macro_rules! plane {
                        ($j:literal) => {{
                            let count = _mm512_popcnt_epi64(_mm512_and_si512(code, planes[$j][c]));
                            weighted = _mm512_add_epi64(weighted, _mm512_slli_epi64::<$j>(count));
                        }};
                    }
                    plane!(0);
                    plane!(1);
                    plane!(2);
                    plane!(3);
                    plane!(4);
                    plane!(5);
                    plane!(6);
                    plane!(7);
                }
                let weighted = _mm512_reduce_add_epi64(weighted) as u64;
                let ones = _mm512_reduce_add_epi64(ones) as u64;
                f(
                    n,
                    (2. * (q.delta * weighted as f64 + q.vmin * ones as f64) - q.sum)
                        / (padded as f64).sqrt(),
                );
            }
        }
        match self.words / 8 {
            1 => rows_with::<1>(self, bits, rows, padded, f),
            2 => rows_with::<2>(self, bits, rows, padded, f),
            _ => {
                let width = self.words * 8;
                for n in 0..rows {
                    // SAFETY: as for `dots`.
                    let dot = unsafe {
                        self.signed_dot_avx512(&bits[n * width..(n + 1) * width], padded)
                    };
                    f(n, dot);
                }
            }
        }
    }

    /// [`Self::signed_dot`] 128 bits at a time with NEON `CNT` (part of every AArch64 CPU).
    /// The bit counts are exact integers, so the result is bit-identical to the scalar path.
    #[cfg(target_arch = "aarch64")]
    #[inline]
    fn signed_dot_neon(&self, bits: &[u8], padded: usize) -> f64 {
        use std::arch::aarch64::*;
        assert!(bits.len() >= self.words * 8 && self.words.is_multiple_of(2));
        assert!(self.planes.len() >= QUERY_BITS * self.words);
        // SAFETY: NEON is mandatory on AArch64, and every load reads 16 bytes inside `bits`
        // (`words * 8` bytes) or a plane (`words` words), as asserted above.
        unsafe {
            let mut ones = vdupq_n_u16(0);
            let mut acc = [vdupq_n_u16(0); QUERY_BITS];
            for chunk in 0..self.words / 2 {
                let code = vld1q_u8(bits.as_ptr().add(chunk * 16));
                ones = vpadalq_u8(ones, vcntq_u8(code));
                for (j, a) in acc.iter_mut().enumerate() {
                    let plane = vld1q_u8(
                        self.planes
                            .as_ptr()
                            .add(j * self.words + chunk * 2)
                            .cast::<u8>(),
                    );
                    *a = vpadalq_u8(*a, vcntq_u8(vandq_u8(code, plane)));
                }
            }
            let mut weighted = 0u64;
            for (j, a) in acc.iter().enumerate() {
                weighted += (vaddlvq_u16(*a) as u64) << j;
            }
            let ones = vaddlvq_u16(ones) as u64;
            (2. * (self.delta * weighted as f64 + self.vmin * ones as f64) - self.sum)
                / (padded as f64).sqrt()
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

    #[cfg(test)]
    pub(crate) fn signed_dot_for_tests(&self, bits: &[u8], padded: usize) -> f64 {
        self.signed_dot(bits, padded)
    }

    /// Estimated squared distance (plain, without the error bound).
    #[cfg(test)]
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
        emit: &mut impl FnMut(usize, f64, f64, bool),
    ) -> crate::Result<()> {
        #[cfg(target_arch = "x86_64")]
        {
            if avx512() && padded.is_multiple_of(512) && self.words == padded / 64 {
                // SAFETY: the CPU features were detected at runtime, and every code holds
                // `padded / 8` bytes, a whole number of 64-byte vectors.
                return unsafe { self.scan_avx512(bytes, rows, compact, padded, emit) };
            }
            if std::is_x86_feature_detected!("popcnt") {
                // SAFETY: the CPU feature was detected at runtime.
                return unsafe { self.scan_popcnt(bytes, rows, compact, padded, emit) };
            }
        }
        #[cfg(target_arch = "aarch64")]
        if padded.is_multiple_of(128) && self.words == padded / 64 {
            return self.scan_impl(bytes, rows, compact, padded, emit, Self::signed_dot_neon);
        }
        self.scan_impl(bytes, rows, compact, padded, emit, Self::signed_dot)
    }

    /// The scan with the inner product computed 512 bits at a time (`VPOPCNTQ`).
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512bw,avx512vpopcntdq,popcnt")]
    unsafe fn scan_avx512(
        &self,
        bytes: &[u8],
        rows: usize,
        compact: bool,
        padded: usize,
        emit: &mut impl FnMut(usize, f64, f64, bool),
    ) -> crate::Result<()> {
        self.scan_impl(bytes, rows, compact, padded, emit, |q, bits, padded| {
            // SAFETY: the caller checked the features and that `bits` is `words * 8` bytes.
            unsafe { q.signed_dot_avx512(bits, padded) }
        })
    }

    /// [`Self::signed_dot`] for codes of a whole number of 512-bit vectors.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512bw,avx512vpopcntdq")]
    #[inline]
    unsafe fn signed_dot_avx512(&self, bits: &[u8], padded: usize) -> f64 {
        use std::arch::x86_64::*;
        debug_assert!(bits.len() >= self.words * 8 && self.words.is_multiple_of(8));
        let mut weighted = _mm512_setzero_si512();
        let mut ones = _mm512_setzero_si512();
        for chunk in 0..self.words / 8 {
            // SAFETY: `bits` holds `words * 8` bytes and `planes` `QUERY_BITS * words` words.
            let code = unsafe { _mm512_loadu_si512(bits.as_ptr().add(chunk * 64).cast()) };
            ones = _mm512_add_epi64(ones, _mm512_popcnt_epi64(code));
            macro_rules! plane {
                ($j:literal) => {{
                    let plane = unsafe {
                        _mm512_loadu_si512(
                            self.planes.as_ptr().add($j * self.words + chunk * 8).cast(),
                        )
                    };
                    let count = _mm512_popcnt_epi64(_mm512_and_si512(code, plane));
                    weighted = _mm512_add_epi64(weighted, _mm512_slli_epi64::<$j>(count));
                }};
            }
            plane!(0);
            plane!(1);
            plane!(2);
            plane!(3);
            plane!(4);
            plane!(5);
            plane!(6);
            plane!(7);
        }
        let weighted = _mm512_reduce_add_epi64(weighted) as u64;
        let ones = _mm512_reduce_add_epi64(ones) as u64;
        (2. * (self.delta * weighted as f64 + self.vmin * ones as f64) - self.sum)
            / (padded as f64).sqrt()
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "popcnt")]
    unsafe fn scan_popcnt(
        &self,
        bytes: &[u8],
        rows: usize,
        compact: bool,
        padded: usize,
        emit: &mut impl FnMut(usize, f64, f64, bool),
    ) -> crate::Result<()> {
        self.scan_impl(bytes, rows, compact, padded, emit, Self::signed_dot)
    }

    #[inline(always)]
    fn scan_impl(
        &self,
        bytes: &[u8],
        rows: usize,
        compact: bool,
        padded: usize,
        emit: &mut impl FnMut(usize, f64, f64, bool),
        dot: impl Fn(&Self, &[u8], usize) -> f64,
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
            let dot = dot(self, &row[head..], padded);
            emit(
                ordinal,
                norm * norm + self.norm2 - 2. * norm * dot / alpha,
                bound,
                zero,
            );
        }
        Ok(())
    }
}

/// Whether this CPU has the AVX-512 subsets the fast scan uses (checked once).
#[cfg(target_arch = "x86_64")]
fn avx512() -> bool {
    static AVX512: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *AVX512.get_or_init(|| {
        std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vpopcntdq")
    })
}

/// Transpose quantized levels into bit planes, 64 values per mask: `VPTESTMB` sets bit `k` of
/// plane `j` when bit `j` of value `k` is set.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn transpose_avx512(levels: &[u8], words: usize, planes: &mut [u64]) {
    use std::arch::x86_64::*;
    debug_assert!(levels.len() >= words * 64 && planes.len() >= QUERY_BITS * words);
    for w in 0..words {
        // SAFETY: `levels` holds `words * 64` bytes.
        let v = unsafe { _mm512_loadu_si512(levels.as_ptr().add(w * 64).cast()) };
        for (j, plane) in planes.chunks_exact_mut(words).take(QUERY_BITS).enumerate() {
            plane[w] = _mm512_test_epi8_mask(v, _mm512_set1_epi8(1 << j));
        }
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
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx512_paths_match_scalar() {
        if !avx512() {
            return;
        }
        let dims = 1024;
        let mut rng = Rng(11);
        let mut sample = |n: usize| -> Vec<f32> {
            (0..n)
                .map(|_| (rng.next() % 2000) as f32 / 1000. - 1.)
                .collect()
        };
        let rq = RaBitQ::new(dims, 5).unwrap();
        let (q, c) = (sample(dims), sample(dims));
        let c_rot: Vec<f32> = rq.rotate(&c).into_iter().map(|x| x as f32).collect();
        let plane = PlaneQuery::new(&rq.rotate(&q), &c_rot);
        // The scalar transpose gives the same planes.
        let words = plane.words;
        let mut scalar = vec![0u64; QUERY_BITS * words];
        let mut levels = vec![0u8; words * 64];
        for ((slot, q), c) in levels.iter_mut().zip(rq.rotate(&q)).zip(&c_rot) {
            let x = q - *c as f64;
            *slot = ((x - plane.vmin) / plane.delta).round().clamp(0., 255.) as u8;
        }
        for (w, block) in levels.as_chunks::<64>().0.iter().enumerate() {
            for (k, &v) in block.iter().enumerate() {
                for j in 0..QUERY_BITS {
                    scalar[j * words + w] |= (((v >> j) & 1) as u64) << k;
                }
            }
        }
        assert_eq!(plane.planes, scalar);
        let reference = PlaneQuery::new_scalar(&rq.rotate(&q), &c_rot);
        assert_eq!(plane.planes, reference.planes);
        assert_eq!((plane.vmin, plane.delta), (reference.vmin, reference.delta));
        assert!((plane.norm2 - reference.norm2).abs() <= 1e-9 * reference.norm2);
        assert!((plane.sum - reference.sum).abs() <= 1e-9 * reference.norm2.sqrt());
        for _ in 0..20 {
            let v = sample(dims);
            let code = rq.encode(&v, &c).unwrap();
            let slow = plane.signed_dot(&code.bits, rq.padded);
            // SAFETY: the features were detected above.
            let fast = unsafe { plane.signed_dot_avx512(&code.bits, rq.padded) };
            assert_eq!(slow.to_bits(), fast.to_bits());
        }
    }
    #[test]
    fn batched_dots_match_scalar() {
        let mut rng = Rng(17);
        for dims in [100, 512, 1000, 1024, 1536] {
            let mut sample = |n: usize| -> Vec<f32> {
                (0..n)
                    .map(|_| (rng.next() % 2000) as f32 / 1000. - 1.)
                    .collect()
            };
            let rq = RaBitQ::new(dims, 5).unwrap();
            let (q, c) = (sample(dims), sample(dims));
            let c_rot: Vec<f32> = rq.rotate(&c).into_iter().map(|x| x as f32).collect();
            let plane = PlaneQuery::new(&rq.rotate(&q), &c_rot);
            let codes: Vec<Vec<u8>> = (0..7)
                .map(|_| rq.encode(&sample(dims), &c).unwrap().bits)
                .collect();
            let width = codes[0].len();
            let bits = codes.concat();
            let mut seen = 0;
            plane.dots(&bits, width, codes.len(), rq.padded, |n, dot| {
                let slow = plane.signed_dot(&codes[n], rq.padded);
                assert_eq!(dot.to_bits(), slow.to_bits(), "dims {dims} row {n}");
                seen += 1;
            });
            assert_eq!(seen, codes.len());
        }
    }
    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_dot_matches_scalar() {
        let mut rng = Rng(13);
        for dims in [128, 1000, 1024, 1536] {
            let mut sample = |n: usize| -> Vec<f32> {
                (0..n)
                    .map(|_| (rng.next() % 2000) as f32 / 1000. - 1.)
                    .collect()
            };
            let rq = RaBitQ::new(dims, 5).unwrap();
            let (q, c) = (sample(dims), sample(dims));
            let c_rot: Vec<f32> = rq.rotate(&c).into_iter().map(|x| x as f32).collect();
            let plane = PlaneQuery::new(&rq.rotate(&q), &c_rot);
            for _ in 0..20 {
                let code = rq.encode(&sample(dims), &c).unwrap();
                let slow = plane.signed_dot(&code.bits, rq.padded);
                let fast = plane.signed_dot_neon(&code.bits, rq.padded);
                assert_eq!(slow.to_bits(), fast.to_bits());
                assert_eq!(slow.to_bits(), plane.dot(&code.bits, rq.padded).to_bits());
            }
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
