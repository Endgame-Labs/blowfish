use std::collections::HashSet;

use base64::{Engine, engine::general_purpose::STANDARD};
use half::f16;
use numkong::{Dot, Euclidean};
use roaring::RoaringBitmap;

use crate::{Error, Result};

/// Wire-compatible distance metrics; smaller values are nearer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Metric {
    /// `1 - dot(a, b) / (norm(a) * norm(b))`, clamped to `[0, 2]`.
    /// Two zero vectors have distance zero; one zero vector has distance one.
    CosineDistance,
    /// The sum of squared coordinate differences.
    EuclideanSquared,
}

impl Metric {
    /// Compute a distance using NumKong's runtime-dispatched SIMD kernels.
    /// Rejects empty, mismatched, or non-finite inputs. Distances exceeding f32
    /// representable range are returned as positive infinity.
    pub fn distance(self, a: &[f32], b: &[f32]) -> Result<f32> {
        validate(a, b.len())?;
        validate(b, a.len())?;
        Ok(self.distance_unchecked(a, b))
    }

    /// Compute directly over half-precision storage using NumKong's native
    /// runtime-dispatched f16 kernels, without converting or allocating.
    pub fn distance_f16(self, a: &[f16], b: &[f16]) -> Result<f32> {
        if a.is_empty() || a.len() != b.len() || a.iter().chain(b).any(|x| !x.is_finite()) {
            return Err(Error::InvalidInput(
                "expected matching, nonempty, finite vectors".into(),
            ));
        }
        // SAFETY: both pinned types are repr(transparent) wrappers around u16,
        // admit all bit patterns, and have the same IEEE binary16 representation.
        // These shared slices retain the original allocation and lifetime.
        let as_numkong = |v: &[f16]| unsafe {
            std::slice::from_raw_parts(v.as_ptr().cast::<numkong::f16>(), v.len())
        };
        let a = as_numkong(a);
        let b = as_numkong(b);
        match self {
            Self::EuclideanSquared => Ok(numkong::f16::sqeuclidean(a, b).unwrap()),
            Self::CosineDistance => {
                let aa = numkong::f16::dot(a, a).unwrap() as f64;
                let bb = numkong::f16::dot(b, b).unwrap() as f64;
                if aa == 0. || bb == 0. {
                    return Ok(if aa == bb { 0. } else { 1. });
                }
                let ab = numkong::f16::dot(a, b).unwrap() as f64;
                Ok((1. - ab / (aa.sqrt() * bb.sqrt())).clamp(0., 2.) as f32)
            }
        }
    }

    pub(crate) fn distance_unchecked(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Self::EuclideanSquared => l2_squared_f32(a, b),
            Self::CosineDistance => {
                let (aa, bb, ab) = norms_and_dot_f32(a, b);
                let (aa, bb, ab) = (aa as f64, bb as f64, ab as f64);
                if !aa.is_finite()
                    || !bb.is_finite()
                    || !ab.is_finite()
                    || (aa == 0.0 && a.iter().any(|x| *x != 0.0))
                    || (bb == 0.0 && b.iter().any(|x| *x != 0.0))
                {
                    return scalar_cosine(a, b);
                }
                if aa == 0.0 || bb == 0.0 {
                    return if aa == bb { 0.0 } else { 1.0 };
                }
                (1.0 - ab / (aa.sqrt() * bb.sqrt())).clamp(0.0, 2.0) as f32
            }
        }
    }
}

fn scalar_cosine(a: &[f32], b: &[f32]) -> f32 {
    let aa: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum();
    let bb: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum();
    let ab: f64 = a.iter().zip(b).map(|(a, b)| *a as f64 * *b as f64).sum();
    if aa == 0.0 || bb == 0.0 {
        return if aa == bb { 0.0 } else { 1.0 };
    }
    (1.0 - ab / (aa.sqrt() * bb.sqrt())).clamp(0.0, 2.0) as f32
}

pub(crate) fn validate(v: &[f32], dims: usize) -> Result<()> {
    if dims == 0 || v.len() != dims || v.iter().any(|x| !x.is_finite()) {
        return Err(Error::InvalidInput(
            "expected matching, nonempty, finite vectors".into(),
        ));
    }
    Ok(())
}

/// Encode standard, padded base64 of little-endian f32 bytes.
pub fn encode_base64_f32(vector: &[f32]) -> String {
    STANDARD.encode(
        vector
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>(),
    )
}

/// Decode wire vectors as little-endian f32, regardless of the column's dtype.
/// Invalid base64, partial elements, and non-finite coordinates are rejected.
pub fn decode_base64_f32(encoded: &str) -> Result<Vec<f32>> {
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|e| Error::InvalidInput(e.to_string()))?;
    if bytes.is_empty() || bytes.len() % 4 != 0 {
        return Err(Error::InvalidInput(
            "base64 vector must contain whole f32 elements".into(),
        ));
    }
    let v: Vec<_> = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| f32::from_le_bytes(*b))
        .collect();
    validate(&v, v.len())?;
    Ok(v)
}

/// Exact top-k over `(row, vector)` pairs, optionally restricted to a row bitmap.
/// All input vectors are validated, including rows excluded by the filter.
/// Ties are ordered by row ID. Row IDs must be unique.
pub fn exact_search<V: AsRef<[f32]>>(
    vectors: &[(u32, V)],
    query: &[f32],
    k: usize,
    metric: Metric,
    filter: Option<&RoaringBitmap>,
) -> Result<Vec<(u32, f32)>> {
    validate(query, query.len())?;
    let mut rows = HashSet::with_capacity(vectors.len());
    let mut result = Vec::new();
    for (row, v) in vectors {
        validate(v.as_ref(), query.len())?;
        if !rows.insert(*row) {
            return Err(Error::InvalidInput("duplicate row ID".into()));
        }
        if filter.is_none_or(|f| f.contains(*row)) {
            result.push((*row, metric.distance_unchecked(query, v.as_ref())));
        }
    }
    sort_truncate(&mut result, k);
    Ok(result)
}

pub(crate) fn sort_truncate(result: &mut Vec<(u32, f32)>, k: usize) {
    let cmp = |a: &(u32, f32), b: &(u32, f32)| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0));
    if k < result.len() {
        result.select_nth_unstable_by(k, cmp);
        result.truncate(k);
    }
    result.sort_unstable_by(cmp);
}

/// Fraction of distinct exact result rows present in ANN results. Empty ground
/// truth has recall 1.0; duplicates never inflate recall.
pub fn recall_at_k(ann: &[(u32, f32)], exact: &[(u32, f32)]) -> f32 {
    let truth: HashSet<_> = exact.iter().map(|x| x.0).collect();
    if truth.is_empty() {
        return 1.0;
    }
    let found: HashSet<_> = ann.iter().map(|x| x.0).collect();
    truth.intersection(&found).count() as f32 / truth.len() as f32
}

/// Squared Euclidean distance between equal-length `f32` vectors, for clustering (k-means
/// training and assignment), where it is nearly all of the work. Sixteen independent `f32`
/// lanes, compiled for AVX-512 or AVX2 when the CPU has them: about four times faster than the
/// general kernel at 1,024 dimensions on x86. Not used for distances a query returns.
pub(crate) fn l2_squared_f32(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            // SAFETY: the CPU supports AVX-512F (checked above).
            return unsafe { l2_squared_avx512(a, b) };
        }
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            // SAFETY: the CPU supports AVX2 and FMA (checked above).
            return unsafe { l2_squared_avx2(a, b) };
        }
    }
    l2_squared_lanes(a, b)
}

/// `(|a|^2, |b|^2, <a, b>)` in one pass, with sixteen `f32` lanes per sum (compiled for
/// AVX-512 or AVX2 when the CPU has them).
pub(crate) fn norms_and_dot_f32(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            // SAFETY: the CPU supports AVX-512F (checked above).
            return unsafe { norms_and_dot_avx512(a, b) };
        }
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            // SAFETY: the CPU supports AVX2 and FMA (checked above).
            return unsafe { norms_and_dot_avx2(a, b) };
        }
    }
    norms_and_dot_lanes(a, b)
}

#[inline(always)]
fn norms_and_dot_lanes(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    let (mut aa, mut bb, mut ab) = ([0f32; 16], [0f32; 16], [0f32; 16]);
    let ((ca, ra), (cb, rb)) = (a.as_chunks::<16>(), b.as_chunks::<16>());
    for (x, y) in ca.iter().zip(cb) {
        for i in 0..16 {
            aa[i] += x[i] * x[i];
            bb[i] += y[i] * y[i];
            ab[i] += x[i] * y[i];
        }
    }
    let (mut saa, mut sbb, mut sab): (f32, f32, f32) =
        (aa.iter().sum(), bb.iter().sum(), ab.iter().sum());
    for (x, y) in ra.iter().zip(rb) {
        saa += x * x;
        sbb += y * y;
        sab += x * y;
    }
    (saa, sbb, sab)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn norms_and_dot_avx512(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    norms_and_dot_lanes(a, b)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn norms_and_dot_avx2(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    norms_and_dot_lanes(a, b)
}

#[inline(always)]
fn l2_squared_lanes(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0f32; 16];
    let ((ca, ra), (cb, rb)) = (a.as_chunks::<16>(), b.as_chunks::<16>());
    for (x, y) in ca.iter().zip(cb) {
        for i in 0..16 {
            let d = x[i] - y[i];
            acc[i] += d * d;
        }
    }
    let mut sum: f32 = acc.iter().sum();
    for (x, y) in ra.iter().zip(rb) {
        sum += (x - y) * (x - y);
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn l2_squared_avx512(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    let n = a.len().min(b.len());
    let (pa, pb) = (a.as_ptr(), b.as_ptr());
    // Four independent accumulators hide the FMA latency (one chain is latency bound).
    let mut acc = [_mm512_setzero_ps(); 4];
    let mut i = 0;
    // SAFETY: every load reads 16 lanes at `i + 16 * j`, below `n` by the loop bounds.
    unsafe {
        while i + 64 <= n {
            for (j, s) in acc.iter_mut().enumerate() {
                let d = _mm512_sub_ps(
                    _mm512_loadu_ps(pa.add(i + 16 * j)),
                    _mm512_loadu_ps(pb.add(i + 16 * j)),
                );
                *s = _mm512_fmadd_ps(d, d, *s);
            }
            i += 64;
        }
        while i + 16 <= n {
            let d = _mm512_sub_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)));
            acc[0] = _mm512_fmadd_ps(d, d, acc[0]);
            i += 16;
        }
        let sum = _mm512_add_ps(_mm512_add_ps(acc[0], acc[1]), _mm512_add_ps(acc[2], acc[3]));
        let mut total = _mm512_reduce_add_ps(sum);
        for k in i..n {
            let d = a[k] - b[k];
            total += d * d;
        }
        total
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn l2_squared_avx2(a: &[f32], b: &[f32]) -> f32 {
    l2_squared_lanes(a, b)
}

/// Dot products of four `f32` rows with one center (each the length of the center). One pass
/// over the center serves all four rows, with one fused multiply-add per lane and row: with the
/// rows' and centers' squared norms, `|r - c|^2 = |r|^2 + |c|^2 - 2 <r, c>` at about a third of
/// the cost of four separate squared distances.
pub(crate) fn dots_f32_x4(rows: [&[f32]; 4], center: &[f32]) -> [f32; 4] {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") && rows.iter().all(|r| r.len() == center.len())
        {
            // SAFETY: the CPU supports AVX-512F, and every row is as long as the center.
            return unsafe { dots_x4_avx512(rows, center) };
        }
    }
    rows.map(|r| dot_lanes(r, center))
}

#[inline(always)]
fn dot_lanes(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0f32; 16];
    let ((ca, ra), (cb, rb)) = (a.as_chunks::<16>(), b.as_chunks::<16>());
    for (x, y) in ca.iter().zip(cb) {
        for i in 0..16 {
            acc[i] += x[i] * y[i];
        }
    }
    let mut sum: f32 = acc.iter().sum();
    for (x, y) in ra.iter().zip(rb) {
        sum += x * y;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn dots_x4_avx512(rows: [&[f32]; 4], center: &[f32]) -> [f32; 4] {
    use std::arch::x86_64::*;
    let n = center.len();
    let pc = center.as_ptr();
    let pr = rows.map(|r| r.as_ptr());
    let mut acc = [_mm512_setzero_ps(); 4];
    let mut i = 0;
    // SAFETY: loads read 16 lanes at `i`, below `n`, from slices of length `n`.
    unsafe {
        while i + 16 <= n {
            let c = _mm512_loadu_ps(pc.add(i));
            for (s, p) in acc.iter_mut().zip(pr) {
                *s = _mm512_fmadd_ps(_mm512_loadu_ps(p.add(i)), c, *s);
            }
            i += 16;
        }
        let mut out = acc.map(|s| _mm512_reduce_add_ps(s));
        for (o, r) in out.iter_mut().zip(rows) {
            for k in i..n {
                *o += r[k] * center[k];
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn kernels_match_scalar(pairs in prop::collection::vec((-100f32..100., -100f32..100.), 1..1100)) {
            let (a, b): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
            let l2: f64 = a.iter().zip(&b).map(|(a,b)| (*a as f64 - *b as f64).powi(2)).sum();
            let got = Metric::EuclideanSquared.distance(&a,&b).unwrap() as f64;
            prop_assert!((l2-got).abs() <= 1e-5 * l2.max(1.));
            let cos = scalar_cosine(&a,&b);
            prop_assert!((cos-Metric::CosineDistance.distance(&a,&b).unwrap()).abs() < 2e-5);
            let ah: Vec<_> = a.iter().map(|x| f16::from_f32(*x)).collect();
            let bh: Vec<_> = b.iter().map(|x| f16::from_f32(*x)).collect();
            let af: Vec<_> = ah.iter().map(|x| x.to_f32()).collect();
            let bf: Vec<_> = bh.iter().map(|x| x.to_f32()).collect();
            let got = Metric::EuclideanSquared.distance_f16(&ah,&bh).unwrap();
            let expected = Metric::EuclideanSquared.distance(&af,&bf).unwrap();
            prop_assert!((got-expected).abs() <= 2e-5*expected.max(1.));
            prop_assert!((Metric::CosineDistance.distance_f16(&ah,&bh).unwrap()-scalar_cosine(&af,&bf)).abs() < 2e-5);
            prop_assert_eq!(decode_base64_f32(&encode_base64_f32(&a)).unwrap(), a);
        }
    }

    #[test]
    fn edge_cases() {
        for metric in [Metric::CosineDistance, Metric::EuclideanSquared] {
            assert!(metric.distance(&[], &[]).is_err());
            assert!(metric.distance(&[1.], &[1., 2.]).is_err());
            assert!(metric.distance(&[f32::NAN], &[1.]).is_err());
        }
        assert_eq!(Metric::CosineDistance.distance(&[1.], &[-1.]).unwrap(), 2.);
        assert_eq!(Metric::CosineDistance.distance(&[0.], &[0.]).unwrap(), 0.);
        assert_eq!(Metric::CosineDistance.distance(&[0.], &[1.]).unwrap(), 1.);
        assert_eq!(
            Metric::CosineDistance
                .distance(&[f32::MAX], &[f32::MAX])
                .unwrap(),
            0.
        );
        assert!(decode_base64_f32("AAAA").is_err());
        assert!(decode_base64_f32(&encode_base64_f32(&[f32::NAN])).is_err());
        assert!(decode_base64_f32("!").is_err());
    }
}
