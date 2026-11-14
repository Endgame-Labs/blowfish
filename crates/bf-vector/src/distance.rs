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

    /// Distance between stored half-precision vectors. Conversion is exact;
    /// arithmetic uses the same SIMD f32 kernels as [`Self::distance`].
    pub fn distance_f16(self, a: &[f16], b: &[f16]) -> Result<f32> {
        let a: Vec<_> = a.iter().map(|v| v.to_f32()).collect();
        let b: Vec<_> = b.iter().map(|v| v.to_f32()).collect();
        self.distance(&a, &b)
    }

    pub(crate) fn distance_unchecked(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Self::EuclideanSquared => f32::sqeuclidean(a, b).unwrap() as f32,
            Self::CosineDistance => {
                let aa = f32::dot(a, a).unwrap();
                let bb = f32::dot(b, b).unwrap();
                let ab = f32::dot(a, b).unwrap();
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
            prop_assert_eq!(Metric::EuclideanSquared.distance_f16(&ah,&bh).unwrap(), Metric::EuclideanSquared.distance(&af,&bf).unwrap());
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
