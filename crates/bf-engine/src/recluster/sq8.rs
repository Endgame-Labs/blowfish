//! The recluster's training spill: one byte per dimension (SQ8, an affine scale per dimension).
//!
//! Training and assignment only need to find a near center, so they read this copy. The exact
//! vectors come from their source segments when a slab is built, and each one is checked against
//! its spilled row, so an exported vector is the stored one, bit for bit.
use crate::{segment, vector_locator::decode};
use bf_types::ApiError;

/// Per-dimension affine 8-bit codes: `x ≈ lo + code * step`.
#[derive(Clone, Debug)]
pub(crate) struct Sq8 {
    lo: Vec<f32>,
    step: Vec<f32>,
}

/// Range widening on each side, so rows outside the sample clip only slightly.
const MARGIN: f32 = 0.1;

/// The range of each dimension over a sample of rows.
pub(crate) struct Range {
    lo: Vec<f32>,
    hi: Vec<f32>,
}

impl Range {
    pub fn new(dims: usize) -> Self {
        Self {
            lo: vec![f32::INFINITY; dims],
            hi: vec![f32::NEG_INFINITY; dims],
        }
    }

    pub fn add(&mut self, v: &[f32]) {
        for ((lo, hi), x) in self.lo.iter_mut().zip(&mut self.hi).zip(v) {
            if x.is_finite() {
                *lo = lo.min(*x);
                *hi = hi.max(*x);
            }
        }
    }

    pub fn merge(&mut self, other: &Range) {
        for i in 0..self.lo.len() {
            self.lo[i] = self.lo[i].min(other.lo[i]);
            self.hi[i] = self.hi[i].max(other.hi[i]);
        }
    }

    pub fn fit(self) -> Sq8 {
        let (mut lo, mut step) = (Vec::new(), Vec::new());
        for (l, h) in self.lo.into_iter().zip(self.hi) {
            if l > h {
                // Never saw a finite value.
                lo.push(0.0);
                step.push(1.0);
                continue;
            }
            let pad = (h - l) * MARGIN;
            let (l, h) = (l - pad, h + pad);
            lo.push(l);
            step.push(if h > l { (h - l) / 255.0 } else { 1.0 });
        }
        Sq8 { lo, step }
    }
}

impl Sq8 {
    /// Exact codes for 8-bit stored vectors: stored `i8` maps to `0..=255` with no loss.
    pub fn identity(dims: usize) -> Self {
        Self {
            lo: vec![-128.0; dims],
            step: vec![1.0; dims],
        }
    }

    /// Encode one vector, clipping values outside the fitted range.
    pub fn encode(&self, v: &[f32], out: &mut [u8]) {
        for (((o, x), lo), step) in out.iter_mut().zip(v).zip(&self.lo).zip(&self.step) {
            // NaN casts to zero.
            *o = ((x - lo) / step).round().clamp(0.0, 255.0) as u8;
        }
    }

    /// Decode a spilled row.
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        codes
            .iter()
            .zip(&self.lo)
            .zip(&self.step)
            .map(|((c, lo), step)| lo + f32::from(*c) * step)
            .collect()
    }

    /// Whether `exact` (a stored vector, at its stored precision) is the row whose spilled
    /// codes are `spilled`.
    pub fn verify(
        &self,
        dtype: bf_vector::VectorType,
        exact: &[u8],
        spilled: &[u8],
    ) -> Result<(), ApiError> {
        let mut again = vec![0u8; spilled.len()];
        self.encode(&decode(dtype, exact), &mut again);
        if again == spilled {
            Ok(())
        } else {
            Err(segment::index_error(
                "recluster: a source vector does not match its spilled row",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_within_half_a_step_and_clip_outside_the_range() {
        let mut range = Range::new(2);
        range.add(&[-1.0, 5.0]);
        range.add(&[1.0, 5.0]);
        let q = range.fit();
        let mut codes = [0u8; 2];
        q.encode(&[0.3, 5.0], &mut codes);
        let back = q.decode(&codes);
        assert!((back[0] - 0.3).abs() <= q.step[0] / 2.0 + 1e-6);
        assert_eq!(back[1], 5.0, "a constant dimension is exact");
        q.encode(&[100.0, -100.0], &mut codes);
        assert_eq!(codes[0], 255);
        let again = q.decode(&codes);
        assert!(again[0].is_finite());
    }

    #[test]
    fn identity_is_lossless_for_i8() {
        let q = Sq8::identity(3);
        let mut codes = [0u8; 3];
        q.encode(&[-128.0, 0.0, 127.0], &mut codes);
        assert_eq!(q.decode(&codes), vec![-128.0, 0.0, 127.0]);
    }

    #[test]
    fn verify_accepts_the_row_and_rejects_another() {
        let mut range = Range::new(2);
        range.add(&[0.0, 0.0]);
        range.add(&[1.0, 1.0]);
        let q = range.fit();
        let a = [half::f16::from_f32(0.25), half::f16::from_f32(0.75)];
        let bytes: Vec<u8> = a.iter().flat_map(|x| x.to_le_bytes()).collect();
        let mut codes = [0u8; 2];
        q.encode(&[0.25, 0.75], &mut codes);
        q.verify(bf_vector::VectorType::F16, &bytes, &codes)
            .unwrap();
        let other = [half::f16::from_f32(0.9), half::f16::from_f32(0.1)];
        let bytes: Vec<u8> = other.iter().flat_map(|x| x.to_le_bytes()).collect();
        assert!(
            q.verify(bf_vector::VectorType::F16, &bytes, &codes)
                .is_err()
        );
    }
}
