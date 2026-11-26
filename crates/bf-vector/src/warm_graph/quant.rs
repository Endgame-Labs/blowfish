//! Symmetric eight-bit scalar codes after the production signed Hadamard rotation.
use super::rabitq::RaBitQ;
use numkong::Dot;
pub struct Codes {
    pub dims: usize,
    pub values: Vec<i8>,
    pub scales: Vec<f32>,
    pub dot: fn(&[i8], &[i8]) -> i32,
}
pub(super) fn dot_kernel() -> fn(&[i8], &[i8]) -> i32 {
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("dotprod") {
        return |a, b| {
            // SAFETY: selected once after runtime detection; graph rows have equal lengths.
            unsafe { dot_neon(a, b) }
        };
    }
    |a, b| i8::dot(a, b).unwrap()
}
// Graph codes have at most 4,096 coordinates, so every partial and final integer sum fits
// i32, even for -128. Independent accumulators preserve the exact sum while breaking the
// single-accumulator dependency chain in NumKong's current neonsdot kernel.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "dotprod")]
unsafe fn dot_neon(a: &[i8], b: &[i8]) -> i32 {
    use std::arch::aarch64::*;
    debug_assert_eq!(a.len(), b.len());
    debug_assert!(a.len() <= 4096);
    // SAFETY: each load stays inside matching slices, and the caller detected dotprod.
    unsafe {
        let mut sums = [vdupq_n_s32(0); 4];
        let mut i = 0;
        while i + 64 <= a.len() {
            for (lane, sum) in sums.iter_mut().enumerate() {
                let offset = i + lane * 16;
                *sum = vdotq_s32(
                    *sum,
                    vld1q_s8(a.as_ptr().add(offset)),
                    vld1q_s8(b.as_ptr().add(offset)),
                );
            }
            i += 64;
        }
        while i + 16 <= a.len() {
            sums[0] = vdotq_s32(
                sums[0],
                vld1q_s8(a.as_ptr().add(i)),
                vld1q_s8(b.as_ptr().add(i)),
            );
            i += 16;
        }
        let mut sum = vaddvq_s32(vaddq_s32(
            vaddq_s32(sums[0], sums[1]),
            vaddq_s32(sums[2], sums[3]),
        ));
        for (&a, &b) in a[i..].iter().zip(&b[i..]) {
            sum += i32::from(a) * i32::from(b);
        }
        sum
    }
}
pub(super) fn encode(row: &[f64]) -> (Vec<i8>, f32) {
    let scale = row.iter().map(|x| x.abs()).fold(0f64, f64::max) / 127.;
    let scale = scale.max(f64::MIN_POSITIVE);
    (
        row.iter()
            .map(|x| (x / scale).round().clamp(-127., 127.) as i8)
            .collect(),
        scale as f32,
    )
}
fn prefetch_row(row: &[i8]) {
    #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
    for offset in
        (0..row.len()).step_by(if cfg!(all(target_arch = "aarch64", target_os = "macos")) {
            128
        } else {
            64
        })
    {
        // SAFETY: each address belongs to this immutable code row. These nonfaulting hints
        // do not dereference a value or change the integer dot product.
        unsafe {
            let address = row.as_ptr().add(offset);
            #[cfg(target_arch = "aarch64")]
            std::arch::asm!("prfm pldl1keep, [{address}]", address = in(reg) address, options(readonly, nostack, preserves_flags));
            #[cfg(target_arch = "x86_64")]
            std::arch::x86_64::_mm_prefetch(address.cast(), std::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    let _ = row;
}
impl Codes {
    pub fn prefetch(&self, id: u32) {
        let start = id as usize * self.dims;
        prefetch_row(&self.values[start..start + self.dims]);
    }
    pub fn between(&self, a: u32, b: u32) -> f32 {
        let a = a as usize;
        let b = b as usize;
        (1. - (self.dot)(
            &self.values[a * self.dims..(a + 1) * self.dims],
            &self.values[b * self.dims..(b + 1) * self.dims],
        ) as f32
            * self.scales[a]
            * self.scales[b])
            .max(0.)
    }
    pub fn prepare(&self, q: &[f32]) -> (Vec<i8>, f32) {
        encode(&RaBitQ::new(q.len(), 42).unwrap().rotate(q))
    }
    pub fn score(&self, q: &[i8], scale: f32, id: u32) -> f32 {
        let start = id as usize * self.dims;
        1. - (self.dot)(q, &self.values[start..start + self.dims]) as f32
            * scale
            * self.scales[id as usize]
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotated_codes_preserve_dot() {
        let dims = 64;
        let q: Vec<_> = (0..dims).map(|i| (i as f32 * 1.7).sin()).collect();
        let q = super::super::vectors::normalized(&q);
        let r: Vec<_> = (0..dims).map(|i| (i as f32 * 1.3).cos()).collect();
        let r = super::super::vectors::normalized(&r);
        let rq = RaBitQ::new(dims, 42).unwrap();
        let (values, s) = encode(&rq.rotate(&r));
        let c = Codes {
            dims,
            values,
            scales: vec![s],
            dot: dot_kernel(),
        };
        let (code, s) = c.prepare(&q);
        let exact = 1. - q.iter().zip(r).map(|(a, b)| a * b).sum::<f32>();
        assert!((c.score(&code, s, 0) - exact).abs() < 0.003);
    }
    #[test]
    fn graph_dot_preserves_integer_sums_for_all_tails_and_extremes() {
        let dot = dot_kernel();
        let a = (0..4096).map(|i| (i * 37) as i8).collect::<Vec<_>>();
        let b = (0..4096).map(|i| (i * 71 + 19) as i8).collect::<Vec<_>>();
        for dims in 1..=4096 {
            let expected: i32 = a[..dims]
                .iter()
                .zip(&b[..dims])
                .map(|(&a, &b)| i32::from(a) * i32::from(b))
                .sum();
            assert_eq!(dot(&a[..dims], &b[..dims]), expected, "dims={dims}");
            assert_eq!(
                dot(&a[..dims], &b[..dims]),
                i8::dot(&a[..dims], &b[..dims]).unwrap()
            );
        }
        for value in [i8::MIN, -127, 0, 127] {
            let a = vec![value; 4096];
            assert_eq!(dot(&a, &a), i32::from(value).pow(2) * 4096);
        }
    }
}
