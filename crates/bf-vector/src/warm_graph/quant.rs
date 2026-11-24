//! Symmetric eight-bit scalar codes after the production signed Hadamard rotation.
use super::rabitq::RaBitQ;
use numkong::Dot;
pub struct Codes {
    pub dims: usize,
    pub values: Vec<i8>,
    pub scales: Vec<f32>,
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
impl Codes {
    pub fn between(&self, a: u32, b: u32) -> f32 {
        let a = a as usize;
        let b = b as usize;
        (1. - i8::dot(
            &self.values[a * self.dims..(a + 1) * self.dims],
            &self.values[b * self.dims..(b + 1) * self.dims],
        )
        .unwrap() as f32
            * self.scales[a]
            * self.scales[b])
            .max(0.)
    }
    pub fn prepare(&self, q: &[f32]) -> (Vec<i8>, f32) {
        encode(&RaBitQ::new(q.len(), 42).unwrap().rotate(q))
    }
    pub fn score(&self, q: &[i8], scale: f32, id: u32) -> f32 {
        let start = id as usize * self.dims;
        1. - i8::dot(q, &self.values[start..start + self.dims]).unwrap() as f32
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
        };
        let (code, s) = c.prepare(&q);
        let exact = 1. - q.iter().zip(r).map(|(a, b)| a * b).sum::<f32>();
        assert!((c.score(&code, s, 0) - exact).abs() < 0.003);
    }
}
