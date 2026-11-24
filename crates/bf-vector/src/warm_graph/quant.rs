//! Symmetric eight-bit scalar codes after the production signed Hadamard rotation.
use super::{rabitq::RaBitQ, vectors::Vectors};
use numkong::Dot;
use rayon::prelude::*;
pub struct Codes {
    pub dims: usize,
    pub values: Vec<i8>,
    pub scales: Vec<f32>,
}
fn encode(row: &[f64]) -> (Vec<i8>, f32) {
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
    pub fn build(v: &Vectors) -> Self {
        let rq = RaBitQ::new(v.dims, 42).unwrap();
        let dims = v.dims.next_power_of_two();
        let mut values = vec![0i8; v.len() * dims];
        let scales = values
            .par_chunks_mut(dims)
            .enumerate()
            .map(|(id, out)| {
                let (code, scale) = encode(&rq.rotate(&v.normalized(id as u32)));
                out.copy_from_slice(&code);
                scale
            })
            .collect();
        Self {
            dims,
            values,
            scales,
        }
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
