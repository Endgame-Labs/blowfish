use half::f16;
use numkong::Dot;
use rayon::prelude::*;
pub struct Matrix {
    pub rows: usize,
    pub cols: usize,
    pub bytes: Vec<u8>,
}
pub struct Vectors {
    pub dims: usize,
    pub values: Vec<f16>,
    pub inv_norms: Vec<f32>,
}
impl Vectors {
    pub fn from_matrix(m: Matrix) -> Self {
        let values: Vec<_> = m
            .bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| f16::from_bits(u16::from_le_bytes(*b)))
            .collect();
        assert_eq!(values.len(), m.rows * m.cols);
        drop(m.bytes);
        let inv_norms = values
            .par_chunks(m.cols)
            .map(|v| {
                let norm = dot(v, v).sqrt();
                assert!(
                    norm.is_finite() && norm > 0.,
                    "graph rows must be finite and nonzero"
                );
                1. / norm
            })
            .collect();
        Self {
            dims: m.cols,
            values,
            inv_norms,
        }
    }
    pub fn len(&self) -> usize {
        self.inv_norms.len()
    }
    pub fn row(&self, i: u32) -> &[f16] {
        let i = i as usize * self.dims;
        &self.values[i..i + self.dims]
    }
    pub fn normalized(&self, i: u32) -> Vec<f32> {
        self.row(i)
            .iter()
            .map(|x| x.to_f32() * self.inv_norms[i as usize])
            .collect()
    }
    pub fn between(&self, a: u32, b: u32) -> f32 {
        (1. - dot(self.row(a), self.row(b))
            * self.inv_norms[a as usize]
            * self.inv_norms[b as usize])
            .max(0.)
    }
    pub fn query(&self, q: &[f16], row: u32) -> f32 {
        1. - dot(q, self.row(row)) * self.inv_norms[row as usize]
    }
}
fn dot(a: &[f16], b: &[f16]) -> f32 {
    // SAFETY: both repr(transparent) types wrap IEEE binary16 u16, with matching alignment.
    let cast = |s: &[f16]| unsafe {
        std::slice::from_raw_parts(s.as_ptr().cast::<numkong::f16>(), s.len())
    };
    numkong::f16::dot(cast(a), cast(b)).unwrap()
}
pub fn normalized(q: &[f32]) -> Vec<f32> {
    let norm = q.iter().map(|x| x * x).sum::<f32>().sqrt();
    q.iter().map(|x| x / norm).collect()
}
