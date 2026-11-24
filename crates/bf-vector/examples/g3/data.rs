//! Strict NumPy input and native half-precision cosine scoring for the offline study.
use half::f16;
use numkong::Dot;
use rayon::prelude::*;
use std::{
    fs::File,
    io::{Read, Seek},
    path::Path,
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct Matrix {
    pub rows: usize,
    pub cols: usize,
    pub bytes: Vec<u8>,
}
impl Matrix {
    pub fn load(path: &Path, dtype: &str, width: usize, limit: usize) -> Result<Self> {
        let mut f = File::open(path)?;
        let mut p = [0; 8];
        f.read_exact(&mut p)?;
        if &p[..6] != b"\x93NUMPY" {
            return Err("not NumPy".into());
        }
        let n = match p[6] {
            1 => {
                let mut b = [0; 2];
                f.read_exact(&mut b)?;
                u16::from_le_bytes(b) as usize
            }
            2 => {
                let mut b = [0; 4];
                f.read_exact(&mut b)?;
                u32::from_le_bytes(b) as usize
            }
            _ => return Err("unsupported NumPy version".into()),
        };
        let mut h = vec![0; n];
        f.read_exact(&mut h)?;
        let h = String::from_utf8(h)?;
        if !h.contains(dtype) || !h.contains("'fortran_order': False") {
            return Err("wrong dtype or layout".into());
        }
        let shape = h
            .split("'shape': (")
            .nth(1)
            .ok_or("missing shape")?
            .split(')')
            .next()
            .ok_or("missing shape end")?;
        let shape: Vec<usize> = shape
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().parse())
            .collect::<std::result::Result<_, _>>()?;
        if shape.len() != 2 || shape.contains(&0) {
            return Err("expected nonempty matrix".into());
        }
        let offset = f.stream_position()?;
        let size = shape[0]
            .checked_mul(shape[1])
            .and_then(|s| s.checked_mul(width))
            .ok_or("matrix overflow")?;
        if f.metadata()?.len() != offset + size as u64 {
            return Err("wrong matrix length".into());
        }
        let rows = shape[0].min(limit);
        let mut bytes = vec![0; rows * shape[1] * width];
        f.read_exact(&mut bytes)?;
        Ok(Self {
            rows,
            cols: shape[1],
            bytes,
        })
    }
    pub fn floats(&self) -> Vec<Vec<f32>> {
        self.bytes
            .chunks_exact(self.cols * 4)
            .map(|row| {
                row.as_chunks::<4>()
                    .0
                    .iter()
                    .map(|b| f32::from_le_bytes(*b))
                    .collect()
            })
            .collect()
    }
    pub fn truth(&self) -> Vec<Vec<u32>> {
        self.bytes
            .chunks_exact(self.cols * 4)
            .map(|row| {
                row.as_chunks::<4>()
                    .0
                    .iter()
                    .take(10)
                    .map(|b| u32::from_le_bytes(*b))
                    .collect()
            })
            .collect()
    }
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
                    "NQ rows must be finite and nonzero"
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
    // Retain f32 query precision for the exact rerank, as in the reference truth.
    pub fn rerank(&self, q: &[f32], row: u32) -> f32 {
        let v = self.row(row);
        1. - v.iter().zip(q).map(|(a, b)| a.to_f32() * b).sum::<f32>()
            * self.inv_norms[row as usize]
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
pub fn rss_kib() -> usize {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|s| s.starts_with("VmRSS:"))
        .and_then(|s| s.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn half_cosine_preserves_scale_and_f32_rerank() {
        let values = [2., 0., 0., 0., 3., 0., 1., 1., 0.];
        let bytes = values
            .into_iter()
            .flat_map(|x| f16::from_f32(x).to_bits().to_le_bytes())
            .collect();
        let v = Vectors::from_matrix(Matrix {
            rows: 3,
            cols: 3,
            bytes,
        });
        assert!(v.between(0, 0).abs() < 1e-6);
        assert!((v.between(0, 1) - 1.).abs() < 1e-6);
        let q = normalized(&[1., 1., 0.]);
        assert!(v.rerank(&q, 2).abs() < 1e-6);
        assert!((v.rerank(&q, 0) - (1. - 2f32.sqrt() / 2.)).abs() < 1e-6);
    }
}
