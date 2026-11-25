//! Symmetric eight-bit scalar codes after the production signed Hadamard rotation.
use super::{
    data::{Result, Vectors},
    rabitq::RaBitQ,
};
use numkong::Dot;
use rayon::prelude::*;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};
pub struct Codes {
    pub dims: usize,
    pub values: Vec<i8>,
    pub scales: Vec<f32>,
    /// Bytes between row starts (G7 kill test); `dims` unless repacked.
    pub stride: usize,
    /// Offset of row 0 inside `values`, so every stride starts on a 4 KiB boundary.
    pub base: usize,
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
            stride: dims,
            base: 0,
        }
    }
    /// Repack rows at `stride` bytes (>= dims), zero padded, with row 0 on a 4 KiB boundary.
    pub fn repack(&self, stride: usize) -> Self {
        assert!(stride >= self.dims);
        let rows = self.scales.len();
        let mut values = vec![0i8; rows * stride + 4096];
        let base = values.as_ptr().align_offset(4096);
        for r in 0..rows {
            let src = self.base + r * self.stride;
            let dst = base + r * stride;
            values[dst..dst + self.dims].copy_from_slice(&self.values[src..src + self.dims]);
        }
        Self {
            dims: self.dims,
            values,
            scales: self.scales.clone(),
            stride,
            base,
        }
    }
    pub fn prepare(&self, q: &[f32]) -> (Vec<i8>, f32) {
        encode(&RaBitQ::new(q.len(), 42).unwrap().rotate(q))
    }
    pub fn score(&self, q: &[i8], scale: f32, id: u32) -> f32 {
        let start = self.base + id as usize * self.stride;
        1. - i8::dot(q, &self.values[start..start + self.dims]).unwrap() as f32
            * scale
            * self.scales[id as usize]
    }
    pub fn write(&self, path: &Path) -> Result<()> {
        let mut f = BufWriter::new(File::create(path)?);
        f.write_all(b"G3CODE08")?;
        for n in [self.scales.len() as u64, self.dims as u64] {
            f.write_all(&n.to_le_bytes())?;
        }
        for s in &self.scales {
            f.write_all(&s.to_le_bytes())?;
        }
        // SAFETY: i8 and u8 have identical layout and alignment, and admit every bit pattern.
        let bytes = unsafe {
            std::slice::from_raw_parts(self.values.as_ptr().cast::<u8>(), self.values.len())
        };
        f.write_all(bytes)?;
        f.flush()?;
        Ok(())
    }
    pub fn read(path: &Path) -> Result<Self> {
        let mut f = BufReader::new(File::open(path)?);
        let mut magic = [0; 8];
        f.read_exact(&mut magic)?;
        if &magic != b"G3CODE08" {
            return Err("not G3 eight-bit codes".into());
        }
        let mut b = [0; 8];
        f.read_exact(&mut b)?;
        let rows = u64::from_le_bytes(b) as usize;
        f.read_exact(&mut b)?;
        let dims = u64::from_le_bytes(b) as usize;
        if rows == 0 || dims == 0 || dims > 65536 {
            return Err("invalid code shape".into());
        }
        let mut scales = Vec::with_capacity(rows);
        for _ in 0..rows {
            let mut b = [0; 4];
            f.read_exact(&mut b)?;
            let s = f32::from_le_bytes(b);
            if !s.is_finite() || s <= 0. {
                return Err("invalid scale".into());
            }
            scales.push(s);
        }
        let mut values = vec![0i8; rows.checked_mul(dims).ok_or("code overflow")?];
        // SAFETY: i8 and u8 have identical layout and alignment, and admit every bit pattern.
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(values.as_mut_ptr().cast::<u8>(), values.len())
        };
        f.read_exact(bytes)?;
        let mut b = [0];
        if f.read(&mut b)? != 0 {
            return Err("trailing codes".into());
        }
        Ok(Self {
            dims,
            values,
            scales,
            stride: dims,
            base: 0,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotated_codes_preserve_dot() {
        let dims = 64;
        let q: Vec<_> = (0..dims).map(|i| (i as f32 * 1.7).sin()).collect();
        let q = super::super::data::normalized(&q);
        let r: Vec<_> = (0..dims).map(|i| (i as f32 * 1.3).cos()).collect();
        let r = super::super::data::normalized(&r);
        let rq = RaBitQ::new(dims, 42).unwrap();
        let (values, s) = encode(&rq.rotate(&r));
        let c = Codes {
            dims,
            values,
            stride: dims,
            base: 0,
            scales: vec![s],
        };
        let (code, s) = c.prepare(&q);
        let exact = 1. - q.iter().zip(r).map(|(a, b)| a * b).sum::<f32>();
        assert!((c.score(&code, s, 0) - exact).abs() < 0.003);
    }
}
