//! Evaluate the proposed two-slab boundary selector before constructing stitched graphs.
//! Usage: stitch_plan EXPORTED_IVF_DIRECTORY
//! Reads one cluster per worker; it does not load or build a full-corpus graph.
use bf_vector::{Bytes, Index, RangeRead};
use half::f16;
use numkong::Dot;
use rayon::prelude::*;
use std::{fs::File, io, ops::Range, os::unix::fs::FileExt, path::Path, time::Instant};
struct Reader(File, u64);
impl RangeRead for Reader {
    fn len(&self) -> u64 {
        self.1
    }
    async fn read(&self, r: Range<u64>) -> io::Result<Bytes> {
        let mut b = vec![0; (r.end - r.start) as usize];
        self.0.read_exact_at(&mut b, r.start)?;
        Ok(Bytes::from(b))
    }
}
fn dot(a: &[f16], b: &[f16]) -> f32 {
    // SAFETY: both transparent wrappers store IEEE binary16 with identical alignment.
    let cast = |s: &[f16]| unsafe {
        std::slice::from_raw_parts(s.as_ptr().cast::<numkong::f16>(), s.len())
    };
    numkong::f16::dot(cast(a), cast(b)).unwrap()
}
fn open(p: &Path) -> bf_vector::Result<Index<Reader>> {
    let file = File::open(p)?;
    let len = file.metadata()?.len();
    futures::executor::block_on(Index::open(Reader(file, len)))
}
fn row(bytes: &[u8]) -> Vec<f16> {
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| f16::from_le_bytes(*b))
        .collect()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .ok_or("usage: stitch_plan EXPORTED_IVF_DIRECTORY")?;
    let mut files = std::fs::read_dir(root)?
        .map(|p| p.map(|p| p.path()))
        .collect::<Result<Vec<_>, _>>()?;
    files.retain(|p| p.extension().is_some_and(|e| e == "bfa"));
    files.sort();
    if files.len() < 2 {
        return Err("at least two slabs required".into());
    }
    let start = Instant::now();
    let mut centers = Vec::new();
    for file in &files {
        let index = open(file)?;
        let dims = index.dims();
        let sum = (0..index.cluster_count())
            .into_par_iter()
            .map(|cluster| {
                let (_, packed) =
                    futures::executor::block_on(index.read_cluster_rows(cluster)).unwrap();
                let mut sum = vec![0f64; dims];
                for bytes in packed.chunks_exact(dims * 2) {
                    let v = row(bytes);
                    let norm = dot(&v, &v).sqrt();
                    for (out, x) in sum.iter_mut().zip(v) {
                        *out += x.to_f64() / norm as f64;
                    }
                }
                sum
            })
            .reduce(
                || vec![0.; dims],
                |mut a, b| {
                    for (a, b) in a.iter_mut().zip(b) {
                        *a += b;
                    }
                    a
                },
            );
        let norm = sum.iter().map(|x| x * x).sum::<f64>().sqrt();
        centers.push(
            sum.iter()
                .map(|x| f16::from_f64(x / norm))
                .collect::<Vec<_>>(),
        );
    }
    let center_inv: Vec<_> = centers.iter().map(|c| dot(c, c).sqrt().recip()).collect();
    let thresholds = [1.01, 1.03, 1.05, 1.10, 1.15];
    let mut totals = [0usize; 6];
    for (slab, file) in files.iter().enumerate() {
        let index = open(file)?;
        let dims = index.dims();
        let counts = (0..index.cluster_count())
            .into_par_iter()
            .map(|cluster| {
                let (_, packed) =
                    futures::executor::block_on(index.read_cluster_rows(cluster)).unwrap();
                let mut counts = [0usize; 6];
                for bytes in packed.chunks_exact(dims * 2) {
                    let v = row(bytes);
                    let inv = dot(&v, &v).sqrt().recip();
                    let mut distances = centers
                        .iter()
                        .zip(&center_inv)
                        .map(|(center, ci)| (1. - dot(&v, center) * inv * ci).max(0.))
                        .collect::<Vec<_>>();
                    distances.sort_unstable_by(f32::total_cmp);
                    for (i, t) in thresholds.iter().enumerate() {
                        counts[i] += usize::from(distances[1] <= distances[0] * t);
                    }
                    counts[5] += 1;
                }
                counts
            })
            .reduce(
                || [0; 6],
                |mut a, b| {
                    for (a, b) in a.iter_mut().zip(b) {
                        *a += b;
                    }
                    a
                },
            );
        for (a, b) in totals.iter_mut().zip(counts) {
            *a += b;
        }
        eprintln!(
            "slab={slab} rows={} seconds={:.3}",
            totals[5],
            start.elapsed().as_secs_f64()
        );
    }
    for (i, t) in thresholds.iter().enumerate() {
        println!(
            "ratio={t:.2} rows={} boundary_rows={} overlap={:.6} seconds={:.3}",
            totals[5],
            totals[i],
            totals[i] as f64 / totals[5] as f64,
            start.elapsed().as_secs_f64()
        );
    }
    Ok(())
}
