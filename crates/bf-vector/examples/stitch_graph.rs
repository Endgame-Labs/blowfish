//! Bounded-RAM partition-and-stitch Vamana experiment. No stored blowfish format changes.
//! Usage: stitch_graph EXPORTED_IVF NQ_DATA SCRATCH_DIRECTORY [boundary_ratio=1.03]
#[allow(dead_code)]
#[path = "g3/data.rs"]
mod data;
#[allow(dead_code)]
#[path = "g3/quant.rs"]
mod quant;
#[allow(dead_code)]
#[path = "../src/rabitq.rs"]
mod rabitq;
#[path = "g3/vamana.rs"]
mod vamana;
pub use bf_vector::{Error, Result};
mod distance {
    pub fn validate(v: &[f32], dims: usize) -> crate::Result<()> {
        if v.len() != dims || v.is_empty() || v.iter().any(|x| !x.is_finite()) {
            Err(crate::Error::InvalidInput("invalid vector".into()))
        } else {
            Ok(())
        }
    }
}
mod format {
    pub fn bad(s: &str) -> crate::Error {
        crate::Error::InvalidFormat(s.into())
    }
}
use bf_vector::{Bytes, Index, RangeRead};
use data::{Matrix, Vectors};
use half::f16;
use numkong::Dot;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    ops::Range,
    os::unix::fs::FileExt,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
const R: usize = 32;
const RECORD: usize = (R + 1) * 4;
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
fn open(p: &Path) -> data::Result<Index<Reader>> {
    let f = File::open(p)?;
    let n = f.metadata()?.len();
    Ok(futures::executor::block_on(Index::open(Reader(f, n)))?)
}
fn dot(a: &[f16], b: &[f16]) -> f32 {
    // SAFETY: both transparent wrappers store IEEE binary16 with matching layout and alignment.
    let cast = |s: &[f16]| unsafe {
        std::slice::from_raw_parts(s.as_ptr().cast::<numkong::f16>(), s.len())
    };
    numkong::f16::dot(cast(a), cast(b)).unwrap()
}
fn decode(b: &[u8]) -> Vec<f16> {
    b.as_chunks::<2>()
        .0
        .iter()
        .map(|b| f16::from_le_bytes(*b))
        .collect()
}
fn cosine(a: &[f16], b: &[f16]) -> f32 {
    (1. - dot(a, b) / (dot(a, a) * dot(b, b)).sqrt()).max(0.)
}
fn word(out: &mut impl Write, n: u32) -> io::Result<()> {
    out.write_all(&n.to_le_bytes())
}
fn main() -> data::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 4 {
        return Err(
            "usage: stitch_graph EXPORTED_IVF NQ_DATA SCRATCH_DIRECTORY [ratio=1.03]".into(),
        );
    }
    let root = Path::new(&args[3]);
    if !root.components().any(|c| c.as_os_str() == ".scratch") {
        return Err("scratch output required".into());
    }
    std::fs::create_dir_all(root)?;
    let ratio = args
        .get(4)
        .map(|s| s.parse::<f32>())
        .transpose()?
        .unwrap_or(1.03);
    let start = Instant::now();
    let mut files = std::fs::read_dir(&args[1])?
        .map(|p| p.map(|p| p.path()))
        .collect::<io::Result<Vec<_>>>()?;
    files.retain(|p| p.extension().is_some_and(|x| x == "bfa"));
    files.sort();
    if files.len() < 2 {
        return Err("at least two slabs required".into());
    }
    let map = std::fs::read(Path::new(&args[1]).join("rowmap.u32"))?;
    let rowmap = map
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| u32::from_le_bytes(*b))
        .collect::<Vec<_>>();
    drop(map);
    let mut centers = Vec::new();
    let mut dims = 0;
    let mut count = 0;
    for file in &files {
        let index = open(file)?;
        dims = index.dims();
        count += index.len();
        let sum = (0..index.cluster_count())
            .into_par_iter()
            .map(|c| {
                let (_, b) = futures::executor::block_on(index.read_cluster_rows(c)).unwrap();
                let mut sum = vec![0f64; dims];
                for b in b.chunks_exact(dims * 2) {
                    let v = decode(b);
                    let norm = dot(&v, &v).sqrt() as f64;
                    for (s, x) in sum.iter_mut().zip(v) {
                        *s += x.to_f64() / norm;
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
    let mut outputs = (0..files.len())
        .map(|i| {
            File::create(root.join(format!("part-{i}.raw")))
                .map(|f| BufWriter::with_capacity(64 << 10, f))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let mut overlap = 0usize;
    for (primary, file) in files.iter().enumerate() {
        let index = open(file)?;
        for cluster in 0..index.cluster_count() {
            let (ids, b) = futures::executor::block_on(index.read_cluster_rows(cluster))?;
            let assignments = b
                .par_chunks_exact(dims * 2)
                .map(|b| {
                    let v = decode(b);
                    let mut order = centers
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (i, cosine(&v, c)))
                        .collect::<Vec<_>>();
                    order.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
                    (order[1].1 <= order[0].1 * ratio)
                        .then(|| order.iter().find(|p| p.0 != primary).unwrap().0)
                })
                .collect::<Vec<_>>();
            for ((row, b), secondary) in ids
                .into_iter()
                .zip(b.chunks_exact(dims * 2))
                .zip(assignments)
            {
                let id = *rowmap.get(row as usize).ok_or("missing corpus row map")?;
                if id as usize >= count {
                    return Err("invalid corpus ID".into());
                }
                word(&mut outputs[primary], id)?;
                outputs[primary].write_all(b)?;
                if let Some(secondary) = secondary {
                    word(&mut outputs[secondary], id)?;
                    outputs[secondary].write_all(b)?;
                    overlap += 1;
                }
            }
        }
    }
    for o in &mut outputs {
        o.flush()?;
    }
    drop(outputs);
    eprintln!(
        "partition rows={count} overlap={overlap} fraction={:.6} seconds={:.3} rss_kib={}",
        overlap as f64 / count as f64,
        start.elapsed().as_secs_f64(),
        data::rss_kib()
    );
    let padded = dims.next_power_of_two();
    let codefile = Arc::new(
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(root.join("rows.graph.codes8"))?,
    );
    codefile.set_len((24 + count * 4 + count * padded) as u64)?;
    codefile.write_all_at(b"G3CODE08", 0)?;
    codefile.write_all_at(&(count as u64).to_le_bytes(), 8)?;
    codefile.write_all_at(&(padded as u64).to_le_bytes(), 16)?;
    let adjacency = Arc::new(
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(root.join("adjacency.raw"))?,
    );
    adjacency.set_len((count * RECORD) as u64)?;
    let present = (0..count)
        .map(|_| AtomicBool::new(false))
        .collect::<Vec<_>>();
    let mut corpus = File::open(Path::new(&args[2]).join("corpus.f16"))?;
    let mut prefix = [0; 12];
    corpus.read_exact(&mut prefix)?;
    if &prefix[..6] != b"\x93NUMPY" {
        return Err("expected NPY corpus".into());
    }
    let offset = match prefix[6] {
        1 => 10 + u16::from_le_bytes(prefix[8..10].try_into()?) as u64,
        2 | 3 => 12 + u32::from_le_bytes(prefix[8..12].try_into()?) as u64,
        _ => return Err("unsupported NPY version".into()),
    };
    let mut medoids = Vec::new();
    for (part, center) in centers.iter().enumerate() {
        let p = root.join(format!("part-{part}.raw"));
        let n = std::fs::metadata(&p)?.len() as usize / (dims * 2 + 4);
        let mut input = BufReader::new(File::open(&p)?);
        let mut ids = Vec::with_capacity(n);
        let mut packed = vec![0; n * dims * 2];
        for row in packed.chunks_exact_mut(dims * 2) {
            let mut b = [0; 4];
            input.read_exact(&mut b)?;
            ids.push(u32::from_le_bytes(b));
            input.read_exact(row)?;
        }
        drop(input);
        let v = Vectors::from_matrix(Matrix {
            rows: n,
            cols: dims,
            bytes: packed,
        });
        let local = ids
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, i as u32))
            .collect::<HashMap<_, _>>();
        let entry = (0..n as u32)
            .min_by(|a, b| v.query(center, *a).total_cmp(&v.query(center, *b)))
            .ok_or("empty partition")?;
        medoids.push(ids[entry as usize]);
        let t = Instant::now();
        let codes = quant::Codes::build(&v);
        let links = vamana::build(&v, entry, R, 64);
        ids.par_iter()
            .enumerate()
            .try_for_each(|(i, &id)| -> io::Result<()> {
                let mut neighbors = links[i]
                    .iter()
                    .map(|n| ids[*n as usize])
                    .collect::<Vec<_>>();
                let existed = present[id as usize].swap(true, Ordering::Relaxed);
                if !existed {
                    codefile.write_all_at(&codes.scales[i].to_le_bytes(), 24 + id as u64 * 4)?;
                    let values = &codes.values[i * padded..(i + 1) * padded];
                    // SAFETY: i8 and u8 have identical layout, alignment, and valid bit patterns.
                    let bytes = unsafe {
                        std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), values.len())
                    };
                    codefile
                        .write_all_at(bytes, 24 + count as u64 * 4 + id as u64 * padded as u64)?;
                }
                if existed {
                    let mut record = [0u8; RECORD];
                    adjacency.read_exact_at(&mut record, id as u64 * RECORD as u64)?;
                    let words = record.as_chunks::<4>().0;
                    let degree = u32::from_le_bytes(words[0]) as usize;
                    if degree > R {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid adjacency degree",
                        ));
                    }
                    neighbors.extend(words[1..1 + degree].iter().map(|w| u32::from_le_bytes(*w)));
                    neighbors.sort_unstable();
                    neighbors.dedup();
                    neighbors.retain(|p| *p != id);
                    let mut vectors = HashMap::new();
                    vectors.insert(id, v.row(i as u32).to_vec());
                    for &p in &neighbors {
                        let values = if let Some(&i) = local.get(&p) {
                            v.row(i).to_vec()
                        } else {
                            let mut bytes = vec![0; dims * 2];
                            corpus
                                .read_exact_at(&mut bytes, offset + p as u64 * dims as u64 * 2)?;
                            decode(&bytes)
                        };
                        vectors.insert(p, values);
                    }
                    neighbors = vamana::prune(id, neighbors, R, 1.2, |a, b| {
                        cosine(&vectors[&a], &vectors[&b])
                    });
                }
                let mut record = [0u8; RECORD];
                record[..4].copy_from_slice(&(neighbors.len() as u32).to_le_bytes());
                for (p, out) in neighbors
                    .into_iter()
                    .zip(record[4..].as_chunks_mut::<4>().0.iter_mut())
                {
                    out.copy_from_slice(&p.to_le_bytes());
                }
                adjacency.write_all_at(&record, id as u64 * RECORD as u64)?;
                Ok(())
            })?;
        eprintln!(
            "part={part} rows={n} build_and_stitch_seconds={:.3} total_seconds={:.3} rss_kib={}",
            t.elapsed().as_secs_f64(),
            start.elapsed().as_secs_f64(),
            data::rss_kib()
        );
        std::fs::remove_file(p)?;
    }
    if present.iter().any(|p| !p.load(Ordering::Relaxed)) {
        return Err("missing primary rows".into());
    }
    medoids.sort_unstable();
    medoids.dedup();
    let mut out = BufWriter::new(File::create(root.join("rows.graph"))?);
    out.write_all(b"G3SCR001")?;
    word(&mut out, count as u32)?;
    word(&mut out, medoids[0])?;
    for id in 0..count as u32 {
        let upper = medoids.contains(&id);
        word(&mut out, if upper { 2 } else { 1 })?;
        let mut record = [0u8; RECORD];
        adjacency.read_exact_at(&mut record, id as u64 * RECORD as u64)?;
        let degree = u32::from_le_bytes(record[..4].try_into()?) as usize;
        out.write_all(&record[..(degree + 1) * 4])?;
        if upper {
            word(&mut out, medoids.len() as u32 - 1)?;
            for &p in &medoids {
                if p != id {
                    word(&mut out, p)?;
                }
            }
        }
    }
    out.flush()?;
    drop(out);
    drop(adjacency);
    std::fs::remove_file(root.join("adjacency.raw"))?;
    println!(
        "stitched rows={count} overlap={overlap} ratio={ratio} degree={R} beam=64 seconds={:.3} rss_kib={}",
        start.elapsed().as_secs_f64(),
        data::rss_kib()
    );
    Ok(())
}
