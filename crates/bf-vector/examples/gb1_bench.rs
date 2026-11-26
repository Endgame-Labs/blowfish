//! Round GB1: build the warm graph from NQ's `corpus.f16` and measure build cost and recall.
//! Usage: gb1_bench NQ_DATA [workers=2] [rows=all] [recall=1] [EXPORTED_IVF]
//! With EXPORTED_IVF (see `g3/export_ivf.py`) rows arrive in slab and cluster order, as in production.
//! Reports build wall and CPU seconds (graph construction only, after source rows are appended),
//! then recall@10 (rerank 30) and recall@100 (rerank 200) against the recorded exact truth.
#[allow(dead_code)]
#[path = "g3/data.rs"]
mod data;
#[allow(dead_code)]
#[path = "g3/measure.rs"]
mod measure;
use bf_vector::{Bytes, Index, RangeRead, warm_graph::HotGraphBuilder};
use numkong::Dot;
use rayon::prelude::*;
use std::{
    fs::File,
    io::{self, Read},
    ops::Range,
    os::unix::fs::FileExt,
    path::Path,
    time::Instant,
};

struct Reader(File, u64);
impl RangeRead for Reader {
    fn len(&self) -> u64 {
        self.1
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let mut bytes = vec![0; (range.end - range.start) as usize];
        self.0.read_exact_at(&mut bytes, range.start)?;
        Ok(bytes.into())
    }
}
fn open(path: &Path) -> bf_vector::Result<Index<Reader>> {
    let f = File::open(path)?;
    let n = f.metadata()?.len();
    futures::executor::block_on(Index::open(Reader(f, n)))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        return Err("usage: gb1_bench NQ_DATA [workers=2] [rows=all] [recall=1]".into());
    }
    let arg = |i: usize, d: usize| {
        args.get(i)
            .map(|s| s.parse())
            .transpose()
            .map(|v| v.unwrap_or(d))
    };
    let workers = arg(2, 2)?;
    let limit = arg(3, usize::MAX)?;
    let recall = arg(4, 1)? != 0;
    let data = Path::new(&args[1]);
    let corpus = File::open(data.join("corpus.f16"))?;
    let (offset, rows, dims) = {
        let mut f = &corpus;
        let mut p = [0; 12];
        f.read_exact(&mut p)?;
        let offset = match p[6] {
            1 => 10 + u16::from_le_bytes(p[8..10].try_into()?) as u64,
            2 | 3 => 12 + u32::from_le_bytes(p[8..12].try_into()?) as u64,
            _ => return Err("unsupported NPY".into()),
        };
        let mut h = vec![0; (offset - 10).min(4096) as usize + 2];
        corpus.read_exact_at(&mut h, 10)?;
        let h = String::from_utf8_lossy(&h);
        let shape = h
            .split("'shape': (")
            .nth(1)
            .ok_or("shape")?
            .split(')')
            .next()
            .ok_or("shape")?;
        let s = shape
            .split(',')
            .filter_map(|x| x.trim().parse::<usize>().ok())
            .collect::<Vec<_>>();
        (offset, s[0], s[1])
    };
    let ivf = args.get(5).map(Path::new);
    let mut files = Vec::new();
    let mut map = Vec::new();
    let mut count = rows.min(limit);
    if let Some(dir) = ivf {
        files = std::fs::read_dir(dir)?
            .map(|e| e.map(|e| e.path()))
            .collect::<io::Result<Vec<_>>>()?;
        files.retain(|p| p.extension().is_some_and(|s| s == "bfa"));
        files.sort();
        map = std::fs::read(dir.join("rowmap.u32"))?
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| u32::from_le_bytes(*b))
            .collect::<Vec<_>>();
        count = limit.min(rows);
    }
    let mut members = Vec::new();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()?;
    let mut builder = HotGraphBuilder::new(dims, count)?;
    let t = Instant::now();
    let c0 = usage().0;
    if ivf.is_some() {
        for file in &files {
            let index = open(file)?;
            for c in 0..index.cluster_count() {
                let (ids, bytes) = futures::executor::block_on(index.read_cluster_rows(c))?;
                let take = ids.len().min(count - members.len());
                if take == 0 {
                    break;
                }
                let ids = ids[..take]
                    .iter()
                    .map(|r| map[*r as usize])
                    .collect::<Vec<_>>();
                builder.append(&ids, &bytes[..take * dims * 2], &|| false)?;
                members.extend(ids);
            }
        }
    } else {
        let chunk = 4096;
        let mut buf = vec![0u8; chunk * dims * 2];
        for start in (0..count).step_by(chunk) {
            let n = chunk.min(count - start);
            let b = &mut buf[..n * dims * 2];
            corpus.read_exact_at(b, offset + (start * dims * 2) as u64)?;
            let ids = (start as u32..(start + n) as u32).collect::<Vec<_>>();
            builder.append(&ids, b, &|| false)?;
        }
    }
    println!(
        "source rows={count} seconds={:.3} cpu_seconds={:.3}",
        t.elapsed().as_secs_f64(),
        usage().0 - c0
    );
    let t = Instant::now();
    let c0 = usage().0;
    let graph = pool.install(|| builder.finish(&|| false))?;
    println!(
        "built rows={} workers={workers} seconds={:.3} cpu_seconds={:.3} peak_rss_kib={}",
        graph.len(),
        t.elapsed().as_secs_f64(),
        usage().0 - c0,
        usage().1
    );
    if !recall {
        return Ok(());
    }
    let queries = data::Matrix::load(&data.join("queries.npy"), "<f4", 4, usize::MAX)?.floats();
    let mut values = vec![half::f16::ZERO; rows * dims];
    // SAFETY: f16 admits every bit pattern; the byte length exactly spans the allocation.
    let bytes = unsafe {
        std::slice::from_raw_parts_mut(values.as_mut_ptr().cast::<u8>(), values.len() * 2)
    };
    corpus.read_exact_at(bytes, offset)?;
    let inv_norms = values
        .par_chunks_exact(dims)
        .map(|v| {
            // SAFETY: both transparent wrappers have identical IEEE binary16 layout.
            let v =
                unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<numkong::f16>(), v.len()) };
            1. / numkong::f16::dot(v, v).unwrap().sqrt()
        })
        .collect();
    let exact = data::Vectors {
        dims,
        values,
        inv_norms,
    };
    let eval = rayon::ThreadPoolBuilder::new().build()?;
    let truth = if count < rows {
        // A subset has no recorded truth: scan it exactly with the half-precision kernel.
        eval.install(|| {
            queries
                .par_iter()
                .map(|q| {
                    let q = q
                        .iter()
                        .map(|x| half::f16::from_f32(*x))
                        .collect::<Vec<_>>();
                    let mut all = members
                        .iter()
                        .map(|&id| (exact.query(&q, id), id))
                        .collect::<Vec<_>>();
                    all.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                    all.iter().take(100).map(|h| h.1).collect::<Vec<u32>>()
                })
                .collect::<Vec<_>>()
        })
    } else {
        let m = data::Matrix::load(&data.join("truth.npy"), "<i4", 4, usize::MAX)?;
        m.bytes
            .chunks_exact(m.cols * 4)
            .map(|r| {
                r.as_chunks::<4>()
                    .0
                    .iter()
                    .take(100)
                    .map(|b| u32::from_le_bytes(*b))
                    .collect::<Vec<u32>>()
            })
            .collect::<Vec<_>>()
    };
    for (k, sl, efs) in [
        (10, 30, &[48usize, 64, 128, 256][..]),
        (100, 200, &[256, 400, 512][..]),
    ] {
        for &ef in efs {
            let r = eval.install(|| {
                measure::parallel(
                    queries.len(),
                    || graph.scratch(),
                    |scratch, i| {
                        let (ids, work) = graph
                            .candidates(&queries[i], ef, sl, None, scratch)
                            .unwrap();
                        let q = data::normalized(&queries[i]);
                        let mut hits = ids
                            .into_iter()
                            .map(|id| (id, exact.rerank(&q, id)))
                            .collect::<Vec<_>>();
                        hits.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                        let t = &truth[i][..k];
                        (
                            hits.iter().take(k).filter(|h| t.contains(&h.0)).count(),
                            work,
                        )
                    },
                )
            });
            let found: usize = r.iter().map(|x| x.0).sum();
            let evals: usize = r.iter().map(|x| x.1).sum();
            println!(
                "recall@{k} ef={ef} rerank={sl} recall={:.6} scores={:.1}",
                found as f64 / (queries.len() * k) as f64,
                evals as f64 / queries.len() as f64
            );
        }
    }
    Ok(())
}

fn usage() -> (f64, u64) {
    let mut out = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes the complete output on success, and the pointer is valid.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, out.as_mut_ptr()) } != 0 {
        return (0., 0);
    }
    // SAFETY: the call succeeded and initialized this rusage.
    let out = unsafe { out.assume_init() };
    let cpu = out.ru_utime.tv_sec as f64
        + out.ru_utime.tv_usec as f64 / 1e6
        + out.ru_stime.tv_sec as f64
        + out.ru_stime.tv_usec as f64 / 1e6;
    (cpu, out.ru_maxrss as u64)
}
