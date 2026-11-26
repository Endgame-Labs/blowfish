//! Round G8 kill test: sweep graph beam width and exact-rerank shortlist at top 100.
//! Usage: g8_sweep EXPORTED_IVF NQ_DATA [build_workers=2]
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
    if args.len() < 3 {
        return Err("usage: g8_sweep EXPORTED_IVF NQ_DATA [build_workers=2]".into());
    }
    let workers = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(2);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()?;
    let mut files = std::fs::read_dir(&args[1])?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?;
    files.retain(|p| p.extension().is_some_and(|s| s == "bfa"));
    files.sort();
    let map = std::fs::read(Path::new(&args[1]).join("rowmap.u32"))?;
    let map = map
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| u32::from_le_bytes(*b))
        .collect::<Vec<_>>();
    let count = files
        .iter()
        .map(|f| open(f).map(|i| i.len()))
        .collect::<bf_vector::Result<Vec<_>>>()?
        .into_iter()
        .sum();
    let dims = open(&files[0])?.dims();
    let start = Instant::now();
    let cpu_before = usage().0;
    let mut builder = HotGraphBuilder::new(dims, count)?;
    for file in &files {
        let index = open(file)?;
        for c in 0..index.cluster_count() {
            let (ids, bytes) = futures::executor::block_on(index.read_cluster_rows(c))?;
            let ids = ids.iter().map(|r| map[*r as usize]).collect::<Vec<_>>();
            builder.append(&ids, &bytes, &|| false)?;
        }
    }
    let graph = pool.install(|| builder.finish(&|| false))?;
    println!(
        "built rows={} workers={} seconds={:.3} cpu_seconds={:.3} resident_bytes={} rss_kib={} peak_rss_kib={}",
        graph.len(),
        workers,
        start.elapsed().as_secs_f64(),
        usage().0 - cpu_before,
        graph.resident_bytes(),
        data::rss_kib(),
        usage().1
    );
    let data = Path::new(&args[2]);
    let queries = data::Matrix::load(&data.join("queries.npy"), "<f4", 4, usize::MAX)?.floats();
    let truth = {
        let m = data::Matrix::load(&data.join("truth.npy"), "<i4", 4, usize::MAX)?;
        m.bytes
            .chunks_exact(m.cols * 4)
            .map(|r| {
                r.as_chunks::<4>()
                    .0
                    .iter()
                    .take(100)
                    .map(|b| u32::from_le_bytes(*b))
                    .collect::<std::collections::HashSet<u32>>()
            })
            .collect::<Vec<_>>()
    };
    let mut corpus = File::open(data.join("corpus.f16"))?;
    let mut prefix = [0; 12];
    corpus.read_exact(&mut prefix)?;
    let offset = match prefix[6] {
        1 => 10 + u16::from_le_bytes(prefix[8..10].try_into()?) as u64,
        2 | 3 => 12 + u32::from_le_bytes(prefix[8..12].try_into()?) as u64,
        _ => return Err("unsupported NPY".into()),
    };
    // Load exact values directly into one f16 allocation after recording the build phase.
    // This gives the same resident rerank boundary as the monolithic reference, without a
    // second full-corpus packed buffer hiding in the measured construction footprint.
    let mut values = vec![half::f16::ZERO; count * dims];
    // SAFETY: f16 admits every bit pattern, its allocation is writable, and the byte length
    // exactly spans the allocation. This harness accepts the little-endian NQ encoding.
    let bytes = unsafe {
        std::slice::from_raw_parts_mut(values.as_mut_ptr().cast::<u8>(), values.len() * 2)
    };
    corpus.read_exact_at(bytes, offset)?;
    let inv_norms = values
        .par_chunks_exact(dims)
        .map(|v| {
            // SAFETY: both transparent wrappers have identical IEEE binary16 layout and alignment.
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
    // Single-thread serial timing: the minimum of three passes, split into navigation and exact rerank.
    let ef_list = [48usize, 64, 100, 128, 160, 200, 256, 320, 400, 512, 768];
    let short_list = [30usize, 100, 150, 200, 300, 400];
    let mut scratch = graph.scratch();
    println!("ef shortlist recall100 recall10 evals nav_us rerank_us total_us");
    for &ef in &ef_list {
        for &sl in &short_list {
            if sl > ef && sl != 30 {
                continue;
            }
            let k = if sl == 30 { 10 } else { 100 };
            let (mut best_nav, mut best_rr) = (f64::MAX, f64::MAX);
            let (mut found, mut found10, mut evals) = (0usize, 0usize, 0usize);
            for pass in 0..3 {
                let (mut nav, mut rr) = (0f64, 0f64);
                let (mut f, mut f10, mut e) = (0, 0, 0);
                for (i, qv) in queries.iter().enumerate() {
                    let t = Instant::now();
                    let (ids, work) = graph.candidates(qv, ef, sl, None, &mut scratch).unwrap();
                    nav += t.elapsed().as_secs_f64();
                    let t = Instant::now();
                    let q = data::normalized(qv);
                    let mut hits = ids
                        .into_iter()
                        .map(|id| (id, exact.rerank(&q, id)))
                        .collect::<Vec<_>>();
                    hits.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                    hits.truncate(k);
                    rr += t.elapsed().as_secs_f64();
                    if pass == 0 {
                        f += hits.iter().filter(|h| truth[i].contains(&h.0)).count();
                        f10 += hits
                            .iter()
                            .take(10)
                            .filter(|h| truth[i].contains(&h.0))
                            .count();
                        e += work;
                    }
                }
                best_nav = best_nav.min(nav);
                best_rr = best_rr.min(rr);
                if pass == 0 {
                    (found, found10, evals) = (f, f10, e);
                }
            }
            let n = queries.len() as f64;
            println!(
                "{ef} {sl} {:.5} {:.5} {:.1} {:.1} {:.1} {:.1}",
                found as f64 / (n * k as f64).min(n * 100.),
                found10 as f64 / (n * 10.),
                evals as f64 / n,
                best_nav / n * 1e6,
                best_rr / n * 1e6,
                (best_nav + best_rr) / n * 1e6
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
    #[cfg(target_os = "macos")]
    let peak = out.ru_maxrss as u64 / 1024;
    #[cfg(not(target_os = "macos"))]
    let peak = out.ru_maxrss as u64;
    (cpu, peak)
}
