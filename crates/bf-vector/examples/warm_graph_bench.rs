//! Build the production bounded graph from exported spatial slabs, then measure NQ recall.
//! Usage: warm_graph_bench EXPORTED_IVF NQ_DATA SCRATCH_DIRECTORY [build_workers=2]
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
    if args.len() < 4 {
        return Err(
            "usage: warm_graph_bench EXPORTED_IVF NQ_DATA SCRATCH_DIRECTORY [build_workers=2]"
                .into(),
        );
    }
    let workers = args.get(4).map(|s| s.parse()).transpose()?.unwrap_or(2);
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
    let mut builder = HotGraphBuilder::new(Path::new(&args[3]), dims, count)?;
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
    let truth = data::Matrix::load(&data.join("truth.npy"), "<i4", 4, usize::MAX)?.truth();
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
    for ef in [32, 48, 64, 96, 128, 160, 192, 256, 384, 512] {
        let region = measure::Region::begin();
        let results = measure::parallel(
            queries.len(),
            || graph.scratch(),
            |scratch, i| {
                let (ids, work) = graph
                    .candidates(&queries[i], ef, 30, None, scratch)
                    .unwrap();
                let q = data::normalized(&queries[i]);
                let mut hits = ids
                    .into_iter()
                    .map(|id| (id, exact.rerank(&q, id)))
                    .collect::<Vec<_>>();
                hits.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                (
                    hits.iter()
                        .take(10)
                        .filter(|h| truth[i].contains(&h.0))
                        .count(),
                    work,
                )
            },
        );
        let elapsed = region.end();
        let found: usize = results.iter().map(|r| r.0).sum();
        let evaluations: usize = results.iter().map(|r| r.1).sum();
        println!(
            "ef={ef} recall={:.6} scores={:.1} qps={:.1} seconds={elapsed:.3} threads={}",
            found as f64 / (queries.len() * 10) as f64,
            evaluations as f64 / queries.len() as f64,
            queries.len() as f64 / elapsed,
            rayon::current_num_threads()
        );
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
