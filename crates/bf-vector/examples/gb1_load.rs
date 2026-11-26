//! Round GB1: foreground IVF throughput and tail latency while the warm graph builds on an
//! idle-priority pool. Usage: gb1_load NQ_DATA EXPORTED_IVF BUILD_THREADS [clients=16] [think=0] [probes=48]
//! BUILD_THREADS=0 measures the foreground alone. `think` sleeps that multiple of each query's
//! service time, a stand-in for HTTP overhead that leaves idle cores for the build.
#[allow(dead_code)]
#[path = "g3/data.rs"]
mod data;
#[allow(dead_code)]
#[path = "../src/fastscan.rs"]
mod fastscan;
#[allow(dead_code)]
#[path = "g3/graph.rs"]
mod graph;
#[allow(dead_code)]
#[path = "g3/ivf.rs"]
mod ivf;
#[allow(dead_code)]
#[path = "g3/measure.rs"]
mod measure;
// Include the production transform without adding a library API for an experiment.
#[allow(dead_code)]
#[path = "g3/quant.rs"]
mod quant;
#[allow(dead_code)]
#[path = "../src/rabitq.rs"]
mod rabitq;
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
use bf_vector::{Bytes, Index, RangeRead, warm_graph::HotGraphBuilder};
use data::{Matrix, Vectors};
use std::{
    fs::File,
    io,
    ops::Range,
    os::unix::fs::FileExt,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
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
fn idle_priority() {
    // SAFETY: both calls affect only the calling worker, as in the engine's build pool.
    unsafe {
        libc::setpriority(libc::PRIO_PROCESS, 0, 19);
        let p = libc::sched_param { sched_priority: 0 };
        libc::sched_setscheduler(0, libc::SCHED_IDLE, &p);
    }
}
fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}
fn report(label: &str, lat: &mut [f64], seconds: f64) {
    lat.sort_by(|a, b| a.total_cmp(b));
    println!(
        "{label} queries={} seconds={seconds:.1} qps={:.1} p50_us={:.0} p99_us={:.0}",
        lat.len(),
        lat.len() as f64 / seconds,
        percentile(lat, 0.5),
        percentile(lat, 0.99)
    );
}
fn main() -> data::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 4 {
        return Err(
            "usage: gb1_load NQ_DATA EXPORTED_IVF BUILD_THREADS [clients] [think] [probes]".into(),
        );
    }
    let (nq, ivf_dir) = (Path::new(&args[1]), Path::new(&args[2]));
    let threads: usize = args[3].parse()?;
    let clients: usize = args.get(4).map(|s| s.parse()).transpose()?.unwrap_or(16);
    let think: f64 = args.get(5).map(|s| s.parse()).transpose()?.unwrap_or(0.);
    let probes: usize = args.get(6).map(|s| s.parse()).transpose()?.unwrap_or(48);
    let v = Arc::new(Vectors::from_matrix(Matrix::load(
        &nq.join("corpus.f16"),
        "<f2",
        2,
        usize::MAX,
    )?));
    let index = Arc::new(ivf::Ivf::load(ivf_dir)?);
    let queries = Arc::new(Matrix::load(&nq.join("queries.npy"), "<f4", 4, usize::MAX)?.floats());
    let stop = Arc::new(AtomicBool::new(false));
    let origin = Instant::now();
    let handles = (0..clients)
        .map(|c| {
            let (v, index, queries, stop) =
                (v.clone(), index.clone(), queries.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut log = Vec::<(f64, f64)>::new();
                let mut i = c * 97;
                while !stop.load(Ordering::Relaxed) {
                    let q = data::normalized(&queries[i % queries.len()]);
                    i += 1;
                    let t = Instant::now();
                    std::hint::black_box(index.search(&v, &q, probes));
                    let service = t.elapsed();
                    log.push((
                        t.duration_since(origin).as_secs_f64(),
                        service.as_secs_f64() * 1e6,
                    ));
                    if think > 0. {
                        std::thread::sleep(service.mul_f64(think));
                    }
                }
                log
            })
        })
        .collect::<Vec<_>>();
    std::thread::sleep(Duration::from_secs(20));
    let alone_end = origin.elapsed().as_secs_f64();
    let (mut build_start, mut build_end) = (alone_end, alone_end);
    if threads > 0 {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .start_handler(|_| idle_priority())
            .build()?;
        let mut files = std::fs::read_dir(ivf_dir)?
            .map(|e| e.map(|e| e.path()))
            .collect::<io::Result<Vec<_>>>()?;
        files.retain(|p| p.extension().is_some_and(|s| s == "bfa"));
        files.sort();
        let map = std::fs::read(ivf_dir.join("rowmap.u32"))?
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| u32::from_le_bytes(*b))
            .collect::<Vec<_>>();
        build_start = origin.elapsed().as_secs_f64();
        let graph = pool.install(|| -> bf_vector::Result<_> {
            let mut builder = HotGraphBuilder::new(v.dims, v.len())?;
            for file in &files {
                let index = open(file)?;
                for c in 0..index.cluster_count() {
                    let (ids, bytes) = futures::executor::block_on(index.read_cluster_rows(c))?;
                    let ids = ids.iter().map(|r| map[*r as usize]).collect::<Vec<_>>();
                    builder.append(&ids, &bytes, &|| false)?;
                }
            }
            builder.finish(&|| false)
        })?;
        build_end = origin.elapsed().as_secs_f64();
        println!(
            "build threads={threads} wall_seconds={:.1} rows={}",
            build_end - build_start,
            graph.len()
        );
    }
    std::thread::sleep(Duration::from_secs(5));
    stop.store(true, Ordering::Relaxed);
    let log = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    let window = |a: f64, b: f64| {
        log.iter()
            .filter(|(t, _)| *t >= a && *t < b)
            .map(|(_, l)| *l)
            .collect::<Vec<_>>()
    };
    report("alone ", &mut window(2., alone_end), alone_end - 2.);
    if threads > 0 {
        report(
            "during",
            &mut window(build_start, build_end),
            build_end - build_start,
        );
    }
    Ok(())
}
