//! Build the production bounded graph from exported spatial slabs, then measure NQ recall.
//! Usage: warm_graph_bench EXPORTED_IVF NQ_DATA SCRATCH_DIRECTORY [build_workers=2]
#[allow(dead_code)]
#[path = "g3/data.rs"]
mod data;
use bf_vector::{Bytes, Index, RangeRead, warm_graph::HotGraphBuilder};
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
    let mut builder = HotGraphBuilder::new(Path::new(&args[3]), dims, count)?;
    for (partition, file) in files.iter().enumerate() {
        let index = open(file)?;
        for c in 0..index.cluster_count() {
            let (ids, bytes) = futures::executor::block_on(index.read_cluster_rows(c))?;
            let ids = ids.iter().map(|r| map[*r as usize]).collect::<Vec<_>>();
            builder.append(partition, &ids, &bytes)?;
        }
    }
    let graph = pool.install(|| builder.finish(&|| false))?;
    println!(
        "built rows={} workers={} seconds={:.3} resident_bytes={} rss_kib={}",
        graph.len(),
        workers,
        start.elapsed().as_secs_f64(),
        graph.resident_bytes(),
        data::rss_kib()
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
    let mut scratch = graph.scratch();
    for ef in [32, 48, 64, 96, 128, 160, 192, 256, 384, 512] {
        let start = Instant::now();
        let mut found = 0;
        let mut evaluations = 0;
        for (q, t) in queries.iter().zip(&truth) {
            let (ids, work) = graph.candidates(q, ef, 30, None, &mut scratch)?;
            evaluations += work;
            let mut hits = Vec::new();
            for id in ids {
                let mut b = vec![0; dims * 2];
                corpus.read_exact_at(&mut b, offset + id as u64 * dims as u64 * 2)?;
                let v = b
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|b| half::f16::from_le_bytes(*b).to_f32())
                    .collect::<Vec<_>>();
                hits.push((id, bf_vector::Metric::CosineDistance.distance(q, &v)?));
            }
            hits.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            found += hits.iter().take(10).filter(|h| t.contains(&h.0)).count();
        }
        println!(
            "ef={ef} recall={:.6} scores={:.1} qps={:.1} seconds={:.3}",
            found as f64 / (queries.len() * 10) as f64,
            evaluations as f64 / queries.len() as f64,
            queries.len() as f64 / start.elapsed().as_secs_f64(),
            start.elapsed().as_secs_f64()
        );
    }
    Ok(())
}
