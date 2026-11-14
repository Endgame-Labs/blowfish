//! Reproducible warm-memory IVF benchmark. Run with `--help` for parameters.
use std::{
    error::Error,
    fs::File,
    io::{BufReader, Read},
    path::Path,
    time::Instant,
};

use bf_vector::{
    BuildParams, Bytes, Index, IndexBuilder, Metric, SearchParams, VectorType, exact_search,
    metadata_len, recall_at_k,
};
use rayon::prelude::*;

type Vectors = Vec<(u32, Vec<f32>)>;

struct Rng(u64);
impl Rng {
    fn value(&mut self) -> f32 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        ((z ^ (z >> 31)) >> 40) as f32 / (1u32 << 24) as f32 * 2. - 1.
    }
}

fn read_vectors(path: &Path, rows: usize, dims: usize) -> Result<Vectors, Box<dyn Error>> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut result = Vec::with_capacity(rows);
    let mut bytes = vec![0u8; dims.checked_mul(4).ok_or("dimension overflow")?];
    for row in 0..rows {
        reader.read_exact(&mut bytes)?;
        result.push((
            row as u32,
            bytes
                .as_chunks::<4>()
                .0
                .iter()
                .map(|x| f32::from_le_bytes(*x))
                .collect(),
        ));
    }
    Ok(result)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() as f64 * p).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1)]
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut rows = 1_000_000usize;
    let mut dims = 768usize;
    let mut queries = 100usize;
    let mut seed = 42u64;
    let mut nprobes = vec![16, 128, 256];
    let mut rerank_factor = SearchParams::default().rerank_factor;
    let mut dtype = VectorType::F32;
    let mut data_path = None;
    let mut query_path = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!(
                "vector-bench [--rows 1000000] [--dims 768] [--queries 100] [--seed 42] [--nprobe 16,128,256] [--rerank-factor 64] [--f16] [--vectors RAW_F32_LE] [--query-vectors RAW_F32_LE]\nSynthetic data has 256 separated clusters. File inputs contain row-major f32-LE with no header. Run in release mode. Set RAYON_NUM_THREADS to control build parallelism. Latency uses a Bytes-backed reader with no network, and no concurrent queries."
            );
            return Ok(());
        }
        if arg == "--f16" {
            dtype = VectorType::F16;
            continue;
        }
        let value = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--rows" => rows = value.parse()?,
            "--dims" => dims = value.parse()?,
            "--queries" => queries = value.parse()?,
            "--seed" => seed = value.parse()?,
            "--nprobe" => nprobes = value.split(',').map(str::parse).collect::<Result<_, _>>()?,
            "--rerank-factor" => rerank_factor = value.parse()?,
            "--vectors" => data_path = Some(value),
            "--query-vectors" => query_path = Some(value),
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    if rows == 0
        || rows > u32::MAX as usize
        || dims == 0
        || queries == 0
        || rerank_factor == 0
        || nprobes.contains(&0)
    {
        return Err("rows, dimensions, queries, probes, and rerank factor must be positive".into());
    }
    let metric = Metric::CosineDistance;
    let mut rng = Rng(seed);
    let centers: Vec<Vec<f32>> = (0..256)
        .map(|_| (0..dims).map(|_| rng.value()).collect())
        .collect();
    eprintln!("loading or generating {rows} x {dims} vectors");
    let mut data = if let Some(path) = &data_path {
        read_vectors(Path::new(path), rows, dims)?
    } else {
        (0..rows)
            .into_par_iter()
            .map(|row| {
                let mut rng = Rng(seed.wrapping_add(row as u64).wrapping_mul(104729));
                (
                    row as u32,
                    centers[row % 256]
                        .iter()
                        .map(|c| *c + 0.35 * rng.value())
                        .collect(),
                )
            })
            .collect()
    };
    if dtype == VectorType::F16 {
        data.par_iter_mut().for_each(|(_, v)| {
            for x in v {
                *x = bf_vector::f16::from_f32(*x).to_f32();
            }
        });
    }
    let query_vectors: Vec<Vec<f32>> = if let Some(path) = query_path {
        read_vectors(Path::new(&path), queries, dims)?
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    } else {
        (0..queries)
            .map(|i| {
                let mut rng = Rng(seed.wrapping_add(i as u64).wrapping_mul(15485863));
                data[(i * 104729) % rows]
                    .1
                    .iter()
                    .map(|x| *x + 0.05 * rng.value())
                    .collect()
            })
            .collect()
    };
    eprintln!("building index");
    let start = Instant::now();
    let blob = IndexBuilder::new(dims, metric)?
        .vector_type(dtype)
        .params(BuildParams {
            seed,
            ..Default::default()
        })
        .build(&data)?;
    let build_seconds = start.elapsed().as_secs_f64();
    let index_bytes = blob.len();
    let meta_len = metadata_len(&blob)?;
    let index =
        futures::executor::block_on(Index::open_with_metadata_len(Bytes::from(blob), meta_len))?;
    eprintln!("built in {build_seconds:.3}s; computing {queries} exact ground-truth queries");
    let ground_truth: Vec<_> = query_vectors
        .iter()
        .map(|q| exact_search(&data, q, 10, metric, None))
        .collect::<Result<_, _>>()?;
    drop(data);
    println!(
        "rows,dims,dtype,clusters,parents,queries,build_seconds,index_bytes,metadata_bytes,nprobe,rerank_factor,recall_at_10,p50_ms,p99_ms"
    );
    for nprobe in nprobes {
        let params = SearchParams {
            nprobe,
            rerank_factor,
        };
        for q in query_vectors.iter().take(3) {
            futures::executor::block_on(index.search(q, 10, params, None))?;
        }
        let mut latencies = Vec::with_capacity(queries);
        let mut recall = 0.;
        for (q, truth) in query_vectors.iter().zip(&ground_truth) {
            let start = Instant::now();
            let ann = futures::executor::block_on(index.search(q, 10, params, None))?;
            latencies.push(start.elapsed().as_secs_f64() * 1000.);
            recall += recall_at_k(&ann, truth);
        }
        latencies.sort_unstable_by(f64::total_cmp);
        println!(
            "{rows},{dims},{dtype:?},{},{},{queries},{build_seconds:.3},{index_bytes},{meta_len},{nprobe},{rerank_factor},{:.5},{:.3},{:.3}",
            index.cluster_count(),
            index.parent_count(),
            recall / queries as f32,
            percentile(&latencies, 0.5),
            percentile(&latencies, 0.99)
        );
    }
    Ok(())
}
