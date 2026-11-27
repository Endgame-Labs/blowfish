//! Round G16: graph navigation with the one-bit screen. Builds the warm graph from a corpus and
//! measures recall@k (nav, then exact rerank of the shortlist) and in-process throughput.
//! Usage: g16_nav DATA_DIR  (corpus.f16, queries.npy, truth.npy)
//! Env: ROWS (default all), THREADS (8), SECONDS (4), ROUNDS (2),
//! CONFIGS="ef:k:count,..." (for example 80:10:30,256:100:150).
#[allow(dead_code)]
#[path = "g3/data.rs"]
mod data;
use bf_vector::warm_graph::{self, HotGraphBuilder};
use std::{path::Path, time::Instant};

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args().nth(1).ok_or("usage: g16_nav DATA_DIR")?;
    let dir = Path::new(&dir);
    let matrix = data::Matrix::load(&dir.join("corpus.f16"), "<f2", 2, env("ROWS", usize::MAX))?;
    let queries = data::Matrix::load(&dir.join("queries.npy"), "<f4", 4, usize::MAX)?.floats();
    let truth: Vec<Vec<u32>> = {
        let m = data::Matrix::load(&dir.join("truth.npy"), "<i4", 4, usize::MAX)?;
        m.bytes
            .chunks_exact(m.cols * 4)
            .map(|r| {
                r.as_chunks::<4>()
                    .0
                    .iter()
                    .map(|b| u32::from_le_bytes(*b))
                    .collect()
            })
            .collect()
    };
    let (rows, dims) = (matrix.rows, matrix.cols);
    let threads: usize = env("THREADS", 8);
    let seconds: f64 = env("SECONDS", 4.);
    let rounds: usize = env("ROUNDS", 2);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()?;
    let t = Instant::now();
    let mut builder = HotGraphBuilder::new(dims, rows)?;
    for first in (0..rows).step_by(50_000) {
        let end = (first + 50_000).min(rows);
        let ids: Vec<u32> = (first as u32..end as u32).collect();
        builder.append(
            &ids,
            &matrix.bytes[first * dims * 2..end * dims * 2],
            &|| false,
        )?;
    }
    let graph = pool.install(|| builder.finish(&|| false))?;
    eprintln!("built {rows} rows in {:.1}s", t.elapsed().as_secs_f64());
    let exact = data::Vectors::from_matrix(matrix);
    let nq = queries.len();
    let normalized: Vec<Vec<f32>> = queries.iter().map(|q| data::normalized(q)).collect();
    let one =
        |scratch: &mut warm_graph::HotScratch, i: usize, ef: usize, k: usize, count: usize| {
            let (cands, _) = graph
                .candidates_with_norms(&queries[i], ef, count, None, scratch)
                .unwrap();
            let mut hits: Vec<(f32, u32)> = cands
                .into_iter()
                .map(|(r, _)| (exact.rerank(&normalized[i], r), r))
                .collect();
            hits.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            hits.truncate(k);
            hits
        };
    let parse = |s: &str| -> Vec<Vec<String>> {
        s.split(',')
            .filter(|x| !x.is_empty())
            .map(|c| c.split(':').map(String::from).collect())
            .collect()
    };
    // Sweep: recall, then paired throughput rounds.
    let configs = parse(&std::env::var("CONFIGS").unwrap_or_default());
    let mut qps: Vec<Vec<f64>> = vec![vec![]; configs.len()];
    let mut recall = vec![0f64; configs.len()];
    let mut evals = vec![0f64; configs.len()];
    for (ci, c) in configs.iter().enumerate() {
        let (ef, k, count): (usize, usize, usize) = (c[0].parse()?, c[1].parse()?, c[2].parse()?);
        let mut scratch = graph.scratch();
        let mut found = 0;
        for (i, truth) in truth.iter().enumerate() {
            let hits = one(&mut scratch, i, ef, k, count);
            found += hits
                .iter()
                .filter(|h| truth.iter().take(k).any(|t| *t == h.1))
                .count();
        }
        recall[ci] = found as f64 / (nq * k) as f64;
        let mut total = 0;
        for query in &queries {
            total += graph
                .candidates_with_norms(query, ef, count, None, &mut scratch)
                .unwrap()
                .1;
        }
        evals[ci] = total as f64 / nq as f64;
    }
    for round in 0..rounds {
        let order: Vec<usize> = if round % 2 == 0 {
            (0..configs.len()).collect()
        } else {
            (0..configs.len()).rev().collect()
        };
        for ci in order {
            let c = &configs[ci];
            let (ef, k, count): (usize, usize, usize) =
                (c[0].parse()?, c[1].parse()?, c[2].parse()?);
            let t0 = Instant::now();
            let total: usize = std::thread::scope(|s| {
                (0..threads)
                    .map(|th| {
                        let one = &one;
                        let graph = &graph;
                        s.spawn(move || {
                            let mut scratch = graph.scratch();
                            let mut n = 0;
                            while t0.elapsed().as_secs_f64() < seconds {
                                one(&mut scratch, (th * 131 + n) % nq, ef, k, count);
                                n += 1;
                            }
                            n
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .sum()
            });
            qps[ci].push(total as f64 / t0.elapsed().as_secs_f64());
        }
    }
    for (ci, c) in configs.iter().enumerate() {
        let mut v = qps[ci].clone();
        v.sort_by(|a, b| a.total_cmp(b));
        println!(
            "margin={} ef={} k={} count={}: recall@{} {:.5} evals/q {:.0} q/s median {:.0} (all {:?})",
            c[0],
            c[1],
            c[2],
            c[3],
            c[2],
            recall[ci],
            evals[ci],
            v.get(v.len() / 2).copied().unwrap_or(0.),
            qps[ci].iter().map(|x| x.round()).collect::<Vec<_>>()
        );
    }
    Ok(())
}
