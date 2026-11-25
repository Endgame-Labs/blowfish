//! Disposable, in-process NQ graph kill tests. This example changes no production API or format.
//! See `examples/g3/README.md` for commands, measurement boundaries, and cleanup.
#[path = "g3/data.rs"]
mod data;
#[allow(dead_code)]
#[path = "../src/fastscan.rs"]
mod fastscan;
#[path = "g3/graph.rs"]
mod graph;
#[path = "g3/ivf.rs"]
mod ivf;
#[path = "g3/measure.rs"]
mod measure;
// Include the production transform without adding a library API for an experiment.
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
use data::{Matrix, Result as AnyResult, Vectors};
use graph::{Graph, Search};
use half::f16;
use std::{path::Path, sync::Arc, time::Instant};
fn main() -> AnyResult<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        return Err(
            "usage: hot_graph build|search DATA_DIR GRAPH [row_limit] [ef=64] [repeats=1]".into(),
        );
    }
    let dir = Path::new(&args[2]);
    let limit = args
        .get(4)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(usize::MAX);
    let t = Instant::now();
    let v = Arc::new(Vectors::from_matrix(Matrix::load(
        &dir.join("corpus.f16"),
        "<f2",
        2,
        limit,
    )?));
    eprintln!(
        "load rows={} dims={} seconds={:.3} rss_kib={}",
        v.len(),
        v.dims,
        t.elapsed().as_secs_f64(),
        data::rss_kib()
    );
    if args[1] == "build" {
        let g = Graph::build(v, 16, 100);
        g.write(Path::new(&args[3]))?;
    } else if args[1] == "encode8" {
        let t = Instant::now();
        let c = quant::Codes::build(&v);
        c.write(Path::new(&args[3]))?;
        eprintln!(
            "encode8 seconds={:.3} rss_kib={}",
            t.elapsed().as_secs_f64(),
            data::rss_kib()
        );
    } else if args[1] == "ivf" {
        let index = ivf::Ivf::load(Path::new(&args[3]))?;
        let queries = Matrix::load(&dir.join("queries.npy"), "<f4", 4, usize::MAX)?.floats();
        let truth = Matrix::load(&dir.join("truth.npy"), "<i4", 4, usize::MAX)?.truth();
        let probes = args.get(5).map(|s| s.parse()).transpose()?.unwrap_or(48);
        let repeats = args.get(6).map(|s| s.parse()).transpose()?.unwrap_or(1);
        let region = measure::Region::begin();
        let results = measure::parallel(
            queries.len() * repeats,
            || (),
            |_, i| {
                let q = data::normalized(&queries[i % queries.len()]);
                let (hits, rows, rerank) = index.search(&v, &q, probes);
                let got = hits
                    .iter()
                    .filter(|h| truth[i % truth.len()].contains(&h.id))
                    .count();
                (got, rows, rerank)
            },
        );
        let seconds = region.end();
        let n = results.len() as f64;
        println!(
            "variant=ivf probes={probes} queries={} recall={:.6} rows={:.1} rerank={:.1} seconds={seconds:.4} qps={:.2} threads={} rss_kib={}",
            results.len(),
            results.iter().map(|x| x.0).sum::<usize>() as f64 / (10. * n),
            results.iter().map(|x| x.1).sum::<usize>() as f64 / n,
            results.iter().map(|x| x.2).sum::<usize>() as f64 / n,
            n / seconds,
            rayon::current_num_threads(),
            data::rss_kib()
        );
    } else if matches!(args[1].as_str(), "search" | "search8") {
        let g = Graph::read(Path::new(&args[3]))?;
        if g.levels.len() != v.len() {
            return Err("graph/corpus mismatch".into());
        }
        let queries = Matrix::load(&dir.join("queries.npy"), "<f4", 4, usize::MAX)?.floats();
        let truth = Matrix::load(&dir.join("truth.npy"), "<i4", 4, usize::MAX)?.truth();
        let ef = args.get(5).map(|s| s.parse()).transpose()?.unwrap_or(64);
        let repeats = args.get(6).map(|s| s.parse()).transpose()?.unwrap_or(1);
        let codes = if args[1] != "search" {
            let c = quant::Codes::read(Path::new(&format!("{}.codes8", args[3])))?;
            // G7 kill test: repack rows at a different byte stride, same graph and kernel.
            match std::env::var("G7_STRIDE") {
                Ok(s) => Some(c.repack(s.parse()?)),
                Err(_) => Some(c),
            }
        } else {
            None
        };
        let rerank = args
            .get(7)
            .map(|s| s.parse())
            .transpose()?
            .unwrap_or(50usize);
        let region = measure::Region::begin();
        let results = measure::parallel(
            queries.len() * repeats,
            || Search::new(v.len()),
            |s, i| {
                let q = data::normalized(&queries[i % queries.len()]);
                let (mut hits, count) = if let Some(c) = &codes {
                    let (q8, scale) = c.prepare(&q);
                    let (mut h, n) = g.search(ef, s, |id| c.score(&q8, scale, id));
                    h.truncate(rerank);
                    for x in &mut h {
                        x.distance = v.rerank(&q, x.id);
                    }
                    h.sort_unstable();
                    (h, n)
                } else {
                    let half: Vec<_> = q.iter().map(|x| f16::from_f32(*x)).collect();
                    g.search(ef, s, |id| v.query(&half, id))
                };
                hits.truncate(10);
                let got = hits
                    .iter()
                    .take(10)
                    .filter(|h| truth[i % truth.len()].contains(&h.id))
                    .count();
                // Order-sensitive digest of the final ids, to prove results do not change.
                let digest = hits.iter().fold(i as u64, |h, x| {
                    (h ^ u64::from(x.id)).wrapping_mul(0x100_0000_01b3)
                });
                (got, count, digest)
            },
        );
        let elapsed = region.end();
        let rerank = if codes.is_some() { rerank } else { 0 };
        let variant = if codes.is_some() { "i8" } else { "f16" };
        println!(
            "variant={variant} rerank={rerank} ef={ef} queries={} recall={:.6} distances={:.1} seconds={elapsed:.4} qps={:.2} threads={} rss_kib={}",
            results.len(),
            results.iter().map(|x| x.0).sum::<usize>() as f64 / (results.len() * 10) as f64,
            results.iter().map(|x| x.1).sum::<usize>() as f64 / results.len() as f64,
            results.len() as f64 / elapsed,
            rayon::current_num_threads(),
            data::rss_kib()
        );
        println!(
            "digest={:016x} stride={}",
            results.iter().fold(0u64, |a, x| a ^ x.2),
            codes.as_ref().map_or(0, |c| c.stride)
        );
    } else {
        return Err("unknown command".into());
    }
    Ok(())
}
