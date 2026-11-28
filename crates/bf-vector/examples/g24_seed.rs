//! Round G24: multi-entry seeding, offline. Builds the production HotGraph over the NQ corpus and
//! sweeps walk options at several beam widths, reporting recall of the rerank shortlist against the
//! exact top 10, SQ8 scores, expansions, and single-thread walk time per query.
//! Usage: g24_seed DATA_DIR [ROWS] [THREADS]   (DATA_DIR holds corpus.f16, queries.npy, truth.npy)
//! Walk configs come from the SEEDS env var: `seeds:-:-:ef:count,...` (seeds 0 = the old walk; the
//! two middle fields are unused).
#[allow(dead_code)]
#[path = "g3/data.rs"]
mod data;
use bf_vector::warm_graph::{HotGraphBuilder, WalkOptions};
use std::{path::Path, time::Instant};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let dir = Path::new(&args[1]);
    let dims = 1024;
    let corpus = std::fs::File::open(dir.join("corpus.f16"))?;
    // The corpus is an `.npy` (`<f2`, C order): skip its header.
    let offset = {
        use std::io::Read;
        let mut head = [0u8; 10];
        (&corpus).read_exact(&mut head)?;
        assert_eq!(&head[..6], b"\x93NUMPY");
        10 + u16::from_le_bytes([head[8], head[9]]) as usize
    };
    let total = (corpus.metadata()?.len() as usize - offset) / (dims * 2);
    let n: usize = args.get(2).map(|s| s.parse()).transpose()?.unwrap_or(total);
    let threads: usize = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(8);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()?;
    let queries = data::Matrix::load(&dir.join("queries.npy"), "<f4", 4, usize::MAX)?;
    let truth = data::Matrix::load(&dir.join("truth.npy"), "<i4", 4, usize::MAX)?;
    let q = |i: usize| -> Vec<f32> {
        queries.bytes[i * dims * 4..(i + 1) * dims * 4]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect()
    };
    let t = |i: usize| -> Vec<u32> {
        truth.bytes[i * truth.cols * 4..i * truth.cols * 4 + 40]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| i32::from_le_bytes(*b) as u32)
            .collect()
    };
    let started = Instant::now();
    let graph = pool.install(|| -> bf_vector::Result<_> {
        let mut b = HotGraphBuilder::new(dims, n)?;
        let mut buf = vec![0u8; 8192 * dims * 2];
        let mut done = 0;
        use std::os::unix::fs::FileExt;
        while done < n {
            let take = (n - done).min(8192);
            let bytes = &mut buf[..take * dims * 2];
            corpus.read_exact_at(bytes, (offset + done * dims * 2) as u64)?;
            let rows = (done as u32..(done + take) as u32).collect::<Vec<_>>();
            b.append(&rows, bytes, &|| false)?;
            done += take;
        }
        b.finish(&|| false)
    })?;
    eprintln!("built {n} rows in {:.0}s", started.elapsed().as_secs_f64());
    // Truth for a subset build is only meaningful for the full corpus; with ROWS < total the
    // truth ids beyond ROWS are simply missed, so use the full corpus for recall.
    let nq = queries.rows;
    let spec = std::env::var("SEEDS").unwrap_or("0:2:1.5:125:30,4:2:1.5:125:30".into());
    for cfg in spec.split(',') {
        let f = cfg.split(':').collect::<Vec<_>>();
        let (seeds, _wide, _tight, ef, count): (usize, f32, f32, usize, usize) = (
            f[0].parse()?,
            f[1].parse()?,
            f[2].parse()?,
            f[3].parse()?,
            f[4].parse()?,
        );
        let o = Some(WalkOptions { seeds });
        // Quality (parallel).
        use rayon::prelude::*;
        let (found, evals, exps) = pool.install(|| {
            (0..nq)
                .into_par_iter()
                .map_init(
                    || graph.scratch(),
                    |scratch, i| {
                        let (hits, e) = graph
                            .candidates_with_options(&q(i), ef, count, None, scratch, o)
                            .unwrap();
                        let truth = t(i);
                        let found = hits.iter().filter(|h| truth[..10].contains(&h.0)).count();
                        (found, e, scratch.expansions())
                    },
                )
                .reduce(|| (0, 0, 0), |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2))
        });
        // Walk time, one thread, after a warm pass.
        let mut scratch = graph.scratch();
        let queries_f = (0..nq).map(q).collect::<Vec<_>>();
        for v in queries_f.iter().take(500) {
            graph
                .candidates_with_options(v, ef, count, None, &mut scratch, o)
                .unwrap();
        }
        let started = Instant::now();
        for v in &queries_f {
            graph
                .candidates_with_options(v, ef, count, None, &mut scratch, o)
                .unwrap();
        }
        let us = started.elapsed().as_secs_f64() * 1e6 / nq as f64;
        println!(
            "cfg {cfg}: recall@10 of shortlist {:.4}, scores {:.0}, expansions {:.1}, walk {:.0} us",
            found as f64 / (nq * 10) as f64,
            evals as f64 / nq as f64,
            exps as f64 / nq as f64,
            us
        );
    }
    Ok(())
}
