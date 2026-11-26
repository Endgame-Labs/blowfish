//! Round G15: the one-bit screen (`HotGraph::scan`) vs the SQ8-only scan on the first ROWS rows of NQ, for hash masks.
//! Usage: g15_screen CORPUS_F16_NPY QUERIES_F32_NPY ROWS [threads=8] [seconds=2]
//! Recall@10 is measured against exhaustive exact cosine over the allowed rows.
use bf_vector::{RoaringBitmap, warm_graph::HotGraphBuilder};
use std::{io::Read, time::Instant};
fn mix(x: u64) -> u64 {
    let mut x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}
const DIMS: usize = 1024;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let rows: usize = args[3].parse()?;
    let threads: usize = args.get(4).map_or(Ok(8), |s| s.parse())?;
    let seconds: f64 = args.get(5).map_or(Ok(2.0), |s| s.parse())?;
    let mut file = std::fs::File::open(&args[1])?;
    let mut header = [0u8; 128];
    file.read_exact(&mut header)?;
    let mut bytes = vec![0u8; rows * DIMS * 2];
    file.read_exact(&mut bytes)?;
    let mut q = std::fs::File::open(&args[2])?;
    q.read_exact(&mut header)?;
    let nq: usize = std::env::var("QUERIES").map_or(64, |s| s.parse().unwrap());
    let mut qb = vec![0u8; nq * DIMS * 4];
    q.read_exact(&mut qb)?;
    let queries: Vec<Vec<f32>> = qb
        .as_chunks::<{ DIMS * 4 }>()
        .0
        .iter()
        .map(|r| {
            r.as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect()
        })
        .collect();
    let corpus: Vec<f32> = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| half::f16::from_le_bytes(*b).to_f32())
        .collect();
    let norms: Vec<f32> = corpus
        .as_chunks::<DIMS>()
        .0
        .iter()
        .map(|r| r.iter().map(|x| x * x).sum::<f32>().sqrt())
        .collect();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()?;
    let t = Instant::now();
    let mut builder = HotGraphBuilder::new(DIMS, rows)?;
    for first in (0..rows).step_by(50_000) {
        let end = (first + 50_000).min(rows);
        let ids: Vec<u32> = (first as u32..end as u32).collect();
        builder.append(&ids, &bytes[first * DIMS * 2..end * DIMS * 2], &|| false)?;
    }
    let graph = pool.install(|| builder.finish(&|| false))?;
    eprintln!(
        "built {rows} rows in {:.1}s, resident {} MB",
        t.elapsed().as_secs_f64(),
        graph.resident_bytes() >> 20
    );
    let exact = |q: &[f32], row: u32| -> f32 {
        let r = &corpus[row as usize * DIMS..(row as usize + 1) * DIMS];
        r.iter().zip(q).map(|(a, b)| a * b).sum::<f32>() / norms[row as usize]
    };
    // Variants: (name, keep); keep 0 means the SQ8-only scan (no screen) with count 30.
    let variants: Vec<(String, usize)> = std::env::var("VARIANTS")
        .unwrap_or("sq8:0,k500:500,k2000:2000,k5000:5000".into())
        .split(',')
        .map(|v| {
            let p: Vec<&str> = v.split(':').collect();
            (p[0].to_string(), p[1].parse().unwrap())
        })
        .collect();
    let run =
        |g: &bf_vector::warm_graph::HotGraph, v: &(String, usize), q: &[f32], m: &RoaringBitmap| {
            g.scan(q, 30, if v.1 == 0 { usize::MAX } else { v.1 }, m)
                .unwrap()
                .0
        };
    let per_mille: Vec<u64> = std::env::var("MILLES")
        .unwrap_or("5,10,20,50,100,200".into())
        .split(',')
        .map(|x| x.parse().unwrap())
        .collect();
    for pm in per_mille {
        let mask: RoaringBitmap = (0..rows as u32)
            .filter(|r| mix(*r as u64) % 1000 < pm)
            .collect();
        let truth: Vec<Vec<u32>> = queries
            .iter()
            .map(|q| {
                let mut s: Vec<(f32, u32)> = mask.iter().map(|r| (-exact(q, r), r)).collect();
                s.sort_by(|a, b| a.0.total_cmp(&b.0));
                s.iter().take(10).map(|x| x.1).collect()
            })
            .collect();
        for v in &variants {
            let mut hit = 0;
            for (q, t) in queries.iter().zip(&truth) {
                let mut c: Vec<(f32, u32)> = run(&graph, v, q, &mask)
                    .into_iter()
                    .map(|(r, _)| (-exact(q, r), r))
                    .collect();
                c.sort_by(|a, b| a.0.total_cmp(&b.0));
                hit += c.iter().take(10).filter(|x| t.contains(&x.1)).count();
            }
            let recall = hit as f64 / (queries.len() * 10) as f64;
            let t0 = Instant::now();
            let mut n = 0;
            while t0.elapsed().as_secs_f64() < seconds / 2. {
                for q in &queries {
                    run(&graph, v, q, &mask);
                    n += 1;
                }
            }
            let serial = t0.elapsed().as_secs_f64() * 1000. / n as f64;
            let t0 = Instant::now();
            let total: usize = std::thread::scope(|s| {
                (0..threads)
                    .map(|i| {
                        let (graph, queries, mask, v) = (&graph, &queries, &mask, v);
                        let run = &run;
                        s.spawn(move || {
                            let mut n = 0;
                            while t0.elapsed().as_secs_f64() < seconds {
                                run(graph, v, &queries[(i + n) % queries.len()], mask);
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
            println!(
                "{:>5.1}% allowed={:>6} {:<8} recall@10 {:.3}  serial {:.3} ms ({:.0} ns/row)  {threads}t {:.0} q/s",
                mask.len() as f64 * 100. / rows as f64,
                mask.len(),
                v.0,
                recall,
                serial,
                serial * 1e6 / mask.len() as f64,
                total as f64 / t0.elapsed().as_secs_f64()
            );
        }
    }
    Ok(())
}
