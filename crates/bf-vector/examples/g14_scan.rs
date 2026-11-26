//! Round G14: time `HotGraph::scan` alone on the first ROWS rows of the NQ corpus, for hash-like masks.
//! Usage: g14_scan CORPUS_F16_NPY ROWS [threads=16] [seconds=3]
use bf_vector::{RoaringBitmap, warm_graph::HotGraphBuilder};
use std::{io::Read, time::Instant};
fn mix(x: u64) -> u64 {
    let mut x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let rows: usize = args[2].parse()?;
    let threads: usize = args.get(3).map_or(Ok(16), |s| s.parse())?;
    let seconds: f64 = args.get(4).map_or(Ok(3.0), |s| s.parse())?;
    let dims = 1024;
    let mut file = std::fs::File::open(&args[1])?;
    let mut header = [0u8; 128];
    file.read_exact(&mut header)?;
    let mut bytes = vec![0u8; (rows + 64) * dims * 2];
    file.read_exact(&mut bytes)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()?;
    let t = Instant::now();
    let mut builder = HotGraphBuilder::new(dims, rows)?;
    for first in (0..rows).step_by(50_000) {
        let end = (first + 50_000).min(rows);
        let ids: Vec<u32> = (first as u32..end as u32).collect();
        builder.append(&ids, &bytes[first * dims * 2..end * dims * 2], &|| false)?;
    }
    let graph = pool.install(|| builder.finish(&|| false))?;
    eprintln!("built {rows} rows in {:.1}s", t.elapsed().as_secs_f64());
    let queries: Vec<Vec<f32>> = (0..64)
        .map(|q| {
            let at = (rows + q) * dims * 2;
            bytes[at..at + dims * 2]
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| half::f16::from_le_bytes(*b).to_f32())
                .collect()
        })
        .collect();
    for per_mille in [5u64, 10, 20, 50, 100] {
        let mask: RoaringBitmap = (0..rows as u32)
            .filter(|r| mix(*r as u64) % 1000 < per_mille)
            .collect();
        // Serial: mean over 64 queries, repeated.
        let t = Instant::now();
        let mut n = 0;
        while t.elapsed().as_secs_f64() < seconds / 2. {
            for q in &queries {
                graph.scan(q, 30, &mask)?;
                n += 1;
            }
        }
        let serial = t.elapsed().as_secs_f64() * 1000. / n as f64;
        // Parallel: every thread loops until the deadline.
        let t = Instant::now();
        let total: usize = std::thread::scope(|s| {
            (0..threads)
                .map(|i| {
                    let (graph, queries, mask) = (&graph, &queries, &mask);
                    s.spawn(move || {
                        let mut n = 0;
                        while t.elapsed().as_secs_f64() < seconds {
                            graph.scan(&queries[(i + n) % 64], 30, mask).unwrap();
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
            "allowed={:>6} ({:.1}%)  serial {:.3} ms/q ({:.0} ns/row)  {threads} threads {:.0} q/s",
            mask.len(),
            mask.len() as f64 * 100. / rows as f64,
            serial,
            serial * 1e6 / mask.len() as f64,
            total as f64 / t.elapsed().as_secs_f64()
        );
    }
    Ok(())
}
