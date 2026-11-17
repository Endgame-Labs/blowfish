//! Reproducible synthetic in-memory benchmark (defaults to 1,000,000 documents).
//! Run: cargo run --release -p bf-text --example benchmark -- 1000000 200
use bf_text::{FtsParams, MemoryRangeRead, Query, SegmentBuilder, SegmentReader, Token};
use futures::executor::block_on;
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let docs: usize = args
        .get(1)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(1_000_000);
    let iterations: usize = args.get(2).map(|s| s.parse()).transpose()?.unwrap_or(200);
    if iterations == 0 {
        return Err("iterations must be positive".into());
    }
    let vocabulary: Vec<_> = (0..4096).map(|i| format!("term{i:04}")).collect();
    let start = Instant::now();
    let mut builder = SegmentBuilder::new(false);
    let mut state = 42u64;
    for row in 0..docs {
        let length = 8 + row % 25;
        let tokens: Vec<_> = (0..length)
            .map(|position| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                // A mix of common and less common terms, with deterministic query diversity.
                let mask = if position % 3 == 0 { 63 } else { 4095 };
                Token::new(
                    &vocabulary[((state >> 32) as usize) & mask],
                    position as u32,
                )
            })
            .collect();
        builder.add_document(&tokens)?;
    }
    let bytes = builder.finish()?;
    let size = bytes.len();
    let build_time = start.elapsed();
    let reader = block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(bytes))))?;
    let stats = block_on(reader.stats())?;
    let params = FtsParams::default();
    let mut latencies = Vec::with_capacity(iterations);
    let mut reads = 0;
    let mut pruned = 0;
    let mut checksum = 0u64;
    for i in 0..iterations + 10 {
        let query = Query::new(
            vec![
                Token::new(&vocabulary[(i * 37) % 4096], 0),
                Token::new(&vocabulary[(i * 71 + 3) % 4096], 1),
            ],
            false,
        );
        let start = Instant::now();
        let (hits, metrics) =
            block_on(reader.search_with_metrics(&query, &stats, &params, 10, None))?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        checksum = checksum.wrapping_add(hits.first().map_or(0, |h| h.row as u64));
        if i >= 10 {
            latencies.push(elapsed);
            reads += metrics.blocks_read;
            pruned += metrics.windows_pruned;
        }
    }
    latencies.sort_by(f64::total_cmp);
    println!(
        "documents={docs} tokens={} vocabulary={} positions=false",
        stats.average_doc_length * docs as f64,
        vocabulary.len()
    );
    println!(
        "index_bytes={size} bytes_per_doc={:.2} build_seconds={:.3}",
        size as f64 / docs.max(1) as f64,
        build_time.as_secs_f64()
    );
    println!(
        "queries={iterations} p50_ms={:.3} p95_ms={:.3} mean_blocks_read={:.1} mean_windows_pruned={:.1} checksum={checksum}",
        latencies[iterations / 2],
        latencies[(iterations * 95 / 100).min(iterations - 1)],
        reads as f64 / iterations as f64,
        pruned as f64 / iterations as f64
    );
    Ok(())
}
