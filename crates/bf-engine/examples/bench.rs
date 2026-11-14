//! Reproducible local-store workload with bounded ingestion batches and real persisted indexes.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::QueryRequest;
use serde_json::json;
use std::{
    error::Error,
    time::{Duration, Instant},
};

fn argument(name: &str, default: &str) -> String {
    let args: Vec<_> = std::env::args().collect();
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.into())
}
fn random(seed: &mut u64) -> f32 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((*seed >> 32) as u32 as f64 / u32::MAX as f64 * 2.0 - 1.0) as f32
}
fn vector(id: usize, dims: usize) -> Vec<f32> {
    let mut center = (id % 256) as u64 + 123;
    let mut noise = id as u64 + 9_876_543;
    (0..dims)
        .map(|_| random(&mut center) + random(&mut noise) * 0.15)
        .collect()
}
fn percentile(values: &mut [f64], q: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() as f64 * q).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1)]
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let rows: usize = argument("--rows", "1000000").parse()?;
    let dims: usize = argument("--dims", "768").parse()?;
    let batch: usize = argument("--batch", "5000").parse()?;
    let queries: usize = argument("--queries", "20").parse()?;
    let recall_queries: usize = argument("--recall-queries", "10").parse()?;
    let nprobe: usize = argument("--nprobe", "128").parse()?;
    if rows == 0 || batch == 0 || queries == 0 || recall_queries == 0 {
        return Err("counts must be positive".into());
    }
    let default_url = format!("file:///tmp/blowfish-bench-{}", uuid::Uuid::new_v4());
    let url = argument("--url", &default_url);
    if !url.starts_with("file://") {
        return Err("benchmark requires a local file:// store".into());
    }
    let store = Storage::open(&StorageConfig {
        url,
        ..Default::default()
    })
    .await?;
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        ann_nprobe: nprobe,
        compaction_segments: argument("--compaction-segments", "4").parse()?,
        ..Default::default()
    };
    let compaction_segments = config.compaction_segments;
    let engine = Engine::with_config(store.clone(), config.clone());
    let ns = format!("bench-{}", uuid::Uuid::new_v4());
    let start = Instant::now();
    let mut writing = Duration::ZERO;
    let mut indexing = Duration::ZERO;
    for first in (0..rows).step_by(batch) {
        let docs: Vec<_> = (first..(first+batch).min(rows)).map(|id|json!({
            "id":id,"vector":vector(id,dims),"text":format!("term{} term{} search engine",id%4096,(id*17+7)%4096),"bucket":id%16
        })).collect();
        let request = if first == 0 {
            json!({"distance_metric":"cosine_distance","schema":{"text":{"type":"string","full_text_search":true}},"upsert_rows":docs})
        } else {
            json!({"upsert_rows":docs})
        };
        let request = serde_json::from_value(request)?;
        let tick = Instant::now();
        engine.write(&ns, request).await?;
        writing += tick.elapsed();
        let tick = Instant::now();
        engine.index_namespace(&ns, true).await?;
        indexing += tick.elapsed();
        if (first / batch).is_multiple_of(10) || first + batch >= rows {
            eprintln!(
                "indexed {}/{} rows, elapsed {:.1}s",
                (first + batch).min(rows),
                rows,
                start.elapsed().as_secs_f64()
            );
        }
    }
    let tick = Instant::now();
    while engine.index_namespace(&ns, true).await? {}
    indexing += tick.elapsed();
    let load_seconds = start.elapsed().as_secs_f64();
    let (_, manifest) = bf_wal::Wal::new(store.clone(), &ns)?
        .latest_manifest()
        .await?
        .ok_or("missing checkpoint")?;
    let live_segments = manifest.segments.len();
    let mut ann_ms = vec![];
    let mut bm25_ms = vec![];
    for i in 0..queries + 3 {
        let mut query = vector((i * 7919) % rows, dims);
        let mut noise = i as u64 + 991;
        for x in &mut query {
            *x += random(&mut noise) * 0.01;
        }
        let request: QueryRequest =
            serde_json::from_value(json!({"rank_by":["vector","ANN",query],"top_k":10}))?;
        let tick = Instant::now();
        let response = engine.query(&ns, request).await?;
        assert_eq!(response.performance.exhaustive_search_count, 0);
        if i >= 3 {
            ann_ms.push(tick.elapsed().as_secs_f64() * 1000.0);
        }
        let request: QueryRequest = serde_json::from_value(
            json!({"rank_by":["text","BM25",format!("term{} term{}",i%4096,(i*17+7)%4096)],"top_k":10}),
        )?;
        let tick = Instant::now();
        engine.query(&ns, request).await?;
        if i >= 3 {
            bm25_ms.push(tick.elapsed().as_secs_f64() * 1000.0);
        }
        eprintln!("timed query {}/{}", i + 1, queries + 3);
    }
    let mut recall = 0.0;
    for i in 0..recall_queries {
        let mut query = vector((i * 7919) % rows, dims);
        let mut noise = i as u64 + 991;
        for x in &mut query {
            *x += random(&mut noise) * 0.01;
        }
        let result = engine
            .recall(
                &ns,
                json!({"rank_by":["vector","ANN",query],"num":1,"top_k":10}),
            )
            .await?;
        recall += result["avg_recall"].as_f64().ok_or("missing recall")?;
        eprintln!("recall sample {}/{}", i + 1, recall_queries);
    }
    drop(engine);
    let tick = Instant::now();
    let cold = Engine::with_config(store.clone(), config);
    cold.schema(&ns).await?;
    let cold_ms = tick.elapsed().as_secs_f64() * 1000.0;
    let files = store.list(&format!("ns/{ns}/segments/")).await?;
    let segment_bytes: u64 = files.iter().map(|f| f.size).sum();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "rows":rows,"dimensions":dims,"batch_rows":batch,"compaction_segments":compaction_segments,"live_segments":live_segments,"queries":queries,"recall_queries":recall_queries,"nprobe":nprobe,
            "write_rows_per_second":rows as f64/writing.as_secs_f64(),"write_seconds":writing.as_secs_f64(),
            "index_seconds":indexing.as_secs_f64(),"load_wall_seconds":load_seconds,
            "ann_p50_ms":percentile(&mut ann_ms,0.5),"ann_p99_ms":percentile(&mut ann_ms,0.99),
            "bm25_p50_ms":percentile(&mut bm25_ms,0.5),"bm25_p99_ms":percentile(&mut bm25_ms,0.99),
            "recall_at_10":recall/recall_queries as f64,"cold_open_ms":cold_ms,"segment_bytes":segment_bytes,
            "cold_open_scope":"new engine, manifest, and empty tail; OS page cache retained",
            "corpus":"256 synthetic vector clusters, 4096 text terms, four tokens per document"
        }))?
    );
    Ok(())
}
