//! Reproducible local-store workload with bounded ingestion batches and real persisted indexes.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::QueryRequest;
use serde_json::json;
use std::{
    error::Error,
    time::{Duration, Instant},
};

// Keep backend diagnostics on stderr, without changing the benchmark's JSON output.
// Only errors are collected; there is no span allocation or steady-state query tracing.
struct BenchmarkErrors;
impl tracing::Subscriber for BenchmarkErrors {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::ERROR
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields;
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                eprintln!("{}={value:?}", field.name());
            }
        }
        event.record(&mut Fields);
    }
}

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
    tracing::subscriber::set_global_default(BenchmarkErrors)?;
    let rows: usize = argument("--rows", "1000000").parse()?;
    let dims: usize = argument("--dims", "768").parse()?;
    let vector_type = argument("--vector-type", "f32");
    if !matches!(vector_type.as_str(), "f16" | "f32") {
        return Err("vector type must be f16 or f32".into());
    }
    let batch: usize = argument("--batch", "5000").parse()?;
    let queries: usize = argument("--queries", "20").parse()?;
    let recall_queries: usize = argument("--recall-queries", "10").parse()?;
    let nprobe: usize = argument("--nprobe", "128").parse()?;
    if rows == 0 || dims == 0 || batch == 0 || queries == 0 {
        return Err("counts must be positive".into());
    }
    // Optional packed corpus and held-out queries let the same workload use NQ embeddings.
    let corpus_path = argument("--corpus-f16", "");
    let corpus = if corpus_path.is_empty() {
        None
    } else {
        use std::io::Read;
        let mut bytes = vec![0u8; rows * dims * 2];
        std::fs::File::open(&corpus_path)?.read_exact(&mut bytes)?;
        Some(bytes)
    };
    let query_path = argument("--queries-f32", "");
    let query_bytes = if query_path.is_empty() {
        None
    } else {
        Some(std::fs::read(&query_path)?)
    };
    if query_bytes
        .as_ref()
        .is_some_and(|b| b.is_empty() || b.len() % (dims * 4) != 0)
    {
        return Err("queries must contain complete packed f32 vectors".into());
    }
    let input_vector = |id: usize| -> Vec<f32> {
        match &corpus {
            None => vector(id, dims),
            Some(bytes) => bytes[id * dims * 2..(id + 1) * dims * 2]
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| bf_vector::f16::from_le_bytes(*b).to_f32())
                .collect(),
        }
    };
    let query_vector = |i: usize| -> Vec<f32> {
        if let Some(bytes) = &query_bytes {
            let row = i % (bytes.len() / (dims * 4));
            return bytes[row * dims * 4..(row + 1) * dims * 4]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect();
        }
        let mut query = input_vector((i * 7919) % rows);
        let mut noise = i as u64 + 991;
        for x in &mut query {
            *x += random(&mut noise) * 0.01;
        }
        query
    };
    let requested_url = argument("--url", "");
    let scratch = if requested_url.is_empty() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
        std::fs::create_dir_all(&root)?;
        Some(tempfile::tempdir_in(root)?)
    } else {
        None
    };
    let url = match &scratch {
        Some(dir) => format!("file://{}", dir.path().canonicalize()?.display()),
        None => requested_url,
    };
    let storage_config = StorageConfig {
        url,
        endpoint: std::env::var("BENCH_ENDPOINT").ok(),
        profile: std::env::var("BENCH_PROFILE").ok(),
        region: std::env::var("BENCH_REGION").ok(),
        cache: bf_storage::CacheConfig {
            block_size: argument("--cache-block-bytes", "65536").parse()?,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = Storage::open(&storage_config).await?;
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        ann_nprobe: nprobe,
        ann_nprobe_explicit: true,
        compaction_segments: argument("--compaction-segments", "4").parse()?,
        ..Default::default()
    };
    let compaction_segments = config.compaction_segments;
    let engine = Engine::with_config(store.clone(), config.clone());
    let reuse = argument("--reuse", "");
    let ns = if reuse.is_empty() {
        format!("bench-{}", uuid::Uuid::new_v4())
    } else {
        reuse.clone()
    };
    let start = Instant::now();
    let mut writing = Duration::ZERO;
    let mut indexing = Duration::ZERO;
    let mut write_ms = vec![];
    for first in (0..if reuse.is_empty() { rows } else { 0 }).step_by(batch) {
        let docs: Vec<_> = (first..(first+batch).min(rows)).map(|id|json!({
            "id":id,"vector":input_vector(id),"text":format!("term{} term{} search engine",id%4096,(id*17+7)%4096),"bucket":id%16
        })).collect();
        let request = if first == 0 {
            json!({"distance_metric":"cosine_distance","schema":{"text":{"type":"string","full_text_search":true},"vector":{"type":format!("[{dims}]{vector_type}"),"ann":true}},"upsert_rows":docs})
        } else {
            json!({"upsert_rows":docs})
        };
        let request = serde_json::from_value(request)?;
        let tick = Instant::now();
        engine.write(&ns, request).await?;
        writing += tick.elapsed();
        write_ms.push(tick.elapsed().as_secs_f64() * 1000.0);
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
    let live_segment_bytes: u64 = manifest
        .segments
        .iter()
        .map(|s| s.live.size + s.files.values().map(|f| f.size).sum::<u64>())
        .sum();
    let live_directory_bytes: u64 = manifest.ann_dirs.values().map(|f| f.size).sum();
    let live_ann: Vec<_> = manifest
        .segments
        .iter()
        .flat_map(|s| &s.files)
        .filter(|(k, _)| {
            k.starts_with("ann:")
                || k.starts_with("annfrag:")
                || k.starts_with("annfrag2:")
                || k.starts_with("annv2:")
        })
        .map(|(_, f)| f)
        .collect();
    let live_ann_bytes: u64 = live_ann.iter().map(|f| f.size).sum();
    let indexed_bytes = live_segment_bytes
        + live_directory_bytes
        + manifest.text_stats.as_ref().map_or(0, |f| f.size);
    drop(engine);
    let query_store = Storage::open(&StorageConfig {
        read_only: true,
        ..storage_config.clone()
    })
    .await?;
    let engine = Engine::with_config(query_store, config.clone());
    let mut cold_ann_ms = 0.0;
    // Measure text on its own fresh storage and engine caches, including shared IDs/live masks.
    let cold_bm25_ms = {
        let cold_store = Storage::open(&StorageConfig {
            read_only: true,
            ..storage_config.clone()
        })
        .await?;
        let cold_text = Engine::with_config(cold_store, config.clone());
        let request =
            serde_json::from_value(json!({"rank_by":["text","BM25","term0 term7"],"top_k":10}))?;
        let tick = Instant::now();
        cold_text.query(&ns, request).await?;
        tick.elapsed().as_secs_f64() * 1000.0
    };
    let filtered_cold_ms = {
        let cold_store = Storage::open(&StorageConfig {
            read_only: true,
            ..storage_config.clone()
        })
        .await?;
        let cold = Engine::with_config(cold_store, config.clone());
        let request = serde_json::from_value(
            json!({"rank_by":["vector","ANN",query_vector(0)],"top_k":10,"filters":["bucket","Eq",3]}),
        )?;
        let tick = Instant::now();
        cold.query(&ns, request).await?;
        tick.elapsed().as_secs_f64() * 1000.
    };
    let mut filtered_ms = vec![];
    let mut ann_execution_ms = vec![];
    let mut bm25_execution_ms = vec![];
    let mut ann_ms = vec![];
    let mut bm25_ms = vec![];
    let consistency = if argument("--consistency", "strong") == "eventual" {
        json!({"level":"eventual"})
    } else {
        json!({"level":"strong"})
    };
    for i in 0..queries + 3 {
        let query = query_vector(i);
        let request: QueryRequest = serde_json::from_value(
            json!({"rank_by":["vector","ANN",query],"top_k":10,"consistency":consistency}),
        )?;
        let tick = Instant::now();
        let response = engine.query(&ns, request).await?;
        assert_eq!(response.performance.exhaustive_search_count, 0);
        if i == 0 {
            cold_ann_ms = tick.elapsed().as_secs_f64() * 1000.0;
        }
        if i >= 3 {
            ann_execution_ms.push(response.performance.query_execution_ms as f64);
            ann_ms.push(tick.elapsed().as_secs_f64() * 1000.0);
        }
        let request: QueryRequest = serde_json::from_value(
            json!({"rank_by":["text","BM25",format!("term{} term{}",i%4096,(i*17+7)%4096)],"top_k":10,"consistency":consistency}),
        )?;
        let tick = Instant::now();
        let response = engine.query(&ns, request).await?;
        if i >= 3 {
            bm25_execution_ms.push(response.performance.query_execution_ms as f64);
            bm25_ms.push(tick.elapsed().as_secs_f64() * 1000.0);
        }
        let filtered = serde_json::from_value(
            json!({"rank_by":["vector","ANN",query_vector(i)],"top_k":10,"consistency":consistency,"filters":["bucket","Eq",3]}),
        )?;
        let tick = Instant::now();
        engine.query(&ns, filtered).await?;
        if i >= 3 {
            filtered_ms.push(tick.elapsed().as_secs_f64() * 1000.);
        }
        eprintln!("timed query {}/{}", i + 1, queries + 3);
    }
    let mut recall = 0.0;
    let mut filtered_recall = 0.0;
    for i in 0..recall_queries {
        let query = query_vector(i);
        let result = engine
            .recall(
                &ns,
                json!({"rank_by":["vector","ANN",query],"num":1,"top_k":10}),
            )
            .await?;
        recall += result["avg_recall"].as_f64().ok_or("missing recall")?;
        let result = engine.recall(&ns, json!({"rank_by":["vector","ANN",query_vector(i)],"filters":["bucket","Eq",3],"num":1,"top_k":10})).await?;
        filtered_recall += result["avg_recall"]
            .as_f64()
            .ok_or("missing filtered recall")?;
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
            "namespace":ns,"cold_ann_ms":cold_ann_ms,"cold_bm25_ms":cold_bm25_ms,
            "ann_execution_p50_ms":percentile(&mut ann_execution_ms,0.5),"bm25_execution_p50_ms":percentile(&mut bm25_execution_ms,0.5),
            "write_batch_p50_ms":if write_ms.is_empty() { None } else { Some(percentile(&mut write_ms,0.5)) },
            "rows":rows,"dimensions":dims,"batch_rows":batch,"compaction_segments":compaction_segments,"live_segments":live_segments,"live_ann_bytes":live_ann_bytes,"live_ann_objects":live_ann.len(),"indexed_bytes":indexed_bytes,"live_directory_bytes":live_directory_bytes,"queries":queries,"recall_queries":recall_queries,"nprobe":nprobe,
            "write_rows_per_second":rows as f64/writing.as_secs_f64(),"write_seconds":writing.as_secs_f64(),
            "index_seconds":indexing.as_secs_f64(),"load_wall_seconds":load_seconds,
            "ann_p50_ms":percentile(&mut ann_ms,0.5),"ann_p99_ms":percentile(&mut ann_ms,0.99),
            "bm25_p50_ms":percentile(&mut bm25_ms,0.5),"bm25_p99_ms":percentile(&mut bm25_ms,0.99),
            "recall_at_10":recall/recall_queries as f64,"filtered_recall_at_10":filtered_recall/recall_queries as f64,"filtered_cold_ms":filtered_cold_ms,"filtered_p50_ms":percentile(&mut filtered_ms,0.5),"filtered_p99_ms":percentile(&mut filtered_ms,0.99),"cold_open_ms":cold_ms,"segment_bytes":segment_bytes,
            "cold_open_scope":"new engine, manifest, and empty tail; OS page cache retained",
            "vector_type":vector_type,"corpus":if corpus.is_some() { "packed f16 input, synthetic text" } else { "256 synthetic vector clusters, 4096 text terms, four tokens per document" }
        }))?
    );
    Ok(())
}
