//! W1 tail-path measurements over NQ vectors; no persistent namespace is created.
use super::*;
use std::io::{Read, Seek, SeekFrom};

fn corpus() -> std::fs::File {
    let path = std::path::PathBuf::from(std::env::var("W1_CORPUS").expect("W1_CORPUS"));
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    assert!(
        path.canonicalize()
            .unwrap()
            .starts_with(root.join(".scratch"))
    );
    let mut file = std::fs::File::open(path).unwrap();
    let mut magic = [0; 8];
    file.read_exact(&mut magic).unwrap();
    assert_eq!(&magic[..6], b"\x93NUMPY");
    let len = if magic[6] == 1 {
        let mut bytes = [0; 2];
        file.read_exact(&mut bytes).unwrap();
        u16::from_le_bytes(bytes) as i64
    } else {
        let mut bytes = [0; 4];
        file.read_exact(&mut bytes).unwrap();
        u32::from_le_bytes(bytes) as i64
    };
    file.seek(SeekFrom::Current(len)).unwrap();
    file
}

/// Pin this invocation to the server CPUs, wrap it in timeout and perf stat, and delete its
/// scratch corpus afterwards. Both revisions run the identical harness in ABAB order.
#[tokio::test]
#[ignore = "W1 diagnostic; requires W1_CORPUS inside this worktree's .scratch"]
async fn resident_tail_benchmark() {
    let mut file = corpus();
    let n: usize = std::env::var("W1_ROWS")
        .unwrap_or_else(|_| "63000".into())
        .parse()
        .unwrap();
    let iterations: usize = std::env::var("W1_QUERIES")
        .unwrap_or_else(|_| "100".into())
        .parse()
        .unwrap();
    let mut state = NamespaceState {
        exists: true,
        schema: serde_json::from_value(json!({"id":{"type":"uint"},"vector":{"type":"[1024]f16"}}))
            .unwrap(),
        ..Default::default()
    };
    let mut decoded = Vec::new();
    for id in 0..n {
        let mut bytes = [0; 2048];
        file.read_exact(&mut bytes).unwrap();
        let v: Vec<f32> = bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| half::f16::from_le_bytes(*b).to_f32())
            .collect();
        let doc =
            serde_json::from_value(json!({"id":id,"vector":encode_vector(&v, false)})).unwrap();
        state.docs.insert(Id::Uint(id as u64), Arc::new(doc));
        decoded.push(v);
    }
    let vector = decoded[n / 2].clone();
    let request: QueryRequest =
        serde_json::from_value(json!({"rank_by":["vector","ANN",vector],"top_k":10})).unwrap();
    let store = Storage::open(&bf_storage::StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let config = EngineConfig {
        recall_sample_rate: 0.0,
        ..Default::default()
    };
    let context = read_cache::ReadContext::new(Arc::new(read_cache::DecodedCache::new(1 << 30, 1)));
    if std::env::var_os("W1_PROFILE").is_some() {
        let start = Instant::now();
        for doc in state.docs.values() {
            std::hint::black_box(decode_vector(&doc["vector"]).unwrap());
        }
        eprintln!("W1 decode_ms={:.3}", start.elapsed().as_secs_f64() * 1000.0);
        let start = Instant::now();
        for v in &decoded {
            std::hint::black_box(query::distance(v, &vector, state.metric));
        }
        eprintln!(
            "W1 distance_ms={:.3}",
            start.elapsed().as_secs_f64() * 1000.0
        );
        let start = Instant::now();
        for _ in 0..10 {
            std::hint::black_box(state.clone());
        }
        eprintln!("W1 clone_ms={:.3}", start.elapsed().as_secs_f64() * 100.0);
    }
    drop(decoded);
    read_cache::scope(context, async {
        for _ in 0..3 {
            indexed::execute(&store, &state, &request, &config, false)
                .await
                .unwrap();
        }
        eprintln!("W1 READY pid={}", std::process::id());
        if let Ok(path) = std::env::var("W1_READY") {
            let path = std::path::PathBuf::from(path);
            let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .canonicalize()
                .unwrap();
            assert!(path.starts_with(root.join(".scratch")));
            std::fs::write(path, std::process::id().to_string()).unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
        let mut times = Vec::new();
        for _ in 0..iterations {
            let start = Instant::now();
            let response = indexed::execute(&store, &state, &request, &config, false)
                .await
                .unwrap();
            assert_eq!(response.result.rows.as_ref().unwrap().len(), 10);
            times.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        times.sort_by(f64::total_cmp);
        eprintln!(
            "W1 rows={n} queries={iterations} p50_ms={:.3} p90_ms={:.3}",
            times[iterations / 2],
            times[iterations * 9 / 10]
        );
    })
    .await;
}
