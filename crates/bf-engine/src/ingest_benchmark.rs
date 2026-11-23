//! IN1 measurements. Every invocation uses an isolated store under `.scratch/`.
use super::*;
use bf_storage::StorageConfig;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

fn number(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(default)
}
fn array(path: &str) -> BufReader<std::fs::File> {
    let mut f = BufReader::new(std::fs::File::open(path).unwrap());
    let mut magic = [0; 8];
    f.read_exact(&mut magic).unwrap();
    if &magic[..6] == b"\x93NUMPY" {
        let len = if magic[6] == 1 {
            let mut b = [0; 2];
            f.read_exact(&mut b).unwrap();
            u16::from_le_bytes(b) as i64
        } else {
            let mut b = [0; 4];
            f.read_exact(&mut b).unwrap();
            u32::from_le_bytes(b) as i64
        };
        f.seek(SeekFrom::Current(len)).unwrap();
    } else {
        f.seek(SeekFrom::Start(0)).unwrap();
    }
    f
}
async fn select(wal: &Wal, run: &std::path::Path) {
    let mut m: bf_wal::Manifest =
        serde_json::from_slice(&std::fs::read(run.join("source.json")).unwrap()).unwrap();
    let (seq, current) = wal.latest_manifest().await.unwrap().unwrap();
    m.indexer_epoch = current.indexer_epoch;
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
}

/// The caller pins CPUs, supplies a timeout, and removes the entire scratch run after measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "IN1 benchmark; requires IN1_RUN under .scratch and an explicit action"]
async fn ingest_upgrade_benchmark() {
    let run = std::path::PathBuf::from(std::env::var("IN1_RUN").expect("IN1_RUN"));
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    assert!(run.is_absolute() && run.starts_with(root.join(".scratch")));
    std::fs::create_dir_all(run.join("store")).unwrap();
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", run.join("store").display()),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            lease_duration: std::time::Duration::ZERO,
            compaction_segments: usize::MAX,
            max_segment_rows: 100_000,
            max_unindexed_bytes: 2_000_000_000,
            ..Default::default()
        },
    );
    let wal = Wal::new(store.clone(), "in1").unwrap();
    let action = std::env::var("IN1_ACTION").expect("IN1_ACTION");
    bf_alloc::start_monitor();
    if action == "build-text" {
        let schema: Schema = serde_json::from_value(
            json!({"id":{"type":"uint"},"text":{"type":"string","full_text_search":true}}),
        )
        .unwrap();
        engine
            .write(
                "in1",
                serde_json::from_value(
                    json!({"schema":schema,"upsert_rows":[{"id":0,"text":"seed"}]}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("in1", true).await.unwrap();
        let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
        let schema = m.schema.clone();
        m.segments.clear();
        m.stats = Default::default();
        m.text_stats = None;
        for s in 0..number("IN1_SEGMENTS", 16) {
            let mut docs = BTreeMap::new();
            let mut builder = bf_text::SegmentBuilder::new(true);
            for row in 0..number("IN1_SEGMENT_ROWS", 10_000) {
                let id = s * number("IN1_SEGMENT_ROWS", 10_000) + row;
                let text = (0..100)
                    .map(|t| format!("word{} ", (id.wrapping_mul(179) + t * 101) % 10_007))
                    .collect::<String>();
                let tokens = bf_text::tokenize(&text, &Default::default());
                builder.add_document(&tokens).unwrap();
                docs.insert(
                    Id::Uint(id as u64),
                    serde_json::from_value::<Document>(json!({"id":id,"text":text})).unwrap(),
                );
            }
            let mut seg = segment::build_with(
                &store,
                "in1",
                &schema,
                DistanceMetric::CosineDistance,
                &docs,
                BTreeMap::new(),
                1,
                1,
            )
            .await
            .unwrap();
            let old = builder.finish_colocated_fixture().unwrap();
            let file = segment::put_file(&store, format!("ns/in1/fixture-{s}.bft"), old, 0)
                .await
                .unwrap();
            seg.files.insert("fts:text".into(), file);
            seg.files.remove("text_keys");
            for doc in docs.values() {
                crate::text::adjust(&mut m.stats, doc, &schema, true);
                m.stats.rows += 1;
                m.stats.logical_bytes += serde_json::to_vec(doc).unwrap().len() as u64;
            }
            m.segments.push(seg);
        }
        let text = std::mem::take(&mut m.stats.text);
        let (bytes, index) = crate::text::encode_text_stats(&text).unwrap();
        let mut f = segment::put_file(&store, "ns/in1/source-stats.bfs".into(), bytes, 0)
            .await
            .unwrap();
        f.stats_index = Some(index);
        m.text_stats = Some(f);
        wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
        std::fs::write(run.join("source.json"), serde_json::to_vec(&m).unwrap()).unwrap();
    } else if action == "upgrade" {
        select(&wal, &run).await;
        let before = Engine::with_config(store.clone(), engine.0.config.clone());
        let mut expected = Vec::new();
        for i in 0..if std::env::var("IN1_VERIFY").as_deref() == Ok("0") {
            0
        } else {
            32
        } {
            let q: QueryRequest = serde_json::from_value(json!({"rank_by":["text","BM25",format!("word{} word{}",i*101,i*101+101)],"top_k":10})).unwrap();
            expected.push((q.clone(), before.query("in1", q).await.unwrap().result.rows));
        }
        drop(before);
        let start = Instant::now();
        let mut passes = 0;
        while engine.index_namespace("in1", false).await.unwrap() {
            passes += 1;
        }
        let seconds = start.elapsed().as_secs_f64();
        let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
        let mut hashes = Vec::new();
        for s in &m.segments {
            let bytes = segment::read_whole(&store, &s.files["fts:text"])
                .await
                .unwrap();
            hashes.push(crc32fast::hash(&bytes));
        }
        let verified_queries = expected.len();
        for (q, rows) in expected {
            assert_eq!(engine.query("in1", q).await.unwrap().result.rows, rows);
        }
        println!(
            "IN1 upgrade seconds={seconds:.6} passes={passes} hashes={hashes:?} verified_queries={verified_queries}"
        );
    } else if action == "load" || action == "update" {
        let rows = number("IN1_ROWS", 100_000);
        let mut vectors = array(&std::env::var("IN1_CORPUS").expect("IN1_CORPUS"));
        let mut attrs = BufReader::new(
            std::fs::File::open(std::env::var("IN1_ATTRS").expect("IN1_ATTRS")).unwrap(),
        )
        .lines();
        let start = Instant::now();
        for first in (0..rows).step_by(1000) {
            let mut docs = Vec::new();
            for id in first..(first + 1000).min(rows) {
                let mut raw = vec![0; 2048];
                vectors.read_exact(&mut raw).unwrap();
                let v: Vec<f32> = raw
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|b| half::f16::from_le_bytes(*b).to_f32())
                    .collect();
                let mut d: Document =
                    serde_json::from_str(&attrs.next().unwrap().unwrap()).unwrap();
                if action == "update" {
                    d.insert("title".into(), json!(format!("Updated title {id}")));
                }
                d.insert("id".into(), json!(id));
                d.insert("vector".into(), json!(bf_types::encode_vector(&v, false)));
                docs.push(d);
            }
            let mut request = json!({"upsert_rows":docs});
            if first == 0 && action == "load" {
                request["schema"] = json!({"vector":{"type":"[1024]f16","ann":true},"text":{"type":"string","full_text_search":true},"title":{"type":"string"}});
                request["distance_metric"] = json!("cosine_distance");
            }
            engine
                .write("in1", serde_json::from_value(request).unwrap())
                .await
                .unwrap();
            if (first + 1000) % number("IN1_FLUSH_ROWS", 100_000) == 0 || first + 1000 >= rows {
                engine.index_namespace("in1", true).await.unwrap();
                eprintln!("IN1 {action} completed={}", (first + 1000).min(rows));
            }
        }
        println!(
            "IN1 {action} rows={rows} seconds={:.6} rows_per_sec={:.3}",
            start.elapsed().as_secs_f64(),
            rows as f64 / start.elapsed().as_secs_f64()
        );
        let m = engine.metadata("in1").await.unwrap();
        println!("IN1 metadata={m}");
    } else {
        panic!("unknown IN1_ACTION");
    }
}
