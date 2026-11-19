//! A manifest with text headers gets a compact reader copy; ANN queries use it, text queries and
//! the indexer take the full manifest, and every answer is the same either way. This test sets the
//! copy threshold through the environment, so it lives in its own test binary.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use bf_wal::{ManifestView, Wal};
use serde_json::{Value, json};

fn vector(id: usize) -> Vec<f32> {
    (0..16)
        .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
        .collect()
}
fn engine(store: &Storage, indexing: bool) -> Engine {
    Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    )
}
async fn run(e: &Engine, body: Value) -> Vec<Value> {
    let request: QueryRequest = serde_json::from_value(body).unwrap();
    let rows = e.query("t", request).await.unwrap().result.rows.unwrap();
    rows.iter().map(|r| r["id"].clone()).collect()
}
fn ann(q: usize) -> Value {
    json!({"rank_by":["vector","ANN",vector(q)],"top_k":10,"consistency":{"level":"strong"}})
}
fn bm25(word: &str) -> Value {
    json!({"rank_by":["text","BM25",word],"top_k":10,"consistency":{"level":"strong"}})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ann_reads_the_compact_copy_and_text_and_writers_upgrade_it() {
    // SAFETY: this binary has one test, and it sets this before any other thread reads it.
    unsafe { std::env::set_var("BF_READER_COPY_MIN_BYTES", "0") };
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let writer = engine(&store, false);
    for batch in 0..3usize {
        let docs: Vec<_> = (0..200)
            .map(|n| {
                let id = batch * 200 + n;
                json!({"id":id,"vector":vector(id),"text":format!("alpha word{} group{}", id % 7, batch)})
            })
            .collect();
        writer
            .write(
                "t",
                serde_json::from_value::<WriteRequest>(json!({
                    "distance_metric":"euclidean_squared",
                    "schema":{"text":{"type":"string","full_text_search":true}},
                    "upsert_rows":docs}))
                .unwrap(),
            )
            .await
            .unwrap();
        writer.index_namespace("t", true).await.unwrap();
    }
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (_, copy) = wal
        .manifest_view_after(0, ManifestView::Reader)
        .await
        .unwrap()
        .unwrap();
    assert!(copy.root, "the reader copy is compact");
    assert!(
        copy.segments
            .iter()
            .flat_map(|s| s.files.values())
            .all(|f| f.text_header.is_none())
    );
    let (_, full) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(!full.root);
    assert!(
        full.segments
            .iter()
            .flat_map(|s| s.files.values())
            .any(|f| f.text_header.is_some()),
        "the slot keeps the text headers"
    );

    // Answers from a full reader (a text query first), then from a reader that starts with ANN
    // on the compact copy and then runs the text query, which upgrades its state.
    let expect_text = run(&engine(&store, false), bm25("word3")).await;
    let expect_ann: Vec<_> = {
        let e = engine(&store, false);
        let mut out = Vec::new();
        for q in [3usize, 250, 599] {
            out.push(run(&e, ann(q)).await);
        }
        out
    };
    let reader = engine(&store, false);
    let mut got = Vec::new();
    for q in [3usize, 250, 599] {
        got.push(run(&reader, ann(q)).await);
    }
    assert_eq!(got, expect_ann);
    assert_eq!(run(&reader, bm25("word3")).await, expect_text);
    assert_eq!(run(&reader, ann(250)).await, expect_ann[1]);

    // An indexer that starts from compact state still publishes the text headers.
    let both = engine(&store, false);
    run(&both, ann(3)).await;
    both.write(
        "t",
        serde_json::from_value::<WriteRequest>(json!({
            "upsert_rows":[{"id":9000,"vector":vector(9000),"text":"alpha word3 fresh"}]}))
        .unwrap(),
    )
    .await
    .unwrap();
    both.index_namespace("t", true).await.unwrap();
    let (_, after) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(
        after
            .segments
            .iter()
            .flat_map(|s| s.files.values())
            .filter(|f| f.key.contains("fts"))
            .all(|f| f.text_header.is_some()),
        "a publish from a reader that held the compact copy keeps every text header"
    );
    assert!(run(&both, bm25("fresh")).await.contains(&json!(9000)));
}
