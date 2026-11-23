//! Text layout upgrades use the normal indexer's fenced publication, and preserve
//! the immutable row ordinals, live masks, missing fields, and namespace statistics.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use bf_wal::Wal;
use serde_json::json;

async fn query(engine: &Engine) -> Vec<bf_types::Document> {
    engine
        .query(
            "text-upgrade",
            serde_json::from_value::<QueryRequest>(json!({
                "rank_by":["text","BM25","fox rare"], "top_k":10,
                "include_attributes":true
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .result
        .rows
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_text_headers_are_automatically_rebuilt_and_retired() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let config = EngineConfig {
        indexing: false,
        lease_duration: std::time::Duration::ZERO,
        recall_sample_rate: 0.0,
        ..Default::default()
    };
    let engine = Engine::with_config(store.clone(), config.clone());
    engine.write("text-upgrade", serde_json::from_value::<WriteRequest>(json!({
        "schema":{"text":{"type":"string","full_text_search":true},"title":{"type":"string","full_text_search":true}},
        "upsert_rows":[{"id":1,"text":"fox rare fox","title":"a fox"},{"id":2,"text":"fox dog"},{"id":3},{"id":4,"text":""}]
    })).unwrap()).await.unwrap();
    engine.index_namespace("text-upgrade", true).await.unwrap();
    engine
        .write(
            "text-upgrade",
            serde_json::from_value::<WriteRequest>(json!({"deletes":[2]})).unwrap(),
        )
        .await
        .unwrap();
    engine.index_namespace("text-upgrade", true).await.unwrap();
    let expected = query(&engine).await;
    let wal = Wal::new(store.clone(), "text-upgrade").unwrap();
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let before_stats = manifest.text_stats.clone();
    let old = manifest.segments[0].files["fts:text"].key.clone();
    // A missing header is how older text formats appear in a manifest. Rebuilding
    // from the document column also handles formats that cannot be converted in place.
    let old_keys: Vec<_> = manifest.segments[0]
        .files
        .iter()
        .filter(|(key, _)| key.starts_with("fts:"))
        .map(|(key, file)| (key.clone(), file.key.clone()))
        .collect();
    for (key, _) in &old_keys {
        manifest.segments[0].files.get_mut(key).unwrap().text_header = None;
    }
    wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
        .await
        .unwrap();
    let upgrader = Engine::with_config(store.clone(), config);
    for completed in 1..=old_keys.len() {
        assert!(
            upgrader
                .index_namespace("text-upgrade", false)
                .await
                .unwrap()
        );
        let (_, current) = wal.latest_manifest().await.unwrap().unwrap();
        let upgraded = old_keys
            .iter()
            .filter(|(key, old)| current.segments[0].files[key].key != *old)
            .count();
        assert_eq!(
            upgraded, completed,
            "each pass must adopt the previous text generation"
        );
    }
    let (_, after) = wal.latest_manifest().await.unwrap().unwrap();
    assert_ne!(after.segments[0].files["fts:text"].key, old);
    assert!(after.segments[0].files["fts:text"].text_header.is_some());
    assert!(after.retired.iter().any(|f| f.key == old));
    assert_eq!(
        serde_json::to_value(after.text_stats).unwrap(),
        serde_json::to_value(before_stats).unwrap()
    );
    assert_eq!(query(&upgrader).await, expected);
    assert!(
        !upgrader
            .index_namespace("text-upgrade", true)
            .await
            .unwrap()
    );
}
