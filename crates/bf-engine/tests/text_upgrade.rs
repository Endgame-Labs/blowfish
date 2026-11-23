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
async fn text_postings_are_automatically_rebundled_and_retired() {
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
    let expected_bytes = store
        .get(&manifest.segments[0].files["fts:text"].key)
        .await
        .unwrap();
    let mut builder = bf_text::SegmentBuilder::new(true);
    for value in [Some("fox rare fox"), Some("fox dog"), None, Some("")] {
        if let Some(value) = value {
            builder
                .add_document(&bf_text::tokenize(value, &Default::default()))
                .unwrap();
        } else {
            builder.add_missing_document().unwrap();
        }
    }
    let bytes = builder.finish_colocated_fixture().unwrap();
    let mut file = manifest.segments[0].files["fts:text"].clone();
    file.key = "ns/text-upgrade/fixture-v4.bft".into();
    file.size = bytes.len() as u64;
    file.metadata_len = 0;
    store.create_unique(&file.key, bytes).await.unwrap();
    manifest.segments[0].files.insert("fts:text".into(), file);
    let old = manifest.segments[0].files["fts:text"].key.clone();
    // Missing manifest headers also occur for current bytes. Both cases avoid document reads.
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
    assert!(
        upgrader
            .index_namespace("text-upgrade", false)
            .await
            .unwrap()
    );
    let (_, after) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(
        old_keys
            .iter()
            .filter(|(key, old)| after.segments[0].files[key].key != *old)
            .count(),
        old_keys.len()
    );
    assert_eq!(
        store
            .get(&after.segments[0].files["fts:text"].key)
            .await
            .unwrap(),
        expected_bytes
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_resume_without_document_reads_or_repeated_id_backfills() {
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
        max_segment_rows: 1,
        compaction_segments: usize::MAX,
        ..Default::default()
    };
    let engine = Engine::with_config(store.clone(), config.clone());
    let rows: Vec<_> = (0..40)
        .map(|id| json!({"id":id,"text":"fish water"}))
        .collect();
    engine.write("text-upgrade",serde_json::from_value(json!({"schema":{"text":{"type":"string","full_text_search":true}},"upsert_rows":rows})).unwrap()).await.unwrap();
    engine.index_namespace("text-upgrade", true).await.unwrap();
    let q: QueryRequest =
        serde_json::from_value(json!({"rank_by":["text","BM25","fish"],"top_k":10})).unwrap();
    let expected = engine
        .query("text-upgrade", q.clone())
        .await
        .unwrap()
        .result
        .rows;
    let wal = Wal::new(store.clone(), "text-upgrade").unwrap();
    let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(m.segments.len(), 40);
    for s in &mut m.segments {
        s.files.get_mut("fts:text").unwrap().text_header = None;
        s.files.remove("text_keys");
        // A rebundle and ID backfill must use postings and IDs, even with documents unavailable.
        store.delete(&s.files["docs"].key).await.unwrap();
    }
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
    let upgrader = Engine::with_config(store.clone(), config);
    for completed in [32, 40] {
        assert!(
            upgrader
                .index_namespace("text-upgrade", false)
                .await
                .unwrap()
        );
        let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
        assert_eq!(
            m.segments
                .iter()
                .filter(|s| s.files["fts:text"].text_header.is_some())
                .count(),
            completed
        );
        assert_eq!(
            m.segments
                .iter()
                .filter(|s| s.files.contains_key("text_keys"))
                .count(),
            completed
        );
    }
    assert!(
        !upgrader
            .index_namespace("text-upgrade", false)
            .await
            .unwrap()
    );
    assert_eq!(
        upgrader.query("text-upgrade", q).await.unwrap().result.rows,
        expected
    );
}
