//! Document conversion must preserve indexed queries at every durable checkpoint.
use super::*;
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use serde_json::{Value, json};

fn vector(id: u32) -> Vec<f32> {
    (0..32)
        .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
        .collect()
}
async fn setup() -> (Storage, Engine, Wal) {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    setup_with(store).await
}
async fn setup_with(store: Storage) -> (Storage, Engine, Wal) {
    let engine = Engine::with_config(
        store.clone(),
        crate::EngineConfig {
            indexing: false,
            ann_nprobe: 4,
            ann_rerank_factor: 8,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    for batch in 0..2 {
        let write: WriteRequest = serde_json::from_value(json!({
            "distance_metric":"cosine_distance", "schema":{
                "vector":{"type":"[32]f16","ann":true},
                "bucket":{"type":"uint","filterable":true},
                "text":{"type":"string","full_text_search":true}
            }, "upsert_rows":(batch*3000..(batch+1)*3000).map(|id|json!({"id":id,"vector":vector(id),"bucket":id%4,"text":format!("fish, water, and café {}",id%5)})).collect::<Vec<_>>()
        })).unwrap();
        engine.write("t", write).await.unwrap();
        engine.index_namespace("t", true).await.unwrap();
    }
    let wal = Wal::new(store.clone(), "t").unwrap();
    (store, engine, wal)
}
fn queries() -> Vec<QueryRequest> {
    [
        json!({"rank_by":["vector","ANN",vector(23)],"include_attributes":["vector"],"top_k":10}),
        json!({"rank_by":["vector","ANN",vector(23)],"filters":["bucket","Eq",1],"include_attributes":["vector","text"],"vector_encoding":"base64","top_k":10}),
        json!({"rank_by":["vector","kNN",vector(23)],"filters":["bucket","Gte",0],"include_attributes":["vector"],"top_k":10}),
        json!({"rank_by":["id","asc"],"include_attributes":true,"top_k":10}),
        json!({"rank_by":["text","BM25","fish"],"include_attributes":["vector","text"],"top_k":10})
    ].into_iter().map(|v|serde_json::from_value(v).unwrap()).collect()
}
async fn answers(engine: &Engine) -> Vec<(Vec<u8>, u64)> {
    let mut found = Vec::new();
    for (i, query) in queries().into_iter().enumerate() {
        let (answer, diagnostic) = engine.query_with_diagnostics("t", query).await.unwrap();
        if i < 2 {
            assert_eq!(diagnostic.document_scan_rows, 0);
            assert!(diagnostic.ann_rows_scanned > 0 && diagnostic.ann_rows_scanned < 6000);
        }
        found.push((
            serde_json::to_vec(&answer.result.rows).unwrap(),
            diagnostic.ann_rows_scanned,
        ));
    }
    found
}

#[tokio::test]
async fn mixed_document_checkpoints_keep_indexes_and_wire_bytes() {
    let (store, engine, wal) = setup().await;
    let before = answers(&engine).await;
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(manifest.segments.len(), 2);
    for seg in &mut manifest.segments {
        segment::upgrade::make_legacy(&store, seg).await.unwrap();
    }
    wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
        .await
        .unwrap();
    assert_eq!(answers(&engine).await, before, "0% converted");
    let dry = engine
        .reindex(
            "t",
            &ReindexOptions {
                dry_run: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(dry.documents.0, 2);
    assert_eq!(dry.publications, 0);
    for stage in 0..2 {
        let (_, snapshot) = wal.latest_manifest().await.unwrap().unwrap();
        let seg = &snapshot.segments[stage];
        let converted = convert_documents(&store, "t", seg, None).await.unwrap();
        let mut report = ReindexReport::default();
        engine
            .publish_batch(&wal, "t", Batch::Documents(vec![converted]), &mut report)
            .await
            .unwrap();
        assert_eq!(report.converted_documents, 1);
        let (_, latest) = wal.latest_manifest().await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(&latest.ann_dirs).unwrap(),
            serde_json::to_value(&snapshot.ann_dirs).unwrap()
        );
        assert_eq!(latest.indexed_through, snapshot.indexed_through);
        assert_eq!(
            answers(&engine).await,
            before,
            "{}% converted",
            (stage + 1) * 50
        );
        for f in &latest.retired {
            assert!(store.head(&f.key).await.is_ok());
        }
    }
    let again = engine.reindex("t", &Default::default()).await.unwrap();
    assert_eq!(again.documents.0, 0);
    assert_eq!(again.converted_documents, 0);
    assert_eq!(again.bytes_written, 0);
    assert_eq!(again.publications, 0);
}

#[tokio::test]
async fn stale_document_conversion_cannot_replace_a_compaction_or_another_upgrade() {
    let (store, _, wal) = setup().await;
    let (_, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    segment::upgrade::make_legacy(&store, &mut m.segments[0])
        .await
        .unwrap();
    let first = convert_documents(&store, "t", &m.segments[0], None)
        .await
        .unwrap();
    let second = convert_documents(&store, "t", &m.segments[0], None)
        .await
        .unwrap();
    assert_eq!(apply_documents(&mut m, &[first]).0, 1);
    let expected: Value = serde_json::to_value(&m.segments).unwrap();
    assert_eq!(apply_documents(&mut m, &[second]).0, 0);
    assert_eq!(serde_json::to_value(&m.segments).unwrap(), expected);
}

#[tokio::test]
async fn background_indexer_upgrades_documents_without_a_write() {
    let (store, engine, wal) = setup().await;
    let before = answers(&engine).await;
    let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    for seg in &mut m.segments {
        segment::upgrade::make_legacy(&store, seg).await.unwrap();
    }
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
    let reader = Engine::with_config(
        store.clone(),
        crate::EngineConfig {
            index_interval: std::time::Duration::from_millis(20),
            ann_nprobe: 4,
            ann_rerank_factor: 8,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
            let mut pending = false;
            for seg in &m.segments {
                pending |= segment::upgrade::needs_upgrade(&store, seg).await.unwrap();
            }
            if !pending {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("automatic document upgrade");
    assert_eq!(answers(&reader).await, before);
}

#[path = "document_fault_tests.rs"]
mod fault_tests;
