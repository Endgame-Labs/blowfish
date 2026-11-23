//! A future manifest or marker must prevent every namespace mutator from acting.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_wal::{MANIFEST_FORMAT_VERSION, ManifestView, Wal};
use serde_json::{Value, json};

async fn setup() -> (tempfile::TempDir, Storage, Engine, Wal) {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", dir.path().display()),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            retention: std::time::Duration::ZERO,
            ..Default::default()
        },
    );
    engine
        .write(
            "test",
            serde_json::from_value(json!({"upsert_rows":[{"id":1,"n":7}]})).unwrap(),
        )
        .await
        .unwrap();
    engine.index_namespace("test", true).await.unwrap();
    let wal = Wal::new(store.clone(), "test").unwrap();
    (dir, store, engine, wal)
}

async fn inventory(store: &Storage) -> Vec<(String, Vec<u8>)> {
    let mut result = vec![];
    for object in store.list("").await.unwrap() {
        result.push((
            object.key.clone(),
            store.get(&object.key).await.unwrap().to_vec(),
        ));
    }
    result
}

#[tokio::test]
async fn future_manifest_fences_readers_writers_indexer_compaction_gc_and_tools() {
    let (_dir, store, engine, wal) = setup().await;
    let (seq, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let mut future = serde_json::to_value(&manifest).unwrap();
    future["format_version"] = json!(MANIFEST_FORMAT_VERSION + 1);
    future["future_objects"] = json!(["ns/test/segments/future-only"]);
    store
        .create("ns/test/segments/future-only", "must survive GC")
        .await
        .unwrap();
    let next = seq + 1;
    store
        .create(
            &format!("ns/test/manifest/{next:020}.json"),
            serde_json::to_vec(&future).unwrap(),
        )
        .await
        .unwrap();
    // Both the matching compact copy and the full manifest must fail closed.
    let mut copy = next.to_be_bytes().to_vec();
    copy.extend(serde_json::to_vec(&future).unwrap());
    store.put("ns/test/manifest-latest", copy).await.unwrap();
    engine.drop_caches().await;
    let before = inventory(&store).await;
    for view in [ManifestView::Full, ManifestView::Reader] {
        let error = wal.manifest_view_after(0, view).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported manifest format version 3")
        );
    }
    assert!(wal.manifest_after(seq).await.is_err());
    assert!(wal.publish_manifest(next + 1, &manifest).await.is_err());
    assert!(
        wal.publish_fenced(next, manifest.indexer_epoch, &manifest)
            .await
            .is_err()
    );
    assert!(wal.write_reader_copy().await.is_err());
    assert!(wal.prune_payloads(1).await.is_err());
    assert!(
        engine
            .write(
                "test",
                serde_json::from_value(json!({"upsert_rows":[{"id":2}]})).unwrap()
            )
            .await
            .is_err()
    );
    assert!(
        engine
            .query(
                "test",
                serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":1})).unwrap()
            )
            .await
            .is_err()
    );
    assert!(engine.index_namespace("test", true).await.is_err());
    assert!(engine.collect_garbage("test").await.is_err());
    assert!(
        engine
            .recluster(
                "test",
                &bf_engine::ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 1,
                    cluster_rows: 1,
                    max_slab_rows: 1,
                    scratch: _dir.path().to_owned(),
                }
            )
            .await
            .is_err()
    );
    assert_eq!(inventory(&store).await, before);
    // No fixed-key copy: the immutable slot and legacy payload indirection are checked too.
    store.delete("ns/test/manifest-latest").await.unwrap();
    assert!(wal.latest_manifest().await.is_err());
    store
        .put(
            "ns/test/manifest-data/future",
            serde_json::to_vec(&future).unwrap(),
        )
        .await
        .unwrap();
    store
        .put(
            &format!("ns/test/manifest/{next:020}.json"),
            br#"{"payload":"ns/test/manifest-data/future"}"#.to_vec(),
        )
        .await
        .unwrap();
    assert!(wal.latest_manifest().await.is_err());
}

#[tokio::test]
async fn a_newer_root_marker_fences_running_and_project_scoped_mutators() {
    let (_dir, store, engine, wal) = setup().await;
    let (seq, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let entry = bf_wal::WalEntry::new(vec![]);
    let staged = wal.stage(2, &entry).await.unwrap();
    store.create("_system/format/3.json", "{}").await.unwrap();
    let before = inventory(&store).await;
    assert!(
        engine
            .collect_garbage("test")
            .await
            .unwrap_err()
            .message
            .contains("version 3")
    );
    assert!(
        engine
            .scoped("projects/test")
            .collect_garbage("missing")
            .await
            .is_err()
    );
    assert!(engine.index_namespace("test", true).await.is_err());
    assert!(
        engine
            .write(
                "other",
                serde_json::from_value(json!({"upsert_rows":[{"id":1}]})).unwrap()
            )
            .await
            .is_err()
    );
    assert!(wal.append(2, &bf_wal::WalEntry::new(vec![])).await.is_err());
    assert!(wal.commit(2, &entry, &staged).await.is_err());
    assert!(wal.publish_manifest(seq + 1, &manifest).await.is_err());
    assert!(wal.prune_payloads(1).await.is_err());
    assert_eq!(inventory(&store).await, before);
}

#[tokio::test]
async fn legacy_manifest_upgrades_on_publication_without_losing_state() {
    let (_dir, store, engine, wal) = setup().await;
    let (seq, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let mut legacy = serde_json::to_value(&manifest).unwrap();
    legacy["format_version"] = json!(1);
    legacy["indexed_through"] = legacy
        .as_object_mut()
        .unwrap()
        .remove("checkpoint_wal")
        .unwrap();
    store
        .put(
            &format!("ns/test/manifest/{seq:020}.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .await
        .unwrap();
    store.delete("ns/test/manifest-latest").await.unwrap();
    store.delete("_system/format/2.json").await.unwrap();
    let legacy = wal.latest_manifest().await.unwrap().unwrap().1;
    assert_eq!(legacy.format_version, 1);
    wal.publish_fenced(seq, legacy.indexer_epoch, &legacy)
        .await
        .unwrap();
    assert_eq!(store.check_bucket_format().await.unwrap(), 2);
    let (_, upgraded) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(upgraded.format_version, MANIFEST_FORMAT_VERSION);
    assert_eq!(upgraded.indexed_through, manifest.indexed_through);
    assert_eq!(upgraded.stats.rows, 1);
    let wire: Value = serde_json::from_slice(
        &store
            .get(&format!("ns/test/manifest/{:020}.json", seq + 1))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(wire.get("indexed_through").is_none());
    assert_eq!(wire["checkpoint_wal"], json!(manifest.indexed_through));
    engine.drop_caches().await;
    let result = engine
        .query(
            "test",
            serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":1})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result.result.rows.unwrap()[0]["id"], json!(1));
}
