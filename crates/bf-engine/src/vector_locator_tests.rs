use super::*;
use crate::{Engine, EngineConfig, ReclusterOptions};
use bf_types::Consistency;
use serde_json::json;

pub(super) async fn fixture() -> (Storage, Engine, NamespaceState) {
    let store = Storage::open(&bf_storage::StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    for batch in 0..2 {
        let docs: Vec<_> = (0..64)
            .map(|r| {
                let id = batch * 64 + r;
                json!({"id": id, "category": id % 3, "vector": [1.0, id as f32 / 16.0, -0.5]})
            })
            .collect();
        engine
            .write(
                "locator",
                serde_json::from_value(json!({
                    "distance_metric": "euclidean_squared",
                    "schema": {"vector": {"type": "[3]f16", "ann": true}},
                    "upsert_rows": docs
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("locator", true).await.unwrap();
    }
    // Sparse columns occur in upgrade fixtures, although current upserts require all vectors.
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let mut segments = Vec::new();
    for seg in &manifest.segments {
        let mut docs = segment::documents(&store, seg, &(0..seg.row_count).collect())
            .await
            .unwrap();
        for (id, doc) in &mut docs {
            if matches!(id, bf_types::Id::Uint(n) if n % 7 == 0) {
                doc.remove("vector");
            }
        }
        segments.push(
            segment::build_with(
                &store,
                "locator",
                &seg.schema,
                manifest.distance_metric,
                &docs,
                BTreeMap::new(),
                seg.wal_start,
                seg.wal_end,
            )
            .await
            .unwrap(),
        );
    }
    manifest.segments = segments;
    wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
        .await
        .unwrap();
    engine.drop_caches().await;
    let parent =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch/d4b-tests");
    std::fs::create_dir_all(&parent).unwrap();
    let scratch = tempfile::tempdir_in(&parent).unwrap();
    engine
        .recluster(
            "locator",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 2,
                cluster_rows: 16,
                max_slab_rows: 40,
                scratch: scratch.path().into(),
            },
        )
        .await
        .unwrap();
    let state = engine
        .snapshot("locator", &Consistency::Strong)
        .await
        .unwrap()
        .0;
    (store, engine, state)
}

#[tokio::test]
async fn locator_reads_direct_coverage_at_stored_precision_and_preserves_sparse_holes() {
    let (store, _engine, state) = fixture().await;
    let sources = Sources::new(&state);
    for seg in &state.segments {
        let rows = (0..seg.row_count).collect();
        let expected = segment::read_packed(&store, seg, &rows, "vector")
            .await
            .unwrap();
        let actual = sources
            .read(&store, seg, &rows, "vector", true)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn locator_follows_reverse_lineage_across_sources_and_rejects_uncovered_segments() {
    let (store, engine, state) = fixture().await;
    let merger = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            compaction_segments: 2,
            max_segment_rows: 256,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    merger.index_namespace("locator", true).await.unwrap();
    let merged = merger
        .snapshot("locator", &Consistency::Strong)
        .await
        .unwrap()
        .0;
    assert!(!merged.lineage.is_empty());
    let sources = Sources::new(&merged);
    for seg in &merged.segments {
        let rows = (0..seg.row_count).collect();
        assert_eq!(
            sources
                .read(&store, seg, &rows, "vector", true)
                .await
                .unwrap(),
            segment::read_packed(&store, seg, &rows, "vector")
                .await
                .unwrap()
        );
    }
    let unrelated = Sources::new(&NamespaceState::default());
    assert!(
        unrelated
            .read(
                &store,
                &state.segments[0],
                &[0].into_iter().collect(),
                "vector",
                true
            )
            .await
            .is_err()
    );
    assert!(
        engine
            .query(
                "locator",
                serde_json::from_value(json!({"rank_by": ["id", "asc"], "top_k": 1})).unwrap()
            )
            .await
            .unwrap()
            .result
            .rows
            .unwrap()[0]["id"]
            == json!(0)
    );
}
