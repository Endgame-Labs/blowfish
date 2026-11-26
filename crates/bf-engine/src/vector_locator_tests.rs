use super::*;
use crate::{Engine, EngineConfig, ReclusterOptions};
use bf_types::Consistency;
use serde_json::json;

pub(super) async fn fixture() -> (Storage, Engine, NamespaceState) {
    fixture_rows(64).await
}
async fn fixture_rows(count: usize) -> (Storage, Engine, NamespaceState) {
    fixture_rows_parts(count, 2).await
}
async fn fixture_rows_parts(count: usize, parts: usize) -> (Storage, Engine, NamespaceState) {
    fixture_rows_parts_kind(count, parts, true).await
}
async fn fixture_rows_parts_kind(
    count: usize,
    parts: usize,
    sparse: bool,
) -> (Storage, Engine, NamespaceState) {
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
    for batch in 0..parts {
        let docs: Vec<_> = (0..count)
            .map(|r| {
                let id = batch * count + r;
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
    if sparse {
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
    }
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
                max_slab_rows: if count < 100 { 40 } else { count * 2 },
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
    let mut original = BTreeMap::new();
    for seg in &state.segments {
        let (rows, bytes) =
            segment::read_packed(&store, seg, &(0..seg.row_count).collect(), "vector")
                .await
                .unwrap();
        let ids = segment::ids(&store, seg).await.unwrap();
        for (row, bytes) in rows.into_iter().zip(bytes.as_chunks::<6>().0) {
            original.insert(ids[row as usize].clone(), bytes.to_vec());
        }
    }
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
        let ids = segment::ids(&store, seg).await.unwrap();
        let mut expected_rows = Vec::new();
        let mut expected_bytes = Vec::new();
        for (row, id) in ids.iter().enumerate() {
            if let Some(bytes) = original.get(id) {
                expected_rows.push(row as u32);
                expected_bytes.extend(bytes);
            }
        }
        assert_eq!(
            sources
                .read(&store, seg, &rows, "vector", true)
                .await
                .unwrap(),
            (expected_rows, expected_bytes)
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

#[tokio::test]
async fn covered_projection_exact_search_and_materialization_keep_identical_values() {
    let (store, _engine, state) = fixture().await;
    let mut retired = state.clone();
    for seg in retired.segments.iter_mut() {
        seg.files.remove("annv2:vector");
    }
    for body in [
        json!({"rank_by": ["id", "asc"], "top_k": 128, "include_attributes": true}),
        json!({"rank_by": ["vector", "kNN", [1.0, 3.125, -0.5]], "filters": ["id", "Gte", 0], "top_k": 25, "include_attributes": true}),
        json!({"rank_by": ["vector", "kNN", [1.0, 3.125, -0.5]], "filters": ["category", "Eq", 1], "top_k": 25, "include_attributes": true}),
        json!({"rank_by": ["vector", "kNN", [0.0, 0.0, 0.0]], "filters": ["id", "Gte", 0], "top_k": 5, "include_attributes": true}),
    ] {
        let q = serde_json::from_value(body).unwrap();
        let before = crate::indexed::execute(&store, &state, &q, &EngineConfig::default(), false)
            .await
            .unwrap();
        let after = crate::indexed::execute(&store, &retired, &q, &EngineConfig::default(), false)
            .await
            .unwrap();
        assert_eq!(before.result.rows, after.result.rows);
    }
    let before = crate::snapshot::materialize(&store, &state).await.unwrap();
    let after = crate::snapshot::materialize(&store, &retired)
        .await
        .unwrap();
    assert_eq!(before.docs, after.docs);
}

#[tokio::test]
async fn patches_and_recall_sampling_read_retired_vectors_after_restart() {
    let (store, engine, _) = fixture().await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    for seg in &mut manifest.segments {
        seg.files.remove("annv2:vector");
    }
    wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
        .await
        .unwrap();
    engine.drop_caches().await;
    engine
        .write(
            "locator",
            serde_json::from_value(json!({"patch_rows": [{"id": 1, "category": 99}]})).unwrap(),
        )
        .await
        .unwrap();
    let restarted = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    let request = serde_json::from_value(json!({"rank_by": ["id", "asc"], "filters": ["id", "Eq", 1], "top_k": 1, "include_attributes": true})).unwrap();
    let rows = restarted
        .query("locator", request)
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    assert_eq!(rows[0]["category"], json!(99));
    assert_eq!(rows[0]["vector"], json!([1.0, 0.0625, -0.5]));
    let recall = restarted
        .recall(
            "locator",
            json!({"num": 3, "top_k": 3, "include_ground_truth": true}),
        )
        .await
        .unwrap();
    assert_eq!(recall["avg_recall"], json!(1.0));
}

#[tokio::test]
async fn vector_scope_inherits_the_context_at_poll_time() {
    let (_, _, state) = fixture().await;
    let cache = Arc::new(read_cache::DecodedCache::new(1 << 20, 1));
    let expected = cache.clone();
    // Construction occurs before the caller installs its real cache and admission state.
    let future = scope(&state, async move {
        let actual = read_cache::context().unwrap();
        assert!(Arc::ptr_eq(&actual.cache, &expected));
        assert!(actual.vectors.is_some());
    });
    read_cache::scope(read_cache::ReadContext::new(cache), future).await;
}

use std::collections::BTreeSet;

#[tokio::test]
async fn filtered_tiers_answer_after_lineage_deletes_and_beside_uncovered_segments() {
    let (store, engine, _) = fixture_rows(4000).await;
    engine
        .write(
            "locator",
            serde_json::from_value(json!({"deletes": (10..20).collect::<Vec<_>>()})).unwrap(),
        )
        .await
        .unwrap();
    engine.index_namespace("locator", true).await.unwrap();
    let merger = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            compaction_segments: 2,
            max_segment_rows: 16000,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    merger.index_namespace("locator", true).await.unwrap();
    let mut state = merger
        .snapshot("locator", &Consistency::Strong)
        .await
        .unwrap()
        .0;
    assert!(!state.lineage.is_empty());
    for seg in state.segments.iter_mut() {
        seg.files.remove("annv2:vector");
    }
    state.ann_dirs.clear();
    let docs = BTreeMap::from([(
        bf_types::Id::Uint(9000),
        serde_json::from_value::<bf_types::Document>(
            json!({"id": 9000, "category": 1, "vector": [1.0, 1.125, -0.5]}),
        )
        .unwrap(),
    )]);
    let rest = segment::build_with(
        &store,
        "locator",
        &state.schema,
        state.metric,
        &docs,
        BTreeMap::new(),
        3,
        3,
    )
    .await
    .unwrap();
    state.segments.push(rest);
    let context =
        read_cache::ReadContext::new(Arc::new(read_cache::DecodedCache::new(64 << 20, 2)));
    for limit in [0, 80, 800, 4000, 10_000, 80, 0] {
        let q: bf_types::QueryRequest = serde_json::from_value(json!({
            "rank_by": ["vector", "ANN", [1.0, 1.125, -0.5]], "filters": ["id", "Lt", limit],
            "top_k": 10, "include_attributes": ["id"]
        }))
        .unwrap();
        let config = EngineConfig {
            exact_fetch_rows: 128,
            ..Default::default()
        };
        let exact = crate::indexed::execute(&store, &state, &q, &config, true)
            .await
            .unwrap();
        let found = read_cache::scope(
            context.clone(),
            crate::ann::search(
                &store,
                &state,
                &q,
                "vector",
                &[1.0, 1.125, -0.5],
                &config,
                10,
                &BTreeSet::new(),
            ),
        )
        .await
        .unwrap();
        assert!(
            found.is_some(),
            "the tier planner must answer, limit={limit}"
        );
        let (scores, _) = found.unwrap();
        let expected: BTreeSet<_> = exact
            .result
            .rows
            .unwrap()
            .iter()
            .map(|r| serde_json::from_value::<bf_types::Id>(r["id"].clone()).unwrap())
            .collect();
        assert_eq!(
            scores.keys().cloned().collect::<BTreeSet<_>>(),
            expected,
            "limit={limit}"
        );
    }
    // A new immutable live mask must not reuse the earlier translated predicate.
    let mut changed = state.clone();
    let segment = &mut changed.segments[0];
    let ids = segment::ids(&store, segment).await.unwrap();
    let row = ids
        .iter()
        .position(|id| *id == bf_types::Id::Uint(20))
        .unwrap() as u32;
    let mut live = segment::live(&store, segment).await.unwrap();
    live.remove(row);
    segment::set_live(&store, "locator", segment, &live)
        .await
        .unwrap();
    let q = serde_json::from_value(json!({"rank_by": ["vector", "ANN", [1.0, 1.125, -0.5]], "filters": ["id", "Lt", 80], "top_k": 10})).unwrap();
    let config = EngineConfig {
        exact_fetch_rows: 128,
        ..Default::default()
    };
    let found = read_cache::scope(
        context,
        crate::ann::search(
            &store,
            &changed,
            &q,
            "vector",
            &[1.0, 1.125, -0.5],
            &config,
            10,
            &BTreeSet::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!found.0.contains_key(&bf_types::Id::Uint(20)));
    assert!(found.0.contains_key(&bf_types::Id::Uint(22)));
}

#[tokio::test]
async fn resident_row_map_backfill_is_bounded_preserves_ann_and_rejects_corruption() {
    let (store, _, _) = fixture().await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (_, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let before: Vec<_> = manifest.ann_tiers["vector"][0]
        .slabs
        .iter()
        .map(|s| s.files["annv2:vector"].key.clone())
        .collect();
    for slab in &mut manifest.ann_tiers.get_mut("vector").unwrap()[0].slabs {
        slab.files.remove("annrows:vector");
    }
    assert!(crate::slab_rows::needs(&manifest.ann_tiers));
    assert_eq!(
        crate::slab_rows::ensure(&store, "locator", &mut manifest, 1, None)
            .await
            .unwrap(),
        1
    );
    assert!(crate::slab_rows::needs(&manifest.ann_tiers));
    crate::slab_rows::ensure(&store, "locator", &mut manifest, usize::MAX, None)
        .await
        .unwrap();
    assert!(!crate::slab_rows::needs(&manifest.ann_tiers));
    assert_eq!(
        before,
        manifest.ann_tiers["vector"][0]
            .slabs
            .iter()
            .map(|s| s.files["annv2:vector"].key.clone())
            .collect::<Vec<_>>()
    );
    let slab = &manifest.ann_tiers["vector"][0].slabs[0];
    let map = crate::slab_rows::load(&store, slab, "vector")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(map.row_ids().unwrap().len(), slab.row_count as usize);
    assert_eq!(
        slab.files["annrows:vector"].size,
        16 + u64::from(slab.row_count) * 6
    );
    let file = &slab.files["annrows:vector"];
    let mut bytes = store.get(&file.key).await.unwrap().to_vec();
    bytes[12] ^= 1;
    store.put(&file.key, bytes).await.unwrap();
    store.flush_cache().await;
    store.drop_caches();
    assert!(
        crate::slab_rows::load(&store, slab, "vector")
            .await
            .is_err()
    );
}

async fn retire_test_sources(store: &Storage) {
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let mut removed = Vec::new();
    for segment in &mut manifest.segments {
        if let Some(file) = segment.files.remove("annv2:vector") {
            removed.push(file.key);
        }
    }
    for file in std::mem::take(&mut manifest.ann_dirs).into_values() {
        crate::indexer::retire(&mut manifest, file.key);
    }
    for key in &removed {
        crate::indexer::retire(&mut manifest, key.clone());
    }
    wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
        .await
        .unwrap();
    for key in removed {
        store.delete(&key).await.unwrap();
    }
}
fn reference_request() -> bf_types::QueryRequest {
    serde_json::from_value(
        json!({"rank_by": ["id", "asc"], "top_k": 10_000, "include_attributes": true}),
    )
    .unwrap()
}
fn maintenance_engine(store: &Storage) -> Engine {
    Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            retention: std::time::Duration::ZERO,
            compaction_segments: 2,
            max_segment_rows: 8192,
            ..Default::default()
        },
    )
}
#[tokio::test]
async fn recluster_reads_its_predecessor_after_segment_ann_objects_are_deleted() {
    let (store, engine, _) = fixture_rows(2000).await;
    let before = engine
        .query_exhaustive("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows;
    retire_test_sources(&store).await;
    let rebuilt = maintenance_engine(&store);
    let parent =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch/d4b-tests");
    let scratch = tempfile::tempdir_in(parent).unwrap();
    rebuilt
        .recluster(
            "locator",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 2,
                cluster_rows: 16,
                max_slab_rows: 8000,
                scratch: scratch.path().into(),
            },
        )
        .await
        .unwrap();
    rebuilt.collect_garbage("locator").await.unwrap();
    assert_eq!(
        maintenance_engine(&store)
            .query_exhaustive("locator", reference_request())
            .await
            .unwrap()
            .result
            .rows,
        before
    );
}
#[tokio::test]
async fn repeated_compactions_read_tiers_and_compose_lineage_without_segment_vectors() {
    let (store, engine, _) = fixture_rows_parts(2000, 3).await;
    let before = engine
        .query_exhaustive("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows;
    retire_test_sources(&store).await;
    assert!(
        maintenance_engine(&store)
            .index_namespace("locator", true)
            .await
            .unwrap()
    );
    retire_test_sources(&store).await;
    assert!(
        maintenance_engine(&store)
            .index_namespace("locator", true)
            .await
            .unwrap()
    );
    retire_test_sources(&store).await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(manifest.lineage.len(), 3);
    let final_engine = maintenance_engine(&store);
    final_engine.collect_garbage("locator").await.unwrap();
    assert_eq!(
        final_engine
            .query_exhaustive("locator", reference_request())
            .await
            .unwrap()
            .result
            .rows,
        before
    );
}

#[tokio::test]
async fn maintenance_backfills_resident_rows_without_recreating_retired_ann() {
    let (store, _, _) = fixture_rows(2000).await;
    retire_test_sources(&store).await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    for slab in manifest
        .ann_tiers
        .values_mut()
        .flatten()
        .flat_map(|s| &mut s.slabs)
    {
        slab.files.remove("annrows:vector");
    }
    wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
        .await
        .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            compaction_segments: usize::MAX,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    assert!(engine.index_namespace("locator", true).await.unwrap());
    for _ in 0..16 {
        if !engine.index_namespace("locator", true).await.unwrap() {
            break;
        }
    }
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(!crate::slab_rows::needs(&manifest.ann_tiers));
    assert!(
        manifest
            .segments
            .iter()
            .all(|s| !s.files.contains_key("annv2:vector"))
    );
    engine
        .reindex("locator", &crate::ReindexOptions::default())
        .await
        .unwrap();
    assert!(
        wal.latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1
            .segments
            .iter()
            .all(|s| !s.files.contains_key("annv2:vector"))
    );
}
#[tokio::test]
async fn statistics_and_filtered_warmup_use_covering_vectors_after_restart() {
    let (store, engine, _) = fixture_rows(2000).await;
    let request: bf_types::QueryRequest = serde_json::from_value(json!({"rank_by": ["vector", "ANN", [1.0, 13.0, -0.5]], "top_k": 10, "filters": ["category", "Eq", 1], "include_attributes": true})).unwrap();
    let expected = engine
        .query("locator", request.clone())
        .await
        .unwrap()
        .result
        .rows;
    let stats = engine.metadata("locator").await.unwrap();
    retire_test_sources(&store).await;
    let restarted = maintenance_engine(&store);
    assert_eq!(
        restarted
            .query("locator", request)
            .await
            .unwrap()
            .result
            .rows,
        expected
    );
    let actual = restarted.metadata("locator").await.unwrap();
    assert_eq!(actual["approx_row_count"], stats["approx_row_count"]);
    assert_eq!(
        actual["approx_logical_bytes"],
        stats["approx_logical_bytes"]
    );
    let state = restarted
        .snapshot("locator", &Consistency::Strong)
        .await
        .unwrap()
        .0;
    let explain = crate::plan::explain(
        &state,
        &serde_json::from_value(
            json!({"rank_by": ["vector", "ANN", [1.0, 13.0, -0.5]], "top_k": 10}),
        )
        .unwrap(),
        &EngineConfig::default(),
    );
    assert!(explain.contains("covering tier: vector"), "{explain}");
    assert!(!explain.contains("fallbacks [annv2:vector]"), "{explain}");
    let (decision, _) = crate::auto_recluster::decide(
        &state.segments,
        &state.ann_tiers,
        &state.lineage,
        Some(1),
        false,
        false,
    );
    assert_eq!(
        decision,
        crate::auto_recluster::Decision::Build("vector".into(), "masked")
    );
}

#[tokio::test]
async fn branches_and_copies_pin_covering_tiers_and_lineage_across_source_gc() {
    let (store, _, _) = fixture_rows(2000).await;
    retire_test_sources(&store).await;
    let source = maintenance_engine(&store);
    source.index_namespace("locator", true).await.unwrap();
    retire_test_sources(&store).await;
    let expected = maintenance_engine(&store)
        .query_exhaustive("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows;
    for (dest, body) in [
        ("branch", json!({"branch_from_namespace": "locator"})),
        (
            "copy",
            json!({"copy_from_namespace": {"source_namespace": "locator"}}),
        ),
        ("descendant", json!({"branch_from_namespace": "branch"})),
    ] {
        source
            .write(dest, serde_json::from_value(body).unwrap())
            .await
            .unwrap();
        // Replay the durable pin before the destination's first checkpoint.
        let restarted = maintenance_engine(&store);
        assert_eq!(
            restarted
                .query_exhaustive(dest, reference_request())
                .await
                .unwrap()
                .result
                .rows,
            expected
        );
        restarted.index_namespace(dest, true).await.unwrap();
    }
    source
        .write(
            "locator",
            serde_json::from_value(json!({"delete_by_filter": ["id", "Gte", 0]})).unwrap(),
        )
        .await
        .unwrap();
    source.index_namespace("locator", true).await.unwrap();
    source.collect_garbage("locator").await.unwrap();
    source.delete_namespace("locator").await.unwrap();
    source.collect_garbage("locator").await.unwrap();
    for dest in ["branch", "copy", "descendant"] {
        let restarted = maintenance_engine(&store);
        restarted.index_namespace(dest, true).await.unwrap();
        restarted.collect_garbage(dest).await.unwrap();
        assert_eq!(
            restarted
                .query_exhaustive(dest, reference_request())
                .await
                .unwrap()
                .result
                .rows,
            expected
        );
        let wal = bf_wal::Wal::new(store.clone(), dest).unwrap();
        let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
        assert!(!manifest.ann_tiers.is_empty());
        assert!(!manifest.lineage.is_empty());
    }
}

#[tokio::test]
async fn dropping_a_stale_tier_restores_surviving_segment_vectors_before_publication() {
    let (store, _, _) = fixture_rows(2000).await;
    retire_test_sources(&store).await;
    let before = maintenance_engine(&store)
        .query_exhaustive("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let removed = manifest.segments.remove(0);
    let gone = segment::ids(&store, &removed).await.unwrap();
    let expected: Vec<_> = before
        .into_iter()
        .filter(|r| !gone.contains(&serde_json::from_value(r["id"].clone()).unwrap()))
        .collect();
    // A stale tier can retain surviving segments even when one source disappeared.
    wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
        .await
        .unwrap();
    let engine = maintenance_engine(&store);
    engine.write("locator", serde_json::from_value(json!({"upsert_rows": [{"id": 100_000, "category": 0, "vector": [1.0, 0.0, -0.5]}]})).unwrap()).await.unwrap();
    engine.index_namespace("locator", true).await.unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(manifest.ann_tiers.is_empty());
    assert!(
        manifest
            .segments
            .iter()
            .all(|s| s.files.contains_key("annv2:vector"))
    );
    engine.collect_garbage("locator").await.unwrap();
    let mut actual = maintenance_engine(&store)
        .query_exhaustive("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    actual.retain(|r| r["id"] != 100_000);
    assert_eq!(actual, expected);
}
#[tokio::test]
async fn schema_rebuild_reads_covering_vectors_before_retiring_tiers() {
    let (store, _, _) = fixture_rows(2000).await;
    retire_test_sources(&store).await;
    let engine = maintenance_engine(&store);
    let before = engine
        .query_exhaustive("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows;
    engine
        .write(
            "locator",
            serde_json::from_value(json!({"schema": {"category": {"filterable": false}}})).unwrap(),
        )
        .await
        .unwrap();
    engine.index_namespace("locator", true).await.unwrap();
    engine.collect_garbage("locator").await.unwrap();
    assert_eq!(
        maintenance_engine(&store)
            .query_exhaustive("locator", reference_request())
            .await
            .unwrap()
            .result
            .rows,
        before
    );
}

#[tokio::test]
async fn last_tier_removal_after_compaction_restores_sparse_stored_precision() {
    let (store, _, _) = fixture_rows(2000).await;
    retire_test_sources(&store).await;
    let engine = maintenance_engine(&store);
    engine.index_namespace("locator", true).await.unwrap();
    retire_test_sources(&store).await;
    let before = maintenance_engine(&store)
        .query_exhaustive("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let sources = Sources::from_manifest(&manifest);
    manifest.ann_tiers.clear();
    crate::vector_locator::restore_uncovered(&store, "locator", &mut manifest, &sources)
        .await
        .unwrap();
    assert!(
        manifest
            .segments
            .iter()
            .all(|s| s.files.contains_key("annv2:vector"))
    );
    wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
        .await
        .unwrap();
    engine.collect_garbage("locator").await.unwrap();
    assert_eq!(
        maintenance_engine(&store)
            .query_exhaustive("locator", reference_request())
            .await
            .unwrap()
            .result
            .rows,
        before
    );
}

#[tokio::test]
async fn retirement_publishes_once_preserves_retention_and_gc_keeps_every_live_reference() {
    let (store, _, _) = fixture_rows(2000).await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (_, before) = wal.latest_manifest().await.unwrap().unwrap();
    let old: Vec<_> = before
        .segments
        .iter()
        .filter_map(|s| s.files.get("annv2:vector").cloned())
        .collect();
    assert!(
        !old.is_empty(),
        "the install pass keeps protected builder inputs until the next pass"
    );
    let retiring = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            compaction_segments: usize::MAX,
            retention: std::time::Duration::from_secs(3600),
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    retiring.index_namespace("locator", true).await.unwrap();
    let (_, retired) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(retired.format_version, bf_wal::MANIFEST_FORMAT_VERSION);
    assert!(
        retired
            .segments
            .iter()
            .all(|s| !s.files.contains_key("annv2:vector"))
    );
    retiring.collect_garbage("locator").await.unwrap();
    for file in &old {
        assert!(store.get(&file.key).await.is_ok());
    }
    let request = serde_json::from_value(json!({"rank_by": ["vector", "ANN", [1.0, 13.0, -0.5]], "top_k": 10, "include_attributes": true})).unwrap();
    let expected = retiring
        .query("locator", request)
        .await
        .unwrap()
        .result
        .rows;
    let collecting = maintenance_engine(&store);
    collecting.collect_garbage("locator").await.unwrap();
    let (_, current) = wal.latest_manifest().await.unwrap().unwrap();
    for file in current
        .segments
        .iter()
        .flat_map(|s| s.files.values().chain([&s.live]))
        .chain(current.ann_tiers.values().flatten().flat_map(|set| {
            set.slabs
                .iter()
                .flat_map(|s| s.files.values())
                .chain([&set.dir])
                .chain(set.pages.iter())
                .chain(set.coarse.iter())
        }))
        .chain(
            current
                .lineage
                .iter()
                .flat_map(|l| l.parts.iter().map(|p| &p.map)),
        )
        .chain(current.ann_dirs.values())
        .chain(current.text_stats.iter())
    {
        assert!(
            store.get(&file.key).await.is_ok(),
            "manifest object missing: {}",
            file.key
        );
    }
    for file in old {
        assert!(store.get(&file.key).await.is_err());
    }
    assert_eq!(maintenance_engine(&store).query("locator", serde_json::from_value(json!({"rank_by": ["vector", "ANN", [1.0, 13.0, -0.5]], "top_k": 10, "include_attributes": true})).unwrap()).await.unwrap().result.rows, expected);
}

#[tokio::test]
async fn gc_pins_predecessor_inputs_named_by_a_fresh_build_record() {
    let (store, _, _) = fixture_rows(2000).await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (_, before) = wal.latest_manifest().await.unwrap().unwrap();
    let old: Vec<_> = before
        .segments
        .iter()
        .map(|s| s.files["annv2:vector"].key.clone())
        .collect();
    let key = "ns/locator/indexer/build-durable-inputs-000000.json";
    store.create(key, serde_json::to_vec(&json!({"prefix": "ns/locator/slabs/slab-durable-inputs-", "started_at": chrono::Utc::now(), "references": old})).unwrap()).await.unwrap();
    retire_test_sources_without_deleting(&store).await;
    maintenance_engine(&store)
        .collect_garbage("locator")
        .await
        .unwrap();
    for key in &old {
        assert!(
            store.get(key).await.is_ok(),
            "active build input was collected"
        );
    }
    store.delete(key).await.unwrap();
    maintenance_engine(&store)
        .collect_garbage("locator")
        .await
        .unwrap();
    for key in &old {
        assert!(store.get(key).await.is_err());
    }
}
async fn retire_test_sources_without_deleting(store: &Storage) {
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    let old: Vec<_> = m
        .segments
        .iter_mut()
        .filter_map(|s| s.files.remove("annv2:vector"))
        .collect();
    for file in std::mem::take(&mut m.ann_dirs).into_values() {
        crate::indexer::retire(&mut m, file.key);
    }
    for f in old {
        crate::indexer::retire(&mut m, f.key);
    }
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
}

#[tokio::test]
async fn a_branch_first_checkpoint_compacts_inherited_coverage_through_lineage() {
    let (store, _, _) = fixture_rows(2000).await;
    retire_test_sources(&store).await;
    let engine = maintenance_engine(&store);
    let expected = engine
        .query_exhaustive("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows;
    engine
        .write(
            "first-compact",
            serde_json::from_value(json!({"branch_from_namespace": "locator"})).unwrap(),
        )
        .await
        .unwrap();
    engine.index_namespace("first-compact", true).await.unwrap();
    let m = bf_wal::Wal::new(store.clone(), "first-compact")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap()
        .1;
    assert_eq!(m.segments.len(), 1);
    assert_eq!(m.ann_tiers["vector"].len(), 1);
    assert_eq!(m.lineage.len(), 2);
    engine.collect_garbage("first-compact").await.unwrap();
    assert_eq!(
        maintenance_engine(&store)
            .query_exhaustive("first-compact", reference_request())
            .await
            .unwrap()
            .result
            .rows,
        expected
    );
}

#[tokio::test]
async fn retirement_preserves_f16_f32_and_i8_projection_and_exact_ranking() {
    for dtype in ["[3]f16", "[3]f32", "[3]i8"] {
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
                compaction_segments: usize::MAX,
                retention: std::time::Duration::ZERO,
                ..Default::default()
            },
        );
        for batch in 0..2 {
            let rows: Vec<_> = (batch * 128..(batch + 1) * 128)
                .map(|id| {
                    let vector = if dtype == "[3]i8" {
                        json!([-128, id - 128, 127])
                    } else {
                        json!([0.12345679, id as f32 / 16.0, 1.0e-10])
                    };
                    json!({"id": id, "vector": vector})
                })
                .collect();
            engine.write("precision", serde_json::from_value(json!({"distance_metric": "euclidean_squared", "schema": {"vector": {"type": dtype}}, "upsert_rows": rows})).unwrap()).await.unwrap();
            engine.index_namespace("precision", true).await.unwrap();
        }
        let order: bf_types::QueryRequest = serde_json::from_value(
            json!({"rank_by": ["id", "asc"], "top_k": 256, "include_attributes": true}),
        )
        .unwrap();
        let query = if dtype == "[3]i8" {
            json!([1, 7, -2])
        } else {
            json!([0.125, 7.25, -2.345])
        };
        let exact: bf_types::QueryRequest = serde_json::from_value(json!({"rank_by": ["vector", "kNN", query], "top_k": 20, "filters": ["id", "Gte", 0], "include_attributes": true})).unwrap();
        let expected = engine
            .query("precision", order.clone())
            .await
            .unwrap()
            .result
            .rows;
        let expected_exact = engine
            .query("precision", exact.clone())
            .await
            .unwrap()
            .result
            .rows;
        let parent =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch/d4b-tests");
        std::fs::create_dir_all(&parent).unwrap();
        let scratch = tempfile::tempdir_in(parent).unwrap();
        engine
            .recluster(
                "precision",
                &ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 2,
                    cluster_rows: 16,
                    max_slab_rows: 128,
                    scratch: scratch.path().into(),
                },
            )
            .await
            .unwrap();
        engine.index_namespace("precision", true).await.unwrap();
        engine.collect_garbage("precision").await.unwrap();
        let m = bf_wal::Wal::new(store.clone(), "precision")
            .unwrap()
            .latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1;
        assert!(
            m.segments
                .iter()
                .all(|s| !s.files.contains_key("annv2:vector")),
            "{dtype}"
        );
        let cold = Engine::with_config(
            store,
            EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                ..Default::default()
            },
        );
        assert_eq!(
            cold.query("precision", order).await.unwrap().result.rows,
            expected,
            "{dtype}"
        );
        assert_eq!(
            cold.query("precision", exact).await.unwrap().result.rows,
            expected_exact,
            "{dtype}"
        );
    }
}

#[tokio::test]
async fn covered_legacy_indexes_and_fragments_retire_without_rewriting_vector_payloads() {
    let (store, engine, _) = fixture_rows_parts_kind(4000, 2, false).await;
    let before = engine
        .query("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    let mut old_keys = Vec::new();
    for seg in &mut m.segments {
        let (rows, packed) =
            segment::read_packed(&store, seg, &(0..seg.row_count).collect(), "vector")
                .await
                .unwrap();
        let legacy = bf_vector::IndexBuilder::new(3, bf_vector::Metric::EuclideanSquared)
            .unwrap()
            .vector_type(bf_vector::VectorType::F16)
            .build_packed(rows, packed)
            .unwrap();
        seg.files.remove("annv2:vector");
        let prefix = format!(
            "ns/locator/segments/{}/legacy-{}",
            seg.id,
            uuid::Uuid::new_v4()
        );
        for blocks in [false, true] {
            let (bytes, metadata) = bf_vector::Fragment::legacy_fragment(&legacy, blocks).unwrap();
            let role = if blocks {
                "annfrag2:vector"
            } else {
                "annfrag:vector"
            };
            let file =
                segment::put_bytes(&store, format!("{prefix}.{role}"), bytes.into(), metadata)
                    .await
                    .unwrap();
            old_keys.push(file.key.clone());
            seg.files.insert(role.into(), file);
        }
        let meta = bf_vector::metadata_len(&legacy).unwrap();
        let file = segment::put_bytes(&store, format!("{prefix}.bfv"), legacy.into(), meta)
            .await
            .unwrap();
        old_keys.push(file.key.clone());
        seg.files.insert("ann:vector".into(), file);
    }
    m.ann_dirs.clear();
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
    let maintenance = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            compaction_segments: usize::MAX,
            retention: std::time::Duration::ZERO,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    assert!(maintenance.index_namespace("locator", true).await.unwrap());
    let m = wal.latest_manifest().await.unwrap().unwrap().1;
    assert!(
        m.segments
            .iter()
            .all(|s| crate::upgrade::files(s, "vector").is_none())
    );
    let report = maintenance
        .reindex("locator", &crate::ReindexOptions::default())
        .await
        .unwrap();
    assert_eq!(
        report.converted_segments, 0,
        "no redundant full ANN rewrite"
    );
    maintenance.collect_garbage("locator").await.unwrap();
    for key in old_keys {
        assert!(store.get(&key).await.is_err());
    }
    assert_eq!(
        maintenance_engine(&store)
            .query("locator", reference_request())
            .await
            .unwrap()
            .result
            .rows,
        before
    );
}
fn d4c_engine(store: &Storage) -> Engine {
    Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            compaction_segments: usize::MAX,
            retention: std::time::Duration::ZERO,
            ..Default::default()
        },
    )
}

#[tokio::test]
async fn d4c_branch_during_backfill_survives_source_recluster_and_gc() {
    let (store, _, _) = fixture_rows(2000).await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    for slab in &mut m.ann_tiers.get_mut("vector").unwrap()[0].slabs {
        let map = slab.files.remove("annrows:vector").unwrap();
        store.delete(&map.key).await.unwrap();
    }
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
    let engine = d4c_engine(&store);
    let request: bf_types::QueryRequest = serde_json::from_value(json!({
        "rank_by": ["vector", "ANN", [1.0, 13.0, -0.5]], "top_k": 10,
        "filters": ["category", "Eq", 1], "include_attributes": true
    }))
    .unwrap();
    let expected = engine
        .query("locator", request.clone())
        .await
        .unwrap()
        .result
        .rows;
    for (dest, body) in [
        ("branch", json!({"branch_from_namespace": "locator"})),
        (
            "copy",
            json!({"copy_from_namespace": {"source_namespace": "locator"}}),
        ),
    ] {
        engine
            .write(dest, serde_json::from_value(body).unwrap())
            .await
            .unwrap();
        for _ in 0..16 {
            if !engine.index_namespace(dest, true).await.unwrap() {
                break;
            }
        }
    }
    let parent = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.scratch/d4c");
    std::fs::create_dir_all(&parent).unwrap();
    let scratch = tempfile::tempdir_in(parent).unwrap();
    engine
        .recluster(
            "locator",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 2,
                cluster_rows: 16,
                max_slab_rows: 8000,
                scratch: scratch.path().into(),
            },
        )
        .await
        .unwrap();
    engine.collect_garbage("locator").await.unwrap();
    for dest in ["branch", "copy"] {
        let cold = d4c_engine(&store);
        assert_eq!(
            cold.query(dest, request.clone()).await.unwrap().result.rows,
            expected
        );
        let m = bf_wal::Wal::new(store.clone(), dest)
            .unwrap()
            .latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1;
        for slab in &m.ann_tiers["vector"][0].slabs {
            let map = &slab.files["annrows:vector"];
            assert!(
                map.key.starts_with(&format!("ns/{dest}/slabs/")),
                "{}",
                map.key
            );
            store.get(&map.key).await.unwrap();
        }
        cold.write(
            dest,
            serde_json::from_value(json!({"patch_rows": [{"id": 10, "category": 9}]})).unwrap(),
        )
        .await
        .unwrap();
        let patched: bf_types::QueryRequest = serde_json::from_value(json!({
            "rank_by": ["id", "asc"], "filters": ["id", "Eq", 10], "top_k": 1, "include_attributes": true
        })).unwrap();
        let rows = cold
            .query(dest, patched)
            .await
            .unwrap()
            .result
            .rows
            .unwrap();
        assert_eq!(rows[0]["category"], 9);
        assert_eq!(
            bf_types::decode_vector(&rows[0]["vector"]).unwrap(),
            vec![1.0, 0.625, -0.5]
        );
        cold.index_namespace(dest, true).await.unwrap();
    }
}

#[tokio::test]
async fn d4c_missing_published_map_recovers_queries_and_repairs_without_a_tail() {
    let (store, _, _) = fixture_rows(2000).await;
    retire_test_sources(&store).await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
    let map = &m.ann_tiers["vector"][0].slabs[0].files["annrows:vector"];
    store.delete(&map.key).await.unwrap();
    store.drop_caches();
    let engine = d4c_engine(&store);
    engine.query("locator", reference_request()).await.unwrap();
    assert!(
        engine.index_namespace("locator", true).await.unwrap(),
        "missing objects must schedule repair without new writes"
    );
    let m = wal.latest_manifest().await.unwrap().unwrap().1;
    for slab in &m.ann_tiers["vector"][0].slabs {
        store.get(&slab.files["annrows:vector"].key).await.unwrap();
    }
    engine.query("locator", reference_request()).await.unwrap();
}

#[tokio::test]
async fn d4c_document_only_legacy_segment_does_not_wedge_publication() {
    let (store, _, _) = fixture_rows(2000).await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    m.ann_tiers.clear();
    m.ann_dirs.clear();
    for seg in &mut m.segments {
        segment::upgrade::make_legacy(&store, seg).await.unwrap();
        seg.files.remove("annv2:vector");
    }
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
    let engine = d4c_engine(&store);
    let before = engine
        .query("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows;
    engine.write("locator", serde_json::from_value(json!({"upsert_rows": [{"id": 100_000, "category": 0, "vector": [1.0, 0.0, -0.5]}]})).unwrap()).await.unwrap();
    assert!(engine.index_namespace("locator", true).await.unwrap());
    let m = wal.latest_manifest().await.unwrap().unwrap().1;
    assert_eq!(
        m.indexed_through,
        m.segments.iter().map(|s| s.wal_end).max().unwrap()
    );
    let mut after = engine
        .query("locator", reference_request())
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    after.retain(|r| r["id"] != 100_000);
    assert_eq!(Some(after), before);
}

#[tokio::test]
async fn d4c_legacy_slab_backfill_is_automatic_and_bounded() {
    let (store, _, _) = fixture_rows(2000).await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    let set = &mut m.ann_tiers.get_mut("vector").unwrap()[0];
    let mut originals = Vec::new();
    for slab in &mut set.slabs {
        let current = slab.files.remove("annv2:vector").unwrap();
        slab.files.remove("annrows:vector");
        let bytes = store.get(&current.key).await.unwrap();
        let index = bf_vector::Index::open_with_metadata_len(bytes, current.metadata_len)
            .await
            .unwrap();
        let rows: RoaringBitmap = index.row_ids().into_iter().collect();
        let (rows, packed) = index.read_packed_rows(&rows).await.unwrap();
        let legacy = bf_vector::IndexBuilder::new(3, bf_vector::Metric::EuclideanSquared)
            .unwrap()
            .vector_type(bf_vector::VectorType::F16)
            .build_packed(rows, packed)
            .unwrap();
        let legacy = bf_vector::downgrade_layout(&legacy)
            .unwrap()
            .unwrap_or(legacy);
        let metadata = bf_vector::metadata_len(&legacy).unwrap();
        let file = segment::put_file(
            &store,
            format!("ns/locator/slabs/{}/legacy.bfv", slab.id),
            legacy.clone(),
            metadata,
        )
        .await
        .unwrap();
        originals.push((file.key.clone(), legacy));
        slab.files.insert("ann:vector".into(), file);
    }
    set.dir = crate::ann_dir::build(
        &store,
        "locator",
        &set.slabs,
        "vector",
        crate::ann_dir::Layout::Slabs,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    set.pages = None;
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
    assert!(
        crate::slab_rows::needs(&m.ann_tiers),
        "legacy slabs must schedule row-map backfill"
    );
    let engine = d4c_engine(&store);
    assert!(engine.index_namespace("locator", true).await.unwrap());
    let m = wal.latest_manifest().await.unwrap().unwrap().1;
    assert_eq!(
        m.ann_tiers["vector"][0]
            .slabs
            .iter()
            .filter(|s| s.files.contains_key("annrows:vector"))
            .count(),
        1
    );
    for _ in 0..16 {
        if !engine.index_namespace("locator", true).await.unwrap() {
            break;
        }
    }
    let m = wal.latest_manifest().await.unwrap().unwrap().1;
    assert!(!crate::slab_rows::needs(&m.ann_tiers));
    for (key, bytes) in originals {
        assert_eq!(store.get(&key).await.unwrap().as_ref(), bytes);
    }
    assert!(
        m.segments
            .iter()
            .all(|s| crate::upgrade::files(s, "vector").is_none())
    );
    engine.query("locator", reference_request()).await.unwrap();
}

#[tokio::test]
async fn d4c_filtered_complete_tier_rebuilds_a_missing_or_stale_directory() {
    let (store, _, _) = fixture_rows(2000).await;
    retire_test_sources(&store).await;
    let engine = d4c_engine(&store);
    let request: bf_types::QueryRequest = serde_json::from_value(json!({
        "rank_by": ["vector", "ANN", [1.0, 13.0, -0.5]], "top_k": 10,
        "filters": ["category", "Eq", 1], "include_attributes": true
    }))
    .unwrap();
    let state = engine
        .snapshot("locator", &Consistency::Strong)
        .await
        .unwrap()
        .0;
    let expected = d4c_ann(&store, &engine, &state, &request).await.unwrap();
    for stale in [false, true] {
        let mut changed = state.clone();
        let set = &mut Arc::make_mut(&mut changed.ann_tiers)
            .get_mut("vector")
            .unwrap()[0];
        if stale {
            let mut slabs = set.slabs.clone();
            slabs[0].id = "stale-slab".into();
            set.dir = crate::ann_dir::build(
                &store,
                "locator",
                &slabs,
                "vector",
                crate::ann_dir::Layout::Slabs,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        } else {
            set.dir.key = "ns/locator/slabs/missing-directory.bfd".into();
        }
        assert_eq!(
            d4c_ann(&store, &engine, &changed, &request).await,
            Some(expected.clone()),
            "a complete filtered tier must answer through ANN, stale={stale}"
        );
    }
}

#[tokio::test]
async fn d4c_restored_ann_has_the_same_recipe_as_a_new_segment() {
    let (store, _, _) = fixture_rows_parts_kind(3000, 1, false).await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (_, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    let original = store
        .get(&m.segments[0].files["annv2:vector"].key)
        .await
        .unwrap();
    m.segments[0].files.remove("annv2:vector");
    let sources = Sources::from_manifest(&m);
    m.ann_tiers.clear();
    restore_uncovered(&store, "locator", &mut m, &sources)
        .await
        .unwrap();
    let restored = store
        .get(&m.segments[0].files["annv2:vector"].key)
        .await
        .unwrap();
    assert_eq!(
        restored, original,
        "identical rows and stored precision must produce the same ANN bytes"
    );
}

async fn d4c_ann(
    store: &Storage,
    engine: &Engine,
    state: &NamespaceState,
    request: &bf_types::QueryRequest,
) -> Option<(
    BTreeMap<bf_types::Id, f64>,
    BTreeMap<bf_types::Id, bf_types::Document>,
)> {
    crate::ann::search(
        store,
        state,
        request,
        "vector",
        &[1.0, 13.0, -0.5],
        &engine.0.config,
        10,
        &std::collections::BTreeSet::new(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn d4c_current_documents_without_any_vector_owner_still_fail_closed() {
    let (store, _, _) = fixture().await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (_, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    manifest.ann_tiers.clear();
    for seg in &mut manifest.segments {
        assert!(!segment::upgrade::needs_upgrade(&store, seg).await.unwrap());
        seg.files.remove("annv2:vector");
    }
    let sources = Sources::from_manifest(&manifest);
    let error = restore_uncovered(&store, "locator", &mut manifest, &sources)
        .await
        .unwrap_err();
    assert_eq!(error.status, 503);
}

#[tokio::test]
async fn d4c_row_map_survey_is_bounded_and_rotates_to_missing_objects() {
    let (store, _, _) = fixture().await;
    let wal = bf_wal::Wal::new(store.clone(), "locator").unwrap();
    let (_, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let slabs = &manifest.ann_tiers["vector"][0].slabs;
    assert!(slabs.len() > 1);
    let last = slabs.len() - 1;
    let missing = slabs[last].files["annrows:vector"].key.clone();
    store.delete(&missing).await.unwrap();
    for probe in 0..last {
        assert!(
            crate::slab_rows::missing(&store, &manifest.ann_tiers, probe)
                .await
                .unwrap()
                .is_none(),
            "a healthy probe must not scan ahead to another slab"
        );
    }
    assert_eq!(
        crate::slab_rows::missing(&store, &manifest.ann_tiers, last)
            .await
            .unwrap(),
        Some(missing.clone())
    );
    assert_eq!(
        crate::slab_rows::ensure(&store, "locator", &mut manifest, 1, Some(&missing))
            .await
            .unwrap(),
        1
    );
    for slab in &manifest.ann_tiers["vector"][0].slabs {
        store.get(&slab.files["annrows:vector"].key).await.unwrap();
    }
}
