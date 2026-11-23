//! The layout upgrade rewrites older (row-major) vector indexes in the fast-scan layout: every
//! index changes layout, queries answer the same, a second run changes nothing, a write racing a
//! run is kept, and the background indexer does it by itself. The older files are made in test
//! code (`downgrade_layout`): no release build writes them.
use bf_engine::{Engine, EngineConfig, ReclusterOptions, ReindexOptions};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use bf_vector::CodeLayout;
use bf_wal::{SegmentFile, Wal};
use serde_json::{Value, json};

fn vector(id: usize) -> Vec<f32> {
    (0..64)
        .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
        .collect()
}
fn engine(store: &Storage) -> Engine {
    Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    )
}
async fn ids(e: &Engine, q: usize) -> Vec<Value> {
    let request: QueryRequest =
        serde_json::from_value(json!({"rank_by":["vector","ANN",vector(q)],"top_k":10})).unwrap();
    let rows = e.query("t", request).await.unwrap().result.rows.unwrap();
    rows.iter().map(|r| r["id"].clone()).collect()
}
async fn write(e: &Engine, body: Value) {
    e.write("t", serde_json::from_value::<WriteRequest>(body).unwrap())
        .await
        .unwrap();
}
async fn answers(store: &Storage, queries: &[usize]) -> Vec<Vec<Value>> {
    let mut out = Vec::new();
    for q in queries {
        out.push(ids(&engine(store), *q).await);
    }
    out
}
/// Every index's layout in the latest manifest: segments, then slabs.
async fn layouts(store: &Storage) -> (Vec<CodeLayout>, Vec<CodeLayout>) {
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let mut seg = Vec::new();
    for s in &manifest.segments {
        let f = s
            .files
            .get("annv2:vector")
            .or_else(|| s.files.get("ann:vector"))
            .unwrap();
        let head = store.get_range(&f.key, 0..64).await.unwrap();
        seg.push(bf_vector::code_layout(&head).unwrap());
    }
    let mut slabs = Vec::new();
    if let Some(set) = manifest.ann_tiers.get("vector").and_then(|t| t.first()) {
        for s in &set.slabs {
            let f = s
                .files
                .get("annv2:vector")
                .or_else(|| s.files.get("ann:vector"))
                .unwrap();
            let head = store.get_range(&f.key, 0..64).await.unwrap();
            slabs.push(bf_vector::code_layout(&head).unwrap());
        }
    }
    (seg, slabs)
}
/// Rewrite every vector index (segments and slabs) in the older layout, as an older release
/// left them, and drop the directories that listed the replaced objects.
async fn downgrade(store: &Storage) {
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    let mut slabs = m.ann_tiers.get("vector").map(|t| t[0].slabs.clone());
    let mut segments = std::mem::take(&mut m.segments);
    for seg in segments.iter_mut().chain(slabs.iter_mut().flatten()) {
        let old = seg.files["annv2:vector"].clone();
        let blob = store.get(&old.key).await.unwrap();
        let Some(blob) = bf_vector::downgrade_layout(&blob).unwrap() else {
            continue;
        };

        seg.files.remove("annv2:vector");
        let meta = bf_vector::metadata_len(&blob).unwrap();
        let put = async |name: &str, data: Vec<u8>, meta: u64| {
            let key = format!("ns/t/test/{}.{name}", uuid::Uuid::new_v4());
            let file = SegmentFile {
                key: key.clone(),
                size: data.len() as u64,
                metadata_len: meta,
                stats_index: None,
                text_header: None,
            };
            store.create_unique(&key, data).await.unwrap();
            file
        };
        // Real legacy fragments must keep serving while the rewrite proceeds.
        for (name, ext, blocks) in [
            ("annfrag:vector", "bff", false),
            ("annfrag2:vector", "bfb", true),
        ] {
            if !blocks && seg.files.is_empty() {
                continue;
            }
            let (fragment, resident) = bf_vector::Fragment::legacy_fragment(&blob, blocks).unwrap();
            seg.files
                .insert(name.into(), put(ext, fragment, resident).await);
        }
        seg.files
            .insert("ann:vector".into(), put("bfv", blob, meta).await);
    }
    m.segments = segments;
    if let Some(slabs) = slabs {
        m.ann_tiers.get_mut("vector").unwrap()[0].slabs = slabs;
    }
    m.ann_dirs.clear();
    let epoch = m.indexer_epoch;
    wal.publish_fenced(seq, epoch, &m).await.unwrap();
}
fn options() -> ReindexOptions {
    ReindexOptions {
        batch_indexes: 1,
        ..Default::default()
    }
}
async fn build(store: &Storage, writer: &Engine) {
    for batch in 0..3usize {
        let docs: Vec<_> = (0..3000)
            .map(|n| json!({"id":batch * 3000 + n,"vector":vector(batch * 3000 + n)}))
            .collect();
        write(
            writer,
            json!({"distance_metric":"euclidean_squared","upsert_rows":docs}),
        )
        .await;
        writer.index_namespace("t", true).await.unwrap();
    }
    let (seg, _) = layouts(store).await;
    assert_eq!(
        seg,
        vec![CodeLayout::FastScan; 3],
        "new indexes are fast-scan"
    );
}
async fn scratch_store() -> (tempfile::TempDir, Storage) {
    let scratch = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    let dir = tempfile::tempdir_in(&scratch).unwrap();
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", dir.path().canonicalize().unwrap().display()),
        ..Default::default()
    })
    .await
    .unwrap();
    (dir, store)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reindex_upgrades_and_survives_concurrent_writes() {
    let (dir, store) = scratch_store().await;
    let writer = engine(&store);
    build(&store, &writer).await;
    let queries = [5usize, 1234, 4321, 8000];
    let before = answers(&store, &queries).await;
    downgrade(&store).await;
    assert_eq!(layouts(&store).await.0, vec![CodeLayout::RowMajor; 3]);
    assert_eq!(answers(&store, &queries).await, before, "older files read");

    // A dry run reports and writes nothing.
    let mut dry = options();
    dry.dry_run = true;
    let report = writer.reindex("t", &dry).await.unwrap();
    assert_eq!(report.segments.row_major.0, 3);
    assert_eq!(report.segments.fast_scan.0, 0);
    assert_eq!(layouts(&store).await.0, vec![CodeLayout::RowMajor; 3]);

    // Upgrade while another engine writes and indexes new rows. The write is never lost.
    let racer = {
        let store = store.clone();
        tokio::spawn(async move {
            let e = engine(&store);
            for n in 0..4usize {
                let id = 20_000 + n;
                write(&e, json!({"upsert_rows":[{"id":id,"vector":vector(id)}]})).await;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                if n % 2 == 1 {
                    // A concurrent publication: the upgrade may have to retry or drop work.
                    let _ = e.index_namespace("t", true).await;
                }
            }
        })
    };
    let report = writer.reindex("t", &options()).await.unwrap();
    assert_current_manifest(&store).await;
    racer.await.unwrap();
    assert!(report.converted_segments + report.superseded >= 3);
    writer.index_namespace("t", true).await.unwrap();
    for id in 20_000..20_004usize {
        assert_eq!(
            ids(&engine(&store), id).await[0],
            json!(id),
            "row {id} kept"
        );
    }
    writer.reindex("t", &options()).await.unwrap();
    // Segments the race flushed are tiny and flat (no codes): nothing to convert.
    let (seg, _) = layouts(&store).await;
    assert!(seg.iter().all(|l| *l != CodeLayout::RowMajor), "{seg:?}");
    assert!(seg.iter().filter(|l| **l == CodeLayout::FastScan).count() >= 3);
    let again = writer.reindex("t", &options()).await.unwrap();
    assert_eq!((again.converted_segments, again.publications), (0, 0));
    assert_eq!(answers(&store, &queries).await, before, "answers unchanged");

    // Slabs.
    writer
        .recluster(
            "t",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 60,
                max_slab_rows: 5000,
                scratch: dir.path().join("spill"),
            },
        )
        .await
        .unwrap();
    let slab_before = answers(&store, &queries).await;
    downgrade(&store).await;
    let (seg, slabs) = layouts(&store).await;
    assert!(!slabs.is_empty() && slabs.iter().all(|l| *l == CodeLayout::RowMajor));
    assert!(seg.contains(&CodeLayout::RowMajor));
    let r = writer.reindex("t", &options()).await.unwrap();
    assert!(r.converted_slabs > 0);
    let (seg, slabs) = layouts(&store).await;
    assert!(seg.iter().chain(&slabs).all(|l| *l != CodeLayout::RowMajor));
    assert_eq!(answers(&store, &queries).await, slab_before);
    let r = writer.reindex("t", &options()).await.unwrap();
    assert_eq!(r.publications, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_background_indexer_upgrades_old_indexes() {
    let (_dir, store) = scratch_store().await;
    let writer = engine(&store);
    build(&store, &writer).await;
    let queries = [5usize, 4321];
    let before = answers(&store, &queries).await;
    downgrade(&store).await;
    assert_eq!(layouts(&store).await.0, vec![CodeLayout::RowMajor; 3]);
    // A server with indexing on finds the namespace and upgrades it unasked.
    // Reproduce a legacy checkpoint: writable startup has already raised the root marker,
    // but no namespace publication has changed the format-1 manifest yet.
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (sequence, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let mut legacy = serde_json::to_value(manifest).unwrap();
    legacy["format_version"] = json!(1);
    legacy["indexed_through"] = legacy
        .as_object_mut()
        .unwrap()
        .remove("checkpoint_wal")
        .unwrap();
    store
        .put(
            &format!("ns/t/manifest/{sequence:020}.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .await
        .unwrap();
    store.delete("ns/t/manifest-latest").await.unwrap();
    assert_eq!(
        wal.latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1
            .format_version,
        1
    );
    let server = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: true,
            index_interval: std::time::Duration::from_millis(50),
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    ids(&server, 5).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if layouts(&store).await.0 == vec![CodeLayout::FastScan; 3] {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "not upgraded in time");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert_current_manifest(&store).await;
    assert_eq!(answers(&store, &queries).await, before);
}

async fn assert_current_manifest(store: &Storage) {
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(manifest.format_version, bf_wal::MANIFEST_FORMAT_VERSION);
    let wire = serde_json::to_value(manifest).unwrap();
    assert!(wire.get("checkpoint_wal").is_some());
    assert!(wire.get("indexed_through").is_none());
}

#[tokio::test]
async fn legacy_flat_indexes_also_upgrade_and_retire_the_old_object() {
    let (_dir, store) = scratch_store().await;
    let writer = engine(&store);
    write(&writer, json!({"distance_metric":"euclidean_squared","upsert_rows":(0..50).map(|id| json!({"id":id,"vector":vector(id)})).collect::<Vec<_>>()})).await;
    writer.index_namespace("t", true).await.unwrap();
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    let blob = bf_vector::IndexBuilder::new(64, bf_vector::Metric::EuclideanSquared)
        .unwrap()
        .build(
            &(0..50)
                .map(|id| (id as u32, vector(id)))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let key = "ns/t/segments/legacy-flat.bfv";
    let file = SegmentFile {
        key: key.into(),
        size: blob.len() as u64,
        metadata_len: bf_vector::metadata_len(&blob).unwrap(),
        stats_index: None,
        text_header: None,
    };
    store.create_unique(key, blob).await.unwrap();
    m.segments[0].files.remove("annv2:vector");
    m.segments[0].files.insert("ann:vector".into(), file);
    m.ann_dirs.clear();
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
    let report = writer.reindex("t", &options()).await.unwrap();
    assert_eq!(report.segments.other.0, 1);
    assert_eq!(report.converted_segments, 1);
    let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(m.segments[0].files.contains_key("annv2:vector"));
    assert!(!m.segments[0].files.contains_key("ann:vector"));
    assert!(m.retired.iter().any(|r| r.key == key));
    assert_eq!(ids(&engine(&store), 5).await[0], json!(5));
    let again = writer.reindex("t", &options()).await.unwrap();
    assert_eq!(again.segments.single_object.0, 1);
    assert_eq!(again.converted_segments, 0);
}
