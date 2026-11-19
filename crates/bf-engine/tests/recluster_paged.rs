//! A recluster of a big enough directory also writes a paged copy; queries read it (a coarse
//! header, then the pages of the cells they probe) and answer like the segments do, honor later
//! deletes, and fall back when a new segment makes the slabs stale. This test sets the paging
//! thresholds through the environment, so it lives in its own test binary.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use bf_wal::Wal;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paged_slab_directory_answers_like_the_segments() {
    // SAFETY: this binary has one test, and it sets these before any other thread reads them.
    unsafe {
        std::env::set_var("BF_ANN_PAGED_MIN_ENTRIES", "1");
        std::env::set_var("BF_ANN_PAGE_ENTRIES", "8");
    }
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let writer = engine(&store);
    for batch in 0..3usize {
        let docs: Vec<_> = (0..3000)
            .map(|n| json!({"id":batch * 3000 + n,"vector":vector(batch * 3000 + n)}))
            .collect();
        write(
            &writer,
            json!({"distance_metric":"euclidean_squared","upsert_rows":docs}),
        )
        .await;
        writer.index_namespace("t", true).await.unwrap();
    }
    let queries = [5usize, 1234, 4321, 8000];
    let mut before = Vec::new();
    for q in queries {
        before.push(ids(&engine(&store), q).await);
    }
    let scratch = std::env::temp_dir().join(format!("bf-recluster-paged-{}", std::process::id()));
    writer
        .recluster(
            "t",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 60,
                max_slab_rows: 5000,
                scratch: scratch.clone(),
            },
        )
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&scratch);
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let set = &manifest.ann_slabs["vector"];
    let pages = set.pages.as_ref().expect("a paged copy was written");
    assert!(pages.metadata_len < pages.size);
    // The header and pages decode, and list every cluster the flat directory lists.
    let bytes = store.get(&pages.key).await.unwrap();
    let paged = bf_vector::PagedDirectory::decode(&bytes[..pages.metadata_len as usize]).unwrap();
    assert!(paged.cell_count() > 4, "{} cells", paged.cell_count());
    for (q, want) in queries.iter().zip(&before) {
        let got = ids(&engine(&store), *q).await;
        assert_eq!(got[0], json!(q), "the query's own row is first");
        let overlap = got.iter().filter(|g| want.contains(g)).count();
        assert!(
            overlap >= 8,
            "paged slab results overlap the segments' ({overlap}/10)"
        );
    }
    // Deletes after the build are honored, and a new segment retires the slabs.
    write(&writer, json!({"deletes":[1234]})).await;
    writer.index_namespace("t", true).await.unwrap();
    assert!(!ids(&engine(&store), 1234).await.contains(&json!(1234)));
    let docs: Vec<_> = (9000..9100)
        .map(|n| json!({"id":n,"vector":vector(n)}))
        .collect();
    write(&writer, json!({"upsert_rows":docs})).await;
    writer.index_namespace("t", true).await.unwrap();
    assert_eq!(ids(&engine(&store), 9050).await[0], json!(9050));
}
