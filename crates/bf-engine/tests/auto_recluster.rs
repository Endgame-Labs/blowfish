//! The automatic recluster's job survives crashes: a build that dies midway leaves the namespace
//! answering from its current layout, GC keeps a running build's objects while its record is
//! fresh and deletes them as orphans afterwards, and a publication that fails (or fails
//! ambiguously, after writing) neither loses the tier nor lets GC take a referenced slab.
use async_trait::async_trait;
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::Storage;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult, path::Path,
};
use serde_json::{Value, json};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Fails the first PUT whose key contains `pattern` after `skip` of them have succeeded: before writing,
/// or (`after`) after writing, the ambiguous case of a crash between the write and its reply.
#[derive(Debug, Default)]
struct Faults {
    inner: object_store::memory::InMemory,
    rule: Mutex<Option<(String, usize, bool)>>,
}
impl Faults {
    fn arm(&self, pattern: &str, skip: usize, after: bool) {
        *self.rule.lock().unwrap() = Some((pattern.to_string(), skip, after));
    }
    fn disarm(&self) {
        *self.rule.lock().unwrap() = None;
    }
    /// `Some(after)` when this PUT fails.
    fn check(&self, path: &Path) -> Option<bool> {
        let mut rule = self.rule.lock().unwrap();
        let (pattern, skip, after) = rule.as_mut()?;
        if !path.as_ref().contains(pattern.as_str()) {
            return None;
        }
        if *skip > 0 {
            *skip -= 1;
            return None;
        }
        // One failure, as one crash.
        let after = *after;
        *rule = None;
        Some(after)
    }
}
fn failure<T>() -> StoreResult<T> {
    Err(object_store::Error::Generic {
        store: "injected",
        source: "crash boundary".into(),
    })
}
impl fmt::Display for Faults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("recluster crash-injection store")
    }
}
#[async_trait]
impl ObjectStore for Faults {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        match self.check(path) {
            Some(false) => failure(),
            Some(true) => {
                self.inner.put_opts(path, payload, opts).await?;
                failure()
            }
            None => self.inner.put_opts(path, payload, opts).await,
        }
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        if self.check(path).is_some() {
            return failure();
        }
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn vector(id: usize) -> Vec<f32> {
    (0..32)
        .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
        .collect()
}
async fn setup() -> (Arc<Faults>, Storage, Engine) {
    let faults = Arc::new(Faults::default());
    let store = Storage::from_backend(faults.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            retention: Duration::ZERO,
            ..Default::default()
        },
    );
    for batch in 0..3usize {
        let docs: Vec<_> = (0..2000)
            .map(|n| batch * 2000 + n)
            .map(|n| json!({"id": n, "vector": vector(n)}))
            .collect();
        engine
            .write(
                "t",
                serde_json::from_value(
                    json!({"distance_metric": "euclidean_squared", "upsert_rows": docs}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("t", true).await.unwrap();
    }
    (faults, store, engine)
}
fn options(scratch: &std::path::Path) -> ReclusterOptions {
    ReclusterOptions {
        attribute: "vector".into(),
        cells: 3,
        cluster_rows: 50,
        max_slab_rows: 1500,
        scratch: scratch.to_owned(),
    }
}
async fn top(e: &Engine, q: usize) -> Value {
    let request =
        serde_json::from_value(json!({"rank_by": ["vector", "ANN", vector(q)], "top_k": 3}))
            .unwrap();
    e.query("t", request).await.unwrap().result.rows.unwrap()[0]["id"].clone()
}
async fn keys(store: &Storage, prefix: &str) -> Vec<String> {
    let mut out: Vec<String> = store
        .list(prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect();
    out.sort();
    out
}
async fn referenced_slab_objects(store: &Storage) -> Vec<String> {
    let wal = bf_wal::Wal::new(store.clone(), "t").unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let mut out: Vec<String> = manifest
        .ann_tiers
        .values()
        .flatten()
        .flat_map(|t| t.slabs.iter().flat_map(|s| s.files.values()))
        .map(|f| f.key.clone())
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_that_dies_midway_leaves_the_layout_and_its_objects_become_orphans() {
    let (faults, store, engine) = setup().await;
    let scratch = tempfile::tempdir().unwrap();
    // A first tier, installed normally.
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    let installed = referenced_slab_objects(&store).await;
    assert!(!installed.is_empty());
    // A second build dies after uploading two slab objects.
    faults.arm("/slabs/", 2, false);
    assert!(
        engine
            .recluster("t", &options(scratch.path()))
            .await
            .is_err()
    );
    faults.disarm();
    assert_eq!(top(&engine, 1234).await, json!(1234));
    assert_eq!(referenced_slab_objects(&store).await, installed);
    let all = keys(&store, "ns/t/slabs/").await;
    let orphans: Vec<&String> = all.iter().filter(|k| !installed.contains(k)).collect();
    assert_eq!(orphans.len(), 2, "{all:?}");
    // While its build record is fresh, GC (retention zero here) keeps the dead build's objects.
    engine.collect_garbage("t").await.unwrap();
    assert_eq!(keys(&store, "ns/t/slabs/").await, all);
    // Once the record is stale (deleted here, as GC does after the retention window), they are
    // ordinary orphans. The installed tier's objects always stay.
    let records: Vec<String> = keys(&store, "ns/t/indexer/")
        .await
        .into_iter()
        .filter(|k| k.contains("/build-"))
        .collect();
    assert!(!records.is_empty());
    for record in records {
        store.delete(&record).await.unwrap();
    }
    engine.collect_garbage("t").await.unwrap();
    assert_eq!(keys(&store, "ns/t/slabs/").await, installed);
    assert_eq!(top(&Engine::new(store.clone()), 4321).await, json!(4321));
    assert!(std::fs::read_dir(scratch.path()).unwrap().next().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_or_ambiguous_install_publication_is_retried_and_keeps_every_referenced_slab() {
    for after in [false, true] {
        let (faults, store, engine) = setup().await;
        let scratch = tempfile::tempdir().unwrap();
        // The install pass's publication fails once (with `after`, once it has been written).
        faults.arm("ns/t/manifest/", 1, after);
        let report = engine
            .recluster("t", &options(scratch.path()))
            .await
            .unwrap();
        faults.disarm();
        assert_eq!(report.rows, 6000);
        let referenced = referenced_slab_objects(&store).await;
        assert!(!referenced.is_empty());
        // GC with retention zero deletes everything unreferenced: never a slab of the tier.
        engine.collect_garbage("t").await.unwrap();
        for key in &referenced {
            assert!(store.get(key).await.is_ok(), "{key} survives GC");
        }
        let fresh = Engine::new(store.clone());
        for q in [17usize, 2500, 5999] {
            assert_eq!(top(&fresh, q).await, json!(q));
        }
    }
}
