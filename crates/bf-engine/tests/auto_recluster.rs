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
    /// PUTs whose key contains this never return, and how many have started.
    hang: Mutex<Option<String>>,
    hung: std::sync::atomic::AtomicUsize,
    /// The next PUT whose key contains this waits until the gate is opened (once).
    gate: Mutex<Option<(String, Arc<tokio::sync::Notify>)>>,
    gated: std::sync::atomic::AtomicUsize,
}
impl Faults {
    /// Hold the next PUT matching `pattern`; notifying the returned handle lets it through.
    fn gate(&self, pattern: &str) -> Arc<tokio::sync::Notify> {
        let open = Arc::new(tokio::sync::Notify::new());
        *self.gate.lock().unwrap() = Some((pattern.to_string(), open.clone()));
        open
    }
    /// Wait until the gate holds a PUT.
    async fn gated(&self) {
        tokio::time::timeout(Duration::from_secs(120), async {
            while self.gated.load(std::sync::atomic::Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a PUT is held");
    }
    fn hang(&self, pattern: Option<&str>) {
        *self.hang.lock().unwrap() = pattern.map(str::to_string);
    }
    /// Wait until a PUT hangs.
    async fn hung(&self) {
        tokio::time::timeout(Duration::from_secs(120), async {
            while self.hung.load(std::sync::atomic::Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a PUT hangs");
    }
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
        let hangs = self
            .hang
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|p| path.as_ref().contains(p.as_str()));
        if hangs {
            self.hung.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            std::future::pending::<()>().await;
        }
        let held = {
            let mut gate = self.gate.lock().unwrap();
            if gate
                .as_ref()
                .is_some_and(|(p, _)| path.as_ref().contains(p.as_str()))
            {
                gate.take()
            } else {
                None
            }
        };
        if let Some((_, open)) = held {
            self.gated.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            open.notified().await;
        }
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
        .flat_map(|t| {
            t.slabs
                .iter()
                .flat_map(|s| s.files.values())
                .chain(t.coarse.iter())
        })
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

async fn write_rows(engine: &Engine, ids: impl Iterator<Item = usize>, shift: usize) {
    let docs: Vec<_> = ids
        .map(|n| json!({"id": n, "vector": vector(n + shift)}))
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
async fn tiers(store: &Storage) -> Vec<bf_wal::SlabSet> {
    let wal = bf_wal::Wal::new(store.clone(), "t").unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    manifest
        .ann_tiers
        .get("vector")
        .cloned()
        .unwrap_or_default()
}
async fn nearest(e: &Engine, v: Vec<f32>) -> Value {
    let request =
        serde_json::from_value(json!({"rank_by": ["vector", "ANN", v], "top_k": 3})).unwrap();
    e.query("t", request).await.unwrap().result.rows.unwrap()[0]["id"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_after_a_base_get_a_delta_on_its_cells_and_then_merge_into_a_new_base() {
    let (_faults, store, engine) = setup().await;
    let scratch = tempfile::tempdir().unwrap();
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    let base = tiers(&store).await;
    assert_eq!(base.len(), 1);
    let cells = base[0]
        .coarse
        .clone()
        .expect("a base keeps its coarse cells");
    assert_eq!(base[0].coarse_rows, 6000);
    // Build records are deleted once the tier is installed.
    let records = |keys: Vec<String>| keys.iter().filter(|k| k.contains("/build-")).count();
    assert_eq!(records(keys(&store, "ns/t/indexer/").await), 0);

    // New rows, and rewritten ones (their old versions are masked in the base): a delta.
    write_rows(&engine, 6000..6400, 0).await;
    write_rows(&engine, 100..200, 50_000).await;
    let report = engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    assert_eq!(report.rows, 500);
    let two = tiers(&store).await;
    assert_eq!(two.len(), 2);
    assert_eq!(two[0].dir.key, base[0].dir.key, "the base stays");
    assert!(two[1].coarse.is_none(), "a delta uses its base's cells");
    assert_eq!(records(keys(&store, "ns/t/indexer/").await), 0);
    for q in [17usize, 3017, 6100, 6399] {
        assert_eq!(top(&engine, q).await, json!(q));
    }
    // A rewritten row answers with its new vector only.
    assert_eq!(nearest(&engine, vector(150 + 50_000)).await, json!(150));
    assert_ne!(nearest(&engine, vector(150)).await, json!(150));

    // More rows: the delta is rebuilt with them, the old delta retired, and the base kept.
    write_rows(&engine, 6400..6600, 0).await;
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    let rebuilt = tiers(&store).await;
    assert_eq!(rebuilt.len(), 2);
    assert_eq!(rebuilt[0].dir.key, base[0].dir.key);
    assert_ne!(rebuilt[1].dir.key, two[1].dir.key);
    assert_eq!(rebuilt[1].rows(), 700);
    // GC (retention zero) keeps every object the tiers use, the shared cells included.
    engine.collect_garbage("t").await.unwrap();
    for set in &rebuilt {
        for key in set
            .slabs
            .iter()
            .flat_map(|s| s.files.values())
            .map(|f| &f.key)
            .chain([&set.dir.key, &cells.key])
        {
            assert!(store.get(key).await.is_ok(), "{key} survives GC");
        }
    }
    let fresh = Engine::new(store.clone());
    for q in [17usize, 6500, 6150] {
        assert_eq!(top(&fresh, q).await, json!(q));
    }

    // Half the base outside it: a merge into one new base, on the same cells.
    write_rows(&engine, 6600..9000, 0).await;
    let report = engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    assert_eq!(report.rows, 9000);
    let merged = tiers(&store).await;
    assert_eq!(merged.len(), 1);
    assert_ne!(merged[0].dir.key, base[0].dir.key);
    assert_eq!(merged[0].coarse.as_ref().map(|f| &f.key), Some(&cells.key));
    assert_eq!(merged[0].coarse_rows, 6000);
    engine.collect_garbage("t").await.unwrap();
    assert!(store.get(&cells.key).await.is_ok());
    for q in [17usize, 6500, 8999] {
        assert_eq!(top(&engine, q).await, json!(q));
    }
    assert_eq!(nearest(&engine, vector(150 + 50_000)).await, json!(150));
    assert!(std::fs::read_dir(scratch.path()).unwrap().next().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delta_builds_beside_a_base_rebuild_and_the_base_keeps_it() {
    let (faults, store, engine) = setup().await;
    let scratch = tempfile::tempdir().unwrap();
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    write_rows(&engine, 6000..6400, 0).await;
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    let before = tiers(&store).await;
    assert_eq!(before.len(), 2);

    // Half the base outside it: a merge into a new base, held at its first slab upload.
    write_rows(&engine, 6400..9000, 0).await;
    let open = faults.gate("/slabs/");
    let base = tokio::spawn({
        let (engine, opts) = (engine.clone(), options(scratch.path()));
        async move { engine.recluster("t", &opts).await }
    });
    faults.gated().await;
    // Rows written meanwhile get a delta beside it, which installs at once.
    write_rows(&engine, 9000..9300, 0).await;
    write_rows(&engine, 150..160, 70_000).await;
    let report = tokio::time::timeout(
        Duration::from_secs(120),
        engine.recluster("t", &options(scratch.path())),
    )
    .await
    .expect("the delta does not wait for the base")
    .unwrap();
    assert_eq!(report.rows, 310);
    let during = tiers(&store).await;
    assert_eq!(during.len(), 3);
    assert_eq!(during[0].dir.key, before[0].dir.key);
    assert_eq!(during[1].dir.key, before[1].dir.key);
    for q in [17usize, 6100, 6500, 9000, 9299] {
        assert_eq!(top(&engine, q).await, json!(q));
    }
    assert_eq!(nearest(&engine, vector(155 + 70_000)).await, json!(155));

    // The base installs over the rows it was built from, and keeps the delta built beside it.
    open.notify_one();
    let report = tokio::time::timeout(Duration::from_secs(120), base)
        .await
        .expect("the base finishes")
        .unwrap()
        .unwrap();
    assert_eq!(report.rows, 9000);
    let after = tiers(&store).await;
    assert_eq!(after.len(), 2);
    assert_ne!(after[0].dir.key, before[0].dir.key);
    assert_eq!(after[1].dir.key, during[2].dir.key);
    engine.collect_garbage("t").await.unwrap();
    let fresh = Engine::new(store.clone());
    for e in [&engine, &fresh] {
        for q in [17usize, 6100, 6500, 8999, 9000, 9299] {
            assert_eq!(top(e, q).await, json!(q));
        }
        assert_eq!(nearest(e, vector(155 + 70_000)).await, json!(155));
        assert_ne!(nearest(e, vector(155)).await, json!(155));
    }
    // The next delta replaces the one built beside the base.
    write_rows(&engine, 9300..9500, 0).await;
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    let next = tiers(&store).await;
    assert_eq!(next.len(), 2);
    assert_eq!(next[0].dir.key, after[0].dir.key);
    assert_eq!(next[1].rows(), 510);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bulk_load_flush_is_kept_in_the_manifest_so_a_restart_still_waits() {
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
            bulk_flush_rows: 400,
            ..Default::default()
        },
    );
    let bulk_at = || async {
        let wal = bf_wal::Wal::new(store.clone(), "t").unwrap();
        wal.latest_manifest().await.unwrap().unwrap().1.bulk_load_at
    };
    // A few rows: not a load.
    write_rows(&engine, 0..50, 0).await;
    assert!(bulk_at().await.is_none());
    // Thousands of rows at once: a load's flush, recorded with the checkpoint.
    write_rows(&engine, 50..5000, 0).await;
    let at = bulk_at().await.expect("a bulk load's flush is recorded");
    assert!((chrono::Utc::now() - at).num_seconds() < 60);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delta_whose_rows_are_all_deleted_leaves_the_joint_plan_to_the_base() {
    let (_faults, store, engine) = setup().await;
    let scratch = tempfile::tempdir().unwrap();
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    write_rows(&engine, 6000..6300, 0).await;
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    assert_eq!(tiers(&store).await.len(), 2);
    assert_eq!(top(&engine, 6100).await, json!(6100));
    let deletes: Vec<usize> = (6000..6300).collect();
    engine
        .write(
            "t",
            serde_json::from_value(json!({"deletes": deletes})).unwrap(),
        )
        .await
        .unwrap();
    engine.index_namespace("t", true).await.unwrap();
    // The delta ranks nothing and withdraws; the base answers alone, without waiting for it.
    for q in [17usize, 5999] {
        let found = tokio::time::timeout(Duration::from_secs(30), top(&engine, q))
            .await
            .expect("no query waits on a withdrawn tier");
        assert_eq!(found, json!(q));
    }
    assert_ne!(top(&engine, 6100).await, json!(6100));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_build_removes_its_spill() {
    let (faults, _, engine) = setup().await;
    let scratch = tempfile::tempdir().unwrap();
    let _open = faults.gate("/slabs/");
    let build = tokio::spawn({
        let (engine, opts) = (engine.clone(), options(scratch.path()));
        async move { engine.recluster("t", &opts).await }
    });
    faults.gated().await;
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 1);
    build.abort();
    assert!(build.await.unwrap_err().is_cancelled());
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_stops_a_running_build_at_once_and_a_restart_builds_again() {
    let (faults, store, engine) = setup().await;
    let scratch = tempfile::tempdir().unwrap();
    // The build's slab uploads never return, as a long build would not.
    faults.hang(Some("/slabs/"));
    let build = tokio::spawn({
        let (engine, opts) = (engine.clone(), options(scratch.path()));
        async move { engine.recluster("t", &opts).await }
    });
    faults.hung().await;
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 1);
    let started = std::time::Instant::now();
    assert!(engine.shutdown(Duration::from_secs(10)).await);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    let stopped = tokio::time::timeout(Duration::from_secs(2), build)
        .await
        .expect("the build returns")
        .unwrap();
    assert_eq!(stopped.unwrap_err().status, 503);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    faults.hang(None);
    // Its record stays, so GC keeps whatever it uploaded until the record is stale.
    assert!(
        keys(&store, "ns/t/indexer/")
            .await
            .iter()
            .any(|k| k.contains("/build-"))
    );
    assert!(tiers(&store).await.is_empty());
    // A restarted engine answers at once, and builds.
    let restarted = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            retention: Duration::ZERO,
            ..Default::default()
        },
    );
    assert_eq!(top(&restarted, 1234).await, json!(1234));
    restarted
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    assert_eq!(tiers(&store).await.len(), 1);
    assert_eq!(top(&restarted, 4321).await, json!(4321));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_stops_background_indexing_mid_flush() {
    let faults = Arc::new(Faults::default());
    let store = Storage::from_backend(faults.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        EngineConfig {
            index_interval: Duration::from_millis(10),
            index_rows: 1,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    // The flush's segment upload never returns.
    faults.hang(Some("/segments/"));
    write_rows_unindexed(&engine, 0..100).await;
    faults.hung().await;
    let started = std::time::Instant::now();
    assert!(engine.shutdown(Duration::from_secs(10)).await);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    faults.hang(None);
    // Nothing was published, and the acknowledged rows are in the WAL: a restart indexes them.
    let restarted = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    restarted.index_namespace("t", true).await.unwrap();
    assert_eq!(top(&restarted, 42).await, json!(42));
}

async fn write_rows_unindexed(engine: &Engine, ids: std::ops::Range<usize>) {
    let docs: Vec<_> = ids.map(|n| json!({"id": n, "vector": vector(n)})).collect();
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
}
