//! Lineage (round R4): compaction merges segments a recluster tier covers, and the tier keeps
//! answering through the lineage maps: rows deleted before or after the merge stay out, rewritten
//! rows answer with their new vectors, merges of merged segments compose, and a crash at any
//! publication of a merge leaves the namespace answering, indexed, with every referenced object.
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
fn config() -> EngineConfig {
    EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        retention: Duration::ZERO,
        ..Default::default()
    }
}
/// An engine that merges segments of 2,000 rows in pairs, and anything under 8,000 rows.
fn merging(store: &Storage) -> Engine {
    Engine::with_config(
        store.clone(),
        EngineConfig {
            compaction_segments: 2,
            max_segment_rows: 8000,
            ..config()
        },
    )
}
async fn write(engine: &Engine, body: Value) {
    engine
        .write("t", serde_json::from_value(body).unwrap())
        .await
        .unwrap();
}
async fn upsert(engine: &Engine, ids: impl Iterator<Item = usize>, shift: usize) {
    let docs: Vec<_> = ids
        .map(|n| json!({"id": n, "vector": vector(n + shift)}))
        .collect();
    write(
        engine,
        json!({"distance_metric": "euclidean_squared", "upsert_rows": docs}),
    )
    .await;
}
async fn delete(engine: &Engine, ids: impl Iterator<Item = usize>) {
    let ids: Vec<usize> = ids.collect();
    write(engine, json!({"deletes": ids})).await;
}
/// Three segments of 2,000 rows and a base tier over them.
async fn setup() -> (Arc<Faults>, Storage, Engine, tempfile::TempDir) {
    let faults = Arc::new(Faults::default());
    let store = Storage::from_backend(faults.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let engine = Engine::with_config(store.clone(), config());
    for batch in 0..3usize {
        upsert(&engine, batch * 2000..(batch + 1) * 2000, 0).await;
        engine.index_namespace("t", true).await.unwrap();
    }
    let scratch = tempfile::tempdir().unwrap();
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    (faults, store, engine, scratch)
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
async fn nearest(e: &Engine, v: Vec<f32>) -> Value {
    let request =
        serde_json::from_value(json!({"rank_by": ["vector", "ANN", v], "top_k": 3})).unwrap();
    e.query("t", request).await.unwrap().result.rows.unwrap()[0]["id"].clone()
}
async fn top(e: &Engine, q: usize) -> Value {
    nearest(e, vector(q)).await
}
async fn manifest(store: &Storage) -> bf_wal::Manifest {
    let wal = bf_wal::Wal::new(store.clone(), "t").unwrap();
    wal.latest_manifest().await.unwrap().unwrap().1
}
/// Every object the manifest references.
fn referenced(m: &bf_wal::Manifest) -> Vec<String> {
    let mut out: Vec<String> = m
        .segments
        .iter()
        .flat_map(|s| s.files.values().chain([&s.live]))
        .chain(m.ann_tiers.values().flatten().flat_map(|t| {
            t.slabs
                .iter()
                .flat_map(|s| s.files.values())
                .chain([&t.dir])
                .chain(t.pages.iter())
                .chain(t.coarse.iter())
        }))
        .chain(
            m.lineage
                .iter()
                .flat_map(|l| l.parts.iter().map(|p| &p.map)),
        )
        .chain(m.ann_dirs.values())
        .map(|f| f.key.clone())
        .collect();
    out.sort();
    out.dedup();
    out
}
async fn all_present(store: &Storage, m: &bf_wal::Manifest) {
    for key in referenced(m) {
        assert!(
            store.get(&key).await.is_ok(),
            "{key} is referenced and present"
        );
    }
}
/// The tiers' directory keys: unchanged means no rebuild.
fn tier_dirs(m: &bf_wal::Manifest) -> Vec<String> {
    m.ann_tiers
        .values()
        .flatten()
        .map(|t| t.dir.key.clone())
        .collect()
}
/// Whether the ANN answers for these rows match the exhaustive ones.
async fn check(e: &Engine, live: &[usize], gone: &[usize], rewritten: &[usize]) {
    for q in live {
        assert_eq!(top(e, *q).await, json!(q), "row {q} answers");
    }
    for q in gone {
        assert_ne!(top(e, *q).await, json!(q), "deleted row {q} stays out");
    }
    for q in rewritten {
        assert_eq!(
            nearest(e, vector(q + 50_000)).await,
            json!(q),
            "row {q}'s new vector"
        );
        assert_ne!(top(e, *q).await, json!(q), "row {q}'s old vector stays out");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_merges_covered_segments_and_the_tier_answers_through_lineage() {
    let (_faults, store, engine, scratch) = setup().await;
    let before = manifest(&store).await;
    assert_eq!(before.ann_tiers["vector"][0].covered.len(), 3);
    // Deletes and rewrites before the merge, in the first two segments.
    delete(&engine, 10..20).await;
    upsert(&engine, 2100..2110, 50_000).await;
    engine.index_namespace("t", true).await.unwrap();
    // A merge of two covered segments: lineage, and the same tier.
    let merger = merging(&store);
    assert!(merger.index_namespace("t", true).await.unwrap());
    let merged = manifest(&store).await;
    assert_eq!(tier_dirs(&merged), tier_dirs(&before), "no rebuild");
    assert_eq!(merged.lineage.len(), 2, "{:?}", merged.lineage);
    for entry in &merged.lineage {
        assert!(!merged.segments.iter().any(|s| s.id == entry.source));
        assert_eq!(entry.parts.len(), 1);
    }
    let rows: u32 = merged.lineage.iter().map(|l| l.parts[0].rows).sum();
    assert_eq!(rows, 4000 - 20, "the merged rows that survived");
    let live = [0usize, 9, 20, 1999, 2000, 2099, 2110, 3999, 4000, 5999];
    let gone: Vec<usize> = (10..20).collect();
    let rewritten: Vec<usize> = (2100..2110).collect();
    check(&engine, &live, &gone, &rewritten).await;
    check(
        &Engine::with_config(store.clone(), config()),
        &live,
        &gone,
        &rewritten,
    )
    .await;

    // Deletes after the merge reach the tier through the target's live rows.
    delete(&engine, [0usize, 2000, 3999].into_iter()).await;
    engine.index_namespace("t", true).await.unwrap();
    check(&engine, &[9, 20, 1999, 2099], &[0, 2000, 3999], &rewritten).await;

    // A merge of the merged segment composes its lineage: one hop, from the original sources.
    assert!(merger.index_namespace("t", true).await.unwrap());
    let again = manifest(&store).await;
    assert_eq!(tier_dirs(&again), tier_dirs(&before), "no rebuild");
    assert_eq!(again.lineage.len(), 3, "{:?}", again.lineage);
    let target = &again.lineage[0].parts[0].target;
    assert!(again.lineage.iter().all(|l| &l.parts[0].target == target));
    let fresh = Engine::with_config(store.clone(), config());
    check(
        &fresh,
        &[9, 20, 1999, 2099, 4000, 5999],
        &[0, 2000, 3999, 15],
        &rewritten,
    )
    .await;
    // GC at retention zero keeps every referenced object, and the old maps go.
    engine.collect_garbage("t").await.unwrap();
    all_present(&store, &again).await;
    check(&fresh, &[9, 4000], &[0], &rewritten[..2]).await;

    // A delta covers the rewritten rows beside the base, which keeps its lineage.
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    let delta = manifest(&store).await;
    assert_eq!(delta.ann_tiers["vector"].len(), 2);
    assert_eq!(delta.lineage.len(), 3);
    check(&engine, &[9, 20, 4000, 5999], &[0, 2000, 15], &rewritten).await;
    // A new base covers the segments as they are now: the lineage is dropped.
    engine
        .recluster("t", &options(scratch.path()))
        .await
        .unwrap();
    let rebuilt = manifest(&store).await;
    assert_eq!(rebuilt.ann_tiers["vector"].len(), 1);
    assert!(rebuilt.lineage.is_empty());
    assert_ne!(tier_dirs(&rebuilt), tier_dirs(&before));
    check(&engine, &[9, 20, 4000, 5999], &[0, 2000, 15], &rewritten).await;
}

/// The query answers from the indexes (no document scan) with this top row.
async fn indexed_top(e: &Engine, q: usize) -> Value {
    let request =
        serde_json::from_value(json!({"rank_by": ["vector", "ANN", vector(q)], "top_k": 3}))
            .unwrap();
    let (response, diagnostics) = e.query_with_diagnostics("t", request).await.unwrap();
    assert_eq!(diagnostics.document_scan_rows, 0, "indexed");
    assert!(diagnostics.ann_rows_scanned > 0, "indexed");
    response.result.rows.unwrap()[0]["id"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merge_that_fails_at_any_publication_point_keeps_the_tier_answering_and_every_object() {
    // A lineage map upload, the publication before writing, and the publication after writing
    // (the ambiguous case): the merge's pass fails there, as a crash would end it.
    for (pattern, skip, after) in [
        ("/lineage-", 0, false),
        ("ns/t/manifest/", 1, false),
        ("ns/t/manifest/", 1, true),
    ] {
        let (faults, store, engine, _scratch) = setup().await;
        let before = manifest(&store).await;
        delete(&engine, 10..20).await;
        engine.index_namespace("t", true).await.unwrap();
        faults.arm(pattern, skip, after);
        let merger = merging(&store);
        let result = merger.index_namespace("t", true).await;
        // A write that landed is recognized as published (its reply is verified); one that did
        // not fails the pass.
        assert!(after || result.is_err(), "{pattern} {after} {result:?}");
        faults.disarm();
        // A restarted node answers at once, indexed, from the tier.
        let restarted = Engine::with_config(store.clone(), config());
        for q in [9usize, 20, 1999, 2000, 5999] {
            assert_eq!(
                indexed_top(&restarted, q).await,
                json!(q),
                "{pattern} {after}"
            );
        }
        assert_ne!(indexed_top(&restarted, 15).await, json!(15));
        // GC at retention zero takes only what nothing references.
        restarted.collect_garbage("t").await.unwrap();
        let now = manifest(&store).await;
        all_present(&store, &now).await;
        assert_eq!(tier_dirs(&now), tier_dirs(&before));
        // The merge then completes (or already had, when the write landed), with no rebuild.
        merging(&store).index_namespace("t", true).await.unwrap();
        let merged = manifest(&store).await;
        // (When the first merge had landed, this one merges its target with the third segment.)
        assert!(merged.lineage.len() >= 2, "{pattern} {after}");
        assert_eq!(tier_dirs(&merged), tier_dirs(&before));
        restarted.collect_garbage("t").await.unwrap();
        all_present(&store, &merged).await;
        let fresh = Engine::with_config(store.clone(), config());
        for q in [9usize, 20, 1999, 2000, 5999] {
            assert_eq!(indexed_top(&fresh, q).await, json!(q), "{pattern} {after}");
        }
        assert_ne!(indexed_top(&fresh, 15).await, json!(15));
    }
}
