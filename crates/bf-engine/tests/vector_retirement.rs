//! Covered ANN retirement: a tier is the sole vector source across reads, maintenance, and copies.
use bf_engine::{Engine, EngineConfig, ReclusterOptions, ReindexOptions};
use bf_storage::{Storage, StorageConfig};
use bf_wal::{Manifest, Wal};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

fn config() -> EngineConfig {
    EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        retention: Duration::ZERO,
        ann_nprobe: 256,
        compaction_segments: 100,
        ..Default::default()
    }
}
fn engine(store: &Storage) -> Engine {
    Engine::with_config(store.clone(), config())
}
fn vector(id: usize) -> Vec<f32> {
    (0..16)
        .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
        .collect()
}
async fn write(e: &Engine, ns: &str, body: Value) {
    e.write(ns, serde_json::from_value(body).unwrap())
        .await
        .unwrap();
}
async fn query(e: &Engine, ns: &str, body: Value) -> Vec<bf_types::Document> {
    e.query(ns, serde_json::from_value(body).unwrap())
        .await
        .unwrap()
        .result
        .rows
        .unwrap()
}
async fn manifest(store: &Storage) -> Manifest {
    Wal::new(store.clone(), "t")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap()
        .1
}
fn scratch() -> tempfile::TempDir {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.scratch/d4-tests");
    std::fs::create_dir_all(&path).unwrap();
    tempfile::tempdir_in(path).unwrap()
}
async fn setup(store: &Storage, path: &Path) -> (Engine, Vec<bf_wal::Segment>) {
    let e = engine(store);
    for batch in 0..3 {
        let docs: Vec<_> = (batch*600..(batch+1)*600).map(|id| {
            json!({"id":id,"vector":vector(id),"group":id%5,"text":format!("fish pond {id}"),"optional":vector(id+7)})
        }).collect();
        write(&e,"t",json!({"distance_metric":"euclidean_squared","schema":{"vector":{"type":"[16]f16"},"optional":{"type":"[16]f32","ann":false},"text":{"type":"string","full_text_search":true}},"upsert_rows":docs})).await;
        e.index_namespace("t", true).await.unwrap();
    }
    let own = manifest(store).await.segments;
    e.recluster(
        "t",
        &ReclusterOptions {
            attribute: "vector".into(),
            cells: 3,
            cluster_rows: 30,
            max_slab_rows: 800,
            scratch: path.into(),
        },
    )
    .await
    .unwrap();
    (e, own)
}
async fn all_named_exist(store: &Storage, manifest: &Manifest) {
    let mut keys = Vec::new();
    for s in &manifest.segments {
        keys.extend(s.files.values().map(|f| &f.key));
        keys.push(&s.live.key);
    }
    keys.extend(manifest.ann_dirs.values().map(|f| &f.key));
    for set in manifest.ann_tiers.values().flatten() {
        keys.push(&set.dir.key);
        keys.extend(set.pages.iter().chain(&set.coarse).map(|f| &f.key));
        for slab in &set.slabs {
            keys.extend(slab.files.values().map(|f| &f.key));
        }
    }
    keys.extend(
        manifest
            .lineage
            .iter()
            .flat_map(|l| l.parts.iter().map(|p| &p.map.key)),
    );
    for key in keys {
        assert!(
            store.head(key).await.is_ok(),
            "manifest names missing {key}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_vectors_survive_gc_projection_exact_filtered_recluster_compaction_and_upgrade() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let scratch = scratch();
    let (e, own) = setup(&store, scratch.path()).await;
    let ann = json!({"rank_by":["vector","ANN",vector(853)],"include_attributes":true,"top_k":10});
    let filtered = json!({"rank_by":["vector","ANN",vector(853)],"filters":["group","Eq",3],"include_attributes":true,"top_k":10});
    let exact = json!({"rank_by":["vector","kNN",vector(853)],"filters":["group","Eq",3],"include_attributes":true,"top_k":10});
    let want = query(&e, "t", ann.clone()).await;
    let want_filtered = query(&e, "t", filtered.clone()).await;
    let want_exact = query(&e, "t", exact.clone()).await;
    assert!(
        manifest(&store)
            .await
            .segments
            .iter()
            .all(|s| !s.files.contains_key("annv2:vector"))
    );
    let grace = Engine::with_config(
        store.clone(),
        EngineConfig {
            retention: Duration::from_secs(3600),
            ..config()
        },
    );
    grace.collect_garbage("t").await.unwrap();
    for seg in &own {
        assert!(
            store.head(&seg.files["annv2:vector"].key).await.is_ok(),
            "retention must preserve retired ANN objects"
        );
    }
    e.collect_garbage("t").await.unwrap();
    for s in &own {
        assert!(matches!(
            store.head(&s.files["annv2:vector"].key).await,
            Err(bf_storage::Error::NotFound)
        ));
    }
    let restarted = engine(&store);
    assert_eq!(query(&restarted, "t", ann.clone()).await, want);
    let (answer, diagnostics) = restarted
        .query_with_diagnostics("t", serde_json::from_value(filtered.clone()).unwrap())
        .await
        .unwrap();
    assert_eq!(answer.result.rows.unwrap(), want_filtered);
    assert_eq!(
        diagnostics.document_scan_rows, 0,
        "filtered ANN must probe tiers"
    );
    assert_eq!(query(&restarted, "t", exact.clone()).await, want_exact);
    assert_eq!(
        query(
            &restarted,
            "t",
            json!({"rank_by":["id","asc"],"top_k":3,"include_attributes":["vector"]})
        )
        .await[0]["vector"]
            .as_array()
            .unwrap()
            .len(),
        16
    );
    restarted
        .reindex("t", &ReindexOptions::default())
        .await
        .unwrap();
    assert_eq!(query(&engine(&store), "t", ann.clone()).await, want);
    // Rebuilding a base reads its predecessor's slabs after the segment objects are gone.
    restarted
        .recluster(
            "t",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 30,
                max_slab_rows: 800,
                scratch: scratch.path().into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(query(&engine(&store), "t", ann.clone()).await, want);
    // Covered compaction reads through lineage on its next merge, too.
    let merging = Engine::with_config(
        store.clone(),
        EngineConfig {
            compaction_segments: 2,
            max_segment_rows: 3000,
            ..config()
        },
    );
    merging.index_namespace("t", true).await.unwrap();
    merging.collect_garbage("t").await.unwrap();
    assert_eq!(query(&engine(&store), "t", ann.clone()).await, want);
    assert_eq!(
        query(&engine(&store), "t", filtered.clone()).await,
        want_filtered
    );
    merging.index_namespace("t", true).await.unwrap();
    merging.collect_garbage("t").await.unwrap();
    assert_eq!(query(&engine(&store), "t", ann.clone()).await, want);
    // Patches hydrate old vectors, and removal statistics must count them through the locator.
    write(
        &merging,
        "t",
        json!({"patch_rows":[{"id":853,"text":"new pond"}],"deletes":[854]}),
    )
    .await;
    merging.index_namespace("t", true).await.unwrap();
    assert_eq!(query(&engine(&store), "t", ann.clone()).await[0]["id"], 853);
    all_named_exist(&store, &manifest(&store).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn branches_and_copies_pin_tiers_and_lineage_after_source_deletion() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let scratch = scratch();
    let (e, _) = setup(&store, scratch.path()).await;
    let merging = Engine::with_config(
        store.clone(),
        EngineConfig {
            compaction_segments: 2,
            max_segment_rows: 3000,
            ..config()
        },
    );
    merging.index_namespace("t", true).await.unwrap();
    e.collect_garbage("t").await.unwrap();
    let q = json!({"rank_by":["vector","ANN",vector(853)],"include_attributes":true,"top_k":10});
    let want = query(&engine(&store), "t", q.clone()).await;
    for (dest, role) in [
        ("branch", "branch_from_namespace"),
        ("copy", "copy_from_namespace"),
    ] {
        write(&e, dest, json!({role:"t"})).await;
    }
    e.delete_namespace("t").await.unwrap();
    e.collect_garbage("t").await.unwrap();
    for dest in ["branch", "copy"] {
        let restarted = engine(&store);
        assert_eq!(query(&restarted, dest, q.clone()).await, want);
        restarted.index_namespace(dest, true).await.unwrap();
        restarted.collect_garbage(dest).await.unwrap();
        assert_eq!(query(&engine(&store), dest, q.clone()).await, want);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncovered_ann_winners_project_vectors_from_another_attributes_full_tier() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let scratch = scratch();
    let (e, _) = setup(&store, scratch.path()).await;
    let options = ReclusterOptions {
        attribute: "optional".into(),
        cells: 3,
        cluster_rows: 30,
        max_slab_rows: 800,
        scratch: scratch.path().into(),
    };
    e.recluster("t", &options).await.unwrap();
    write(&e, "t", json!({"upsert_rows":[{"id":9999,"vector":vector(9999),"optional":vector(10006),"group":3,"text":"fish pond"}]})).await;
    e.index_namespace("t", true).await.unwrap();
    e.recluster("t", &options).await.unwrap(); // Delta for the new row.
    e.recluster("t", &options).await.unwrap(); // Base covering both old and new segments.
    e.collect_garbage("t").await.unwrap();
    let m = manifest(&store).await;
    assert!(
        m.segments
            .iter()
            .all(|s| !s.files.contains_key("annv2:optional"))
    );
    assert!(
        m.segments
            .iter()
            .any(|s| s.files.contains_key("annv2:vector"))
    );
    let q = json!({"rank_by":["vector","ANN",vector(9999)],"top_k":1,"include_attributes":true});
    let rows = query(&engine(&store), "t", q).await;
    assert_eq!(rows[0]["id"], 9999);
    assert_eq!(rows[0]["optional"], json!(vector(10006)));
}

// These tests use a real child and SIGKILL, so destructors and graceful engine shutdown cannot
// conceal a missing publication, a partial GC, or an in-memory dependency.
#[cfg(unix)]
mod crashes {
    use super::*;
    use async_trait::async_trait;
    use futures::{StreamExt, stream::BoxStream};
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
        path::Path as ObjectPath,
    };
    use std::{
        fmt,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Debug)]
    struct CrashStore {
        inner: object_store::local::LocalFileSystem,
        mode: String,
        ready: std::path::PathBuf,
        publications: AtomicUsize,
    }
    impl fmt::Display for CrashStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("D4 process-kill store")
        }
    }
    impl CrashStore {
        async fn pause(&self) {
            std::fs::write(&self.ready, b"ready").unwrap();
            std::future::pending::<()>().await;
        }
    }
    #[async_trait]
    impl ObjectStore for CrashStore {
        async fn put_opts(
            &self,
            path: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> StoreResult<PutResult> {
            if self.mode == "rows-before" && path.as_ref().ends_with(".rows") {
                self.pause().await;
            }
            let boundary = path.as_ref().starts_with("ns/t/manifest/")
                && self.publications.fetch_add(1, Ordering::SeqCst) == 1;
            if boundary && self.mode == "before" {
                self.pause().await;
            }
            let result = self.inner.put_opts(path, payload, opts).await?;
            if self.mode == "rows-after" && path.as_ref().ends_with(".rows") {
                self.pause().await;
            }
            if boundary && self.mode == "after" {
                self.pause().await;
            }
            Ok(result)
        }
        async fn put_multipart_opts(
            &self,
            path: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(path, opts).await
        }
        async fn get_opts(&self, path: &ObjectPath, opts: GetOptions) -> StoreResult<GetResult> {
            self.inner.get_opts(path, opts).await
        }
        fn delete_stream(
            &self,
            paths: BoxStream<'static, StoreResult<ObjectPath>>,
        ) -> BoxStream<'static, StoreResult<ObjectPath>> {
            let ready = self.ready.clone();
            let gc = self.mode == "gc";
            self.inner
                .delete_stream(paths)
                .then(move |result| {
                    let ready = ready.clone();
                    async move {
                        if gc
                            && result.as_ref().is_ok_and(|p| {
                                p.as_ref().contains("/segments/") && p.as_ref().contains("/ann.")
                            })
                        {
                            std::fs::write(ready, b"ready").unwrap();
                            std::future::pending::<()>().await;
                        }
                        result
                    }
                })
                .boxed()
        }
        fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> StoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            opts: CopyOptions,
        ) -> StoreResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retired_vector_crash_child() {
        let Ok(root) = std::env::var("BF_D4_CRASH_ROOT") else {
            return;
        };
        let mode = std::env::var("BF_D4_CRASH_MODE").unwrap();
        let backend = Arc::new(CrashStore {
            inner: object_store::local::LocalFileSystem::new_with_prefix(
                Path::new(&root).join("bucket"),
            )
            .unwrap(),
            mode: mode.clone(),
            ready: Path::new(&root).join("ready"),
            publications: AtomicUsize::new(0),
        });
        let store = Storage::from_backend(backend, Default::default(), false, false)
            .await
            .unwrap();
        let e = engine(&store);
        if mode == "gc" {
            e.collect_garbage("t").await.unwrap();
        } else {
            e.index_namespace("t", true).await.unwrap();
        }
        panic!("crash boundary was not reached");
    }
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kill_before_and_after_retirement_publication_and_during_gc() {
        for mode in ["rows-before", "rows-after", "before", "after", "gc"] {
            let root = scratch();
            let bucket = root.path().join("bucket");
            std::fs::create_dir(&bucket).unwrap();
            let store = Storage::open(&StorageConfig {
                url: format!("file://{}", bucket.display()),
                ..Default::default()
            })
            .await
            .unwrap();
            let (e, own) = setup(&store, root.path()).await;
            // Reconstruct the pre-D4 publication: tiers plus their redundant segment objects.
            let wal = Wal::new(store.clone(), "t").unwrap();
            let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
            for seg in &mut m.segments {
                let old = own.iter().find(|s| s.id == seg.id).unwrap();
                seg.files
                    .insert("annv2:vector".into(), old.files["annv2:vector"].clone());
            }
            for set in m.ann_tiers.values_mut().flatten() {
                for slab in &mut set.slabs {
                    slab.files.remove("vector_rows");
                }
            }
            wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
            let q = json!({"rank_by":["vector","ANN",vector(853)],"filters":["group","Eq",3],"include_attributes":true,"top_k":10});
            let want = query(&engine(&store), "t", q.clone()).await;
            if mode == "gc" {
                engine(&store).index_namespace("t", true).await.unwrap();
            }
            drop(e);
            let mut child = Child(
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "crashes::retired_vector_crash_child",
                        "--nocapture",
                    ])
                    .env("BF_D4_CRASH_ROOT", root.path())
                    .env("BF_D4_CRASH_MODE", mode)
                    .spawn()
                    .unwrap(),
            );
            tokio::time::timeout(Duration::from_secs(30), async {
                while !root.path().join("ready").exists() {
                    assert!(
                        child.0.try_wait().unwrap().is_none(),
                        "child exited before {mode}"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            child.0.kill().unwrap(); // SIGKILL on Unix.
            let status = child.0.wait().unwrap();
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
            let restarted = engine(&store);
            assert_eq!(query(&restarted, "t", q.clone()).await, want, "{mode}");
            restarted.collect_garbage("t").await.unwrap();
            all_named_exist(&store, &manifest(&store).await).await;
            restarted.index_namespace("t", true).await.unwrap();
            restarted.collect_garbage("t").await.unwrap();
            let current = manifest(&store).await;
            assert!(
                current
                    .segments
                    .iter()
                    .all(|s| !s.files.contains_key("annv2:vector"))
            );
            all_named_exist(&store, &current).await;
            assert_eq!(query(&engine(&store), "t", q).await, want, "{mode}");
        }
    }
}
