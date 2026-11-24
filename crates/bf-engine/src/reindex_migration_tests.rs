//! Migration checkpoints exercise the actual ANN entry point and query-local scan counters.
use super::*;
use bf_storage::{Storage, StorageConfig};
use serde_json::json;

// Check the actual ANN entry point: explain output alone describes intent and can hide scans.
#[tokio::test]
async fn mixed_legacy_segments_keep_the_ann_path() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        crate::EngineConfig {
            indexing: false,
            ann_nprobe: 4,
            ann_rerank_factor: 4,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    for batch in 0..4 {
        engine.write("t", serde_json::from_value(json!({"distance_metric":"euclidean_squared","schema":{"v":{"type":"[32]f32","ann":true}},
                "upsert_rows": (0..3000).map(|n| { let id=batch*3000+n;
                    json!({"id":id,"v":(0..32).map(|d| ((id*7919+d*104729) as f64).sin() as f32).collect::<Vec<_>>()}) }).collect::<Vec<_>>()
            })).unwrap()).await.unwrap();
        engine.index_namespace("t", true).await.unwrap();
    }
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (_, mut m) = wal.latest_manifest().await.unwrap().unwrap();
    let query: bf_types::QueryRequest = serde_json::from_value(json!({"rank_by":["v","ANN",(0..32).map(|d| ((4321*7919+d*104729) as f64).sin() as f32).collect::<Vec<_>>()],"top_k":10,"include_attributes":false})).unwrap();
    let state_for = |m: &Manifest| state(m);
    let bf_types::RankBy::Vector { vector, .. } = query.rank_by.as_ref().unwrap() else {
        unreachable!()
    };
    let before = crate::ann::search(
        &store,
        &state_for(&m),
        &query,
        "v",
        vector,
        &engine.0.config,
        10,
        &BTreeSet::new(),
    )
    .await
    .unwrap()
    .unwrap()
    .0;
    for seg in &mut m.segments {
        let current = seg.files.remove("annv2:v").unwrap();
        let bytes = store.get(&current.key).await.unwrap();
        let legacy = bf_vector::downgrade_layout(&bytes).unwrap().unwrap();
        let meta = bf_vector::metadata_len(&legacy).unwrap();
        let (fragment, resident) = bf_vector::Fragment::legacy_fragment(&legacy, true).unwrap();
        let file = segment::put_file(&store, format!("ns/t/test/{}.bfv", seg.id), legacy, meta)
            .await
            .unwrap();
        let frag = segment::put_file(
            &store,
            format!("ns/t/test/{}.bfb", seg.id),
            fragment,
            resident,
        )
        .await
        .unwrap();
        seg.files.insert("ann:v".into(), file);
        seg.files.insert("annfrag2:v".into(), frag);
    }
    m.ann_dirs.clear();
    // Two of four segments converted: a durable, resumable 50% checkpoint.
    for i in 0..2 {
        let seg = &m.segments[i];
        let converted = engine
            .convert_segment_index("t", seg, "v", &seg.files["ann:v"])
            .await
            .unwrap();
        apply_segments(&store, "t", &mut m, &[converted])
            .await
            .unwrap();
    }
    let found = crate::ann::search(
        &store,
        &state_for(&m),
        &query,
        "v",
        vector,
        &engine.0.config,
        10,
        &BTreeSet::new(),
    )
    .await
    .unwrap();
    assert!(
        found.is_some(),
        "migration must keep ANN, never fall back to document scans"
    );
    assert_eq!(
        found.unwrap().0,
        before,
        "same candidates, distances, and recall"
    );
}

fn state(m: &Manifest) -> crate::NamespaceState {
    crate::NamespaceState {
        segments: m.segments.clone().into(),
        ann_dirs: m.ann_dirs.clone(),
        ann_tiers: m.ann_tiers.clone().into(),
        schema: m.schema.clone(),
        metric: m.distance_metric,
        ..Default::default()
    }
}

async fn legacyize(
    store: &Storage,
    segments: &mut [Segment],
    attr: &str,
    kind: &str,
    row_major: bool,
) {
    for seg in segments {
        let file = seg.files.remove(&format!("annv2:{attr}")).unwrap();
        let bytes = store.get(&file.key).await.unwrap();
        let old = bf_vector::downgrade_layout(&bytes).unwrap().unwrap();
        let old = if row_major {
            old
        } else {
            bf_vector::upgrade_layout(&old).unwrap().unwrap_or(old)
        };
        let stem = format!("ns/t/test/{}", uuid::Uuid::new_v4());
        let meta = bf_vector::metadata_len(&old).unwrap();
        for blocks in [false, true] {
            if kind == "index" || (kind == "plain" && blocks) {
                continue;
            }
            // Sparse global slab rows have no dense plain membership map.
            if !blocks && !seg.files.contains_key("docs") {
                continue;
            }
            let (bytes, meta) = bf_vector::Fragment::legacy_fragment(&old, blocks).unwrap();
            let name = if blocks { "annfrag2" } else { "annfrag" };
            let ext = if blocks { "bfb" } else { "bff" };
            let file = segment::put_file(store, format!("{stem}.{ext}"), bytes, meta)
                .await
                .unwrap();
            seg.files.insert(format!("{name}:{attr}"), file);
        }
        let file = segment::put_file(store, format!("{stem}.bfv"), old, meta)
            .await
            .unwrap();
        seg.files.insert(format!("ann:{attr}"), file);
    }
}

async fn fixture() -> (Storage, Engine, Manifest) {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        crate::EngineConfig {
            indexing: false,
            ann_nprobe: 4,
            ann_rerank_factor: 4,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    for batch in 0..4 {
        let rows = (0..3000)
            .map(|n| {
                let id = batch * 3000 + n;
                json!({"id":id,"bucket":id%3,"vector":test_vector(id)})
            })
            .collect::<Vec<_>>();
        engine
            .write(
                "t",
                serde_json::from_value(
                    json!({"distance_metric":"euclidean_squared","upsert_rows":rows}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("t", true).await.unwrap();
    }
    let (_, m) = Wal::new(store.clone(), "t")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    (store, engine, m)
}
fn test_vector(id: usize) -> Vec<f32> {
    (0..32)
        .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
        .collect()
}

async fn ann_answer(
    store: &Storage,
    engine: &Engine,
    m: &Manifest,
    filtered: bool,
    slab_only: bool,
) -> std::collections::BTreeMap<bf_types::Id, f64> {
    let vector = test_vector(4321);
    let mut q: bf_types::QueryRequest = serde_json::from_value(
        json!({"rank_by":["vector","ANN",vector],"top_k":10,"include_attributes":false}),
    )
    .unwrap();
    if filtered {
        q.filters = Some(serde_json::from_value(json!(["bucket", "Eq", 1])).unwrap());
    }
    let mut state = state(m);
    if slab_only {
        // An unsuccessful slab path cannot quietly answer from segment indexes.
        for seg in state.segments.iter_mut() {
            seg.files.retain(|k, _| !k.starts_with("ann"));
        }
    }
    crate::ann::search(
        store,
        &state,
        &q,
        "vector",
        &vector,
        &engine.0.config,
        10,
        &BTreeSet::new(),
    )
    .await
    .unwrap()
    .expect("upgrade must answer through ANN")
    .0
}

#[tokio::test]
async fn every_legacy_reader_keeps_ann_at_each_segment_checkpoint() {
    let (store, engine, current) = fixture().await;
    let before = ann_answer(&store, &engine, &current, false, false).await;
    let filtered_before = ann_answer(&store, &engine, &current, true, false).await;
    for kind in ["blocks", "plain", "index", "earlier-fragment"] {
        for row_major in [false, true] {
            let mut m = current.clone();
            legacyize(&store, &mut m.segments, "vector", kind, row_major).await;
            if kind == "earlier-fragment" {
                for seg in &mut m.segments {
                    for prefix in ["annfrag", "annfrag2"] {
                        let file = seg.files.get_mut(&format!("{prefix}:vector")).unwrap();
                        file.size = 16;
                        file.metadata_len = 16;
                        let key = format!("{}-earlier", file.key);
                        store.create_unique(&key, vec![0; 16]).await.unwrap();
                        file.key = key;
                    }
                }
            }

            m.ann_dirs.clear();
            for n in 0..=4 {
                assert_eq!(
                    ann_answer(&store, &engine, &m, false, false).await,
                    before,
                    "{kind}, row_major={row_major}, checkpoint={n}"
                );
                assert_eq!(
                    ann_answer(&store, &engine, &m, true, false).await,
                    filtered_before,
                    "filtered {kind}, row_major={row_major}, checkpoint={n}"
                );
                if n == 4 {
                    break;
                }
                let seg = &m.segments[n];
                let c = engine
                    .convert_segment_index("t", seg, "vector", &seg.files["ann:vector"])
                    .await
                    .unwrap();
                apply_segments(&store, "t", &mut m, &[c]).await.unwrap();
            }
        }
    }
}

async fn force_pages(store: &Storage, m: &mut Manifest) {
    let set = &mut m.ann_tiers.get_mut("vector").unwrap()[0];
    let dir = ann_dir::load(store, &set.dir, None).await.unwrap();
    let files = set
        .slabs
        .iter()
        .zip(dir.segments())
        .map(|(s, d)| {
            let size = |key: &str| {
                let f = s.files.values().find(|f| f.key == key).unwrap();
                (f.size, f.metadata_len)
            };
            [size(&d.key), size(&d.fragment)]
        })
        .collect::<Vec<_>>();
    let (bytes, meta) = dir.build_paged(8, 7, &files).unwrap();
    set.pages = Some(
        segment::put_file(
            store,
            format!("ns/t/test/{}.bfd", uuid::Uuid::new_v4()),
            bytes,
            meta,
        )
        .await
        .unwrap(),
    );
}

#[tokio::test]
async fn old_slabs_keep_serving_through_flat_and_paged_conversion_batches() {
    let (store, engine, _) = fixture().await;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
    std::fs::create_dir_all(&root).unwrap();
    let scratch = tempfile::tempdir_in(root).unwrap();
    engine
        .recluster(
            "t",
            &crate::ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 60,
                max_slab_rows: 3000,
                scratch: scratch.path().join("spill"),
            },
        )
        .await
        .unwrap();
    let (_, current) = Wal::new(store.clone(), "t")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    let before = ann_answer(&store, &engine, &current, false, true).await;
    for paged in [false, true] {
        let mut m = current.clone();
        let set = &mut m.ann_tiers.get_mut("vector").unwrap()[0];
        legacyize(&store, &mut set.slabs, "vector", "blocks", false).await;
        set.dir = ann_dir::build(
            &store,
            "t",
            &set.slabs,
            "vector",
            ann_dir::Layout::Slabs,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        set.pages = None;
        let count = set.slabs.len();
        assert!(count >= 2);
        for n in 0..=count {
            if paged {
                force_pages(&store, &mut m).await;
            }
            assert_eq!(
                ann_answer(&store, &engine, &m, false, true).await,
                before,
                "paged={paged}, checkpoint={n}/{count}"
            );
            if n == count {
                break;
            }
            let slab = &m.ann_tiers["vector"][0].slabs[n];
            let c = engine
                .convert_slab("t", slab, "vector", &slab.files["ann:vector"])
                .await
                .unwrap();
            engine
                .apply_slabs("t", &mut m, "vector", &[c])
                .await
                .unwrap();
        }
    }
}

/// Reproduce the migration benchmark without a server or production storage. Inputs, logs,
/// and output belong under .scratch; TempDir removes the object store even on test failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "200K migration benchmark; run in release mode on CPUs 6–7"]
async fn migration_200k_benchmark() {
    use std::io::Read;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
    std::fs::create_dir_all(&root).unwrap();
    let scratch = tempfile::tempdir_in(root).unwrap();
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", scratch.path().display()),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        crate::EngineConfig {
            indexing: false,
            ann_nprobe: 48,
            ann_rerank_factor: 32,
            recall_sample_rate: 0.0,
            compaction_segments: 100,
            ..Default::default()
        },
    );
    let rows = 200_000;
    let dims = 1024;
    let mut corpus = std::env::var("D1_CORPUS_F16")
        .ok()
        .map(|p| std::fs::File::open(p).unwrap());
    let mut seed_buffer = vec![0; dims * 2];
    let synth = |id: usize| {
        let mut center = (id % 256) as u64 + 123;
        let mut noise = id as u64 + 9_876_543;
        let random = |seed: &mut u64| {
            *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((*seed >> 32) as u32 as f64 / u32::MAX as f64 * 2. - 1.) as f32
        };
        (0..dims)
            .map(|_| random(&mut center) + random(&mut noise) * 0.15)
            .collect::<Vec<_>>()
    };
    for batch in 0..4 {
        let mut docs = Vec::new();
        for id in batch * 50_000..(batch + 1) * 50_000 {
            let v = if let Some(file) = &mut corpus {
                file.read_exact(&mut seed_buffer).unwrap();
                seed_buffer
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|c| half::f16::from_le_bytes(*c).to_f32())
                    .collect::<Vec<_>>()
            } else {
                synth(id)
            };
            docs.push(json!({"id":id,"bucket":id%16,"vector":v}));
        }
        engine.write("t",serde_json::from_value(json!({"distance_metric":"cosine_distance","schema":{"vector":{"type":"[1024]f16","ann":true}},"upsert_rows":docs})).unwrap()).await.unwrap();
        engine.index_namespace("t", true).await.unwrap();
    }
    let queries = if let Ok(path) = std::env::var("D1_QUERIES_F32") {
        std::fs::read(path)
            .unwrap()
            .chunks_exact(dims * 4)
            .take(64)
            .map(|row| {
                row.as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| f32::from_le_bytes(*c))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    } else {
        (0..64).map(|i| synth(rows + i * 7919)).collect()
    };
    let requests = queries
        .iter()
        .flat_map(|v| {
            [false, true].map(|filtered| {
                let mut q =
                    json!({"rank_by":["vector","ANN",v],"top_k":10,"include_attributes":false});
                if filtered {
                    q["filters"] = json!(["bucket", "Eq", 3]);
                }
                serde_json::from_value::<bf_types::QueryRequest>(q).unwrap()
            })
        })
        .collect::<Vec<_>>();
    let wal = Wal::new(store.clone(), "t").unwrap();
    for slab in [false, true] {
        if slab {
            engine
                .recluster(
                    "t",
                    &crate::ReclusterOptions {
                        attribute: "vector".into(),
                        cells: 6,
                        cluster_rows: 200,
                        max_slab_rows: 60_000,
                        scratch: scratch.path().join("spill"),
                    },
                )
                .await
                .unwrap();
        }
        let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
        let mut truth = Vec::new();
        for request in requests.iter().take(32) {
            let mut q = serde_json::to_value(request).unwrap();
            q["rank_by"][1] = json!("kNN");
            if q.get("filters").is_none() || q["filters"].is_null() {
                q["filters"] = json!(["id", "Gte", 0]);
            }
            let result = engine
                .query("t", serde_json::from_value(q).unwrap())
                .await
                .unwrap();
            truth.push(
                result
                    .result
                    .rows
                    .unwrap()
                    .into_iter()
                    .map(|r| r["id"].clone())
                    .collect::<Vec<_>>(),
            );
        }
        if slab {
            legacyize(
                &store,
                &mut m.ann_tiers.get_mut("vector").unwrap()[0].slabs,
                "vector",
                "blocks",
                false,
            )
            .await;
        } else {
            legacyize(&store, &mut m.segments, "vector", "blocks", false).await;
            m.ann_dirs.clear();
        }
        if slab {
            let set = &mut m.ann_tiers.get_mut("vector").unwrap()[0];
            set.dir = ann_dir::build(
                &store,
                "t",
                &set.slabs,
                "vector",
                ann_dir::Layout::Slabs,
                None,
            )
            .await
            .unwrap()
            .unwrap();
            set.pages = None;
        }
        wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
        let total = if slab {
            m.ann_tiers["vector"][0].slabs.len()
        } else {
            m.segments.len()
        };
        let mut baseline: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut converted = 0;
        for target in [0, total / 2, total] {
            while converted < target {
                let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
                let seg = if slab {
                    &m.ann_tiers["vector"][0].slabs[converted]
                } else {
                    &m.segments[converted]
                };
                let c = engine
                    .convert_segment_index("t", seg, "vector", &seg.files["ann:vector"])
                    .await
                    .unwrap();
                let batch = if slab {
                    Batch::Slabs("vector".into(), vec![c])
                } else {
                    Batch::Segments(vec![c])
                };
                engine
                    .publish_batch(&wal, "t", batch, &mut ReindexReport::default())
                    .await
                    .unwrap();
                converted += 1;
            }
            // A new engine checks durable publication and gives one first-query sample, followed
            // by a full warm-up and timed pass of identical requests at each checkpoint.
            let reader = Engine::with_config(store.clone(), engine.0.config.clone());
            let tick = std::time::Instant::now();
            reader.query("t", requests[0].clone()).await.unwrap();
            let first_ms = tick.elapsed().as_secs_f64() * 1000.;
            for q in &requests {
                reader.query("t", q.clone()).await.unwrap();
            }
            let mut times = [Vec::new(), Vec::new()];
            let mut scanned = [Vec::new(), Vec::new()];
            let mut recall = [0., 0.];
            let mut output = Vec::new();
            for (i, q) in requests.iter().enumerate() {
                let tick = std::time::Instant::now();
                let (result, d) = reader.query_with_diagnostics("t", q.clone()).await.unwrap();
                times[i % 2].push(tick.elapsed().as_secs_f64() * 1000.);
                scanned[i % 2].push(d.ann_rows_scanned);
                assert_eq!(
                    d.document_scan_rows, 0,
                    "checkpoint must never scan document vectors"
                );
                assert!(
                    d.ann_rows_scanned > 0 && d.ann_rows_scanned < rows as u64,
                    "ANN must visit fewer than every row"
                );
                let ids = result
                    .result
                    .rows
                    .unwrap()
                    .into_iter()
                    .map(|r| r["id"].clone())
                    .collect::<Vec<_>>();
                if i < truth.len() {
                    recall[i % 2] +=
                        ids.iter().filter(|id| truth[i].contains(id)).count() as f64 / 10.;
                }
                output.push(ids);
            }
            if target == 0 {
                baseline = output;
            } else {
                assert_eq!(
                    output, baseline,
                    "migration preserves every query's neighbors"
                );
            }
            for kind in 0..2 {
                times[kind].sort_by(f64::total_cmp);
                scanned[kind].sort_unstable();
                eprintln!(
                    "D1_MIGRATION {}",
                    json!({"layout":if slab {"slabs"} else {"segments"},"converted":target,"objects":total,"percent":100*target/total,"filtered":kind==1,"p50_ms":times[kind][31],"p90_ms":times[kind][57],"first_query_ms":first_ms,"rows_p50":scanned[kind][31],"rows_min":scanned[kind][0],"rows_max":scanned[kind][63],"recall10":recall[kind]/16.})
                );
            }
        }
    }
}
