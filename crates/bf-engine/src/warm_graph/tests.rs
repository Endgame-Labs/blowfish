use super::*;
use crate::{EngineConfig, ReclusterOptions};
use bf_storage::{CacheConfig, StorageConfig};
use bf_types::WriteRequest;
use serde_json::{Value, json};
struct Cleanup(std::path::PathBuf);
#[test]
fn source_locations_compact_dense_rows_and_preserve_sparse_coordinates() {
    let count = 10000;
    let source = (0..count)
        .rev()
        .map(|row| (row, [row / 100, row % 100, row * 3]))
        .collect::<Vec<_>>();
    let dense = Locations::new(source.clone());
    assert!(dense.resident_bytes() < u64::from(count) * 16);
    for (row, at) in source {
        assert_eq!(dense.get(row), Some(at));
    }
    assert_eq!(dense.get(count), None);
    assert_eq!(dense.get(u32::MAX), None);
    let sparse = Locations::new(vec![(u32::MAX, [7, 8, 9]), (0, [1, 2, 3]), (12, [4, 5, 6])]);
    for (row, at) in [(0, [1, 2, 3]), (12, [4, 5, 6]), (u32::MAX, [7, 8, 9])] {
        assert_eq!(sparse.get(row), Some(at));
    }
    assert_eq!(sparse.get(1), None);
    assert_eq!(sparse.get(u32::MAX - 1), None);
    assert_eq!(Locations::new(Vec::new()).get(0), None);
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
async fn query(e: &Engine, ns: &str, body: Value) -> Value {
    serde_json::to_value(
        e.query(ns, serde_json::from_value(body).unwrap())
            .await
            .unwrap(),
    )
    .unwrap()["rows"]
        .clone()
}
fn same_rows(left: Value, right: Value) {
    assert_eq!(left, right);
    for (a, b) in left
        .as_array()
        .unwrap()
        .iter()
        .zip(right.as_array().unwrap())
    {
        assert_eq!(
            a["$dist"].as_f64().map(f64::to_bits),
            b["$dist"].as_f64().map(f64::to_bits)
        );
    }
}
// Other unit tests create engines with large nominal cache budgets in this process. A real
// optional graph may refuse that pressure. Wait for admission rather than assuming exclusivity.
async fn admitted_entry(e: &Engine, set: &SlabSet) -> Entry {
    let started = Instant::now();
    loop {
        let result = e
            .construct_hot_graph("hot", "vector", set, &HotCancellation::default())
            .await;
        match result {
            Ok(Some(entry)) => return entry,
            result => {
                assert!(
                    started.elapsed() < Duration::from_secs(120),
                    "test graph was never admitted; last result was an error: {}",
                    result.is_err()
                );
                let answer = query(e, "hot", json!({"rank_by":["id","asc"],"top_k":1})).await;
                assert_eq!(answer.as_array().unwrap().len(), 1);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_identity_masks_wal_fallback_and_reservation_lifetimes() {
    let _exclusive = ADMISSION_GATE.write().await;
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.scratch")
        .join(format!("g4-lifecycle-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let config = StorageConfig {
        url: "memory://".into(),
        cache: CacheConfig {
            disk_dir: Some(root.join("cache")),
            memory_bytes: 32 << 20,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = Storage::open(&config).await.unwrap();
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.,
        decoded_cache_bytes: 64 << 20,
        ..Default::default()
    };
    let e = Engine::with_config(store.clone(), config.clone());
    let control = Engine::with_config(store.clone(), config.clone());
    let mut seed = 7u64;
    let vectors = (0..2500)
        .map(|_| {
            (0..16)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    (seed >> 40) as f32 / 16777216. * 2. - 1.
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let rows = vectors
        .iter()
        .enumerate()
        .map(|(id, v)| json!({"id":id,"vector":v,"group":id%3}))
        .collect::<Vec<_>>();
    e.write("hot",serde_json::from_value::<WriteRequest>(json!({"distance_metric":"cosine_distance","schema":{"vector":{"type":"[16]f16","ann":true}},"upsert_rows":rows})).unwrap()).await.unwrap();
    e.index_namespace("hot", true).await.unwrap();
    let opts = ReclusterOptions {
        attribute: "vector".into(),
        cells: 2,
        cluster_rows: 32,
        max_slab_rows: 5000,
        scratch: root.clone(),
    };
    e.run_recluster_job("hot", &opts).await.unwrap();
    let (state, _) = e
        .snapshot("hot", &bf_types::Consistency::Strong)
        .await
        .unwrap();
    let set = &state.ann_tiers["vector"][0];
    let key = Key::new("hot", &state, "vector", set);
    // The private constructor exercises exactly the automatic source/build path, without
    // making a test spend ten minutes generating heat.
    let entry = admitted_entry(&e, set).await;
    assert_eq!(entry.graph.len(), 2500);
    let held = Arc::new(entry);
    let mut heat = Heat::new(Instant::now());
    heat.rate = 100.;
    heat.ivf_seconds = 1.;
    heat.ready = Some(held.clone());
    e.0.hot_graph
        .entries
        .lock()
        .unwrap()
        .insert(key.clone(), heat);
    let request = |i: usize| json!({"rank_by":["vector","ANN",vectors[i]],"top_k":10,"include_attributes":["group"]});
    assert_eq!(
        query(&e, "hot", request(17)).await,
        query(&control, "hot", request(17)).await,
        "uncalibrated graphs retain IVF"
    );
    assert_eq!(e.0.hot_graph.answers.load(Ordering::Relaxed), 0);
    // Verify the real exact-sampling integration, then drive economics and quality with
    // deterministic observations. Tiny fixtures need not have a profitable graph, and a
    // lifecycle test must not depend on the scheduler or improvements to IVF recall.
    let last_foreground = e.0.hot_graph.entries.lock().unwrap()[&key].last;
    e.recall(
        "hot",
        json!({"num":1,"top_k":10,"rank_by":["vector","ANN",vectors[17]]}),
    )
    .await
    .unwrap();
    assert!(held.searches.load(Ordering::Relaxed) > 0);
    assert_eq!(
        e.0.hot_graph.entries.lock().unwrap()[&key].last,
        last_foreground,
        "recall graph searches must not retime foreground heat observations"
    );
    assert_eq!(
        e.0.hot_graph.entries.lock().unwrap()[&key].ivf_seconds,
        1.,
        "an individual scan-interleaved trial must not replace the warm IVF baseline"
    );
    for i in 1..8 {
        e.recall(
            "hot",
            json!({"num":1,"top_k":10,"rank_by":["vector","ANN",vectors[17+i]]}),
        )
        .await
        .unwrap();
    }
    assert!(
        e.0.hot_graph.entries.lock().unwrap()[&key].ivf_seconds < 1.,
        "the paired warm block must replace the initial IVF baseline"
    );
    {
        let mut calibration = held.calibration.lock().unwrap();
        calibration.quality = effort::Effort::new();
        calibration.build_seconds = 1.;
    }
    for _ in 0..128 {
        let scope = Scope::new(e.0.hot_graph.clone(), "hot").trial();
        *scope.used.lock().unwrap() = Some(vec![key.clone()]);
        held.calibration.lock().unwrap().trial_ratio = 0.1;
        scope.record_ivf_cpu(0.01);
        scope.feedback(1., 1.);
    }
    assert!(
        held.calibration.lock().unwrap().usable,
        "matched quality and positive measured payback must admit the graph"
    );
    // A graph selected during planning can lose its query scratch slot before execution.
    // The complete ordinary IVF plan must be restored rather than keeping independent seats.
    let permit = held
        .slots
        .acquire_many(held.slots.available_permits() as u32)
        .await
        .unwrap();
    let before = e.0.hot_graph.answers.load(Ordering::Relaxed);
    assert_eq!(
        query(&e, "hot", request(17)).await,
        query(&control, "hot", request(17)).await
    );
    assert_eq!(e.0.hot_graph.answers.load(Ordering::Relaxed), before);
    drop(permit);
    let before = e.0.hot_graph.answers.load(Ordering::Relaxed);
    assert_eq!(query(&e, "hot", request(17)).await[0]["id"], 17);
    assert!(e.0.hot_graph.answers.load(Ordering::Relaxed) > before);
    // Round G8: requests up to 100 rows (k plus offset) are served by the graph with an automatic
    // beam and shortlist, and return the IVF control's rows. Since Round G16 the walk screens
    // neighbors with one-bit codes, so it is approximate: beyond the first rows it may swap a
    // near-tie for its neighbor, as on NQ (recall@100 -0.0013). The first ten must match
    // exactly; the deep pages share at least 95 percent of their rows with the control, in
    // distance order and without repeats.
    for body in [
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":100}),
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":40,"offset":50}),
    ] {
        let before = e.0.hot_graph.answers.load(Ordering::Relaxed);
        let got = query(&e, "hot", body.clone()).await;
        let want = query(&control, "hot", body.clone()).await;
        let (got, want) = (got.as_array().unwrap(), want.as_array().unwrap());
        assert_eq!(got.len(), want.len());
        if body.get("offset").is_none() {
            assert_eq!(got[..10], want[..10]);
        }
        let ids = |rows: &[Value]| {
            rows.iter()
                .map(|r| r["id"].as_u64().unwrap())
                .collect::<std::collections::HashSet<_>>()
        };
        let (g, w) = (ids(got), ids(want));
        assert_eq!(g.len(), got.len(), "repeated rows");
        assert!(
            g.intersection(&w).count() * 100 >= want.len() * 95,
            "graph shares {} of {} rows with IVF",
            g.intersection(&w).count(),
            want.len()
        );
        let dist = |r: &Value| r["$dist"].as_f64().unwrap();
        assert!(got.windows(2).all(|p| dist(&p[0]) <= dist(&p[1])));
        assert!(e.0.hot_graph.answers.load(Ordering::Relaxed) > before);
    }
    // Round G12: an ID-only query is answered from the resident row-to-ID table once the
    // background build finishes, with the same ordered rows and score bits as IVF and as the
    // document path (the first pass, before the table exists).
    {
        let ids_only =
            |i: usize, k: usize| json!({"rank_by":["vector","ANN",vectors[i]],"top_k":k});
        let mut first = Vec::new();
        for i in [17, 400, 1999] {
            for k in [10, 100, 37, 1] {
                first.push(query(&e, "hot", ids_only(i, k)).await);
            }
        }
        let started = Instant::now();
        while held.ids.get().is_none() && started.elapsed() < Duration::from_secs(30) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(held.ids.get().is_some(), "the ID table was never built");
        let mut want = first.into_iter();
        for i in [17, 400, 1999] {
            for k in [10, 100, 37, 1] {
                let table = query(&e, "hot", ids_only(i, k)).await;
                same_rows(want.next().unwrap(), table.clone());
                same_rows(query(&control, "hot", ids_only(i, k)).await, table);
            }
        }
    }
    for body in [
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":10,"filters":["group","Eq",1]}),
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":101}),
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":1,"offset":100}),
        json!({"rank_by":["vector","ANN",vec![0.;16]],"top_k":10}),
        json!({"rank_by":["vector","kNN",vectors[17]],"top_k":10,"filters":["group","Eq",1]}),
    ] {
        let before = e.0.hot_graph.answers.load(Ordering::Relaxed);
        same_rows(
            query(&e, "hot", body.clone()).await,
            query(&control, "hot", body).await,
        );
        assert_eq!(e.0.hot_graph.answers.load(Ordering::Relaxed), before);
    }
    e.write(
        "hot",
        serde_json::from_value::<WriteRequest>(
            json!({"deletes":[17],"upsert_rows":(0..50).map(|i| json!({"id":5000+i,"vector":vectors[if i==0 {17} else {30+i}],"group":1})).collect::<Vec<_>>()}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(query(&e, "hot", request(17)).await[0]["id"], 5000);
    e.index_namespace("hot", true).await.unwrap();
    assert_eq!(query(&e, "hot", request(17)).await[0]["id"], 5000);
    e.run_recluster_job("hot", &opts).await.unwrap();
    assert_eq!(query(&e, "hot", request(17)).await[0]["id"], 5000);
    let (_, new_state) = Wal::new(store.clone(), "hot")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    assert!(
        new_state.ann_tiers["vector"].len() > 1,
        "deltas stay independently searchable"
    );
    // A successful delta-graph plan can be discarded when the base loses scratch admission.
    // The returned IVF result must not feed either graph's recall or CPU calibration.
    let (current, _) = e
        .snapshot("hot", &bf_types::Consistency::Strong)
        .await
        .unwrap();
    let delta_set = current.ann_tiers["vector"]
        .iter()
        .find(|s| s.dir.key != key.tier)
        .unwrap();
    let delta_key = Key::new("hot", &current, "vector", delta_set);
    let delta = Arc::new(admitted_entry(&e, delta_set).await);
    assert_eq!(delta.graph.len(), 50);
    let mut delta_heat = Heat::new(Instant::now());
    delta_heat.rate = 100.;
    delta_heat.ivf_seconds = 1.;
    delta_heat.ready = Some(delta.clone());
    e.0.hot_graph
        .entries
        .lock()
        .unwrap()
        .insert(delta_key.clone(), delta_heat);
    let permit = held
        .slots
        .acquire_many(held.slots.available_permits() as u32)
        .await
        .unwrap();
    let answered = e.0.hot_graph.answers.load(Ordering::Relaxed);
    e.recall(
        "hot",
        json!({"num":1,"top_k":10,"rank_by":["vector","ANN",vectors[17]]}),
    )
    .await
    .unwrap();
    assert!(
        e.0.hot_graph.answers.load(Ordering::Relaxed) > answered,
        "the delta must actually contribute before the complete IVF retry"
    );
    // Parallel tests can legitimately evict the optional base graph. In that case the
    // delta and ordinary base IVF form a successful plan, so its feedback is valid.
    let base_retained =
        e.0.hot_graph
            .entries
            .lock()
            .unwrap()
            .get(&key)
            .and_then(|h| h.ready.as_ref())
            .is_some_and(|base| Arc::ptr_eq(base, &held));
    if base_retained {
        let calibration = delta.calibration.lock().unwrap();
        assert_eq!(
            calibration.quality.len(),
            0,
            "discarded plans cannot calibrate recall"
        );
        assert_eq!(
            calibration.graph_ratio, 0.,
            "discarded plans cannot calibrate CPU savings"
        );
    }
    // Check the abandonment contract independently of optional global admission pressure.
    let before_samples = delta.calibration.lock().unwrap().quality.len();
    let scope = Scope::new(e.0.hot_graph.clone(), "hot").trial();
    *scope.used.lock().unwrap() = Some(vec![delta_key.clone()]);
    scope.abandon();
    scope.record_ivf_cpu(1.);
    scope.feedback(0., 1.);
    assert_eq!(
        delta.calibration.lock().unwrap().quality.len(),
        before_samples
    );
    assert_eq!(delta.calibration.lock().unwrap().graph_ratio, 0.);
    drop(permit);
    e.0.hot_graph.entries.lock().unwrap().remove(&delta_key);
    drop(delta);
    // Engine ownership includes the backend, even when namespaces have identical names.
    let other = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let other = Engine::with_config(other, config);
    assert!(other.0.hot_graph.entries.lock().unwrap().is_empty());
    assert!(
        control
            .0
            .hot_graph
            .entries
            .lock()
            .unwrap()
            .values()
            .all(|h| h.ready.is_none() && h.building.is_none())
    );
    // Above ten percent masked rows the exact same request uses IVF, including score bits.
    e.write(
        "hot",
        serde_json::from_value::<WriteRequest>(json!({"deletes":(0..350).collect::<Vec<_>>()}))
            .unwrap(),
    )
    .await
    .unwrap();
    e.index_namespace("hot", true).await.unwrap();
    control.drop_caches().await;
    same_rows(
        query(&e, "hot", request(531)).await,
        query(&control, "hot", request(531)).await,
    );
    assert!(
        e.0.hot_graph
            .entries
            .lock()
            .unwrap()
            .get(&key)
            .unwrap()
            .ready
            .is_none()
    );
    // A query's Arc retains its charge after whole-graph eviction, until the query finishes.
    let charge = held._reservation.0;
    assert!(RESERVED.load(Ordering::Relaxed) >= charge);
    e.delete_namespace("hot").await.unwrap();
    assert!(e.0.hot_graph.entries.lock().unwrap().is_empty());
    drop(held);
    assert!(e.shutdown(Duration::from_secs(5)).await);
    assert!(control.shutdown(Duration::from_secs(5)).await);
    assert!(other.shutdown(Duration::from_secs(5)).await);
}
#[test]
fn sustained_heat_resets_after_a_gap_and_decays() {
    let now = Instant::now();
    let mut heat = Heat::new(now);
    for second in 1..=601 {
        heat.observe(now + Duration::from_secs(second), 0.002);
    }
    assert!(heat.last.duration_since(heat.first) >= DWELL);
    assert!(heat.rate > 0.4);
    let old = heat.rate;
    heat.observe(now + Duration::from_secs(1601), 0.002);
    assert_eq!(heat.first, heat.last);
    assert!(heat.rate < old / 2.);
    let mut busy = Heat::new(now);
    busy.rate = 800.;
    assert!(busy.rate_at(now + Duration::from_secs(60)) < 750.);
    assert_eq!(
        busy.rate, 800.,
        "read-only heat evaluation must not invent arrivals"
    );
    busy.observe(now + Duration::from_secs(60), 0.001);
    assert!(
        busy.rate < 750.,
        "a real gap must decay an already warm rate"
    );
    busy.observe(now + Duration::from_secs(59), 0.001);
    assert_eq!(
        busy.last,
        now + Duration::from_secs(60),
        "late lock acquisition must not move the heat clock backwards"
    );
}
#[test]
fn cancelled_entries_are_dropped_on_namespace_generation_replacement() {
    let cache = Cache::default();
    let mut state = NamespaceState::default();
    let cancel = HotCancellation::default();
    let key = Key {
        namespace: "a".into(),
        generation: state.created_at,
        attribute: "vector".into(),
        tier: "old".into(),
    };
    let mut heat = Heat::new(Instant::now());
    heat.building = Some(cancel.clone());
    cache.entries.lock().unwrap().insert(key, heat);
    state.created_at += chrono::Duration::seconds(1);
    cache.prune("a", &state, Instant::now());
    assert!(cancel.cancelled());
    assert!(cache.entries.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn migrated_query_polls_charge_cpu_without_charging_io_waits() {
    let cpu = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let name = crate::query_worker::run(
        measure_cpu(cpu.clone(), async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            std::thread::current()
                .name()
                .unwrap_or_default()
                .to_string()
        }),
        false,
    )
    .await
    .unwrap();
    assert!(
        !name.starts_with("bf-query-"),
        "a yielded plan must continue on the I/O worker"
    );
    let charged = Duration::from_nanos(cpu.load(Ordering::Relaxed));
    assert!(charged > Duration::ZERO);
    assert!(
        charged < start.elapsed() / 2,
        "timer waits are not foreground CPU"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recall_trials_admit_sparse_graph_reads_without_admitting_the_exact_scan() {
    let _exclusive = ADMISSION_GATE.write().await;
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.scratch")
        .join(format!("g4-recall-cache-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        cache: CacheConfig {
            block_size: 16 << 10,
            // The graph and paired IVF working sets fit; the separate truth objects do not.
            memory_bytes: 4 << 20,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .unwrap();
    let e = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            decoded_cache_bytes: 64 << 20,
            query_concurrency: 1,
            ..Default::default()
        },
    );
    let mut seed = 19u64;
    let vectors = (0..20_000)
        .map(|_| {
            (0..64)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    (seed >> 40) as f32 / 16777216. * 2. - 1.
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    e.write("hot", serde_json::from_value(json!({
        "distance_metric":"cosine_distance",
        "schema":{"vector":{"type":"[64]f16","ann":true}},
        "upsert_rows":vectors.iter().enumerate().map(|(id,v)| json!({"id":id,"vector":v})).collect::<Vec<_>>()
    })).unwrap()).await.unwrap();
    e.index_namespace("hot", true).await.unwrap();
    e.run_recluster_job(
        "hot",
        &ReclusterOptions {
            attribute: "vector".into(),
            cells: 2,
            cluster_rows: 64,
            max_slab_rows: 20_000,
            scratch: root.clone(),
        },
    )
    .await
    .unwrap();
    let (state, _) = e
        .snapshot("hot", &bf_types::Consistency::Strong)
        .await
        .unwrap();
    let set = &state.ann_tiers["vector"][0];
    let key = Key::new("hot", &state, "vector", set);
    let entry = Arc::new(admitted_entry(&e, set).await);
    let mut heat = Heat::new(Instant::now());
    heat.rate = 100.;
    heat.ivf_seconds = 1.;
    heat.ready = Some(entry.clone());
    e.0.hot_graph.entries.lock().unwrap().insert(key, heat);
    store.drop_caches();
    e.0.read_cache.clear();
    let vector = &vectors[171];
    let (hits, _) = entry
        .graph
        .candidates(vector, 64, 30, None, &mut entry.graph.scratch())
        .unwrap();
    let [slab, cluster, ordinal] = entry.locations.get(hits[0]).unwrap();
    let probe = || {
        let mut context = crate::read_cache::ReadContext::new(e.0.read_cache.clone());
        context.admit = false;
        context
    };
    let before = probe();
    let before_stats = before.stats.clone();
    crate::read_cache::scope(
        before,
        entry.sources[slab as usize].rerank_candidates(vector, 1, vec![(cluster, ordinal)]),
    )
    .await
    .unwrap();
    assert_eq!(before_stats.cache_hit_ratio(), 0.);
    let answered = e.0.hot_graph.answers.load(Ordering::Relaxed);
    e.recall(
        "hot",
        json!({"num":1,"top_k":10,"rank_by":["vector","ANN",vector]}),
    )
    .await
    .unwrap();
    assert_eq!(
        e.0.hot_graph.answers.load(Ordering::Relaxed),
        answered + 1,
        "the IVF cost reference and exhaustive truth must not use the graph"
    );
    assert_eq!(entry.searches.load(Ordering::Relaxed), 1);
    assert_eq!(
        e.0.hot_graph
            .entries
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .ivf_seconds,
        1.,
        "single cold IVF observations cannot skew promotion economics"
    );
    assert!(
        entry
            .phase_cpu
            .iter()
            .all(|v| v.load(Ordering::Relaxed) > 0)
    );
    let after = probe();
    let after_stats = after.stats.clone();
    crate::read_cache::scope(
        after,
        entry.sources[slab as usize].rerank_candidates(vector, 1, vec![(cluster, ordinal)]),
    )
    .await
    .unwrap();
    assert!(
        after_stats.cache_hit_ratio() > 0.,
        "ANN's sparse vector must remain cached after the larger exhaustive pass"
    );
    // Eight truth observations produce one cost block. Its graph plans run twice (an untimed
    // warm-up pass, then the timed one), while IVF cost plans and exact truth never select the graph.
    assert_eq!(entry.calibration.lock().unwrap().trial_seconds, 0.);
    for i in 1..8 {
        e.recall(
            "hot",
            json!({"num":1,"top_k":10,"rank_by":["vector","ANN",vectors[171+i]]}),
        )
        .await
        .unwrap();
    }
    assert_eq!(entry.searches.load(Ordering::Relaxed), 24);
    assert_eq!(e.0.hot_graph.answers.load(Ordering::Relaxed), answered + 24);
    assert!(entry.calibration.lock().unwrap().trial_seconds > 0.);
    assert!(entry.pending_cost.lock().unwrap().queries.is_empty());
    e.shutdown(Duration::from_secs(3)).await;
}

#[cfg(target_os = "linux")]
#[test]
fn production_priority_is_applied_without_waiting_for_idle_work() {
    let (tid_tx, tid_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        // SAFETY: gettid returns the calling thread's kernel identifier.
        let tid = unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t };
        tid_tx.send(tid).unwrap();
        background_priority();
        // The inspecting thread releases this channel; it never waits for work scheduled
        // after lowering priority. Dropping the handle lets this thread exit when scheduled.
        let _ = release_rx.recv();
    });
    let tid = tid_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let started = Instant::now();
    loop {
        // SAFETY: these read-only calls inspect our live worker by kernel thread ID.
        let (nice, policy) = unsafe {
            (
                libc::getpriority(libc::PRIO_PROCESS, tid as u32),
                libc::sched_getscheduler(tid),
            )
        };
        if nice == 19 && (policy == libc::SCHED_IDLE || policy == libc::SCHED_OTHER) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "priority was not applied: nice={nice}, policy={policy}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    release_tx.send(()).unwrap();
    drop(worker);
    // Lifecycle tests retain normal scheduling even while other builds saturate the host.
    assert!(
        pool()
            .broadcast(|_| {
                // SAFETY: inspect only the calling test-pool worker.
                unsafe { libc::sched_getscheduler(0) != libc::SCHED_IDLE }
            })
            .into_iter()
            .all(|normal| normal)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_metric_and_precision_keep_identical_ivf_answers() {
    let _exclusive = ADMISSION_GATE.write().await;
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.scratch")
        .join(format!("g4-unsupported-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    for (metric, precision) in [("euclidean_squared", "f16"), ("cosine_distance", "f32")] {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let config = EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            decoded_cache_bytes: 32 << 20,
            ..Default::default()
        };
        let engine = Engine::with_config(store.clone(), config.clone());
        let control = Engine::with_config(store, config);
        let vectors = (0..200)
            .map(|i| vec![1., i as f32 / 200., (i % 7) as f32 / 7., -0.5])
            .collect::<Vec<_>>();
        engine.write("hot", serde_json::from_value(json!({"distance_metric":metric,"schema":{"vector":{"type":format!("[4]{precision}"),"ann":true}},"upsert_rows":vectors.iter().enumerate().map(|(i,v)| json!({"id":i,"vector":v})).collect::<Vec<_>>()})).unwrap()).await.unwrap();
        engine.index_namespace("hot", true).await.unwrap();
        engine
            .run_recluster_job(
                "hot",
                &ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 2,
                    cluster_rows: 16,
                    max_slab_rows: 1000,
                    scratch: root.clone(),
                },
            )
            .await
            .unwrap();
        let (state, _) = engine
            .snapshot("hot", &bf_types::Consistency::Strong)
            .await
            .unwrap();
        let set = &state.ann_tiers["vector"][0];
        assert!(
            engine
                .construct_hot_graph("hot", "vector", set, &HotCancellation::default())
                .await
                .unwrap()
                .is_none()
        );
        for i in [0, 17, 113] {
            let request = json!({"rank_by":["vector","ANN",vectors[i]],"top_k":10,"include_attributes":false});
            same_rows(
                query(&engine, "hot", request.clone()).await,
                query(&control, "hot", request).await,
            );
        }
        assert_eq!(engine.0.hot_graph.answers.load(Ordering::Relaxed), 0);
        assert!(engine.shutdown(Duration::from_secs(5)).await);
        assert!(control.shutdown(Duration::from_secs(5)).await);
    }
}

#[test]
fn saving_is_a_fraction_of_the_current_ivf_cost_so_a_top_k_change_cannot_flip_it() {
    // Round G11: the graph stopped serving `top_k` 100 queries three minutes after a `top_k` 10
    // soak, because graph CPU timed on `top_k` 100 queries was compared with a `top_k` 10 IVF
    // baseline. A ratio measured on the same blocks as the IVF cost follows the baseline instead.
    let c = Calibration {
        quality: effort::Effort::new(),
        trial_seconds: 0.,
        trial_ratio: 0.,
        usable: false,
        graph_ratio: 0.2,
        build_seconds: 1.,
    };
    assert!(
        (c.saving(0.0017) - 0.00136).abs() < 1e-9,
        "top_k 10 IVF baseline"
    );
    assert!(
        (c.saving(0.0100) - 0.00800).abs() < 1e-9,
        "top_k 100 IVF baseline"
    );
    let slower = Calibration {
        graph_ratio: 1.3,
        ..c
    };
    assert_eq!(
        slower.saving(0.0017),
        0.,
        "a graph slower than IVF saves nothing"
    );
}

// Round G14: a selective filter is answered by scanning the ready graph's codes for the allowed
// rows, then the usual exact rerank, with the IVF engine (no graph) as the control.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selective_filters_scan_the_graph_and_match_the_ivf_control() {
    let _exclusive = ADMISSION_GATE.write().await;
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.scratch")
        .join(format!("g14-scan-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        cache: CacheConfig {
            memory_bytes: 32 << 20,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .unwrap();
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.,
        decoded_cache_bytes: 64 << 20,
        ..Default::default()
    };
    let e = Engine::with_config(store.clone(), config.clone());
    let control = Engine::with_config(store.clone(), config);
    let mut seed = 11u64;
    let mut vector = || {
        (0..16)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (seed >> 40) as f32 / 16777216. * 2. - 1.
            })
            .collect::<Vec<_>>()
    };
    let vectors = (0..2500).map(|_| vector()).collect::<Vec<_>>();
    let rows = vectors
        .iter()
        .enumerate()
        .map(|(id, v)| json!({"id":id,"vector":v,"bucket":id%100}))
        .collect::<Vec<_>>();
    e.write("hot",serde_json::from_value::<WriteRequest>(json!({"distance_metric":"cosine_distance","schema":{"vector":{"type":"[16]f16","ann":true}},"upsert_rows":rows})).unwrap()).await.unwrap();
    e.index_namespace("hot", true).await.unwrap();
    e.run_recluster_job(
        "hot",
        &ReclusterOptions {
            attribute: "vector".into(),
            cells: 2,
            cluster_rows: 32,
            max_slab_rows: 5000,
            scratch: root.clone(),
        },
    )
    .await
    .unwrap();
    let (state, _) = e
        .snapshot("hot", &bf_types::Consistency::Strong)
        .await
        .unwrap();
    let set = &state.ann_tiers["vector"][0];
    let key = Key::new("hot", &state, "vector", set);
    let held = Arc::new(admitted_entry(&e, set).await);
    // Ready, but not calibrated: a scan does not depend on the beam, so it needs neither.
    let mut heat = Heat::new(Instant::now());
    heat.ready = Some(held.clone());
    e.0.hot_graph.entries.lock().unwrap().insert(key, heat);
    let ask = |i: usize, filter: Value, k: usize| json!({"rank_by":["vector","ANN",vectors[i]],"top_k":k,"filters":filter,"include_attributes":["bucket"]});
    let answers = || e.0.hot_graph.answers.load(Ordering::Relaxed);
    // 1% of the rows (25), a single row, fewer rows than k, and no rows at all.
    for (filter, rows, k) in [
        (json!(["bucket", "Eq", 7]), 25, 10),
        (json!(["bucket", "Eq", 7]), 25, 100),
        (json!(["id", "Eq", 17]), 1, 10),
        (json!(["id", "Lt", 4]), 4, 10),
        (json!(["bucket", "Eq", 150]), 0, 10),
    ] {
        let before = answers();
        let got = query(&e, "hot", ask(7, filter.clone(), k)).await;
        same_rows(got.clone(), query(&control, "hot", ask(7, filter, k)).await);
        assert_eq!(got.as_array().unwrap().len(), rows.min(k));
        // A filter that matches nothing needs no graph answer; the others must have used it.
        assert_eq!(answers() > before, rows > 0, "rows={rows} k={k}");
    }
    // Above the crossover (1 row in 8), the IVF tier answers: 20% of the rows.
    let before = answers();
    let wide = json!(["bucket", "Lt", 20]);
    same_rows(
        query(&e, "hot", ask(7, wide.clone(), 10)).await,
        query(&control, "hot", ask(7, wide, 10)).await,
    );
    assert_eq!(answers(), before);
    // Rows rewritten, moved to another bucket, and deleted since the tier was built are shadowed:
    // the scan must not return their old versions.
    let moved = vector();
    e.write(
        "hot",
        serde_json::from_value::<WriteRequest>(json!({
            "upsert_rows":[
                {"id":7,"vector":moved,"bucket":7},
                {"id":107,"vector":vectors[7],"bucket":8},
                {"id":2600,"vector":vectors[7],"bucket":7}],
            "deletes":[207]
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    for bucket in [7, 8] {
        let filter = json!(["bucket", "Eq", bucket]);
        let before = answers();
        let got = query(&e, "hot", ask(7, filter.clone(), 10)).await;
        same_rows(
            got.clone(),
            query(&control, "hot", ask(7, filter, 10)).await,
        );
        let ids = got
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_u64().unwrap())
            .collect::<Vec<_>>();
        assert!(!ids.contains(&207));
        // 107 left bucket 7 and joined bucket 8.
        assert_eq!(ids.contains(&107), bucket == 8);
        assert!(ids.iter().filter(|id| **id == 7).count() <= 1);
        assert!(answers() > before, "shadowed rows still use the graph");
    }
    e.shutdown(Duration::from_secs(3)).await;
    control.shutdown(Duration::from_secs(3)).await;
}

#[test]
fn headroom_counts_anonymous_memory_but_not_reclaimable_page_cache() {
    let stat = "anon 1000\nfile 5000\ninactive_file 3000\nactive_file 1500\nshmem 10\n";
    // A cgroup of 10,000 bytes charged 6,500 (1,000 anonymous, 4,500 of file pages and 1,000
    // more): only the 4,500 reclaimable file bytes come back.
    assert_eq!(cgroup_headroom(10_000, 6_500, stat), 8_000);
    // A cgroup full of page cache is not full: refusing here would refuse every warm host's build.
    assert_eq!(cgroup_headroom(10_000, 10_000, stat), 4_500);
    // Without file pages in the stat, the whole charge counts.
    assert_eq!(cgroup_headroom(10_000, 6_500, "anon 6500\n"), 3_500);
    // A charge above the limit, or below its own page cache, never underflows.
    assert_eq!(cgroup_headroom(1_000, 9_000, "anon 9000\n"), 0);
    assert_eq!(cgroup_headroom(10_000, 100, stat), 10_000);
}
#[tokio::test]
async fn admission_refuses_when_memory_is_truly_short_and_overcommit_cancels() {
    // Other tests in this binary create engines (foreground claims) and build graphs
    // (reservations) concurrently, so this test asserts only what holds whatever they hold.
    let _exclusive = ADMISSION_GATE.write().await;
    // Memory that does not exist is never available; nothing is needed when nothing is asked.
    #[cfg(target_os = "linux")]
    assert!(!memory_available(u64::MAX));
    assert!(memory_available(0));
    assert_eq!(build_floor(8 << 30), 512 << 20);
    // The reservation gate refuses anything past its cap and admits what fits.
    assert!(Reservation::new(1, 0).is_none());
    let held = Reservation::new(1 << 20, u64::MAX).expect("an unbounded cap admits");
    drop(held);
    // A build is cancelled once builds plus foreground caches pass four fifths of the limit,
    // so a cache that grows during a build trips it and shrinking it again clears it.
    let limit = 10 << 30;
    let cap = limit / 5 * 4;
    assert!(!overcommitted_at(cap - (1 << 20), 0, limit));
    assert!(!overcommitted_at(cap - (1 << 20), 1 << 20, limit));
    assert!(overcommitted_at(cap - (1 << 20), (1 << 20) + 1, limit));
    assert!(overcommitted_at(u64::MAX, u64::MAX, limit));
}

/// Round G13: while unindexed writes shadow base rows, the graph still answers (up to `top_k`
/// 100), returns exactly the rows the IVF control returns, and never returns a shadowed row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadowed_rows_stay_traversable_but_are_never_returned() {
    let _exclusive = ADMISSION_GATE.write().await;
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.scratch")
        .join(format!("g13-shadow-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let config = StorageConfig {
        url: "memory://".into(),
        cache: CacheConfig {
            disk_dir: Some(root.join("cache")),
            memory_bytes: 32 << 20,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = Storage::open(&config).await.unwrap();
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.,
        decoded_cache_bytes: 64 << 20,
        ..Default::default()
    };
    let e = Engine::with_config(store.clone(), config.clone());
    let control = Engine::with_config(store.clone(), config);
    let mut seed = 11u64;
    let vectors = (0..2500)
        .map(|_| {
            (0..16)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    (seed >> 40) as f32 / 16777216. * 2. - 1.
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let rows = vectors
        .iter()
        .enumerate()
        .map(|(id, v)| json!({"id":id,"vector":v}))
        .collect::<Vec<_>>();
    e.write("hot",serde_json::from_value::<WriteRequest>(json!({"distance_metric":"cosine_distance","schema":{"vector":{"type":"[16]f16","ann":true}},"upsert_rows":rows})).unwrap()).await.unwrap();
    e.index_namespace("hot", true).await.unwrap();
    let opts = ReclusterOptions {
        attribute: "vector".into(),
        cells: 2,
        cluster_rows: 32,
        max_slab_rows: 5000,
        scratch: root.clone(),
    };
    e.run_recluster_job("hot", &opts).await.unwrap();
    let (state, _) = e
        .snapshot("hot", &bf_types::Consistency::Strong)
        .await
        .unwrap();
    let set = &state.ann_tiers["vector"][0];
    let key = Key::new("hot", &state, "vector", set);
    let held = Arc::new(admitted_entry(&e, set).await);
    let mut heat = Heat::new(Instant::now());
    heat.rate = 100.;
    heat.ivf_seconds = 1.;
    heat.ready = Some(held.clone());
    e.0.hot_graph.entries.lock().unwrap().insert(key, heat);
    {
        let mut calibration = held.calibration.lock().unwrap();
        calibration.quality = effort::Effort::new();
        calibration.build_seconds = 1.;
    }
    let key = Key::new("hot", &state, "vector", set);
    for _ in 0..128 {
        let scope = Scope::new(e.0.hot_graph.clone(), "hot").trial();
        *scope.used.lock().unwrap() = Some(vec![key.clone()]);
        held.calibration.lock().unwrap().trial_ratio = 0.1;
        scope.record_ivf_cpu(0.01);
        scope.feedback(1., 1.);
    }
    assert!(held.calibration.lock().unwrap().usable);
    // Overwrite (the same vector, so the row would still rank first), delete, and move a row
    // (an upsert that gives id 40 the vector of id 17 and so shadows its old position).
    e.write(
        "hot",
        serde_json::from_value::<WriteRequest>(json!({
        "deletes":[3,4,5],
        "upsert_rows":[
            {"id":17,"vector":vectors[900]},
            {"id":40,"vector":vectors[17]},
            {"id":41,"vector":vectors[17]},
        ]}))
        .unwrap(),
    )
    .await
    .unwrap();
    let hidden = [3u64, 4, 5, 17, 40, 41];
    for top_k in [10, 100] {
        for q in [17usize, 3, 4, 40, 900, 1200] {
            let body = json!({"rank_by":["vector","ANN",vectors[q]],"top_k":top_k});
            let before = e.0.hot_graph.answers.load(Ordering::Relaxed);
            let got = query(&e, "hot", body.clone()).await;
            assert!(
                e.0.hot_graph.answers.load(Ordering::Relaxed) > before,
                "the graph serves top_k {top_k} while writes are pending"
            );
            let want = query(&control, "hot", body).await;
            let ids = |rows: &Value| {
                rows.as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r["id"].as_u64().unwrap())
                    .collect::<Vec<_>>()
            };
            // Ids 40, 41, and 17 appear only through their new (tail) rows; 3, 4, and 5 never.
            let got_ids = ids(&got);
            assert_eq!(got_ids.len(), top_k);
            assert!(!got_ids.iter().any(|i| [3, 4, 5].contains(i)));
            let mut seen = got_ids.clone();
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen.len(), top_k, "no duplicate rows");
            if top_k == 10 {
                same_rows(got, want);
            } else {
                // The ID sets can differ at the very tail (graph recall); nearly all must match.
                let want_ids = ids(&want);
                let common = got_ids.iter().filter(|i| want_ids.contains(i)).count();
                assert!(common * 100 >= top_k * 95, "recall {common}/{top_k}");
            }
        }
    }
    let _ = hidden;
    // The moved row answers at its new position and the old row is gone.
    let rows = query(
        &e,
        "hot",
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":3}),
    )
    .await;
    let ids = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert!(ids.contains(&40) && ids.contains(&41) && !ids.contains(&17));
    e.delete_namespace("hot").await.unwrap();
    drop(held);
    assert!(e.shutdown(Duration::from_secs(5)).await);
    assert!(control.shutdown(Duration::from_secs(5)).await);
}

// Round M13: G13 and G14 together. A filtered query below the crossover scans the allowed rows'
// codes minus the rows the tail shadows (no `k + slack` over-fetch), and returns exactly what the
// IVF control returns: overwrites and deletes inside the allowed set, and upserts that move a row
// into or out of the filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtered_scan_hides_shadowed_rows_inside_the_allowed_set() {
    let _exclusive = ADMISSION_GATE.write().await;
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.scratch")
        .join(format!("m13-scan-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        cache: CacheConfig {
            memory_bytes: 32 << 20,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .unwrap();
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.,
        decoded_cache_bytes: 64 << 20,
        ..Default::default()
    };
    let e = Engine::with_config(store.clone(), config.clone());
    let control = Engine::with_config(store.clone(), config);
    let mut seed = 23u64;
    let vectors = (0..2500)
        .map(|_| {
            (0..16)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    (seed >> 40) as f32 / 16777216. * 2. - 1.
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let rows = vectors
        .iter()
        .enumerate()
        .map(|(id, v)| json!({"id":id,"vector":v,"bucket":id%100}))
        .collect::<Vec<_>>();
    e.write("hot",serde_json::from_value::<WriteRequest>(json!({"distance_metric":"cosine_distance","schema":{"vector":{"type":"[16]f16","ann":true}},"upsert_rows":rows})).unwrap()).await.unwrap();
    e.index_namespace("hot", true).await.unwrap();
    e.run_recluster_job(
        "hot",
        &ReclusterOptions {
            attribute: "vector".into(),
            cells: 2,
            cluster_rows: 32,
            max_slab_rows: 5000,
            scratch: root.clone(),
        },
    )
    .await
    .unwrap();
    let (state, _) = e
        .snapshot("hot", &bf_types::Consistency::Strong)
        .await
        .unwrap();
    let set = &state.ann_tiers["vector"][0];
    let key = Key::new("hot", &state, "vector", set);
    let held = Arc::new(admitted_entry(&e, set).await);
    let mut heat = Heat::new(Instant::now());
    heat.ready = Some(held.clone());
    e.0.hot_graph.entries.lock().unwrap().insert(key, heat);
    // Bucket 7 holds ids 7, 107, ..., 2407 (25 rows). Shadow inside it: overwrite 7 (same vector,
    // so it would still rank first), delete 107 through 407, move 507 out to bucket 8, and move
    // 608 into bucket 7. Id 2600 is new.
    e.write(
        "hot",
        serde_json::from_value::<WriteRequest>(json!({
            "deletes":[107, 207, 307, 407],
            "upsert_rows":[
                {"id":7,"vector":vectors[7],"bucket":7},
                {"id":507,"vector":vectors[7],"bucket":8},
                {"id":608,"vector":vectors[7],"bucket":7},
                {"id":2600,"vector":vectors[907],"bucket":7}]
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    let ask = |i: usize, filter: Value, k: usize| json!({"rank_by":["vector","ANN",vectors[i]],"top_k":k,"filters":filter,"include_attributes":["bucket"]});
    let answers = || e.0.hot_graph.answers.load(Ordering::Relaxed);
    for (bucket, wanted) in [(7u64, 25usize), (8, 25)] {
        for k in [10, 100] {
            for q in [7usize, 107, 907, 1200] {
                let filter = json!(["bucket", "Eq", bucket]);
                let before = answers();
                let got = query(&e, "hot", ask(q, filter.clone(), k)).await;
                same_rows(got.clone(), query(&control, "hot", ask(q, filter, k)).await);
                let ids = got
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r["id"].as_u64().unwrap())
                    .collect::<Vec<_>>();
                assert!(!ids.iter().any(|i| [107, 207, 307, 407].contains(i)));
                // 507 moved out of bucket 7 into 8; 608 moved into 7 (from bucket 8).
                assert!(!ids.contains(&507) || bucket == 8);
                assert!(!ids.contains(&608) || bucket == 7);
                let mut unique = ids.clone();
                unique.sort_unstable();
                unique.dedup();
                assert_eq!(unique.len(), ids.len(), "no duplicate rows");
                // Bucket 7: 25 - 4 deleted - 1 moved out + 1 moved in + 1 new = 22 live rows.
                if bucket == 7 {
                    assert_eq!(ids.len(), 22.min(k), "bucket {bucket} of {wanted}");
                }
                assert!(
                    answers() > before,
                    "bucket {bucket} k={k} stays on the graph"
                );
            }
        }
    }
    e.shutdown(Duration::from_secs(3)).await;
    control.shutdown(Duration::from_secs(3)).await;
}
