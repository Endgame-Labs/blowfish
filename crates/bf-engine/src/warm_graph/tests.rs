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
        scope.record_cpu(0.001);
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
    for body in [
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":10,"filters":["group","Eq",1]}),
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":100}),
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":1,"offset":30}),
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
            calibration.graph_seconds, 0.,
            "discarded plans cannot calibrate CPU savings"
        );
    }
    // Check the abandonment contract independently of optional global admission pressure.
    let before_samples = delta.calibration.lock().unwrap().quality.len();
    let scope = Scope::new(e.0.hot_graph.clone(), "hot").trial();
    *scope.used.lock().unwrap() = Some(vec![delta_key.clone()]);
    scope.abandon();
    scope.record_cpu(1.);
    scope.record_ivf_cpu(1.);
    scope.feedback(0., 1.);
    assert_eq!(
        delta.calibration.lock().unwrap().quality.len(),
        before_samples
    );
    assert_eq!(delta.calibration.lock().unwrap().graph_seconds, 0.);
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
    // Eight truth observations produce one cost block. Its eight graph plans run
    // together, while IVF cost plans and exact truth never select the graph.
    assert_eq!(entry.calibration.lock().unwrap().trial_seconds, 0.);
    for i in 1..8 {
        e.recall(
            "hot",
            json!({"num":1,"top_k":10,"rank_by":["vector","ANN",vectors[171+i]]}),
        )
        .await
        .unwrap();
    }
    assert_eq!(entry.searches.load(Ordering::Relaxed), 16);
    assert_eq!(e.0.hot_graph.answers.load(Ordering::Relaxed), answered + 16);
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

/// The synchronous resident path returns exactly the rows and distance bits of the ordinary
/// plan, declines every shape and state it cannot prove resident, and never answers stale data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resident_answers_match_the_ordinary_plan_and_decline_what_they_cannot_prove() {
    let _exclusive = ADMISSION_GATE.write().await;
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.scratch")
        .join(format!("g6-resident-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let config = StorageConfig {
        url: "memory://".into(),
        cache: CacheConfig {
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
    let held = Arc::new(admitted_entry(&e, set).await);
    let mut heat = Heat::new(Instant::now());
    heat.rate = 100.;
    heat.ivf_seconds = 1.;
    heat.ready = Some(held.clone());
    e.0.hot_graph
        .entries
        .lock()
        .unwrap()
        .insert(key.clone(), heat);
    let resident = |body: Value| {
        let e = e.clone();
        async move {
            e.query_resident("hot", &serde_json::from_value(body).unwrap())
                .await
                .unwrap()
                .map(|r| serde_json::to_value(r).unwrap()["rows"].clone())
        }
    };
    let request = |i: usize| json!({"rank_by":["vector","ANN",vectors[i]],"top_k":10,"include_attributes":false});
    // Uncalibrated graphs never answer.
    assert!(resident(request(17)).await.is_none());
    {
        let mut calibration = held.calibration.lock().unwrap();
        calibration.quality = effort::Effort::new();
        calibration.build_seconds = 1.;
    }
    for _ in 0..128 {
        let scope = Scope::new(e.0.hot_graph.clone(), "hot").trial();
        *scope.used.lock().unwrap() = Some(vec![key.clone()]);
        scope.record_cpu(0.001);
        scope.record_ivf_cpu(0.01);
        scope.feedback(1., 1.);
    }
    assert!(held.calibration.lock().unwrap().usable);
    // Cold ID blocks decline; the ordinary plan then warms them.
    assert!(resident(request(17)).await.is_none());
    let before = e.0.hot_graph.answers.load(Ordering::Relaxed);
    query(&e, "hot", request(17)).await;
    assert!(e.0.hot_graph.answers.load(Ordering::Relaxed) > before);
    let mut answered = 0;
    for i in (0..2500).step_by(97) {
        let expected = query(&e, "hot", request(i)).await;
        let before = e.0.hot_graph.answers.load(Ordering::Relaxed);
        match resident(request(i)).await {
            Some(rows) => {
                same_rows(rows, expected);
                assert_eq!(e.0.hot_graph.answers.load(Ordering::Relaxed), before + 1);
                answered += 1;
            }
            // A query whose rows sit in an unwarmed ID block declines; warm and retry.
            None => {
                query(&e, "hot", request(i)).await;
            }
        }
    }
    assert!(
        answered > 0,
        "warm ID blocks must let the resident path answer"
    );
    // Shapes outside the bare top-k ANN query decline.
    for body in [
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":10,"filters":["group","Eq",1]}),
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":11}),
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":1,"offset":3}),
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":10,"include_attributes":["group"]}),
        json!({"rank_by":["vector","ANN",vectors[17]],"top_k":10,"consistency":{"level":"eventual"}}),
        json!({"rank_by":["vector","ANN",vec![0.;16]],"top_k":10}),
        json!({"rank_by":["vector","kNN",vectors[17]],"top_k":10,"filters":["group","Eq",1]}),
        json!({"rank_by":["vector","ANN",vec![0.5;3]],"top_k":10}),
    ] {
        assert!(resident(body).await.is_none());
    }
    // A write after the snapshot makes the published state stale: the check declines it.
    e.write(
        "hot",
        serde_json::from_value::<WriteRequest>(
            json!({"deletes":[17],"upsert_rows":[{"id":9000,"vector":vectors[17],"group":1}]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert!(resident(request(17)).await.is_none());
    assert_eq!(query(&e, "hot", request(17)).await[0]["id"], 9000);
}
