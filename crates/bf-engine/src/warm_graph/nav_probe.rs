//! Ignored, same-object navigation diagnosis on the target host; absent from production builds.
use super::*;
use bf_types::QueryRequest;
use std::{collections::BTreeMap, process::Command, sync::atomic::AtomicU32};

#[allow(dead_code)]
#[path = "../../../bf-vector/examples/g3/data.rs"]
mod data;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static QOS: AtomicU32 = AtomicU32::new(0);
static TOTAL: std::sync::LazyLock<Mutex<Total>> = std::sync::LazyLock::new(Default::default);

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(super) struct Counts {
    instructions: u64,
    cycles: u64,
    user_ticks: u64,
    kernel_ticks: u64,
}
unsafe extern "C" {
    fn thread_selfcounts(kind: u32, destination: *mut libc::c_void, size: usize) -> i32;
    fn qos_class_self() -> u32;
    #[link_name = "pthread_set_qos_class_self_np"]
    fn set_qos(class: u32, relative: i32) -> i32;
}
fn counts() -> [Counts; 2] {
    let mut out = [Counts::default(); 2];
    // SAFETY: Apple's THSC_TIME_CPI_PER_PERF_LEVEL ABI is four u64s per performance
    // level. This manual M4 diagnosis has two levels, verified before construction.
    assert_eq!(
        unsafe { thread_selfcounts(4, out.as_mut_ptr().cast(), std::mem::size_of_val(&out)) },
        0
    );
    out
}
pub(super) struct Start {
    values: [Counts; 2],
    cpu: f64,
    previous_qos: u32,
    qos: u32,
}
#[derive(Default)]
struct Total {
    queries: u64,
    evaluations: u64,
    digest: u64,
    instructions: [u64; 2],
    cycles: [u64; 2],
    cpu: f64,
    qos: BTreeMap<u32, u64>,
}
pub(super) fn begin() -> Option<Start> {
    if !ACTIVE.load(Ordering::Relaxed) {
        return None;
    }
    // SAFETY: both calls apply only to this current test thread.
    let previous_qos = unsafe { qos_class_self() };
    let forced = QOS.load(Ordering::Relaxed);
    if forced != 0 {
        assert_eq!(unsafe { set_qos(forced, 0) }, 0);
    }
    Some(Start {
        values: counts(),
        cpu: thread_cpu(),
        previous_qos,
        qos: unsafe { qos_class_self() },
    })
}
pub(super) fn end(start: Option<Start>, evaluations: usize, hits: &[u32]) {
    let Some(start) = start else {
        return;
    };
    let cpu = thread_cpu() - start.cpu;
    let after = counts();
    let mut total = TOTAL.lock().unwrap();
    total.queries += 1;
    total.evaluations += evaluations as u64;
    total.cpu += cpu;
    total.digest = total.digest.wrapping_add(hits.iter().fold(0u64, |h, r| {
        h.wrapping_mul(1099511628211).wrapping_add(u64::from(*r))
    }));
    for (i, value) in after.iter().enumerate() {
        total.instructions[i] += value.instructions - start.values[i].instructions;
        total.cycles[i] += value.cycles - start.values[i].cycles;
    }
    *total.qos.entry(start.qos).or_default() += 1;
    if QOS.load(Ordering::Relaxed) != 0 {
        // SAFETY: restore only the current test thread's prior scheduling class.
        assert_eq!(unsafe { set_qos(start.previous_qos.max(0x15), 0) }, 0);
    }
}
fn record(label: &str, started: Instant) -> serde_json::Value {
    ACTIVE.store(false, Ordering::Relaxed);
    let total = std::mem::take(&mut *TOTAL.lock().unwrap());
    let n = total.queries as f64;
    let value = serde_json::json!({"mode":label,"queries":total.queries,"seconds":started.elapsed().as_secs_f64(),"cpu_ms":total.cpu*1000./n,"evaluations":total.evaluations as f64/n,"instructions_by_level":total.instructions.map(|v|v as f64/n),"cycles_by_level":total.cycles.map(|v|v as f64/n),"qos":total.qos,"digest":total.digest});
    println!("NAV_DIAG {value}");
    value
}
struct Cleanup(std::path::PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Heater(std::process::Child);
impl Drop for Heater {
    fn drop(&mut self) {
        // SAFETY: this child created its own process group, containing only its uv launcher
        // and Python load generator. Kill the group so an interrupted test leaves no clients.
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGTERM);
        }
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual same-graph M4 diagnosis; needs G4_NAV_STORAGE, G4_NAV_DATA, and G4_NAV_ROOT"]
async fn same_graph_navigation_cpu() {
    use rayon::prelude::*;
    use std::os::unix::process::CommandExt;
    assert_eq!(
        Command::new("sysctl")
            .args(["-n", "hw.nperflevels"])
            .output()
            .unwrap()
            .stdout,
        b"2\n"
    );
    let _ = counts(); // Fail an unsupported counter interface before any expensive build.
    let root = std::path::PathBuf::from(std::env::var("G4_NAV_ROOT").unwrap())
        .join(format!("probe-{}", std::process::id()));
    assert!(root.components().any(|v| v.as_os_str() == ".scratch"));
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let input = std::path::PathBuf::from(std::env::var("G4_NAV_DATA").unwrap());
    let queries = data::Matrix::load(&input.join("queries.npy"), "<f4", 4, usize::MAX)
        .unwrap()
        .floats();
    let indices = (0..552)
        .map(|i| (i * 47 + 19) % queries.len())
        .collect::<Vec<_>>();
    let requests = indices.iter().map(|&i|serde_json::from_value::<QueryRequest>(serde_json::json!({"rank_by":["vector","ANN",queries[i]],"top_k":10,"include_attributes":false})).unwrap()).collect::<Vec<_>>();
    for (&i, request) in indices.iter().zip(&requests) {
        let Some(RankBy::Vector { vector, .. }) = &request.rank_by else {
            panic!()
        };
        assert!(
            vector
                .iter()
                .zip(&queries[i])
                .all(|(a, b)| a.to_bits() == b.to_bits())
        );
    }
    let mut config: bf_storage::StorageConfig =
        serde_json::from_slice(&std::fs::read(std::env::var("G4_NAV_STORAGE").unwrap()).unwrap())
            .unwrap();
    config.read_only = true;
    let engine = Engine::with_config(
        Storage::open(&config).await.unwrap(),
        crate::EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            ..crate::EngineConfig::from_env()
        },
    );
    for q in &requests {
        engine.query("nq", q.clone()).await.unwrap();
    }
    let (state, _) = engine
        .snapshot("nq", &bf_types::Consistency::Strong)
        .await
        .unwrap();
    let set = &state.ann_tiers["vector"][0];
    let count = usize::try_from(set.rows()).unwrap();
    let workers = engine.0.config.query_concurrency.max(1);
    let reserve = HotGraph::reservation(count, queries[0].len(), workers).unwrap() + (256 << 20);
    let reservation = Reservation::new(
        reserve,
        bf_alloc::memory_limit_bytes() * 4 / 5 - FOREGROUND.load(Ordering::Relaxed),
    )
    .unwrap();
    let start = Instant::now();
    let Source {
        builder,
        sources,
        mut locations,
    } = read_source(
        &engine,
        set,
        "vector",
        queries[0].len(),
        count,
        &HotCancellation::default(),
    )
    .await
    .unwrap();
    // This diagnosis serves no queries while constructing. Eight offline workers shorten
    // setup; production remains limited to its separate two-worker background pool.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap();
    let graph = pool.install(|| builder.finish(&|| false)).unwrap();
    locations.sort_unstable_by_key(|p| p.0);
    println!(
        "NAV_DIAG_BUILD seconds={} rows={}",
        start.elapsed().as_secs_f64(),
        graph.len()
    );
    let mut quality = effort::Effort::new();
    quality.trial = 90;
    let entry = Arc::new(Entry {
        graph,
        sources,
        locations,
        scratch: Mutex::new(Vec::new()),
        mask_live: Mutex::new(None),
        slots: tokio::sync::Semaphore::new(workers),
        calibration: Mutex::new(Calibration {
            quality,
            trial_seconds: 0.,
            usable: false,
            graph_seconds: 0.,
            build_seconds: 0.,
        }),
        _reservation: reservation,
        installed_at: Instant::now(),
        build_wall_seconds: 0.,
        measured_cpu_seconds: 0.,
        build_reservation_bytes: reserve,
        phase_cpu: Default::default(),
        searches: AtomicU64::new(0),
        pending_cost: Mutex::default(),
    });
    let key = Key::new("nq", &state, "vector", set);
    let mut heat = Heat::new(Instant::now());
    heat.rate = 800.;
    heat.ivf_seconds = 0.002;
    heat.ready = Some(entry.clone());
    engine.0.hot_graph.entries.lock().unwrap().insert(key, heat);
    let at = crate::lineage::place(engine.storage(), &state.segments, &state.lineage, set)
        .await
        .unwrap()
        .unwrap();
    let mask = crate::ann::tier_mask(engine.storage(), &state, set, &at)
        .await
        .unwrap();
    let native = |parallel: bool, label: &str| {
        ACTIVE.store(true, Ordering::Relaxed);
        let started = Instant::now();
        let run = |scratch: &mut HotScratch, index: usize| {
            let before = begin();
            let (hits, n) = entry
                .graph
                .candidates(&queries[indices[index]], 90, 30, mask.as_deref(), scratch)
                .unwrap();
            end(before, n, &hits);
        };
        if parallel {
            pool.install(|| {
                (0..8).into_par_iter().for_each(|worker| {
                    let mut scratch = entry.graph.scratch();
                    for _ in 0..4 {
                        for i in (worker..indices.len()).step_by(8) {
                            run(&mut scratch, i);
                        }
                    }
                })
            });
        } else {
            let mut scratch = entry.graph.scratch();
            for _ in 0..4 {
                for i in 0..indices.len() {
                    run(&mut scratch, i);
                }
            }
        }
        record(label, started)
    };
    let mut results = vec![native(false, "native_one"), native(true, "native_eight")];
    // A recall comparison streams the entire corpus between sparse graph trials. Evict
    // CPU caches explicitly to separate that observer effect from serving-path overhead.
    let eviction = vec![37u8; 128 << 20];
    let evict = || {
        let sum: u64 = eviction.iter().step_by(64).map(|v| u64::from(*v)).sum();
        std::hint::black_box(sum);
    };
    ACTIVE.store(true, Ordering::Relaxed);
    let started = Instant::now();
    let mut scratch = entry.graph.scratch();
    for _ in 0..4 {
        for &i in &indices {
            evict();
            let before = begin();
            let (hits, evaluations) = entry
                .graph
                .candidates(&queries[i], 90, 30, mask.as_deref(), &mut scratch)
                .unwrap();
            end(before, evaluations, &hits);
        }
    }
    results.push(record("native_one_cold", started));
    async fn trials(
        engine: &Engine,
        state: &NamespaceState,
        requests: &[QueryRequest],
        label: &str,
        eviction: Option<&[u8]>,
    ) -> serde_json::Value {
        ACTIVE.store(true, Ordering::Relaxed);
        let started = Instant::now();
        for _ in 0..4 {
            for q in requests {
                if let Some(bytes) = eviction {
                    std::hint::black_box(
                        bytes.iter().step_by(64).map(|v| u64::from(*v)).sum::<u64>(),
                    );
                }
                let context = read_cache::ReadContext {
                    hot_graph: Some(Scope::new(engine.0.hot_graph.clone(), "nq").trial()),
                    ..read_cache::ReadContext::new(engine.0.read_cache.clone())
                };
                let _ = trial_plan(engine.storage(), state, q, &engine.0.config, context)
                    .await
                    .unwrap();
            }
        }
        record(label, started)
    }
    results.push(trials(&engine, &state, &requests, "trial_alone", None).await);
    results.push(trials(&engine, &state, &requests, "trial_cold", Some(&eviction)).await);
    let app = axum::Router::new()
        .route(
            "/v2/namespaces/nq/query",
            axum::routing::post(
                |axum::extract::State(e): axum::extract::State<Engine>,
                 axum::Json(q): axum::Json<QueryRequest>| async move {
                    axum::Json(e.query("nq", q).await.unwrap())
                },
            ),
        )
        .route(
            "/measure/nq/status",
            axum::routing::get(
                |axum::extract::State(e): axum::extract::State<Engine>| async move {
                    axum::Json(e.hot_graph_status("nq"))
                },
            ),
        )
        .with_state(engine.clone());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let log = std::fs::File::create(root.join("heater.log")).unwrap();
    let mut command = Command::new("uv");
    command
        .args([
            "run",
            "--python",
            "3.12",
            "--no-project",
            "--with",
            "numpy",
            "python",
            &std::env::var("G4_NAV_HEATER").unwrap(),
        ])
        .arg(&input)
        .arg(port.to_string())
        .args(["800", "180"])
        .arg(root.join("heat.jsonl"))
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .stdin(std::process::Stdio::null())
        .process_group(0);
    let heater = Heater(command.spawn().unwrap());
    tokio::time::sleep(Duration::from_secs(15)).await;
    results.push(native(false, "native_one_with_heat"));
    results.push(trials(&engine, &state, &requests, "trial_with_heat", None).await);
    QOS.store(0x19, Ordering::Relaxed);
    results.push(
        trials(
            &engine,
            &state,
            &requests,
            "trial_user_initiated_with_heat",
            None,
        )
        .await,
    );
    QOS.store(0, Ordering::Relaxed);
    drop(heater);
    server.abort();
    let _ = server.await;
    assert!(
        results.iter().all(|v| v["queries"] == results[0]["queries"]
            && v["digest"] == results[0]["digest"]
            && v["evaluations"] == results[0]["evaluations"]),
        "same graph and query vectors must navigate identically: {results:?}"
    );
    assert!(engine.shutdown(Duration::from_secs(10)).await);
}
