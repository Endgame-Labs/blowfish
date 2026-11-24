//! Non-shipped control separating the compiled scorer from engine and HTTP scheduling.
//! Linux CPU accounting covers the warmed process, including cache housekeeping, and
//! reports user and system microseconds per query independently of PMU event counts.
use super::WarmTier;
use crate::{Engine, EngineConfig, read_cache};
use bf_storage::{CacheConfig, Storage, StorageConfig};
use bf_types::{QueryRequest, RankBy};
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc, time::Instant};
mod counter {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/support/counter.rs"
    ));
}
use counter::Counter;

/// Linux accounts CPU time separately from elapsed time and PMU user cycles.
fn process_cpu_seconds() -> Option<(f64, f64)> {
    #[cfg(target_os = "linux")]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the complete rusage on success; only this
        // process's accounting is requested, and the pointer is valid for that write.
        let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        assert_eq!(result, 0, "process CPU accounting unavailable");
        // SAFETY: the successful getrusage call initialized this value.
        let usage = unsafe { usage.assume_init() };
        let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1_000_000.;
        Some((seconds(usage.ru_utime), seconds(usage.ru_stime)))
    }
    #[cfg(not(target_os = "linux"))]
    None
}

#[derive(Deserialize)]
struct Profile {
    store: PathBuf,
    namespace: String,
    queries: PathBuf,
    dims: usize,
    probes: usize,
    threads: usize,
    repeats: usize,
}

/// The JSON control file, store, and headerless f32 queries must all be under `.scratch`.
/// Warm through ordinary engine queries, then time the identical compiled scorer on OS
/// threads, with no HTTP, snapshots, hydration, or CPU-worker handoffs in the timed loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a frozen tier, BF_BENCH_COMPILED JSON control, and Linux PMU access"]
async fn compiled_kernel_profile() -> Result<(), Box<dyn std::error::Error>> {
    let control = PathBuf::from(std::env::var_os("BF_BENCH_COMPILED").ok_or("missing control")?)
        .canonicalize()?;
    let mut profile: Profile = serde_json::from_slice(&std::fs::read(&control)?)?;
    profile.store = profile.store.canonicalize()?;
    profile.queries = profile.queries.canonicalize()?;
    for path in [&control, &profile.store, &profile.queries] {
        assert!(path.components().any(|part| part.as_os_str() == ".scratch"));
    }
    assert!(profile.dims > 0 && profile.threads > 0 && profile.repeats > 0);
    let bytes = std::fs::read(&profile.queries)?;
    assert!(bytes.len().is_multiple_of(profile.dims * 4));
    let queries: Vec<Vec<f32>> = bytes
        .chunks_exact(profile.dims * 4)
        .map(|q| {
            q.as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect()
        })
        .collect();
    assert!(!queries.is_empty());
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", profile.store.display()),
        read_only: true,
        cache: CacheConfig {
            memory_bytes: 4 << 30,
            disk_bytes: 0,
            ..Default::default()
        },
        ..Default::default()
    })
    .await?;
    let engine = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            decoded_cache_bytes: 4 << 30,
            ann_nprobe: profile.probes,
            ..Default::default()
        },
    );
    let requests: Vec<_> = queries
        .iter()
        .map(|vector| QueryRequest {
            rank_by: Some(RankBy::Vector {
                attribute: "vector".into(),
                exact: false,
                vector: vector.clone(),
                wire_vector: serde_json::Value::Null,
            }),
            top_k: Some(10),
            ..Default::default()
        })
        .collect();
    for _ in 0..3 {
        futures::stream::iter(requests.iter().cloned().map(|request| {
            let engine = &engine;
            let ns = &profile.namespace;
            async move { engine.query(ns, request).await.map(|_| ()) }
        }))
        .buffer_unordered(16)
        .try_collect::<Vec<_>>()
        .await?;
    }
    let (state, _) = engine
        .snapshot(&profile.namespace, &Default::default())
        .await?;
    let key = &state.ann_tiers["vector"][0].dir.key;
    let warm: Arc<WarmTier> = read_cache::scope(
        read_cache::ReadContext::new(engine.0.read_cache.clone()),
        async { read_cache::get(key, 0) },
    )
    .await
    .ok_or("compiled tier was not admitted")?;
    drop(engine);
    let barrier = std::sync::Barrier::new(profile.threads);
    let cpu_started = std::sync::OnceLock::new();
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..profile.threads)
            .map(|thread| {
                let (queries, warm, barrier, profile, cpu_started) =
                    (&queries, &warm, &barrier, &profile, &cpu_started);
                scope.spawn(move || {
                    for query in queries {
                        std::hint::black_box(warm.search(query, profile.probes, 32, 10).unwrap());
                    }
                    let extra: Vec<_> = std::env::var("BF_BENCH_EVENTS")
                        .unwrap_or_default()
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(|event| {
                            let (name, config) = event.split_once(':').expect("name:hex-config");
                            let config =
                                u64::from_str_radix(config.trim_start_matches("0x"), 16).unwrap();
                            (name.to_owned(), Counter::raw(config).unwrap())
                        })
                        .collect();
                    let cycles = Counter::open(0).unwrap();
                    let instructions = (extra.len() <= 1).then(|| Counter::open(1).unwrap());
                    if std::env::var_os("BF_BENCH_PROFILE_STOP").is_some()
                        && barrier.wait().is_leader()
                    {
                        eprintln!("NATIVE_PROFILE_READY {}", std::process::id());
                        #[cfg(unix)]
                        // SAFETY: stop only this diagnostic process after every worker has warmed.
                        unsafe {
                            libc::raise(libc::SIGSTOP);
                        }
                    }
                    if barrier.wait().is_leader() {
                        cpu_started.set(process_cpu_seconds()).unwrap();
                    }
                    barrier.wait();
                    let start = Instant::now();
                    cycles.start();
                    if let Some(counter) = &instructions {
                        counter.start();
                    }
                    for (_, counter) in &extra {
                        counter.start();
                    }
                    let (mut count, mut misses) = (0, 0);
                    for _ in 0..profile.repeats {
                        for q in (thread..queries.len()).step_by(profile.threads) {
                            let found = warm.search(&queries[q], profile.probes, 32, 10).unwrap();
                            misses += usize::from(found.is_none());
                            std::hint::black_box(found);
                            count += 1;
                        }
                    }
                    let mut events: Vec<_> = extra
                        .into_iter()
                        .map(|(name, counter)| (name, counter.stop()))
                        .collect();
                    events.push(("cycles".into(), cycles.stop()));
                    if let Some(counter) = instructions {
                        events.push(("instructions".into(), counter.stop()));
                    }
                    (count, misses, start.elapsed().as_secs_f64(), events)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    let count: usize = results.iter().map(|r| r.0).sum();
    let misses: usize = results.iter().map(|r| r.1).sum();
    let seconds = results.iter().map(|r| r.2).fold(0., f64::max);
    let cpu = cpu_started
        .into_inner()
        .flatten()
        .zip(process_cpu_seconds())
        .map(|(before, after)| (after.0 - before.0, after.1 - before.1));
    let mut events = std::collections::BTreeMap::<_, (u64, u64, u64)>::new();
    for (_, _, _, counters) in results {
        for (name, (value, enabled, running)) in counters {
            let e = events.entry(name).or_default();
            e.0 += value;
            e.1 += enabled;
            e.2 += running;
        }
    }
    let events: std::collections::BTreeMap<_, _> = events.into_iter().map(|(name, (value, enabled, running))| {
        (name, serde_json::json!({"per_query":value as f64/count as f64,"coverage":running as f64/enabled as f64}))
    }).collect();
    println!(
        "{}",
        serde_json::json!({"compiled_queries":count,"misses":misses,"seconds":seconds,"qps":count as f64/seconds,"cpu_user_us_per_query":cpu.map(|s|s.0*1e6/count as f64),"cpu_system_us_per_query":cpu.map(|s|s.1*1e6/count as f64),"events":events})
    );
    assert_eq!(
        misses, 0,
        "a partial compiled run is not a complete-query ceiling"
    );
    Ok(())
}
