//! SIGTERM stops the server within seconds, even while a recluster build runs, and the bucket it
//! leaves starts cleanly: the build's record stays (GC reclaims its objects once it is stale),
//! queries answer at once, and the build starts again.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::json;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const DIMS: usize = 96;
const BATCH: usize = 20_000;
const BATCHES: usize = 6;

fn vector(id: usize) -> Vec<f32> {
    (0..DIMS)
        .map(|d| ((id * 7919 + d * 104_729) as f64).sin() as f32)
        .collect()
}

/// A namespace a server starts a base build on at once: 120,000 rows in six segments, written
/// below the bulk-load rate so no bulk-load guard holds the build back.
async fn seed(url: &str) {
    let store = Storage::open(&StorageConfig {
        url: url.to_string(),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            compaction_segments: 1_000,
            ..Default::default()
        },
    );
    for batch in 0..BATCHES {
        let rows: Vec<_> = (batch * BATCH..(batch + 1) * BATCH)
            .map(|n| json!({"id": n, "vector": vector(n)}))
            .collect();
        engine
            .write(
                "t",
                serde_json::from_value(
                    json!({"distance_metric": "euclidean_squared", "upsert_rows": rows}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("t", true).await.unwrap();
    }
    assert!(engine.shutdown(Duration::from_secs(5)).await);
}

/// A server on `url`, and its log lines.
fn serve(url: &str, port: u16) -> (Child, mpsc::Receiver<String>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_blowfish"))
        .args([
            "serve",
            "--storage-url",
            url,
            "--listen",
            &format!("127.0.0.1:{port}"),
        ])
        .env_remove("BLOWFISH_ADMIN_KEY")
        .env("RUST_LOG", "blowfish=info,bf_engine=info")
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (tx, rx) = mpsc::channel();
    for stream in [
        Box::new(child.stdout.take().unwrap()) as Box<dyn std::io::Read + Send>,
        Box::new(child.stderr.take().unwrap()),
    ] {
        let tx = tx.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stream).lines().map_while(Result::ok) {
                let _ = tx.send(line);
            }
        });
    }
    (child, rx)
}

/// Wait for a log line containing `text`, keeping every line seen in `log`.
fn wait_for(rx: &mpsc::Receiver<String>, log: &mut Vec<String>, text: &str, within: Duration) {
    let deadline = Instant::now() + within;
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match rx.recv_timeout(left) {
            Ok(line) => {
                let found = line.contains(text);
                log.push(line);
                if found {
                    return;
                }
            }
            Err(_) => break,
        }
    }
    panic!("no {text:?} within {within:?}:\n{}", log.join("\n"));
}

fn terminate(child: &mut Child) -> Duration {
    let sent = Instant::now();
    // SAFETY: signals only the child this test started.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "{status:?}");
            return sent.elapsed();
        }
        if sent.elapsed() > Duration::from_secs(30) {
            let _ = child.kill();
            panic!("still running 30 s after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_during_a_recluster_build_exits_within_seconds_and_restarts_cleanly() {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("shutdown-{}", uuid::Uuid::new_v4().simple()));
    let url = format!("file://{}", dir.display());
    let t0 = Instant::now();
    seed(&url).await;
    eprintln!("seeded in {:?}", t0.elapsed());

    let (mut server, rx) = serve(&url, free_port());
    let mut log = Vec::new();
    // Rows are in their coarse cells: the build is training and assigning leaves now, its
    // longest step on the CPU (minutes on a big namespace), which a runtime waits for on exit.
    wait_for(&rx, &mut log, " cells (largest ", Duration::from_secs(180));
    std::thread::sleep(Duration::from_millis(500));
    let took = terminate(&mut server);
    eprintln!("first exit after SIGTERM: {took:?}");
    log.extend(rx.try_iter());
    assert!(
        !log.iter().any(|l| l.contains("tier installed")),
        "the build finished before the signal, so this proved nothing:\n{}",
        log.join("\n")
    );
    // Within the 5 s grace (systemd allows 90 s). Before, the exit waited for the CPU step to
    // end: 8 s here in a debug build, minutes on a big namespace.
    assert!(
        took < Duration::from_secs(5),
        "exit took {took:?}:\n{}",
        log.join("\n")
    );
    assert!(
        log.iter()
            .any(|l| l.contains("background_jobs_stopped=true"))
    );
    let records = std::fs::read_dir(dir.join("ns/t/indexer"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().starts_with("build-"))
        .count();
    assert!(records > 0, "the interrupted build's record stays");

    // A clean restart: it answers at once, and builds again.
    let port = free_port();
    let (mut server, rx) = serve(&url, port);
    let mut log = Vec::new();
    wait_for(&rx, &mut log, "blowfish is ready", Duration::from_secs(60));
    let response: serde_json::Value = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/v2/namespaces/t/query"))
        .json(&json!({"rank_by": ["vector", "ANN", vector(4321)], "top_k": 1}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["rows"][0]["id"], json!(4321), "{response}");
    wait_for(
        &rx,
        &mut log,
        "recluster: building",
        Duration::from_secs(120),
    );
    let took = terminate(&mut server);
    eprintln!("second exit after SIGTERM: {took:?}");
    assert!(took < Duration::from_secs(5), "second exit took {took:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
