//! Deterministic, offline PGO training through the production loopback HTTP server.
//!
//! Invoked by `scripts/pgo-build.sh`; no downloaded corpus or vendor service is used.
//! The trainer's own counters are discarded: only child server profiles are merged.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use serde_json::{Value, json};
use std::{error::Error, path::Path, process::Child, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(600))
        .build()?)
}
const DIMS: usize = 1024;
const ROWS: usize = 16_384;
const TERMS: [&str; 8] = [
    "history of the world",
    "united states president",
    "science and technology",
    "river city",
    "film music",
    "university school",
    "first world war",
    "national park",
];

fn random(seed: &mut u64) -> f32 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((*seed >> 32) as u32 as f64 / u32::MAX as f64 * 2.0 - 1.0) as f32
}
fn vector(id: usize) -> Vec<f32> {
    let mut center = (id % 256) as u64 + 123;
    let mut noise = id as u64 + 9_876_543;
    (0..DIMS)
        .map(|_| random(&mut center) + random(&mut noise) * 0.15)
        .collect()
}
fn row(id: usize) -> Value {
    json!({"id":id,"vector":vector(id),"category":id%8,
        "text":format!("{} {} passage {}", TERMS[id%8], TERMS[(id/8)%8], id)})
}
async fn fixture(root: &Path) -> Result<()> {
    let store = root.join("store");
    std::fs::create_dir(&store)?;
    let engine = Engine::with_config(
        Storage::open(&StorageConfig {
            url: url::Url::from_directory_path(&store)
                .map_err(|()| "invalid store path")?
                .into(),
            ..Default::default()
        })
        .await?,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            compaction_segments: 1024,
            ..Default::default()
        },
    );
    for (namespace, rows) in [("vectors", ROWS), ("small", 10_000)] {
        for start in (0..rows).step_by(2048) {
            engine.write(namespace, serde_json::from_value(json!({
                "distance_metric":"cosine_distance",
                "schema":{"vector":{"type":"[1024]f16"},
                    "category":{"type":"uint"},"text":{"type":"string","full_text_search":true}},
                "upsert_rows":(start..(start+2048).min(rows)).map(row).collect::<Vec<_>>()
            }))?).await?;
            engine.index_namespace(namespace, true).await?;
        }
    }
    let report = engine
        .recluster(
            "vectors",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 8,
                cluster_rows: 200,
                max_slab_rows: 8192,
                scratch: root.to_owned(),
            },
        )
        .await?;
    if report.rows != ROWS as u64 || report.clusters < 64 {
        return Err("training fixture did not build the expected IVF tier".into());
    }
    engine.shutdown(Duration::from_secs(5)).await;
    eprintln!(
        "PGO fixture: {} rows, {} slabs, {} leaves",
        report.rows, report.slabs, report.clusters
    );
    Ok(())
}

struct Server(Child);
impl Server {
    fn stop(&mut self) -> Result<()> {
        if self.0.try_wait()?.is_none() {
            // The child is owned by this process. SIGTERM flushes LLVM's counters.
            unsafe {
                libc::kill(self.0.id() as i32, libc::SIGTERM);
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.0.try_wait()? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("training server exited: {status}").into())
                };
            }
            if std::time::Instant::now() > deadline {
                self.0.kill()?;
                self.0.wait()?;
                return Err("training server failed to stop gracefully".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
async fn start(
    root: &Path,
    profile: &str,
    writable: bool,
    s3: Option<&str>,
) -> Result<(Server, String)> {
    // Reserve an available loopback port. Startup detects a competing binder as an error.
    let socket = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = socket.local_addr()?;
    let store = if writable {
        root.join("writes")
    } else {
        root.join("store")
    };
    std::fs::create_dir_all(&store)?;
    let log = std::fs::File::create(root.join(if writable {
        "write-server.log"
    } else {
        "server.log"
    }))?;
    let mut command = std::process::Command::new(std::env::var("BF_PGO_BINARY")?);
    command
        .env_clear()
        .args([
            "serve",
            "--listen",
            &address.to_string(),
            "--storage-url",
            &s3.map_or_else(
                || url::Url::from_directory_path(store).unwrap().to_string(),
                |_| "s3://local".into(),
            ),
            "--indexing",
            "false",
            "--recall-sample-rate",
            "0",
            "--ann-nprobe",
            "48",
            "--ann-rerank-factor",
            "32",
            "--cache-memory-bytes",
            "1073741824",
            "--cache-disk-bytes",
            "0",
        ])
        .env("LLVM_PROFILE_FILE", profile)
        .env("TOKIO_WORKER_THREADS", "8")
        .env("RAYON_NUM_THREADS", "8")
        .env("BF_DECODED_CACHE_BYTES", "1073741824")
        .env("TMPDIR", root)
        .stdout(log.try_clone()?)
        .stderr(log);
    if !writable {
        command.arg("--read-only");
    }
    if let Some(endpoint) = s3 {
        // Dummy credentials for the owned loopback facade; no vendor account is used.
        command
            .args([
                "--storage-endpoint",
                endpoint,
                "--storage-region",
                "us-east-1",
            ])
            .env("AWS_ACCESS_KEY_ID", "local")
            .env("AWS_SECRET_ACCESS_KEY", "local")
            .env("AWS_EC2_METADATA_DISABLED", "true");
    }
    drop(socket);
    let mut server = Server(command.spawn()?);
    let base = format!("http://{address}");
    let client = client()?;
    for _ in 0..600 {
        if server.0.try_wait()?.is_some() {
            return Err("training server failed; inspect server.log".into());
        }
        if let Ok(response) = client
            .get(&base)
            .timeout(Duration::from_secs(1))
            .send()
            .await
            && response.status().is_success()
        {
            return Ok((server, base));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("training server readiness timeout".into())
}
fn query(i: usize, workload: &str) -> Value {
    if workload == "bm25" {
        return json!({"rank_by":["text","BM25",TERMS[i%8]],"top_k":10,"include_attributes":["id"]});
    }
    let mut body = json!({"rank_by":["vector","ANN",vector(ROWS+i%512)],"top_k":10,"include_attributes":["id"]});
    if workload == "filtered" {
        body["filters"] = json!(["category", "Eq", i % 8]);
    }
    body
}
async fn queries(base: &str, workload: &'static str, count: usize) -> Result<()> {
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..16 {
        let base = base.to_owned();
        workers.spawn(async move {
            let client = client()?;
            let namespace = if workload == "vector" {
                "vectors"
            } else {
                "small"
            };
            let bodies: Vec<_> = (0..512).map(|i| query(i, workload)).collect();
            for i in (worker..count).step_by(16) {
                let result: Value = client
                    .post(format!("{base}/v2/namespaces/{namespace}/query"))
                    .json(&bodies[i % 512])
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if result["rows"].as_array().is_none_or(|r| r.len() != 10)
                    || result["performance"]["exhaustive_search_count"] != 0
                {
                    return Err("training query missed indexed rows".into());
                }
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        });
    }
    while let Some(result) = workers.join_next().await {
        result??;
    }
    Ok(())
}
// A read-only S3 facade keeps cold storage-client paths in the release profile.
// It serves only the generated fixture, on an ephemeral loopback port.
async fn facade(root: &Path) -> Result<(String, tokio::task::JoinHandle<()>)> {
    #[derive(Clone)]
    struct State {
        root: std::path::PathBuf,
        keys: Vec<(String, u64)>,
    }
    fn collect(root: &Path, directory: &Path, keys: &mut Vec<(String, u64)>) -> Result<()> {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                collect(root, &entry.path(), keys)?;
            } else {
                keys.push((
                    entry
                        .path()
                        .strip_prefix(root)?
                        .to_string_lossy()
                        .into_owned(),
                    entry.metadata()?.len(),
                ));
            }
        }
        Ok(())
    }
    async fn serve(
        axum::extract::State(state): axum::extract::State<std::sync::Arc<State>>,
        request: axum::extract::Request,
    ) -> axum::response::Response {
        use axum::{
            body::Body,
            http::{Response, StatusCode},
        };
        let empty = |status| {
            Response::builder()
                .status(status)
                .body(Body::empty())
                .unwrap()
        };
        if request.method() != "GET" && request.method() != "HEAD" {
            return empty(StatusCode::METHOD_NOT_ALLOWED);
        }
        let parameters: std::collections::BTreeMap<_, _> =
            url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
                .into_owned()
                .collect();
        let mut response = Response::builder()
            .header("etag", "\"training\"")
            .header("last-modified", "Sat, 01 Jan 2000 00:00:00 GMT");
        if parameters.contains_key("list-type") {
            let prefix = parameters.get("prefix").map_or("", String::as_str);
            let after = parameters
                .get("start-after")
                .or_else(|| parameters.get("continuation-token"))
                .map_or("", String::as_str);
            let limit = parameters
                .get("max-keys")
                .and_then(|n| n.parse().ok())
                .unwrap_or(1000);
            let keys: Vec<_> = state
                .keys
                .iter()
                .filter(|(key, _)| key.starts_with(prefix) && key.as_str() > after)
                .collect();
            let mut body = format!(
                "<ListBucketResult><Name>local</Name><IsTruncated>{}</IsTruncated>",
                keys.len() > limit
            );
            for (key, size) in keys.iter().take(limit) {
                // All keys come from generated namespace and object names, with no XML metacharacters.
                body.push_str(&format!("<Contents><Key>{key}</Key><LastModified>2000-01-01T00:00:00Z</LastModified><ETag>training</ETag><Size>{size}</Size></Contents>"));
            }
            if keys.len() > limit && limit > 0 {
                body.push_str(&format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    keys[limit - 1].0
                ));
            }
            body.push_str("</ListBucketResult>");
            return response
                .header("content-type", "application/xml")
                .body(Body::from(body))
                .unwrap();
        }
        let Ok(path) = percent_encoding::percent_decode_str(request.uri().path()).decode_utf8()
        else {
            return empty(StatusCode::BAD_REQUEST);
        };
        let Some(key) = path.strip_prefix("/local/") else {
            return empty(StatusCode::NOT_FOUND);
        };
        if !Path::new(key)
            .components()
            .all(|p| matches!(p, std::path::Component::Normal(_)))
        {
            return empty(StatusCode::BAD_REQUEST);
        }
        let Ok(bytes) = tokio::fs::read(state.root.join(key)).await else {
            return empty(StatusCode::NOT_FOUND);
        };
        let mut start = 0;
        let mut end = bytes.len();
        if let Some(range) = request.headers().get("range") {
            let Some((lo, hi)) = range
                .to_str()
                .ok()
                .and_then(|r| r.strip_prefix("bytes="))
                .and_then(|r| r.split_once('-'))
            else {
                return empty(StatusCode::RANGE_NOT_SATISFIABLE);
            };
            if lo.is_empty() {
                let Ok(suffix) = hi.parse::<usize>() else {
                    return empty(StatusCode::RANGE_NOT_SATISFIABLE);
                };
                start = end.saturating_sub(suffix);
            } else {
                let Ok(lo) = lo.parse::<usize>() else {
                    return empty(StatusCode::RANGE_NOT_SATISFIABLE);
                };
                start = lo;
                if !hi.is_empty() {
                    let Some(hi) = hi.parse::<usize>().ok().and_then(|n| n.checked_add(1)) else {
                        return empty(StatusCode::RANGE_NOT_SATISFIABLE);
                    };
                    end = end.min(hi);
                }
            }
            if start >= end {
                return empty(StatusCode::RANGE_NOT_SATISFIABLE);
            }
            response = response.status(StatusCode::PARTIAL_CONTENT).header(
                "content-range",
                format!("bytes {start}-{}/{len}", end - 1, len = bytes.len()),
            );
        }
        let body = if request.method() == "HEAD" {
            Body::empty()
        } else {
            Body::from(bytes[start..end].to_vec())
        };
        response
            .header("content-length", end - start)
            .header("accept-ranges", "bytes")
            .body(body)
            .unwrap()
    }
    let mut keys = Vec::new();
    collect(root, root, &mut keys)?;
    keys.sort();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let app = axum::Router::new()
        .fallback(serve)
        .with_state(std::sync::Arc::new(State {
            root: root.to_owned(),
            keys,
        }));
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("training S3 facade");
    });
    Ok((endpoint, task))
}

async fn train(root: &Path, profile: &str) -> Result<()> {
    fixture(root).await?;
    let (mut server, base) = start(root, profile, false, None).await?;
    queries(&base, "vector", 20_712).await?;
    let metrics = client()?
        .get(format!("{base}/metrics"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    if !metrics.lines().any(|line| {
        line.strip_prefix("blowfish_ann_compiled_queries_total ")
            .and_then(|n| n.parse::<u64>().ok())
            .is_some_and(|n| n > 0)
    }) {
        return Err("trainer did not exercise the compiled IVF scorer".into());
    }
    queries(&base, "filtered", 1024).await?;
    queries(&base, "bm25", 1024).await?;
    server.stop()?;
    let (mut server, base) = start(root, profile, true, None).await?;
    let client = client()?;
    for start in (0..4096).step_by(16) {
        let mut body = json!({"upsert_rows":(start..start+16).map(row).collect::<Vec<_>>()});
        if start == 0 {
            body["distance_metric"] = json!("cosine_distance");
            body["schema"] = json!({"vector":{"type":"[1024]f16"},"text":{"type":"string","full_text_search":true}});
        }
        let result: Value = client
            .post(format!("{base}/v2/namespaces/writes"))
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if result["rows_upserted"] != 16 {
            return Err("training write did not insert rows".into());
        }
    }
    server.stop()?;
    // Fresh processes exercise startup, disk decoding, and cache-cold query paths.
    let (endpoint, facade) = facade(&root.join("store")).await?;
    for i in 0..8 {
        let (mut server, base) = start(root, profile, false, Some(&endpoint)).await?;
        queries(&base, "vector", 16 + i).await?;
        server.stop()?;
    }
    facade.abort();
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("expected NEW_SCRATCH_DIRECTORY SERVER_PROFILE_PATTERN".into());
    }
    let root = Path::new(&args[1]);
    let parent = root
        .parent()
        .ok_or("missing scratch parent")?
        .canonicalize()?;
    if !parent.components().any(|c| c.as_os_str() == ".scratch") {
        return Err("trainer requires .scratch".into());
    }
    std::fs::create_dir(root)?;
    let root = root.canonicalize()?;
    let started = std::time::Instant::now();
    let result = train(&root, &args[2]).await;
    if result.is_err() {
        for name in ["server.log", "write-server.log"] {
            if let Ok(log) = std::fs::read_to_string(root.join(name)) {
                eprintln!("{name}:\n{log}");
            }
        }
    }
    std::fs::remove_dir_all(&root)?;
    result?;
    eprintln!(
        "PGO training completed in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixture_is_deterministic_and_queries_cover_the_wire_contract() {
        assert_eq!(vector(42), vector(42));
        assert_ne!(vector(42), vector(43));
        assert_eq!(vector(42).len(), DIMS);
        assert!(vector(42).iter().all(|v| v.is_finite()));
        assert_eq!(
            query(9, "filtered")["filters"],
            json!(["category", "Eq", 1])
        );
        assert_eq!(query(9, "bm25")["rank_by"][1], "BM25");
    }

    #[tokio::test]
    async fn cold_facade_serves_ranges_metadata_and_lists() -> Result<()> {
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.scratch")
            .join(format!("pgo-facade-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root)?;
        let cleanup = Cleanup(root);
        std::fs::write(cleanup.0.join("blob"), b"0123456789")?;
        let (base, task) = facade(&cleanup.0).await?;
        let client = client()?;
        let head = client.head(format!("{base}/local/blob")).send().await?;
        assert_eq!(head.status(), 200);
        assert_eq!(head.headers()["content-length"], "10");
        assert!(head.headers().contains_key("last-modified"));
        assert_eq!(head.headers()["etag"], "\"training\"");
        let range = client
            .get(format!("{base}/local/blob"))
            .header("range", "bytes=2-5")
            .send()
            .await?;
        assert_eq!(range.status(), 206);
        assert_eq!(range.headers()["content-range"], "bytes 2-5/10");
        assert_eq!(range.text().await?, "2345");
        let suffix = client
            .get(format!("{base}/local/blob"))
            .header("range", "bytes=-3")
            .send()
            .await?;
        assert_eq!(suffix.text().await?, "789");
        let list = client
            .get(format!("{base}/local?list-type=2&prefix=b"))
            .send()
            .await?
            .text()
            .await?;
        assert!(list.contains("<Key>blob</Key>"));
        assert_eq!(
            client
                .get(format!("{base}/local/missing"))
                .send()
                .await?
                .status(),
            404
        );
        assert_eq!(
            client
                .post(format!("{base}/local/blob"))
                .send()
                .await?
                .status(),
            405
        );
        task.abort();
        Ok(())
    }
}
