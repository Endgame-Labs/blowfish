//! Persistent HTTP/1.1 BM25 load driver with exhaustive ID and score-bit checks.
//!
//! Run against an already indexed, disposable server. Every worker uses one TCP
//! connection with TCP_NODELAY. Connections and request bodies are prepared before
//! timing; response parsing is timed, and exhaustive validation follows every reply.
//! The server must return Content-Length JSON responses. No retries hide failures.
//!
//! Usage: concurrent_bm25 CLIENTS WORKLOAD_JSON TRUTH_JSON [ADDRESS] [NAMESPACE]
//! Defaults: ADDRESS=127.0.0.1:18323, NAMESPACE=bm25. The workload and truth use
//! bench/scale/data/bm25-msmarco.json and exact_bm25's output formats, respectively.
use serde_json::Value;
use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
struct Client {
    stream: TcpStream,
    pending: Vec<u8>,
}
impl Client {
    fn new(address: &str) -> Self {
        let stream = TcpStream::connect(address).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(120)))
            .unwrap();
        stream
            .set_write_timeout(Some(std::time::Duration::from_secs(120)))
            .unwrap();
        Self {
            stream,
            pending: Vec::new(),
        }
    }
    fn one(&mut self, request: &[u8]) -> Value {
        self.stream.write_all(request).unwrap();
        let header = loop {
            if let Some(i) = self.pending.windows(4).position(|x| x == b"\r\n\r\n") {
                break i + 4;
            }
            let mut bytes = [0u8; 8192];
            let n = self.stream.read(&mut bytes).unwrap();
            assert!(n > 0);
            self.pending.extend_from_slice(&bytes[..n]);
        };
        let h = std::str::from_utf8(&self.pending[..header]).unwrap();
        assert!(h.starts_with("HTTP/1.1 200"), "{h}");
        let length: usize = h
            .lines()
            .find_map(|s| {
                s.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|s| s.trim().parse().unwrap())
            })
            .unwrap();
        while self.pending.len() < header + length {
            let mut bytes = [0u8; 8192];
            let n = self.stream.read(&mut bytes).unwrap();
            assert!(n > 0);
            self.pending.extend_from_slice(&bytes[..n]);
        }
        let response = serde_json::from_slice(&self.pending[header..header + length]).unwrap();
        self.pending.drain(..header + length);
        response
    }
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        (4..=6).contains(&args.len()),
        "usage: concurrent_bm25 CLIENTS WORKLOAD_JSON TRUTH_JSON [ADDRESS] [NAMESPACE]"
    );
    let clients: usize = args[1].parse().unwrap();
    assert!(clients > 0);
    let address = args.get(4).map_or("127.0.0.1:18323", String::as_str);
    let namespace = args.get(5).map_or("bm25", String::as_str);
    assert!(!namespace.contains(['\r', '\n', ' ']));
    let items: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[2]).unwrap()).unwrap();
    let truth: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[3]).unwrap()).unwrap();
    assert!(!items.is_empty(), "the workload is empty");
    assert_eq!(items.len(), truth.len(), "workload/truth length mismatch");
    for (item, expected) in items.iter().zip(&truth) {
        assert_eq!(item["query_id"], expected["query_id"]);
    }
    let requests: Vec<Vec<u8>> = items
        .iter()
        .map(|item| {
            let mut body = item["request"].clone();
            body["include_attributes"] = serde_json::json!(["id"]);
            let body = serde_json::to_vec(&body).unwrap();
            let header = format!(
                "POST /v2/namespaces/{namespace}/query HTTP/1.1\r\n\
             Host: {address}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n",
                body.len()
            );
            let mut request = header.into_bytes();
            request.extend(body);
            request
        })
        .collect();
    let next = AtomicUsize::new(0);
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut results = Vec::new();
    let connections: Vec<_> = (0..clients).map(|_| Client::new(address)).collect();
    let elapsed = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for mut cl in connections {
            let next = &next;
            let requests = &requests;
            let truth = &truth;
            let barrier = &barrier;
            workers.push(scope.spawn(move || {
                let mut results = Vec::new();
                barrier.wait();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= requests.len() {
                        break;
                    }
                    let start = Instant::now();
                    let response = cl.one(&requests[i]);
                    let ms = start.elapsed().as_secs_f64() * 1000.;
                    let rows = response["rows"].as_array().unwrap();
                    let expected = &truth[i];
                    assert_eq!(
                        rows.iter()
                            .map(|r| r["id"].as_u64().unwrap())
                            .collect::<Vec<_>>(),
                        expected["ids"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| v.as_u64().unwrap())
                            .collect::<Vec<_>>(),
                        "query {i}"
                    );
                    assert_eq!(
                        rows.iter()
                            .map(|r| (r["$dist"].as_f64().unwrap() as f32).to_bits())
                            .collect::<Vec<_>>(),
                        expected["scores"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| (v.as_f64().unwrap() as f32).to_bits())
                            .collect::<Vec<_>>(),
                        "query {i}"
                    );
                    results.push(ms);
                }
                results
            }));
        }
        let start = Instant::now();
        barrier.wait();
        for worker in workers {
            results.extend(worker.join().unwrap());
        }
        start.elapsed().as_secs_f64()
    });
    results.sort_by(f64::total_cmp);
    let output = serde_json::json!({
        "clients": clients,
        "seconds": elapsed,
        "qps": results.len() as f64 / elapsed,
        "p50_ms": results[results.len() / 2],
        "p90_ms": results[results.len() * 9 / 10],
        "p99_ms": results[results.len() * 99 / 100],
        "sum_request_seconds": results.iter().sum::<f64>() / 1000.0,
        "validated": results.len(),
    });
    println!("{output}");
}
