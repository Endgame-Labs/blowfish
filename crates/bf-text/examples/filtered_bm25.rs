//! Persistent HTTP/1.1 filtered BM25 load driver (Round BW13).
//!
//! Like `concurrent_bm25`, but every request carries a `filters` expression, and replies are
//! compared with a reference file instead of the unfiltered truth: `record` writes each query's
//! ordered IDs and f32 score bits, `check` requires an exact match, and `none` only times.
//!
//! A `null` FILTER_JSON sends no filter (the unfiltered control).
//!
//! Usage: filtered_bm25 CLIENTS WORKLOAD_JSON ADDRESS NAMESPACE FILTER_JSON MODE REFERENCE_FILE [REPEAT]
//! The workload is `bench/scale/data/bm25-msmarco.json`. `REPEAT` replays the workload that many times.
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
        (8..=9).contains(&args.len()),
        "usage: filtered_bm25 CLIENTS WORKLOAD_JSON ADDRESS NAMESPACE FILTER_JSON MODE REFERENCE_FILE [REPEAT]"
    );
    let clients: usize = args[1].parse().unwrap();
    assert!(clients > 0);
    let address = args[3].as_str();
    let namespace = args[4].as_str();
    assert!(!namespace.contains(['\r', '\n', ' ']));
    let filter: Value = serde_json::from_str(&args[5]).unwrap();
    let mode = args[6].as_str();
    assert!(["record", "check", "none"].contains(&mode));
    let repeat: usize = args.get(8).map_or(1, |r| r.parse().unwrap());
    let items: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[2]).unwrap()).unwrap();
    assert!(!items.is_empty(), "the workload is empty");
    let reference: Vec<Option<Vec<(u64, u32)>>> = if mode == "check" {
        serde_json::from_slice::<Vec<Vec<(u64, u32)>>>(&std::fs::read(&args[7]).unwrap())
            .unwrap()
            .into_iter()
            .map(Some)
            .collect()
    } else {
        vec![None; items.len()]
    };
    assert!(mode != "check" || reference.len() == items.len());
    let requests: Vec<Vec<u8>> = items
        .iter()
        .map(|item| {
            let mut body = item["request"].clone();
            body["include_attributes"] = serde_json::json!(["id"]);
            if !filter.is_null() {
                body["filters"] = filter.clone();
            }
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
    let total = requests.len() * repeat;
    let next = AtomicUsize::new(0);
    let barrier = Arc::new(Barrier::new(clients + 1));
    let connections: Vec<_> = (0..clients).map(|_| Client::new(address)).collect();
    let mut latencies = Vec::new();
    let mut recorded: Vec<Vec<(u64, u32)>> = vec![Vec::new(); items.len()];
    let elapsed = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for mut cl in connections {
            let (next, requests, barrier, reference) = (&next, &requests, &barrier, &reference);
            workers.push(scope.spawn(move || {
                let mut results = Vec::new();
                barrier.wait();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= total {
                        break;
                    }
                    let q = i % requests.len();
                    let start = Instant::now();
                    let response = cl.one(&requests[q]);
                    let ms = start.elapsed().as_secs_f64() * 1000.;
                    let rows = response["rows"].as_array().unwrap();
                    let got: Vec<(u64, u32)> = rows
                        .iter()
                        .map(|r| {
                            (
                                r["id"].as_u64().unwrap(),
                                (r["$dist"].as_f64().unwrap() as f32).to_bits(),
                            )
                        })
                        .collect();
                    if let Some(expected) = &reference[q] {
                        assert_eq!(&got, expected, "query {q}");
                    }
                    results.push((ms, q, got));
                }
                results
            }));
        }
        let start = Instant::now();
        barrier.wait();
        for worker in workers {
            for (ms, q, got) in worker.join().unwrap() {
                latencies.push(ms);
                if mode == "record" {
                    if recorded[q].is_empty() {
                        recorded[q] = got;
                    } else {
                        assert_eq!(recorded[q], got, "unstable query {q}");
                    }
                }
            }
        }
        start.elapsed().as_secs_f64()
    });
    if mode == "record" {
        std::fs::write(&args[7], serde_json::to_vec(&recorded).unwrap()).unwrap();
    }
    latencies.sort_by(f64::total_cmp);
    let hits: usize = if mode == "record" {
        recorded.iter().map(Vec::len).sum()
    } else {
        0
    };
    println!(
        "{}",
        serde_json::json!({
            "clients": clients,
            "seconds": elapsed,
            "qps": latencies.len() as f64 / elapsed,
            "p50_ms": latencies[latencies.len() / 2],
            "p90_ms": latencies[latencies.len() * 9 / 10],
            "replies": latencies.len(),
            "recorded_hits": hits,
        })
    );
}
