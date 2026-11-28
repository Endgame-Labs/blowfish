//! Native HTTP load driver for the M4 vector benchmarks (Round G20). It replaces the Python drivers
//! (`g18_drive.py`) so the clients cost as little CPU as possible and the server gets the cores.
//!
//! Usage: `native_drive DATA PORT bf|qdrant K SECONDS [EF] [OUT_JSON] [SERVER_PID]`
//!
//! `NATIVE_DRIVE_FILTER` adds a JSON `filters` clause to every request and `NATIVE_DRIVE_ROWS` keeps the first N queries.
//!
//! `DATA` holds `queries.npy` (`<f4`) and `truth.npy` (`<i4`; `NATIVE_DRIVE_TRUTH` names another file, `NATIVE_DRIVE_OFFSET` pages). It opens 16 persistent keep-alive
//! connections (one thread each), encodes every request body before the clock starts (floats
//! written as the shortest `f64` text of the `f32` value, the same digits as Python's `tolist()`),
//! runs one serial quality pass over all queries on its own connection (recall and a SHA-256 of every
//! response's rows), then the timed pass of SECONDS. It prints one JSON line: q/s, p50 and p99 latency,
//! recall, the driver's own CPU seconds per second (`client_cpu_s`) and, when SERVER_PID is given,
//! the server's CPU seconds over the same timed window (read with `ps`), so CPU per query follows.
//! Requests are hand-written HTTP/1.1 over `TcpStream`; responses are read by `Content-Length`.
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const CLIENTS: usize = 16;

/// The corpus row of a UUID ID written by `bench/nq/load.py --uuid-ids`: its text is
/// `(row + 1) * M mod 2^128`, so the row is that times M's inverse, less one.
fn row_of_uuid(text: &str) -> Option<i64> {
    const M: u128 = 0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835;
    let value = u128::from_str_radix(&text.replace('-', ""), 16).ok()?;
    // Newton's iteration doubles the correct low bits of the inverse of an odd number.
    let mut inverse: u128 = 1;
    for _ in 0..7 {
        inverse = inverse.wrapping_mul(2u128.wrapping_sub(M.wrapping_mul(inverse)));
    }
    Some(value.wrapping_mul(inverse).wrapping_sub(1) as i64)
}

fn npy(path: &str, dtype: &str) -> (Vec<usize>, Vec<u8>) {
    let raw = std::fs::read(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    assert_eq!(&raw[..6], b"\x93NUMPY", "{path}: not npy");
    let (header_start, header_len) = if raw[6] == 1 {
        (10, u16::from_le_bytes([raw[8], raw[9]]) as usize)
    } else {
        (
            12,
            u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize,
        )
    };
    let header = std::str::from_utf8(&raw[header_start..header_start + header_len]).unwrap();
    assert!(
        header.contains(dtype),
        "{path}: expected {dtype} in {header}"
    );
    assert!(header.contains("False"), "{path}: fortran order");
    let shape = header
        .split("'shape': (")
        .nth(1)
        .unwrap()
        .split(')')
        .next()
        .unwrap();
    let shape = shape
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().parse().unwrap())
        .collect();
    (shape, raw[header_start + header_len..].to_vec())
}

struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
    head: Vec<u8>,
}

impl Connection {
    fn open(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(300)))
            .unwrap();
        Self {
            stream,
            buffer: Vec::with_capacity(1 << 16),
            head: Vec::new(),
        }
    }

    /// Sends one POST and reads the whole response; returns the body in `self.buffer`.
    fn post(&mut self, route: &str, body: &[u8]) -> &[u8] {
        self.head.clear();
        write!(
            self.head,
            "POST {route} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .unwrap();
        self.head.extend_from_slice(body);
        self.stream.write_all(&self.head).expect("write");
        self.buffer.clear();
        let mut chunk = [0u8; 1 << 16];
        let mut end = None;
        while end.is_none() {
            let n = self.stream.read(&mut chunk).expect("read");
            assert!(n > 0, "connection closed");
            self.buffer.extend_from_slice(&chunk[..n]);
            end = self.buffer.windows(4).position(|w| w == b"\r\n\r\n");
        }
        let end = end.unwrap() + 4;
        let head = std::str::from_utf8(&self.buffer[..end])
            .unwrap()
            .to_ascii_lowercase();
        assert!(
            head.starts_with("http/1.1 200"),
            "status: {}",
            head.lines().next().unwrap()
        );
        let length: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .expect("response needs content-length")
            .trim()
            .parse()
            .unwrap();
        while self.buffer.len() < end + length {
            let n = self.stream.read(&mut chunk).expect("read");
            assert!(n > 0, "connection closed");
            self.buffer.extend_from_slice(&chunk[..n]);
        }
        &self.buffer[end..end + length]
    }
}

fn own_cpu() -> f64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(usage.ru_utime) + t(usage.ru_stime)
}

/// CPU seconds of another process, from `ps` (cumulative `[dd-][hh:]mm:ss.ff`).
fn process_cpu(pid: u32) -> f64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    let text = String::from_utf8_lossy(&out.stdout);
    let text = text.trim();
    let (days, rest) = text
        .split_once('-')
        .map_or((0.0, text), |(d, r)| (d.parse().unwrap(), r));
    days * 86400.0
        + rest
            .split(':')
            .fold(0.0, |acc, p| acc * 60.0 + p.parse::<f64>().unwrap())
}

fn encode(engine: &str, k: usize, ef: u64, offset: usize, filter: &str, q: &[f32]) -> Vec<u8> {
    let vector = q
        .iter()
        .map(|x| serde_json::to_string(&f64::from(*x)).unwrap())
        .collect::<Vec<_>>()
        .join(",");
    if engine == "bf" {
        let filters = if filter.is_empty() {
            String::new()
        } else {
            format!(r#""filters":{filter},"#)
        };
        format!(r#"{{"rank_by":["vector","ANN",[{vector}]],{filters}"top_k":{k},"offset":{offset},"include_attributes":false}}"#)
    } else {
        format!(r#"{{"vector":[{vector}],"limit":{k},"with_payload":false,"with_vector":false,"params":{{"hnsw_ef":{ef}}}}}"#)
    }
    .into_bytes()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() >= 6,
        "usage: native_drive DATA PORT bf|qdrant K SECONDS [EF] [OUT_JSON] [SERVER_PID]"
    );
    let data = &args[1];
    let port: u16 = args[2].parse().unwrap();
    let engine = args[3].clone();
    let k: usize = args[4].parse().unwrap();
    let seconds: f64 = args[5].parse().unwrap();
    let ef: u64 = args.get(6).map_or(0, |s| s.parse().unwrap());
    let out_path = args.get(7).filter(|s| !s.is_empty());
    let server_pid: Option<u32> = args.get(8).map(|s| s.parse().unwrap());
    // NATIVE_DRIVE_OFFSET pages: the request skips this many rows, and recall is of ranks
    // OFFSET..OFFSET+K against the truth file's columns (which must be at least that wide).
    let offset: usize = std::env::var("NATIVE_DRIVE_OFFSET").map_or(0, |s| s.parse().unwrap());
    // NATIVE_DRIVE_FILTER is a JSON filter sent with every request (the truth file must be the
    // exact answer under it), and NATIVE_DRIVE_ROWS keeps only the first queries.
    let filter = std::env::var("NATIVE_DRIVE_FILTER").unwrap_or_default();
    let (shape, raw) = npy(&format!("{data}/queries.npy"), "<f4");
    let (mut rows, dims) = (shape[0], shape[1]);
    if let Ok(n) = std::env::var("NATIVE_DRIVE_ROWS") {
        rows = rows.min(n.parse().unwrap());
    }
    let queries: Vec<f32> = raw
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| f32::from_le_bytes(*b))
        .collect();
    let (tshape, traw) = npy(
        &format!(
            "{data}/{}",
            std::env::var("NATIVE_DRIVE_TRUTH").unwrap_or_else(|_| "truth.npy".into())
        ),
        "<i4",
    );
    let truth: Vec<i32> = traw
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| i32::from_le_bytes(*b))
        .collect();
    let width = tshape[1];
    let route: &'static str = if engine == "bf" {
        "/v2/namespaces/nq/query"
    } else {
        "/collections/nq/points/search"
    };
    let bodies: Arc<Vec<Vec<u8>>> = Arc::new(
        (0..rows)
            .map(|i| {
                encode(
                    &engine,
                    k,
                    ef,
                    offset,
                    &filter,
                    &queries[i * dims..(i + 1) * dims],
                )
            })
            .collect(),
    );

    // Serial quality pass.
    let mut connection = Connection::open(port);
    let (mut digest, mut overlap) = (Sha256::new(), 0usize);
    for (i, body) in bodies.iter().enumerate() {
        let answer: Value = serde_json::from_slice(connection.post(route, body)).unwrap();
        let found = if engine == "bf" {
            &answer["rows"]
        } else {
            &answer["result"]
        };
        digest.update(serde_json::to_vec(found).unwrap());
        let want = &truth[i * width + offset..i * width + offset + k];
        overlap += found
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| {
                let id = r["id"]
                    .as_i64()
                    .or_else(|| r["id"].as_str().and_then(|s| s.parse().ok()))
                    .or_else(|| r["id"].as_str().and_then(row_of_uuid))
                    // Round G28's string IDs end in the row number: `nq:doc:00000123`.
                    .or_else(|| r["id"].as_str()?.rsplit(':').next()?.parse().ok())
                    .unwrap();
                want.contains(&(id as i32))
            })
            .count();
    }
    drop(connection);

    // Timed pass: connect first, then start together.
    let barrier = Arc::new(Barrier::new(CLIENTS + 1));
    let handles: Vec<_> = (0..CLIENTS)
        .map(|index| {
            let (bodies, barrier) = (bodies.clone(), barrier.clone());
            std::thread::spawn(move || {
                let mut c = Connection::open(port);
                let mut latencies = Vec::with_capacity(1 << 16);
                barrier.wait();
                let end = Instant::now() + Duration::from_secs_f64(seconds);
                let mut i = index;
                while Instant::now() < end {
                    let t = Instant::now();
                    c.post(route, &bodies[i % bodies.len()]);
                    latencies.push(t.elapsed().as_secs_f32());
                    i += CLIENTS;
                }
                latencies
            })
        })
        .collect();
    barrier.wait();
    let (cpu0, server0, start) = (own_cpu(), server_pid.map(process_cpu), Instant::now());
    let mut latencies: Vec<f32> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let elapsed = start.elapsed().as_secs_f64();
    let (client_cpu, server_cpu) = (
        own_cpu() - cpu0,
        server_pid.map(process_cpu).zip(server0).map(|(a, b)| a - b),
    );
    latencies.sort_by(f32::total_cmp);
    let pct = |p: f64| f64::from(latencies[((latencies.len() - 1) as f64 * p) as usize]) * 1e3;
    let n = latencies.len() as f64;
    let result = serde_json::json!({
        "driver": "native", "engine": engine, "k": k, "ef": ef, "queries": latencies.len(), "seconds": elapsed,
        "qps": n / elapsed, "p50_ms": pct(0.5), "p99_ms": pct(0.99),
        "recall": overlap as f64 / (rows * k) as f64, "rows_sha256": digest.finalize().iter().map(|b| format!("{b:02x}")).collect::<String>(),
        "client_cpu_cores": client_cpu / elapsed, "client_cpu_us_per_query": client_cpu / n * 1e6,
        "server_cpu_cores": server_cpu.map(|c| c / elapsed), "server_cpu_us_per_query": server_cpu.map(|c| c / n * 1e6),
    });
    println!("{result}");
    if let Some(path) = out_path {
        std::fs::write(path, format!("{result}\n")).unwrap();
    }
}
