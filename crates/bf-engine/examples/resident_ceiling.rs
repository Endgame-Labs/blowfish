//! Non-shipped warm IVF ceiling harness. Startup uses Tokio only to read the manifest;
//! timed queries run synchronously on N OS threads, with resident arrays and per-thread scratch.
//! Usage: resident_ceiling STORE NAMESPACE QUERIES_F32 THREADS PROBES REPEATS [IDS_JSON]
//! QUERIES_F32 is a headerless little-endian f32 array of the directory's dimension.
use bf_storage::{Storage, StorageConfig};
use bf_vector::{Bytes, Directory, ResidentIndex, ResidentQueryScratch};
use bf_wal::Wal;
use std::{
    error::Error,
    fs::File,
    io::{Read, Seek, SeekFrom},
    ops::Range,
    path::Path,
    sync::{Arc, Barrier},
    time::Instant,
};

fn read(file: &mut File, range: Range<u64>) -> std::io::Result<Bytes> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    let header = if &magic == b"BFPUT001" { 24 } else { 0 };
    file.seek(SeekFrom::Start(range.start + header))?;
    let mut bytes = vec![0; (range.end - range.start) as usize];
    file.read_exact(&mut bytes)?;
    Ok(bytes.into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let all_started = Instant::now();
    let a: Vec<_> = std::env::args().collect();
    if a.len() < 7 {
        return Err(
            "expected STORE NAMESPACE QUERIES_F32 THREADS PROBES REPEATS [IDS_JSON]".into(),
        );
    }
    let root = Path::new(&a[1]).canonicalize()?;
    let threads: usize = a[4].parse()?;
    let probes: usize = a[5].parse()?;
    let repeats: usize = a[6].parse()?;
    if threads == 0 || repeats == 0 {
        return Err("threads and repeats must be positive".into());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let manifest = runtime.block_on(async {
        let store = Storage::open(&StorageConfig {
            url: format!("file://{}", root.display()),
            read_only: true,
            ..Default::default()
        })
        .await?;
        Ok::<_, Box<dyn Error>>(
            Wal::new(store, &a[2])?
                .latest_manifest()
                .await?
                .ok_or("missing manifest")?
                .1,
        )
    })?;
    drop(runtime);
    let tiers = manifest
        .ann_tiers
        .get("vector")
        .ok_or("missing vector tier")?;
    if tiers.len() != 1 {
        return Err("ceiling requires exactly one tier".into());
    }
    let tier = &tiers[0];
    let dir = Directory::decode(&read(
        &mut File::open(root.join(&tier.dir.key))?,
        0..tier.dir.metadata_len,
    )?)?;
    let mut indexes = Vec::<ResidentIndex>::new();
    for (i, segment) in dir.segments().iter().enumerate() {
        let mut fragment = File::open(root.join(&segment.fragment))?;
        let mut vectors = File::open(root.join(&segment.key))?;
        let mut partial = dir.partial_index(i, Bytes::new())?;
        let mut codes = vec![Bytes::new(); segment.clusters];
        let mut exact = vec![Bytes::new(); segment.clusters];
        for entry in dir.segment_entries(i) {
            partial.install(entry.cluster, &read(&mut fragment, entry.blob_range())?)?;
            codes[entry.cluster] = read(&mut fragment, entry.codes_range())?;
            exact[entry.cluster] = read(&mut vectors, entry.vectors_range())?;
        }
        indexes.push(partial.into_index().into_resident(codes, Some(exact))?);
    }
    let queries = std::fs::read(&a[3])?;
    if !queries.len().is_multiple_of(dir.dims() * 4) {
        return Err("partial query vector".into());
    }
    let queries: Vec<Vec<f32>> = queries
        .chunks_exact(dir.dims() * 4)
        .map(|q| {
            q.as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect()
        })
        .collect();
    if queries.is_empty() {
        return Err("no queries".into());
    }
    let resident_bytes: u64 = indexes.iter().map(ResidentIndex::resident_bytes).sum();
    let mut external_ids = vec![
        0u64;
        tier.covered
            .iter()
            .map(|c| c.base as usize + c.rows as usize)
            .max()
            .unwrap_or(0)
    ];
    for cover in &tier.covered {
        let segment = manifest
            .segments
            .iter()
            .find(|s| s.id == cover.segment)
            .ok_or("covered segment was compacted")?;
        let file = segment.files.get("ids").ok_or("missing ID index")?;
        let bytes = read(&mut File::open(root.join(&file.key))?, 0..file.size)?;
        // ID-value files start with their JSON value, before the column directory and footer.
        let value = serde_json::Deserializer::from_slice(&bytes)
            .into_iter::<serde_json::Value>()
            .next()
            .ok_or("empty ID index")??;
        let ids = value["ids"].as_array().ok_or("missing IDs")?;
        for (row, id) in ids.iter().enumerate() {
            external_ids[cover.base as usize + row] =
                id.as_u64().ok_or("harness requires uint IDs")?;
        }
    }
    let tier_ids = a.get(7);
    let barrier = Arc::new(Barrier::new(threads));
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for thread in 0..threads {
            let (dir, indexes, queries, barrier, external_ids) =
                (&dir, &indexes, &queries, barrier.clone(), &external_ids);
            handles.push(scope.spawn(move || {
                let mut scratch = ResidentQueryScratch::default();
                // Warm every query on every worker so all capacities and lazy tables are ready.
                for q in queries {
                    dir.search_resident(indexes, q, probes, 32, 10, &mut scratch)
                        .unwrap();
                }
                let mut ids = vec![Vec::with_capacity(10); queries.len()];
                let counter = Counter::open(1).expect("instructions counter unavailable");
                let cycles = Counter::open(0).expect("cycle counter unavailable");
                barrier.wait();
                let tick = Instant::now();
                counter.start();
                cycles.start();
                let mut count = 0;
                for _ in 0..repeats {
                    for i in (thread..queries.len()).step_by(threads) {
                        let hits = dir
                            .search_resident(indexes, &queries[i], probes, 32, 10, &mut scratch)
                            .unwrap();
                        std::hint::black_box(hits);
                        ids[i].clear();
                        ids[i].extend(hits.iter().map(|(row, _)| external_ids[*row as usize]));
                        count += 1;
                    }
                }
                let cycles = cycles.stop();
                let instructions = counter.stop();
                (
                    count,
                    instructions,
                    tick.elapsed().as_secs_f64(),
                    ids,
                    cycles,
                )
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    let count: usize = results.iter().map(|r| r.0).sum();
    let instructions: u64 = results.iter().map(|r| r.1).sum();
    let cycles: u64 = results.iter().map(|r| r.4).sum();
    let seconds = results.iter().map(|r| r.2).fold(0., f64::max);
    if let Some(path) = tier_ids {
        let mut ids = vec![Vec::new(); queries.len()];
        for result in &results {
            for (i, rows) in result.3.iter().enumerate() {
                if !rows.is_empty() {
                    ids[i] = rows.clone();
                }
            }
        }
        // Save external IDs for recall and full-result comparison.
        std::fs::write(
            path,
            serde_json::to_vec(&serde_json::json!({ "ids": ids }))?,
        )?;
    }
    println!(
        "{}",
        serde_json::json!({"queries":count, "threads":threads, "probes":probes, "seconds":seconds, "qps":count as f64/seconds, "instructions_per_query":instructions as f64/count as f64, "cycles_per_query":cycles as f64/count as f64, "resident_bytes":resident_bytes, "startup_and_warm_seconds":all_started.elapsed().as_secs_f64()-seconds, "peak_rss_bytes":rss("VmHWM:"), "rss_bytes":rss("VmRSS:")})
    );
    Ok(())
}

fn rss(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix(field)
                .and_then(|v| v.split_whitespace().next())
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        })
}

#[cfg(target_os = "linux")]
struct Counter(File);
#[cfg(target_os = "linux")]
impl Counter {
    fn open(event: u64) -> std::io::Result<Self> {
        use std::os::fd::FromRawFd;
        // perf_event_attr v0 (64 bytes): hardware event, user space only, disabled.
        let mut attr = [0u64; 8];
        attr[0] = 64u64 << 32;
        attr[1] = event;
        attr[5] = 1 | (1 << 5) | (1 << 6);
        // SAFETY: valid attr pointer, this thread, no CPU affinity, and no group.
        let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, attr.as_ptr(), 0, -1, -1, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: perf_event_open returned a new owned file descriptor.
        Ok(Self(unsafe { File::from_raw_fd(fd as i32) }))
    }
    fn start(&self) {
        use std::os::fd::AsRawFd;
        // SAFETY: these perf ioctls take no pointer argument.
        unsafe {
            libc::ioctl(self.0.as_raw_fd(), 0x2403, 0);
            libc::ioctl(self.0.as_raw_fd(), 0x2400, 0);
        }
    }
    fn stop(mut self) -> u64 {
        use std::os::fd::AsRawFd;
        // SAFETY: PERF_EVENT_IOC_DISABLE takes no pointer argument.
        unsafe {
            libc::ioctl(self.0.as_raw_fd(), 0x2401, 0);
        }
        let mut bytes = [0; 8];
        self.0.read_exact(&mut bytes).unwrap();
        u64::from_ne_bytes(bytes)
    }
}
#[cfg(not(target_os = "linux"))]
struct Counter;
#[cfg(not(target_os = "linux"))]
impl Counter {
    fn open(_event: u64) -> std::io::Result<Self> {
        Err(std::io::Error::other("hardware counts require Linux perf"))
    }
    fn start(&self) {}
    fn stop(self) -> u64 {
        0
    }
}
