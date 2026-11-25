//! Explicit, ignored full-corpus experiments; ordinary tests never create a fixture.
use super::*;
use std::{io::Write, path::PathBuf};

pub(super) fn capture(
    present: &[u32],
    read: &(dyn Fn(u32, &mut Vec<u8>) -> Vec<f32> + Sync),
    coarse: &[Vec<f32>],
    cells: &[Vec<u32>],
    cluster_rows: usize,
    max: usize,
) {
    let Some(root) = std::env::var_os("RR4_CAPTURE") else {
        return;
    };
    let root = PathBuf::from(root);
    assert!(root.components().any(|p| p.as_os_str() == ".scratch"));
    std::fs::create_dir_all(&root).unwrap();
    let mut file =
        std::io::BufWriter::new(std::fs::File::create(root.join("vectors.f32")).unwrap());
    let mut buf = Vec::new();
    let mut offsets = vec![u32::MAX; present.iter().copied().max().unwrap() as usize + 1];
    for (i, g) in present.iter().enumerate() {
        offsets[*g as usize] = i as u32;
        for x in read(*g, &mut buf) {
            file.write_all(&x.to_le_bytes()).unwrap();
        }
    }
    file.flush().unwrap();
    std::fs::write(
        root.join("input.json"),
        serde_json::to_vec(&(present, coarse, cells, cluster_rows, max, offsets)).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
#[ignore = "requires an owned RR4_STORE and a new RR4_CAPTURE beneath .scratch"]
async fn freeze_engine_spill() {
    let root = PathBuf::from(std::env::var_os("RR4_STORE").expect("RR4_STORE"));
    let store = bf_storage::Storage::open(&bf_storage::StorageConfig {
        url: format!("file://{}", root.display()),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store,
        crate::EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            ..Default::default()
        },
    );
    let report = engine
        .recluster(
            "nq",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 32,
                cluster_rows: 0,
                max_slab_rows: 250_000,
                scratch: root.join(".scratch"),
            },
        )
        .await
        .unwrap();
    println!("freeze rows={} seconds={}", report.rows, report.seconds);
}

#[test]
#[ignore = "requires frozen RR4_INPUT beneath .scratch, and RR4_OUTPUT"]
fn replay_leaf_assignment() {
    let root = PathBuf::from(std::env::var_os("RR4_INPUT").expect("RR4_INPUT"));
    assert!(root.components().any(|p| p.as_os_str() == ".scratch"));
    type Input = (
        Vec<u32>,
        Vec<Vec<f32>>,
        Vec<Vec<u32>>,
        usize,
        usize,
        Vec<u32>,
    );
    let (present, coarse, cells, cluster_rows, max, offsets): Input =
        serde_json::from_slice(&std::fs::read(root.join("input.json")).unwrap()).unwrap();
    let half = root.join("vectors.f16").exists();
    let file =
        std::fs::File::open(root.join(if half { "vectors.f16" } else { "vectors.f32" })).unwrap();
    let dims = coarse[0].len();
    let read =
        |g: u32, buf: &mut Vec<u8>| read_flat_row(&file, offsets[g as usize], dims, half, buf);
    let threads = std::env::var("RR4_THREADS")
        .unwrap_or_else(|_| "4".into())
        .parse()
        .unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap();
    println!(
        "RR4 native_bf16_matrix={}",
        bf_vector::bf16_matrix_accelerated()
    );
    let start = std::time::Instant::now();
    let jobs = pool
        .install(|| {
            assign_leaves(&present, &read, &coarse, cells, cluster_rows, max, &|| {
                false
            })
        })
        .unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    let output = PathBuf::from(std::env::var_os("RR4_OUTPUT").expect("RR4_OUTPUT"));
    assert!(output.components().any(|p| p.as_os_str() == ".scratch"));
    let mut file = std::io::BufWriter::new(std::fs::File::create(output).unwrap());
    for job in jobs {
        for x in job.centers.iter().flatten() {
            file.write_all(&x.to_le_bytes()).unwrap();
        }
        for (row, leaf) in job.rows.iter().zip(job.assignment) {
            file.write_all(&row.to_le_bytes()).unwrap();
            file.write_all(&(leaf as u32).to_le_bytes()).unwrap();
        }
    }
    println!("RR4 leaf seconds={elapsed:.6} threads={threads}");
}

struct Bound {
    groups: std::sync::Arc<[usize]>,
    best: u32,
    upper: f64,
    lower: f64,
}

/// Quantify the safe drift skip before deciding whether to retain its per-row memory.
pub(super) struct Drift {
    bounds: std::sync::Mutex<Vec<Option<Bound>>>,
    centers: Vec<Vec<f32>>,
    drift: Vec<f64>,
    max_drift: f64,
    error_factor: f64,
    max_norm: f64,
    rows: std::sync::atomic::AtomicU64,
    subset: std::sync::atomic::AtomicU64,
    skipped: std::sync::atomic::AtomicU64,
}
impl Drift {
    pub fn new(rows: usize, centers: &[Vec<f32>]) -> Option<Self> {
        std::env::var_os("RR4_DRIFT")?;
        let ne = (centers[0].len() as f64 + 32.) * f64::from(f32::EPSILON);
        Some(Self {
            bounds: std::sync::Mutex::new((0..rows).map(|_| None).collect()),
            centers: centers.to_vec(),
            drift: Vec::new(),
            max_drift: 0.,
            error_factor: 4. * ne / (1. - ne),
            max_norm: centers
                .iter()
                .map(|c| f64::from(norm2(c)))
                .fold(0., f64::max),
            rows: 0.into(),
            subset: 0.into(),
            skipped: 0.into(),
        })
    }
    pub fn advance(&mut self, centers: &[Vec<f32>]) {
        self.drift = self
            .centers
            .iter()
            .zip(centers)
            .map(|(old, new)| {
                old.iter()
                    .zip(new)
                    .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
                    .sum::<f64>()
                    .sqrt()
                    * (1. + 4. * old.len() as f64 * f64::EPSILON)
            })
            .collect();
        self.max_drift = self.drift.iter().copied().fold(0., f64::max);
        self.max_norm = centers
            .iter()
            .map(|c| f64::from(norm2(c)))
            .fold(0., f64::max);
        eprintln!("RR4 maximum_center_drift={:.8}", self.max_drift);
    }
    pub fn observe(
        &self,
        positions: &[u32],
        wanted: &[usize],
        norms: &[f32],
        candidates: &[u32],
        distances: &[Vec<f32>],
    ) {
        let groups: std::sync::Arc<[usize]> = wanted.into();
        let mut bounds = self.bounds.lock().unwrap();
        for ((position, norm), ds) in positions.iter().zip(norms).zip(distances) {
            let mut sorted: Vec<_> = ds.iter().copied().zip(candidates.iter().copied()).collect();
            sorted.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let error = self.error_factor * (f64::from(*norm) + self.max_norm);
            if self.drift.is_empty() {
                bounds[*position as usize] = Some(Bound {
                    groups: groups.clone(),
                    best: sorted[0].1,
                    upper: (f64::from(sorted[0].0) + error).max(0.).sqrt(),
                    lower: sorted
                        .get(1)
                        .map_or(f64::INFINITY, |x| (f64::from(x.0) - error).max(0.).sqrt()),
                });
            } else {
                self.rows.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let old = bounds[*position as usize].as_ref().unwrap();
                if wanted.iter().all(|g| old.groups.binary_search(g).is_ok())
                    && candidates.contains(&old.best)
                {
                    self.subset
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let upper = old.upper + self.drift[old.best as usize];
                    let other_drift = candidates
                        .iter()
                        .filter(|l| **l != old.best)
                        .map(|l| self.drift[*l as usize])
                        .fold(0., f64::max);
                    let lower = (old.lower - other_drift).max(0.);
                    if upper * upper + error < lower * lower - error {
                        assert_eq!(old.best, sorted[0].1, "drift bound changed winner");
                        self.skipped
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }
    }
}
impl Drop for Drift {
    fn drop(&mut self) {
        eprintln!(
            "RR4 drift_rows={} candidate_subset_rows={} certified_skips={}",
            self.rows.load(std::sync::atomic::Ordering::Relaxed),
            self.subset.load(std::sync::atomic::Ordering::Relaxed),
            self.skipped.load(std::sync::atomic::Ordering::Relaxed)
        );
    }
}

pub(super) fn perf_control(enable: bool) {
    static PASS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let Some(path) = std::env::var_os("RR4_PERF_CONTROL") else {
        return;
    };
    if enable {
        PASS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let pass = std::env::var("RR4_PERF_PASS")
        .unwrap_or_else(|_| "0".into())
        .parse::<usize>()
        .unwrap();
    if pass == 0 || pass == PASS.load(std::sync::atomic::Ordering::Relaxed) {
        let mut fifo = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        fifo.write_all(if enable { b"enable\n" } else { b"disable\n" })
            .unwrap();
    }
}

pub(super) fn record_assignments(leaf_of: &[u32]) {
    static PASS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let Some(output) = std::env::var_os("RR4_OUTPUT") else {
        return;
    };
    let output = PathBuf::from(output);
    assert!(output.components().any(|c| c.as_os_str() == ".scratch"));
    let pass = PASS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let bytes: Vec<_> = leaf_of.iter().flat_map(|l| l.to_le_bytes()).collect();
    std::fs::write(output.with_extension(format!("pass{pass}.u32")), bytes).unwrap();
}

fn read_flat_row(
    file: &std::fs::File,
    row: u32,
    dims: usize,
    half: bool,
    buf: &mut Vec<u8>,
) -> Vec<f32> {
    let width = dims * if half { 2 } else { 4 };
    buf.resize(width, 0);
    file.read_exact_at(buf, u64::from(row) * width as u64)
        .unwrap();
    if half {
        let decoded: Vec<_> = buf
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| half::f16::from_bits(u16::from_le_bytes(*b)).to_f32())
            .collect();
        bf_vector::prepare_vector(&decoded, bf_vector::Metric::CosineDistance)
    } else {
        buf.as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect()
    }
}

#[test]
#[ignore = "requires an owned raw f16 corpus in RR4_INPUT/vectors.f16 beneath .scratch"]
fn prepare_flat_spill() {
    let root = PathBuf::from(std::env::var_os("RR4_INPUT").expect("RR4_INPUT"));
    assert!(root.components().any(|c| c.as_os_str() == ".scratch"));
    assert!(!root.join("input.json").exists());
    let file = std::fs::File::open(root.join("vectors.f16")).unwrap();
    let dims = 1024;
    let rows = file.metadata().unwrap().len() as usize / (dims * 2);
    assert_eq!(file.metadata().unwrap().len() as usize, rows * dims * 2);
    assert!(rows > 0 && rows < u32::MAX as usize);
    let present: Vec<u32> = (0..rows as u32).collect();
    let read = |g: u32, buf: &mut Vec<u8>| read_flat_row(&file, g, dims, true, buf);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let (coarse, cells) = pool.install(|| {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let picks: Vec<_> = (0..19_200.min(rows))
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                present[state as usize % present.len()]
            })
            .collect();
        let sample: Vec<_> = picks
            .par_iter()
            .map_init(Vec::new, |buf, g| read(*g, buf))
            .collect();
        let refs: Vec<_> = sample.iter().map(Vec::as_slice).collect();
        let coarse = math::kmeans(&refs, 32.min(rows), 8, 7);
        let packed = math::Centers::new(coarse.iter().map(Vec::as_slice));
        let assignment: Vec<usize> = present
            .par_chunks(128)
            .flat_map_iter(|ids| {
                let mut buf = Vec::new();
                let rows: Vec<_> = ids.iter().map(|g| read(*g, &mut buf)).collect();
                let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
                packed.nearest(&refs)
            })
            .collect();
        let mut cells = vec![Vec::new(); coarse.len()];
        for (g, c) in present.iter().zip(assignment) {
            cells[c].push(*g);
        }
        (coarse, cells)
    });
    let cluster_rows = target_leaf_rows(rows as u64);
    std::fs::write(
        root.join("input.json"),
        serde_json::to_vec(&(
            &present,
            coarse,
            cells,
            cluster_rows,
            250_000usize,
            &present,
        ))
        .unwrap(),
    )
    .unwrap();
    println!("RR4 flat fixture rows={rows} dims={dims} cluster_rows={cluster_rows}");
}

/// Full-tier replay over an owned, freshly prepared untiered namespace. Copy the same
/// checkpoint before each arm; the clock includes the engine's fenced installation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned RR7_CONFIG, RR7_SCRATCH, and RR7_OUTPUT beneath .scratch"]
async fn rr7_install() {
    use sha2::{Digest, Sha256};
    let config: bf_storage::StorageConfig =
        serde_json::from_slice(&std::fs::read(std::env::var_os("RR7_CONFIG").unwrap()).unwrap())
            .unwrap();
    assert!(config.url.contains("/tmp/rr7/"));
    let scratch = PathBuf::from(std::env::var_os("RR7_SCRATCH").unwrap());
    assert!(scratch.components().any(|p| p.as_os_str() == ".scratch"));
    let store = bf_storage::Storage::open(&config).await.unwrap();
    let engine = Engine::with_config(
        store.clone(),
        crate::EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            ..Default::default()
        },
    );
    let wal = Wal::new(store.clone(), "nq").unwrap();
    assert!(
        wal.latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1
            .ann_tiers
            .is_empty()
    );
    let start = std::time::Instant::now();
    let report = engine
        .recluster(
            "nq",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 32,
                cluster_rows: 0,
                max_slab_rows: 250_000,
                scratch,
            },
        )
        .await
        .unwrap();
    let seconds = start.elapsed().as_secs_f64();
    let manifest = wal.latest_manifest().await.unwrap().unwrap().1;
    let mut hashes = Vec::new();
    for slab in &manifest.ann_tiers["vector"][0].slabs {
        let bytes = store.get(&slab.files["annv2:vector"].key).await.unwrap();
        hashes.push(
            Sha256::digest(&bytes)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        );
    }
    let output = PathBuf::from(std::env::var_os("RR7_OUTPUT").unwrap());
    assert!(output.components().any(|p| p.as_os_str() == ".scratch"));
    std::fs::write(output, hashes.join("\n")).unwrap();
    println!(
        "RR7 installed seconds={seconds:.6} build={:.6} rows={} slabs={} leaves={}",
        report.seconds, report.rows, report.slabs, report.clusters
    );
}

#[allow(dead_code)]
#[path = "../../../bf-vector/examples/g3/data.rs"]
mod nq;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned RR7_CONFIG, RR7_DATA NumPy matrices, and RR7_OUTPUT beneath .scratch"]
async fn rr7_quality() {
    let _gate = crate::warm_graph::ADMISSION_GATE.write().await;
    let mut config: bf_storage::StorageConfig =
        serde_json::from_slice(&std::fs::read(std::env::var_os("RR7_CONFIG").unwrap()).unwrap())
            .unwrap();
    assert!(config.url.contains("/tmp/rr7/") || config.url.contains("/.scratch/"));
    config.read_only = true;
    let root = PathBuf::from(std::env::var_os("RR7_DATA").unwrap());
    let queries = nq::Matrix::load(&root.join("queries.npy"), "<f4", 4, usize::MAX)
        .unwrap()
        .floats();
    let truth = nq::Matrix::load(&root.join("truth.npy"), "<i4", 4, usize::MAX)
        .unwrap()
        .truth();
    assert_eq!(queries.len(), truth.len());
    let mut outputs = Vec::new();
    for probes in [48, 64] {
        let store = bf_storage::Storage::open(&config).await.unwrap();
        let engine = Engine::with_config(
            store,
            crate::EngineConfig {
                indexing: false,
                recall_sample_rate: 0.,
                ann_nprobe: probes,
                ann_nprobe_explicit: true,
                ann_rerank_factor: 32,
                ..Default::default()
            },
        );
        let results: Vec<_> = futures::stream::iter(queries.iter().map(|q| {
            let engine = &engine;
            async move {
                let request = serde_json::from_value(serde_json::json!({
                    "rank_by":["vector", "ANN", q], "top_k":10, "include_attributes":false
                }))
                .unwrap();
                engine
                    .query("nq", request)
                    .await
                    .unwrap()
                    .result
                    .rows
                    .unwrap()
            }
        }))
        .buffered(4)
        .collect()
        .await;
        let hits: usize = results
            .iter()
            .zip(&truth)
            .map(|(rows, truth)| {
                rows.iter()
                    .filter(|r| truth.contains(&(r["id"].as_u64().unwrap() as u32)))
                    .count()
            })
            .sum();
        println!(
            "RR7 quality probes={probes} hits={hits} total={} recall={:.8}",
            queries.len() * 10,
            hits as f64 / (queries.len() * 10) as f64
        );
        outputs.push(results);
    }
    let output = PathBuf::from(std::env::var_os("RR7_OUTPUT").unwrap());
    assert!(output.components().any(|p| p.as_os_str() == ".scratch"));
    std::fs::write(output, serde_json::to_vec(&outputs).unwrap()).unwrap();
}
