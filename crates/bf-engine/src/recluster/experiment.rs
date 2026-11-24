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
    let file = std::fs::File::open(root.join("vectors.f32")).unwrap();
    let dims = coarse[0].len();
    let read = |g: u32, buf: &mut Vec<u8>| {
        buf.resize(dims * 4, 0);
        file.read_exact_at(buf, u64::from(offsets[g as usize]) * (dims * 4) as u64)
            .unwrap();
        buf.as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect()
    };
    let threads = std::env::var("RR4_THREADS")
        .unwrap_or_else(|_| "4".into())
        .parse()
        .unwrap();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap();
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
                    let lower = (old.lower - self.max_drift).max(0.);
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
