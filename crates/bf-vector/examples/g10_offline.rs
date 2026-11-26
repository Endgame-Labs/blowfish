//! Round G10 offline proof: does the effort controller, driven by the exact-distance top-k of the union
//! of the graph's and the IVF tier's answers (IVF at twice the serving probes), settle where the
//! exhaustive-driven controller does?
//!
//! Usage: g10_offline EXPORTED_IVF NQ_DATA IVF_DIR [build_workers=2] [shuffles=100]
//! IVF_DIR holds `ivf{64,128}_k{10,100}.npy`: int32 `[queries, k]` corpus IDs (-1 pads) that the
//! production server returned at those probes (see `g10_ivf_dump.py`). The controller under test
//! is the engine's own `effort.rs`, included by path, so it cannot drift from the production one.
#[allow(dead_code)]
#[path = "g3/data.rs"]
mod data;
#[allow(dead_code)]
#[path = "../../bf-engine/src/warm_graph/effort.rs"]
mod effort;
use bf_vector::{
    Bytes, Index, RangeRead,
    warm_graph::{HotGraph, HotGraphBuilder, HotScratch},
};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read},
    ops::Range,
    os::unix::fs::FileExt,
    path::Path,
};
struct Reader(File, u64);
impl RangeRead for Reader {
    fn len(&self) -> u64 {
        self.1
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let mut bytes = vec![0; (range.end - range.start) as usize];
        self.0.read_exact_at(&mut bytes, range.start)?;
        Ok(bytes.into())
    }
}
fn open(path: &Path) -> bf_vector::Result<Index<Reader>> {
    let f = File::open(path)?;
    let n = f.metadata()?.len();
    futures::executor::block_on(Index::open(Reader(f, n)))
}
fn ids_matrix(path: &Path) -> Result<Vec<Vec<u32>>, Box<dyn std::error::Error>> {
    let m = data::Matrix::load(path, "<i4", 4, usize::MAX)?;
    Ok(m.bytes
        .chunks_exact(m.cols * 4)
        .map(|r| {
            r.as_chunks::<4>()
                .0
                .iter()
                .map(|b| i32::from_le_bytes(*b))
                .filter(|v| *v >= 0)
                .map(|v| v as u32)
                .collect()
        })
        .collect())
}
struct World {
    graph: HotGraph,
    exact: data::Vectors,
    queries: Vec<Vec<f32>>,
    normalized: Vec<Vec<f32>>,
    truth: Vec<Vec<u32>>,
    /// `[probes index 0=64,1=128][k index 0=10,1=100][query]`
    ivf: [[Vec<Vec<u32>>; 2]; 2],
}
struct Local<'a> {
    world: &'a World,
    scratch: HotScratch,
    cache: HashMap<(usize, usize, usize), Vec<(u32, f32)>>,
}
impl Local<'_> {
    /// The graph's exact-reranked top `k` at the controller's beam, as serving shapes it: shortlist
    /// 30 up to k 30, and above that beam max(beam, 2.56 k) with shortlist 1.5 k.
    fn graph(&mut self, q: usize, beam: usize, k: usize) -> &Vec<(u32, f32)> {
        let w = self.world;
        let scratch = &mut self.scratch;
        self.cache.entry((q, beam, k)).or_insert_with(|| {
            let (ef, short) = if k <= 30 {
                (beam, 30)
            } else {
                (beam.max((k as f64 * 2.56) as usize), k * 3 / 2)
            };
            let (ids, _) = w
                .graph
                .candidates(&w.queries[q], ef, short, None, scratch)
                .unwrap();
            let mut hits = ids
                .into_iter()
                .map(|id| (id, w.exact.rerank(&w.normalized[q], id)))
                .collect::<Vec<_>>();
            hits.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            hits.truncate(k);
            hits
        })
    }
}
fn overlap(a: &[u32], b: &[u32]) -> usize {
    a.iter().filter(|x| b.contains(x)).count()
}
/// Exact-distance top `k` of the union of the answers.
fn union_top(w: &World, q: usize, lists: &[&[u32]], k: usize) -> Vec<u32> {
    let mut all = lists
        .iter()
        .flat_map(|l| l.iter().copied())
        .collect::<Vec<_>>();
    all.sort_unstable();
    all.dedup();
    let mut scored = all
        .into_iter()
        .map(|id| (id, w.exact.rerank(&w.normalized[q], id)))
        .collect::<Vec<_>>();
    scored.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    scored.into_iter().take(k).map(|h| h.0).collect()
}
fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}
struct Outcome {
    beam: usize,
    samples: usize,
    settled: bool,
}
fn drive(local: &mut Local, k: usize, union: bool, seed: u64, cap: usize) -> Outcome {
    let w = local.world;
    let ki = usize::from(k == 100);
    let mut e = effort::Effort::new();
    let mut rng = seed;
    let n = w.queries.len();
    for samples in 1..=cap {
        let q = (xorshift(&mut rng) % n as u64) as usize;
        let g: Vec<u32> = local.graph(q, e.trial, k).iter().map(|h| h.0).collect();
        let i = &w.ivf[0][ki][q];
        let (rg, ri);
        if union {
            let u = union_top(w, q, &[&g, i, &w.ivf[1][ki][q]], k);
            rg = overlap(&g, &u) as f64 / k as f64;
            ri = overlap(i, &u) as f64 / k as f64;
        } else {
            let t = &w.truth[q][..k];
            rg = overlap(&g, t) as f64 / k as f64;
            ri = overlap(i, t) as f64 / k as f64;
        }
        e.observe(rg, ri);
        if e.settled() {
            return Outcome {
                beam: e.trial,
                samples,
                settled: true,
            };
        }
    }
    Outcome {
        beam: e.serving.unwrap_or(e.trial),
        samples: cap,
        settled: false,
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 4 {
        return Err(
            "usage: g10_offline EXPORTED_IVF NQ_DATA IVF_DIR [build_workers=2] [shuffles=100]"
                .into(),
        );
    }
    let workers = args.get(4).map(|s| s.parse()).transpose()?.unwrap_or(2);
    let shuffles: u64 = args.get(5).map(|s| s.parse()).transpose()?.unwrap_or(100);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()?;
    let mut files = std::fs::read_dir(&args[1])?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?;
    files.retain(|p| p.extension().is_some_and(|s| s == "bfa"));
    files.sort();
    let map = std::fs::read(Path::new(&args[1]).join("rowmap.u32"))?;
    let map = map
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| u32::from_le_bytes(*b))
        .collect::<Vec<_>>();
    let count = files
        .iter()
        .map(|f| open(f).map(|i| i.len()))
        .collect::<bf_vector::Result<Vec<_>>>()?
        .into_iter()
        .sum();
    let dims = open(&files[0])?.dims();
    let mut builder = HotGraphBuilder::new(dims, count)?;
    for file in &files {
        let index = open(file)?;
        for c in 0..index.cluster_count() {
            let (ids, bytes) = futures::executor::block_on(index.read_cluster_rows(c))?;
            let ids = ids.iter().map(|r| map[*r as usize]).collect::<Vec<_>>();
            builder.append(&ids, &bytes, &|| false)?;
        }
    }
    let graph = pool.install(|| builder.finish(&|| false))?;
    eprintln!("graph built: {} rows", graph.len());
    let data = Path::new(&args[2]);
    let queries = data::Matrix::load(&data.join("queries.npy"), "<f4", 4, usize::MAX)?.floats();
    let truth = ids_matrix(&data.join("truth.npy"))?;
    let mut corpus = File::open(data.join("corpus.f16"))?;
    let mut prefix = [0; 12];
    corpus.read_exact(&mut prefix)?;
    let offset = match prefix[6] {
        1 => 10 + u16::from_le_bytes(prefix[8..10].try_into()?) as u64,
        2 | 3 => 12 + u32::from_le_bytes(prefix[8..12].try_into()?) as u64,
        _ => return Err("unsupported NPY".into()),
    };
    let mut values = vec![half::f16::ZERO; count * dims];
    // SAFETY: f16 admits every bit pattern and the byte length exactly spans the allocation.
    let bytes = unsafe {
        std::slice::from_raw_parts_mut(values.as_mut_ptr().cast::<u8>(), values.len() * 2)
    };
    corpus.read_exact_at(bytes, offset)?;
    let inv_norms = values
        .par_chunks_exact(dims)
        .map(|v| {
            // SAFETY: both transparent wrappers have identical IEEE binary16 layout and alignment.
            let v =
                unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<numkong::f16>(), v.len()) };
            1. / numkong::Dot::dot(v, v).unwrap().sqrt()
        })
        .collect();
    let exact = data::Vectors {
        dims,
        values,
        inv_norms,
    };
    let dir = Path::new(&args[3]);
    let load = |p: usize, k: usize| ids_matrix(&dir.join(format!("ivf{p}_k{k}.npy")));
    let ivf = [
        [load(64, 10)?, load(64, 100)?],
        [load(128, 10)?, load(128, 100)?],
    ];
    let normalized = queries.iter().map(|q| data::normalized(q)).collect();
    let world = World {
        graph,
        exact,
        queries,
        normalized,
        truth,
        ivf,
    };
    let nq = world.queries.len();
    // Sanity: the dump's IDs are corpus rows (IVF recall against the exact truth).
    for (pi, p) in [64, 128].into_iter().enumerate() {
        for (ki, k) in [10usize, 100].into_iter().enumerate() {
            let found: usize = (0..nq)
                .map(|q| overlap(&world.ivf[pi][ki][q], &world.truth[q][..k]))
                .sum();
            println!(
                "ivf probes={p} k={k} recall={:.4}",
                found as f64 / (nq * k) as f64
            );
        }
    }
    let true_recall = |beam: usize, k: usize| -> f64 {
        let found: usize = (0..nq)
            .into_par_iter()
            .map_init(
                || Local {
                    world: &world,
                    scratch: world.graph.scratch(),
                    cache: HashMap::new(),
                },
                |l, q| {
                    let g: Vec<u32> = l.graph(q, beam, k).iter().map(|h| h.0).collect();
                    l.cache.clear();
                    overlap(&g, &world.truth[q][..k])
                },
            )
            .sum();
        found as f64 / (nq * k) as f64
    };
    // Diagnostics: at fixed beams, the paired recall gap (graph minus IVF) under each truth.
    println!(
        "DIAG k beam graph_exh ivf_exh graph_union ivf_union gap_exh gap_union sd_of_per_query_gap_difference"
    );
    for k in [10usize, 100] {
        let ki = usize::from(k == 100);
        for beam in [64usize, 80, 100, 128, 160, 256] {
            let rows = (0..nq)
                .into_par_iter()
                .map_init(
                    || Local {
                        world: &world,
                        scratch: world.graph.scratch(),
                        cache: HashMap::new(),
                    },
                    |l, q| {
                        let g: Vec<u32> = l.graph(q, beam, k).iter().map(|h| h.0).collect();
                        l.cache.clear();
                        let i = &world.ivf[0][ki][q];
                        let t = &world.truth[q][..k];
                        let u = union_top(&world, q, &[&g, i, &world.ivf[1][ki][q]], k);
                        let f = |a: usize| a as f64 / k as f64;
                        (
                            f(overlap(&g, t)),
                            f(overlap(i, t)),
                            f(overlap(&g, &u)),
                            f(overlap(i, &u)),
                        )
                    },
                )
                .collect::<Vec<_>>();
            let m = |f: &dyn Fn(&(f64, f64, f64, f64)) -> f64| {
                rows.iter().map(f).sum::<f64>() / nq as f64
            };
            let (ge, ie, gu, iu) = (m(&|r| r.0), m(&|r| r.1), m(&|r| r.2), m(&|r| r.3));
            let sd = (rows
                .iter()
                .map(|r| ((r.2 - r.3) - (r.0 - r.1) - ((gu - iu) - (ge - ie))).powi(2))
                .sum::<f64>()
                / nq as f64)
                .sqrt();
            println!(
                "DIAG {k} {beam} {ge:.5} {ie:.5} {gu:.5} {iu:.5} {:.5} {:.5} {sd:.5}",
                ge - ie,
                gu - iu
            );
        }
    }
    println!(
        "k shuffle beam_exhaustive beam_union samples_exhaustive samples_union settled_e settled_u true_e true_u"
    );
    for k in [10usize, 100] {
        let rows = (0..shuffles)
            .into_par_iter()
            .map_init(
                || Local {
                    world: &world,
                    scratch: world.graph.scratch(),
                    cache: HashMap::new(),
                },
                |l, s| {
                    let seed =
                        0x9E37_79B9_7F4A_7C15u64 ^ (s + 1).wrapping_mul(0xD1B5_4A32_D192_ED03);
                    let e = drive(l, k, false, seed, 40_000);
                    let u = drive(l, k, true, seed, 40_000);
                    let c = drive(l, k, false, seed ^ 0xABCDEF, 40_000);
                    (s, e, u, c)
                },
            )
            .collect::<Vec<_>>();
        let mut beams = std::collections::BTreeSet::new();
        for (_, e, u, c) in &rows {
            beams.insert(c.beam);
            beams.insert(e.beam);
            beams.insert(u.beam);
        }
        let recalls = beams
            .iter()
            .map(|b| (*b, true_recall(*b, k)))
            .collect::<HashMap<_, _>>();
        let (mut within_recall, mut within_step, mut ctl_recall, mut ctl_step) = (0, 0, 0, 0);
        for (s, e, u, c) in &rows {
            ctl_recall += usize::from((recalls[&e.beam] - recalls[&c.beam]).abs() <= 0.001);
            ctl_step += usize::from(e.beam.max(c.beam) * 4 <= e.beam.min(c.beam) * 5 + 4);
            let (re, ru) = (recalls[&e.beam], recalls[&u.beam]);
            println!(
                "{k} {s} {} {} {} {} {} {} {re:.5} {ru:.5}",
                e.beam, u.beam, e.samples, u.samples, e.settled, u.settled
            );
            within_recall += usize::from((re - ru).abs() <= 0.001);
            let (hi, lo) = (e.beam.max(u.beam), e.beam.min(u.beam));
            within_step += usize::from(hi * 4 <= lo * 5 + 4);
        }
        println!(
            "SUMMARY k={k} shuffles={shuffles} within_one_step={within_step} true_recall_within_0.001={within_recall} CONTROL_exh_vs_other_seed within_one_step={ctl_step} true_recall_within_0.001={ctl_recall}"
        );
    }
    Ok(())
}
