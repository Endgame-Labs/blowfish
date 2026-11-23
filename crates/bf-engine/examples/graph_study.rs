//! Offline G2 study. Usage: graph_study DATA_DIR RUN_DIR [query_count=3452] [sizes=200,100,64,32] [cells=32].
//! A cell count of eight runs only the frozen-center assignment comparison.
//! Inputs are the NumPy files produced by `bench/nq/prepare.py`. No server or bucket is used.
//! The run directory must be new; it is removed on success, error, or unwinding.
use bf_alloc as _;
use bf_vector::{
    Bytes, Directory, Fragment, Index, IndexBuilder, Metric, PartialIndex, VectorType,
};
use rayon::prelude::*;
use std::{
    error::Error as StdError,
    fs::File,
    io::{Read, Seek},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
#[path = "graph_study/baseline.rs"]
mod baseline;
#[path = "graph_study/graph.rs"]
mod graph;
// Reuse the production center-code transform and SIMD scorer without exposing a production API.
mod distance {
    pub fn validate(v: &[f32], dims: usize) -> bf_vector::Result<()> {
        if dims == 0 || v.len() != dims || v.iter().any(|x| !x.is_finite()) {
            return Err(bf_vector::Error::InvalidInput(
                "expected matching, nonempty, finite vectors".into(),
            ));
        }
        Ok(())
    }
}
#[allow(dead_code)]
#[path = "../../bf-vector/src/rabitq.rs"]
mod rabitq;
mod format {
    pub fn bad(s: &str) -> bf_vector::Error {
        bf_vector::Error::InvalidFormat(s.into())
    }
}
pub use bf_vector::{Error, Result};
use graph::{Graph, Hit, Search};
type AnyResult<T> = std::result::Result<T, Box<dyn StdError>>;
struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Npy {
    file: File,
    offset: u64,
    rows: usize,
    cols: usize,
    width: usize,
}
impl Npy {
    fn open(path: &Path, dtype: &str, width: usize) -> AnyResult<Self> {
        let mut file = File::open(path)?;
        let mut prefix = [0u8; 8];
        file.read_exact(&mut prefix)?;
        if &prefix[..6] != b"\x93NUMPY" {
            return Err("not a NumPy file".into());
        }
        let n = match prefix[6] {
            1 => {
                let mut b = [0; 2];
                file.read_exact(&mut b)?;
                u16::from_le_bytes(b) as usize
            }
            2 => {
                let mut b = [0; 4];
                file.read_exact(&mut b)?;
                u32::from_le_bytes(b) as usize
            }
            _ => return Err("unsupported NumPy version".into()),
        };
        let mut header = vec![0; n];
        file.read_exact(&mut header)?;
        let header = String::from_utf8(header)?;
        if !header.contains(dtype) || !header.contains("'fortran_order': False") {
            return Err("expected little-endian, C-order NumPy array".into());
        }
        let shape = header
            .split("'shape': (")
            .nth(1)
            .ok_or("missing shape")?
            .split(')')
            .next()
            .ok_or("missing shape end")?;
        let shape: Vec<usize> = shape
            .split(',')
            .filter(|x| !x.trim().is_empty())
            .map(|x| x.trim().parse())
            .collect::<std::result::Result<_, _>>()?;
        if shape.len() != 2 || shape.contains(&0) {
            return Err("expected nonempty matrix".into());
        }
        let offset = file.stream_position()?;
        if file.metadata()?.len() != offset + (shape[0] * shape[1] * width) as u64 {
            return Err("truncated NumPy matrix".into());
        }
        Ok(Self {
            file,
            offset,
            rows: shape[0],
            cols: shape[1],
            width,
        })
    }
    fn bytes(&self, row: usize, buf: &mut Vec<u8>) {
        assert!(row < self.rows);
        buf.resize(self.cols * self.width, 0);
        let offset = self.offset + (row * buf.len()) as u64;
        self.file.read_exact_at(buf, offset).expect("matrix row");
    }
    fn vector(&self, row: usize, buf: &mut Vec<u8>) -> Vec<f32> {
        self.bytes(row, buf);
        let v: Vec<f32> = if self.width == 2 {
            buf.as_chunks::<2>()
                .0
                .iter()
                .map(|b| half::f16::from_le_bytes([b[0], b[1]]).to_f32())
                .collect()
        } else {
            buf.as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect()
        };
        bf_vector::prepare_vector(&v, Metric::CosineDistance)
    }
}

struct Codes {
    rq: rabitq::RaBitQ,
    centroids: Vec<Vec<f32>>,
    cell_of: Vec<usize>,
    codes: Vec<rabitq::RaBitQCode>,
}
impl Codes {
    fn new(layout: &baseline::Layout) -> Self {
        let dims = layout.leaves[0].len();
        let rq = rabitq::RaBitQ::new(dims, 42).unwrap();
        let counts = counts(&layout.leaf_of, layout.leaves.len());
        let centroids: Vec<Vec<f32>> = layout
            .ranges
            .iter()
            .map(|r| {
                let mut c = vec![0f32; dims];
                let mut n = 0;
                for l in r.clone().filter(|&l| counts[l] > 0) {
                    n += 1;
                    for (a, b) in c.iter_mut().zip(&layout.leaves[l]) {
                        *a += b;
                    }
                }
                for a in &mut c {
                    *a /= n.max(1) as f32;
                }
                c
            })
            .collect();
        let cell_of: Vec<usize> = layout
            .ranges
            .iter()
            .enumerate()
            .flat_map(|(c, r)| std::iter::repeat_n(c, r.len()))
            .collect();
        let codes = layout
            .leaves
            .par_iter()
            .enumerate()
            .map(|(l, v)| rq.encode(v, &centroids[cell_of[l]]).unwrap())
            .collect();
        let centroids = centroids
            .iter()
            .map(|c| rq.rotate(c).iter().map(|x| *x as f32).collect())
            .collect();
        Self {
            rq,
            centroids,
            cell_of,
            codes,
        }
    }
    fn query(&self, q: &[f32]) -> Vec<rabitq::PlaneQuery> {
        let q = self.rq.rotate(q);
        self.centroids
            .iter()
            .map(|c| rabitq::PlaneQuery::new(&q, c))
            .collect()
    }
    fn score(&self, q: &[rabitq::PlaneQuery], l: usize) -> f32 {
        let c = &self.codes[l];
        let p = &q[self.cell_of[l]];
        let dot = p.dot(&c.bits, self.centroids[0].len());
        (p.norm2 + c.norm * c.norm - 2.0 * c.norm * dot / c.alpha) as f32
    }
}
fn counts(assignment: &[u32], n: usize) -> Vec<usize> {
    let mut counts = vec![0; n];
    for &l in assignment {
        counts[l as usize] += 1;
    }
    counts
}
fn coarse(corpus: &Npy, cells: usize) -> (Vec<Vec<f32>>, Vec<Vec<u32>>) {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let sample: Vec<_> = (0..(cells * 600).clamp(8_000, 50_000).min(corpus.rows))
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            corpus.vector(state as usize % corpus.rows, &mut Vec::new())
        })
        .collect();
    let refs: Vec<&[f32]> = sample.iter().map(Vec::as_slice).collect();
    let centers = bf_vector::kmeans(&refs, cells, 8, 7);
    let assignment: Vec<_> = (0..corpus.rows)
        .into_par_iter()
        .map_init(Vec::new, |buf, r| {
            bf_vector::nearest_center(&corpus.vector(r, buf), &centers)
        })
        .collect();
    let mut rows = vec![Vec::new(); cells];
    for (r, c) in assignment.into_iter().enumerate() {
        rows[c].push(r as u32);
    }
    (centers, rows)
}
struct Scanner {
    indexes: Vec<PartialIndex<Bytes>>,
    blocks: Vec<Vec<Bytes>>,
    directory: Directory,
    pages: bf_vector::PagedDirectory,
    page_bytes: Bytes,
    page_of: Vec<usize>,
    global: Vec<usize>,
    local: Vec<(usize, usize)>,
}
impl Scanner {
    fn build(corpus: &Npy, layout: &baseline::Layout, run: &Path) -> AnyResult<Self> {
        let mut by_cell = vec![Vec::new(); layout.ranges.len()];
        let cell_of: Vec<_> = layout
            .ranges
            .iter()
            .enumerate()
            .flat_map(|(c, r)| std::iter::repeat_n(c, r.len()))
            .collect();
        for (r, &l) in layout.leaf_of.iter().enumerate() {
            by_cell[cell_of[l as usize]].push(r as u32);
        }
        let mut sources = Vec::new();
        let mut files = Vec::new();
        let mut fragments = Vec::new();
        let mut all_blocks = Vec::new();
        for (c, rows) in by_cell.into_iter().enumerate() {
            let range = &layout.ranges[c];
            let mut data = Vec::with_capacity(rows.len() * corpus.cols * 2);
            let mut buf = Vec::new();
            for &r in &rows {
                corpus.bytes(r as usize, &mut buf);
                data.extend_from_slice(&buf);
            }
            let assigned = rows
                .iter()
                .map(|&r| layout.leaf_of[r as usize] as usize - range.start)
                .collect();
            let blob = IndexBuilder::new(corpus.cols, Metric::CosineDistance)?
                .vector_type(VectorType::F16)
                .build_packed_assigned(
                    rows,
                    data,
                    layout.leaves[range.clone()].to_vec(),
                    assigned,
                )?;
            let file_size = blob.len() as u64;
            let index = futures::executor::block_on(Index::open(Bytes::from(blob.clone())))?;
            let ids: Vec<_> = (0..index.cluster_count()).collect();
            let blocks = futures::executor::block_on(index.read_codes(&ids))?;
            // Copy only codes; otherwise Bytes slices keep the entire vector object alive.
            all_blocks.push(blocks.iter().map(|b| Bytes::copy_from_slice(b)).collect());
            drop(index);
            let (fragment, meta) = Fragment::build_blocks(&blob)?;
            let frag_len = fragment.len() as u64;
            sources.push((
                format!("cell{c}"),
                format!("index{c}"),
                format!("fragment{c}"),
                Fragment::decode(&fragment[..meta as usize])?,
            ));
            fragments.push(Bytes::from(fragment));
            files.push([
                (file_size, bf_vector::metadata_len(&blob)?),
                (frag_len, meta),
            ]);
            // A study checkpoint for diagnostics; removed with RUN_DIR.
            std::fs::write(run.join("last-cell"), c.to_string())?;
        }
        let directory =
            Directory::decode(&Directory::build(&sources)?.ok_or("incompatible fragments")?)?;
        let mut indexes: Vec<_> = (0..layout.ranges.len())
            .map(|c| directory.partial_index(c, Bytes::new()))
            .collect::<Result<_>>()?;
        for e in directory.entries() {
            indexes[e.segment].install(
                e.cluster,
                &fragments[e.segment]
                    .slice(e.blob_range().start as usize..e.blob_range().end as usize),
            )?;
        }
        let (page_bytes, header_len) = directory.build_paged(24, 7, &files)?;
        let pages = bf_vector::PagedDirectory::decode(&page_bytes[..header_len as usize])?;
        let page_bytes = Bytes::from(page_bytes);
        let mut page_of = vec![usize::MAX; layout.leaves.len()];
        let mut global = Vec::new();
        let mut local = vec![(0, 0); layout.leaves.len()];
        for e in directory.entries() {
            let g = layout.ranges[e.segment].start + e.cluster;
            global.push(g);
            local[g] = (e.segment, e.cluster);
        }
        for p in 0..pages.cell_count() {
            let range = pages.page_range(p);
            let page =
                Arc::new(pages.decode_page(&page_bytes[range.start as usize..range.end as usize])?);
            let dir = pages.assemble(&[page]);
            for e in dir.entries() {
                page_of[layout.ranges[e.segment].start + e.cluster] = p;
            }
        }
        Ok(Self {
            indexes,
            blocks: all_blocks,
            directory,
            pages,
            page_bytes,
            page_of,
            global,
            local,
        })
    }
    fn scan(&self, corpus: &Npy, q: &[f32], chosen: &[usize]) -> AnyResult<(Vec<u32>, u128, u128)> {
        let rotated = self.directory.rotate_query(q)?;
        let t = Instant::now();
        let mut by_cell = vec![Vec::new(); self.indexes.len()];
        for &l in chosen {
            let (c, i) = self.local[l];
            by_cell[c].push(i);
        }
        let mut candidates = Vec::new();
        for (c, ids) in by_cell
            .iter()
            .enumerate()
            .filter(|(_, ids)| !ids.is_empty())
        {
            let blocks = ids.iter().map(|&l| self.blocks[c][l].clone()).collect();
            candidates.extend(
                self.indexes[c]
                    .index()
                    .scan_blocks_bounded_rotated(&rotated, ids, blocks, None, 320)?,
            );
        }
        let scan = t.elapsed().as_nanos();
        let t = Instant::now();
        if candidates.len() > 320 {
            candidates.select_nth_unstable_by(319, |a, b| {
                a.distance.total_cmp(&b.distance).then(a.row.cmp(&b.row))
            });
            candidates.truncate(320);
        }
        let mut buf = Vec::new();
        let mut reranked: Vec<_> = candidates
            .iter()
            .map(|c| {
                let v = corpus.vector(c.row as usize, &mut buf);
                Hit(bf_vector::squared_distance(q, &v), c.row as usize)
            })
            .collect();
        reranked.sort_unstable();
        reranked.truncate(10);
        Ok((
            reranked.iter().map(|h| h.1 as u32).collect(),
            scan,
            t.elapsed().as_nanos(),
        ))
    }
}

const PROBES: &[usize] = &[
    64, 96, 128, 192, 256, 384, 524, 768, 1024, 1536, 2048, 3072, 4096,
];
#[derive(Default)]
struct Point {
    covered: usize,
    rows: usize,
    rank_ns: u128,
    visits: usize,
    edges: usize,
    lost: usize,
}
#[allow(clippy::too_many_arguments)] // Independent fixtures for an offline experiment.
fn evaluate(
    corpus: &Npy,
    queries: &[Vec<f32>],
    truth: &[Vec<u32>],
    layout: &baseline::Layout,
    scanner: &Scanner,
    graph: &Graph,
    codes: &Codes,
    size: usize,
    cells: usize,
) -> AnyResult<()> {
    let count = counts(&layout.leaf_of, layout.leaves.len());
    let live = |l: usize| count[l] > 0;
    let mut stats: Vec<Vec<Point>> = (0..4)
        .map(|_| (0..PROBES.len()).map(|_| Point::default()).collect())
        .collect();
    let mut orders: Vec<Vec<Vec<usize>>> = (0..4).map(|_| Vec::new()).collect();
    let mut search = Search::new(count.len());
    let max = *PROBES.last().unwrap();
    // Each fetched page has two resident entry seeds. No traversal causes an object read.
    let mut page_seeds = vec![Vec::new(); scanner.pages.cell_count()];
    for &l in &scanner.global {
        let seeds = &mut page_seeds[scanner.page_of[l]];
        if seeds.len() < 2 {
            seeds.push(l);
        }
    }
    for (qi, q) in queries.iter().enumerate() {
        let t = Instant::now();
        let plane = codes.query(q);
        let setup = t.elapsed().as_nanos();
        let t = Instant::now();
        let mut exact: Vec<Hit> = (0..count.len())
            .filter(|&l| live(l))
            .map(|l| Hit(bf_vector::squared_distance(q, &layout.leaves[l]), l))
            .collect();
        exact.sort_unstable();
        let exact_ns = t.elapsed().as_nanos();
        let t = Instant::now();
        let mut quantized: Vec<Hit> = scanner
            .global
            .iter()
            .map(|&l| Hit(codes.score(&plane, l), l))
            .collect();
        quantized.sort_unstable();
        let quant_ns = t.elapsed().as_nanos() + setup;
        let t = Instant::now();
        let (graph_hits, visited) = search.run(&graph.edges, &[graph.entry], max, live, |l| {
            codes.score(&plane, l)
        });
        let graph_ns = t.elapsed().as_nanos() + setup;
        orders[0].push(exact.into_iter().map(|h| h.1).take(max).collect());
        orders[1].push(quantized.into_iter().map(|h| h.1).take(max).collect());
        orders[2].push(graph_hits.into_iter().map(|h| h.1).collect());
        for mode in 0..3 {
            for (pi, &probes) in PROBES.iter().enumerate() {
                let picked = &orders[mode][qi][..probes.min(orders[mode][qi].len())];
                let st = &mut stats[mode][pi];
                st.rows += picked.iter().map(|&l| count[l]).sum::<usize>();
                st.covered += truth[qi]
                    .iter()
                    .take(10)
                    .filter(|&&r| picked.contains(&(layout.leaf_of[r as usize] as usize)))
                    .count();
                st.rank_ns += [exact_ns, quant_ns, graph_ns][mode];
                st.visits += if mode == 2 {
                    visited
                } else {
                    scanner.global.len()
                };
            }
        }
        // Cold/page-restricted routing uses the actual current 24-entry page construction,
        // and its margin of 16 entries fetched for every leaf probed.
        for (pi, &probes) in PROBES.iter().enumerate() {
            let t = Instant::now();
            let fetched = scanner.pages.select_cells(q, probes * 16, 0.5)?;
            let mut allowed_pages = vec![false; scanner.pages.cell_count()];
            let mut seeds = Vec::new();
            for p in fetched {
                allowed_pages[p] = true;
                seeds.extend_from_slice(&page_seeds[p]);
            }
            let allowed = |l: usize| live(l) && allowed_pages[scanner.page_of[l]];
            let (hits, visited) = search.run(&graph.edges, &seeds, probes * 2, allowed, |l| {
                codes.score(&plane, l)
            });
            let ns = t.elapsed().as_nanos() + setup;
            let picked: Vec<_> = hits.into_iter().take(probes).map(|h| h.1).collect();
            let st = &mut stats[3][pi];
            st.rows += picked.iter().map(|&l| count[l]).sum::<usize>();
            st.covered += truth[qi]
                .iter()
                .take(10)
                .filter(|&&r| picked.contains(&(layout.leaf_of[r as usize] as usize)))
                .count();
            st.visits += visited;
            st.rank_ns += ns;
            for &l in &picked {
                for &n in &graph.edges[l] {
                    st.edges += 1;
                    st.lost += usize::from(!allowed(n));
                }
            }
            // Keep only a finalist's order later; page routing is recomputed for real scan.
        }
        if qi % 256 == 0 {
            eprintln!(
                "routing size={size} cells={cells} query={qi}/{}",
                queries.len()
            );
        }
    }
    let names = [
        "exact-f32",
        "exhaustive-code",
        "graph-code",
        "paged-graph-code",
    ];
    for mode in 0..4 {
        for (pi, &probes) in PROBES.iter().enumerate() {
            let st = &stats[mode][pi];
            let n = queries.len() as f64;
            println!(
                "CURVE,{cells},{size},{},{probes},{:.6},{:.1},{:.1},{:.1},{:.6}",
                names[mode],
                st.covered as f64 / (n * 10.0),
                st.rows as f64 / n,
                st.visits as f64 / n,
                st.rank_ns as f64 / n,
                st.lost as f64 / st.edges.max(1) as f64
            );
        }
        // Validate the first routing point at 0.95, and the following two probe counts.
        // These are measured points, never interpolated.
        if let Some(start) = stats[mode]
            .iter()
            .position(|s| s.covered as f64 / (queries.len() * 10) as f64 >= 0.95)
        {
            for &probes in PROBES.iter().skip(start).take(3) {
                let mut hits = 0;
                let mut scan_ns = 0;
                let mut rerank_ns = 0;
                let mut rank_ns = 0;
                let mut scanned_rows = 0;
                let mut visits = 0;
                for (qi, q) in queries.iter().enumerate() {
                    let t = Instant::now();
                    let plane = codes.query(q);
                    let (picked, visited): (Vec<usize>, usize) = match mode {
                        0 | 1 => {
                            let mut scored: Vec<Hit> = scanner
                                .global
                                .iter()
                                .map(|&l| {
                                    Hit(
                                        if mode == 0 {
                                            bf_vector::squared_distance(q, &layout.leaves[l])
                                        } else {
                                            codes.score(&plane, l)
                                        },
                                        l,
                                    )
                                })
                                .collect();
                            if scored.len() > probes {
                                scored.select_nth_unstable(probes - 1);
                                scored.truncate(probes);
                            }
                            scored.sort_unstable();
                            (
                                scored.into_iter().map(|h| h.1).collect(),
                                scanner.global.len(),
                            )
                        }
                        2 => {
                            let (found, v) =
                                search.run(&graph.edges, &[graph.entry], probes * 2, live, |l| {
                                    codes.score(&plane, l)
                                });
                            (found.into_iter().take(probes).map(|h| h.1).collect(), v)
                        }
                        _ => {
                            let fetched = scanner.pages.select_cells(q, probes * 16, 0.5)?;
                            let mut allowed_pages = vec![false; scanner.pages.cell_count()];
                            let mut seeds = Vec::new();
                            for p in fetched {
                                allowed_pages[p] = true;
                                seeds.extend_from_slice(&page_seeds[p]);
                            }
                            let (found, v) = search.run(
                                &graph.edges,
                                &seeds,
                                probes * 2,
                                |l| live(l) && allowed_pages[scanner.page_of[l]],
                                |l| codes.score(&plane, l),
                            );
                            (found.into_iter().take(probes).map(|h| h.1).collect(), v)
                        }
                    };
                    rank_ns += t.elapsed().as_nanos();
                    visits += visited;
                    scanned_rows += picked.iter().map(|&l| count[l]).sum::<usize>();
                    let (result, scan, rerank) = scanner.scan(corpus, q, &picked)?;
                    hits += result
                        .iter()
                        .filter(|r| truth[qi][..10].contains(r))
                        .count();
                    scan_ns += scan;
                    rerank_ns += rerank;
                }
                let n = queries.len() as f64;
                println!(
                    "ANN,{cells},{size},{},{probes},{:.6},{:.1},{:.1},{:.1},{:.1},{:.1},{:.6}",
                    names[mode],
                    hits as f64 / (n * 10.0),
                    scanned_rows as f64 / n,
                    visits as f64 / n,
                    rank_ns as f64 / n,
                    scan_ns as f64 / n,
                    rerank_ns as f64 / n,
                    rank_ns as f64 / scan_ns as f64
                );
            }
        }
    }
    let edges: usize = graph
        .edges
        .iter()
        .enumerate()
        .filter(|(l, _)| live(*l))
        .map(|(_, ns)| ns.len())
        .sum();
    let lost: usize = graph
        .edges
        .iter()
        .enumerate()
        .filter(|(l, _)| live(*l))
        .map(|(l, ns)| {
            ns.iter()
                .filter(|&&n| scanner.page_of[l] != scanner.page_of[n])
                .count()
        })
        .sum();
    println!(
        "PAGES,{cells},{size},{},{edges},{lost},{:.6},{}",
        scanner.pages.cell_count(),
        lost as f64 / edges as f64,
        scanner.page_bytes.len()
    );
    Ok(())
}

fn assignment_study(
    corpus: &Npy,
    layout: &baseline::Layout,
    graph: &Graph,
    codes: &Codes,
    size: usize,
    queries: &[Vec<f32>],
    truth: &[Vec<u32>],
) {
    let count = counts(&layout.leaf_of, layout.leaves.len());
    const COVERAGE_PROBES: &[usize] = &[256, 524, 1024, 2048, 4096, 8192];
    // The frozen query order is shared by every assignment and probe count. I keep only
    // its longest measured prefix, outside the timed assignment work.
    let query_order: Vec<Vec<u32>> = queries
        .par_iter()
        .map(|q| {
            let plane = codes.query(q);
            let mut ranking: Vec<_> = (0..layout.leaves.len())
                .filter(|&l| count[l] > 0)
                .map(|l| Hit(codes.score(&plane, l), l))
                .collect();
            let keep = *COVERAGE_PROBES.last().unwrap();
            if ranking.len() > keep {
                ranking.select_nth_unstable(keep - 1);
                ranking.truncate(keep);
            }
            ranking.sort_unstable();
            ranking.into_iter().map(|h| h.1 as u32).collect()
        })
        .collect();
    for candidates in [8, 16, 32, 64] {
        let t = Instant::now();
        let results: Vec<_> = (0..corpus.rows)
            .into_par_iter()
            .map_init(
                || (Vec::new(), Search::new(layout.leaves.len())),
                |(buf, search), r| {
                    let q = corpus.vector(r, buf);
                    let plane = codes.query(&q);
                    let (found, visited) = search.run(
                        &graph.edges,
                        &[graph.entry],
                        candidates * 2,
                        |l| count[l] > 0,
                        |l| codes.score(&plane, l),
                    );
                    let best = found
                        .iter()
                        .take(candidates)
                        .map(|h| Hit(bf_vector::squared_distance(&q, &layout.leaves[h.1]), h.1))
                        .min()
                        .unwrap();
                    let old = layout.leaf_of[r] as usize;
                    let old_d = bf_vector::squared_distance(&q, &layout.leaves[old]);
                    (best.1 as u32, visited, best.0, old_d)
                },
            )
            .collect();
        let seconds = t.elapsed().as_secs_f64();
        let new_assignment: Vec<_> = results.iter().map(|r| r.0).collect();
        let new_counts = counts(&new_assignment, layout.leaves.len());
        for &probes in COVERAGE_PROBES {
            let mut before = 0;
            let mut after = 0;
            let mut old_rows = 0;
            let mut new_rows = 0;
            for (order, gt) in query_order.iter().zip(truth) {
                let chosen = &order[..probes.min(order.len())];
                old_rows += chosen.iter().map(|&l| count[l as usize]).sum::<usize>();
                new_rows += chosen
                    .iter()
                    .map(|&l| new_counts[l as usize])
                    .sum::<usize>();
                before += gt
                    .iter()
                    .filter(|&&r| chosen.contains(&layout.leaf_of[r as usize]))
                    .count();
                after += gt
                    .iter()
                    .filter(|&&r| chosen.contains(&new_assignment[r as usize]))
                    .count();
            }
            let n = queries.len() as f64;
            println!(
                "ASSIGN_COVERAGE,8,{size},{candidates},{probes},{:.6},{:.6},{:.1},{:.1}",
                before as f64 / (n * 10.0),
                after as f64 / (n * 10.0),
                old_rows as f64 / n,
                new_rows as f64 / n
            );
        }

        let agree = results
            .iter()
            .zip(&layout.leaf_of)
            .filter(|(a, b)| a.0 == **b)
            .count();
        let better = results.iter().filter(|r| r.2 <= r.3 + 1e-6).count();
        let visits: usize = results.iter().map(|r| r.1).sum();
        let old: f64 = results.iter().map(|r| r.3 as f64).sum();
        let new: f64 = results.iter().map(|r| r.2 as f64).sum();
        println!(
            "ASSIGN,8,{size},{candidates},{seconds:.6},{:.6},{:.6},{:.1},{:.6}",
            agree as f64 / corpus.rows as f64,
            better as f64 / corpus.rows as f64,
            visits as f64 / corpus.rows as f64,
            new / old
        );
    }
}
fn main() -> AnyResult<()> {
    let a: Vec<_> = std::env::args().collect();
    if a.len() < 3 {
        return Err("expected DATA_DIR RUN_DIR [query_count] [sizes] [cells]".into());
    }
    let run = PathBuf::from(&a[2]);
    std::fs::create_dir(&run)?;
    let _cleanup = Cleanup(run.clone());
    rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build_global()?;
    let data = Path::new(&a[1]);
    let corpus = Npy::open(&data.join("corpus.f16"), "<f2", 2)?;
    let queries = Npy::open(&data.join("queries.npy"), "<f4", 4)?;
    let truth = Npy::open(&data.join("truth.npy"), "<i4", 4)?;
    if corpus.cols != queries.cols || queries.rows != truth.rows || truth.cols < 10 {
        return Err("input shapes disagree".into());
    }
    let n = a
        .get(3)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(queries.rows)
        .min(queries.rows);
    if n == 0 {
        return Err("query count must be positive".into());
    }
    let mut q = Vec::new();
    let mut gt = Vec::new();
    for i in 0..n {
        let row = i * queries.rows / n;
        q.push(queries.vector(row, &mut Vec::new()));
        let mut buf = Vec::new();
        truth.bytes(row, &mut buf);
        let ids: Vec<u32> = buf
            .as_chunks::<4>()
            .0
            .iter()
            .take(10)
            .map(|b| u32::from_le_bytes(*b))
            .collect();
        if ids.iter().any(|&id| id as usize >= corpus.rows) {
            return Err("truth row outside corpus".into());
        }
        gt.push(ids);
    }
    let sizes: Vec<usize> = a
        .get(4)
        .map(String::as_str)
        .unwrap_or("200,100,64,32")
        .split(',')
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    let cells = a.get(5).map(|s| s.parse()).transpose()?.unwrap_or(32);
    if cells == 0 || sizes.contains(&0) {
        return Err("cells and leaf sizes must be positive".into());
    }
    let t = Instant::now();
    let (centers, rows) = coarse(&corpus, cells);
    println!(
        "COARSE,{cells},{},{},{:.6}",
        corpus.rows,
        n,
        t.elapsed().as_secs_f64()
    );
    let present: Vec<u32> = (0..corpus.rows as u32).collect();
    for size in sizes {
        let t = Instant::now();
        let layout = baseline::build(
            &present,
            &|r, buf| corpus.vector(r as usize, buf),
            &centers,
            rows.clone(),
            size,
        );
        println!(
            "LAYOUT,{cells},{size},{},{:.6}",
            layout.leaves.len(),
            t.elapsed().as_secs_f64()
        );
        let t = Instant::now();
        let mut graph = Graph::build(&layout.leaves);
        if layout.leaf_of.iter().all(|&l| l as usize != graph.entry) {
            graph.entry = layout.leaf_of[0] as usize;
        }
        println!(
            "GRAPH,{cells},{size},{},{:.6}",
            layout.leaves.len(),
            t.elapsed().as_secs_f64()
        );
        let t = Instant::now();
        let codes = Codes::new(&layout);
        println!("CODES,{cells},{size},{:.6}", t.elapsed().as_secs_f64());
        if cells == 8 {
            assignment_study(&corpus, &layout, &graph, &codes, size, &q, &gt);
            continue;
        }
        let t = Instant::now();
        let scanner = Scanner::build(&corpus, &layout, &run)?;
        println!("SCANNER,{cells},{size},{:.6}", t.elapsed().as_secs_f64());
        evaluate(
            &corpus, &q, &gt, &layout, &scanner, &graph, &codes, size, cells,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn write_array(path: &Path, descr: &str, rows: usize, cols: usize, bytes: &[u8]) {
        let header = format!(
            "{{'descr': '{descr}', 'fortran_order': False, 'shape': ({rows}, {cols}), }}\n"
        );
        let mut out = b"\x93NUMPY\x01\x00".to_vec();
        out.extend_from_slice(&(header.len() as u16).to_le_bytes());
        out.extend(header.as_bytes());
        out.extend(bytes);
        std::fs::write(path, out).unwrap();
    }
    #[test]
    fn scan_codes_pages_and_numpy_match_exact_truth() {
        std::fs::create_dir_all(".scratch").unwrap();
        let temp = tempfile::Builder::new()
            .prefix("graph-study-test-")
            .tempdir_in(".scratch")
            .unwrap();
        let path = temp.path().join("corpus.f16");
        let mut bytes = Vec::new();
        for i in 0..128 {
            for d in 0..16 {
                let v = ((i * 37 + d * 17) % 101) as f32 / 50.0 - 1.0;
                bytes.extend_from_slice(&half::f16::from_f32(v).to_le_bytes());
            }
        }
        write_array(&path, "<f2", 128, 16, &bytes);
        let corpus = Npy::open(&path, "<f2", 2).unwrap();
        let (coarse, rows) = coarse(&corpus, 2);
        let layout = baseline::build(
            &(0..128).collect::<Vec<_>>(),
            &|r, b| corpus.vector(r as usize, b),
            &coarse,
            rows,
            16,
        );
        let scanner = Scanner::build(&corpus, &layout, temp.path()).unwrap();
        let codes = Codes::new(&layout);
        assert_eq!(
            scanner
                .global
                .iter()
                .map(|&l| counts(&layout.leaf_of, layout.leaves.len())[l])
                .sum::<usize>(),
            128
        );
        assert!(
            scanner
                .global
                .iter()
                .all(|&l| scanner.page_of[l] < scanner.pages.cell_count())
        );
        for i in [0, 17, 63, 127] {
            let q = corpus.vector(i, &mut Vec::new());
            let plane = codes.query(&q);
            let production = scanner.directory.center_distances_on(&q, false).unwrap();
            for (&l, &d) in scanner.global.iter().zip(&production) {
                assert!(
                    (codes.score(&plane, l) - d).abs() < 1e-5,
                    "center score differs"
                );
            }
            let (got, _, _) = scanner.scan(&corpus, &q, &scanner.global).unwrap();
            let mut exact: Vec<_> = (0..128)
                .map(|r| {
                    Hit(
                        bf_vector::squared_distance(&q, &corpus.vector(r, &mut Vec::new())),
                        r,
                    )
                })
                .collect();
            exact.sort_unstable();
            // Ties from duplicate input vectors are ordered by document ID in both paths.
            assert_eq!(
                got,
                exact[..10].iter().map(|h| h.1 as u32).collect::<Vec<_>>()
            );
        }
        std::fs::write(&path, b"invalid").unwrap();
        assert!(Npy::open(&path, "<f2", 2).is_err());
    }
}
