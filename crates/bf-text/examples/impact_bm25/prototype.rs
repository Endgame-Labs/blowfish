use super::*;
use std::io::{BufRead, Write};
use std::path::Path;
use std::time::Instant;

struct ImpactBlock {
    rows: Vec<(u32, u32)>,
    max_tf: u32,
    min_length: u32,
}
struct List {
    blocks: Vec<ImpactBlock>,
    dense: Vec<u32>,
    maxima: Vec<f64>,
}
struct Index {
    lists: BTreeMap<String, List>,
    lengths: Vec<u32>,
    norms: Vec<f64>,
}
fn open(root: &Path, i: usize) -> SegmentReader {
    futures::executor::block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
        std::fs::read(root.join(format!("segment-{i}.bft"))).unwrap(),
    ))))
    .unwrap()
}
fn packed_size(rows: &[(u32, u32)]) -> usize {
    let mut deltas = [0; BLOCK];
    let mut tfs = [0; BLOCK];
    let mut prev = rows[0].0;
    for (i, &(row, tf)) in rows.iter().enumerate() {
        deltas[i] = row - prev;
        tfs[i] = tf;
        prev = row;
    }
    let packer = BitPacker4x::new();
    2 + 16 * (packer.num_bits(&deltas) as usize + packer.num_bits(&tfs) as usize)
}
fn prepare(
    reader: &SegmentReader,
    wanted: &std::collections::BTreeSet<String>,
    reorder: bool,
) -> (Index, i64, usize) {
    let stats = futures::executor::block_on(reader.stats()).unwrap();
    let table = Saturation::new(&stats, &FtsParams::default());
    let lengths = reader.lens().to_vec();
    let mut lists = BTreeMap::new();
    let mut growth = 0i64;
    let mut long = 0;
    for chunk in futures::executor::block_on(reader.all_terms())
        .unwrap()
        .chunks(128)
    {
        let metas =
            futures::executor::block_on(reader.load(chunk.iter().map(|(_, id)| *id))).unwrap();
        for (word, id) in chunk {
            let word = std::str::from_utf8(word).unwrap();
            let meta = &metas[id];
            let is_long = meta.df > 4096 || meta.blocks.len() > 32;
            if !is_long && !wanted.contains(word) {
                continue;
            }
            let mut rows = Vec::with_capacity(meta.df as usize);
            for b in &meta.blocks {
                rows.extend_from_slice(
                    &futures::executor::block_on(reader.postings(b, meta.df)).unwrap(),
                );
            }
            if is_long && reorder {
                long += 1;
                rows.sort_unstable_by(|&(a, at), &(b, bt)| {
                    table
                        .factor(bt, lengths[b as usize])
                        .total_cmp(&table.factor(at, lengths[a as usize]))
                        .then(a.cmp(&b))
                });
            }
            let blocks: Vec<_> = rows
                .chunks(BLOCK)
                .map(|rows| {
                    let mut rows = rows.to_vec();
                    rows.sort_unstable();
                    ImpactBlock {
                        max_tf: rows.iter().map(|r| r.1).max().unwrap(),
                        min_length: rows.iter().map(|r| lengths[r.0 as usize]).min().unwrap(),
                        rows,
                    }
                })
                .collect();
            if is_long {
                growth += blocks
                    .iter()
                    .map(|b| packed_size(&b.rows) as i64)
                    .sum::<i64>()
                    - meta.blocks.iter().map(|b| i64::from(b.size)).sum::<i64>();
            }
            if wanted.contains(word) {
                let mut dense = if !reorder && is_long {
                    vec![0; lengths.len().div_ceil(BLOCK) * BLOCK]
                } else {
                    vec![]
                };
                // Recomputed below using namespace statistics before queries run.
                let maxima = vec![];
                if !dense.is_empty() {
                    for b in &blocks {
                        for &(row, tf) in &b.rows {
                            dense[row as usize] = tf;
                        }
                    }
                }
                lists.insert(
                    word.to_owned(),
                    List {
                        blocks,
                        dense,
                        maxima,
                    },
                );
            }
        }
    }
    (
        Index {
            lists,
            lengths,
            norms: vec![],
        },
        growth,
        long,
    )
}

// An indexed bounded heap supports increasing lower scores without duplicate hits.
struct Top {
    heap: Vec<SearchHit>,
    positions: Vec<usize>,
    k: usize,
}
impl Top {
    fn new(rows: usize, k: usize) -> Self {
        Self {
            heap: vec![],
            positions: vec![usize::MAX; rows],
            k,
        }
    }
    fn swap(&mut self, a: usize, b: usize) {
        self.heap.swap(a, b);
        self.positions[self.heap[a].row as usize] = a;
        self.positions[self.heap[b].row as usize] = b;
    }
    fn down(&mut self, mut i: usize) {
        loop {
            let mut child = i * 2 + 1;
            if child >= self.heap.len() {
                break;
            }
            if child + 1 < self.heap.len() && self.heap[child + 1] < self.heap[child] {
                child += 1;
            }
            if self.heap[i] <= self.heap[child] {
                break;
            }
            self.swap(i, child);
            i = child;
        }
    }
    fn add(&mut self, hit: SearchHit) {
        let pos = self.positions[hit.row as usize];
        if pos != usize::MAX {
            self.heap[pos] = hit;
            self.down(pos);
        } else if self.heap.len() < self.k {
            self.positions[hit.row as usize] = self.heap.len();
            self.heap.push(hit);
            let mut i = self.heap.len() - 1;
            while i > 0 && self.heap[i] < self.heap[(i - 1) / 2] {
                self.swap(i, (i - 1) / 2);
                i = (i - 1) / 2;
            }
        } else if hit > self.heap[0] {
            self.positions[self.heap[0].row as usize] = usize::MAX;
            self.positions[hit.row as usize] = 0;
            self.heap[0] = hit;
            self.down(0);
        }
    }
    fn floor(&self) -> f32 {
        if self.heap.len() == self.k {
            self.heap[0].score
        } else {
            0.0
        }
    }
    fn hits(mut self) -> Vec<SearchHit> {
        self.heap.sort_unstable_by(|a, b| b.cmp(a));
        self.heap
    }
}

#[derive(Clone, Copy, PartialEq)]
struct Next {
    bound: f64,
    term: usize,
}
impl Eq for Next {}
impl Ord for Next {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bound
            .total_cmp(&other.bound)
            .then(self.term.cmp(&other.term))
    }
}
impl PartialOrd for Next {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn impact(
    index: &Index,
    query: &Query,
    stats: &CorpusStats,
    table: &Saturation,
    floor: f32,
    perfect: bool,
) -> (Vec<SearchHit>, usize) {
    let params = FtsParams::default();
    let terms: Vec<_> = query
        .exact_terms()
        .into_iter()
        .filter_map(|(word, qf)| {
            index.lists.get(word).map(|list| {
                (
                    list,
                    TermWeight::new(qf, stats.document_frequencies[word], stats, &params),
                )
            })
        })
        .collect();
    let n = terms.len();
    if n == 0 {
        return (vec![], 0);
    }
    assert!(n < 64);
    let rows = index.lengths.len();
    let mut suffixes = Vec::new();
    let mut queue = BinaryHeap::new();
    let mut heads = vec![0; n];
    let mut remaining = vec![0.0f64; n];
    for (t, (list, weight)) in terms.iter().enumerate() {
        let mut bounds: Vec<_> = list
            .blocks
            .iter()
            .enumerate()
            .map(|(i, b)| {
                if perfect {
                    f64::from(weight.score_saturation(list.maxima[i]))
                } else {
                    f64::from(weight.score_cached(b.max_tf, b.min_length, table))
                }
            })
            .collect();
        bounds.push(0.0);
        for i in (0..bounds.len() - 1).rev() {
            bounds[i] = bounds[i].max(bounds[i + 1]);
        }
        remaining[t] = bounds[0];
        queue.push(Next {
            term: t,
            bound: bounds[0],
        });
        suffixes.push(bounds);
    }
    let mut scores = vec![0.0f32; rows * n];
    let mut masks = vec![0u64; rows];
    let mut touched = Vec::new();
    let mut top = Top::new(rows, 10);
    let mut blocks: usize = 0;
    let mut active = (1u64 << n) - 1;
    let margin = 1.0 / (1.0 - (n as f64 + 8.0) * f64::from(f32::EPSILON));
    while let Some(Next { term: t, .. }) = queue.pop() {
        let (list, weight) = &terms[t];
        let b = &list.blocks[heads[t]];
        for &(row, tf) in &b.rows {
            let r = row as usize;
            if masks[r] == 0 {
                touched.push(row);
            }
            scores[r * n + t] = weight.score_cached(tf, index.lengths[r], table);
            masks[r] |= 1 << t;
            let score = scores[r * n..(r + 1) * n].iter().copied().sum::<f32>();
            top.add(SearchHit { row, score });
        }
        blocks += 1;
        heads[t] += 1;
        remaining[t] = suffixes[t][heads[t]];
        if heads[t] < list.blocks.len() {
            queue.push(Next {
                term: t,
                bound: remaining[t],
            });
        } else {
            active &= !(1 << t);
        }
        if !blocks.is_multiple_of(32) && !queue.is_empty() {
            continue;
        }
        let kth = top.floor().max(floor);
        if kth <= 0.0 || remaining.iter().sum::<f64>() * margin >= f64::from(kth) {
            continue;
        }
        let mut unresolved = false;
        for &row in &touched {
            let r = row as usize;
            let missing = active & !masks[r];
            if missing == 0 {
                continue;
            }
            // Winners must have exact scores; a partial lower bound cannot certify a result.
            if top.positions[r] != usize::MAX {
                unresolved = true;
                break;
            }
            let mut upper = 0.0;
            for t in 0..n {
                upper += if masks[r] & (1 << t) == 0 {
                    remaining[t]
                } else {
                    f64::from(scores[r * n + t])
                };
            }
            if upper * margin >= f64::from(kth) {
                unresolved = true;
                break;
            }
        }
        if !unresolved {
            break;
        }
    }
    (
        top.hits()
            .into_iter()
            .filter(|h| h.score >= floor)
            .collect(),
        blocks,
    )
}
fn perf(command: &str) {
    if let Some(path) = std::env::var_os("BF_TEXT_PERF_CONTROL") {
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        writeln!(file, "{command}").unwrap();
        if let Some(path) = std::env::var_os("BF_TEXT_PERF_ACK") {
            let mut line = String::new();
            std::io::BufReader::new(std::fs::File::open(path).unwrap())
                .read_line(&mut line)
                .unwrap();
        }
    }
}

pub fn run() {
    let args: Vec<_> = std::env::args().collect();
    let root = Path::new(args.get(1).expect("fixture directory"));
    let mode = args.get(2).expect("baseline|impact|impact-perfect|dense|dense-scalar|dense-integrated|dense-integrated-scalar|cold");
    assert!(
        matches!(
            mode.as_str(),
            "baseline"
                | "impact"
                | "impact-perfect"
                | "dense"
                | "dense-scalar"
                | "dense-integrated"
                | "dense-integrated-scalar"
                | "cold"
        ),
        "unknown variant: {mode}"
    );
    let workload: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(root.join("workload.json")).unwrap()).unwrap();
    let params = FtsParams::default();
    let queries: Vec<_> = workload
        .iter()
        .map(|w| {
            Query::new(
                crate::tokenize(w["request"]["rank_by"][2].as_str().unwrap(), &params),
                false,
            )
        })
        .collect();
    let stats: CorpusStats =
        serde_json::from_slice(&std::fs::read(root.join("stats.json")).unwrap()).unwrap();
    let table = Saturation::new(&stats, &params);
    let mut readers: Vec<_> = (0..5).map(|i| open(root, i)).collect();
    let wanted = queries
        .iter()
        .flat_map(|q| q.tokens.iter().map(|t| t.text.clone()))
        .collect();
    let mut indexes = Vec::new();
    let mut growth = 0i64;
    if mode.starts_with("impact") || mode.starts_with("dense") {
        for r in &readers {
            let (mut index, delta, long) = prepare(r, &wanted, mode.starts_with("impact"));
            index.norms = normalizers(&index.lengths, &stats);
            for list in index.lists.values_mut() {
                if mode.starts_with("impact") {
                    list.maxima = list
                        .blocks
                        .iter()
                        .map(|b| {
                            b.rows
                                .iter()
                                .map(|&(row, tf)| table.factor(tf, index.lengths[row as usize]))
                                .fold(0.0, f64::max)
                        })
                        .collect();
                } else {
                    list.maxima = vec![0.0; index.lengths.len().div_ceil(BLOCK)];
                    for b in &list.blocks {
                        for &(row, tf) in &b.rows {
                            let m = &mut list.maxima[row as usize / BLOCK];
                            *m = m.max(table.factor(tf, index.lengths[row as usize]));
                        }
                    }
                }
            }
            growth += delta;
            eprintln!(
                "prepared {} terms; {long} long lists; delta={delta}",
                index.lists.len()
            );
            indexes.push(index);
        }
        readers = (0..5).map(|i| open(root, i)).collect();
    }
    let truth_path = root.join("b4-truth.json");
    assert!(
        mode == "baseline" || truth_path.exists(),
        "run baseline first to establish truth"
    );
    let mut truth: Vec<Vec<SearchHit>> = if truth_path.exists() {
        let data: Vec<Vec<(u32, f32)>> =
            serde_json::from_slice(&std::fs::read(&truth_path).unwrap()).unwrap();
        data.into_iter()
            .map(|hits| {
                hits.into_iter()
                    .map(|(row, score)| SearchHit { row, score })
                    .collect()
            })
            .collect()
    } else {
        vec![]
    };
    let start = Instant::now();
    let mut blocks: usize = 0;
    let passes = if mode == "cold" { 1 } else { 2 };
    for pass in 0..passes {
        if pass == passes - 1 {
            perf("enable");
        }
        for (q, query) in queries.iter().enumerate() {
            let mut hits = Vec::new();
            let threshold = SearchThreshold::new(10);
            let mut floor = 0.0f32;
            for i in 0..5 {
                let (mut found, count) = if mode.starts_with("dense-integrated") {
                    let (found, m) = futures::executor::block_on(readers[i].dense_integrated(
                        query,
                        &stats,
                        &params,
                        SearchMode::Shared(&threshold),
                        &indexes[i],
                        mode.ends_with("scalar"),
                    ))
                    .unwrap();
                    (found, m.blocks_read)
                } else if mode.starts_with("dense") {
                    dense(
                        &indexes[i],
                        query,
                        &stats,
                        &table,
                        floor,
                        mode == "dense-scalar",
                    )
                } else if mode.starts_with("impact") {
                    impact(
                        &indexes[i],
                        query,
                        &stats,
                        &table,
                        floor,
                        mode == "impact-perfect",
                    )
                } else {
                    if mode == "cold" {
                        readers[i].decoded = Default::default();
                        readers[i].saturation = Default::default();
                    }
                    let r = &readers[i];
                    let (found, m) = futures::executor::block_on(r.search_internal(
                        query,
                        &stats,
                        &params,
                        10,
                        None,
                        SearchMode::Shared(&threshold),
                    ))
                    .unwrap();
                    (found, m.blocks_read)
                };
                if pass == passes - 1 {
                    blocks += count;
                }
                for h in &mut found {
                    h.row += i as u32 * 200_000;
                }
                hits.extend(found);
                hits.sort_unstable_by(|a, b| b.cmp(a));
                hits.truncate(10);
                if hits.len() == 10 {
                    floor = hits[9].score;
                }
            }
            if truth.len() <= q {
                truth.push(hits);
            } else {
                assert_eq!(hits, truth[q], "{mode}, query {q}");
            }
        }
        if pass == passes - 1 {
            perf("disable");
        }
        eprintln!("pass {pass} done in {:.3}s", start.elapsed().as_secs_f64());
    }
    let serial: Vec<Vec<_>> = truth
        .iter()
        .map(|h| h.iter().map(|h| (h.row, h.score)).collect())
        .collect();
    if mode == "baseline" {
        std::fs::write(truth_path, serde_json::to_vec(&serial).unwrap()).unwrap();
    }
    eprintln!(
        "{}",
        serde_json::json!({"mode":mode,"queries":queries.len(),"blocks":blocks,"posting_delta":growth,"seconds":start.elapsed().as_secs_f64()})
    );
}

// Exact f64 arithmetic in two NEON lanes, preserving the reference operation order.
// Normalizers are query invariant and are prepared outside the measured pass.
#[cfg(target_arch = "aarch64")]
fn vector_scores(tfs: &[u32], norms: &[f64], idf: f64, qf: f64, out: &mut [f32]) {
    use std::arch::aarch64::*;
    assert_eq!(tfs.len(), norms.len());
    assert_eq!(tfs.len(), out.len());
    let k1 = FtsParams::default().k1 as f64;
    // SAFETY: NEON is mandatory on aarch64, and the slices have equal lengths.
    unsafe {
        let scale = vdupq_n_f64(k1 + 1.0);
        let idf = vdupq_n_f64(idf);
        let qf = vdupq_n_f64(qf);
        for i in (0..tfs.len() / 2 * 2).step_by(2) {
            let tf = vcvtq_f64_u64(vmovl_u32(vld1_u32(tfs.as_ptr().add(i))));
            let norm = vld1q_f64(norms.as_ptr().add(i));
            let sat = vdivq_f64(vmulq_f64(tf, scale), vaddq_f64(tf, norm));
            vst1_f32(
                out.as_mut_ptr().add(i),
                vcvt_f32_f64(vmulq_f64(vmulq_f64(idf, sat), qf)),
            );
        }
        if !tfs.len().is_multiple_of(2) {
            let i = tfs.len() - 1;
            let tf = tfs[i] as f64;
            out[i] = (vgetq_lane_f64::<0>(idf)
                * (tf * (k1 + 1.0) / (tf + norms[i]))
                * vgetq_lane_f64::<0>(qf)) as f32;
        }
    }
}
#[cfg(not(target_arch = "aarch64"))]
fn vector_scores(tfs: &[u32], norms: &[f64], idf: f64, qf: f64, out: &mut [f32]) {
    let k1 = FtsParams::default().k1 as f64;
    for ((&tf, &norm), out) in tfs.iter().zip(norms).zip(out) {
        let tf = tf as f64;
        *out = (idf * (tf * (k1 + 1.0) / (tf + norm)) * qf) as f32;
    }
}

fn dense(
    index: &Index,
    query: &Query,
    stats: &CorpusStats,
    table: &Saturation,
    floor: f32,
    scalar: bool,
) -> (Vec<SearchHit>, usize) {
    let params = FtsParams::default();
    let terms: Vec<_> = query
        .exact_terms()
        .into_iter()
        .filter_map(|(word, qf)| {
            index.lists.get(word).map(|list| {
                let df = stats.document_frequencies[word] as f64;
                let idf = (1.0 + (stats.total_docs as f64 - df + 0.5) / (df + 0.5)).ln();
                let qsat = qf as f64 * (params.k3 as f64 + 1.0) / (qf as f64 + params.k3 as f64);
                (
                    list,
                    TermWeight::new(qf, df as u64, stats, &params),
                    idf,
                    qsat,
                )
            })
        })
        .collect();
    let n = terms.len();
    let windows = index.lengths.len().div_ceil(BLOCK);
    let margin = 1.0 / (1.0 - (n as f64 + 8.0) * f64::from(f32::EPSILON));
    let mut bounds = vec![0.0; windows];
    let mut per_term = Vec::new();
    let mut sparse = vec![0.0f32; index.lengths.len()];
    for (list, weight, _, _) in &terms {
        let best: Vec<_> = list
            .maxima
            .iter()
            .map(|&v| f64::from(weight.score_saturation(v)))
            .collect();
        if !list.dense.is_empty() {
            for (b, v) in bounds.iter_mut().zip(&best) {
                *b += v;
            }
        } else {
            for b in &list.blocks {
                for &(row, tf) in &b.rows {
                    sparse[row as usize] +=
                        weight.score_cached(tf, index.lengths[row as usize], table);
                }
            }
        }
        per_term.push(best);
    }
    let dense_bounds = bounds.clone();
    for (w, rows) in sparse.chunks(BLOCK).enumerate() {
        bounds[w] += f64::from(rows.iter().copied().fold(0.0, f32::max));
    }
    let mut order: Vec<_> = bounds
        .into_iter()
        .enumerate()
        .filter(|(_, b)| *b > 0.0)
        .collect();
    order.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    let mut top = Top::new(index.lengths.len(), 10);
    for (w, bound) in order {
        let kth = floor.max(top.floor());
        if bound * margin < f64::from(kth) {
            break;
        }
        let lo = w * BLOCK;
        let hi = (lo + BLOCK).min(index.lengths.len());
        let mut candidates = [true; BLOCK];
        let mut by_bound: Vec<_> = (0..n).map(|t| (t, per_term[t][w])).collect();
        by_bound.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
        let mut essential = vec![true; n];
        let mut nonessential = 0.0;
        for (t, b) in by_bound {
            if (nonessential + b) * margin >= f64::from(kth) {
                break;
            }
            nonessential += b;
            essential[t] = false;
        }
        if essential.iter().any(|&b| !b) {
            candidates.fill(false);
            for (t, (list, _, _, _)) in terms.iter().enumerate().filter(|(t, _)| essential[*t]) {
                let _ = t;
                if !list.dense.is_empty() {
                    for r in lo..hi {
                        candidates[r - lo] |= list.dense[r] > 0;
                    }
                } else {
                    for block in &list.blocks {
                        if block.rows.last().unwrap().0 < lo as u32 {
                            continue;
                        }
                        if block.rows[0].0 >= hi as u32 {
                            break;
                        }
                        for &(r, _) in &block.rows {
                            if (lo..hi).contains(&(r as usize)) {
                                candidates[r as usize - lo] = true;
                            }
                        }
                    }
                }
            }
        }
        for r in lo..hi {
            candidates[r - lo] &=
                (f64::from(sparse[r]) + dense_bounds[w]) * margin >= f64::from(kth);
        }
        if !candidates.iter().any(|&v| v) {
            continue;
        }
        let mut scores = [0.0f32; BLOCK];
        for (list, weight, idf, qsat) in &terms {
            if !list.dense.is_empty() {
                if !scalar && candidates[..hi - lo].iter().filter(|&&v| v).count() >= 16 {
                    let mut term_scores = [0.0f32; BLOCK];
                    vector_scores(
                        &list.dense[lo..hi],
                        &index.norms[lo..hi],
                        *idf,
                        *qsat,
                        &mut term_scores[..hi - lo],
                    );
                    for (score, term) in scores.iter_mut().zip(term_scores) {
                        *score += term;
                    }
                } else {
                    for r in lo..hi {
                        if candidates[r - lo] {
                            scores[r - lo] +=
                                weight.score_cached(list.dense[r], index.lengths[r], table);
                        }
                    }
                }
            } else {
                // Directory endpoints avoid scanning unrelated sparse blocks.
                let first = list
                    .blocks
                    .partition_point(|b| b.rows.last().unwrap().0 < lo as u32);
                for block in &list.blocks[first..] {
                    if block.rows[0].0 >= hi as u32 {
                        break;
                    }
                    let first = block.rows.partition_point(|&(r, _)| r < lo as u32);
                    for &(r, tf) in &block.rows[first..] {
                        if r >= hi as u32 {
                            break;
                        }
                        if candidates[r as usize - lo] {
                            scores[r as usize - lo] +=
                                weight.score_cached(tf, index.lengths[r as usize], table);
                        }
                    }
                }
            }
        }
        for r in lo..hi {
            if candidates[r - lo] && scores[r - lo] > 0.0 {
                top.add(SearchHit {
                    row: r as u32,
                    score: scores[r - lo],
                });
            }
        }
    }
    (
        top.hits()
            .into_iter()
            .filter(|h| h.score >= floor)
            .collect(),
        0,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn vector_arithmetic_preserves_reference_bits_including_zero_and_tail() {
        let params = FtsParams::default();
        for average in [1.0, 57.123, 1000.0] {
            let stats = CorpusStats {
                total_docs: 10000,
                average_doc_length: average,
                ..Default::default()
            };
            let table = Saturation::new(&stats, &params);
            for qf in [1, 3, 10] {
                let df = 123;
                let weight = TermWeight::new(qf, df, &stats, &params);
                let n = stats.total_docs as f64;
                let df = df as f64;
                let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                let qsat = qf as f64 * (params.k3 as f64 + 1.0) / (qf as f64 + params.k3 as f64);
                let tfs: Vec<u32> = (0..129).map(|i| i % 23).collect();
                let lengths: Vec<_> = (0..129).map(|i| (i * 47 + 100) as u32).collect();
                let k1 = params.k1 as f64;
                let b = params.b as f64;
                let norms: Vec<_> = lengths
                    .iter()
                    .map(|&len| k1 * (1.0 - b + b * len as f64 / average))
                    .collect();
                let mut out = vec![0.0; 129];
                vector_scores(&tfs, &norms, idf, qsat, &mut out);
                for i in 0..129 {
                    assert_eq!(
                        out[i].to_bits(),
                        weight.score_cached(tfs[i], lengths[i], &table).to_bits(),
                        "row {i}"
                    );
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        #[test]
        fn impact_and_dense_match_exhaustive_with_missing_terms_and_changed_average(
            documents in prop::collection::vec(prop::collection::vec(0u8..6,0..32),1..300),
            query_terms in prop::collection::vec(0u8..8,1..12),
            average in 1.0f64..200.0,
        ) {
            let p = FtsParams::default();
            let docs: Vec<_> = documents.iter().map(|words| crate::tokenize(&words.iter().map(|w| format!("t{w}")).collect::<Vec<_>>().join(" "),&p)).collect();
            let query = Query::new(crate::tokenize(&query_terms.iter().map(|w| format!("t{w}")).collect::<Vec<_>>().join(" "),&p),false);
            let mut stats = CorpusStats::from_documents(&docs);
            stats.average_doc_length = average;
            let table = Saturation::new(&stats,&p);
            let mut lists = BTreeMap::new();
            let lengths: Vec<_> = docs.iter().map(|d| d.len().max(1) as u32).collect();
            for word in query.exact_terms().keys() {
                let mut rows: Vec<_> = docs.iter().enumerate().filter_map(|(r,d)| {
                    let tf = d.iter().filter(|t| t.text == *word).count() as u32;
                    (tf > 0).then_some((r as u32,tf))
                }).collect();
                if rows.is_empty() { continue; }
                // Use deliberately small impact blocks to exercise multiple termination checks.
                rows.sort_unstable_by(|&(a,at),&(b,bt)| table.factor(bt,lengths[b as usize]).total_cmp(&table.factor(at,lengths[a as usize])));
                let blocks: Vec<_> = rows.chunks(4).map(|chunk| {
                    let mut rows = chunk.to_vec(); rows.sort_unstable();
                    ImpactBlock { max_tf:rows.iter().map(|r| r.1).max().unwrap(),min_length:rows.iter().map(|r| lengths[r.0 as usize]).min().unwrap(),rows }
                }).collect();
                let maxima = blocks.iter().map(|b| b.rows.iter().map(|&(r,tf)| table.factor(tf,lengths[r as usize])).fold(0.0,f64::max)).collect();
                lists.insert((*word).to_owned(),List { blocks,dense:vec![],maxima });
            }
            let norms = normalizers(&lengths,&stats);
            let index = Index { lists,lengths,norms };
            let mut truth: Vec<_> = docs.iter().enumerate().filter_map(|(row,d)| {
                let score = crate::scoring::score_document(d,&query,&stats,&p);
                (score > 0.0).then_some(SearchHit { row:row as u32,score })
            }).collect();
            truth.sort_unstable_by(|a,b| b.cmp(a)); truth.truncate(10);
            for perfect in [false,true] { prop_assert_eq!(&impact(&index,&query,&stats,&table,0.0,perfect).0,&truth); }
            // Dense blocks use row order, with exact window maxima cached as an optimistic warm ceiling.
            let mut index = index;
            for list in index.lists.values_mut() {
                let mut rows: Vec<_> = list.blocks.iter().flat_map(|b| b.rows.iter().copied()).collect(); rows.sort_unstable();
                list.dense = vec![0;index.lengths.len().div_ceil(BLOCK)*BLOCK];
                list.maxima = vec![0.0;index.lengths.len().div_ceil(BLOCK)];
                for &(r,tf) in &rows { list.dense[r as usize] = tf; let m = &mut list.maxima[r as usize/BLOCK]; *m = m.max(table.factor(tf,index.lengths[r as usize])); }
                list.blocks = rows.chunks(BLOCK).map(|rows| ImpactBlock { max_tf:0,min_length:0,rows:rows.to_vec() }).collect();
            }
            for scalar in [false,true] { prop_assert_eq!(&dense(&index,&query,&stats,&table,0.0,scalar).0,&truth); }
        }
    }
}

fn normalizers(lengths: &[u32], stats: &CorpusStats) -> Vec<f64> {
    let p = FtsParams::default();
    let k1 = p.k1 as f64;
    let b = p.b as f64;
    lengths
        .iter()
        .map(|&len| k1 * (1.0 - b + b * len as f64 / stats.average_doc_length))
        .collect()
}

// Same planner, bounds, decoded cache, and reads as the production reader.
// Only the final scoring loop reads the pretransposed warm dense-frequency cache.
impl SegmentReader {
    async fn dense_integrated(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        mode: SearchMode<'_>,
        index: &Index,
        scalar: bool,
    ) -> io::Result<(Vec<SearchHit>, SearchMetrics)> {
        let k = 10;
        let allowed: Option<&RoaringBitmap> = None;
        let prefetch = !matches!(mode, SearchMode::Demand);
        let threshold = if let SearchMode::Shared(threshold) = mode {
            Some(threshold)
        } else {
            None
        };
        params.validate().map_err(|e| input(&e))?;
        self.ensure_lengths().await?;
        let mut metrics = SearchMetrics::default();
        if k == 0 || query.tokens.is_empty() {
            return Ok((Vec::new(), metrics));
        }
        // A fully live, unfiltered segment needs no per-posting bitmap probe.
        // Check the actual range, rather than cardinality, because callers may
        // supply a bitmap containing ordinals outside this segment.
        let allowed = allowed.filter(|rows| !rows.contains_range(0..self.docs));
        let exact = query.exact_terms();
        let mut selections: BTreeMap<u64, (u32, bool, u64)> = BTreeMap::new();
        let exact_terms: Vec<&str> = exact.keys().copied().collect();
        let found = self.lookup(&exact_terms).await?;
        let mut dense_by_id = BTreeMap::new();
        for ((term, qf), id) in exact.iter().zip(found) {
            if let Some(id) = id {
                if let Some(list) = index.lists.get(*term).filter(|l| !l.dense.is_empty()) {
                    let n = stats.total_docs as f64;
                    let df = stats.document_frequencies[*term] as f64;
                    let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                    let qsat =
                        *qf as f64 * (params.k3 as f64 + 1.0) / (*qf as f64 + params.k3 as f64);
                    dense_by_id.insert(id, (&list.dense, idf, qsat));
                }

                selections.insert(
                    id,
                    (
                        *qf,
                        false,
                        *stats.document_frequencies.get(*term).unwrap_or(&0),
                    ),
                );
            }
        }
        if let Some(prefix) = query.prefix() {
            for id in self.prefix_terms(prefix).await? {
                selections.entry(id).or_insert((0, false, 0)).1 = true;
            }
        }
        let saturation = self.saturation(stats, params);
        let lengths = self.lens();
        let metas = self.load(selections.keys().copied()).await?;
        let windows = (self.num_docs() as usize).div_ceil(BLOCK);
        // Bound auxiliary window metadata, including broad prefix expansions.
        let cached_windows = if windows.saturating_mul(selections.len()) <= 65_536 {
            windows
        } else {
            0
        };
        let aligned: Vec<_> = selections
            .keys()
            .map(|id| dense_by_id.get(id).copied())
            .collect();
        let mut terms: Vec<_> = selections
            .iter()
            .map(|(&id, &(qf, prefix, df))| {
                SearchTerm::new(
                    metas[&id].clone(),
                    TermWeight::new(qf, df, stats, params),
                    prefix,
                )
            })
            .collect();
        if terms.is_empty() {
            return Ok((Vec::new(), metrics));
        }
        {
            let mut memo = self.decoded.lock().unwrap();
            for term in &mut terms {
                // Dense frames are consulted only after window pruning. Probing
                // them here hashes and retains every common-word frame, including
                // those that cannot affect the top k.
                term.postings = if self.sparse_term(term.meta.df) {
                    term.meta
                        .blocks
                        .iter()
                        .map(|b| memo.get(b.offset))
                        .collect()
                } else {
                    vec![None; term.meta.blocks.len()]
                };
                term.small = prefetch
                    && !term.prefix
                    && (term.meta.blocks.len() <= SMALL_BLOCKS
                        || (self.sparse_term(term.meta.df)
                            && term.postings.iter().all(Option::is_some)));
            }
        }
        // Sparse partials and final scores each accumulate at most n positive
        // terms. Two n-unit-roundoff allowances fit within n * EPSILON; the
        // additional margin covers bounds and the prefix contribution.
        let roundoff = (terms.len() as f64 + 8.0) * f64::from(f32::EPSILON);
        let margin = if roundoff < 1.0 {
            1.0 / (1.0 - roundoff)
        } else {
            f64::INFINITY
        };
        let mut bounds = vec![0.0f64; windows];
        let mut prefix_windows = vec![false; windows];
        let mut heap: BinaryHeap<Reverse<SearchHit>> = BinaryHeap::new();
        if prefetch {
            let wanted = terms
                .iter()
                .enumerate()
                .filter(|(_, term)| term.small)
                .flat_map(|(t, term)| {
                    term.postings
                        .iter()
                        .enumerate()
                        .filter(|(_, p)| p.is_none())
                        .map(move |(b, _)| (t, b))
                })
                .collect();
            self.read_search_blocks(&mut terms, wanted, &saturation, &mut metrics)
                .await?;
        }
        if terms.iter().all(|term| term.small) {
            // Every posting is already available. Accumulate once in reference
            // order instead of building window bounds and scoring those rows again.
            let mut scores = vec![0.0f32; self.docs as usize];
            let mut touched = Vec::new();
            for term in &terms {
                for postings in term.postings.iter().flatten() {
                    for &(row, tf) in postings.iter() {
                        if allowed.is_some_and(|rows| !rows.contains(row)) {
                            continue;
                        }
                        let score =
                            term.weight
                                .score_cached(tf, lengths[row as usize], &saturation);
                        if score <= 0.0 {
                            continue;
                        }
                        if scores[row as usize] == 0.0 {
                            touched.push(row);
                        }
                        scores[row as usize] += score;
                    }
                }
            }
            metrics.rows_scored = touched.len();
            let floor = search_floor(&heap, k, threshold);
            for row in touched {
                if floor.is_some_and(|floor| f64::from(scores[row as usize]) < floor) {
                    continue;
                }
                let hit = SearchHit {
                    row,
                    score: scores[row as usize],
                };
                if heap.len() < k {
                    heap.push(Reverse(hit));
                } else if hit > heap.peek().unwrap().0 {
                    *heap.peek_mut().unwrap() = Reverse(hit);
                }
            }
            if let Some(threshold) = threshold {
                threshold.publish(&heap);
            }
            let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
            hits.sort_unstable_by(|a, b| b.cmp(a));
            return Ok((hits, metrics));
        }
        let mut row_bounds = if terms.iter().any(|term| term.small) {
            vec![0.0f32; self.docs as usize]
        } else {
            Vec::new()
        };
        for term in &mut terms {
            term.cache_windows(cached_windows);
        }
        {
            let mut maxima = saturation.maxima.lock().unwrap();
            for term in &mut terms {
                let mut best = vec![0.0f64; windows];
                if term.small {
                    for postings in term.postings.iter().flatten() {
                        for &(row, tf) in postings.iter() {
                            let score =
                                term.weight
                                    .score_cached(tf, lengths[row as usize], &saturation);
                            row_bounds[row as usize] += score;
                            best[row as usize / BLOCK] =
                                best[row as usize / BLOCK].max(f64::from(score));
                        }
                    }
                } else {
                    let key = term.meta.blocks.first().map(|b| b.offset);
                    let mut apply = |normalized: &[f64]| {
                        for (w, &maximum) in normalized.iter().enumerate() {
                            if maximum >= 0.0 {
                                best[w] = f64::from(
                                    term.weight.score_saturation(maximum)
                                        * (1.0 + 8.0 * f32::EPSILON),
                                );
                                prefix_windows[w] |= term.prefix;
                            }
                        }
                    };
                    if let Some(cached) = key.and_then(|key| maxima.term(key)) {
                        // Reuse query-independent window maxima while the set of
                        // decoded frame bounds is unchanged. New frames invalidate
                        // this summary, allowing their tighter bounds to take effect.
                        apply(cached);
                    } else {
                        let mut normalized = vec![-1.0f64; windows];
                        for b in &term.meta.blocks {
                            if let Some(windows) = maxima.frames.get(&b.offset) {
                                for &(w, maximum) in windows {
                                    normalized[w] = normalized[w].max(maximum);
                                }
                            } else {
                                let maximum = saturation.factor(b.max_tf, b.min_length).max(0.0);
                                for value in &mut normalized
                                    [b.first as usize / BLOCK..=b.last as usize / BLOCK]
                                {
                                    *value = value.max(maximum);
                                }
                            }
                        }
                        apply(&normalized);
                        if let Some(key) = key {
                            maxima.remember_term(key, normalized);
                        }
                    }
                    for (bound, &value) in bounds.iter_mut().zip(&best) {
                        *bound += value;
                    }
                }
                if cached_windows > 0 {
                    term.bounds = best;
                }
            }
        }
        let dense_bounds = bounds.clone();
        // Known sparse terms must co-occur at a row, rather than at unrelated
        // maxima in the same window. This tightens the bound before any dense read.
        for (w, scores) in row_bounds.chunks(BLOCK).enumerate() {
            bounds[w] += f64::from(scores.iter().copied().fold(0.0, f32::max));
        }
        let mut order: Vec<_> = bounds
            .into_iter()
            .enumerate()
            .map(|(w, b)| (w, (b + f64::from(prefix_windows[w])) * margin))
            .filter(|(_, b)| *b > 0.0)
            .collect();
        order.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        let mut next = 0;
        let mut batch_size = if prefetch { FIRST_WINDOWS } else { usize::MAX };
        let mut by_bound = Vec::with_capacity(terms.len());
        let mut essential = vec![true; terms.len()];
        while next < order.len() {
            let mut batch = Vec::new();
            while next < order.len() && batch.len() < batch_size {
                let (window, bound) = order[next];
                if search_floor(&heap, k, threshold).is_some_and(|floor| bound < floor) {
                    // Bounds descend, and the heap threshold only rises.
                    metrics.windows_pruned += order.len() - next;
                    next = order.len();
                    break;
                }
                next += 1;
                let start = (window * BLOCK) as u32;
                let end = ((window + 1) * BLOCK).min(self.num_docs() as usize) as u32;
                if allowed.is_some_and(|rows| rows.range(start..end).next().is_none()) {
                    continue;
                }
                batch.push((window, bound));
            }
            if batch.is_empty() {
                break;
            }
            if std::env::var_os("BF_TRACE_STAGES").is_some() {
                eprintln!(
                    "FTS batch windows={} of {} first_bound={:.2} last_bound={:.2} heap_min={:?}",
                    batch.len(),
                    order.len(),
                    batch[0].1,
                    batch.last().unwrap().1,
                    heap.peek().map(|h| h.0.score)
                );
            }
            batch_size = usize::MAX;
            if prefetch {
                let mut wanted = Vec::new();
                // A frame can intersect several windows. Sweep their union in
                // row order, requesting each frame once, without sorting a much
                // larger vector of duplicate term/frame pairs. Scoring retains
                // the original descending-bound window order.
                let mut windows: Vec<_> = batch.iter().map(|&(w, _)| w).collect();
                windows.sort_unstable();
                for (t, term) in terms.iter().enumerate() {
                    let mut after = 0;
                    for &window in &windows {
                        let span = term.span(window);
                        for b in span.start.max(after)..span.end {
                            let meta = &term.meta.blocks[b];
                            if term.postings[b].is_none()
                                && allowed.is_none_or(|rows| {
                                    rows.range(meta.first..=meta.last).next().is_some()
                                })
                            {
                                wanted.push((t, b));
                            }
                        }
                        after = after.max(span.end);
                    }
                }
                self.read_search_blocks(&mut terms, wanted, &saturation, &mut metrics)
                    .await?;
            }
            for (window, bound) in batch {
                if search_floor(&heap, k, threshold).is_some_and(|floor| bound < floor) {
                    metrics.windows_pruned += 1;
                    continue;
                }
                let start = (window * BLOCK) as u32;
                let end = ((window + 1) * BLOCK).min(self.num_docs() as usize) as u32;
                if !prefetch {
                    let mut wanted = Vec::new();
                    for (t, term) in terms.iter().enumerate() {
                        for b in term.span(window) {
                            let meta = &term.meta.blocks[b];
                            if term.postings[b].is_none()
                                && allowed.is_none_or(|rows| {
                                    rows.range(meta.first..=meta.last).next().is_some()
                                })
                            {
                                wanted.push((t, b));
                            }
                        }
                    }
                    self.read_search_blocks(&mut terms, wanted, &saturation, &mut metrics)
                        .await?;
                }
                let floor = search_floor(&heap, k, threshold);
                let mut candidates = [true; BLOCK];
                if let Some(floor) = floor {
                    // Block-max MaxScore: low-impact terms cannot create a hit on
                    // their own. Generate candidates only from the essential terms.
                    // Strict inequality retains every score tie. Prefix expansions
                    // each get a conservative flat contribution, counted only once
                    // when computing the final score.
                    by_bound.clear();
                    by_bound.extend(
                        terms
                            .iter()
                            .enumerate()
                            .map(|(t, term)| (t, term.bound(window, &saturation))),
                    );
                    by_bound.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
                    essential.fill(true);
                    let mut nonessential = 0.0;
                    let mut skipped = 0;
                    for &(t, bound) in &by_bound {
                        if (nonessential + bound) * margin >= floor {
                            break;
                        }
                        nonessential += bound;
                        essential[t] = false;
                        skipped += 1;
                    }
                    if skipped > 0 {
                        candidates.fill(false);
                        for (_, term) in terms.iter().enumerate().filter(|(t, _)| essential[*t]) {
                            for b in term.span(window) {
                                let Some(postings) = &term.postings[b] else {
                                    continue;
                                };
                                let lo = if term.meta.blocks[b].first < start {
                                    postings.partition_point(|&(row, _)| row < start)
                                } else {
                                    0
                                };
                                for &(row, _) in
                                    postings[lo..].iter().take_while(|&&(row, _)| row < end)
                                {
                                    candidates[(row - start) as usize] = true;
                                }
                            }
                        }
                    }
                }
                // Test the sparse bound and allowed mask once per candidate, not
                // again for every term that hits it.
                let competitive: Vec<_> = (start..end)
                    .filter(|&row| {
                        candidates[(row - start) as usize]
                            && allowed.is_none_or(|rows| rows.contains(row))
                            && floor.is_none_or(|floor| {
                                row_bounds.is_empty()
                                    || (f64::from(row_bounds[row as usize])
                                        + dense_bounds[window]
                                        + f64::from(prefix_windows[window]))
                                        * margin
                                        >= floor
                            })
                    })
                    .collect();
                if competitive.is_empty() {
                    continue;
                }
                candidates.fill(false);
                for &row in &competitive {
                    candidates[(row - start) as usize] = true;
                }
                let mut scores = [0.0f32; BLOCK];
                let mut prefix_matches = [false; BLOCK];
                for (t, term) in terms.iter().enumerate() {
                    if let Some((tfs, idf, qsat)) = aligned[t] {
                        if !scalar && competitive.len() >= 16 {
                            let mut values = [0.0f32; BLOCK];
                            vector_scores(
                                &tfs[start as usize..end as usize],
                                &index.norms[start as usize..end as usize],
                                idf,
                                qsat,
                                &mut values[..(end - start) as usize],
                            );
                            for &row in &competitive {
                                scores[(row - start) as usize] += values[(row - start) as usize];
                            }
                        } else {
                            for &row in &competitive {
                                scores[(row - start) as usize] += term.weight.score_cached(
                                    tfs[row as usize],
                                    lengths[row as usize],
                                    &saturation,
                                );
                            }
                        }
                        continue;
                    }
                    for b in term.span(window) {
                        let Some(postings) = &term.postings[b] else {
                            continue;
                        };
                        // The directory already proves either endpoint when the
                        // frame begins or ends inside this window. Search only
                        // the remaining boundary, within the retained suffix.
                        let block = &term.meta.blocks[b];
                        let lo = if block.first < start {
                            postings.partition_point(|&(row, _)| row < start)
                        } else {
                            0
                        };
                        let hi = if block.last >= end {
                            lo + postings[lo..].partition_point(|&(row, _)| row < end)
                        } else {
                            postings.len()
                        };
                        let postings = &postings[lo..hi];
                        let mut add = |row: u32, tf| {
                            let i = (row - start) as usize;
                            scores[i] +=
                                term.weight
                                    .score_cached(tf, lengths[row as usize], &saturation);
                            prefix_matches[i] |= term.prefix;
                        };
                        if competitive.len() * 8 < postings.len() {
                            // A common-word frame can have 128 entries but only one
                            // rare-term candidate. Seek those rows instead of scanning
                            // the entire frame and testing the same mask repeatedly.
                            for &row in &competitive {
                                if let Ok(i) = postings.binary_search_by_key(&row, |&(r, _)| r) {
                                    add(row, postings[i].1);
                                }
                            }
                        } else {
                            for &(row, tf) in postings {
                                if candidates[(row - start) as usize] {
                                    add(row, tf);
                                }
                            }
                        }
                    }
                }
                for row in competitive {
                    let i = (row - start) as usize;
                    let score = scores[i] + f32::from(prefix_matches[i]);
                    if score <= 0.0 {
                        continue;
                    }
                    metrics.rows_scored += 1;
                    let hit = SearchHit { row, score };
                    if heap.len() < k {
                        heap.push(Reverse(hit));
                    } else if hit > heap.peek().unwrap().0 {
                        *heap.peek_mut().unwrap() = Reverse(hit);
                    }
                }
                if let Some(threshold) = threshold {
                    threshold.publish(&heap);
                }
            }
        }
        let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
        hits.sort_unstable_by(|a, b| b.cmp(a));
        Ok((hits, metrics))
    }
}

#[cfg(test)]
#[test]
fn integrated_dense_loop_matches_exhaustive_with_long_lists() {
    let p = FtsParams::default();
    let docs: Vec<_> = (0..6000)
        .map(|row| {
            let mut text = vec!["common"; row % 19 + 1];
            if row % 2 == 0 {
                text.push("other");
            }
            if row % 37 == 0 {
                text.push("rare");
            }
            crate::tokenize(&text.join(" "), &p)
        })
        .collect();
    let queries: Vec<_> = [
        "common",
        "common common rare",
        "common other",
        "rare other",
        "absent common",
    ]
    .iter()
    .map(|q| Query::new(crate::tokenize(q, &p), false))
    .collect();
    let mut builder = SegmentBuilder::new(false);
    for doc in &docs {
        builder.add_document(doc).unwrap();
    }
    let reader = futures::executor::block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
        builder.finish().unwrap(),
    ))))
    .unwrap();
    let wanted = queries
        .iter()
        .flat_map(|q| q.tokens.iter().map(|t| t.text.clone()))
        .collect();
    let (mut index, _, _) = prepare(&reader, &wanted, false);
    for average in [0.1, 10.0, 1000.0] {
        let mut stats = CorpusStats::from_documents(&docs);
        stats.average_doc_length = average;
        index.norms = normalizers(&index.lengths, &stats);
        for query in &queries {
            let mut truth: Vec<_> = docs
                .iter()
                .enumerate()
                .filter_map(|(row, doc)| {
                    let score = crate::scoring::score_document(doc, query, &stats, &p);
                    (score > 0.0).then_some(SearchHit {
                        row: row as u32,
                        score,
                    })
                })
                .collect();
            truth.sort_unstable_by(|a, b| b.cmp(a));
            truth.truncate(10);
            for scalar in [false, true] {
                let (found, _) = futures::executor::block_on(reader.dense_integrated(
                    query,
                    &stats,
                    &p,
                    SearchMode::Prefetched,
                    &index,
                    scalar,
                ))
                .unwrap();
                assert_eq!(found, truth);
            }
        }
    }
}
