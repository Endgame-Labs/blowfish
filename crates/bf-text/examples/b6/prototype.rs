use super::*;
use futures::executor::block_on;
use std::io::{BufRead, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Instant;

fn perf(command: &str) {
    if let Some(path) = std::env::var_os("BF_TEXT_PERF_CONTROL") {
        writeln!(
            std::fs::OpenOptions::new().write(true).open(path).unwrap(),
            "{command}"
        )
        .unwrap();
        let mut line = String::new();
        std::io::BufReader::new(
            std::fs::File::open(std::env::var_os("BF_TEXT_PERF_ACK").unwrap()).unwrap(),
        )
        .read_line(&mut line)
        .unwrap();
    }
}
fn varint(out: &mut Vec<u8>, mut n: u32) {
    while n >= 128 {
        out.push(n as u8 | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn take(bytes: &[u8], at: &mut usize) -> u32 {
    let mut n = 0;
    for shift in (0..35).step_by(7) {
        let b = bytes[*at];
        *at += 1;
        n |= u32::from(b & 127) << shift;
        if b < 128 {
            return n;
        }
    }
    panic!("invalid prototype varint")
}
fn vocabulary(reader: &SegmentReader) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for entry in match &reader.dictionary {
        Dictionary::Chunked { entries, .. } => entries,
        _ => panic!("chunked fixtures"),
    } {
        let bytes = block_on(
            reader
                .source
                .read(entry.offset..entry.offset + u64::from(entry.len)),
        )
        .unwrap();
        let map = SegmentReader::decode_bundle(&bytes).unwrap();
        let mut stream = map.stream();
        while let Some((term, handle)) = stream.next() {
            out.insert(String::from_utf8(term.to_vec()).unwrap(), handle);
        }
    }
    out
}
struct Forward {
    offsets: Vec<u32>,
    bytes: Vec<u8>,
}
impl Forward {
    fn build(docs: &[Vec<(u32, u32)>]) -> Self {
        let mut offsets = Vec::with_capacity(docs.len() + 1);
        let mut bytes = Vec::new();
        for doc in docs {
            offsets.push(u32::try_from(bytes.len()).unwrap());
            let mut previous = 0;
            for &(id, tf) in doc {
                // Fold the common tf=1 into the low bit of the term delta.
                varint(&mut bytes, (id - previous) * 2 + u32::from(tf > 1));
                if tf > 1 {
                    varint(&mut bytes, tf - 2);
                }
                previous = id;
            }
        }
        offsets.push(u32::try_from(bytes.len()).unwrap());
        Self { offsets, bytes }
    }
    fn score(&self, row: u32, query: &[(u32, TermWeight)], length: u32, sat: &Saturation) -> f32 {
        let bytes = &self.bytes
            [self.offsets[row as usize] as usize..self.offsets[row as usize + 1] as usize];
        let mut at = 0;
        let mut id = 0;
        let mut q = 0;
        let mut score = 0.0f32;
        while at < bytes.len() && q < query.len() {
            let code = take(bytes, &mut at);
            id += code / 2;
            let tf = if code & 1 == 0 {
                1
            } else {
                take(bytes, &mut at) + 2
            };
            while q < query.len() && query[q].0 < id {
                q += 1;
            }
            if q < query.len() && query[q].0 == id {
                score += query[q].1.score_cached(tf, length, sat);
                q += 1;
            }
        }
        score
    }
    fn size(&self) -> usize {
        self.offsets.len() * 4 + self.bytes.len()
    }
}
struct ImpactBlock {
    rows: Vec<u8>,
    bits: u8,
    count: usize,
    maximum: f64,
}
struct Impact {
    blocks: Vec<ImpactBlock>,
}
fn impact(mut list: Vec<(u32, u32)>, lengths: &[u32], sat: &Saturation) -> Impact {
    list.sort_unstable_by(|&(ar, at), &(br, bt)| {
        sat.factor(bt, lengths[br as usize])
            .total_cmp(&sat.factor(at, lengths[ar as usize]))
            .then(ar.cmp(&br))
    });
    let packer = BitPacker4x::new();
    let mut blocks = Vec::new();
    for rows in list.chunks(BLOCK) {
        let mut values = [0u32; BLOCK];
        for (i, &(row, _)) in rows.iter().enumerate() {
            values[i] = row;
        }
        let bits = packer.num_bits(&values);
        let mut packed = vec![0; 16 * bits as usize];
        packer.compress(&values, &mut packed, bits);
        blocks.push(ImpactBlock {
            rows: packed,
            bits,
            count: rows.len(),
            maximum: sat.factor(rows[0].1, lengths[rows[0].0 as usize]),
        });
    }
    Impact { blocks }
}
struct Ta {
    terms: BTreeMap<String, u32>,
    forward: Forward,
    lists: BTreeMap<u32, Impact>,
    saturation: Saturation,
    seen: Vec<u32>,
    generation: u32,
    blocks: usize,
    completed: usize,
    lex_bytes: usize,
    rank_bytes: usize,
}
impl Ta {
    fn build(
        reader: &SegmentReader,
        stats: &CorpusStats,
        params: &FtsParams,
        wanted: &std::collections::BTreeSet<String>,
    ) -> Self {
        let saturation = Saturation::new(stats, params);
        let mut docs = vec![Vec::new(); reader.num_docs() as usize];
        let mut terms = BTreeMap::new();
        let mut lists = BTreeMap::new();
        for (term, handle) in vocabulary(reader) {
            let id = terms.len() as u32;
            terms.insert(term.clone(), id);
            let meta = block_on(reader.load([handle]))
                .unwrap()
                .remove(&handle)
                .unwrap();
            let mut list = Vec::with_capacity(meta.df as usize);
            for b in &meta.blocks {
                for &(row, tf) in block_on(reader.postings(b, meta.df)).unwrap().iter() {
                    docs[row as usize].push((id, tf));
                    if wanted.contains(&term) {
                        list.push((row, tf));
                    }
                }
            }
            if wanted.contains(&term) {
                lists.insert(id, impact(list, reader.lens(), &saturation));
            }
        }
        let forward = Forward::build(&docs);
        let lex_bytes = forward.size();
        // Also measure a more favorable frequency-ranked term-ID assignment.
        let mut by_frequency: Vec<_> = terms
            .iter()
            .map(|(t, &id)| (stats.document_frequencies[t], id))
            .collect();
        by_frequency.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        let mut permutation = vec![0; terms.len()];
        for (rank, &(_, id)) in by_frequency.iter().enumerate() {
            permutation[id as usize] = rank as u32;
        }
        for doc in &mut docs {
            for (id, _) in doc.iter_mut() {
                *id = permutation[*id as usize];
            }
            doc.sort_unstable();
        }
        let rank_bytes = Forward::build(&docs).size() + permutation.len() * 4;
        Self {
            terms,
            forward,
            lists,
            saturation,
            seen: vec![0; docs.len()],
            generation: 0,
            blocks: 0,
            completed: 0,
            lex_bytes,
            rank_bytes,
        }
    }
    fn search(
        &mut self,
        reader: &SegmentReader,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        base: u32,
        heap: &mut BinaryHeap<Reverse<SearchHit>>,
    ) {
        let query: Vec<_> = query
            .exact_terms()
            .into_iter()
            .filter_map(|(term, qf)| {
                self.terms.get(term).map(|&id| {
                    (
                        id,
                        TermWeight::new(qf, stats.document_frequencies[term], stats, params),
                    )
                })
            })
            .collect();
        let lists: Vec<_> = query.iter().map(|(id, _)| &self.lists[id]).collect();
        let mut cursors = vec![0; lists.len()];
        self.generation += 1;
        let mut rows = [0u32; BLOCK];
        loop {
            // Monotone f32 addition in canonical order is a conservative bound,
            // including rounding and row-ID ties; stop only at strict inequality.
            let mut bound = 0.0f32;
            let mut selected = None;
            let mut largest = -1.0f32;
            for (t, list) in lists.iter().enumerate() {
                if let Some(block) = list.blocks.get(cursors[t]) {
                    let s = query[t].1.score_saturation(block.maximum);
                    bound += s;
                    if s > largest {
                        largest = s;
                        selected = Some(t);
                    }
                }
            }
            if heap.len() == 10 && bound < heap.peek().unwrap().0.score {
                break;
            }
            let Some(t) = selected else {
                break;
            };
            let block = &lists[t].blocks[cursors[t]];
            cursors[t] += 1;
            self.blocks += 1;
            BitPacker4x::new().decompress(&block.rows, &mut rows, block.bits);
            for &row in &rows[..block.count] {
                if self.seen[row as usize] == self.generation {
                    continue;
                }
                self.seen[row as usize] = self.generation;
                self.completed += 1;
                let score =
                    self.forward
                        .score(row, &query, reader.lens()[row as usize], &self.saturation);
                let hit = SearchHit {
                    row: base + row,
                    score,
                };
                if score <= 0.0 {
                    continue;
                }
                if heap.len() < 10 {
                    heap.push(Reverse(hit));
                } else if hit > heap.peek().unwrap().0 {
                    *heap.peek_mut().unwrap() = Reverse(hit);
                }
            }
        }
    }
}

pub(super) struct Bands {
    pub(super) terms: BTreeMap<u64, Vec<Vec<u64>>>,
    pub(super) ceilings: Vec<f64>,
    pub(super) bitwise: bool,
    pub(super) skipped_rows: AtomicUsize,
    pub(super) skipped_windows: AtomicUsize,
}
impl Bands {
    fn build(
        reader: &SegmentReader,
        stats: &CorpusStats,
        params: &FtsParams,
        count: usize,
    ) -> Self {
        let mut frequent: Vec<_> = stats.document_frequencies.iter().collect();
        frequent.sort_unstable_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        let saturation = Saturation::new(stats, params);
        let ceilings: Vec<_> = (1..=count)
            .map(|b| (params.k1 as f64 + 1.0) * b as f64 / count as f64)
            .collect();
        let mut terms = BTreeMap::new();
        for (word, _) in frequent.into_iter().take(32) {
            let Some(handle) = block_on(reader.term(word)).unwrap() else {
                continue;
            };
            let meta = block_on(reader.load([handle]))
                .unwrap()
                .remove(&handle)
                .unwrap();
            let mut bands =
                vec![vec![0u64; (reader.num_docs() as usize).div_ceil(BLOCK) * 2]; count];
            for block in &meta.blocks {
                for &(row, tf) in block_on(reader.postings(block, meta.df)).unwrap().iter() {
                    let value = saturation.factor(tf, reader.lens()[row as usize]);
                    let band = ceilings
                        .partition_point(|&ceil| ceil < value)
                        .min(count - 1);
                    assert!(ceilings[band] >= value);
                    bands[band][row as usize / 64] |= 1 << (row % 64);
                }
            }
            terms.insert(handle, bands);
        }
        Self {
            terms,
            ceilings,
            bitwise: false,
            skipped_rows: AtomicUsize::new(0),
            skipped_windows: AtomicUsize::new(0),
        }
    }
}

// Per-query user-space CPU cycles supplement the outer B5 perf FIFO counters.
#[cfg(target_os = "linux")]
struct Cycles(i32);
#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn syscall(number: std::ffi::c_long, ...) -> std::ffi::c_long;
    fn ioctl(fd: i32, request: std::ffi::c_ulong, ...) -> i32;
    fn read(fd: i32, buf: *mut std::ffi::c_void, count: usize) -> isize;
    fn close(fd: i32) -> i32;
}
#[cfg(target_os = "linux")]
impl Cycles {
    fn new() -> Option<Self> {
        std::env::var_os("BF_TEXT_CYCLES")?;
        let mut attr = [0u64; 16];
        attr[0] = 128u64 << 32; // PERF_TYPE_HARDWARE, sizeof(perf_event_attr)
        attr[5] = 1 | (1 << 5) | (1 << 6); // disabled, exclude kernel, and exclude hypervisor
        #[cfg(target_arch = "aarch64")]
        let number = 241;
        #[cfg(target_arch = "x86_64")]
        let number = 298;
        let fd = unsafe { syscall(number, attr.as_ptr(), 0i32, -1i32, -1i32, 0u64) } as i32;
        assert!(
            fd >= 0,
            "perf_event_open: {}",
            std::io::Error::last_os_error()
        );
        assert_eq!(unsafe { ioctl(fd, 0x2400, 0) }, 0);
        Some(Self(fd))
    }
    fn value(&self) -> u64 {
        let mut n = 0u64;
        assert_eq!(unsafe { read(self.0, (&mut n as *mut u64).cast(), 8) }, 8);
        n
    }
}
#[cfg(target_os = "linux")]
impl Drop for Cycles {
    fn drop(&mut self) {
        unsafe { close(self.0) };
    }
}

fn baseline(
    readers: &[SegmentReader],
    query: &Query,
    stats: &CorpusStats,
    params: &FtsParams,
    bands: Option<&[Bands]>,
) -> (Vec<SearchHit>, usize, usize) {
    let threshold = SearchThreshold::new(10);
    let mut hits = Vec::new();
    let mut blocks = 0;
    let mut rows = 0;
    for (i, reader) in readers.iter().enumerate() {
        let (found, metrics) = if let Some(bands) = bands {
            block_on(reader.search_bands(
                query,
                stats,
                params,
                10,
                None,
                SearchMode::Shared(&threshold),
                &bands[i],
            ))
            .unwrap()
        } else {
            block_on(reader.search_internal(
                query,
                stats,
                params,
                10,
                None,
                SearchMode::Shared(&threshold),
            ))
            .unwrap()
        };
        blocks += metrics.blocks_read;
        rows += metrics.rows_scored;
        hits.extend(found.into_iter().map(|mut h| {
            h.row += i as u32 * 200_000;
            h
        }));
    }
    hits.sort_unstable_by(|a, b| b.cmp(a));
    hits.truncate(10);
    (hits, blocks, rows)
}
pub fn run() {
    let args: Vec<_> = std::env::args().collect();
    let root = Path::new(&args[1]);
    let mode = &args[2];
    let readers: Vec<_> = (0..5)
        .map(|i| {
            block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
                std::fs::read(root.join(format!("segment-{i}.bft"))).unwrap(),
            ))))
            .unwrap()
        })
        .collect();
    let stats: CorpusStats =
        serde_json::from_slice(&std::fs::read(root.join("stats.json")).unwrap()).unwrap();
    let workload: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(root.join("workload.json")).unwrap()).unwrap();
    let truth: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(root.join("truth.json")).unwrap()).unwrap();
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
    let expected: Vec<_> = queries
        .iter()
        .enumerate()
        .map(|(q, query)| {
            let hits = baseline(&readers, query, &stats, &params, None).0;
            assert_eq!(hits.len(), truth[q]["ids"].as_array().unwrap().len());
            for (h, id) in hits.iter().zip(truth[q]["ids"].as_array().unwrap()) {
                assert_eq!(h.row as u64, id.as_u64().unwrap(), "truth row {q}");
            }
            for (h, s) in hits.iter().zip(truth[q]["scores"].as_array().unwrap()) {
                assert_eq!(
                    h.score.to_bits(),
                    (s.as_f64().unwrap() as f32).to_bits(),
                    "truth score {q}"
                );
            }
            hits
        })
        .collect();
    let wanted = queries
        .iter()
        .flat_map(|q| q.exact_terms().into_keys().map(str::to_owned))
        .collect();
    let mut ta: Vec<_> = if mode == "ta" || mode == "footprint" {
        readers
            .iter()
            .map(|r| Ta::build(r, &stats, &params, &wanted))
            .collect()
    } else {
        Vec::new()
    };
    if !ta.is_empty() {
        eprintln!(
            "{}",
            serde_json::json!({"lex_forward_bytes":ta.iter().map(|t|t.lex_bytes).sum::<usize>(),"rank_forward_bytes":ta.iter().map(|t|t.rank_bytes).sum::<usize>(),"impact_query_bytes":ta.iter().flat_map(|t|t.lists.values()).flat_map(|l|&l.blocks).map(|b|b.rows.len()+16).sum::<usize>()})
        );
    }
    if mode == "footprint" {
        return;
    }
    let bands: Vec<_> = if mode.starts_with("bands") || mode.starts_with("bitbands") {
        let count = if mode.ends_with("4") { 4 } else { 8 };
        readers
            .iter()
            .map(|r| {
                let mut b = Bands::build(r, &stats, &params, count);
                b.bitwise = mode.starts_with("bitbands");
                b
            })
            .collect()
    } else {
        Vec::new()
    };
    let cycles = Cycles::new();
    let mut times = Vec::new();
    let mut cycle_counts = Vec::new();
    let mut blocks = 0;
    let mut rows = 0;
    for pass in 0..2 {
        if pass == 1 {
            for t in &mut ta {
                t.blocks = 0;
                t.completed = 0;
            }
            for b in &bands {
                b.skipped_rows.store(0, AtomicOrdering::Relaxed);
                b.skipped_windows.store(0, AtomicOrdering::Relaxed);
            }
            perf("enable");
        }
        for (q, query) in queries.iter().enumerate() {
            let before = cycles.as_ref().map(Cycles::value).unwrap_or(0);
            let start = Instant::now();
            let hits = if mode == "ta" {
                let mut heap = BinaryHeap::new();
                for (i, (ta, reader)) in ta.iter_mut().zip(&readers).enumerate() {
                    ta.search(
                        reader,
                        query,
                        &stats,
                        &params,
                        i as u32 * 200_000,
                        &mut heap,
                    );
                }
                let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
                hits.sort_unstable_by(|a, b| b.cmp(a));
                hits
            } else {
                let (hits, b, r) = baseline(
                    &readers,
                    query,
                    &stats,
                    &params,
                    (!bands.is_empty()).then_some(bands.as_slice()),
                );
                if pass == 1 {
                    blocks += b;
                    rows += r;
                }
                hits
            };
            if pass == 1 {
                times.push(start.elapsed().as_secs_f64() * 1000.0);
                cycle_counts.push(cycles.as_ref().map(Cycles::value).unwrap_or(0) - before);
            }
            assert_eq!(hits, expected[q], "{mode}, query {q}");
            for (h, e) in hits.iter().zip(&expected[q]) {
                assert_eq!(h.score.to_bits(), e.score.to_bits());
            }
        }
        if pass == 1 {
            perf("disable");
        }
    }
    times.sort_unstable_by(f64::total_cmp);
    cycle_counts.sort_unstable();
    eprintln!(
        "{}",
        serde_json::json!({"mode":mode,"queries":queries.len(),"p50_ms":times[times.len()/2],"p90_ms":times[times.len()*9/10],"p90_cycles":cycle_counts[cycle_counts.len()*9/10],"cycles_sum":cycle_counts.iter().sum::<u64>(),"blocks_read":blocks,"rows_scored":rows,"impact_blocks":ta.iter().map(|t|t.blocks).sum::<usize>(),"completed_rows":ta.iter().map(|t|t.completed).sum::<usize>(),"band_skipped_rows":bands.iter().map(|b|b.skipped_rows.load(AtomicOrdering::Relaxed)).sum::<usize>(),"band_skipped_windows":bands.iter().map(|b|b.skipped_windows.load(AtomicOrdering::Relaxed)).sum::<usize>()})
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forward_round_trip_and_reference_bits() {
        let docs = vec![vec![(0, 1), (3, 2), (100000, 300)], vec![], vec![(2, 17)]];
        let f = Forward::build(&docs);
        let stats = CorpusStats {
            total_docs: 3,
            average_doc_length: 120.0,
            ..Default::default()
        };
        let params = FtsParams::default();
        let sat = Saturation::new(&stats, &params);
        let query: Vec<_> = [0, 2, 3, 999, 100000]
            .into_iter()
            .map(|id| (id, TermWeight::new(2, 1, &stats, &params)))
            .collect();
        for (row, doc) in docs.iter().enumerate() {
            let mut expected = 0.0f32;
            for (id, w) in &query {
                expected += w.score(doc.iter().find(|t| t.0 == *id).map_or(0, |t| t.1), 400);
            }
            assert_eq!(
                f.score(row as u32, &query, 400, &sat).to_bits(),
                expected.to_bits()
            );
        }
    }
    #[test]
    fn ta_and_bands_match_exhaustive_with_ties_repetitions_and_changed_average() {
        for seed in 1..=12u64 {
            let mut rng = seed;
            let mut builder = SegmentBuilder::new(true);
            let mut documents = Vec::new();
            let params = FtsParams::default();
            for row in 0..if seed == 1 { 12_000 } else { 640 } {
                let mut text = String::new();
                for word in 0..16 {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    if rng >> 61 == 0 || word < 2 || row % 91 == 0 {
                        for _ in 0..(rng >> 58) % 4 + 1 {
                            text.push_str(&format!(" w{word}"));
                        }
                    }
                }
                let tokens = crate::tokenize(&text, &params);
                builder.add_document(&tokens).unwrap();
                documents.push(tokens);
            }
            let reader = block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
                builder.finish().unwrap(),
            ))))
            .unwrap();
            let mut stats = CorpusStats::from_documents(&documents);
            stats.average_doc_length *= if seed % 2 == 0 { 0.7 } else { 1.4 };
            let texts = ["w0 w1 w12", "w2 w2 w7", "absent w14", "w0", "w9 w3 w12 w1"];
            let queries: Vec<_> = texts
                .iter()
                .map(|s| Query::new(crate::tokenize(s, &params), false))
                .collect();
            let wanted = queries
                .iter()
                .flat_map(|q| q.exact_terms().into_keys().map(str::to_owned))
                .collect();
            let mut ta = Ta::build(&reader, &stats, &params, &wanted);
            for (count, bitwise) in [(4, false), (8, false), (4, true), (8, true)] {
                let mut bands = Bands::build(&reader, &stats, &params, count);
                bands.bitwise = bitwise;
                for query in &queries {
                    let mut expected: Vec<_> = documents
                        .iter()
                        .enumerate()
                        .filter_map(|(row, d)| {
                            let score = crate::score_document(d, query, &stats, &params);
                            (score > 0.0).then_some(SearchHit {
                                row: row as u32,
                                score,
                            })
                        })
                        .collect();
                    expected.sort_unstable_by(|a, b| b.cmp(a));
                    expected.truncate(10);
                    let mut heap = BinaryHeap::new();
                    ta.search(&reader, query, &stats, &params, 0, &mut heap);
                    let mut found: Vec<_> = heap.into_iter().map(|h| h.0).collect();
                    found.sort_unstable_by(|a, b| b.cmp(a));
                    assert_eq!(found, expected, "TA seed {seed}");
                    let threshold = SearchThreshold::new(10);
                    let (found, _) = block_on(reader.search_bands(
                        query,
                        &stats,
                        &params,
                        10,
                        None,
                        SearchMode::Shared(&threshold),
                        &bands,
                    ))
                    .unwrap();
                    assert_eq!(found, expected, "bands seed {seed}");
                }
                if seed == 1 {
                    assert!(bands.skipped_rows.load(AtomicOrdering::Relaxed) > 0);
                }
            }
        }
    }
}

pub(super) type BandChoices = [(f64, [u64; 2]); 9];
#[allow(clippy::too_many_arguments)]
pub(super) fn combine_bands(
    choices: &[BandChoices],
    suffix: &[f64],
    count: usize,
    score: f64,
    mask: [u64; 2],
    target: f64,
    out: &mut [u64; 2],
) {
    if score + suffix[0] < target {
        return;
    }
    if score >= target || choices.is_empty() {
        out[0] |= mask[0];
        out[1] |= mask[1];
        return;
    }
    for &(value, bits) in &choices[0][..=count] {
        let next = [mask[0] & bits[0], mask[1] & bits[1]];
        if next != [0; 2] {
            combine_bands(
                &choices[1..],
                &suffix[1..],
                count,
                score + value,
                next,
                target,
                out,
            );
        }
    }
}
