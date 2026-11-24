//! Opt-in, corpus-backed kill tests. Set BF_TEXT_EXPERIMENT to an owned scratch directory.
use super::*;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

fn root() -> PathBuf {
    PathBuf::from(std::env::var_os("BF_TEXT_EXPERIMENT").expect("BF_TEXT_EXPERIMENT"))
}
fn reader(bytes: Vec<u8>) -> SegmentReader {
    futures::executor::block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(bytes))))
        .unwrap()
}
fn perf(command: &str) {
    if let Some(path) = std::env::var_os("BF_TEXT_PERF_CONTROL") {
        let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        writeln!(f, "{command}").unwrap();
        if let Some(ack) = std::env::var_os("BF_TEXT_PERF_ACK") {
            let mut line = String::new();
            std::io::BufReader::new(std::fs::File::open(ack).unwrap())
                .read_line(&mut line)
                .unwrap();
        }
    }
}

#[test]
#[ignore = "requires the first million MS MARCO passages in BF_TEXT_EXPERIMENT/corpus.jsonl"]
fn prepare_msmarco() {
    let root = root();
    let input = std::io::BufReader::new(std::fs::File::open(root.join("corpus.jsonl")).unwrap());
    let mut builder = SegmentBuilder::new(true);
    let mut rows = 0;
    let mut part = 0;
    let p = FtsParams::default();
    let start = Instant::now();
    let mut build_seconds = 0.0;
    for line in input.lines() {
        let d: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let tokens = crate::tokenize(d["text"].as_str().unwrap(), &p);
        builder.add_document(&tokens).unwrap();
        rows += 1;
        if rows % 200_000 == 0 {
            let t = Instant::now();
            let bytes = builder.finish().unwrap();
            build_seconds += t.elapsed().as_secs_f64();
            std::fs::write(root.join(format!("segment-{part}.bft")), bytes).unwrap();
            builder = SegmentBuilder::new(true);
            part += 1;
            eprintln!(
                "prepared {rows} rows in {:.3}s",
                start.elapsed().as_secs_f64()
            );
        }
    }
    assert_eq!(rows, 1_000_000);
    let readers: Vec<_> = (0..part)
        .map(|i| reader(std::fs::read(root.join(format!("segment-{i}.bft"))).unwrap()))
        .collect();
    let mut stats = CorpusStats::default();
    for reader in readers {
        stats.merge(&futures::executor::block_on(reader.stats()).unwrap());
    }
    std::fs::write(root.join("stats.json"), serde_json::to_vec(&stats).unwrap()).unwrap();
    eprintln!("finish_seconds={build_seconds:.6}");
}

fn varint_bytes(mut value: u32) -> usize {
    let mut n = 1;
    while value >= 128 {
        n += 1;
        value >>= 7;
    }
    n
}

#[test]
#[ignore = "requires prepared corpus segments in BF_TEXT_EXPERIMENT"]
fn inspect_msmarco() {
    let mut total = 0;
    let mut positions = 0;
    let mut compressed = 0;
    let mut postings = 0;
    let mut records = 0;
    let mut lengths_bytes = 0;
    let mut arrays = 0;
    for part in 0..5 {
        let bytes = std::fs::read(root().join(format!("segment-{part}.bft"))).unwrap();
        total += bytes.len();
        let h = text_header(&bytes).unwrap().unwrap();
        let lo = u64::from_le_bytes(h.footer[32..40].try_into().unwrap());
        let hi = u64::from_le_bytes(h.footer[48..56].try_into().unwrap());
        lengths_bytes += (hi - lo) as usize;
        let r = reader(bytes);
        let handles = futures::executor::block_on(r.prefix_terms("")).unwrap();
        // Bound the metadata memo and temporary allocations while inspecting every block.
        for handles in handles.chunks(128) {
            let terms = futures::executor::block_on(r.load(handles.iter().copied())).unwrap();
            for term in terms.values() {
                records += RECORD_HEADER + BLOCK_META * term.blocks.len() + 4;
                for block in &term.blocks {
                    postings += block.size as usize;
                    positions += block.positions_size as usize;
                    let list = futures::executor::block_on(r.postings(block, term.df)).unwrap();
                    let raw =
                        futures::executor::block_on(r.source.read(
                            block.positions..block.positions + u64::from(block.positions_size),
                        ))
                        .unwrap();
                    verify(&raw, block.positions_crc).unwrap();
                    let mut decoder =
                        positions::Decoder::new(&raw, r.compressed_positions).unwrap();
                    for &(_, tf) in list.iter() {
                        let mut previous = 0;
                        for (element, position) in decoder.document(tf).unwrap() {
                            arrays += usize::from(element != 0);
                            assert_eq!(element, 0);
                            compressed += varint_bytes(position - previous);
                            previous = position;
                        }
                    }
                    decoder.finish().unwrap();
                }
            }
        }
    }
    eprintln!(
        "{}",
        serde_json::json!({"total":total,"positions":positions,"varint_positions":compressed,"postings":postings,"records":records,"lengths":lengths_bytes,"other":total-positions-postings-records-lengths_bytes,"nonzero_elements":arrays})
    );
}

#[test]
#[ignore = "requires prepared corpus segments, workload.json, and optional truth.json"]
fn query_msmarco() {
    let root = root();
    let readers: Vec<_> = (0..5)
        .map(|i| reader(std::fs::read(root.join(format!("segment-{i}.bft"))).unwrap()))
        .collect();
    let stats: CorpusStats =
        serde_json::from_slice(&std::fs::read(root.join("stats.json")).unwrap()).unwrap();
    let workload: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(root.join("workload.json")).unwrap()).unwrap();
    let params = FtsParams::default();
    if std::env::var_os("BF_TEXT_PERFECT_BOUNDS").is_some() {
        let wanted: std::collections::BTreeSet<_> = workload
            .iter()
            .flat_map(|w| {
                crate::tokenize(w["request"]["rank_by"][2].as_str().unwrap(), &params)
                    .into_iter()
                    .map(|t| t.text)
            })
            .collect();
        for r in &readers {
            let sat = r.saturation(&stats, &params);
            for word in &wanted {
                let Some(handle) = futures::executor::block_on(r.term(word)).unwrap() else {
                    continue;
                };
                let meta = futures::executor::block_on(r.load([handle]))
                    .unwrap()
                    .remove(&handle)
                    .unwrap();
                for b in &meta.blocks {
                    let p = futures::executor::block_on(r.postings(b, meta.df)).unwrap();
                    let mut maxima = BTreeMap::<usize, f64>::new();
                    for &(row, tf) in p.iter() {
                        let value = sat.factor(tf, r.lens()[row as usize]);
                        let maximum = maxima.entry(row as usize / BLOCK).or_default();
                        *maximum = maximum.max(value);
                    }
                    // An intentionally unlimited, perfect bound is a ceiling on
                    // the possible benefit of compact joint block metadata.
                    sat.maxima
                        .lock()
                        .unwrap()
                        .frames
                        .insert(b.offset, maxima.into_iter().collect());
                }
            }
        }
    }
    let queries: Vec<_> = workload
        .iter()
        .map(|w| {
            Query::new(
                crate::tokenize(w["request"]["rank_by"][2].as_str().unwrap(), &params),
                false,
            )
        })
        .collect();
    let ideal = std::env::var_os("BF_TEXT_IDEAL_FLOOR").is_some();
    let seed_floors: Option<Vec<f32>> = std::env::var_os("BF_TEXT_SEED_FLOOR").map(|_| {
        serde_json::from_slice(&std::fs::read(root.join("seed-floors.json")).unwrap()).unwrap()
    });
    let permutations: Vec<Option<Vec<u32>>> = (0..5)
        .map(|i| {
            std::fs::read(root.join(format!("permutation-{i}.json")))
                .ok()
                .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        })
        .collect();
    let mut truth: Vec<Vec<SearchHit>> = Vec::new();
    let mut latencies = Vec::new();
    let mut blocks = 0;
    let mut scored = 0;
    let mut pruned = 0;
    for pass in 0..2 {
        if pass == 1 {
            perf("enable");
        }
        for (q, query) in queries.iter().enumerate() {
            let threshold = SearchThreshold::new(10);
            if ideal && pass == 1 && truth[q].len() == 10 {
                threshold.score.store(
                    truth[q][9].score.to_bits(),
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            if pass == 1
                && let Some(floors) = &seed_floors
            {
                threshold
                    .score
                    .store(floors[q].to_bits(), std::sync::atomic::Ordering::Relaxed);
            }
            let start = Instant::now();
            let mut hits = Vec::new();
            for (i, reader) in readers.iter().enumerate() {
                let (found, metrics) = futures::executor::block_on(reader.search_internal(
                    query,
                    &stats,
                    &params,
                    10,
                    None,
                    SearchMode::Shared(&threshold),
                ))
                .unwrap();
                hits.extend(found.into_iter().map(|mut h| {
                    h.row = permutations[i]
                        .as_ref()
                        .map_or(h.row, |p| p[h.row as usize])
                        + i as u32 * 200_000;
                    h
                }));
                if pass == 1 {
                    blocks += metrics.blocks_read;
                    scored += metrics.rows_scored;
                    pruned += metrics.windows_pruned;
                }
            }
            hits.sort_unstable_by(|a, b| b.cmp(a));
            hits.truncate(10);
            if pass == 0 {
                truth.push(hits);
            } else {
                latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(hits, truth[q], "query {q}");
            }
        }
        if pass == 1 {
            perf("disable");
        }
    }
    let output: Vec<_> = truth.iter().zip(&workload).map(|(hits,w)| serde_json::json!({"query_id":w["query_id"],"ids":hits.iter().map(|h| h.row).collect::<Vec<_>>(),"scores":hits.iter().map(|h| h.score).collect::<Vec<_>>()})).collect();
    std::fs::write(
        root.join(if ideal { "ideal.json" } else { "baseline.json" }),
        serde_json::to_vec(&output).unwrap(),
    )
    .unwrap();
    latencies.sort_unstable_by(f64::total_cmp);
    eprintln!(
        "{}",
        serde_json::json!({"ideal":ideal,"queries":queries.len(),"p50_ms":latencies[latencies.len()/2],"p90_ms":latencies[latencies.len()*9/10],"blocks_read":blocks,"rows_scored":scored,"windows_pruned":pruned})
    );
}

#[test]
#[ignore = "requires prepared format-5 corpus segments"]
fn upgrade_msmarco() {
    let root = root();
    let output = root.join("upgraded");
    std::fs::create_dir_all(&output).unwrap();
    let start = Instant::now();
    let mut before = 0;
    let mut after = 0;
    for i in 0..5 {
        let old = std::fs::read(root.join(format!("segment-{i}.bft"))).unwrap();
        before += old.len();
        let new = upgrade_segment(old).unwrap().unwrap();
        after += new.len();
        std::fs::write(output.join(format!("segment-{i}.bft")), new).unwrap();
    }
    for file in ["stats.json", "workload.json", "corpus.jsonl"] {
        std::fs::hard_link(root.join(file), output.join(file)).unwrap();
    }
    eprintln!(
        "{}",
        serde_json::json!({"before":before,"after":after,"upgrade_seconds":start.elapsed().as_secs_f64()})
    );
}

#[test]
#[ignore = "requires prepared corpus segments; builds a 64-row per-term seed ceiling"]
fn seeds_msmarco() {
    let root = root();
    let readers: Vec<_> = (0..5)
        .map(|i| reader(std::fs::read(root.join(format!("segment-{i}.bft"))).unwrap()))
        .collect();
    let stats: CorpusStats =
        serde_json::from_slice(&std::fs::read(root.join("stats.json")).unwrap()).unwrap();
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
    let wanted: std::collections::BTreeSet<_> = queries
        .iter()
        .flat_map(|q| q.exact_terms().keys().copied().collect::<Vec<_>>())
        .collect();
    let mut postings: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
    let mut lengths = Vec::new();
    for (part, r) in readers.iter().enumerate() {
        lengths.extend_from_slice(r.lens());
        for &word in &wanted {
            let Some(handle) = futures::executor::block_on(r.term(word)).unwrap() else {
                continue;
            };
            let term = futures::executor::block_on(r.load([handle]))
                .unwrap()
                .remove(&handle)
                .unwrap();
            let list = postings.entry(word.to_owned()).or_default();
            for b in &term.blocks {
                let data = futures::executor::block_on(r.postings(b, term.df)).unwrap();
                list.extend(
                    data.iter()
                        .map(|&(row, tf)| (row + part as u32 * 200_000, tf)),
                );
            }
        }
    }
    let saturation = Saturation::new(&stats, &params);
    let mut seeds = BTreeMap::new();
    for (word, list) in &postings {
        let mut selected = Vec::new();
        for part in 0..5 {
            let mut best: Vec<_> = list
                .iter()
                .filter(|&&(row, _)| row / 200_000 == part)
                .map(|&(row, tf)| (row, saturation.factor(tf, lengths[row as usize])))
                .collect();
            best.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            best.truncate(64);
            selected.extend(best.into_iter().map(|(row, _)| row));
        }
        seeds.insert(word, selected);
    }
    let mut floors = Vec::new();
    let mut seed_rows = 0;
    for q in queries {
        let exact = q.exact_terms();
        let candidates: std::collections::BTreeSet<_> = exact
            .keys()
            .flat_map(|&word| seeds.get(&word.to_owned()).into_iter().flatten().copied())
            .collect();
        seed_rows += candidates.len();
        let mut scores: Vec<_> = candidates
            .into_iter()
            .map(|row| {
                let mut score = 0.0f32;
                for (&word, &qf) in &exact {
                    let Some(list) = postings.get(word) else {
                        continue;
                    };
                    if let Ok(i) = list.binary_search_by_key(&row, |&(r, _)| r) {
                        score +=
                            TermWeight::new(qf, stats.document_frequencies[word], &stats, &params)
                                .score_cached(list[i].1, lengths[row as usize], &saturation);
                    }
                }
                score
            })
            .collect();
        scores.sort_unstable_by(|a, b| b.total_cmp(a));
        floors.push(scores.get(9).copied().unwrap_or(0.0));
    }
    std::fs::write(
        root.join("seed-floors.json"),
        serde_json::to_vec(&floors).unwrap(),
    )
    .unwrap();
    eprintln!("seed_rows={seed_rows}");
}

// Experimental BP: balance each split, cluster shared terms, and stop at one row window.
fn bisect_documents(builder: &SegmentBuilder) -> Vec<u32> {
    let mut edges = vec![Vec::<u32>::new(); builder.lengths.len()];
    let mut terms = 0;
    for postings in builder.postings.values().filter(|p| p.rows.len() >= 3) {
        // Unique terms cannot improve the partition objective.
        for &row in &postings.rows {
            edges[row as usize].push(terms as u32);
        }
        terms += 1;
    }
    struct Work<'a> {
        edges: &'a [Vec<u32>],
        total: Vec<u32>,
        left: Vec<u32>,
        weight: Vec<f32>,
        score: Vec<f32>,
    }
    impl Work<'_> {
        fn partition(&mut self, rows: &mut [u32]) {
            if rows.len() <= BLOCK {
                rows.sort_unstable();
                return;
            }
            let mut touched = Vec::new();
            for &row in rows.iter() {
                for &term in &self.edges[row as usize] {
                    if self.total[term as usize] == 0 {
                        touched.push(term as usize);
                    }
                    self.total[term as usize] += 1;
                }
            }
            let mid = rows.len() / 2;
            for _ in 0..4 {
                for &row in &rows[..mid] {
                    for &term in &self.edges[row as usize] {
                        self.left[term as usize] += 1;
                    }
                }
                for &term in &touched {
                    self.weight[term] = ((self.left[term] as f32 + 0.5)
                        / (self.total[term] as f32 - self.left[term] as f32 + 0.5))
                        .ln();
                    self.left[term] = 0;
                }
                for &row in rows.iter() {
                    self.score[row as usize] = self.edges[row as usize]
                        .iter()
                        .map(|&t| self.weight[t as usize])
                        .sum();
                }
                rows.select_nth_unstable_by(mid, |a, b| {
                    self.score[*b as usize]
                        .total_cmp(&self.score[*a as usize])
                        .then(a.cmp(b))
                });
            }
            for term in touched {
                self.total[term] = 0;
            }
            let (left, right) = rows.split_at_mut(mid);
            self.partition(left);
            self.partition(right);
        }
    }
    let mut rows: Vec<_> = (0..builder.lengths.len() as u32).collect();
    let mut work = Work {
        edges: &edges,
        total: vec![0; terms],
        left: vec![0; terms],
        weight: vec![0.0; terms],
        score: vec![0.0; rows.len()],
    };
    work.partition(&mut rows);
    rows
}

fn permute_builder(mut builder: SegmentBuilder, order: &[u32]) -> SegmentBuilder {
    let mut inverse = vec![0; order.len()];
    for (new, &old) in order.iter().enumerate() {
        inverse[old as usize] = new as u32;
    }
    builder.lengths = order
        .iter()
        .map(|&old| builder.lengths[old as usize])
        .collect();
    for p in builder.postings.values_mut() {
        let mut rows: Vec<_> = p
            .rows
            .iter()
            .enumerate()
            .map(|(i, &old)| (inverse[old as usize], i))
            .collect();
        rows.sort_unstable();
        let mut new = TermPostings::default();
        for (row, old) in rows {
            new.rows.push(row);
            new.positions.extend_from_slice(p.of(old));
            new.ends.push(new.positions.len() as u32);
        }
        *p = new;
    }
    builder
}

#[test]
#[ignore = "requires corpus.jsonl; experimental recursive graph bisection kill test"]
fn reorder_msmarco() {
    let root = root();
    let output = root.join("reordered");
    std::fs::create_dir_all(&output).unwrap();
    let input = std::io::BufReader::new(std::fs::File::open(root.join("corpus.jsonl")).unwrap());
    let mut builder = SegmentBuilder::new(true);
    let params = FtsParams::default();
    let (mut rows, mut part) = (0, 0);
    for line in input.lines() {
        let doc: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        builder
            .add_document(&crate::tokenize(doc["text"].as_str().unwrap(), &params))
            .unwrap();
        rows += 1;
        if rows % 200_000 != 0 {
            continue;
        }
        let start = Instant::now();
        let order = bisect_documents(&builder);
        let partition_seconds = start.elapsed().as_secs_f64();
        assert!(
            partition_seconds <= 20.0,
            "BP killed: >20s per 200K segment"
        );
        builder = permute_builder(builder, &order);
        let bytes = builder.finish().unwrap();
        eprintln!(
            "{}",
            serde_json::json!({"part":part,"partition_seconds":partition_seconds,"reorder_and_finish_seconds":start.elapsed().as_secs_f64(),"bytes":bytes.len()})
        );
        std::fs::write(output.join(format!("segment-{part}.bft")), bytes).unwrap();
        std::fs::write(
            output.join(format!("permutation-{part}.json")),
            serde_json::to_vec(&order).unwrap(),
        )
        .unwrap();
        builder = SegmentBuilder::new(true);
        part += 1;
    }
    assert_eq!(rows, 1_000_000);
    for file in ["stats.json", "workload.json"] {
        std::fs::hard_link(root.join(file), output.join(file)).unwrap();
    }
}

#[test]
#[ignore = "requires format-5 segments and their upgraded copies"]
fn lossless_msmarco() {
    let root = root();
    let mut occurrences = 0;
    let mut raw_bytes = 0;
    let mut compressed_bytes = 0;
    for part in 0..5 {
        let a = reader(std::fs::read(root.join(format!("segment-{part}.bft"))).unwrap());
        let b = reader(
            std::fs::read(root.join("upgraded").join(format!("segment-{part}.bft"))).unwrap(),
        );
        assert_eq!(
            futures::executor::block_on(a.stats()).unwrap(),
            futures::executor::block_on(b.stats()).unwrap()
        );
        let left = futures::executor::block_on(a.prefix_terms("")).unwrap();
        let right = futures::executor::block_on(b.prefix_terms("")).unwrap();
        assert_eq!(left.len(), right.len());
        for (left, right) in left.chunks(128).zip(right.chunks(128)) {
            let aa = futures::executor::block_on(a.load(left.iter().copied())).unwrap();
            let bb = futures::executor::block_on(b.load(right.iter().copied())).unwrap();
            for (aa, bb) in aa.values().zip(bb.values()) {
                assert_eq!(aa.df, bb.df);
                for (x, y) in aa.blocks.iter().zip(&bb.blocks) {
                    assert_eq!(
                        (x.first, x.last, x.count, x.crc),
                        (y.first, y.last, y.count, y.crc)
                    );
                    let postings = futures::executor::block_on(a.postings(x, aa.df)).unwrap();
                    let raw = futures::executor::block_on(
                        a.source
                            .read(x.positions..x.positions + u64::from(x.positions_size)),
                    )
                    .unwrap();
                    let compressed = futures::executor::block_on(
                        b.source
                            .read(y.positions..y.positions + u64::from(y.positions_size)),
                    )
                    .unwrap();
                    verify(&raw, x.positions_crc).unwrap();
                    verify(&compressed, y.positions_crc).unwrap();
                    raw_bytes += raw.len();
                    compressed_bytes += compressed.len();
                    let mut raw = positions::Decoder::new(&raw, false).unwrap();
                    let mut compressed = positions::Decoder::new(&compressed, true).unwrap();
                    for &(_, tf) in postings.iter() {
                        assert_eq!(raw.document(tf).unwrap(), compressed.document(tf).unwrap());
                        occurrences += u64::from(tf);
                    }
                    raw.finish().unwrap();
                    compressed.finish().unwrap();
                }
            }
        }
    }
    eprintln!(
        "{}",
        serde_json::json!({"occurrences":occurrences,"raw_bytes":raw_bytes,"compressed_bytes":compressed_bytes})
    );
}
