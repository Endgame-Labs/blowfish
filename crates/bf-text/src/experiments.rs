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
                    let mut c = Cursor::new(&raw);
                    for &(_, tf) in list.iter() {
                        let mut previous = 0;
                        for _ in 0..tf {
                            let element = c.u32().unwrap();
                            let position = c.u32().unwrap();
                            arrays += usize::from(element != 0);
                            assert_eq!(element, 0);
                            compressed += varint_bytes(position - previous);
                            previous = position;
                        }
                    }
                    assert_eq!(c.at, raw.len());
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
                    h.row += i as u32 * 200_000;
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
