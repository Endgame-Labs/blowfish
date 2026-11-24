use super::*;
use futures::executor::block_on;
use std::io::{BufRead, Write};
use std::path::Path;
use std::time::Instant;

mod cold_pack {
    include!("cold_pack.rs");
}

const FORWARD_BLOCK: usize = 64;

fn perf(command: &str) {
    if let Some(path) = std::env::var_os("BF_TEXT_PERF_CONTROL") {
        writeln!(
            std::fs::OpenOptions::new().write(true).open(path).unwrap(),
            "{command}"
        )
        .unwrap();
        if let Some(ack) = std::env::var_os("BF_TEXT_PERF_ACK") {
            let mut line = String::new();
            std::io::BufReader::new(std::fs::File::open(ack).unwrap())
                .read_line(&mut line)
                .unwrap();
        }
    }
}

fn open(root: &Path) -> Vec<SegmentReader> {
    (0..5)
        .map(|i| {
            block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
                std::fs::read(root.join(format!("segment-{i}.bft"))).unwrap(),
            ))))
            .unwrap()
        })
        .collect()
}

fn varint(out: &mut Vec<u8>, mut n: u32) {
    while n >= 128 {
        out.push(n as u8 | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn take_varint(bytes: &[u8], at: &mut usize) -> u32 {
    let mut n = 0;
    for shift in (0..35).step_by(7) {
        let b = bytes[*at];
        *at += 1;
        n |= u32::from(b & 127) << shift;
        if b < 128 {
            return n;
        }
    }
    panic!("bad prototype varint")
}

struct ForwardDoc {
    row: u32,
    length: u32,
    terms: Vec<(u32, u32)>,
}
struct ForwardBlock {
    docs: Vec<ForwardDoc>,
}
struct Bmp {
    blocks: Vec<ForwardBlock>,
    /// Lexicographic term IDs keep score addition in canonical order.
    terms: BTreeMap<String, u32>,
    inverted: Vec<Vec<(u32, u8)>>,
    saturation: Saturation,
    bytes: u64,
    build_seconds: f64,
}
impl Bmp {
    fn build(reader: &SegmentReader, stats: &CorpusStats, params: &FtsParams) -> Self {
        let start = Instant::now();
        let sat = Saturation::new(stats, params);
        let all = block_on(reader.prefix_terms("")).unwrap();
        let mut docs: Vec<_> = reader
            .lens()
            .iter()
            .enumerate()
            .map(|(row, &length)| ForwardDoc {
                row: row as u32,
                length,
                terms: Vec::new(),
            })
            .collect();
        let mut terms = BTreeMap::new();
        let mut df = Vec::new();
        let mut positions = 0u64;
        let mut dictionary_bytes = 0;
        // Walk the FST directly: prefix_terms returns handles rather than term text.
        let mut vocabulary = BTreeMap::new();
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
                vocabulary.insert(String::from_utf8(term.to_vec()).unwrap(), handle);
            }
        }
        assert_eq!(all.len(), vocabulary.len());
        for (term, handle) in vocabulary {
            let id = terms.len() as u32;
            dictionary_bytes += term.len() as u64 + 8;
            terms.insert(term.clone(), id);
            df.push(stats.document_frequencies[&term]);
            let meta = block_on(reader.load([handle]))
                .unwrap()
                .remove(&handle)
                .unwrap();
            for b in &meta.blocks {
                positions += u64::from(b.positions_size);
                for &(row, tf) in block_on(reader.postings(b, meta.df)).unwrap().iter() {
                    docs[row as usize].terms.push((id, tf));
                }
            }
        }
        // Rare terms first, with deterministic term and row tie breaks. All terms
        // participate, including unique terms, as specified by the cheap-sort proposal.
        let mut keys = vec![(u32::MAX, u32::MAX, 0, 0); docs.len()];
        for doc in &docs {
            let mut rare = doc
                .terms
                .iter()
                .map(|&(id, _)| (df[id as usize], id))
                .collect::<Vec<_>>();
            rare.sort_unstable();
            keys[doc.row as usize] = (
                rare.first().map_or(u32::MAX, |v| v.1),
                rare.get(1).map_or(u32::MAX, |v| v.1),
                doc.length,
                doc.row,
            );
        }
        docs.sort_unstable_by_key(|d| keys[d.row as usize]);
        let mut inverted = vec![Vec::new(); terms.len()];
        let mut blocks = Vec::new();
        let mut bytes = positions + dictionary_bytes;
        let mut iter = docs.into_iter();
        loop {
            let docs: Vec<_> = iter.by_ref().take(FORWARD_BLOCK).collect();
            if docs.is_empty() {
                break;
            }
            let id = blocks.len() as u32;
            let mut maxima = BTreeMap::<u32, f64>::new();
            let mut encoded = Vec::new();
            for doc in &docs {
                varint(&mut encoded, doc.row);
                varint(&mut encoded, doc.length);
                varint(&mut encoded, doc.terms.len() as u32);
                let mut previous = 0;
                for &(term, tf) in &doc.terms {
                    varint(&mut encoded, term - previous);
                    previous = term;
                    varint(&mut encoded, tf);
                    let maximum = maxima.entry(term).or_default();
                    *maximum = maximum.max(sat.factor(tf, doc.length));
                }
            }
            bytes += encoded.len() as u64 + 12; // offset, frame length, and checksum
            for (term, maximum) in maxima {
                let q = (maximum / (params.k1 as f64 + 1.0) * 255.0)
                    .ceil()
                    .clamp(0.0, 255.0) as u8;
                inverted[term as usize].push((id, q));
            }
            blocks.push(ForwardBlock { docs });
        }
        for list in &inverted {
            let mut encoded = Vec::new();
            let mut previous = 0;
            for &(id, q) in list {
                varint(&mut encoded, id - previous);
                encoded.push(q);
                previous = id;
            }
            bytes += encoded.len() as u64 + 12;
        }
        Self {
            blocks,
            terms,
            inverted,
            saturation: sat,
            bytes,
            build_seconds: start.elapsed().as_secs_f64(),
        }
    }
}

fn bmp_search(
    segments: &[Bmp],
    query: &Query,
    stats: &CorpusStats,
    params: &FtsParams,
    k: usize,
) -> (Vec<SearchHit>, usize, usize) {
    if k == 0 {
        return (Vec::new(), 0, 0);
    }
    let exact = query.exact_terms();
    let weights: Vec<Vec<_>> = segments
        .iter()
        .map(|s| {
            exact
                .iter()
                .filter_map(|(&term, &qf)| {
                    s.terms.get(term).map(|&id| {
                        (
                            id,
                            TermWeight::new(qf, stats.document_frequencies[term], stats, params),
                        )
                    })
                })
                .collect()
        })
        .collect();
    let mut candidates = Vec::new();
    for (part, s) in segments.iter().enumerate() {
        let mut bounds = vec![0.0f32; s.blocks.len()];
        for (term, weight) in &weights[part] {
            for &(block, q) in &s.inverted[*term as usize] {
                // Upward-rounded saturation, rounded contribution, and canonical
                // f32 addition ensure every bound dominates every actual score.
                let saturation = (f64::from(q) / 255.0 * (params.k1 as f64 + 1.0)).next_up();
                bounds[block as usize] += weight.score_saturation(saturation).next_up();
            }
        }
        candidates.extend(
            bounds
                .into_iter()
                .enumerate()
                .filter(|(_, b)| *b > 0.0)
                .map(|(block, bound)| (bound, part, block)),
        );
    }
    candidates.sort_unstable_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    let mut heap = BinaryHeap::<Reverse<SearchHit>>::new();
    let (mut blocks, mut rows) = (0, 0);
    for (bound, part, block) in candidates {
        if heap.len() == k && bound < heap.peek().unwrap().0.score {
            break;
        }
        blocks += 1;
        for doc in &segments[part].blocks[block].docs {
            rows += 1;
            let mut score = 0.0f32;
            let mut qi = 0;
            for &(term, tf) in &doc.terms {
                while qi < weights[part].len() && weights[part][qi].0 < term {
                    qi += 1;
                }
                if qi == weights[part].len() {
                    break;
                }
                if weights[part][qi].0 == term {
                    score += weights[part][qi].1.score_cached(
                        tf,
                        doc.length,
                        &segments[part].saturation,
                    );
                }
            }
            let h = SearchHit {
                row: part as u32 * 200_000 + doc.row,
                score,
            };
            if score > 0.0 && (heap.len() < k || h > heap.peek().unwrap().0) {
                heap.push(Reverse(h));
                if heap.len() > k {
                    heap.pop();
                }
            }
        }
    }
    let mut hits: Vec<_> = heap.into_iter().map(|h| h.0).collect();
    hits.sort_unstable_by(|a, b| b.cmp(a));
    (hits, blocks, rows)
}

fn baseline(
    readers: &[SegmentReader],
    query: &Query,
    stats: &CorpusStats,
    params: &FtsParams,
    k: usize,
) -> (Vec<SearchHit>, usize, usize) {
    let shared = SearchThreshold::new(k);
    let mut hits = Vec::new();
    let (mut blocks, mut rows) = (0, 0);
    for (part, r) in readers.iter().enumerate() {
        let (found, m) =
            block_on(r.search_internal(query, stats, params, k, None, SearchMode::Shared(&shared)))
                .unwrap();
        hits.extend(found.into_iter().map(|h| SearchHit {
            row: h.row + part as u32 * 200_000,
            score: h.score,
        }));
        blocks += m.blocks_read;
        rows += m.rows_scored;
    }
    hits.sort_unstable_by(|a, b| b.cmp(a));
    hits.truncate(k);
    (hits, blocks, rows)
}

pub fn run() {
    let args: Vec<_> = std::env::args().collect();
    let root = Path::new(args.get(1).expect("fixture directory"));
    let mode = args.get(2).map_or("baseline", String::as_str);
    if mode == "pack" {
        cold_pack::pack(root);
        return;
    }
    let readers = open(root);
    let params = FtsParams::default();
    let stats: CorpusStats =
        serde_json::from_slice(&std::fs::read(root.join("stats.json")).unwrap()).unwrap();
    if mode == "footprint" {
        footprint(&readers);
        return;
    }
    let workload: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(root.join("workload.json")).unwrap()).unwrap();
    let single = mode.ends_with("-single");
    let k = if mode.ends_with("-1000") { 1000 } else { 10 };
    let queries: Vec<_> = workload
        .iter()
        .map(|w| {
            let mut tokens = crate::tokenize(w["request"]["rank_by"][2].as_str().unwrap(), &params);
            if single {
                tokens.truncate(1);
            }
            Query::new(tokens, false)
        })
        .collect();
    let bmp: Vec<_> = if mode.starts_with("bmp") {
        readers
            .iter()
            .map(|r| Bmp::build(r, &stats, &params))
            .collect()
    } else {
        Vec::new()
    };
    if !bmp.is_empty() {
        eprintln!(
            "{}",
            serde_json::json!({"serialized_estimate":bmp.iter().map(|b|b.bytes).sum::<u64>(),"offline_build_seconds":bmp.iter().map(|b|b.build_seconds).sum::<f64>(),"forward_decode":"predecoded optimistic", "bounds":"rebuilt for current namespace statistics"})
        );
    }
    let mut truth = Vec::new();
    for query in &queries {
        truth.push(baseline(&readers, query, &stats, &params, k).0);
    }
    let (mut total_blocks, mut total_rows) = (0, 0);
    let mut timings = Vec::new();
    for pass in 0..2 {
        if pass == 1 {
            perf("enable");
        }
        for (i, query) in queries.iter().enumerate() {
            let start = Instant::now();
            let (hits, blocks, rows) = if bmp.is_empty() {
                baseline(&readers, query, &stats, &params, k)
            } else {
                bmp_search(&bmp, query, &stats, &params, k)
            };
            assert_eq!(hits, truth[i], "query {i}");
            if pass == 1 {
                total_blocks += blocks;
                total_rows += rows;
                timings.push(start.elapsed().as_secs_f64() * 1000.0);
            }
        }
        if pass == 1 {
            perf("disable");
        }
    }
    timings.sort_unstable_by(f64::total_cmp);
    eprintln!(
        "{}",
        serde_json::json!({"mode":mode,"k":k,"queries":queries.len(),"p50_ms":timings[timings.len()/2],"p90_ms":timings[timings.len()*9/10],"blocks_per_query":total_blocks as f64/queries.len() as f64,"rows_per_query":total_rows as f64/queries.len() as f64,"exact":true})
    );
    let out:Vec<_>=truth.iter().map(|hits|serde_json::json!({"rows":hits.iter().map(|h|h.row).collect::<Vec<_>>(),"scores":hits.iter().map(|h|h.score).collect::<Vec<_>>()})).collect();
    std::fs::write(
        root.join(format!("truth-{k}-{single}.json")),
        serde_json::to_vec(&out).unwrap(),
    )
    .unwrap();
}

fn footprint(readers: &[SegmentReader]) {
    let (mut original, mut postings, mut records, mut positions, mut other) = (0, 0, 0, 0, 0);
    let (mut new_postings, mut new_records) = (0, 0);
    for r in readers {
        let mut region_post = 0;
        let mut region_records = 0;
        let mut region_pos = 0;
        for handles in block_on(r.prefix_terms("")).unwrap().chunks(128) {
            let meta = block_on(r.load(handles.iter().copied())).unwrap();
            for t in meta.values() {
                region_records += if r.compact {
                    compact::record_size(&t.blocks)
                } else {
                    RECORD_HEADER + BLOCK_META * t.blocks.len() + 4
                };
                // Two base offsets, record length, and checksum. All offsets and
                // counts within a term follow from block sizes and document frequency.
                new_records += 24;
                for b in &t.blocks {
                    region_post += b.size as usize;
                    region_pos += b.positions_size as usize;
                    let p = block_on(r.postings(b, t.df)).unwrap();
                    if b.count < BLOCK as u32 {
                        let mut encoded = Vec::new();
                        let mut previous = b.first;
                        for (i, &(row, tf)) in p.iter().enumerate() {
                            if i > 0 {
                                varint(&mut encoded, row - previous);
                            }
                            varint(&mut encoded, tf);
                            previous = row;
                        }
                        let mut at = 0;
                        let mut row = b.first;
                        for (i, &expected) in p.iter().enumerate() {
                            if i > 0 {
                                row += take_varint(&encoded, &mut at);
                            }
                            assert_eq!((row, take_varint(&encoded, &mut at)), expected);
                        }
                        assert_eq!(at, encoded.len());
                        new_postings += encoded.len();
                    } else {
                        new_postings += b.size as usize;
                    }
                    let mut record = Vec::new();
                    for n in [
                        b.first,
                        b.last - b.first,
                        b.max_tf,
                        b.min_length,
                        b.size,
                        b.positions_size,
                    ] {
                        varint(&mut record, n);
                    }
                    new_records += record.len() + 8; // independent posting and phrase checksums
                    let bytes = block_on(
                        r.source
                            .read(b.positions..b.positions + u64::from(b.positions_size)),
                    )
                    .unwrap();
                    verify(&bytes, b.positions_crc).unwrap();
                    if !bytes.is_empty() {
                        let mut decoder = positions::Decoder::new(&bytes, true).unwrap();
                        for &(_, tf) in p.iter() {
                            decoder.document(tf).unwrap();
                        }
                        decoder.finish().unwrap();
                    }
                }
            }
        }
        original += r.source.len() as usize;
        postings += region_post;
        records += region_records;
        positions += region_pos;
        other += r.source.len() as usize - region_post - region_records - region_pos;
    }
    eprintln!(
        "{}",
        serde_json::json!({"original":original,"postings":postings,"records":records,"positions":positions,"other":other,"compact_postings":new_postings,"compact_records":new_records,"estimated_total":new_postings+new_records+positions+other,"note":"codec accounting; original dictionary padding retained; no format bump"})
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clustered_forward_scoring_preserves_bits_ties_and_query_frequencies() {
        let params = FtsParams::default();
        let mut stats = CorpusStats::default();
        let mut readers = Vec::new();
        for part in 0..3 {
            let docs: Vec<_> = (0..257)
                .map(|row| {
                    crate::tokenize(
                        &format!(
                            "common common term{} term{} {}",
                            (row * 7 + part) % 19,
                            row % 5,
                            if row % 17 == 0 { "jackpot" } else { "" }
                        ),
                        &params,
                    )
                })
                .collect();
            stats.merge(&CorpusStats::from_documents(&docs));
            let mut builder = SegmentBuilder::new(true);
            for doc in &docs {
                builder.add_document(doc).unwrap();
            }
            readers.push(
                block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
                    builder.finish().unwrap(),
                ))))
                .unwrap(),
            );
        }
        // Changed namespace normalization must rebuild bounds. This prototype
        // deliberately gives the proposal that work for free during preparation.
        for average in [stats.average_doc_length, 1.0, 1025.0] {
            stats.average_doc_length = average;
            let bmp: Vec<_> = readers
                .iter()
                .map(|r| Bmp::build(r, &stats, &params))
                .collect();
            for text in [
                "common",
                "common common jackpot",
                "term1 term3 common",
                "missing",
                "term4",
            ] {
                let q = Query::new(crate::tokenize(text, &params), false);
                for k in [0, 1, 10, 1000] {
                    assert_eq!(
                        bmp_search(&bmp, &q, &stats, &params, k).0,
                        baseline(&readers, &q, &stats, &params, k).0
                    );
                }
            }
        }
    }

    #[test]
    fn prototype_unsigned_varints_round_trip_boundary_values() {
        let values = [0, 1, 127, 128, 16383, 16384, u32::MAX];
        let mut out = Vec::new();
        for n in values {
            varint(&mut out, n);
        }
        let mut at = 0;
        for n in values {
            assert_eq!(take_varint(&out, &mut at), n);
        }
        assert_eq!(at, out.len());
    }
}
