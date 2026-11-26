use bf_text::{
    CorpusStats, FtsParams, MemoryRangeRead, Query, RangeRead, SearchHit, SearchThreshold,
    SegmentBuilder, SegmentReader, Token, Tokenizer, matches_all_tokens, matches_any_token,
    matches_token_sequence, score_document, tokenize, tokenize_array,
};
use bytes::Bytes;
use futures::executor::block_on;
use proptest::prelude::*;
use roaring::RoaringBitmap;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};

fn build(docs: &[Vec<Token>], positions: bool, tokenizer: Tokenizer) -> Vec<u8> {
    let mut builder = SegmentBuilder::with_tokenizer(positions, tokenizer);
    for (row, doc) in docs.iter().enumerate() {
        assert_eq!(builder.add_document(doc).unwrap(), row as u32);
    }
    builder.finish().unwrap()
}
fn open(bytes: Vec<u8>) -> SegmentReader {
    block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(bytes)))).unwrap()
}
fn expected(
    docs: &[Vec<Token>],
    query: &Query,
    stats: &CorpusStats,
    params: &FtsParams,
    k: usize,
    allowed: Option<&RoaringBitmap>,
) -> Vec<SearchHit> {
    let mut hits: Vec<_> = docs
        .iter()
        .enumerate()
        .filter(|(row, _)| allowed.is_none_or(|a| a.contains(*row as u32)))
        .map(|(row, doc)| SearchHit {
            row: row as u32,
            score: score_document(doc, query, stats, params),
        })
        .filter(|h| h.score > 0.0)
        .collect();
    hits.sort_unstable_by(|a, b| b.cmp(a));
    hits.truncate(k);
    hits
}
fn assert_hits(actual: &[SearchHit], expected: &[SearchHit]) {
    assert_eq!(actual.len(), expected.len());
    for (a, b) in actual.iter().zip(expected) {
        assert_eq!(a.row, b.row);
        assert!((a.score - b.score).abs() < 1e-5, "{a:?} != {b:?}");
    }
}

#[test]
fn empty_and_single_row_roundtrip() {
    for docs in [
        vec![],
        vec![vec![]],
        vec![tokenize("fox fox", &FtsParams::default())],
    ] {
        let reader = open(build(&docs, true, Tokenizer::WordV4));
        assert_eq!(
            block_on(reader.stats()).unwrap(),
            CorpusStats::from_documents(&docs)
        );
        assert_eq!(reader.num_docs(), docs.len() as u32);
        let empty = Query::default();
        assert_eq!(
            block_on(reader.contains_all_tokens(&empty)).unwrap().len(),
            docs.len() as u64
        );
        assert!(
            block_on(reader.contains_any_token(&empty))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            block_on(reader.contains_token_sequence(&empty))
                .unwrap()
                .len(),
            docs.len() as u64
        );
        assert!(
            block_on(reader.search(
                &empty,
                &block_on(reader.stats()).unwrap(),
                &FtsParams::default(),
                10,
                None
            ))
            .unwrap()
            .is_empty()
        );
    }
}

#[test]
fn masked_document_frequencies_count_rows_and_preserve_input_order() {
    let docs: Vec<_> = (0..5137)
        .map(|row| {
            if row % 17 == 0 {
                None
            } else if row % 19 == 0 {
                Some(Vec::new())
            } else {
                let mut tokens = vec![Token::new("common", 0), Token::new("common", 1)];
                if row % 97 == 0 {
                    tokens.push(Token::new("rare", 2));
                }
                if row % 7 == 0 {
                    tokens.push(Token::new("prefix", 3));
                }
                Some(tokens)
            }
        })
        .collect();
    let mut builder = SegmentBuilder::new(true);
    for doc in &docs {
        if let Some(tokens) = doc {
            builder.add_document(tokens).unwrap();
        } else {
            builder.add_missing_document().unwrap();
        }
    }
    let bytes = builder.finish().unwrap();
    let terms = [
        "common", "rare", "common", "pre", "prefix", "absent", "Common",
    ];
    for divisor in [1, 2, 7, 97, 5137] {
        let rows: RoaringBitmap = (0..docs.len() as u32)
            .filter(|row| row % divisor == 0)
            .collect();
        let expected: Vec<_> = terms
            .iter()
            .map(|term| {
                rows.iter()
                    .filter(|row| {
                        docs[*row as usize]
                            .as_ref()
                            .is_some_and(|tokens| tokens.iter().any(|t| &t.text == term))
                    })
                    .count() as u64
            })
            .collect();
        // A cold pass and a cached pass cover both posting sources. Header opens
        // also check lazy lengths and packed tails crossing block boundaries.
        let header = bf_text::text_header(&bytes).unwrap().unwrap();
        let reader = SegmentReader::open_with_header(
            Arc::new(MemoryRangeRead::from(bytes.clone())),
            &header,
        )
        .unwrap();
        for _ in 0..2 {
            assert_eq!(
                block_on(reader.document_frequencies(&terms, &rows)).unwrap(),
                expected,
                "divisor={divisor}"
            );
        }
    }
}

#[test]
fn masked_frequencies_skip_empty_masks_and_gap_free_payloads() {
    let docs: Vec<_> = (0..16_385)
        .map(|row| {
            let mut tokens = vec![Token::new("common", 0)];
            if row % 257 == 0 {
                tokens.push(Token::new("rare", 1));
            }
            tokens
        })
        .collect();
    let bytes = build(&docs, true, Tokenizer::WordV4);
    let header = bf_text::text_header(&bytes).unwrap().unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let reader = SegmentReader::open_with_header(
        Arc::new(Recording {
            bytes: bytes.into(),
            reads: reads.clone(),
        }),
        &header,
    )
    .unwrap();
    assert_eq!(
        block_on(reader.document_frequencies(&["common"], &RoaringBitmap::new())).unwrap(),
        [0]
    );
    assert!(
        block_on(reader.document_frequencies(&[], &[7].into_iter().collect()))
            .unwrap()
            .is_empty()
    );
    assert!(
        block_on(reader.document_frequencies(&["common"], &[16_385].into_iter().collect()))
            .is_err()
    );
    assert!(reads.lock().unwrap().is_empty());
    let all: RoaringBitmap = (0..16_385).collect();
    assert_eq!(
        block_on(reader.document_frequencies(&["common", "rare"], &all)).unwrap(),
        [16_385, 64]
    );
    // Dictionary counts need neither lengths nor decoded postings.
    assert!(reader.doc_lengths().is_empty());
    let sparse = [7, 257, 258, 16_384].into_iter().collect();
    assert_eq!(
        block_on(reader.document_frequencies(&["common"], &sparse)).unwrap(),
        [4]
    );
    assert!(
        reader.doc_lengths().is_empty(),
        "gap-free blocks need no payload decode"
    );
    reads.lock().unwrap().clear();
    assert_eq!(
        block_on(reader.document_frequencies(&["common"], &sparse)).unwrap(),
        [4]
    );
    assert!(reads.lock().unwrap().is_empty());
    assert_eq!(
        block_on(reader.document_frequencies(&["rare"], &sparse)).unwrap(),
        [1]
    );
    assert_eq!(reader.doc_lengths().len(), docs.len());
}

#[test]
#[cfg(feature = "legacy-fixtures")]
fn masked_frequencies_read_legacy_layouts_with_empty_and_missing_fields() {
    for version in 4..=6 {
        let mut builder = SegmentBuilder::new(true);
        for row in 0..1027 {
            if row % 17 == 0 {
                builder.add_missing_document().unwrap();
            } else if row % 19 == 0 {
                builder.add_document(&[]).unwrap();
            } else {
                let mut tokens = vec![Token::new("common", 0), Token::new("common", 1)];
                if row % 7 == 0 {
                    tokens.push(Token::new("rare", 2));
                }
                builder.add_document(&tokens).unwrap();
            }
        }
        let bytes = match version {
            4 => builder.finish_colocated_fixture(),
            5 => builder.finish_bundled_fixture(),
            6 => builder.finish_compressed_fixture(),
            _ => unreachable!(),
        }
        .unwrap();
        let reader = open(bytes);
        for divisor in [1, 2, 127] {
            let rows: RoaringBitmap = (0..1027).filter(|row| row % divisor == 0).collect();
            let common = rows
                .iter()
                .filter(|row| row % 17 != 0 && row % 19 != 0)
                .count() as u64;
            let rare = rows
                .iter()
                .filter(|row| row % 17 != 0 && row % 19 != 0 && row % 7 == 0)
                .count() as u64;
            assert_eq!(
                block_on(
                    reader.document_frequencies(&["rare", "common", "absent", "common"], &rows)
                )
                .unwrap(),
                [rare, common, 0, common],
                "version={version} divisor={divisor}"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn masked_frequencies_match_document_presence(
        raw in prop::collection::vec(prop::option::of(prop::collection::vec(0u8..8, 0..16)), 0..600),
        divisor in 1u32..15, offset in 0u32..15,
    ) {
        let mut builder = SegmentBuilder::new(false);
        for doc in &raw {
            if let Some(words) = doc {
                builder.add_document(&words.iter().enumerate().map(|(i, word)|Token::new(format!("term{word}"), i as u32)).collect::<Vec<_>>()).unwrap();
            } else {
                builder.add_missing_document().unwrap();
            }
        }
        let reader = open(builder.finish().unwrap());
        let rows: RoaringBitmap = (0..raw.len() as u32).filter(|row| (row + offset) % divisor == 0).collect();
        let terms: Vec<_> = (0..10).map(|word|format!("term{word}")).collect();
        let wanted: Vec<_> = terms.iter().map(String::as_str).collect();
        let expected: Vec<_> = (0..10u8).map(|word| rows.iter().filter(|row|raw[*row as usize].as_ref().is_some_and(|words|words.contains(&word))).count() as u64).collect();
        prop_assert_eq!(block_on(reader.document_frequencies(&wanted, &rows)).unwrap(), expected);
    }
}

#[test]
fn arrays_stopword_gaps_repetitions_and_boolean_subwords() {
    let p = FtsParams::default();
    let docs = vec![
        tokenize_array(&["fox", "dog"], &p),
        tokenize("fox dog", &p),
        tokenize("fox the dog", &p),
        tokenize("fox fox", &p),
        tokenize("don't 3.14 foo_bar", &p),
    ];
    let reader = open(build(&docs, true, p.tokenizer));
    for (query, prefix) in [
        ("fox dog", false),
        ("fox the dog", false),
        ("fox fox", false),
        ("don", false),
        ("3", false),
        ("foo", false),
        ("fox do", true),
        ("don t", false),
    ] {
        let q = Query::new(tokenize(query, &p), prefix);
        let all: RoaringBitmap = docs
            .iter()
            .enumerate()
            .filter(|(_, d)| matches_all_tokens(d, &q, p.tokenizer))
            .map(|(i, _)| i as u32)
            .collect();
        let any: RoaringBitmap = docs
            .iter()
            .enumerate()
            .filter(|(_, d)| matches_any_token(d, &q, p.tokenizer))
            .map(|(i, _)| i as u32)
            .collect();
        assert_eq!(block_on(reader.contains_all_tokens(&q)).unwrap(), all);
        assert_eq!(block_on(reader.contains_any_token(&q)).unwrap(), any);
        if !prefix {
            let phrase: RoaringBitmap = docs
                .iter()
                .enumerate()
                .filter(|(_, d)| matches_token_sequence(d, &q))
                .map(|(i, _)| i as u32)
                .collect();
            assert_eq!(
                block_on(reader.contains_token_sequence(&q)).unwrap(),
                phrase
            );
        }
    }
    let p = FtsParams {
        remove_stopwords: true,
        ..p
    };
    let docs = vec![
        tokenize("fox the dog", &p),
        tokenize("fox dog", &p),
        tokenize_array(&["fox", "the dog"], &p),
    ];
    assert_eq!(
        docs[0].iter().map(|t| t.position).collect::<Vec<_>>(),
        vec![0, 2]
    );
    let reader = open(build(&docs, true, p.tokenizer));
    let q = Query::new(tokenize("fox the dog", &p), false);
    assert_eq!(
        block_on(reader.contains_token_sequence(&q))
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![0]
    );
    let q = Query::new(tokenize("fox dog", &p), false);
    assert_eq!(
        block_on(reader.contains_token_sequence(&q))
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
fn namespace_stats_produce_segment_independent_scores() {
    let p = FtsParams::default();
    let docs: Vec<_> = ["a fox", "fox fox dog dog dog", "cat", "dog fox", ""]
        .iter()
        .map(|s| tokenize(s, &p))
        .collect();
    let mut stats = CorpusStats::from_documents(&docs[..2]);
    stats.merge(&CorpusStats::from_documents(&docs[2..]));
    assert_eq!(stats, CorpusStats::from_documents(&docs));
    let q = Query::new(tokenize("fox fox dog", &p), false);
    for (base, chunk) in [(0, &docs[..2]), (2, &docs[2..])] {
        let reader = open(build(chunk, false, p.tokenizer));
        let hits = block_on(reader.search(&q, &stats, &p, 10, None)).unwrap();
        for hit in hits {
            assert!(
                (hit.score - score_document(&docs[base + hit.row as usize], &q, &stats, &p)).abs()
                    < 1e-6
            );
        }
        assert_eq!(
            block_on(reader.contains_token_sequence(&q))
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
}

#[derive(Clone)]
struct Recording {
    bytes: Bytes,
    reads: Arc<Mutex<Vec<Range<u64>>>>,
}
#[async_trait::async_trait]
impl RangeRead for Recording {
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        self.reads.lock().unwrap().push(range.clone());
        Ok(self.bytes.slice(range.start as usize..range.end as usize))
    }
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

#[test]
fn small_open_one_read_and_positions_are_lazy() {
    let p = FtsParams::default();
    let docs = vec![tokenize("fox dog fox", &p)];
    let bytes = build(&docs, true, p.tokenizer);
    let reads = Arc::new(Mutex::new(Vec::new()));
    let source = Recording {
        bytes: bytes.into(),
        reads: reads.clone(),
    };
    let reader = block_on(SegmentReader::open(Arc::new(source))).unwrap();
    assert_eq!(reads.lock().unwrap().len(), 1);
    let stats = block_on(reader.stats()).unwrap();
    reads.lock().unwrap().clear();
    let q = Query::new(tokenize("fox", &p), false);
    block_on(reader.search(&q, &stats, &p, 10, None)).unwrap();
    // Statistics fetched the dictionary bundle, including this short term's
    // directory and postings. Scoring needs no further backend read.
    assert!(reads.lock().unwrap().is_empty());
    block_on(reader.contains_token_sequence(&q)).unwrap();
    // Phrase positions stay separate from dictionary and scoring payloads.
    assert_eq!(reads.lock().unwrap().len(), 1);
}

#[test]
fn block_max_prunes_and_allowed_rows_and_ties_stay_correct() {
    let p = FtsParams::default();
    let docs: Vec<_> = (0..2048)
        .map(|i| {
            tokenize(
                if i < 128 {
                    "fox fox fox"
                } else {
                    "fox a b c d e f g h i j k l m n o p"
                },
                &p,
            )
        })
        .collect();
    let reader = open(build(&docs, false, p.tokenizer));
    let q = Query::new(tokenize("fox", &p), false);
    let stats = block_on(reader.stats()).unwrap();
    let (hits, metrics) = block_on(reader.search_with_metrics(&q, &stats, &p, 5, None)).unwrap();
    assert_hits(&hits, &expected(&docs, &q, &stats, &p, 5, None));
    assert!(metrics.windows_pruned > 0, "{metrics:?}");
    assert_eq!(metrics.blocks_read, 1);
    let allowed: RoaringBitmap = [150, 250, 500, 1100, 1600].into_iter().collect();
    let hits = block_on(reader.search(&q, &stats, &p, 3, Some(&allowed))).unwrap();
    assert_hits(&hits, &expected(&docs, &q, &stats, &p, 3, Some(&allowed)));
}

#[test]
fn corrupt_sections_and_short_reads_return_errors() {
    let p = FtsParams::default();
    let docs = vec![tokenize("fox dog fox", &p)];
    let bytes = build(&docs, true, p.tokenizer);
    for index in [bytes.len() - 1, bytes.len() - 72, bytes.len() - 80] {
        let mut corrupt = bytes.clone();
        corrupt[index] ^= 1;
        assert!(
            block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
                corrupt
            ))))
            .is_err()
        );
    }
    let mut corrupt = bytes.clone();
    let header = bf_text::text_header(&bytes).unwrap().unwrap();
    corrupt[header.chunks[0].1 as usize] ^= 1;
    let reader = open(corrupt);
    assert!(block_on(reader.stats()).is_err());
    for len in 0..72 {
        assert!(
            block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
                bytes[..len].to_vec()
            ))))
            .is_err()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn indexed_search_equals_brute_force(
        raw in prop::collection::vec(prop::collection::vec(0u8..12,0..28),0..350),
        query in prop::collection::vec(0u8..14,0..8), prefix in any::<bool>(),
        k in 0usize..25, b in 0.0f32..1.0, k1 in 0.01f32..4.0,k3 in 0.01f32..20.0,
        mask in 1u32..8,
    ) {
        let params=FtsParams { b,k1,k3,..FtsParams::default() };
        let docs:Vec<_>=raw.iter().map(|d|d.iter().enumerate().map(|(i,t)|Token::new(format!("term{t}"),i as u32)).collect()).collect();
        let query=Query::new(query.iter().enumerate().map(|(i,t)|Token::new(format!("term{t}"),i as u32)).collect(),prefix);
        let allowed:RoaringBitmap=(0..docs.len() as u32).filter(|i|i%mask==0).collect();
        let reader=open(build(&docs,true,params.tokenizer));
        // Add an external collection to ensure pruning never assumes local statistics.
        let mut stats=block_on(reader.stats()).unwrap();stats.merge(&CorpusStats::from_documents(&[vec![Token::new("term1",0),Token::new("external",1)]]));
        for allowed in [None,Some(&allowed)] {
            let actual=block_on(reader.search(&query,&stats,&params,k,allowed)).unwrap();
            assert_hits(&actual,&expected(&docs,&query,&stats,&params,k,allowed));
            let prefetched=block_on(reader.search_prefetched(&query,&stats,&params,k,allowed)).unwrap();
            prop_assert_eq!(prefetched, actual);
        }
        let all:RoaringBitmap=docs.iter().enumerate().filter(|(_,d)|matches_all_tokens(d,&query,params.tokenizer)).map(|(i,_)|i as u32).collect();
        prop_assert_eq!(block_on(reader.contains_all_tokens(&query)).unwrap(),all);
        if !prefix {
            let phrase:RoaringBitmap=docs.iter().enumerate().filter(|(_,d)|matches_token_sequence(d,&query)).map(|(i,_)|i as u32).collect();
            prop_assert_eq!(block_on(reader.contains_token_sequence(&query)).unwrap(),phrase);
        }
    }
}

#[test]
fn long_lists_preserve_exact_scores_with_rare_terms_stopwords_and_prefixes() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..32_769)
        .map(|row| {
            let mut words = vec!["common", "the", "of"];
            if row % 5 == 0 {
                words.push("common");
            }
            if row % 97 == 0 {
                words.push("rare");
            }
            if row % 193 == 0 {
                words.push("other");
            }
            if row % 257 == 0 {
                words.push("prefix");
            }
            if row % 128 == 0 {
                words.extend(["boundary", "boundary"]);
            }
            if row == 32_768 {
                words.extend(std::iter::repeat_n("rare", 1_030));
            }
            tokenize(&words.join(" "), &params)
        })
        .collect();
    let reader = open(build(&docs, false, params.tokenizer));
    let mut stats = CorpusStats::from_documents(&docs);
    stats.merge(&CorpusStats::from_documents(&[tokenize(
        "external rare common",
        &params,
    )]));
    let allowed: RoaringBitmap = (129..3_000).chain(32_760..32_780).collect();
    for (text, prefix) in [
        ("common the of", false),
        ("the rare rare of other boundary", false),
        ("rare common pre", true),
        ("common pre", true),
        ("pre", true),
    ] {
        let query = Query::new(tokenize(text, &params), prefix);
        for k in [1, 10, 129] {
            for filter in [None, Some(&allowed)] {
                let reference = expected(&docs, &query, &stats, &params, k, filter);
                for _ in 0..2 {
                    let hits =
                        block_on(reader.search_prefetched(&query, &stats, &params, k, filter))
                            .unwrap();
                    assert_eq!(
                        hits,
                        reference,
                        "{text}, k={k}, filtered={}",
                        filter.is_some()
                    );
                }
            }
        }
    }
    // Reuse the same reader after both IDF-only changes and normalization changes.
    // Cached frame maxima must remain independent of IDF and query frequency,
    // and must be discarded with the old normalization table.
    stats.total_docs *= 2;
    let query = Query::new(tokenize("rare rare common of", &params), false);
    for (k1, b, k3, average_scale) in [
        (1.2, 0.75, 19.0, 1.0),
        (0.01, 1.0, 0.01, 1.3),
        (4.0, 0.0, 8.0, 0.8),
        (1.2, 0.75, 8.0, 1.0),
    ] {
        let params = FtsParams {
            k1,
            b,
            k3,
            ..params.clone()
        };
        stats.average_doc_length *= average_scale;
        let reference = expected(&docs, &query, &stats, &params, 10, None);
        for _ in 0..2 {
            assert_eq!(
                block_on(reader.search_prefetched(&query, &stats, &params, 10, None)).unwrap(),
                reference
            );
        }
    }
}

#[test]
fn broad_prefix_expansion_keeps_window_metadata_bounded_and_scores_exact() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..4_097)
        .map(|row| tokenize(&format!("common p{row:05}a p{row:05}b"), &params))
        .collect();
    let reader = open(build(&docs, false, params.tokenizer));
    let stats = CorpusStats::from_documents(&docs);
    let query = Query::new(tokenize("common p", &params), true);
    let allowed: RoaringBitmap = (1_001..1_123).collect();
    for filter in [None, Some(&allowed)] {
        let hits = block_on(reader.search_prefetched(&query, &stats, &params, 10, filter)).unwrap();
        assert_eq!(hits, expected(&docs, &query, &stats, &params, 10, filter));
    }
}

#[test]
fn shared_threshold_keeps_global_top_k_ties_and_late_winners() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..30_000)
        .map(|row| {
            tokenize(
                if (10_000..20_000).contains(&row) {
                    "common common needle prefix"
                } else {
                    "common filler filler filler filler filler filler prefix"
                },
                &params,
            )
        })
        .collect();
    let readers: Vec<_> = docs
        .chunks(10_000)
        .map(|part| open(build(part, false, params.tokenizer)))
        .collect();
    let stats = CorpusStats::from_documents(&docs);
    for (text, prefix) in [
        ("common", false),
        ("common needle needle", false),
        ("common pre", true),
    ] {
        let query = Query::new(tokenize(text, &params), prefix);
        for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2]] {
            let threshold = SearchThreshold::new(10);
            let mut hits = Vec::new();
            for part in order {
                let result = block_on(
                    readers[part]
                        .search_prefetched_shared(&query, &stats, &params, None, &threshold),
                )
                .unwrap();
                if part == 2 && order[0] == 1 {
                    assert!(
                        result.is_empty(),
                        "the established floor excludes every weaker row"
                    );
                }
                hits.extend(result.into_iter().map(|mut hit| {
                    hit.row += part as u32 * 10_000;
                    hit
                }));
            }
            hits.sort_unstable_by(|a, b| b.cmp(a));
            hits.truncate(10);
            assert_eq!(hits, expected(&docs, &query, &stats, &params, 10, None));
        }
        let threshold = SearchThreshold::new(10);
        let mut hits: Vec<_> = std::thread::scope(|scope| {
            let tasks: Vec<_> = readers
                .iter()
                .enumerate()
                .map(|(part, reader)| {
                    let query = &query;
                    let stats = &stats;
                    let threshold = &threshold;
                    let params = &params;
                    scope.spawn(move || {
                        block_on(
                            reader.search_prefetched_shared(query, stats, params, None, threshold),
                        )
                        .unwrap()
                        .into_iter()
                        .map(|mut hit| {
                            hit.row += part as u32 * 10_000;
                            hit
                        })
                        .collect::<Vec<_>>()
                    })
                })
                .collect();
            tasks
                .into_iter()
                .flat_map(|task| task.join().unwrap())
                .collect()
        });
        hits.sort_unstable_by(|a, b| b.cmp(a));
        hits.truncate(10);
        assert_eq!(hits, expected(&docs, &query, &stats, &params, 10, None));
    }
}

#[test]
fn missing_and_empty_fields_have_distinct_corpus_statistics() {
    let p = FtsParams::default();
    let mut builder = SegmentBuilder::new(true);
    builder.add_document(&tokenize("fox fox", &p)).unwrap();
    builder.add_document(&[]).unwrap();
    builder.add_missing_document().unwrap();
    let reader = open(builder.finish().unwrap());
    assert_eq!(reader.num_docs(), 3);
    assert_eq!(reader.doc_lengths(), &[2, 1, 0]);
    assert_eq!(block_on(reader.stats()).unwrap().total_docs, 2);
    assert_eq!(block_on(reader.stats()).unwrap().average_doc_length, 1.5);
    let q = Query::new(tokenize("fox", &p), false);
    let hits =
        block_on(reader.search(&q, &block_on(reader.stats()).unwrap(), &p, 10, None)).unwrap();
    let stats = CorpusStats::from_documents(&[tokenize("fox fox", &p), vec![]]);
    assert!(
        (hits[0].score - score_document(&tokenize("fox fox", &p), &q, &stats, &p)).abs() < 1e-6
    );
    let all = block_on(reader.contains_all_tokens(&Query::default())).unwrap();
    assert_eq!(all.len(), 3);
}

struct ShortRead(MemoryRangeRead);
#[async_trait::async_trait]
impl RangeRead for ShortRead {
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let bytes = self.0.read(range).await?;
        Ok(bytes.slice(..bytes.len().saturating_sub(1)))
    }
    fn len(&self) -> u64 {
        self.0.len()
    }
}

#[test]
fn truncated_range_responses_return_unexpected_eof() {
    let source = ShortRead(MemoryRangeRead::from(build(&[], false, Tokenizer::WordV4)));
    let error = block_on(SegmentReader::open(Arc::new(source)))
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn prefetched_postings_match_demand_search_with_prefixes_repetition_and_filters() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..2000)
        .map(|row| {
            tokenize(
                &format!("common term{} term{} prefix{}", row % 19, row % 7, row % 11),
                &params,
            )
        })
        .collect();
    let reader = open(build(&docs, true, params.tokenizer));
    let stats = block_on(reader.stats()).unwrap();
    for (text, prefix) in [
        ("common term1 term1", false),
        ("term1 pre", true),
        ("missing", false),
        ("", false),
    ] {
        let query = Query::new(tokenize(text, &params), prefix);
        let allowed: RoaringBitmap = (130..170).chain(1024..1200).collect();
        for filter in [None, Some(&allowed)] {
            let plan = block_on(reader.prefetch_plan(&query, filter)).unwrap();
            assert!(
                plan.windows(2)
                    .all(|ranges| ranges[0].end <= ranges[1].start)
            );
            assert!(plan.iter().all(|range| range.start < range.end));
            let actual =
                block_on(reader.search_prefetched(&query, &stats, &params, 20, filter)).unwrap();
            let demand = block_on(reader.search(&query, &stats, &params, 20, filter)).unwrap();
            assert_hits(&actual, &demand);
            assert_hits(
                &actual,
                &expected(&docs, &query, &stats, &params, 20, filter),
            );
        }
    }
    let query = Query::new(tokenize("common", &params), false);
    assert!(
        block_on(reader.prefetch_plan(&query, Some(&RoaringBitmap::new())))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn chunked_dictionary_reads_only_the_query_terms_chunks() {
    let p = FtsParams::default();
    // 12,000 distinct terms span many dictionary bundles.
    let docs: Vec<_> = (0..4000)
        .map(|i| {
            tokenize(
                &format!("w{:05} w{:05} w{:05}", i * 3, i * 3 + 1, i * 3 + 2),
                &p,
            )
        })
        .collect();
    let bytes = build(&docs, false, p.tokenizer);
    let total = bytes.len() as u64;
    let reads = Arc::new(Mutex::new(Vec::new()));
    let reader = block_on(SegmentReader::open(Arc::new(Recording {
        bytes: bytes.into(),
        reads: reads.clone(),
    })))
    .unwrap();
    reads.lock().unwrap().clear();
    let stats = CorpusStats {
        total_docs: 4000,
        average_doc_length: 3.0,
        document_frequencies: (0..12000).map(|i| (format!("w{i:05}"), 1)).collect(),
    };
    let q = Query::new(tokenize("w09000", &p), false);
    let hits = block_on(reader.search(&q, &stats, &p, 10, None)).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].row, 3000);
    // Only the term's dictionary bundle, including its record and posting block.
    let bytes_read: u64 = reads.lock().unwrap().iter().map(|r| r.end - r.start).sum();
    assert!(bytes_read < total / 10, "{bytes_read} of {total}");
    // Every term resolves across chunk boundaries, and prefixes span chunks.
    for i in [0u32, 2047, 2048, 2049, 6000, 11999] {
        let q = Query::new(tokenize(&format!("w{i:05}"), &p), false);
        let hits = block_on(reader.search(&q, &stats, &p, 10, None)).unwrap();
        assert_eq!(hits.len(), 1, "term {i}");
        assert_eq!(hits[0].row, i / 3);
    }
    let prefix = Query::new(tokenize("w0", &p), true);
    let hits = block_on(reader.search(&prefix, &stats, &p, 20000, None)).unwrap();
    assert_eq!(hits.len(), 3334);
    let stats = block_on(reader.stats()).unwrap();
    assert_eq!(stats.document_frequencies.len(), 12000);
}

/// Current segments open from a header kept elsewhere (no object read), load lengths on demand, and
/// the pruned prefetching search matches the exhaustive reference on a corpus with hot and rare terms.
#[test]
fn header_opened_reader_searches_like_the_full_reader() {
    let params = FtsParams::default();
    let mut docs = Vec::new();
    for i in 0..6000u32 {
        let mut text = format!("the common word{} filler{}", i % 7, i % 1301);
        if i % 3 == 0 {
            text.push_str(" often often");
        }
        if i % 997 == 0 {
            text.push_str(" rare alpha");
        }
        if i % 2003 == 5 {
            text.push_str(" rare beta beta");
        }
        docs.push(tokenize(&text, &params));
    }
    let bytes = build(&docs, true, Tokenizer::WordV4);
    let header = bf_text::text_header(&bytes)
        .unwrap()
        .expect("current text header");
    let source = Arc::new(MemoryRangeRead::from(bytes.clone()));
    let hinted = SegmentReader::open_with_header(source, &header).unwrap();
    let full = open(bytes);
    let stats = CorpusStats::from_documents(&docs);
    for text in [
        "rare alpha",
        "the rare beta",
        "common often rare",
        "the common word3",
        "filler5 often alpha",
        "missing",
    ] {
        let query = Query::new(tokenize(text, &params), false);
        let want = expected(&docs, &query, &stats, &params, 10, None);
        let a = block_on(hinted.search_prefetched(&query, &stats, &params, 10, None)).unwrap();
        let b = block_on(full.search_prefetched(&query, &stats, &params, 10, None)).unwrap();
        assert_hits(&a, &want);
        assert_hits(&b, &want);
    }
}

#[test]
fn dictionary_bundle_fetches_short_terms_without_a_record_or_posting_wave() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..2000)
        .map(|i| tokenize(&format!("common unique{i:05}"), &params))
        .collect();
    let bytes = build(&docs, true, params.tokenizer);
    let header = bf_text::text_header(&bytes).unwrap().unwrap();
    assert!(header.is_current());
    let reads = Arc::new(Mutex::new(Vec::new()));
    let reader = SegmentReader::open_with_header(
        Arc::new(Recording {
            bytes: bytes.into(),
            reads: reads.clone(),
        }),
        &header,
    )
    .unwrap();
    assert!(reads.lock().unwrap().is_empty());
    let query = Query::new(tokenize("unique01000", &params), false);
    block_on(reader.prefetch_plan(&query, None)).unwrap();
    assert_eq!(
        reads.lock().unwrap().len(),
        2,
        "lengths and one dictionary bundle"
    );
    reads.lock().unwrap().clear();
    let stats = CorpusStats::from_documents(&docs);
    let hits = block_on(reader.search_prefetched(&query, &stats, &params, 10, None)).unwrap();
    assert_hits(&hits, &expected(&docs, &query, &stats, &params, 10, None));
    assert!(
        reads.lock().unwrap().is_empty(),
        "record and postings arrived with the dictionary"
    );
}

#[test]
fn decoded_blocks_are_reused_with_new_statistics_and_allowed_rows() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..20000)
        .map(|i| {
            tokenize(
                if i % 32 == 0 {
                    "common needle"
                } else {
                    "common"
                },
                &params,
            )
        })
        .collect();
    let bytes = build(&docs, false, params.tokenizer);
    let reads = Arc::new(Mutex::new(Vec::new()));
    let reader = block_on(SegmentReader::open(Arc::new(Recording {
        bytes: bytes.into(),
        reads: reads.clone(),
    })))
    .unwrap();
    let mut stats = CorpusStats::from_documents(&docs);
    let query = Query::new(tokenize("needle", &params), false);
    // Read all sparse-term blocks, so any later allowed mask is covered.
    block_on(reader.search_prefetched(&query, &stats, &params, docs.len(), None)).unwrap();
    stats.total_docs *= 3;
    stats.average_doc_length *= 1.5;
    let allowed = (0..20000).filter(|row| row % 13 == 0).collect();
    let query = Query::new(tokenize("needle needle", &params), false);
    reads.lock().unwrap().clear();
    let hits =
        block_on(reader.search_prefetched(&query, &stats, &params, 10, Some(&allowed))).unwrap();
    assert_hits(
        &hits,
        &expected(&docs, &query, &stats, &params, 10, Some(&allowed)),
    );
    assert!(
        reads.lock().unwrap().is_empty(),
        "decoded immutable blocks need no range reads"
    );
}

#[test]
fn row_aligned_sparse_bounds_prune_disjoint_term_maxima() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..20000)
        .map(|row| {
            let mut tokens = vec![Token::new("common", 0)];
            // Two sparse terms share many row windows, but usually occur on different
            // rows. Only twelve rows contain both and establish the true top-k threshold.
            if row % 32 == 0 {
                tokens.push(Token::new("alpha", 1));
            }
            if row % 32 == 1 || (row < 384 && row % 32 == 0) {
                tokens.push(Token::new("beta", 2));
            }
            tokens
        })
        .collect();
    let reader = open(build(&docs, false, params.tokenizer));
    let stats = CorpusStats::from_documents(&docs);
    let query = Query::new(tokenize("common alpha beta", &params), false);
    let (hits, metrics) =
        block_on(reader.search_prefetched_with_metrics(&query, &stats, &params, 10, None)).unwrap();
    assert_hits(&hits, &expected(&docs, &query, &stats, &params, 10, None));
    assert!(metrics.windows_pruned > 100, "{metrics:?}");
    assert!(metrics.rows_scored < 4000, "{metrics:?}");
}

#[test]
fn dense_frames_reuse_data_but_rescore_changed_statistics_and_masks() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..16_384)
        .map(|row| {
            tokenize(
                if row % 7 == 0 {
                    "common common needle"
                } else {
                    "common filler filler filler"
                },
                &params,
            )
        })
        .collect();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let reader = block_on(SegmentReader::open(Arc::new(Recording {
        bytes: build(&docs, false, params.tokenizer).into(),
        reads: reads.clone(),
    })))
    .unwrap();
    let mut stats = CorpusStats::from_documents(&docs);
    let query = Query::new(tokenize("common", &params), false);
    block_on(reader.search_prefetched(&query, &stats, &params, docs.len(), None)).unwrap();
    stats.total_docs *= 2;
    stats.average_doc_length *= 1.7;
    let params = FtsParams {
        k1: 0.73,
        b: 1.0,
        k3: 0.13,
        ..params
    };
    let query = Query::new(tokenize("common common", &params), false);
    let allowed: RoaringBitmap = (0..docs.len() as u32).filter(|row| row % 5 == 0).collect();
    reads.lock().unwrap().clear();
    let hits =
        block_on(reader.search_prefetched(&query, &stats, &params, 10, Some(&allowed))).unwrap();
    assert_eq!(
        hits,
        expected(&docs, &query, &stats, &params, 10, Some(&allowed))
    );
    assert!(
        reads.lock().unwrap().is_empty(),
        "validated dense data needs no repeated range read"
    );
}

#[test]
fn cached_normalization_bounds_preserve_prefix_presence_without_idf() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..16_385)
        .map(|row| tokenize(if row % 512 < 256 { "common" } else { "filler" }, &params))
        .collect();
    let reader = open(build(&docs, false, params.tokenizer));
    // Prefix scoring does not depend on corpus statistics. Even a zero
    // normalization value must retain presence, and distinguish empty windows.
    let stats = CorpusStats::default();
    let query = Query::new(tokenize("common co", &params), true);
    let empty_windows: RoaringBitmap = (256..512).chain(768..1024).collect();
    for _ in 0..3 {
        for filter in [None, Some(&empty_windows)] {
            let hits =
                block_on(reader.search_prefetched(&query, &stats, &params, 10, filter)).unwrap();
            assert_eq!(hits, expected(&docs, &query, &stats, &params, 10, filter));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn rounded_sparse_partials_preserve_exact_top_k(
        k1 in 0.01f32..4.0, b in 0.0f32..1.0, k3 in 0.01f32..20.0,
        repeats in 1usize..5, bias in 0u32..64,
    ) {
        let params = FtsParams { k1, b, k3, ..FtsParams::default() };
        let docs: Vec<_> = (0..8_192u32).map(|row| {
            let mut words = vec!["a", "c", "e"];
            if row % 5 == 0 { words.push("c"); }
            for (divisor, term) in [(7,"b"),(11,"d"),(13,"f")] {
                if row % divisor == 0 { words.push(term); }
            }
            words.extend(std::iter::repeat_n("filler", (row % 9) as usize));
            words.into_iter().enumerate().map(|(i,t)| Token::new(t,i as u32)).collect()
        }).collect();
        let reader = open(build(&docs, false, params.tokenizer));
        let mut stats = CorpusStats::from_documents(&docs);
        stats.total_docs += u64::from(bias);
        let query = Query::new((0..repeats).flat_map(|_| ["a","b","c","d","e","f"])
            .enumerate().map(|(i,t)| Token::new(t,i as u32)).collect(), false);
        let allowed: RoaringBitmap = (0..docs.len() as u32).filter(|row| (row+bias)%3 != 0).collect();
        for k in [1,10,31] {
            for filter in [None,Some(&allowed)] {
                let hits = block_on(reader.search_prefetched(&query,&stats,&params,k,filter)).unwrap();
                prop_assert_eq!(hits, expected(&docs,&query,&stats,&params,k,filter));
            }
        }
    }
}

#[test]
fn dense_frequency_arrays_preserve_score_bits_and_respect_the_budget() {
    let params = FtsParams::default();
    // A nonsparse term, partial final windows, missing rows, and byte overflows.
    let docs: Vec<_> = (0..9001)
        .map(|row| {
            let count = match row % 13 {
                0 => 0,
                1 => 254,
                2 => 255,
                3 => 300,
                _ => row % 5 + 1,
            };
            tokenize(
                &format!(
                    "{} rare{} tail{}",
                    "common ".repeat(count),
                    row % 997,
                    row % 17
                ),
                &params,
            )
        })
        .collect();
    let bytes = build(&docs, true, params.tokenizer);
    let baseline = open(bytes.clone());
    let direct = open(bytes.clone());
    let stats = block_on(baseline.stats()).unwrap();
    // Charges reported to the owning cache: arrays must be paid for, within the pool budget.
    let charged = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let hook = charged.clone();
    direct.set_memo_charge(
        docs.len() * 8,
        Arc::new(move |bytes| hook.store(bytes, std::sync::atomic::Ordering::Relaxed)),
    );
    let first = Query::new(tokenize("common rare1 missing", &params), false);
    block_on(direct.search(&first, &stats, &params, 10, None)).unwrap();
    assert_eq!(
        direct.dense_arrays(),
        (0, 0),
        "one use does not earn an array"
    );
    // A prefix-only expansion is never counted.
    let prefix = Query::new(Vec::new(), true);
    let _ = block_on(direct.search(&prefix, &stats, &params, 10, None));
    block_on(direct.search(&first, &stats, &params, 10, None)).unwrap();
    let (terms, held) = direct.dense_arrays();
    assert_eq!(
        terms, 1,
        "only the dense term earns one: rare and missing terms do not"
    );
    assert!(held > docs.len() && held <= docs.len() * 8);
    assert!(charged.load(std::sync::atomic::Ordering::Relaxed) >= held as u64);
    block_on(direct.search(&first, &stats, &params, 10, None)).unwrap();
    assert_eq!(
        direct.dense_arrays(),
        (terms, held),
        "arrays are built once"
    );
    // A budget too small for an array keeps the posting path.
    let tight = open(bytes.clone());
    tight.set_memo_charge(1, Arc::new(|_| {}));
    for _ in 0..3 {
        block_on(tight.search(&first, &stats, &params, 10, None)).unwrap();
    }
    assert_eq!(tight.dense_arrays(), (0, 0));
    let mut changed = stats.clone();
    changed.average_doc_length *= 1.5;
    let allowed: RoaringBitmap = (0..docs.len() as u32).filter(|r| r % 3 == 0).collect();
    for stats in [&stats, &changed, &stats] {
        for (text, prefix) in [
            ("common", false),
            ("common common rare1", false),
            ("rare1 com", true),
            ("common tail1", false),
        ] {
            let query = Query::new(tokenize(text, &params), prefix);
            for filter in [None, Some(&allowed)] {
                for k in [0, 1, 10, 130] {
                    let expected =
                        block_on(baseline.search_prefetched(&query, stats, &params, k, filter))
                            .unwrap();
                    for actual in [
                        block_on(direct.search_prefetched(&query, stats, &params, k, filter))
                            .unwrap(),
                        block_on(direct.search(&query, stats, &params, k, filter)).unwrap(),
                    ] {
                        assert_eq!(
                            actual
                                .iter()
                                .map(|h| (h.row, h.score.to_bits()))
                                .collect::<Vec<_>>(),
                            expected
                                .iter()
                                .map(|h| (h.row, h.score.to_bits()))
                                .collect::<Vec<_>>()
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn selective_masks_score_allowed_rows_with_the_same_bits_as_the_unfiltered_ranking() {
    let params = FtsParams::default();
    let docs: Vec<_> = (0..9001)
        .map(|row| {
            let count = match row % 13 {
                0 => 0,
                1 => 254,
                2 => 255,
                3 => 300,
                _ => row % 5 + 1,
            };
            tokenize(
                &format!(
                    "{} rare{} tail{} mid{}",
                    "common ".repeat(count),
                    row % 997,
                    row % 17,
                    row % 61
                ),
                &params,
            )
        })
        .collect();
    let bytes = build(&docs, true, params.tokenizer);
    let reader = open(bytes.clone());
    let stats = block_on(reader.stats()).unwrap();
    let mut changed = stats.clone();
    changed.average_doc_length *= 1.5;
    let n = docs.len() as u32;
    let random: RoaringBitmap = (0..n)
        .filter(|r| r.wrapping_mul(2_654_435_761) % 200 == 7)
        .collect();
    let contiguous: RoaringBitmap = (4000..4000 + n / 100).collect();
    let mut outside = random.clone();
    outside.insert_range(n..n + 50);
    let masks = [
        random,
        contiguous,
        outside,
        RoaringBitmap::new(),
        (0..3).collect(),
        (0..n).filter(|r| r % 9 == 0).collect(),
        (0..n).filter(|r| r % 5 == 0).collect(),
    ];
    // The second pass runs after dense arrays are earned.
    for pass in 0..2 {
        for stats in [&stats, &changed] {
            for text in [
                "common",
                "common common rare1",
                "rare1 tail1",
                "mid3 common tail1",
                "missing rare5",
            ] {
                let query = Query::new(tokenize(text, &params), false);
                let everything =
                    block_on(reader.search(&query, stats, &params, n as usize, None)).unwrap();
                for mask in &masks {
                    for k in [1, 10, 130] {
                        let expected: Vec<_> = everything
                            .iter()
                            .filter(|h| mask.contains(h.row))
                            .take(k)
                            .map(|h| (h.row, h.score.to_bits()))
                            .collect();
                        for hits in [
                            block_on(reader.search(&query, stats, &params, k, Some(mask))).unwrap(),
                            block_on(reader.search_prefetched(
                                &query,
                                stats,
                                &params,
                                k,
                                Some(mask),
                            ))
                            .unwrap(),
                        ] {
                            let actual: Vec<_> =
                                hits.iter().map(|h| (h.row, h.score.to_bits())).collect();
                            assert_eq!(actual, expected, "pass {pass} {text:?} k={k}");
                        }
                    }
                }
            }
        }
        if pass == 0 {
            assert!(reader.dense_arrays().0 > 0, "the first pass earns arrays");
        }
    }
}

/// Round BW21 tried seeding window scores from the small terms' partial sums and skipping
/// small-term postings below a shared floor, and measured neither worth keeping. This test
/// stays as the exactness guard they needed: terms add in dictionary order and float addition
/// is not associative, so every mix of small and dense terms (small first, interleaved, dense
/// first) must match the reference scorer bit for bit, alone and under a shared floor, including
/// after the dense arrays are earned on a second use.
#[test]
fn mixed_small_and_dense_terms_match_the_reference_bit_for_bit_alone_and_under_a_shared_floor() {
    let params = FtsParams::default();
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let docs: Vec<_> = (0..40_000u64)
        .map(|_| {
            let mut text = String::new();
            let mut add = |word: &str, n: u64| {
                for _ in 0..n {
                    text.push_str(word);
                    text.push(' ');
                }
            };
            if next() % 100 == 0 {
                add("alpha", 1 + next() % 4);
            }
            if next() % 150 == 0 {
                add("zebra", 1 + next() % 3);
            }
            if next() % 3 != 0 {
                add("mango", 1 + next() % 6);
            }
            if next() % 6 == 0 {
                add("kiwi", 1 + next() % 2);
            }
            add("filler", 1 + next() % 40);
            tokenize(&text, &params)
        })
        .collect();
    let reader = open(build(&docs, false, params.tokenizer));
    let stats = CorpusStats::from_documents(&docs);
    for text in [
        "alpha mango",
        "alpha zebra mango",
        "alpha mango zebra",
        "alpha zebra kiwi mango",
        "mango zebra",
        "kiwi mango alpha zebra",
        "alpha alpha kiwi",
        "alpha mango zebra filler",
    ] {
        let query = Query::new(tokenize(text, &params), false);
        for k in [10, 100] {
            for _ in 0..4 {
                let hits =
                    block_on(reader.search_prefetched(&query, &stats, &params, k, None)).unwrap();
                assert_eq!(
                    hits,
                    expected(&docs, &query, &stats, &params, k, None),
                    "{text}"
                );
            }
        }
    }

    // Segments searched one after another under a shared floor: the merged top k is exact in any order.
    let parts: Vec<_> = docs
        .chunks(10_000)
        .map(|part| open(build(part, false, params.tokenizer)))
        .collect();
    for text in [
        "alpha mango",
        "alpha zebra kiwi mango",
        "zebra kiwi",
        "alpha mango zebra",
    ] {
        let query = Query::new(tokenize(text, &params), false);
        for order in [[0, 1, 2, 3], [3, 2, 1, 0], [2, 0, 3, 1], [1, 3, 0, 2]] {
            for _ in 0..3 {
                let threshold = SearchThreshold::new(10);
                let mut hits = Vec::new();
                for part in order {
                    let found = block_on(
                        parts[part]
                            .search_prefetched_shared(&query, &stats, &params, None, &threshold),
                    )
                    .unwrap();
                    hits.extend(found.into_iter().map(|mut hit| {
                        hit.row += part as u32 * 10_000;
                        hit
                    }));
                }
                hits.sort_unstable_by(|a, b| b.cmp(a));
                hits.truncate(10);
                assert_eq!(
                    hits,
                    expected(&docs, &query, &stats, &params, 10, None),
                    "{text}"
                );
            }
        }
    }
}
