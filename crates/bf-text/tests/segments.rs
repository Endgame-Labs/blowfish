use bf_text::{
    CorpusStats, FtsParams, MemoryRangeRead, Query, RangeRead, SearchHit, SegmentBuilder,
    SegmentReader, Token, Tokenizer, matches_all_tokens, matches_any_token, matches_token_sequence,
    score_document, tokenize, tokenize_array,
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
        assert_eq!(reader.stats(), CorpusStats::from_documents(&docs));
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
            block_on(reader.search(&empty, &reader.stats(), &FtsParams::default(), 10, None))
                .unwrap()
                .is_empty()
        );
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
    reads.lock().unwrap().clear();
    let q = Query::new(tokenize("fox", &p), false);
    block_on(reader.search(&q, &reader.stats(), &p, 10, None)).unwrap();
    assert_eq!(reads.lock().unwrap().len(), 1);
    // One-bit docs plus two-bit frequency in a 128-value block, plus two header bytes.
    let range = reads.lock().unwrap()[0].clone();
    assert!(range.end - range.start <= 514);
    reads.lock().unwrap().clear();
    block_on(reader.contains_token_sequence(&q)).unwrap();
    assert!(reads.lock().unwrap().len() >= 2);
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
    let stats = reader.stats();
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
    corrupt[0] ^= 1;
    let reader = open(corrupt);
    let q = Query::new(tokenize("dog fox", &p), false);
    assert!(block_on(reader.search(&q, &reader.stats(), &p, 10, None)).is_err());
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
        let mut stats=reader.stats();stats.merge(&CorpusStats::from_documents(&[vec![Token::new("term1",0),Token::new("external",1)]]));
        for allowed in [None,Some(&allowed)] {
            let actual=block_on(reader.search(&query,&stats,&params,k,allowed)).unwrap();
            assert_hits(&actual,&expected(&docs,&query,&stats,&params,k,allowed));
        }
        let all:RoaringBitmap=docs.iter().enumerate().filter(|(_,d)|matches_all_tokens(d,&query,params.tokenizer)).map(|(i,_)|i as u32).collect();
        prop_assert_eq!(block_on(reader.contains_all_tokens(&query)).unwrap(),all);
        if !prefix {
            let phrase:RoaringBitmap=docs.iter().enumerate().filter(|(_,d)|matches_token_sequence(d,&query)).map(|(i,_)|i as u32).collect();
            prop_assert_eq!(block_on(reader.contains_token_sequence(&query)).unwrap(),phrase);
        }
    }
}
