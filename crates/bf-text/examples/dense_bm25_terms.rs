//! Select dense terms using training-query frequency times corpus document frequency.
//! Usage: dense_bm25_terms TRAINING_WORKLOAD SEGMENT [SEGMENT ...]
use bf_text::{FtsParams, MemoryRangeRead, SegmentReader, tokenize};
use futures::executor::block_on;
use std::{collections::BTreeMap, sync::Arc};
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(args.len() >= 3);
    let workload: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(&args[1]).unwrap()).unwrap();
    let mut frequencies = BTreeMap::<String, u64>::new();
    for item in workload {
        let terms: std::collections::BTreeSet<_> = tokenize(
            item["request"]["rank_by"][2].as_str().unwrap(),
            &FtsParams::default(),
        )
        .into_iter()
        .map(|t| t.text)
        .collect();
        for term in terms {
            *frequencies.entry(term).or_default() += 1;
        }
    }
    let terms: Vec<_> = frequencies.keys().map(String::as_str).collect();
    let mut dfs = vec![0; terms.len()];
    for path in &args[2..] {
        let reader = block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
            std::fs::read(path).unwrap(),
        ))))
        .unwrap();
        let counts =
            block_on(reader.document_frequencies(&terms, &(0..reader.num_docs()).collect()))
                .unwrap();
        for (df, count) in dfs.iter_mut().zip(counts) {
            *df += count;
        }
    }
    let mut ranked: Vec<_> = terms
        .into_iter()
        .zip(dfs)
        .map(|(t, df)| (t, frequencies[t] * df))
        .collect();
    ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    println!(
        "{}",
        ranked
            .iter()
            .take(32)
            .map(|(t, _)| *t)
            .collect::<Vec<_>>()
            .join(",")
    );
    for (term, cost) in ranked.into_iter().take(32) {
        eprintln!("{term}\t{cost}");
    }
}
