//! Exhaustive BM25 top-10 over a corpus, as the offline truth for overlap@10 measurements.
//!
//! Reads `{"docid": "...", "text": "..."}` JSON lines on stdin (the first N MS MARCO passages), tokenizes
//! with the default full-text settings (word_v4, no stemming, no stopword removal; k1 1.2, b 0.75, k3 8),
//! computes namespace-wide statistics, and scores every document that shares a term with each query.
//! Usage: curl ... corpus.jsonl.gz | zcat | head -n 1000000 | exact_bm25 workload.json > truth.json
//! The workload is the JSON array written by `apples_workloads.py` (`request.rank_by[2]` is the query).
//! Scores use the engine's formula in f64 and are rounded to f32 like the engine's.
use bf_text::{FtsParams, tokenize};
use std::collections::{BTreeMap, HashMap};
use std::io::BufRead;

fn main() {
    let params = FtsParams::default();
    let workload: Vec<serde_json::Value> = serde_json::from_slice(
        &std::fs::read(std::env::args().nth(1).expect("workload.json")).unwrap(),
    )
    .unwrap();
    let queries: Vec<(u64, BTreeMap<String, u32>)> = workload
        .iter()
        .map(|w| {
            let mut terms = BTreeMap::new();
            for t in tokenize(w["request"]["rank_by"][2].as_str().unwrap(), &params) {
                *terms.entry(t.text).or_default() += 1u32;
            }
            (w["query_id"].as_u64().unwrap(), terms)
        })
        .collect();
    let wanted: std::collections::HashSet<&str> = queries
        .iter()
        .flat_map(|(_, t)| t.keys().map(String::as_str))
        .collect();
    // Postings only for the query terms (everything else only needs a count for df).
    let mut postings: HashMap<String, Vec<(u32, u32)>> = HashMap::new();
    let mut ids: Vec<u64> = vec![];
    let mut lengths: Vec<u32> = vec![];
    for line in std::io::stdin().lock().lines() {
        let d: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let row = ids.len() as u32;
        ids.push(d["docid"].as_str().unwrap().parse().unwrap());
        let tokens = tokenize(d["text"].as_str().unwrap(), &params);
        lengths.push(tokens.len().max(1) as u32);
        let mut tf: HashMap<&str, u32> = HashMap::new();
        for t in &tokens {
            if wanted.contains(t.text.as_str()) {
                *tf.entry(t.text.as_str()).or_default() += 1;
            }
        }
        for (t, n) in tf {
            postings.entry(t.to_owned()).or_default().push((row, n));
        }
    }
    let n = ids.len() as f64;
    let avg = lengths.iter().map(|&l| l as f64).sum::<f64>() / n;
    let (k1, b, k3) = (params.k1 as f64, params.b as f64, params.k3 as f64);
    let mut out = vec![];
    for (qid, terms) in &queries {
        let mut scores: HashMap<u32, f32> = HashMap::new();
        for (t, &qf) in terms {
            let Some(list) = postings.get(t) else {
                continue;
            };
            let df = list.len() as f64;
            let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
            for &(row, tf) in list {
                let tf = tf as f64;
                let norm = k1 * (1.0 - b + b * lengths[row as usize] as f64 / avg);
                let s = (idf
                    * (tf * (k1 + 1.0) / (tf + norm))
                    * (qf as f64 * (k3 + 1.0) / (qf as f64 + k3))) as f32;
                *scores.entry(row).or_default() += s;
            }
        }
        let mut ranked: Vec<_> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        ranked.truncate(10);
        out.push(serde_json::json!({"query_id": qid,
            "ids": ranked.iter().map(|r| ids[r.0 as usize]).collect::<Vec<_>>(),
            "scores": ranked.iter().map(|r| r.1).collect::<Vec<_>>()}));
    }
    println!("{}", serde_json::to_string(&out).unwrap());
    eprintln!("{} docs, {} queries", ids.len(), out.len());
}
