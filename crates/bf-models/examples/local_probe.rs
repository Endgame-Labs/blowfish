//! Smoke-test local GGUF models: `cargo run --release --features local-models --example local_probe -- EMBED.gguf RERANK.gguf`
use bf_models::{InputKind, Models};
use serde_json::json;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let models = Models::new(
        serde_json::from_value(json!({
            "emb": {"provider":"local","task":"embedding","path":args[1],"dims":768,"allowed_dims":[128,256,512],
                    "max_input_tokens":2048,"query_prefix":"task: search result | query: ","document_prefix":"title: none | text: "},
            "rr": {"provider":"local","task":"rerank","path":args[2],"max_input_tokens":2048},
        }))
        .unwrap(),
    )
    .unwrap();
    let docs: Vec<String> = [
        "The Danube is a river in Central Europe, the second longest in the continent.",
        "Mount Everest is the highest mountain above sea level, in the Himalayas.",
        "The quick brown fox jumps over the lazy dog.",
        "Rhine river flows through Switzerland, Germany, and the Netherlands.",
    ]
    .map(String::from)
    .to_vec();
    let t = Instant::now();
    let d = models
        .embed("emb", &docs, InputKind::Document, None)
        .await
        .unwrap();
    println!(
        "embed 4 docs (incl. load): {:?} tokens={}",
        t.elapsed(),
        d.tokens
    );
    let t = Instant::now();
    let q = models
        .embed(
            "emb",
            &["a famous river in europe".into()],
            InputKind::Query,
            None,
        )
        .await
        .unwrap();
    println!("embed query: {:?}", t.elapsed());
    for (i, v) in d.vectors.iter().enumerate() {
        let dot: f32 = v.iter().zip(&q.vectors[0]).map(|(a, b)| a * b).sum();
        println!("  cos(q, doc{i}) = {dot:.4}  (len {})", v.len());
    }
    let q128 = models
        .embed(
            "emb",
            &["a famous river in europe".into()],
            InputKind::Query,
            Some(128),
        )
        .await
        .unwrap();
    println!(
        "128-d norm {}",
        q128.vectors[0].iter().map(|x| x * x).sum::<f32>().sqrt()
    );
    let t = Instant::now();
    let r = models
        .rerank("rr", "a famous river in europe", &docs)
        .await
        .unwrap();
    println!("rerank 4 docs (incl. load): {:?}", t.elapsed());
    println!("scores {:?}", r.scores);
}
