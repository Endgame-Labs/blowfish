//! Time local embedding calls: `local_bench EMBED.gguf DOCS.json [max_input_tokens]`
use bf_models::{InputKind, Models};
use serde_json::json;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let docs: Vec<serde_json::Value> =
        serde_json::from_str(&std::fs::read_to_string(&args[2]).unwrap()).unwrap();
    let docs: Vec<String> = docs
        .iter()
        .map(|d| d["text"].as_str().unwrap().to_string())
        .collect();
    let max: usize = args.get(3).map_or(2048, |s| s.parse().unwrap());
    let models = Models::new(
        serde_json::from_value(json!({
            "emb": {"provider":"local","task":"embedding","path":args[1],"dims":768,"max_input_tokens":max,
                    "document_prefix":"title: none | text: ", "query_prefix":"task: search result | query: "},
        }))
        .unwrap(),
    )
    .unwrap();
    models
        .embed("emb", &["warm".into()], InputKind::Document, None)
        .await
        .unwrap();
    for n in [1, 4, 16, 32] {
        let t = Instant::now();
        let out = models
            .embed("emb", &docs[..n], InputKind::Document, None)
            .await
            .unwrap();
        println!("{n} docs: {:?} tokens={}", t.elapsed(), out.tokens);
    }
    let t = Instant::now();
    models
        .embed("emb", &["a famous river".into()], InputKind::Query, None)
        .await
        .unwrap();
    println!("query: {:?}", t.elapsed());
}
