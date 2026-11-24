//! Opt-in BW1 validation against the independent, exhaustive MS MARCO scorer.
use super::*;
use std::io::Write;
use std::process::{Command, Stdio};

/// Point BW1_STORE at a disposable copy of the million-passage local store,
/// BW1_WORKLOAD at the checked-in workload, and BW1_REFERENCE at exact_bm25.
/// All paths must be inside this checkout's scratch directory, except the workload.
#[tokio::test]
#[ignore = "BW1 million-row serialized snapshot validation; requires BW1_STORE and BW1_REFERENCE"]
async fn bm25_serialized_msmarco_snapshots() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let path = std::path::PathBuf::from(std::env::var_os("BW1_STORE").expect("BW1_STORE"))
        .canonicalize()
        .unwrap();
    assert!(path.starts_with(root.join(".scratch")));
    let reference =
        std::path::PathBuf::from(std::env::var_os("BW1_REFERENCE").expect("BW1_REFERENCE"))
            .canonicalize()
            .unwrap();
    assert!(reference.starts_with(root.join(".scratch")));
    let workload = root.join("bench/scale/data/bm25-msmarco.json");
    let queries: Vec<Value> = serde_json::from_slice(&std::fs::read(&workload).unwrap()).unwrap();
    let store = Storage::open(&bf_storage::StorageConfig {
        url: format!("file://{}", path.display()),
        ..Default::default()
    })
    .await
    .unwrap();
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        ..Default::default()
    };
    let engine = Engine::with_config(store.clone(), config.clone());
    let context = read_cache::ReadContext::new(Arc::new(read_cache::DecodedCache::new(1 << 30, 1)));
    read_cache::scope(context,async {
        // Loading metadata can precede the final checkpoint. Drain the initial
        // load explicitly before disabling background work for captured snapshots.
        while engine.index_namespace("msmarco",true).await.unwrap() {}
        let mut pending=Vec::new();
        for phase in 0..4 {
            if phase==1 {
                for batch in 0..50 {
                    let rows:Vec<_>=(batch*100..(batch+1)*100).map(|id|json!({"id":id,"text":format!("updated passage red fox update{id} repeated repeated")})).collect();
                    engine.write("msmarco",serde_json::from_value(json!({"upsert_rows":rows})).unwrap()).await.unwrap();
                }
            } else if phase==2 {
                let rows:Vec<_>=(0..500).map(|id|json!({"id":id,"text":if id%7==0 {Value::Null}else{json!(format!("new updated passage {id}"))}})).chain((1_000_000..1_000_100).map(|id|json!({"id":id,"text":"red fox new passage"}))).collect();
                engine.write("msmarco",serde_json::from_value(json!({"upsert_rows":rows,"deletes":(500..800).collect::<Vec<_>>()})).unwrap()).await.unwrap();
            } else if phase==3 {engine.index_namespace("msmarco",true).await.unwrap();}
            let (state,_)=engine.snapshot("msmarco",&Consistency::Strong).await.unwrap();
            let mut child=Command::new(&reference).arg(&workload).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
            {
                let full=snapshot::materialize(&store,&state).await.unwrap();
                let mut input=std::io::BufWriter::new(child.stdin.take().unwrap());
                for (id,doc) in &full.docs {
                    // The independent reference counts every input field as present.
                    // Missing/null text must therefore be omitted from its corpus.
                    let Some(text)=doc.get("text").filter(|v|!v.is_null()) else {continue;};
                    let Id::Uint(id)=id else {panic!("MS MARCO uses numeric IDs")};
                    serde_json::to_writer(&mut input,&json!({"docid":id.to_string(),"text":text})).unwrap();
                    writeln!(input).unwrap();
                }
            }
            pending.push((phase,state,child));
        }
        // Reference passes are independent CPU work, outside every timed run.
        // Retaining all snapshots also checks cache reuse across later publications.
        for (phase,state,child) in pending {
            let output=child.wait_with_output().unwrap();assert!(output.status.success());
            std::fs::write(path.parent().unwrap().join(format!("bm25-truth-phase-{phase}.json")), &output.stdout).unwrap();
            let truth:Vec<Value>=serde_json::from_slice(&output.stdout).unwrap();
            for (item,expected) in queries.iter().zip(&truth) {
                let mut request=item["request"].clone();request["include_attributes"]=json!(["id"]);
                let request:QueryRequest=serde_json::from_value(request).unwrap();
                let actual=indexed::execute(&store,&state,&request,&config,false).await.unwrap();
                let actual=serde_json::to_value(actual.result).unwrap();
                let rows=actual["rows"].as_array().unwrap();
                assert_eq!(rows.len(),expected["ids"].as_array().unwrap().len());
                for (rank,row) in rows.iter().enumerate() {
                    assert_eq!(row["id"],expected["ids"][rank],"phase={phase} query={} rank={rank}",item["query_id"]);
                    assert_eq!((row["$dist"].as_f64().unwrap() as f32).to_bits(),(expected["scores"][rank].as_f64().unwrap() as f32).to_bits(),"phase={phase} query={} rank={rank}",item["query_id"]);
                }
            }
            eprintln!("BW1 exact phase={phase} seq={} queries={} tail={} shadowed={}",state.seq,queries.len(),state.docs.len(),state.shadowed.len());
        }
    }).await;
}
