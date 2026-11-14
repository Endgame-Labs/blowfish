use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::QueryRequest;
use serde_json::json;

#[tokio::test]
async fn streaming_blocks_groups_diversification_and_tail_match_reference() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            compaction_segments: 100,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    // Overlapping segments, duplicate array values, missing fields, and keys arriving in reverse order.
    for batch in 0..3 {
        let docs: Vec<_> = (0..700).map(|i| {
            let id = batch * 600 + i;
            let mut row = json!({"id":id,"n":2000-id,"bucket":id%13,"tags":[format!("t{}",id%17),format!("t{}",id%17),format!("t{}",id%7)],"value":id as i64-900,"wide":9007199254740993u64});
            if id % 11 == 0 { row.as_object_mut().unwrap().remove("value"); }
            row
        }).collect();
        engine
            .write(
                "stream",
                serde_json::from_value(json!({"upsert_rows":docs})).unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("stream", true).await.unwrap();
    }
    engine.write("stream", serde_json::from_value(json!({"deletes":[1,7,1000],"patch_rows":[{"id":5,"n":-10,"bucket":0}],"upsert_rows":[{"id":5000,"n":-20,"bucket":1,"value":-25,"tags":[]}]})).unwrap()).await.unwrap();
    for body in [
        json!({"queries":[{"rank_by":["n","asc"],"top_k":20},{"rank_by":["id","desc"],"top_k":20}],"rerank_by":["RRF",{"rank_constant":1,"weights":[2,1]}],"limit":7}),
        json!({"rank_by":[["n","asc"],["id","desc"]],"top_k":17,"offset":9,"include_attributes":true}),
        json!({"rank_by":[["value","desc"],["id","asc"]],"limit":{"total":19,"per":{"attributes":["bucket"],"limit":2}},"offset":4,"include_attributes":true}),
        json!({"rank_by":["id","desc"],"top_k":20,"filters":["value","Lt",10]}),
        json!({"aggregate_by":{"n":["Count"],"present":["Count","value"],"sum":["Sum","value"],"min":["Min","value"],"max":["Max","wide"]}}),
        json!({"aggregate_by":{"n":["Count"],"sum":["Sum","value"]},"group_by":["bucket"],"top_k":3}),
        json!({"aggregate_by":{"n":["Count"],"sum":["Sum","value"]},"group_by":[{"tag":["ForEachUnique","tags"]},"bucket"],"top_k":7}),
        json!({"aggregate_by":{"n":["Count"],"sum":["Sum","value"],"min":["Min","value"]},"filters":["id","Lt",0]}),
        json!({"queries":[{"rank_by":["n","asc"],"top_k":5},{"aggregate_by":{"n":["Count"]},"group_by":["bucket"],"top_k":4}]}),
    ] {
        let q: QueryRequest = serde_json::from_value(body.clone()).unwrap();
        let actual = engine.query("stream", q.clone()).await.unwrap();
        let expected = engine.query_exhaustive("stream", q).await.unwrap();
        assert_eq!(
            serde_json::to_value(actual.result).unwrap(),
            serde_json::to_value(expected.result).unwrap(),
            "{body}"
        );
        assert_eq!(
            serde_json::to_value(actual.results).unwrap(),
            serde_json::to_value(expected.results).unwrap(),
            "{body}"
        );
    }
}
