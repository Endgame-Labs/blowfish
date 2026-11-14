use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use bf_wal::Wal;
use serde_json::{Value, json};
use std::time::Duration;

async fn node() -> Engine {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            retention: Duration::ZERO,
            compaction_segments: 3,
            ..Default::default()
        },
    )
}
async fn write(e: &Engine, body: Value) {
    e.write(
        "test",
        serde_json::from_value::<WriteRequest>(body).unwrap(),
    )
    .await
    .unwrap();
}
async fn query(e: &Engine, body: Value) -> Value {
    let r = e
        .query(
            "test",
            serde_json::from_value::<QueryRequest>(body).unwrap(),
        )
        .await
        .unwrap();
    let mut value = serde_json::to_value(r).unwrap();
    value.as_object_mut().unwrap().remove("performance");
    value
}
fn initial() -> Value {
    json!({"distance_metric":"cosine_distance","schema":{"text":{"type":"string","full_text_search":true},"small":"[2]f16"},
        "upsert_rows":(0..30).map(|id|json!({"id":id,"n":id%7,"text":if id%2==0 {"red fox"}else{"blue bird"},"vector":[id as f32+1.0,1.0],"small":[1.0,0.3333],"tags":["a","b"]})).collect::<Vec<_>>()})
}
#[tokio::test]
async fn checkpoints_overlay_compaction_gc_and_cold_reopen_match_reference() {
    let indexed = node().await;
    let reference = node().await;
    write(&indexed, initial()).await;
    write(&reference, initial()).await;
    assert!(indexed.index_namespace("test", true).await.unwrap());
    let mut seed = 17u64;
    for round in 0..36 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = seed % 30;
        let mutation = match round % 6 {
            0 => json!({"patch_rows":[{"id":id,"n":round,"text":"red bird bird"}]}),
            1 => json!({"deletes":[id]}),
            2 => {
                json!({"upsert_rows":[{"id":id,"n":round,"text":"green fox","vector":[2.0,3.0],"small":[0.4,0.1],"tags":["c"]}]})
            }
            3 => json!({"patch_by_filter":{"filters":["n","Eq",3],"patch":{"text":"blue fox"}}}),
            4 => json!({"delete_by_filter":["n","Eq",2]}),
            _ => json!({"patch_rows":[{"id":id,"n":1}],"patch_condition":["n","Lt",20]}),
        };
        write(&indexed, mutation.clone()).await;
        write(&reference, mutation).await;
        if round % 3 == 0 {
            indexed.index_namespace("test", true).await.unwrap();
        }
        for q in [
            json!({"rank_by":["id","asc"],"top_k":100,"include_attributes":true}),
            json!({"rank_by":[["n","desc"],["id","asc"]],"top_k":10,"filters":["Or",[["n","Lt",5],["tags","Contains","c"]]]}),
            json!({"rank_by":["vector","ANN",[1.0,2.0]],"top_k":10}),
            json!({"rank_by":["small","kNN",[0.1,0.2]],"top_k":10,"filters":["id","Gt",0]}),
            json!({"rank_by":["text","BM25","fox bird"],"top_k":10}),
            json!({"rank_by":["Sum",[["text","BM25","bird"],["Max",[["Attribute","n"],0]]]],"top_k":10}),
            json!({"aggregate_by":{"count":["Count"],"sum":["Sum","n"]},"group_by":["n"]}),
            json!({"aggregate_by":{"count":["Count"],"min":["Min","n"],"max":["Max","n"]}}),
            json!({"aggregate_by":{"count":["Count"]},"group_by":[{"tag":["ForEachUnique","tags"]}]}),
            json!({"rank_by":["Product",2,["text","BM25","bird"]],"top_k":10}),
            json!({"rank_by":["Max",[["text","BM25","fox"],["n","Eq",1]]],"top_k":10}),
            json!({"rank_by":["Saturate",["Attribute","n"],{"midpoint":2}],"top_k":10}),
            json!({"rank_by":["Decay",["Dist",["Attribute","n"],5],{"midpoint":2}],"top_k":10}),
            json!({"rank_by":["n","Eq",1],"top_k":10}),
            json!({"rank_by":["id","desc"],"limit":{"total":5,"per":{"attributes":["n"],"limit":1}},"offset":1}),
            json!({"rank_by":["id","asc"],"top_k":5,"exclude_attributes":["small","tags"],"vector_encoding":"base64"}),
            json!({"rank_by":["id","asc"],"top_k":10,"filters":["text","ContainsAllTokens","fox"]}),
            json!({"queries":[{"rank_by":["vector","ANN",[1.0,2.0]],"top_k":5},{"rank_by":["text","BM25","fox"],"top_k":5}],"rerank_by":["RRF"],"limit":5}),
        ] {
            assert_eq!(
                query(&indexed, q.clone()).await,
                query(&reference, q.clone()).await,
                "round {round}, query {q}"
            );
        }
    }
    indexed.index_namespace("test", true).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    indexed.collect_garbage("test").await.unwrap();
    assert!(
        indexed
            .storage()
            .list("ns/test/wal-data/")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !indexed
            .storage()
            .list("ns/test/wal/")
            .await
            .unwrap()
            .is_empty()
    );
    let cold = Engine::with_config(
        indexed.storage().clone(),
        EngineConfig {
            indexing: false,
            ..Default::default()
        },
    );
    let q = json!({"rank_by":["id","asc"],"top_k":100,"include_attributes":true});
    assert_eq!(query(&cold, q.clone()).await, query(&reference, q).await);
    let r = cold
        .query(
            "test",
            serde_json::from_value(json!({"aggregate_by":{"n":["Count"]}})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.performance.exhaustive_search_count, 0);
    let wal = Wal::new(indexed.storage().clone(), "test").unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest.stats.rows,
        r.result.aggregations.unwrap()["n"].as_u64().unwrap()
    );
}

#[tokio::test]
async fn segment_delete_recreate_and_backpressure() {
    let e = node().await;
    write(&e, initial()).await;
    e.index_namespace("test", true).await.unwrap();
    e.delete_namespace("test").await.unwrap();
    write(&e, json!({"upsert_rows":[{"id":"new","value":7}]})).await;
    e.index_namespace("test", true).await.unwrap();
    let cold = Engine::with_config(
        e.storage().clone(),
        EngineConfig {
            indexing: false,
            ..Default::default()
        },
    );
    assert_eq!(
        query(&cold, json!({"rank_by":["id","asc"],"top_k":5})).await["rows"],
        json!([{"id":"new"}])
    );
    let blocked = Engine::with_config(
        e.storage().clone(),
        EngineConfig {
            indexing: false,
            max_unindexed_bytes: 1,
            ..Default::default()
        },
    );
    blocked
        .write(
            "pressure",
            serde_json::from_value(json!({"upsert_rows":[{"id":1}]})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        blocked
            .write(
                "pressure",
                serde_json::from_value(json!({"upsert_rows":[{"id":2}]})).unwrap()
            )
            .await
            .unwrap_err()
            .status,
        429
    );
    blocked.index_namespace("pressure", true).await.unwrap();
    blocked
        .write(
            "pressure",
            serde_json::from_value(json!({"upsert_rows":[{"id":2}]})).unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn ivf_ann_recall_filtered_knn_and_f16_files() {
    let e = node().await;
    let mut seed = 42u64;
    let mut vectors = vec![];
    let mut rows = vec![];
    for id in 0..5000 {
        let vector: Vec<f32> = (0..16)
            .map(|_| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                ((seed >> 32) as u32 as f64 / u32::MAX as f64 * 2.0 - 1.0) as f32
            })
            .collect();
        rows.push(json!({"id":id,"vector":vector,"small":[0.3333,1.0],"bucket":id%3}));
        vectors.push(vector);
    }
    write(
        &e,
        json!({"distance_metric":"cosine_distance","schema":{"small":"[2]f16"},"upsert_rows":rows}),
    )
    .await;
    e.index_namespace("test", true).await.unwrap();
    let mut recall = 0.0;
    for vector in vectors.iter().step_by(251).take(20) {
        let result = e
            .recall(
                "test",
                json!({"rank_by":["vector","ANN",vector],"top_k":10,"num":1}),
            )
            .await
            .unwrap();
        recall += result["avg_recall"].as_f64().unwrap();
        let q: QueryRequest = serde_json::from_value(
            json!({"rank_by":["vector","kNN",vector],"filters":["bucket","Eq",1],"top_k":10}),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(e.query("test", q.clone()).await.unwrap().result).unwrap(),
            serde_json::to_value(e.query_exhaustive("test", q).await.unwrap().result).unwrap()
        );
    }
    assert!(recall / 20.0 >= 0.95, "mean recall@10 = {}", recall / 20.0);
    let (_, m) = Wal::new(e.storage().clone(), "test")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    let f = &m.segments[0].files["ann:small"];
    let index = bf_vector::Index::open(e.storage().get(&f.key).await.unwrap())
        .await
        .unwrap();
    assert_eq!(index.vector_type(), bf_vector::VectorType::F16);
}

#[tokio::test]
async fn text_statistics_empty_missing_prefix_positions_and_schema_rebuild() {
    let e = node().await;
    write(&e,json!({"schema":{"text":{"type":"[]string","full_text_search":{"stemming":true,"remove_stopwords":true}}},"upsert_rows":[
        {"id":1,"text":["the jumping fox","red birds"]},{"id":2,"text":[]},{"id":3},{"id":4,"text":["jump fox red"]}
    ]})).await;
    e.index_namespace("test", true).await.unwrap();
    write(
        &e,
        json!({"patch_rows":[{"id":1,"text":["red fox birds birds"]}],"deletes":[4]}),
    )
    .await;
    for indexed in [false, true] {
        if indexed {
            e.index_namespace("test", true).await.unwrap();
        }
        for q in [
            json!({"rank_by":["text","BM25","red birds"],"top_k":10}),
            json!({"rank_by":["text","BM25","red bi",{"last_as_prefix":true}],"top_k":10}),
            json!({"rank_by":["id","asc"],"filters":["text","ContainsTokenSequence","fox birds"],"top_k":10}),
            json!({"rank_by":["id","asc"],"filters":["text","ContainsAllTokens",""],"top_k":10}),
            json!({"rank_by":["Decay",["text","BM25","bird"],{"midpoint":1}],"top_k":10}),
        ] {
            let q: QueryRequest = serde_json::from_value(q).unwrap();
            assert_eq!(
                serde_json::to_value(e.query("test", q.clone()).await.unwrap().result).unwrap(),
                serde_json::to_value(e.query_exhaustive("test", q).await.unwrap().result).unwrap()
            );
        }
    }
    let (_, m) = Wal::new(e.storage().clone(), "test")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.stats.text["text"].documents, 2);
    assert_eq!(m.stats.text["text"].total_length, 5);
    write(
        &e,
        json!({"schema":{"text":{"full_text_search":{"tokenizer":"pre_tokenized_array"}}}}),
    )
    .await;
    let q: QueryRequest = serde_json::from_value(
        json!({"rank_by":["text","BM25",["red fox birds birds"]],"top_k":10}),
    )
    .unwrap();
    let before = serde_json::to_value(e.query("test", q.clone()).await.unwrap().result).unwrap();
    e.index_namespace("test", true).await.unwrap();
    assert_eq!(
        before,
        serde_json::to_value(e.query("test", q.clone()).await.unwrap().result).unwrap()
    );
    assert_eq!(
        before,
        serde_json::to_value(e.query_exhaustive("test", q).await.unwrap().result).unwrap()
    );
    let metadata = e.metadata("test").await.unwrap();
    assert!(metadata["schema"]["text"]["full_text_search"]["language"].is_null());
    assert!(metadata["schema"]["text"]["full_text_search"]["max_token_length"].is_null());
}

#[tokio::test]
async fn cjk_tokenizers_and_pre_tokenized_phrase_filters_use_indexes() {
    for (tokenizer, text, term) in [
        ("smartcn", "我是中国人", "中国"),
        ("kuromoji", "東京の猫", "東京"),
        ("nori", "한국 사람", "한국"),
    ] {
        let e = node().await;
        write(&e,json!({"schema":{"text":{"type":"string","full_text_search":{"tokenizer":tokenizer}}},"upsert_rows":[{"id":1,"text":text}]})).await;
        e.index_namespace("test", true).await.unwrap();
        let q: QueryRequest =
            serde_json::from_value(json!({"rank_by":["text","BM25",term],"top_k":10})).unwrap();
        assert_eq!(
            serde_json::to_value(e.query("test", q.clone()).await.unwrap().result).unwrap(),
            serde_json::to_value(e.query_exhaustive("test", q).await.unwrap().result).unwrap()
        );
    }
    let e = node().await;
    write(&e,json!({"schema":{"text":{"type":"[]string","full_text_search":{"tokenizer":"pre_tokenized_array"}}},"upsert_rows":[{"id":1,"text":["A","B","C"]}]})).await;
    e.index_namespace("test", true).await.unwrap();
    assert_eq!(query(&e,json!({"rank_by":["id","asc"],"top_k":5,"filters":["text","ContainsTokenSequence",["B","C"]]})).await["rows"],json!([{"id":1}]));
}

#[tokio::test]
async fn gc_fences_orphan_builders_and_erases_deleted_namespace_segments() {
    let e = node().await;
    write(&e, initial()).await;
    e.index_namespace("test", true).await.unwrap();
    let wal = Wal::new(e.storage().clone(), "test").unwrap();
    let (sequence, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let orphan = "ns/test/segments/abandoned/docs.bfc";
    e.storage().create(orphan, "unpublished").await.unwrap();
    e.collect_garbage("test").await.unwrap();
    assert!(matches!(
        e.storage().get(orphan).await,
        Err(bf_storage::Error::NotFound)
    ));
    assert!(matches!(
        wal.publish_fenced(sequence, manifest.indexer_epoch, &manifest)
            .await,
        Err(bf_wal::Error::Fenced)
    ));
    e.delete_namespace("test").await.unwrap();
    e.collect_garbage("test").await.unwrap();
    e.collect_garbage("test").await.unwrap();
    assert!(
        e.storage()
            .list("ns/test/segments/")
            .await
            .unwrap()
            .is_empty()
    );
    let cold = Engine::with_config(
        e.storage().clone(),
        EngineConfig {
            indexing: false,
            ..Default::default()
        },
    );
    assert_eq!(cold.schema("test").await.unwrap_err().status, 404);
    write(&cold, json!({"upsert_rows":[{"id":"fresh","n":9}]})).await;
    cold.index_namespace("test", true).await.unwrap();
    assert_eq!(
        query(&cold, json!({"rank_by":["id","asc"],"top_k":5})).await["rows"],
        json!([{"id":"fresh"}])
    );
}

#[tokio::test]
async fn eventual_tail_cap_and_segment_checksum_fail_closed() {
    let e = node().await;
    write(&e, initial()).await;
    let bounded = Engine::with_config(
        e.storage().clone(),
        EngineConfig {
            indexing: false,
            eventual_tail_bytes: 1,
            ..Default::default()
        },
    );
    let q: QueryRequest = serde_json::from_value(
        json!({"rank_by":["id","asc"],"top_k":100,"consistency":{"level":"eventual"}}),
    )
    .unwrap();
    assert_eq!(
        bounded.query("test", q.clone()).await.unwrap_err().status,
        409
    );
    e.index_namespace("test", true).await.unwrap();
    assert_eq!(
        bounded
            .query("test", q.clone())
            .await
            .unwrap()
            .result
            .rows
            .unwrap()
            .len(),
        30
    );
    write(&e, json!({"deletes":[1]})).await;
    assert_eq!(
        bounded
            .query("test", q)
            .await
            .unwrap()
            .result
            .rows
            .unwrap()
            .len(),
        30
    );
    assert_eq!(
        query(&bounded, json!({"aggregate_by":{"n":["Count"]}})).await["aggregations"]["n"],
        29
    );
    let (_, m) = Wal::new(e.storage().clone(), "test")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    let file = &m.segments[0].files["docs"];
    let mut bytes = e.storage().get(&file.key).await.unwrap().to_vec();
    bytes[0] ^= 1;
    e.storage().put(&file.key, bytes).await.unwrap();
    let q: QueryRequest = serde_json::from_value(
        json!({"rank_by":["id","asc"],"top_k":100,"include_attributes":true}),
    )
    .unwrap();
    assert!(bounded.query("test", q).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_conditional_writes_remain_serializable_across_nodes_and_compaction() {
    let e = node().await;
    write(&e, initial()).await;
    e.index_namespace("test", true).await.unwrap();
    let mut workers = vec![];
    for _ in 0..2 {
        let node = Engine::with_config(
            e.storage().clone(),
            EngineConfig {
                indexing: false,
                ..Default::default()
            },
        );
        workers.push(tokio::spawn(async move {
            for _ in 0..20 {
                loop {
                    let row=query(&node,json!({"rank_by":["id","asc"],"filters":["id","Eq",0],"top_k":1,"include_attributes":["n"]})).await;
                    let n=row["rows"][0]["n"].as_u64().unwrap();
                    let result=node.write("test",serde_json::from_value(json!({"patch_rows":[{"id":0,"n":n+1}],"patch_condition":["n","Eq",n]})).unwrap()).await.unwrap();
                    if result.rows_affected==1 {break;}
                }
            }
        }));
    }
    let indexer = e.clone();
    let maintenance = tokio::spawn(async move {
        for _ in 0..12 {
            indexer.index_namespace("test", true).await.unwrap();
            tokio::task::yield_now().await;
        }
    });
    for worker in workers {
        worker.await.unwrap();
    }
    maintenance.await.unwrap();
    e.index_namespace("test", true).await.unwrap();
    assert_eq!(query(&e,json!({"rank_by":["id","asc"],"filters":["id","Eq",0],"top_k":1,"include_attributes":["n"]})).await["rows"][0]["n"],40);
}

#[tokio::test]
async fn schema_drops_do_not_copy_surviving_vectors_into_the_wal() {
    let e = node().await;
    write(&e, initial()).await;
    e.index_namespace("test", true).await.unwrap();
    write(&e, json!({"schema":{"tags":{"drop":true}}})).await;
    let wal = Wal::new(e.storage().clone(), "test").unwrap();
    let entry = wal.read(wal.head().await.unwrap()).await.unwrap().unwrap();
    assert!(entry.batches[0].upserts.is_empty());
    for indexed in [false, true] {
        if indexed {
            e.index_namespace("test", true).await.unwrap();
        }
        let result = query(
            &e,
            json!({"rank_by":["id","asc"],"top_k":100,"include_attributes":true}),
        )
        .await;
        assert_eq!(result["rows"].as_array().unwrap().len(), 30);
        assert!(
            result["rows"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row.get("tags").is_none())
        );
    }
    write(&e, json!({"patch_rows":[{"id":0,"n":99}]})).await;
    let result = query(
        &e,
        json!({"rank_by":["vector","ANN",[1.0,2.0]],"top_k":30,"include_attributes":true}),
    )
    .await;
    assert!(
        result["rows"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row.get("tags").is_none())
    );
}
