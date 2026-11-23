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
async fn filtered_ann_skips_unused_columns_and_preserves_rrf_clause_projection() {
    async fn uncached_node() -> Engine {
        let mut config = StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        };
        config.cache.memory_bytes = 0;
        config.cache.disk_bytes = 0;
        config.cache.block_size = 4096;
        Engine::with_config(
            Storage::open(&config).await.unwrap(),
            EngineConfig {
                indexing: false,
                decoded_cache_bytes: 0,
                ..Default::default()
            },
        )
    }
    let indexed = uncached_node().await;
    let reference = uncached_node().await;
    let mut seed = 17u64;
    let rows: Vec<_> = (1..=64)
        .map(|id| {
            let padding: String = (0..65536)
                .map(|_| {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (b'a' + ((seed >> 32) % 26) as u8) as char
                })
                .collect();
            json!({"id":id,"n":id,"tag":if id%2==0 {"z"}else{"a"},
                "vector":[id as f32+1.0,1.0],"padding":padding})
        })
        .collect();
    let body = json!({"distance_metric":"cosine_distance",
        "schema":{"padding":{"type":"string","filterable":false}},"upsert_rows":rows});
    write(&indexed, body.clone()).await;
    write(&reference, body).await;
    assert!(indexed.index_namespace("test", true).await.unwrap());
    for filter in [
        json!(["tag", "Eq", "z"]),
        json!(["And", [["tag", "Eq", "z"], ["Not", ["n", "Lt", 5]]]]),
        json!(["id", "Gt", 4]),
    ] {
        let request = json!({"rank_by":["vector","ANN",[1.0,0.0]],
            "top_k":16,"filters":filter});
        let (response, diagnostic) = indexed
            .query_with_diagnostics("test", serde_json::from_value(request.clone()).unwrap())
            .await
            .unwrap();
        let mut actual = serde_json::to_value(response).unwrap();
        actual.as_object_mut().unwrap().remove("performance");
        assert_eq!(actual, query(&reference, request.clone()).await);
        let skipped_bytes: u64 = diagnostic.backend.iter().map(|b| b.bytes).sum();
        let mut projected = request;
        projected["include_attributes"] = json!(["padding"]);
        let (response, diagnostic) = indexed
            .query_with_diagnostics("test", serde_json::from_value(projected).unwrap())
            .await
            .unwrap();
        let projected_bytes: u64 = diagnostic.backend.iter().map(|b| b.bytes).sum();
        assert!(
            projected_bytes > skipped_bytes * 2,
            "unused padding was read: unprojected={skipped_bytes}, projected={projected_bytes}"
        );
        let mut projected = serde_json::to_value(response).unwrap();
        projected.as_object_mut().unwrap().remove("performance");
        for row in projected["rows"].as_array_mut().unwrap() {
            assert_eq!(row["padding"].as_str().unwrap().len(), 65536);
            row.as_object_mut().unwrap().remove("padding");
        }
        assert_eq!(actual, projected);
    }
    let request = json!({"queries":[
        {"rank_by":["vector","ANN",[1.0,0.0]],"top_k":16,"filters":["tag","Eq","z"],"include_attributes":["padding"]},
        {"rank_by":["vector","ANN",[1.0,0.0]],"top_k":16,"filters":["id","Gt",4]}
    ],"rerank_by":["RRF"],"limit":16});
    assert_eq!(
        query(&indexed, request.clone()).await,
        query(&reference, request).await
    );
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
    let f = &m.segments[0].files["annv2:small"];
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
    // Per-term statistics live in their own object; the manifest only references it.
    assert!(m.stats.text.is_empty());
    let file = m.text_stats.expect("text statistics object");
    assert_eq!(
        e.storage().get(&file.key).await.unwrap().len() as u64,
        file.size
    );
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
    use object_store::ObjectStoreExt;
    let backend = std::sync::Arc::new(object_store::memory::InMemory::new());
    let e = Engine::with_config(
        Storage::from_backend(backend.clone(), Default::default(), false, false)
            .await
            .unwrap(),
        EngineConfig {
            indexing: false,
            ..Default::default()
        },
    );
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
    let path = object_store::path::Path::from(file.key.as_str());
    let mut bytes = backend
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    // Preserve the portable create header, and corrupt the first logical data byte.
    bytes[24] ^= 1;
    backend.put(&path, bytes.into()).await.unwrap();
    // Immutable-object caches intentionally retain verified bytes. A cold reader
    // must reject backend corruption instead of serving it into a newly filled cache.
    let bounded = Engine::with_config(
        Storage::from_backend(backend, Default::default(), true, false)
            .await
            .unwrap(),
        EngineConfig {
            indexing: false,
            ..Default::default()
        },
    );
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

#[tokio::test]
async fn segment_caps_split_flushes_and_bound_compaction_without_losing_updates() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let e = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            max_segment_rows: 7,
            max_segment_bytes: 4096,
            compaction_segments: 2,
            retention: Duration::ZERO,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    write(&e, initial()).await;
    e.index_namespace("test", true).await.unwrap();
    for round in 0..4 {
        write(&e, json!({"upsert_rows": [{"id":100+round,"text":"new fox","vector":[1.0,2.0],"small":[1.0,2.0]}],"patch_rows":[{"id":round,"n":100}]})).await;
        e.index_namespace("test", true).await.unwrap();
    }
    for _ in 0..8 {
        e.index_namespace("test", true).await.unwrap();
    }
    let (_, m) = Wal::new(store.clone(), "test")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    assert!(m.segments.len() > 1);
    assert!(m.segments.iter().all(|s| s.row_count <= 7));
    let q = json!({"rank_by":["id","asc"],"top_k":100,"include_attributes":true});
    let before = query(&e, q.clone()).await;
    assert_eq!(before["rows"].as_array().unwrap().len(), 34);
    e.collect_garbage("test").await.unwrap();
    let cold = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            ..Default::default()
        },
    );
    assert_eq!(query(&cold, q).await, before);
}

#[tokio::test]
async fn byte_cap_splits_large_documents_and_compact_vectors_preserve_wire_encodings() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let e = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            max_segment_rows: 100,
            max_segment_bytes: 600,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    write(&e, json!({"distance_metric":"cosine_distance","schema":{"small":"[2]f16"},"upsert_rows":
        (0..12).map(|id|json!({"id":id,"text":"a".repeat(280),"vector":[0.25,1.0],"small":[0.3333,1.0]})).collect::<Vec<_>>()
    })).await;
    let wal = Wal::new(store, "test").unwrap();
    let entries = wal.tail(0).await.unwrap();
    assert!(entries[0].1.batches[0].upserts[0]["vector"].is_string());
    let request = json!({"rank_by":["id","asc"],"top_k":12,"include_attributes":true});
    let expected = query(&e, request.clone()).await;
    assert!(expected["rows"][0]["vector"].is_array());
    let encoded = query(&e, json!({"rank_by":["id","asc"],"top_k":12,"include_attributes":true,"vector_encoding":"base64"})).await;
    assert!(encoded["rows"][0]["vector"].is_string());
    e.index_namespace("test", true).await.unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(manifest.segments.len(), 12);
    assert_eq!(query(&e, request).await, expected);
}

/// Upserts, patches, and deletes of IDs that live in older segments must find them through each
/// segment's ID filter (range plus Bloom), and a segment without a filter (one built before
/// filters existed) must still answer from its ID list.
#[tokio::test]
async fn id_filters_find_existing_rows_across_many_segments_and_legacy_segments() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let e = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            defer_compaction_segments: 1_000,
            compaction_segments: 1_000,
            retention: Duration::ZERO,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    write(&e, json!({"distance_metric":"cosine_distance","schema":{"text":{"type":"string","full_text_search":true}},
        "upsert_rows":[{"id":"seed","text":"seed","vector":[1.0,1.0]}]}))
    .await;
    e.index_namespace("test", true).await.unwrap();
    // Ten segments of ten rows each, with string IDs spread across the key space.
    for batch in 0..10 {
        let rows: Vec<_> = (0..10)
            .map(|i| json!({"id":format!("doc-{i}-{batch}"),"text":"fox","vector":[i as f32+1.0,batch as f32+1.0]}))
            .collect();
        write(&e, json!({"upsert_rows": rows})).await;
        e.index_namespace("test", true).await.unwrap();
    }
    let (_, m) = Wal::new(store.clone(), "test")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    assert!(m.segments.len() >= 10);
    assert!(m.segments.iter().all(|s| s.files.contains_key("idfilter")));
    // Replace rows in several different segments, delete some, patch some, then index.
    write(
        &e,
        json!({"upsert_rows":[
        {"id":"doc-3-2","text":"replaced","vector":[9.0,9.0]},
        {"id":"doc-7-8","text":"replaced","vector":[9.0,9.0]},
        {"id":"brand-new","text":"new","vector":[1.0,2.0]}],
        "patch_rows":[{"id":"doc-0-0","text":"patched"}],
        "deletes":["doc-5-5","doc-9-9","never-existed"]}),
    )
    .await;
    e.index_namespace("test", true).await.unwrap();
    let q = json!({"rank_by":["id","asc"],"top_k":200,"include_attributes":["text"]});
    let rows = query(&e, q.clone()).await["rows"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        rows.len(),
        1 + 100 + 1 - 2,
        "no duplicates, two deletes, one insert"
    );
    let text_of = |id: &str| {
        rows.iter()
            .find(|r| r["id"] == id)
            .map(|r| r["text"].clone())
    };
    assert_eq!(text_of("doc-3-2"), Some(json!("replaced")));
    assert_eq!(text_of("doc-7-8"), Some(json!("replaced")));
    assert_eq!(text_of("doc-0-0"), Some(json!("patched")));
    assert_eq!(text_of("doc-5-5"), None);
    assert_eq!(text_of("brand-new"), Some(json!("new")));
    // Strip every filter from the manifest, as in a namespace written by an earlier version.
    let wal = Wal::new(store.clone(), "test").unwrap();
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    for seg in &mut manifest.segments {
        seg.files.remove("idfilter");
    }
    let epoch = manifest.indexer_epoch;
    manifest.indexer_epoch += 1;
    wal.publish_fenced(seq, epoch, &manifest).await.unwrap();
    let legacy = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    write(&legacy, json!({"upsert_rows":[{"id":"doc-1-1","text":"legacy replaced","vector":[2.0,2.0]}],"deletes":["doc-2-2"]})).await;
    let after = query(&legacy, q).await["rows"].as_array().unwrap().clone();
    assert_eq!(
        after.len(),
        rows.len() - 1,
        "one delete through a legacy segment"
    );
    let legacy_text = after
        .iter()
        .find(|r| r["id"] == "doc-1-1")
        .map(|r| r["text"].clone());
    assert_eq!(legacy_text, Some(json!("legacy replaced")));
    assert!(after.iter().all(|r| r["id"] != "doc-2-2"));
}
