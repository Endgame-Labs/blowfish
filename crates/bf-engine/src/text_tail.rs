//! Snapshot-independent, byte-bounded analysis of immutable WAL rows.
//! Queries reuse counts; commits perform no extra tokenization or index maintenance.
use crate::{read_cache, text};
use bf_text::{DocumentTerms, FtsParams};
use bf_types::{Document, Schema};
use rayon::prelude::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) enum Key {
    Tail(usize, Arc<str>),
}

pub(crate) struct Analysis {
    // Reserving the allocation identity prevents address reuse after a WAL row retires.
    _source: Option<Weak<Document>>,
    key_bytes: usize,
    pub fields: BTreeMap<String, DocumentTerms>,
}
impl Analysis {
    pub(crate) fn bytes(&self) -> u64 {
        1536 + self.key_bytes as u64
            + self
                .fields
                .iter()
                .map(|(name, terms)| name.capacity() as u64 + 128 + terms.resident_bytes())
                .sum::<u64>()
    }
}
pub(crate) struct Analyzer {
    fields: Vec<(String, FtsParams)>,
    key: Arc<str>,
    cache: Option<Arc<read_cache::ClockCache<Arc<Analysis>, Key>>>,
    prepared: Option<Arc<PreparedTail>>,
}

/// Query-local immutable summaries. Weak document references reserve every
/// allocation identity, and the full analysis key prevents cross-schema reuse.
pub(crate) struct PreparedTail {
    key: Arc<str>,
    pub rows: BTreeMap<usize, Arc<Analysis>>,
}
impl Analyzer {
    pub fn new(schema: &Schema) -> Self {
        let fields: Vec<_> = schema
            .iter()
            .filter_map(|(name, cfg)| cfg.text_config().map(|cfg| (name.clone(), cfg)))
            .collect();
        // Include the complete analysis configuration, including array semantics.
        let key = serde_json::to_string(&fields).expect("text configurations serialize");
        let context = read_cache::context();
        let prepared = context
            .as_ref()
            .and_then(|context| context.text_tail.clone())
            .filter(|tail| tail.key.as_ref() == key);
        Self {
            fields,
            key: key.into(),
            cache: context.map(|context| context.cache.text_terms.clone()),
            prepared,
        }
    }
    fn analyze(&self, doc: &Document, source: Option<Weak<Document>>) -> Analysis {
        Analysis {
            _source: source,
            key_bytes: self.key.len(),
            fields: self
                .fields
                .iter()
                .filter_map(|(name, cfg)| {
                    doc.get(name)
                        .filter(|value| !value.is_null())
                        .map(|value| (name.clone(), DocumentTerms::new(&text::tokens(value, cfg))))
                })
                .collect(),
        }
    }
    pub fn tail(&self, doc: &Arc<Document>) -> Arc<Analysis> {
        if let Some(value) = self
            .prepared
            .as_ref()
            .and_then(|tail| tail.rows.get(&(Arc::as_ptr(doc) as usize)))
        {
            return value.clone();
        }
        let key = Key::Tail(Arc::as_ptr(doc) as usize, self.key.clone());
        if let Some(value) = self.cache.as_ref().and_then(|cache| cache.get(&key)) {
            return value;
        }
        let value = Arc::new(self.analyze(doc, Some(Arc::downgrade(doc))));
        if let Some(cache) = &self.cache {
            cache.insert(key, value.clone());
        }
        value
    }

    /// Analyze each WAL row once for statistics and ranking. Capture the cache
    /// before entering Rayon: task-local read contexts do not follow CPU jobs.
    pub fn gather(&self, docs: &BTreeMap<bf_types::Id, Arc<Document>>) -> PreparedTail {
        let docs: Vec<_> = docs.values().collect();
        let analyze = |doc: &&Arc<Document>| (Arc::as_ptr(doc) as usize, self.tail(doc));
        let rows = if docs.len() >= 512 && crate::fan_out() {
            // Async posting reads can resume on an I/O worker. Retain the query
            // pool's CPU budget instead of silently switching to global Rayon.
            let pool = crate::query_worker::workers();
            // Bound runnable chunks, leaving capacity for WAL completions.
            let chunks = pool.current_num_threads().saturating_sub(1).max(1);
            let rows: Vec<Vec<_>> = pool.install(|| {
                docs.par_chunks(docs.len().div_ceil(chunks))
                    .map(|chunk| chunk.iter().map(analyze).collect())
                    .collect()
            });
            rows.into_iter().flatten().collect()
        } else {
            docs.iter().map(analyze).collect()
        };
        PreparedTail {
            key: self.key.clone(),
            rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Engine, EngineConfig, indexed, query, snapshot};
    use bf_storage::Storage;
    use bf_types::{Consistency, QueryRequest};
    use serde_json::json;

    #[tokio::test]
    async fn shadow_statistics_use_indexes_without_document_columns() {
        let store = Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
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
        engine
            .write(
                "shadow",
                serde_json::from_value(json!({
                    "schema":{"text":{"type":"string","full_text_search":true}},
                    "upsert_rows":(0..600).map(|id|match id%5 {
                        0=>json!({"id":id}),
                        1=>json!({"id":id,"text":null}),
                        2=>json!({"id":id,"text":""}),
                        _=>json!({"id":id,"text":"red fox fox prefix"}),
                    }).collect::<Vec<_>>()
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("shadow", true).await.unwrap();
        engine
            .write(
                "shadow",
                serde_json::from_value(json!({
                    "upsert_rows":[{"id":3,"text":"red blue"},{"id":4,"text":"prefix blue"}],
                    "deletes":[8,9]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("shadow", true).await.unwrap();
        engine.write("shadow", serde_json::from_value(json!({
            "upsert_rows":[{"id":0,"text":"red fox"},{"id":1,"text":"red fox"},{"id":2,"text":"red fox"},{"id":3,"text":null},{"id":4,"text":""},{"id":1000,"text":"blue fox"}],
            "deletes":[8,9,13,14]
        })).unwrap()).await.unwrap();
        let (state, _) = engine
            .snapshot("shadow", &Consistency::Strong)
            .await
            .unwrap();
        let mut reference = snapshot::materialize(&store, &state).await.unwrap();
        text::reference_stats(&mut reference);
        // IDs-only BM25 must not fetch shadowed text columns, even on the first
        // query with a zero decoded-cache budget. Previously those files were
        // needed to analyze the replaced documents.
        for seg in &state.segments {
            store.delete(&seg.files["docs"].key).await.unwrap();
        }
        store.drop_caches();
        for budget in [0, 1 << 24] {
            let context =
                read_cache::ReadContext::new(Arc::new(read_cache::DecodedCache::new(budget, 2)));
            read_cache::scope(context, async {
                for rank in [
                    json!(["text", "BM25", "red fox red"]),
                    json!(["text", "BM25", "blue"]),
                    json!(["text","BM25","pre",{"last_as_prefix":true}]),
                ] {
                    let request: QueryRequest = serde_json::from_value(
                        json!({"rank_by":rank,"top_k":10,"include_attributes":["id"]}),
                    )
                    .unwrap();
                    let expected = query::execute(&reference, &request).unwrap();
                    for _ in 0..2 {
                        let actual = indexed::execute(&store, &state, &request, &config, false)
                            .await
                            .unwrap();
                        assert_eq!(
                            serde_json::to_value(actual.result).unwrap(),
                            serde_json::to_value(&expected.result).unwrap()
                        );
                        assert!(
                            read_cache::context().unwrap().text_shadowed.is_none(),
                            "masks must leave scope with their snapshot"
                        );
                    }
                }
            })
            .await;
        }
    }
    #[tokio::test]
    async fn immutable_counts_follow_replacement_schema_changes_and_copy_on_write() {
        let schema: Schema =
            serde_json::from_value(json!({"text":{"type":"string","full_text_search":true}}))
                .unwrap();
        let cache = Arc::new(read_cache::DecodedCache::new(1 << 24, 1));
        read_cache::scope(read_cache::ReadContext::new(cache), async {
            let analyzer = Analyzer::new(&schema);
            let mut doc: Arc<Document> =
                Arc::new(serde_json::from_value(json!({"id":1,"text":"Fox fox"})).unwrap());
            let old = analyzer.tail(&doc);
            assert_eq!(old.fields["text"].frequency("fox"), 2);
            assert!(Arc::ptr_eq(&old, &analyzer.tail(&doc)));
            assert_eq!(Arc::strong_count(&doc), 1);
            let original = doc.clone();
            Arc::make_mut(&mut doc).insert("text".into(), json!("blue"));
            assert_eq!(analyzer.tail(&doc).fields["text"].frequency("fox"), 0);
            assert!(Arc::ptr_eq(&old, &analyzer.tail(&original)));
            let schema: Schema = serde_json::from_value(
                json!({"text":{"type":"string","full_text_search":{"case_sensitive":true}}}),
            )
            .unwrap();
            assert_eq!(
                Analyzer::new(&schema).tail(&original).fields["text"].frequency("Fox"),
                1
            );
            drop(original);
            Arc::make_mut(&mut doc).insert("text".into(), json!(null));
            assert!(analyzer.tail(&doc).fields.is_empty());
            Arc::make_mut(&mut doc).insert("text".into(), json!(""));
            assert!(analyzer.tail(&doc).fields["text"].is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn text_count_cache_evicts_and_honors_a_zero_budget() {
        let schema: Schema =
            serde_json::from_value(json!({"text":{"type":"string","full_text_search":true}}))
                .unwrap();
        for budget in [0, 1 << 24] {
            let cache = Arc::new(read_cache::DecodedCache::new(budget, 1));
            read_cache::scope(read_cache::ReadContext::new(cache.clone()), async {
                let analyzer = Analyzer::new(&schema);
                for id in 0..5000 {
                    let word = format!("term{id}");
                    let doc =
                        Arc::new(serde_json::from_value(json!({"id":id,"text":word})).unwrap());
                    assert_eq!(analyzer.tail(&doc).fields["text"].frequency(&word), 1);
                }
                assert!(cache.text_terms.resident_bytes() <= budget / 8);
                if budget > 0 {
                    assert!(cache.text_terms.resident_bytes() > 0);
                }
                cache.clear();
                assert_eq!(cache.text_terms.resident_bytes(), 0);
            })
            .await;
        }
    }

    #[tokio::test]
    async fn gathered_tail_is_query_local_and_reserves_replaced_document_identities() {
        let schema: Schema = serde_json::from_value(json!({
            "text":{"type":"string","full_text_search":true}
        }))
        .unwrap();
        for budget in [0, 1 << 24] {
            let cache = Arc::new(read_cache::DecodedCache::new(budget, 2));
            let context = read_cache::ReadContext::new(cache);
            read_cache::scope(context.clone(), async {
                let analyzer = Analyzer::new(&schema);
                let mut docs: BTreeMap<_, _> = (0..1024)
                    .map(|id| {
                        (
                            bf_types::Id::Uint(id),
                            Arc::new(
                                serde_json::from_value(json!({
                                    "id":id,"text":"Fox fox blue"
                                }))
                                .unwrap(),
                            ),
                        )
                    })
                    .collect();
                let prepared = Arc::new(analyzer.gather(&docs));
                assert_eq!(prepared.rows.len(), docs.len());
                let original = docs[&bf_types::Id::Uint(0)].clone();
                let summary = prepared.rows[&(Arc::as_ptr(&original) as usize)].clone();
                let mut scoped = context;
                scoped.text_tail = Some(prepared);
                read_cache::scope(scoped, async {
                    let analyzer = Analyzer::new(&schema);
                    assert!(Arc::ptr_eq(&summary, &analyzer.tail(&original)));
                    Arc::make_mut(docs.get_mut(&bf_types::Id::Uint(0)).unwrap())
                        .insert("text".into(), json!("green"));
                    let replacement = analyzer.tail(&docs[&bf_types::Id::Uint(0)]);
                    assert_eq!(replacement.fields["text"].frequency("green"), 1);
                    assert_eq!(replacement.fields["text"].frequency("fox"), 0);
                    let changed: Schema = serde_json::from_value(json!({
                        "text":{"type":"string","full_text_search":{"case_sensitive":true}}
                    }))
                    .unwrap();
                    let changed = Analyzer::new(&changed).tail(&original);
                    assert_eq!(changed.fields["text"].frequency("Fox"), 1);
                    assert!(!Arc::ptr_eq(&changed, &summary));
                })
                .await;
                assert!(read_cache::context().unwrap().text_tail.is_none());
            })
            .await;
        }
    }

    #[tokio::test]
    async fn bm25_tail_ties_offsets_and_projections_match_snapshot() {
        let store = Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let config = EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            max_segment_rows: 200,
            ..Default::default()
        };
        let engine = Engine::with_config(store.clone(), config.clone());
        engine
            .write(
                "ties",
                serde_json::from_value(json!({
                    "schema":{"text":{"type":"string","full_text_search":true}},
                    "upsert_rows":(0..1200).map(|id|json!({"id":id,"group":id%3,
                        "text":if id%4==0 {"red red blue"}else{"quiet"}})).collect::<Vec<_>>()
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("ties", true).await.unwrap();
        engine.write("ties", serde_json::from_value(json!({
            "upsert_rows":(0..300).chain(1200..1600).filter(|id| ![10,16,24].contains(id)).map(|id|json!({"id":id,
                "group":id%3,"text":if id==1201 {json!(null)}else if id==1202 {json!("")}else{json!("red red blue")}})).collect::<Vec<_>>(),
            "deletes":[10,16,24]
        })).unwrap()).await.unwrap();
        let (state, _) = engine.snapshot("ties", &Consistency::Strong).await.unwrap();
        let mut reference = snapshot::materialize(&store, &state).await.unwrap();
        text::reference_stats(&mut reference);
        for budget in [0, 1 << 24] {
            let context =
                read_cache::ReadContext::new(Arc::new(read_cache::DecodedCache::new(budget, 2)));
            read_cache::scope(context, async {
                for offset in [0, 3, 9] {
                    for rank in [json!(["text","BM25","red red"]), json!(["text","BM25","re",{"last_as_prefix":true}])] {
                        for filter in [None, Some(json!(["group","Eq",1]))] {
                            for projection in [json!(false), json!(["text","group"]), json!(true)] {
                                let mut body = json!({"rank_by":rank,"top_k":10,"offset":offset,
                                    "include_attributes":projection,"compute_attributes":{"blue":["text","BM25","blue"]}});
                                if let Some(filter)=&filter {body["filters"]=filter.clone();}
                                let request: QueryRequest = serde_json::from_value(body).unwrap();
                                let expected=query::execute(&reference,&request).unwrap();
                                let actual=indexed::execute(&store,&state,&request,&config,false).await.unwrap();
                                assert_eq!(serde_json::to_value(actual.result).unwrap(),serde_json::to_value(expected.result).unwrap(),"budget={budget} offset={offset}");
                                assert!(read_cache::context().unwrap().text_tail.is_none());
                            }
                        }
                    }
                }
            }).await;
        }
    }

    #[tokio::test]
    async fn serialized_snapshots_match_reference_across_writes_deletes_and_publications() {
        for budget in [0, 1 << 24] {
            let store = Storage::open(&bf_storage::StorageConfig {
                url: "memory://".into(),
                ..Default::default()
            })
            .await
            .unwrap();
            let config = EngineConfig {
                indexing: false,
                decoded_cache_bytes: budget,
                recall_sample_rate: 0.0,
                ..Default::default()
            };
            let engine = Engine::with_config(store.clone(), config.clone());
            let initial = json!({"schema":{"text":{"type":"string","full_text_search":true},"other":{"type":"[]string","full_text_search":true}},"upsert_rows":(0..80).map(|id|json!({"id":id,"group":id%3,"text":if id%7==0 {json!(null)}else if id%5==0 {json!("")}else {json!(format!("red fox fox blue term{}",id%11))},"other":["red fox","blue"]})).collect::<Vec<_>>()});
            engine
                .write("tail", serde_json::from_value(initial).unwrap())
                .await
                .unwrap();
            engine.index_namespace("tail", true).await.unwrap();
            let context =
                read_cache::ReadContext::new(Arc::new(read_cache::DecodedCache::new(budget, 1)));
            read_cache::scope(context,async {
                for step in 0..12u64 {
                    let body=json!({"upsert_rows":[{"id":step*5,"group":step%3,"text":format!("red blue blue update{step}"),"other":["fox", ""]},{"id":100+step,"group":1,"text":"red prefix","other":[]}],"deletes":[step*5+1]});
                    engine.write("tail",serde_json::from_value(body).unwrap()).await.unwrap();
                    let (state,_)=engine.snapshot("tail",&Consistency::Strong).await.unwrap();
                    let mut reference=snapshot::materialize(&store,&state).await.unwrap();
                    text::reference_stats(&mut reference);
                    for raw in [json!(["text","BM25","red fox red"]),json!(["text","BM25","pre",{"last_as_prefix":true}]),json!(["Sum",[["text","BM25","blue"],["other","BM25","fox"]]])] {
                        for filter in [None,Some(json!(["group","Eq",1]))] {
                            let mut body=json!({"rank_by":raw,"top_k":10,"include_attributes":["id"],"compute_attributes":{"blue_score":["text","BM25","blue blue"],"other_score":["other","BM25","red"]}});
                            if let Some(filter)=filter {body["filters"]=filter;}
                            let request:QueryRequest=serde_json::from_value(body).unwrap();
                            let expected=query::execute(&reference,&request).unwrap();
                            for _ in 0..2 {
                                let actual=indexed::execute(&store,&state,&request,&config,false).await.unwrap();
                                assert_eq!(serde_json::to_value(actual.result).unwrap(),serde_json::to_value(&expected.result).unwrap(),"budget={budget} step={step}");
                            }
                        }
                    }
                    if step%3==2 {engine.index_namespace("tail",true).await.unwrap();}
                }
            }).await;
        }
    }
}
