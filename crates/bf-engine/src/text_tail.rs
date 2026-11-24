//! Snapshot-independent, byte-bounded analysis of immutable WAL and segment rows.
//! Queries reuse counts; commits perform no extra tokenization or index maintenance.
use crate::{NamespaceState, read_cache, segment, text};
use bf_storage::Storage;
use bf_text::{DocumentTerms, FtsParams};
use bf_types::{ApiError, Document, Schema};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) enum Key {
    Tail(usize, Arc<str>),
    Segment(Arc<str>, u32, Arc<str>),
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
}
impl Analyzer {
    pub fn new(schema: &Schema) -> Self {
        let fields: Vec<_> = schema
            .iter()
            .filter_map(|(name, cfg)| cfg.text_config().map(|cfg| (name.clone(), cfg)))
            .collect();
        // Include the complete analysis configuration, including array semantics.
        let key = serde_json::to_string(&fields).expect("text configurations serialize");
        Self {
            fields,
            key: key.into(),
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
        let key = Key::Tail(Arc::as_ptr(doc) as usize, self.key.clone());
        let context = read_cache::context();
        if let Some(value) = context.as_ref().and_then(|c| c.cache.text_terms.get(&key)) {
            return value;
        }
        let value = Arc::new(self.analyze(doc, Some(Arc::downgrade(doc))));
        if let Some(context) = context {
            context.cache.text_terms.insert(key, value.clone());
        }
        value
    }
    /// Read only previously unseen shadow rows; immutable object identities and
    /// the analysis configuration make cached summaries safe across publications.
    pub async fn shadowed(
        &self,
        store: &Storage,
        state: &NamespaceState,
    ) -> Result<Vec<Arc<Analysis>>, ApiError> {
        let fields: BTreeSet<_> = self.fields.iter().map(|(name, _)| name.clone()).collect();
        let mut summaries = Vec::new();
        let context = read_cache::context();
        for seg in &state.segments {
            let live = segment::live(store, seg).await?;
            let shadowed = segment::lookup_many(store, seg, &state.shadowed).await? & live;
            let file = seg.files.get("docs").ok_or_else(segment::corrupt)?;
            let object: Arc<str> = context
                .as_ref()
                .map_or_else(|| file.key.clone(), |c| c.cache.key(&file.key))
                .into();
            let mut missing = roaring::RoaringBitmap::new();
            for row in &shadowed {
                let key = Key::Segment(object.clone(), row, self.key.clone());
                if let Some(value) = context.as_ref().and_then(|c| c.cache.text_terms.get(&key)) {
                    summaries.push(value);
                } else {
                    missing.insert(row);
                }
            }
            if !missing.is_empty() {
                for (row, doc) in
                    segment::projected_rows(store, seg, &missing, Some(&fields)).await?
                {
                    let value = Arc::new(self.analyze(&doc, None));
                    let key = Key::Segment(object.clone(), row, self.key.clone());
                    if let Some(context) = &context {
                        context.cache.text_terms.insert(key, value.clone());
                    }
                    summaries.push(value);
                }
            }
        }
        Ok(summaries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Engine, EngineConfig, indexed, query, snapshot};
    use bf_types::{Consistency, QueryRequest};
    use serde_json::json;
    #[tokio::test]
    async fn immutable_counts_follow_replacement_schema_changes_and_copy_on_write() {
        let schema: Schema =
            serde_json::from_value(json!({"text":{"type":"string","full_text_search":true}}))
                .unwrap();
        let analyzer = Analyzer::new(&schema);
        let cache = Arc::new(read_cache::DecodedCache::new(1 << 24, 1));
        read_cache::scope(read_cache::ReadContext::new(cache), async {
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
        let analyzer = Analyzer::new(&schema);
        for budget in [0, 1 << 24] {
            let cache = Arc::new(read_cache::DecodedCache::new(budget, 1));
            read_cache::scope(read_cache::ReadContext::new(cache.clone()), async {
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
