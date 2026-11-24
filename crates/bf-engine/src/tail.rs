//! Exact scoring of resident WAL documents, with byte-bounded decoded vector reuse.
use bf_types::{ApiError, Document, decode_vector};
use std::sync::{Arc, Weak};

type Key = (usize, Arc<str>);

/// A cached allocation or a query-local decode when admission is disabled.
pub(crate) enum Values {
    Resident(Arc<[f32]>),
    Transient(Vec<f32>),
}
impl AsRef<[f32]> for Values {
    fn as_ref(&self) -> &[f32] {
        match self {
            Self::Resident(v) => v,
            Self::Transient(v) => v,
        }
    }
}

/// A weak reference reserves the source allocation's identity without keeping obsolete
/// documents alive. It prevents pointer reuse from aliasing a newer document's vector.
struct Vector {
    _source: Weak<Document>,
    values: Arc<[f32]>,
    attribute_bytes: usize,
}

/// Part of the engine's existing decoded-cache budget. Writes allocate nothing here;
/// queries decode each immutable document/attribute once while it remains cached.
#[derive(Clone)]
pub(crate) struct VectorCache(Arc<crate::read_cache::ClockCache<Arc<Vector>, Key>>, u64);

impl VectorCache {
    pub(crate) fn new(bytes: u64) -> Self {
        Self(
            Arc::new(crate::read_cache::ClockCache::new(
                bytes,
                |value: &Arc<Vector>| {
                    (value.values.len() * size_of::<f32>() + value.attribute_bytes + 256) as u64
                },
            )),
            bytes,
        )
    }

    pub(crate) fn clear(&self) {
        self.0.clear();
    }

    /// Oversized tails still use any cached vectors, but do not churn the cache on every
    /// exhaustive pass. Their transient decodes allocate only the reference decoder's Vec.
    pub(crate) fn fits(&self, rows: usize, dimensions: usize, attribute_bytes: usize) -> bool {
        let per_row = (dimensions as u64)
            .saturating_mul(4)
            .saturating_add(attribute_bytes as u64)
            .saturating_add(256);
        rows as u64 <= self.1 / per_row
    }

    /// Document Arcs are immutable. Schema changes use copy-on-write, and replacements
    /// allocate a new Arc, so neither can return a stale cached vector.
    pub(crate) fn get(
        &self,
        doc: &Arc<Document>,
        attribute: &Arc<str>,
        admit: bool,
    ) -> Result<Values, ApiError> {
        let key = (Arc::as_ptr(doc) as usize, attribute.clone());
        if !admit {
            if let Some(vector) = self.0.get(&key) {
                return Ok(Values::Resident(vector.values.clone()));
            }
            return Ok(Values::Transient(decode_vector(&doc[attribute.as_ref()])?));
        }
        let vector = self.0.get_or_try_insert(key, || -> Result<_, ApiError> {
            Ok(Arc::new(Vector {
                _source: Arc::downgrade(doc),
                values: decode_vector(&doc[attribute.as_ref()])?.into(),
                attribute_bytes: attribute.len(),
            }))
        })?;
        Ok(Values::Resident(vector.values.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reuses_immutable_vectors_and_tracks_replacements_and_copy_on_write() {
        let cache = VectorCache::new(1 << 20);
        let attribute: Arc<str> = "vector".into();
        let mut doc = Arc::new(
            serde_json::from_value(json!({"id":1,"vector":[1.0,2.0],"other":[3.0,4.0]})).unwrap(),
        );
        let old = cache.get(&doc, &attribute, true).unwrap();
        assert!(std::ptr::eq(
            old.as_ref(),
            cache.get(&doc, &attribute, true).unwrap().as_ref()
        ));
        assert_eq!(
            cache.get(&doc, &Arc::from("other"), true).unwrap().as_ref(),
            &[3.0, 4.0]
        );
        assert_eq!(
            Arc::strong_count(&doc),
            1,
            "cache must not retain document contents"
        );
        let snapshot = doc.clone();
        Arc::make_mut(&mut doc).insert("vector".into(), json!([5.0, 6.0]));
        assert_eq!(
            cache.get(&doc, &attribute, true).unwrap().as_ref(),
            &[5.0, 6.0]
        );
        assert!(std::ptr::eq(
            old.as_ref(),
            cache.get(&snapshot, &attribute, true).unwrap().as_ref()
        ));
        drop(snapshot);
        // With only a weak cache reference left, make_mut still dissociates it and gives
        // the edited document a new identity.
        Arc::make_mut(&mut doc).insert("vector".into(), json!([7.0, 8.0]));
        assert_eq!(
            cache.get(&doc, &attribute, true).unwrap().as_ref(),
            &[7.0, 8.0]
        );
    }

    #[test]
    fn byte_budget_zero_budget_and_nonadmitting_reads() {
        let attribute: Arc<str> = "vector".into();
        let cache = VectorCache::new(64 * 1024);
        assert!(cache.fits(8, 1024, 6));
        assert!(!cache.fits(100, 1024, 6));
        let docs: Vec<Arc<Document>> = (0..1000)
            .map(|id| {
                Arc::new(serde_json::from_value(json!({"id":id,"vector":[1.0,2.0]})).unwrap())
            })
            .collect();
        for doc in &docs {
            cache.get(doc, &attribute, true).unwrap();
        }
        assert!(cache.0.resident_bytes() <= 64 * 1024);
        cache.clear();
        assert_eq!(cache.0.resident_bytes(), 0);
        cache.get(&docs[0], &attribute, false).unwrap();
        assert_eq!(cache.0.resident_bytes(), 0);
        let disabled = VectorCache::new(0);
        disabled.get(&docs[0], &attribute, true).unwrap();
        assert_eq!(disabled.0.resident_bytes(), 0);
    }

    #[tokio::test]
    async fn tail_top_k_preserves_ties_zeros_missing_vectors_and_diversification() {
        use crate::*;
        for metric in [
            DistanceMetric::CosineDistance,
            DistanceMetric::EuclideanSquared,
        ] {
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
            let rows: Vec<_> = (0..96)
                .map(|id| {
                    let mut doc = json!({"id":format!("row{id:03}"),"group":id%4});
                    doc["vector"] = if id % 3 == 0 {
                        json!([0.0, 0.0, 0.0])
                    } else {
                        json!([1.0, 0.0, 0.0])
                    };
                    doc
                })
                .collect();
            engine.write("tail",serde_json::from_value(json!({"distance_metric":metric,"schema":{"vector":{"type":"[3]f16"}},"upsert_rows":rows})).unwrap()).await.unwrap();
            let (mut state, _) = engine.snapshot("tail", &Consistency::Strong).await.unwrap();
            // Historical rows can lack a vector attribute introduced after their write.
            for (i, doc) in state.docs.values_mut().enumerate() {
                if i % 5 == 0 {
                    Arc::make_mut(doc).remove("vector");
                }
            }
            let context = read_cache::ReadContext::new(engine.0.read_cache.clone());
            read_cache::scope(context, async {
                for vector in [json!([1.0, 0.0, 0.0]), json!([0.0, 0.0, 0.0])] {
                    for limit in [
                        json!({"total":9}),
                        json!({"total":9,"per":{"attributes":["group"],"limit":3}}),
                    ] {
                        let request: QueryRequest = serde_json::from_value(
                            json!({"rank_by":["vector","ANN",vector],"limit":limit,"offset":4,
                            "include_attributes":true,"filters":["group","In",[0,1,2]]}),
                        )
                        .unwrap();
                        let expected = query::execute(&state, &request).unwrap().result.rows;
                        for _ in 0..2 {
                            let got = indexed::execute(&store, &state, &request, &config, false)
                                .await
                                .unwrap()
                                .result
                                .rows;
                            assert_eq!(got, expected, "{metric:?}, {request:?}");
                        }
                    }
                }
            })
            .await;
        }
    }

    /// Score the optimized and exhaustive plans from exactly the same captured snapshot
    /// while a writer replaces, patches, and deletes rows and an indexer publishes segments.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_tail_queries_equal_the_same_snapshot_reference() {
        use crate::*;
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
            for metric in [DistanceMetric::CosineDistance, DistanceMetric::EuclideanSquared] {
                for (dimensions, count, updates, rounds) in [(17, 192, 8, 32), (65, 4096, 2304, 8)] {
                    for dtype in [format!("[{dimensions}]f32"), format!("[{dimensions}]f16")] {
                        let store = Storage::open(&bf_storage::StorageConfig { url:"memory://".into(), ..Default::default() }).await.unwrap();
                        let config = EngineConfig { indexing:false, recall_sample_rate:0.0, lease_duration:std::time::Duration::ZERO, ..Default::default() };
                        let engine = Engine::with_config(store.clone(), config.clone());
                        let row = move |id: u64, step: u64| json!({"id":id,"group":id%4,"version":step,
                            "vector":(0..dimensions).map(|d| match d { 0=>1.0, 1=>((id%17) as f32+1.0+step as f32/97.0)/16.0, _=>d as f32*0.001 }).collect::<Vec<_>>()});
                        engine.write("tail", serde_json::from_value(json!({"schema":{"vector":{"type":dtype}},"distance_metric":metric,
                            "upsert_rows":(0..count).map(|id|row(id,0)).collect::<Vec<_>>() })).unwrap()).await.unwrap();
                        engine.index_namespace("tail",true).await.unwrap();
                        let writer = engine.clone();
                        let (ready,mut steps) = tokio::sync::mpsc::channel(1);
                        let proceed = Arc::new(tokio::sync::Notify::new());
                        let next = proceed.clone();
                        let task = tokio::spawn(async move {
                            for step in 1..=rounds {
                                writer.write("tail",serde_json::from_value(json!({
                                    "upsert_rows":(0..updates).map(|i|row((step*11+i)%(count-64), step)).collect::<Vec<_>>(),
                                    "deletes":[count-64+step%32],
                                    "patch_rows":[{"id":count-32+step%32,"version":step+100}]
                                })).unwrap()).await.unwrap();
                                if step%5==0 { writer.index_namespace("tail",true).await.unwrap(); }
                                ready.send(step).await.unwrap();
                                next.notified().await;
                            }
                        });
                        let context = read_cache::ReadContext::new(engine.0.read_cache.clone());
                        read_cache::scope(context, async {
                            for round in 0..rounds {
                                steps.recv().await.expect("writer committed this consistency point");
                                let (state, _) = engine.snapshot("tail", &Consistency::Strong).await.unwrap();
                                // The next write/index publication runs while both evaluators use
                                // this captured consistency point. No timing assumptions are needed.
                                proceed.notify_one();
                                let full = snapshot::materialize(&store, &state).await.unwrap();
                                let vector: Vec<f32> = (0..dimensions).map(|d| if d==0 {1.0}else{0.0}).collect();
                                let request: QueryRequest = serde_json::from_value(json!({
                                    "rank_by":["vector","ANN",vector],"top_k":9,"offset":3,
                                    "include_attributes":["version","group","vector"],
                                    "filters":match round%3 {0=>json!(["group","In",[0,2]]),1=>json!(["id","Gte",0]),_=>serde_json::Value::Null}
                                })).unwrap();
                                let got = indexed::execute(&store,&state,&request,&config,true).await.unwrap().result.rows.unwrap();
                                let expected = query::execute(&full,&request).unwrap().result.rows.unwrap();
                                assert_eq!(got.iter().map(|d|&d["id"]).collect::<Vec<_>>(),expected.iter().map(|d|&d["id"]).collect::<Vec<_>>(),"{dtype}, {metric:?}, seq={}",state.seq);
                                for (mut actual,mut reference) in got.into_iter().zip(expected) {
                                    let a=actual.remove("$dist").unwrap().as_f64().unwrap();
                                    let b=reference.remove("$dist").unwrap().as_f64().unwrap();
                                    assert!((a-b).abs()<=1e-5*b.abs().max(1.0));
                                    assert_eq!(actual,reference);
                                }
                                tokio::task::yield_now().await;
                            }
                        }).await;
                        task.await.unwrap();
                    }
                }
            }
        }).await.expect("concurrent tail regression timed out");
    }
}
