//! Exact scoring of resident WAL documents, with byte-bounded decoded vector reuse and a
//! prepared block of each snapshot's rows.
use bf_types::{ApiError, DistanceMetric, Document, decode_vector};
use bf_vector::{CosineNorm, CosineQuery};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

type Key = (usize, Arc<str>);

/// An immutable decoded tail vector with everything a query would otherwise recompute for
/// every row: whether it is a valid vector (nonempty and finite), whether it is all zeros, and
/// its squared norm in the exact distance kernel's accumulation order.
///
/// A weak reference reserves the source document allocation's identity without keeping
/// obsolete documents alive. It prevents pointer reuse from aliasing a newer document's vector.
pub(crate) struct Vector {
    _source: Weak<Document>,
    values: Vec<f32>,
    valid: bool,
    zero: bool,
    norm: Option<CosineNorm>,
    attribute_bytes: usize,
}
impl Vector {
    fn new(doc: &Arc<Document>, attribute: &str) -> Result<Self, ApiError> {
        let values = decode_vector(&doc[attribute])?;
        let valid = !values.is_empty() && values.iter().all(|x| x.is_finite());
        Ok(Self {
            _source: Arc::downgrade(doc),
            zero: values.iter().all(|x| *x == 0.0),
            norm: valid.then(|| CosineNorm::new(&values).ok()).flatten(),
            valid,
            values,
            attribute_bytes: attribute.len(),
        })
    }
}

/// A cached prepared vector or a query-local decode when admission is disabled.
pub(crate) enum Values {
    Resident(Arc<Vector>),
    Transient(Vec<f32>),
}
impl AsRef<[f32]> for Values {
    fn as_ref(&self) -> &[f32] {
        match self {
            Self::Resident(v) => &v.values,
            Self::Transient(v) => v,
        }
    }
}

/// One query's scorer over tail vectors: bit-identical to `query::distance`, but a resident
/// vector costs one dot product (cosine) or one squared-distance pass (Euclidean) instead of
/// two validation passes and a three-sum pass.
pub(crate) struct Scorer<'a> {
    metric: DistanceMetric,
    query: &'a [f32],
    cosine: Option<CosineQuery<'a>>,
    query_zero: bool,
    query_finite: bool,
}
impl<'a> Scorer<'a> {
    pub(crate) fn new(metric: DistanceMetric, query: &'a [f32]) -> Self {
        Self {
            metric,
            query,
            cosine: (metric == DistanceMetric::CosineDistance)
                .then(|| CosineQuery::new(query).ok())
                .flatten(),
            query_zero: query.iter().all(|x| *x == 0.0),
            query_finite: query.iter().all(|x| x.is_finite()),
        }
    }
    pub(crate) fn distance(&self, values: &Values) -> f64 {
        match values {
            Values::Resident(vector) => self.resident(vector),
            Values::Transient(values) => crate::query::distance(values, self.query, self.metric),
        }
    }
    pub(crate) fn resident(&self, p: &Vector) -> f64 {
        let comparable = p.valid && p.values.len() == self.query.len();
        match (self.metric, &self.cosine, p.norm) {
            (DistanceMetric::CosineDistance, _, _) if self.query_zero || p.zero => 1.0,
            (DistanceMetric::CosineDistance, Some(query), Some(norm)) if comparable => {
                query.distance(&p.values, norm) as f64
            }
            (DistanceMetric::EuclideanSquared, _, _) if comparable && self.query_finite => {
                bf_vector::Metric::EuclideanSquared.distance_unchecked(&p.values, self.query) as f64
            }
            _ => crate::query::distance(&p.values, self.query, self.metric),
        }
    }
}

/// The prepared vectors of one snapshot's tail, in the snapshot's document order. A query
/// scores contiguous rows with no lock, hash lookup, or reference count per row. Each row
/// records its document's identity, and a query uses a row only when the document at that
/// position is the same allocation, so a block for another snapshot can only cost speed.
pub(crate) struct Block {
    rows: Vec<(usize, Option<Arc<Vector>>)>,
}
impl Block {
    /// The prepared vector for the `index`th document, when this block was built from it.
    /// `Some(None)` is a document without the attribute.
    pub(crate) fn row(&self, index: usize, doc: &Arc<Document>) -> Option<Option<&Vector>> {
        let (identity, vector) = self.rows.get(index)?;
        (*identity == Arc::as_ptr(doc) as usize).then_some(vector.as_deref())
    }
}

/// One snapshot's block, built once by whichever query arrives first (single flight).
struct Slot {
    seq: u64,
    rows: usize,
    block: OnceLock<Option<Arc<Block>>>,
}

/// Part of the engine's existing decoded-cache budget. Writes allocate nothing here;
/// queries decode each immutable document/attribute once while it remains cached.
#[derive(Clone)]
pub(crate) struct VectorCache {
    cache: Arc<crate::read_cache::ClockCache<Arc<Vector>, Key>>,
    bytes: u64,
    /// The latest block per namespace and attribute (at most `LATEST` of them).
    blocks: Arc<Mutex<HashMap<Key, Arc<Slot>>>>,
}

impl VectorCache {
    const LATEST: usize = 16;

    pub(crate) fn new(bytes: u64) -> Self {
        Self {
            cache: Arc::new(crate::read_cache::ClockCache::new(
                bytes,
                |value: &Arc<Vector>| {
                    (value.values.len() * size_of::<f32>() + value.attribute_bytes + 256) as u64
                },
            )),
            bytes,
            blocks: Default::default(),
        }
    }

    pub(crate) fn clear(&self) {
        self.cache.clear();
        self.blocks.lock().expect("tail blocks").clear();
    }

    /// Oversized tails still use any cached vectors, but do not churn the cache on every
    /// exhaustive pass. Their transient decodes allocate only the reference decoder's Vec.
    pub(crate) fn fits(&self, rows: usize, dimensions: usize, attribute_bytes: usize) -> bool {
        let per_row = (dimensions as u64)
            .saturating_mul(4)
            .saturating_add(attribute_bytes as u64)
            .saturating_add(256);
        rows as u64 <= self.bytes / per_row
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
            if let Some(vector) = self.cache.get(&key) {
                return Ok(Values::Resident(vector));
            }
            return Ok(Values::Transient(decode_vector(&doc[attribute.as_ref()])?));
        }
        let vector = self
            .cache
            .get_or_try_insert(key, || Vector::new(doc, attribute).map(Arc::new))?;
        Ok(Values::Resident(vector))
    }

    /// The prepared block for this snapshot's documents, built on first use and shared by the
    /// queries that follow on the same snapshot. `identity` names the namespace (any pointer
    /// that is stable between its writes), and `seq` the snapshot. `None` when a document's
    /// vector cannot be decoded: the per-row path then reports the error as before.
    pub(crate) fn block<'a>(
        &self,
        identity: usize,
        seq: u64,
        attribute: &Arc<str>,
        docs: impl ExactSizeIterator<Item = &'a Arc<Document>>,
    ) -> Option<Arc<Block>> {
        let slot = {
            let mut blocks = self.blocks.lock().expect("tail blocks");
            let key = (identity, attribute.clone());
            let stale = blocks
                .get(&key)
                .is_none_or(|slot| slot.seq != seq || slot.rows != docs.len());
            if stale {
                if blocks.len() >= Self::LATEST && !blocks.contains_key(&key) {
                    blocks.clear();
                }
                let slot = Arc::new(Slot {
                    seq,
                    rows: docs.len(),
                    block: OnceLock::new(),
                });
                blocks.insert(key.clone(), slot);
            }
            blocks[&key].clone()
        };
        slot.block
            .get_or_init(|| {
                let rows: Result<Vec<_>, ApiError> = docs
                    .map(|doc| {
                        let vector = if doc.contains_key(attribute.as_ref()) {
                            match self.get(doc, attribute, true)? {
                                Values::Resident(vector) => Some(vector),
                                Values::Transient(_) => unreachable!("admitted reads are resident"),
                            }
                        } else {
                            None
                        };
                        Ok((Arc::as_ptr(doc) as usize, vector))
                    })
                    .collect();
                rows.ok().map(|rows| Arc::new(Block { rows }))
            })
            .clone()
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

    /// The prepared scorer must return the reference distance's exact bits for ordinary,
    /// zero, denormal, huge, and mismatched-length vectors, for both metrics.
    #[test]
    fn prepared_scorer_is_bit_identical_to_the_reference_distance() {
        let cache = VectorCache::new(8 << 20);
        let attribute: Arc<str> = "vector".into();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0
        };
        for dims in [1usize, 3, 15, 16, 17, 64, 100, 1024] {
            let shaped = |scale: f32, zero: bool, next: &mut dyn FnMut() -> f32| -> Vec<f32> {
                (0..dims)
                    .map(|_| if zero { 0.0 } else { next() * scale })
                    .collect()
            };
            let mut vectors = vec![];
            for (scale, zero) in [
                (1.0, false),
                (1.0, true),
                (f32::MAX, false),
                (f32::from_bits(1), false),
                (1e-20, false),
                (3e19, false),
            ] {
                for _ in 0..4 {
                    vectors.push(shaped(scale, zero, &mut next));
                }
            }
            vectors.push(vec![1.0; dims + 1]);
            for metric in [
                DistanceMetric::CosineDistance,
                DistanceMetric::EuclideanSquared,
            ] {
                for query in vectors.iter().filter(|q| q.len() == dims) {
                    let scorer = Scorer::new(metric, query);
                    for candidate in &vectors {
                        if candidate.len() != dims {
                            continue;
                        }
                        let doc: Arc<Document> = Arc::new(
                            serde_json::from_value(json!({"id":1,"vector":candidate})).unwrap(),
                        );
                        let values = cache.get(&doc, &attribute, true).unwrap();
                        let expected = crate::query::distance(candidate, query, metric);
                        let got = scorer.distance(&values);
                        assert_eq!(got.to_bits(), expected.to_bits(), "{metric:?} d={dims}");
                    }
                }
            }
        }
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
        assert!(cache.cache.resident_bytes() <= 64 * 1024);
        cache.clear();
        assert_eq!(cache.cache.resident_bytes(), 0);
        cache.get(&docs[0], &attribute, false).unwrap();
        assert_eq!(cache.cache.resident_bytes(), 0);
        let disabled = VectorCache::new(0);
        disabled.get(&docs[0], &attribute, true).unwrap();
        assert_eq!(disabled.cache.resident_bytes(), 0);
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

    /// A prepared block is shared by queries on one snapshot and rebuilt for the next. After
    /// every kind of tail change (overwrite, delete, patch, schema-less new row), vector,
    /// filtered, `top_k` 10, and `top_k` 100 queries return the reference's ordered IDs and
    /// exact distance values, for both metrics and both storage types.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prepared_block_matches_the_reference_across_tail_changes() {
        use crate::*;
        for metric in [
            DistanceMetric::CosineDistance,
            DistanceMetric::EuclideanSquared,
        ] {
            for dtype in ["[64]f32", "[64]f16"] {
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
                let row = |id: u64, step: u64| {
                    let mut vector: Vec<f32> = (0..64)
                        .map(|d| ((id * 31 + d * 7 + step * 13) % 97) as f32 / 97.0 - 0.4)
                        .collect();
                    if id.is_multiple_of(41) {
                        vector = vec![0.0; 64];
                    }
                    json!({"id":id,"group":id % 5,"vector":vector})
                };
                engine
                    .write(
                        "tail",
                        serde_json::from_value(
                            json!({"distance_metric":metric,"schema":{"vector":{"type":dtype}},
                    "upsert_rows":(0..2300).map(|id| row(id, 0)).collect::<Vec<_>>()}),
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                for step in 0..4u64 {
                    if step > 0 {
                        engine.write("tail", serde_json::from_value(json!({
                            "upsert_rows":(0..40).map(|i| row(step * 100 + i * 3, step)).chain([row(5000 + step, step)]).collect::<Vec<_>>(),
                            "deletes":[step * 7, step * 7 + 1],
                            "patch_rows":[{"id":step * 11 + 2,"group":9}]})).unwrap()).await.unwrap();
                    }
                    let (state, _) = engine.snapshot("tail", &Consistency::Strong).await.unwrap();
                    let context = read_cache::ReadContext::new(engine.0.read_cache.clone());
                    read_cache::scope(context, async {
                        for query in [json!([0.9, 0.1, 0.2]), json!(row(77, 0)["vector"]), json!(vec![0.0f32; 64])] {
                            let query = if query.as_array().unwrap().len() == 3 { json!((0..64).map(|d| (d % 3) as f32 * 0.3 + 0.1).collect::<Vec<f32>>()) } else { query };
                            for total in [10, 100] {
                                for filters in [None, Some(json!(["group", "In", [0, 1, 2]]))] {
                                    let mut body = json!({"rank_by":["vector","ANN",query],"limit":{"total":total},"include_attributes":["group"]});
                                    if let Some(f) = filters { body["filters"] = f; }
                                    let request: QueryRequest = serde_json::from_value(body).unwrap();
                                    let expected = query::execute(&state, &request).unwrap().result.rows;
                                    for _ in 0..2 {
                                        let got = indexed::execute(&store, &state, &request, &config, false).await.unwrap().result.rows;
                                        assert_eq!(got, expected, "{metric:?} {dtype} step {step} top_k {total}");
                                    }
                                }
                            }
                        }
                    }).await;
                }
            }
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
