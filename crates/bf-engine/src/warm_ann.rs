//! A generation-scoped compiled tier, populated entirely by ordinary query reads.
use crate::read_cache;
use bf_vector::{CompiledTier, Directory, ResidentQueryScratch, ValidatedVector, VectorRecorder};
use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

pub(crate) struct WarmTier {
    pub codes: CompiledTier,
    vectors: moka::sync::Cache<u32, ValidatedVector>,
    ready: AtomicBool,
    pub rows: usize,
    #[cfg(test)]
    hits: std::sync::atomic::AtomicUsize,
}
pub(crate) struct Found {
    pub winners: Vec<(f32, usize, u32)>,
    pub scanned_rows: usize,
}
struct Seen(AtomicU8);
thread_local! {
    static SCRATCH: RefCell<ResidentQueryScratch> = RefCell::new(ResidentQueryScratch::default());
}
impl WarmTier {
    /// A first query uses only the normal cold path. A second query can reserve a compiled
    /// view if its complete code working set and sparse-vector cache fit the decoded budget.
    pub fn for_tier(key: &str, directory: Arc<Directory>) -> Option<Arc<Self>> {
        if let Some(warm) = read_cache::get::<Self>(key, 0) {
            return Some(warm);
        }
        if read_cache::capacity() == 0 {
            return None;
        }
        let seen = read_cache::get::<Seen>(key, 0)
            .or_else(|| read_cache::insert_once(key, 64, Seen(AtomicU8::new(0))))?;
        if seen.0.load(Ordering::Relaxed) == 2
            || seen
                .0
                .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            return None;
        }
        // Paged directories list a query-specific subset of leaves, so they retain their
        // normal reader. A compiled view always describes the complete immutable tier.
        let listed: usize = directory.entries().iter().map(|e| e.rows as usize).sum();
        let total: usize = directory.segment_totals().iter().map(|(n, _)| n).sum();
        let vector_budget = CompiledTier::code_bytes(&directory);
        let reservation = CompiledTier::reservation(&directory).saturating_add(vector_budget);
        if listed != total || total <= 2048 || reservation > read_cache::capacity() {
            seen.0.store(2, Ordering::Relaxed);
            return None;
        }
        let codes = match CompiledTier::new(directory) {
            Ok(codes) => codes,
            Err(_) => {
                seen.0.store(2, Ordering::Relaxed);
                return None;
            }
        };
        let vectors = moka::sync::Cache::builder()
            .max_capacity(vector_budget)
            .weigher(|_: &u32, v: &ValidatedVector| {
                v.resident_bytes().min(u64::from(u32::MAX)) as u32
            })
            .build();
        read_cache::insert_once(
            key,
            reservation,
            Self {
                codes,
                vectors,
                ready: AtomicBool::new(false),
                rows: total,
                #[cfg(test)]
                hits: std::sync::atomic::AtomicUsize::new(0),
            },
        )
    }

    /// The complete scoring pipeline runs on the query's compute worker. Scratch is released
    /// before hydration can yield; in-flight queries retain this generation's immutable Arc.
    pub fn search(
        &self,
        vector: &[f32],
        nprobe: usize,
        rerank_factor: usize,
        k: usize,
    ) -> bf_vector::Result<Option<Found>> {
        if !self.ready.load(Ordering::Acquire) {
            return Ok(None);
        }
        SCRATCH.with(|scratch| {
            let mut scratch = scratch.borrow_mut();
            Ok(self
                .codes
                .try_search(
                    vector,
                    nprobe,
                    rerank_factor,
                    k,
                    |row| self.vectors.get(&row),
                    &mut scratch,
                )?
                .map(|found| {
                    #[cfg(test)]
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    Found {
                        winners: found.hits.to_vec(),
                        scanned_rows: found.scanned_rows,
                    }
                }))
        })
    }
}
impl VectorRecorder for WarmTier {
    fn record(&self, vector: ValidatedVector) {
        self.vectors.insert(vector.row(), vector);
        self.ready.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Engine, EngineConfig, ReclusterOptions};
    use bf_storage::{Storage, StorageConfig};
    use bf_types::{Document, QueryRequest, WriteRequest};
    use serde_json::{Value, json};

    fn vector(id: usize) -> Vec<f32> {
        (0..32)
            .map(|d| {
                if id.is_multiple_of(200) {
                    0.
                } else {
                    ((id * 7919 + d * 104729) as f64).sin() as f32
                }
            })
            .collect()
    }
    fn engine(store: &Storage, decoded_cache_bytes: u64) -> Engine {
        Engine::with_config(
            store.clone(),
            EngineConfig {
                indexing: false,
                recall_sample_rate: 0.,
                decoded_cache_bytes,
                ..Default::default()
            },
        )
    }
    async fn query(engine: &Engine, body: Value) -> Vec<Document> {
        engine
            .query(
                "compiled",
                serde_json::from_value::<QueryRequest>(body).unwrap(),
            )
            .await
            .unwrap()
            .result
            .rows
            .unwrap()
    }
    async fn current(engine: &Engine, key: &str) -> Option<Arc<WarmTier>> {
        read_cache::scope(
            read_cache::ReadContext::new(engine.0.read_cache.clone()),
            async { read_cache::get(key, 0) },
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compiled_queries_preserve_snapshots_masks_projections_and_cache_eviction() {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let writer = engine(&store, 64 << 20);
        for batch in 0..2 {
            let docs: Vec<_> = (batch * 3000..(batch + 1) * 3000)
                .map(|id| {
                    json!({
                        "id":format!("row-{:05}", 6000-id), "vector":vector(id), "tag":id % 4,
                    })
                })
                .collect();
            writer.write("compiled", serde_json::from_value::<WriteRequest>(json!({
                "distance_metric":"cosine_distance", "schema":{"vector":{"type":"[32]f16"}}, "upsert_rows":docs,
            })).unwrap()).await.unwrap();
            writer.index_namespace("compiled", true).await.unwrap();
        }
        let scratch = tempfile::tempdir().unwrap();
        writer
            .recluster(
                "compiled",
                &ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 1,
                    cluster_rows: 60,
                    max_slab_rows: 10000,
                    scratch: scratch.path().to_owned(),
                },
            )
            .await
            .unwrap();
        let wal = bf_wal::Wal::new(store.clone(), "compiled").unwrap();
        let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
        let key = &manifest.ann_tiers["vector"][0].dir.key;
        let normal = engine(&store, 0);
        let warm = engine(&store, 64 << 20);
        let request = |id| json!({"rank_by":["vector","ANN",vector(id)],"top_k":10,"include_attributes":["id","tag"]});
        let first = query(&warm, request(17)).await;
        assert_eq!(first, query(&normal, request(17)).await);
        assert!(
            current(&warm, key).await.is_none(),
            "a first cold query must not reserve a tier"
        );
        for id in [17, 2111, 5999, 0] {
            let want = query(&normal, request(id)).await;
            for _ in 0..4 {
                assert_eq!(query(&warm, request(id)).await, want);
            }
        }
        let compiled = current(&warm, key).await.expect("hot tier admitted");
        assert!(
            compiled.hits.load(Ordering::Relaxed) > 0,
            "exercise the real compiled branch"
        );
        // A concurrent reader cannot return an old speculative result over an unindexed write.
        writer.write("compiled", serde_json::from_value::<WriteRequest>(json!({
            "deletes":["row-05983"], "upsert_rows":[{"id":"new","vector":vector(17),"tag":1}],
        })).unwrap()).await.unwrap();
        let got = query(&warm, request(17)).await;
        assert_eq!(got, query(&normal, request(17)).await);
        assert!(got.iter().all(|row| row["id"] != "row-05983"));
        assert!(got.iter().any(|row| row["id"] == "new"));
        // Indexed masks and uncovered segments use the existing query plan.
        writer.index_namespace("compiled", true).await.unwrap();
        warm.0.namespaces.lock().await.clear();
        normal.0.namespaces.lock().await.clear();
        let before_mask = compiled.hits.load(Ordering::Relaxed);
        assert_eq!(
            query(&warm, request(17)).await,
            query(&normal, request(17)).await
        );
        assert_eq!(
            compiled.hits.load(Ordering::Relaxed),
            before_mask,
            "masked tier bypasses the fully live compiled plan"
        );
        let filtered = json!({"rank_by":["vector","ANN",vector(17)],"top_k":20,"filters":["tag","Eq",1],"include_attributes":["tag"]});
        assert_eq!(
            query(&warm, filtered.clone()).await,
            query(&normal, filtered).await
        );
        let large = json!({"rank_by":["vector","ANN",vector(0)],"top_k":1000});
        assert_eq!(
            query(&warm, large.clone()).await,
            query(&normal, large).await
        );
        warm.0.read_cache.clear();
        assert!(current(&warm, key).await.is_none());
        assert_eq!(
            query(&warm, request(17)).await,
            query(&normal, request(17)).await
        );
        let tiny = engine(&store, 128);
        for _ in 0..3 {
            assert_eq!(
                query(&tiny, request(2111)).await,
                query(&normal, request(2111)).await
            );
        }
        assert!(
            current(&tiny, key).await.is_none(),
            "oversized tiers are never retained"
        );
    }
}
