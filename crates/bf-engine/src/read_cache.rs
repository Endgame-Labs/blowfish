//! Engine-owned decoded metadata LRU and query-local range accounting.
use bf_storage::CacheStats;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

type Key = (TypeId, String);
struct Entry {
    value: Arc<dyn Any + Send + Sync>,
    bytes: u64,
    used: u64,
}
#[derive(Default)]
struct Entries {
    values: HashMap<Key, Entry>,
    bytes: u64,
    clock: u64,
}
/// Immutable keys include the object generation. In-flight users retain their own Arc.
pub(crate) struct DecodedCache {
    budget: u64,
    entries: Mutex<Entries>,
    pub workers: Arc<Semaphore>,
}
impl DecodedCache {
    pub fn new(budget: u64, workers: usize) -> Self {
        Self {
            budget,
            entries: Mutex::new(Entries::default()),
            workers: Arc::new(Semaphore::new(workers.max(1))),
        }
    }
    fn get<T: Any + Send + Sync>(&self, key: &str) -> Option<Arc<T>> {
        let mut entries = self.entries.lock().expect("decoded cache lock");
        entries.clock += 1;
        let used = entries.clock;
        let entry = entries.values.get_mut(&(TypeId::of::<T>(), key.into()))?;
        entry.used = used;
        entry.value.clone().downcast().ok()
    }
    fn insert<T: Any + Send + Sync>(&self, key: &str, bytes: u64, value: Arc<T>) {
        if bytes > self.budget || self.budget == 0 {
            return;
        }
        let mut retired = Vec::new();
        let mut entries = self.entries.lock().expect("decoded cache lock");
        let key = (TypeId::of::<T>(), key.into());
        if let Some(old) = entries.values.remove(&key) {
            entries.bytes -= old.bytes;
            retired.push(old);
        }
        while entries.bytes > self.budget - bytes {
            let oldest = entries
                .values
                .iter()
                .min_by_key(|(_, v)| v.used)
                .map(|(k, _)| k.clone())
                .expect("nonempty over-budget cache");
            let old = entries.values.remove(&oldest).expect("LRU entry");
            entries.bytes -= old.bytes;
            retired.push(old);
        }
        entries.clock += 1;
        let used = entries.clock;
        entries.bytes += bytes;
        entries.values.insert(key, Entry { value, bytes, used });
        drop(entries);
        // Large posting directories can own millions of allocations. Their
        // destruction must not serialize unrelated cache lookups or insertions.
        drop(retired);
    }
}
#[derive(Clone)]
pub(crate) struct ReadContext {
    pub cache: Arc<DecodedCache>,
    pub stats: Arc<CacheStats>,
    pub backend: Arc<bf_storage::BackendStats>,
}
tokio::task_local! {
    static CURRENT: ReadContext;
}
pub(crate) fn context() -> Option<ReadContext> {
    CURRENT.try_with(Clone::clone).ok()
}
pub(crate) async fn scope<T>(context: ReadContext, future: impl Future<Output = T>) -> T {
    let backend = context.backend.clone();
    backend
        .scope("search", CURRENT.scope(context, future))
        .await
}
pub(crate) fn stats() -> Arc<CacheStats> {
    CURRENT.try_with(|c| c.stats.clone()).unwrap_or_default()
}
pub(crate) fn get<T: Any + Send + Sync>(key: &str, logical_bytes: u64) -> Option<Arc<T>> {
    let context = context()?;
    let value = context.cache.get(key)?;
    context.stats.record_memory_hit(logical_bytes);
    Some(value)
}
pub(crate) fn insert<T: Any + Send + Sync>(key: &str, bytes: u64, value: T) -> Arc<T> {
    let value = Arc::new(value);
    if let Some(context) = context() {
        context.cache.insert(key, bytes, value.clone());
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_budget_lru_type_separation_and_pinned_users() {
        let cache = DecodedCache::new(10, 1);
        let pinned = Arc::new(1u64);
        cache.insert("a", 4, pinned.clone());
        cache.insert("b", 4, Arc::new(2u64));
        assert_eq!(*cache.get::<u64>("a").unwrap(), 1);
        assert!(cache.get::<u32>("a").is_none());
        cache.insert("c", 4, Arc::new(3u64));
        assert!(cache.get::<u64>("b").is_none());
        cache.insert("oversize", 11, Arc::new(4u64));
        assert!(cache.get::<u64>("oversize").is_none());
        cache.insert("a", 8, Arc::new(5u64));
        assert!(cache.get::<u64>("c").is_none());
        assert_eq!(*pinned, 1);
        assert_eq!(cache.entries.lock().unwrap().bytes, 8);
    }

    #[test]
    fn retired_metadata_drops_do_not_block_cache_hits() {
        use std::{sync::mpsc, thread, time::Duration};

        struct SlowDrop {
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }
        impl Drop for SlowDrop {
            fn drop(&mut self) {
                let _ = self.entered.send(());
                let _ = self.release.lock().unwrap().recv();
            }
        }

        let cache = Arc::new(DecodedCache::new(1, 1));
        let (entered, dropping) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        cache.insert(
            "old",
            1,
            Arc::new(SlowDrop {
                entered,
                release: Mutex::new(resume),
            }),
        );
        let writer = cache.clone();
        let writer = thread::spawn(move || writer.insert("new", 1, Arc::new(9u64)));
        dropping.recv_timeout(Duration::from_secs(5)).unwrap();
        let (done, result) = mpsc::channel();
        let reader = thread::spawn(move || {
            done.send(cache.get::<u64>("new").map(|value| *value))
                .unwrap();
        });
        let observed = result.recv_timeout(Duration::from_secs(5));
        // Release the destructor before asserting, so a regression cannot leave
        // either thread stuck while the test unwinds.
        release.send(()).unwrap();
        writer.join().unwrap();
        reader.join().unwrap();
        assert!(matches!(observed, Ok(Some(9))));
    }
}
