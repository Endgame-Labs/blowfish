use bf_storage::{CacheConfig, CacheStats, Storage};
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use object_store::{ObjectStoreExt, path::Path};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Value {
    number: Mutex<f64>,
    samples: Mutex<Vec<f64>>,
}
impl CounterFn for Value {
    fn increment(&self, value: u64) {
        *self.number.lock().unwrap() += value as f64;
    }
    fn absolute(&self, value: u64) {
        let mut n = self.number.lock().unwrap();
        *n = n.max(value as f64);
    }
}
impl GaugeFn for Value {
    fn increment(&self, value: f64) {
        *self.number.lock().unwrap() += value;
    }
    fn decrement(&self, value: f64) {
        *self.number.lock().unwrap() -= value;
    }
    fn set(&self, value: f64) {
        *self.number.lock().unwrap() = value;
    }
}
impl HistogramFn for Value {
    fn record(&self, value: f64) {
        self.samples.lock().unwrap().push(value);
    }
}
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<HashMap<Key, Arc<Value>>>>);
impl Capture {
    fn value(&self, key: &Key) -> Arc<Value> {
        self.0
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .clone()
    }
    fn number(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        let key = Key::from_parts(
            name.to_owned(),
            labels
                .iter()
                .map(|(k, v)| metrics::Label::new((*k).to_owned(), (*v).to_owned()))
                .collect::<Vec<_>>(),
        );
        *self.value(&key).number.lock().unwrap()
    }
}
impl Recorder for Capture {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(self.value(key))
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(self.value(key))
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(self.value(key))
    }
}
#[tokio::test]
async fn facade_reports_bytes_hits_evictions_errors_and_lifetimes() {
    // A separate test executable gives the global recorder an isolated process. It also captures
    // filesystem work on Tokio's blocking threads, unlike a thread-local recorder.
    let capture = Capture::default();
    metrics::set_global_recorder(capture.clone()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(object_store::memory::InMemory::new());
    backend
        .put(&Path::from("object"), vec![1u8; 256].into())
        .await
        .unwrap();
    let cfg = CacheConfig {
        block_size: 64,
        memory_bytes: 64,
        disk_bytes: 208,
        disk_dir: Some(dir.path().to_owned()),
        namespace: Some("metrics-test".into()),
        ..Default::default()
    };
    let store = Storage::from_backend_with_cache(
        backend.clone(),
        Path::default(),
        true,
        false,
        cfg.clone(),
    )
    .await
    .unwrap();
    let reader = store
        .range_reader_with_length("object", 256, false)
        .unwrap();
    let stats = CacheStats::default();
    reader.read_with_stats(0..64, &stats).await.unwrap();
    store.flush_cache().await;
    assert_eq!(stats.temperature(), "cold");
    assert_eq!(
        capture.number("blowfish_cache_bytes", &[("tier", "memory")]),
        64.0
    );
    assert_eq!(
        capture.number("blowfish_cache_bytes", &[("tier", "disk")]),
        80.0
    );
    reader.read(0..64).await.unwrap();
    assert_eq!(
        capture.number("blowfish_cache_hits_total", &[("tier", "memory")]),
        1.0
    );
    reader.read(64..256).await.unwrap();
    store.flush_cache().await;
    assert!(capture.number("blowfish_cache_evictions_total", &[("tier", "memory")]) >= 3.0);
    assert_eq!(
        capture.number("blowfish_cache_evictions_total", &[("tier", "disk")]),
        2.0
    );
    assert!(capture.number("blowfish_cache_bytes", &[("tier", "memory")]) <= 64.0);
    assert_eq!(
        capture.number("blowfish_cache_bytes", &[("tier", "disk")]),
        160.0
    );
    assert_eq!(
        capture.number(
            "blowfish_object_store_requests_total",
            &[("op", "get_range"), ("status", "ok")]
        ),
        2.0
    );
    assert_eq!(
        capture.number("blowfish_object_store_bytes_total", &[("op", "get_range")]),
        256.0
    );
    assert!(store.get("missing").await.is_err());
    assert_eq!(
        capture.number(
            "blowfish_object_store_requests_total",
            &[("op", "get"), ("status", "not_found")]
        ),
        1.0
    );
    assert_eq!(store.list("").await.unwrap().len(), 1);
    assert_eq!(
        capture.number(
            "blowfish_object_store_requests_total",
            &[("op", "list"), ("status", "ok")]
        ),
        1.0
    );
    drop(reader);
    drop(store);
    assert_eq!(
        capture.number("blowfish_cache_bytes", &[("tier", "memory")]),
        0.0
    );
    assert_eq!(
        capture.number("blowfish_cache_bytes", &[("tier", "disk")]),
        0.0
    );

    let store =
        Storage::from_backend_with_cache(backend.clone(), Path::default(), false, false, cfg)
            .await
            .unwrap();
    assert_eq!(
        capture.number("blowfish_cache_bytes", &[("tier", "disk")]),
        160.0
    );
    let reader = store
        .range_reader_with_length("object", 256, false)
        .unwrap();
    let stats = CacheStats::default();
    reader.read_with_stats(128..192, &stats).await.unwrap();
    assert_eq!(stats.snapshot().disk_bytes, 64);
    assert_eq!(
        capture.number("blowfish_cache_hits_total", &[("tier", "disk")]),
        1.0
    );
    store.create("immutable", "payload").await.unwrap();
    store.create("immutable", "conflict").await.unwrap();
    assert_eq!(
        capture.number(
            "blowfish_object_store_requests_total",
            &[("op", "put"), ("status", "conflict")]
        ),
        2.0
    ); // Includes the CAS probe.
    store.head("immutable").await.unwrap();
    assert_eq!(
        capture.number(
            "blowfish_object_store_requests_total",
            &[("op", "head"), ("status", "ok")]
        ),
        1.0
    );
    store.delete("immutable").await.unwrap();
    assert_eq!(
        capture.number(
            "blowfish_object_store_requests_total",
            &[("op", "delete"), ("status", "ok")]
        ),
        2.0
    ); // Includes the CAS probe.
    for (key, value) in capture.0.lock().unwrap().iter() {
        if key.name() == "blowfish_object_store_request_seconds" {
            let samples = value.samples.lock().unwrap();
            assert!(!samples.is_empty());
            assert!(samples.iter().all(|s| s.is_finite() && *s >= 0.0));
        }
    }
    drop(reader);
    drop(store);
    assert_eq!(
        capture.number("blowfish_cache_bytes", &[("tier", "memory")]),
        0.0
    );
    assert_eq!(
        capture.number("blowfish_cache_bytes", &[("tier", "disk")]),
        0.0
    );
}
