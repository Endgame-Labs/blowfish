use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_wal::Wal;
use serde_json::{Value, json};
use std::time::Instant;

async fn node(store: &Storage) -> Engine {
    Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    )
}

async fn store() -> Storage {
    Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap()
}

async fn write(engine: &Engine, body: Value) {
    engine
        .write("metadata", serde_json::from_value(body).unwrap())
        .await
        .unwrap();
}

async fn pending(engine: &Engine, rows: u64) {
    let wal = Wal::new(engine.storage().clone(), "metadata").unwrap();
    let through = wal
        .latest_manifest()
        .await
        .unwrap()
        .map_or(0, |(_, manifest)| manifest.indexed_through);
    let bytes: u64 = wal
        .tail(through)
        .await
        .unwrap()
        .iter()
        .map(|(_, entry)| serde_json::to_vec(entry).unwrap().len() as u64)
        .sum();
    assert!(bytes > 0);
    assert_eq!(
        engine.metadata("metadata").await.unwrap()["index"],
        json!({"status":"updating","unindexed_bytes":bytes,"unindexed_rows":rows})
    );
    let response = engine
        .query(
            "metadata",
            serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":10})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.performance.exhaustive_search_count, rows);
}

async fn ready(engine: &Engine) {
    assert_eq!(
        engine.metadata("metadata").await.unwrap()["index"],
        json!({"status":"up-to-date"})
    );
}

#[tokio::test]
async fn metadata_counts_live_tail_rows_and_bytes_until_publication() {
    // Scalar filter indexes and the ID index also need a WAL flush. Flat small-vector
    // indexes are finished indexes, and do not have to wait for a recluster tier.
    for initial in [
        json!({"upsert_rows":[{"id":1,"n":1},{"id":2,"n":2},{"id":3,"n":3}]}),
        json!({"schema":{"n":{"type":"int","filterable":false}},"upsert_rows":[{"id":1,"n":1},{"id":2,"n":2},{"id":3,"n":3}]}),
        json!({"upsert_rows":[{"id":1},{"id":2},{"id":3}]}),
        json!({"distance_metric":"cosine_distance","upsert_rows":[{"id":1,"vector":[1.,0.]},{"id":2,"vector":[0.,1.]},{"id":3,"vector":[1.,1.]}]}),
        json!({"schema":{"text":{"type":"string","full_text_search":true}},"upsert_rows":[{"id":1,"text":"red fish"},{"id":2,"text":"blue fish"},{"id":3,"text":"little fish"}]}),
    ] {
        let engine = node(&store().await).await;
        let replacement = initial["upsert_rows"][0].clone();
        let mut new_row = replacement.clone();
        new_row["id"] = json!(4);
        write(&engine, initial).await;
        pending(&engine, 3).await;
        write(&engine, json!({"upsert_rows":[replacement.clone()]})).await;
        write(&engine, json!({"deletes":[2]})).await;
        pending(&engine, 2).await;
        assert!(engine.index_namespace("metadata", true).await.unwrap());
        ready(&engine).await;
        ready(&node(engine.storage()).await).await;

        // Replacements of checkpointed rows and repeated updates of a tail ID count once.
        write(
            &engine,
            json!({"upsert_rows":[replacement,new_row.clone()]}),
        )
        .await;
        write(&engine, json!({"upsert_rows":[new_row]})).await;
        write(&engine, json!({"deletes":[3]})).await;
        pending(&engine, 2).await;
        assert!(engine.index_namespace("metadata", true).await.unwrap());
        ready(&engine).await;

        // Tombstones leave pending WAL bytes even when no live tail rows remain.
        write(&engine, json!({"deletes":[1,4]})).await;
        pending(&engine, 0).await;
        assert!(engine.index_namespace("metadata", true).await.unwrap());
        ready(&engine).await;
        assert_eq!(
            engine.metadata("metadata").await.unwrap()["approx_row_count"],
            0
        );
    }
}

#[tokio::test]
async fn metadata_poll_observes_another_nodes_index_publication() {
    let store = store().await;
    let writer = node(&store).await;
    let reader = node(&store).await;
    write(&writer, json!({"upsert_rows":[{"id":1},{"id":2}]})).await;
    pending(&reader, 2).await;
    writer.index_namespace("metadata", true).await.unwrap();
    // No subsequent write or retention timeout should be needed to see completion.
    ready(&reader).await;
    write(&writer, json!({"upsert_rows":[{"id":3}]})).await;
    pending(&reader, 1).await;
    writer.index_namespace("metadata", true).await.unwrap();
    ready(&reader).await;
}

#[tokio::test]
async fn metadata_reports_schema_only_work_with_zero_pending_rows() {
    let engine = node(&store().await).await;
    write(
        &engine,
        json!({"upsert_rows":[{"id":1,"text":"little fish"}]}),
    )
    .await;
    engine.index_namespace("metadata", true).await.unwrap();
    ready(&engine).await;
    write(
        &engine,
        json!({"schema":{"text":{"type":"string","full_text_search":true}}}),
    )
    .await;
    pending(&engine, 0).await;
    engine.index_namespace("metadata", true).await.unwrap();
    ready(&engine).await;
}

/// A thread-local userspace instruction counter, so other workers and scheduler stalls do
/// not distort the manual measurement. The kernel copies the 64-byte perf ABI v0 header.
#[cfg(target_os = "linux")]
struct Instructions(std::fs::File);

#[cfg(target_os = "linux")]
impl Instructions {
    fn open() -> Self {
        use std::os::fd::FromRawFd;
        let mut attr = [0u8; 64];
        // PERF_TYPE_HARDWARE = 0, PERF_COUNT_HW_INSTRUCTIONS = 1.
        attr[4..8].copy_from_slice(&64u32.to_ne_bytes());
        attr[8..16].copy_from_slice(&1u64.to_ne_bytes());
        // Exclude kernel and hypervisor execution; the event follows this thread on any CPU.
        attr[40..48].copy_from_slice(&((1u64 << 5) | (1u64 << 6)).to_ne_bytes());
        // SAFETY: attr is a readable perf_event_attr v0 header, and no pointers are retained.
        let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, attr.as_ptr(), 0, -1, -1, 0) };
        assert!(
            fd >= 0,
            "perf_event_open: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: the syscall returned a new, owned file descriptor.
        Self(unsafe { std::fs::File::from_raw_fd(fd as i32) })
    }

    fn read(&mut self) -> u64 {
        let mut bytes = [0; 8];
        std::io::Read::read_exact(&mut self.0, &mut bytes).unwrap();
        u64::from_ne_bytes(bytes)
    }
}

/// Paired before/after measurements include the complete metadata operation, with a warm
/// checkpoint and then a pending tail. On Linux, instruction counts cover only the 200 calls
/// and their timing/assertion harness, excluding namespace construction and warm-up.
#[tokio::test]
#[ignore = "manual metadata latency measurement over 375,000 indexed rows"]
async fn metadata_latency_large_namespace() {
    let engine = node(&store().await).await;
    write(
        &engine,
        json!({"upsert_rows": (0..375_000).map(|id| json!({"id":id,"n":id%17})).collect::<Vec<_>>()}),
    )
    .await;
    engine.index_namespace("metadata", true).await.unwrap();
    for pending in [false, true] {
        if pending {
            write(
                &engine,
                json!({"upsert_rows": (375_000..376_000).map(|id| json!({"id":id,"n":id%17})).collect::<Vec<_>>()}),
            )
            .await;
        }
        for _ in 0..10 {
            std::hint::black_box(engine.metadata("metadata").await.unwrap());
        }
        let mut micros = Vec::new();
        #[cfg(target_os = "linux")]
        let mut instructions = Instructions::open();
        #[cfg(target_os = "linux")]
        let start_instructions = instructions.read();
        for _ in 0..200 {
            let start = Instant::now();
            let metadata = engine.metadata("metadata").await.unwrap();
            micros.push(start.elapsed().as_secs_f64() * 1e6);
            assert_eq!(
                metadata["approx_row_count"],
                if pending { 376_000 } else { 375_000 }
            );
            std::hint::black_box(metadata);
        }
        #[cfg(target_os = "linux")]
        eprintln!(
            "metadata pending={pending} instructions_per_call={}",
            (instructions.read() - start_instructions) / 200
        );
        micros.sort_by(f64::total_cmp);
        eprintln!(
            "metadata pending={pending} p50_us={:.3} p95_us={:.3}",
            micros[100], micros[190]
        );
    }
}
