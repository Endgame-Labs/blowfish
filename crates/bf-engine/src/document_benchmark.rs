//! Reproducible D2/D3 measurements over disposable, explicitly selected scratch stores.
use super::*;
use bf_storage::{Storage, StorageConfig};
use bf_types::{Document, Id, QueryRequest, Schema};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
    time::Instant,
};

fn synth(id: usize) -> Vec<f32> {
    let mut center = (id % 256) as u64 + 123;
    let mut noise = id as u64 + 9_876_543;
    let random = |seed: &mut u64| {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((*seed >> 32) as u32 as f64 / u32::MAX as f64 * 2. - 1.) as f32
    };
    (0..1024)
        .map(|_| random(&mut center) + random(&mut noise) * 0.15)
        .collect()
}
// The NQ harness stores its f16 corpus as NPY despite the extension. Also accept raw bytes.
fn array_file(path: impl AsRef<Path>) -> BufReader<std::fs::File> {
    let mut file = BufReader::new(std::fs::File::open(path).unwrap());
    let mut magic = [0; 8];
    file.read_exact(&mut magic).unwrap();
    if &magic[..6] == b"\x93NUMPY" {
        let len = if magic[6] == 1 {
            let mut bytes = [0; 2];
            file.read_exact(&mut bytes).unwrap();
            u16::from_le_bytes(bytes) as u64
        } else {
            let mut bytes = [0; 4];
            file.read_exact(&mut bytes).unwrap();
            u32::from_le_bytes(bytes) as u64
        };
        file.seek(SeekFrom::Current(len as i64)).unwrap();
    } else {
        file.seek(SeekFrom::Start(0)).unwrap();
    }
    file
}
fn inventory(m: &Manifest) -> Value {
    let mut bytes = BTreeMap::<String, u64>::new();
    for s in &m.segments {
        for (role, f) in &s.files {
            *bytes.entry(role.clone()).or_default() += f.size;
        }
        *bytes.entry("live".into()).or_default() += s.live.size;
    }
    for tiers in m.ann_tiers.values() {
        for set in tiers {
            for slab in &set.slabs {
                for f in slab.files.values() {
                    *bytes.entry("slabs".into()).or_default() += f.size;
                }
            }
            *bytes.entry("directories".into()).or_default() += set.dir.size;
            if let Some(pages) = &set.pages {
                *bytes.entry("directories".into()).or_default() += pages.size;
            }
        }
    }
    for file in m.ann_dirs.values() {
        *bytes.entry("directories".into()).or_default() += file.size;
    }
    let total: u64 = bytes.values().sum();
    let rows: u64 = m.segments.iter().map(|s| u64::from(s.row_count)).sum();
    json!({"rows":rows,"bytes":total,"bytes_per_row":total as f64/rows as f64,"by_role":bytes})
}
async fn select(wal: &Wal, run: &Path, name: &str) {
    let mut m: Manifest =
        serde_json::from_slice(&std::fs::read(run.join(format!("{name}.json"))).unwrap()).unwrap();
    let (seq, latest) = wal.latest_manifest().await.unwrap().unwrap();
    m.indexer_epoch = latest.indexer_epoch;
    wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
}
fn save(run: &Path, name: &str, m: &Manifest) {
    std::fs::write(
        run.join(format!("{name}.json")),
        serde_json::to_vec(m).unwrap(),
    )
    .unwrap();
}

/// Every external invocation must use `taskset -c 6,7 timeout ...`; the caller removes its run
/// directory, including the object store, on both success and failure.
#[tokio::test]
#[ignore = "D2/D3 footprint and latency benchmark; requires a disposable D2_RUN under .scratch"]
async fn document_footprint_benchmark() {
    let run = std::path::PathBuf::from(std::env::var("D2_RUN").expect("D2_RUN"));
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    assert!(run.is_absolute() && run.starts_with(root.join(".scratch")));
    std::fs::create_dir_all(run.join("store")).unwrap();
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", run.join("store").display()),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        crate::EngineConfig {
            indexing: false,
            ann_nprobe: 48,
            ann_rerank_factor: 32,
            recall_sample_rate: 0.0,
            compaction_segments: 100,
            max_segment_rows: 200_000,
            ..Default::default()
        },
    );
    let wal = Wal::new(store.clone(), "t").unwrap();
    let action = std::env::var("D2_ACTION").unwrap_or_else(|_| "query".into());
    if action == "build" {
        let rows: usize = std::env::var("D2_ROWS")
            .unwrap_or_else(|_| "200000".into())
            .parse()
            .unwrap();
        let schema: Schema = serde_json::from_value(json!({
            "id":{"type":"uint"},"vector":{"type":"[1024]f16","ann":true},
            "title":{"type":"string"},"text":{"type":"string","full_text_search":true},
            "bucket":{"type":"uint","filterable":true}
        }))
        .unwrap();
        engine.write("t",serde_json::from_value(json!({"schema":schema,"distance_metric":"cosine_distance","upsert_rows":[{"id":0,"vector":synth(0),"text":"fish","title":"fish","bucket":0}]})).unwrap()).await.unwrap();
        engine.index_namespace("t", true).await.unwrap();
        let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
        m.segments.clear();
        m.ann_dirs.clear();
        m.stats.rows = rows as u64;
        let mut corpus = std::env::var("D2_CORPUS").ok().map(array_file);
        let mut attrs = std::env::var("D2_ATTRS")
            .ok()
            .map(|p| BufReader::new(std::fs::File::open(p).unwrap()).lines());
        let start = Instant::now();
        for first in (0..rows).step_by(100_000) {
            let end = (first + 100_000).min(rows);
            let mut docs = BTreeMap::<Id, Document>::new();
            let mut packed = segment::PackedColumn::new(1024, bf_vector::VectorType::F16);
            for id in first..end {
                let mut doc: Document = if let Some(lines) = &mut attrs {
                    serde_json::from_str(&lines.next().unwrap().unwrap()).unwrap()
                } else {
                    serde_json::from_value(json!({"title":format!("Page {}",id%1000),"text":format!("Page {} describes fish, water, and habitats. ",id%1000).repeat(10)})).unwrap()
                };
                doc.insert("id".into(), json!(id));
                doc.insert("bucket".into(), json!(id % 16));
                docs.insert(Id::Uint(id as u64), doc);
                if let Some(file) = &mut corpus {
                    let start = packed.data.len();
                    packed.data.resize(start + 2048, 0);
                    file.read_exact(&mut packed.data[start..]).unwrap();
                    packed.rows.push((id - first) as u32);
                } else {
                    packed.push((id - first) as u32, &synth(id)).unwrap();
                }
            }
            let seg = segment::build_with(
                &store,
                "t",
                &schema,
                bf_types::DistanceMetric::CosineDistance,
                &docs,
                BTreeMap::from([("vector".into(), packed)]),
                1,
                1,
            )
            .await
            .unwrap();
            m.segments.push(seg);
            eprintln!(
                "D2 build {end}/{rows}, {:.1}s",
                start.elapsed().as_secs_f64()
            );
        }
        let file = ann_dir::build(
            &store,
            "t",
            &m.segments,
            "vector",
            ann_dir::Layout::Segments,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        m.ann_dirs.insert("annv2:vector".into(), file);
        wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
        save(&run, "current", &m);
        println!(
            "D2 {}",
            json!({"stage":"current","inventory":inventory(&m),"build_seconds":start.elapsed().as_secs_f64()})
        );
        for seg in &mut m.segments {
            segment::upgrade::make_legacy(&store, seg).await.unwrap();
        }
        save(&run, "legacy", &m);
        select(&wal, &run, "legacy").await;
        println!("D2 {}", json!({"stage":"legacy","inventory":inventory(&m)}));
    } else if action == "legacy" || action == "current" {
        select(&wal, &run, &action).await;
    } else if action == "upgrade" {
        let r = engine.reindex("t", &Default::default()).await.unwrap();
        let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
        println!(
            "D2 {}",
            json!({"stage":"upgrade","objects":r.converted_documents,"bytes_written":r.bytes_written,"seconds":r.seconds,"publications":r.publications,"inventory":inventory(&m)})
        );
    } else if action == "recluster" {
        select(&wal, &run, "current").await;
        let r = engine
            .recluster(
                "t",
                &crate::ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 6,
                    cluster_rows: 200,
                    max_slab_rows: 60_000,
                    scratch: run.join("spill"),
                },
            )
            .await
            .unwrap();
        let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
        save(&run, "current", &m);
        let legacy: Manifest =
            serde_json::from_slice(&std::fs::read(run.join("legacy.json")).unwrap()).unwrap();
        let mut old = m.clone();
        for seg in &mut old.segments {
            let source = legacy.segments.iter().find(|s| s.id == seg.id).unwrap();
            for role in ["docs", "ids"] {
                seg.files.insert(role.into(), source.files[role].clone());
            }
        }
        save(&run, "legacy", &old);
        println!(
            "D2 {}",
            json!({"stage":"recluster","seconds":r.seconds,"current":inventory(&m),"legacy":inventory(&old)})
        );
    } else if action == "compact" {
        let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
        let start = Instant::now();
        let strip = BTreeSet::from(["vector".into()]);
        let mut docs = BTreeMap::new();
        let mut parts: BTreeMap<String, Vec<segment::PackedPart>> = BTreeMap::new();
        for seg in m.segments.iter().take(2) {
            let (d, v) =
                segment::compaction_rows(&store, seg, &(0..seg.row_count).collect(), &strip)
                    .await
                    .unwrap();
            docs.extend(d);
            for (name, p) in v {
                parts.entry(name).or_default().push(p);
            }
        }
        let read_seconds = start.elapsed().as_secs_f64();
        let packed = segment::merge_packed(&m.schema, &docs, parts).unwrap();
        let seg = segment::build_with(
            &store,
            "t",
            &m.schema,
            m.distance_metric,
            &docs,
            packed,
            1,
            1,
        )
        .await
        .unwrap();
        println!(
            "D2 {}",
            json!({"stage":"compact","rows":seg.row_count,"read_seconds":read_seconds,"seconds":start.elapsed().as_secs_f64(),"bytes_written":seg.files.values().map(|f|f.size).sum::<u64>()})
        );
    } else if action == "query" {
        let count: usize = std::env::var("D2_QUERIES")
            .unwrap_or_else(|_| "64".into())
            .parse()
            .unwrap();
        let offset: usize = std::env::var("D2_QUERY_OFFSET")
            .unwrap_or_else(|_| "0".into())
            .parse()
            .unwrap();
        let queries: Vec<Vec<f32>> = if let Ok(path) = std::env::var("D2_QUERIES_F32") {
            let mut bytes = Vec::new();
            array_file(path)
                .take(((offset + count) * 4096) as u64)
                .read_to_end(&mut bytes)
                .unwrap();
            bytes
                .as_chunks::<4096>()
                .0
                .iter()
                .skip(offset)
                .take(count)
                .map(|v| {
                    v.as_chunks::<4>()
                        .0
                        .iter()
                        .map(|x| f32::from_le_bytes(*x))
                        .collect()
                })
                .collect()
        } else {
            (offset..offset + count)
                .map(|id| synth(200_000 + id * 7919))
                .collect()
        };
        let mut requests = Vec::new();
        for (i, v) in queries.into_iter().enumerate() {
            for (kind, filtered, projected) in [
                ("ann", false, false),
                ("filtered", true, false),
                ("vector", false, true),
                ("text", false, false),
            ] {
                if std::env::var("D2_KIND").is_ok_and(|wanted| wanted != kind) {
                    continue;
                }
                let mut q =
                    json!({"rank_by":["vector","ANN",v],"top_k":10,"include_attributes":false});
                if filtered {
                    q["filters"] = json!(["bucket", "Eq", 3]);
                }
                if projected {
                    q["include_attributes"] = json!(["vector"]);
                    q["vector_encoding"] = json!("base64");
                }
                if kind == "text" {
                    q["include_attributes"] = json!(["title", "text"]);
                }
                requests.push((i, kind, serde_json::from_value::<QueryRequest>(q).unwrap()));
            }
        }
        if std::env::var("D2_WARMUP").is_ok() {
            for (_, _, q) in &requests {
                engine.query("t", q.clone()).await.unwrap();
            }
        }
        for (i, kind, q) in requests {
            let start = Instant::now();
            let (response, d) = engine.query_with_diagnostics("t", q).await.unwrap();
            assert_eq!(d.document_scan_rows, 0);
            println!(
                "D2 {}",
                json!({"stage":"query","i":i,"kind":kind,"ms":start.elapsed().as_secs_f64()*1000.,"ann_rows":d.ann_rows_scanned,"document_rows":d.document_scan_rows,"backend_requests":d.backend.iter().map(|b|b.requests).sum::<u64>(),"backend_bytes":d.backend.iter().map(|b|b.bytes).sum::<u64>(),"rows":response.result.rows})
            );
        }
    } else {
        panic!("unknown action {action}");
    }
}
