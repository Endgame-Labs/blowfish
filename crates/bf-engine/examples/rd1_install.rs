//! Round RD1: time one full recluster of a namespace and fingerprint every exported vector.
//! Usage: rd1_install storage.json NAMESPACE SCRATCH_DIR OUTPUT.json
//! The storage must be an owned copy (the recluster writes a new tier and manifest into it).
//! The fingerprints are order-independent sums of SHA-256 over each leaf row's stored vector
//! bytes, with and without the row id, so two builds that export identical vectors match.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use bf_wal::Wal;
use sha2::{Digest, Sha256};
use std::error::Error;

async fn fingerprint(store: &Storage, ns: &str) -> Result<(String, String, u64), Box<dyn Error>> {
    let wal = Wal::new(store.clone(), ns)?;
    let manifest = wal.latest_manifest().await?.ok_or("no manifest")?.1;
    let (mut keyed, mut free, mut rows) = (0u128, 0u128, 0u64);
    for set in manifest.ann_tiers.get("vector").into_iter().flatten() {
        for slab in &set.slabs {
            let file = &slab.files["annv2:vector"];
            let blob = store.get(&file.key).await?;
            let index = bf_vector::Index::open_with_metadata_len(blob, file.metadata_len).await?;
            for cluster in 0..index.cluster_count() {
                let (ids, bytes) = index.read_cluster_rows(cluster).await?;
                let width = index.vector_row_bytes();
                for (id, row) in ids.iter().zip(bytes.chunks_exact(width)) {
                    let head = |h: sha2::digest::Output<Sha256>| {
                        u128::from_le_bytes(h[..16].try_into().unwrap())
                    };
                    free = free.wrapping_add(head(Sha256::digest(row)));
                    keyed = keyed.wrapping_add(head(
                        Sha256::new()
                            .chain_update(id.to_le_bytes())
                            .chain_update(row)
                            .finalize(),
                    ));
                    rows += 1;
                }
            }
        }
    }
    Ok((format!("{keyed:032x}"), format!("{free:032x}"), rows))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        return Err("usage: rd1_install storage.json NAMESPACE SCRATCH_DIR OUTPUT.json".into());
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let ns = args[2].as_str();
    let store = Storage::open(&config).await?;
    let engine = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            ..EngineConfig::from_env()
        },
    );
    let before = fingerprint(&store, ns).await?;
    eprintln!("RD1 before rows={} keyed={} free={}", before.2, before.0, before.1);
    let start = std::time::Instant::now();
    let report = engine
        .recluster(
            ns,
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 32,
                cluster_rows: 0,
                max_slab_rows: 250_000,
                scratch: args[3].clone().into(),
            },
        )
        .await?;
    let seconds = start.elapsed().as_secs_f64();
    let after = fingerprint(&store, ns).await?;
    eprintln!("RD1 after rows={} keyed={} free={}", after.2, after.0, after.1);
    std::fs::write(
        &args[4],
        serde_json::json!({
            "install_seconds": seconds, "build_seconds": report.seconds, "rows": report.rows,
            "slabs": report.slabs, "leaves": report.clusters,
            "before": {"rows": before.2, "keyed": before.0, "free": before.1},
            "after": {"rows": after.2, "keyed": after.0, "free": after.1},
        })
        .to_string(),
    )?;
    println!("RD1 installed seconds={seconds:.3} rows={} slabs={}", report.rows, report.slabs);
    Ok(())
}
