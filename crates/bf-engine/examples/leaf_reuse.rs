//! Upper-bound the opportunity for four-query leaf reuse in a recorded HTTP arrival stream.
//! Usage: leaf_reuse STORE NAMESPACE QUERIES_F32 ARRIVALS_JSON PROBES
//! This measures reuse and queue delay, rather than pretending an unimplemented batch kernel wins.
use bf_storage::{Storage, StorageConfig};
use bf_vector::Directory;
use bf_wal::Wal;
use std::{collections::BTreeSet, error::Error, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 6 {
        return Err("expected STORE NAMESPACE QUERIES_F32 ARRIVALS_JSON PROBES".into());
    }
    let root = PathBuf::from(&args[1]).canonicalize()?;
    let probes: usize = args[5].parse()?;
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", root.display()),
        read_only: true,
        ..Default::default()
    })
    .await?;
    let manifest = Wal::new(store.clone(), &args[2])?
        .latest_manifest()
        .await?
        .ok_or("missing manifest")?
        .1;
    let tiers = manifest.ann_tiers.get("vector").ok_or("missing tier")?;
    let [tier] = tiers.as_slice() else {
        return Err("requires one final tier".into());
    };
    let dir = Directory::decode(
        &store
            .get_range(&tier.dir.key, 0..tier.dir.metadata_len)
            .await?,
    )?;
    let bytes = std::fs::read(&args[3])?;
    if !bytes.len().is_multiple_of(dir.dims() * 4) {
        return Err("partial query vector".into());
    }
    let queries: Vec<Vec<f32>> = bytes
        .chunks_exact(dir.dims() * 4)
        .map(|q| {
            q.as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect()
        })
        .collect();
    let mut arrivals: Vec<(i64, usize)> = serde_json::from_slice(&std::fs::read(&args[4])?)?;
    arrivals.sort_unstable();
    if arrivals.len() != queries.len() || arrivals.iter().any(|(_, id)| *id >= queries.len()) {
        return Err("arrival/query mismatch".into());
    }
    let entries = dir.entries();
    let rows: usize = entries.iter().map(|e| e.rows as usize).sum();
    let probes = ((probes as f64 * (entries.len() as f64 / (rows as f64).sqrt()).max(1.)).ceil()
        as usize)
        .min(entries.len());
    let mut selected = Vec::with_capacity(queries.len());
    for q in &queries {
        let rotated = dir.rotate_query(q)?;
        let distances = dir.center_distances_rotated(&rotated, false)?;
        let mut order: Vec<_> = (0..entries.len())
            .filter(|i| entries[*i].rows > 0)
            .collect();
        order.sort_unstable_by(|a, b| distances[*a].total_cmp(&distances[*b]).then(a.cmp(b)));
        let mut count = 0;
        let mut scanned = 0;
        for i in &order {
            count += 1;
            scanned += entries[*i].rows as usize;
            if count >= probes && scanned >= 320.min(rows) {
                break;
            }
        }
        order.truncate(count);
        selected.push(order);
    }
    let (mut separate, mut unique) = (0u64, 0u64);
    let mut delays = Vec::new();
    for batch in arrivals.chunks(4) {
        let ready = batch.last().ok_or("empty batch")?.0;
        let mut union = BTreeSet::new();
        for &(at, id) in batch {
            delays.push((ready - at) as f64 / 1e6);
            for &leaf in &selected[id] {
                let range = entries[leaf].codes_range();
                separate += range.end - range.start;
                union.insert(leaf);
            }
        }
        for leaf in union {
            let range = entries[leaf].codes_range();
            unique += range.end - range.start;
        }
    }
    delays.sort_by(f64::total_cmp);
    println!(
        "{}",
        serde_json::json!({"queries":queries.len(),"batch":4,"leaf_probes":probes,
        "separate_code_bytes":separate,"unique_code_bytes":unique,"shared_fraction":1.-unique as f64/separate as f64,
        "maximum_load_speedup":separate as f64/unique as f64,
        "queue_p50_ms":delays[delays.len()/2],"queue_p90_ms":delays[delays.len()*9/10]})
    );
    Ok(())
}
