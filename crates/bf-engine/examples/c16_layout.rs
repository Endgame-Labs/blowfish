//! Round C16: dump a tier's slabs and leaf ranges (and optionally exact leaf centers) for offline layout study.
//! Usage: c16_layout URL NAMESPACE OUT_DIR (read-only; OUT_DIR must be under .scratch/).
use bf_storage::{Storage, StorageConfig};
use bf_vector::Directory;
use bf_wal::Wal;
use std::{error::Error, io::Write, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("expected URL NAMESPACE OUT_DIR".into());
    }
    let out = PathBuf::from(&args[3]);
    if !out.components().any(|p| p.as_os_str() == ".scratch") {
        return Err("output must be under .scratch/".into());
    }
    std::fs::create_dir_all(&out)?;
    let store = Storage::open(&StorageConfig {
        url: args[1].clone(),
        region: std::env::var("AWS_REGION")
            .ok()
            .or(Some("us-west-2".into())),
        profile: std::env::var("AWS_PROFILE").ok(),
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
    eprintln!("tiers {}", tiers.len());
    for (t, tier) in tiers.iter().enumerate() {
        eprintln!(
            "tier {t}: slabs {} rows {} dir {} bytes pages {:?}",
            tier.slabs.len(),
            tier.rows(),
            tier.dir.size,
            tier.pages.as_ref().map(|p| p.size)
        );
        let dir = Directory::decode(&store.get_range(&tier.dir.key, 0..tier.dir.size).await?)?;
        eprintln!("entries {} dims {}", dir.entries().len(), dir.dims());
        let mut f = std::io::BufWriter::new(std::fs::File::create(
            out.join(format!("tier{t}.entries.tsv")),
        )?);
        for e in dir.entries() {
            let s = &dir.segments()[e.segment];
            let v = e.vectors_range();
            let b = e.blob_range();
            writeln!(
                f,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                e.segment, e.cluster, e.rows, v.start, v.end, b.start, b.end, s.key, s.fragment
            )?;
        }
        f.flush()?;
        // Exact rotated leaf centers (the first `padded` f32 of each leaf blob), in entry order.
        use futures::StreamExt;
        let padded = dir.dims().next_power_of_two();
        let jobs: Vec<_> = dir
            .entries()
            .iter()
            .map(|e| {
                (
                    dir.segments()[e.segment].fragment.clone(),
                    e.blob_range().start,
                )
            })
            .collect();
        let mut co = std::io::BufWriter::new(std::fs::File::create(
            out.join(format!("tier{t}.centers.f32")),
        )?);
        let mut stream = futures::stream::iter(jobs.into_iter().map(|(key, start)| {
            let store = store.clone();
            async move {
                store
                    .get_range(&key, start..start + padded as u64 * 4)
                    .await
            }
        }))
        .buffered(256);
        let mut n = 0usize;
        while let Some(r) = stream.next().await {
            co.write_all(&r?)?;
            n += 1;
            if n.is_multiple_of(50_000) {
                eprintln!("centers {n}");
            }
        }
        co.flush()?;
    }
    Ok(())
}
