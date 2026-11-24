//! Rebuild a private benchmark copy with an explicit target leaf size.
//!
//! Usage: recluster_copy storage.json namespace cluster_rows|auto scratch_dir [--upgrade-only]
//! The storage URL must use an owned `tmp/` prefix, and the scratch directory must
//! be under `.scratch/`. Copy a quiescent checkpoint first, and delete the owned
//! prefix after measurement. Existing scale prefixes are never writable here.
//! Use `--upgrade-only` to prepare an unchanged-center control before copying it
//! and rebuilding leaves. The normal run upgrades old objects before rebuilding.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use std::{error::Error, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(args.len() == 4 || (args.len() == 5 && args[4] == "--upgrade-only")) {
        return Err(
            "expected storage.json namespace cluster_rows|auto scratch_dir [--upgrade-only]".into(),
        );
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    if !owned_s3_prefix(&config.url) || config.read_only {
        return Err("rebuild requires writable S3 storage under an owned tmp/ prefix".into());
    }
    let scratch = PathBuf::from(&args[3]);
    if !scratch.components().any(|c| c.as_os_str() == ".scratch") {
        return Err("scratch directory must be beneath .scratch/".into());
    }
    std::fs::create_dir_all(&scratch)?;
    let scratch = scratch.canonicalize()?;
    if !scratch.components().any(|c| c.as_os_str() == ".scratch") {
        return Err("scratch directory must be beneath .scratch/".into());
    }
    let cluster_rows = match args[2].as_str() {
        "auto" => 0,
        value => {
            let rows: usize = value.parse()?;
            if rows == 0 {
                return Err("cluster_rows must be positive, or auto".into());
            }
            rows
        }
    };
    let engine = Engine::with_config(
        Storage::open(&config).await?,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..EngineConfig::from_env()
        },
    );
    // A quiescent copy may predate the single-object ANN layout. Finish the normal
    // automatic upgrade before building leaves; this does not retrain its centers.
    let upgrade = engine.reindex(&args[1], &Default::default()).await?;
    eprintln!(
        "upgraded {} segments, {} slabs, and {} documents in {:.3}s",
        upgrade.converted_segments,
        upgrade.converted_slabs,
        upgrade.converted_documents,
        upgrade.seconds
    );
    if args.len() == 5 {
        return Ok(());
    }
    let report = engine
        .recluster(
            &args[1],
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 32,
                cluster_rows,
                max_slab_rows: 250_000,
                scratch,
            },
        )
        .await?;
    println!(
        "{}",
        serde_json::json!({
            "rows":report.rows, "slabs":report.slabs, "clusters":report.clusters,
            "directory_bytes":report.directory_bytes, "seconds":report.seconds,
            "cluster_rows":report.cluster_rows,
        })
    );
    Ok(())
}

// Restrict this writable tool to simple temporary prefixes. Reject traversal,
// percent escapes, URL queries, and fragments before opening storage.
fn owned_s3_prefix(url: &str) -> bool {
    let Some((bucket, prefix)) = url.strip_prefix("s3://").and_then(|s| s.split_once('/')) else {
        return false;
    };
    let mut parts = prefix.split('/');
    !bucket.is_empty()
        && bucket
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-'))
        && parts.next() == Some("tmp")
        && parts.clone().next().is_some()
        && parts.all(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
        })
}

#[cfg(test)]
mod tests {
    use super::owned_s3_prefix;

    #[test]
    fn only_owned_temporary_prefixes_can_be_opened_for_writes() {
        assert!(owned_s3_prefix("s3://example-bucket/tmp/leaf-test"));
        assert!(owned_s3_prefix("s3://example-bucket/tmp/leaf-test/nested"));
        for url in [
            "s3://example-bucket/scale",
            "s3://example-bucket/v014/big",
            "s3://example-bucket/tmp",
            "s3://example-bucket/tmp/",
            "s3://example-bucket/tmp/../scale",
            "s3://example-bucket/tmp/run/../../scale",
            "s3://example-bucket/tmp/%2e%2e/scale",
            "s3://example-bucket/tmp/run?prefix=scale",
            "s3://example-bucket/tmp/run#scale",
            "s3:///tmp/run",
            "file:///tmp/run",
        ] {
            assert!(!owned_s3_prefix(url), "unsafe storage URL accepted: {url}");
        }
    }
}
