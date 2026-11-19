//! Rewrite a namespace's fixed-key manifest copy in the compact reader form (no text headers), for a
//! namespace published before compact copies existed. Safe beside running servers: readers verify the
//! copy against the manifest log.
//! Usage: readercopy storage.json namespace
use bf_storage::{Storage, StorageConfig};
use bf_wal::Wal;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let a: Vec<_> = std::env::args().collect();
    if a.len() < 3 {
        return Err("expected storage.json namespace".into());
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&a[1])?)?;
    let store = Storage::open(&config).await?;
    let wal = Wal::new(store, &a[2])?;
    match wal.write_reader_copy().await? {
        Some((seq, bytes)) => println!("manifest {seq}: wrote a reader copy of {bytes} bytes"),
        None => println!("nothing to compact"),
    }
    Ok(())
}
