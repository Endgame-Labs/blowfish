//! Verify every row of the HTTP ingest experiment by reading retained WAL objects.
//! I2_URL must identify this experiment's disposable prefix. I2_CORPUS is its
//! little-endian f16 NumPy matrix, and I2_ATTRS is its title/text JSONL input.
use bf_storage::{Storage, StorageConfig};
use bf_types::{Document, decode_vector};
use bf_wal::Wal;
use std::{
    error::Error,
    io::{BufRead, Read, Seek, SeekFrom},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("I2_URL")?;
    if !url.contains("/tmp/i2/") && !url.starts_with("file://") {
        return Err("I2_URL must name a disposable I2 prefix".into());
    }
    let rows: usize = std::env::var("I2_ROWS")
        .unwrap_or_else(|_| "500000".into())
        .parse()?;
    let mut corpus = std::fs::File::open(std::env::var("I2_CORPUS")?)?;
    let mut prefix = [0; 8];
    corpus.read_exact(&mut prefix)?;
    assert_eq!(&prefix[..6], b"\x93NUMPY");
    let header_len = match prefix[6] {
        1 => {
            let mut len = [0; 2];
            corpus.read_exact(&mut len)?;
            u16::from_le_bytes(len) as usize
        }
        2 | 3 => {
            let mut len = [0; 4];
            corpus.read_exact(&mut len)?;
            u32::from_le_bytes(len) as usize
        }
        _ => return Err("unsupported NumPy header".into()),
    };
    let mut header = vec![0; header_len];
    corpus.read_exact(&mut header)?;
    let header = std::str::from_utf8(&header)?;
    assert!(header.contains("'<f2'") && header.contains("'fortran_order': False"));
    let start = corpus.stream_position()?;
    assert_eq!(corpus.metadata()?.len(), start + rows as u64 * 2048);
    let attrs: Vec<Document> =
        std::io::BufReader::new(std::fs::File::open(std::env::var("I2_ATTRS")?)?)
            .lines()
            .map(|line| Ok(serde_json::from_str(&line?)?))
            .collect::<Result<_, Box<dyn Error>>>()?;
    assert_eq!(attrs.len(), rows);
    let store = Storage::open(&StorageConfig {
        url,
        region: Some("us-west-2".into()),
        ..Default::default()
    })
    .await?;
    let wal = Wal::new(store, "i2")?;
    let mut seen = vec![false; rows];
    let mut count = 0;
    let mut sequence = 1;
    while let Some(entry) = wal.read(sequence).await? {
        assert!(!entry.tombstone);
        for batch in entry.batches {
            assert!(batch.deletes.is_empty());
            for mut doc in batch.upserts {
                let id = doc
                    .remove("id")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing ID")? as usize;
                assert!(id < rows && !seen[id], "duplicate or unexpected ID {id}");
                seen[id] = true;
                let vector = decode_vector(&doc.remove("vector").ok_or("missing vector")?)?;
                assert_eq!(vector.len(), 1024);
                let mut bytes = [0; 2048];
                corpus.seek(SeekFrom::Start(start + id as u64 * 2048))?;
                corpus.read_exact(&mut bytes)?;
                for (dimension, (actual, expected)) in
                    vector.iter().zip(bytes.as_chunks::<2>().0).enumerate()
                {
                    assert_eq!(
                        actual.to_bits(),
                        half::f16::from_le_bytes(*expected).to_f32().to_bits(),
                        "row {id}, dimension {dimension}"
                    );
                }
                assert_eq!(doc, attrs[id], "attributes of row {id}");
                count += 1;
            }
        }
        sequence += 1;
    }
    assert_eq!(count, rows);
    assert!(seen.iter().all(|seen| *seen));
    println!(
        "rows={count} coordinates={} slots={} all_values_identical=true",
        count * 1024,
        sequence - 1
    );
    Ok(())
}
