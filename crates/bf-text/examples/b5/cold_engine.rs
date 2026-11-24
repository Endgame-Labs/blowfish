// Compiled only by the temporary engine patch used for the B5 cold kill test.
use super::*;
use std::sync::{Mutex, OnceLock};
static READY: tokio::sync::Notify = tokio::sync::Notify::const_new();
static FRAMES: OnceLock<Mutex<Option<Vec<Vec<u8>>>>> = OnceLock::new();
fn u32_at(bytes: &[u8], at: &mut usize) -> u32 {
    let n = u32::from_le_bytes(bytes[*at..*at + 4].try_into().unwrap());
    *at += 4;
    n
}
fn u64_at(bytes: &[u8], at: &mut usize) -> u64 {
    let n = u64::from_le_bytes(bytes[*at..*at + 8].try_into().unwrap());
    *at += 8;
    n
}
pub(crate) async fn corpus(
    store: &Storage,
    file: &bf_wal::SegmentFile,
    terms: &QueryTerms,
) -> Result<std::sync::Arc<BTreeMap<String, CorpusStats>>, ApiError> {
    let index = file.stats_index.as_ref().expect("packed statistics index");
    let f = &index.fields["text"];
    let wanted = terms.get("text").cloned().unwrap_or_default();
    let mut groups = std::collections::BTreeSet::new();
    for term in &wanted {
        if let Some(i) = f
            .chunks
            .partition_point(|c| c.0.as_str() <= term.as_str())
            .checked_sub(1)
        {
            groups.insert(i);
        }
    }
    let groups: Vec<_> = groups.into_iter().collect();
    let ranges = groups
        .iter()
        .map(|&i| {
            let (_, o, n, _) = &f.chunks[i];
            *o..*o + u64::from(*n)
        })
        .collect();
    let blobs = segment::read_ranges(store, file, ranges)
        .await
        .map_err(segment::index_error)?;
    let mut stats = CorpusStats {
        total_docs: f.documents,
        average_doc_length: f.total_length as f64 / f.documents as f64,
        ..Default::default()
    };
    let mut frames = BTreeMap::<String, Vec<u8>>::new();
    for (&group, blob) in groups.iter().zip(blobs) {
        if crc32fast::hash(&blob) != f.chunks[group].3 {
            return Err(ApiError::new(500, "packed CRC mismatch"));
        }
        let mut at = 0;
        let count = u32_at(&blob, &mut at);
        for _ in 0..count {
            let len = u32_at(&blob, &mut at) as usize;
            let frame = &blob[at..at + len];
            at += len;
            let mut pos = 0;
            let n = u32_at(frame, &mut pos) as usize;
            let word = std::str::from_utf8(&frame[pos..pos + n]).unwrap();
            pos += n;
            let df = u64_at(frame, &mut pos);
            if wanted.contains(word) {
                stats.document_frequencies.insert(word.to_owned(), df);
                frames.insert(word.to_owned(), frame.to_vec());
            }
        }
    }
    *FRAMES.get_or_init(Default::default).lock().unwrap() = Some(frames.into_values().collect());
    READY.notify_waiters();
    Ok(BTreeMap::from([("text".into(), stats)]).into())
}
pub(crate) async fn install(
    index: &mut bf_text::SegmentReader,
    file: &str,
) -> Result<(), ApiError> {
    loop {
        let ready = READY.notified();
        {
            let frames = FRAMES.get_or_init(Default::default).lock().unwrap();
            if let Some(frames) = frames.as_ref() {
                return index.b5_install(frames, file).map_err(segment::index_error);
            }
        }
        ready.await;
    }
}
