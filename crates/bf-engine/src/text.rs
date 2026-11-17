//! Shared analysis and deletion-adjusted, field-level corpus statistics.
use crate::{NamespaceState, segment};
use bf_storage::Storage;
use bf_text::{CorpusStats, FtsParams, Token};
use bf_types::{ApiError, Document, Schema};
use bf_wal::{NamespaceStats, TextStats};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn tokens(value: &Value, params: &FtsParams) -> Vec<Token> {
    match value {
        Value::String(s) => bf_text::tokenize(s, params),
        Value::Array(a) => bf_text::tokenize_array(
            &a.iter().filter_map(Value::as_str).collect::<Vec<_>>(),
            params,
        ),
        _ => vec![],
    }
}
pub(crate) fn prefix(params: Option<&Value>) -> bool {
    params
        .and_then(|p| p.get("last_as_prefix"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}
pub(crate) fn adjust(stats: &mut NamespaceStats, doc: &Document, schema: &Schema, add: bool) {
    for (name, cfg) in schema {
        let Some(params) = cfg.text_config() else {
            continue;
        };
        let Some(value) = doc.get(name).filter(|v| !v.is_null()) else {
            continue;
        };
        let tokens = tokens(value, &params);
        let field = stats.text.entry(name.clone()).or_default();
        let length = tokens.len().max(1) as u64;
        let terms: BTreeSet<_> = tokens.iter().map(|t| &t.text).collect();
        if add {
            field.documents += 1;
            field.total_length += length;
            for term in terms {
                *field.document_frequencies.entry(term.clone()).or_default() += 1;
            }
        } else {
            field.documents = field.documents.saturating_sub(1);
            field.total_length = field.total_length.saturating_sub(length);
            for term in terms {
                if let Some(count) = field.document_frequencies.get_mut(term) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        field.document_frequencies.remove(term);
                    }
                }
            }
        }
    }
}
fn corpus(stats: &TextStats) -> CorpusStats {
    CorpusStats {
        total_docs: stats.documents,
        average_doc_length: if stats.documents == 0 {
            0.0
        } else {
            stats.total_length as f64 / stats.documents as f64
        },
        document_frequencies: stats.document_frequencies.clone(),
    }
}
/// Decode immutable checkpoint statistics once, rather than once per BM25 query.
pub(crate) fn checkpoint_corpus(
    stats: &NamespaceStats,
) -> std::sync::Arc<BTreeMap<String, CorpusStats>> {
    stats
        .text
        .iter()
        .map(|(name, stats)| (name.clone(), corpus(stats)))
        .collect::<BTreeMap<_, _>>()
        .into()
}
type TextMap = BTreeMap<String, TextStats>;
const STATS_MAGIC: &[u8; 8] = b"BFSTAT01";
const CHUNKED_MAGIC: &[u8; 8] = b"BFSTAT02";
/// Terms per statistics chunk.
const STATS_CHUNK_TERMS: usize = 4096;
/// Out-of-line checkpoint text statistics: per field, the document count and total length plus
/// FSTs from term to document frequency. Queries look up only their own terms, directly in the
/// downloaded bytes, so there is nothing to decode or clone.
pub(crate) struct TextIndex {
    fields: BTreeMap<String, (u64, u64, Vec<fst::Map<bytes::Bytes>>)>,
    bytes: u64,
}
/// Serialize per-term statistics in chunks (`MAGIC | chunk | chunk | ...`, each chunk an FST of
/// up to `STATS_CHUNK_TERMS` terms with its own checksum). The chunk index goes in the manifest,
/// next to the object's reference, so a query can range-read the chunks it needs.
pub(crate) fn encode_text_stats(
    text: &TextMap,
) -> Result<(Vec<u8>, bf_wal::TextStatsIndex), ApiError> {
    let mut out = CHUNKED_MAGIC.to_vec();
    let mut index = bf_wal::TextStatsIndex::default();
    for (name, stats) in text {
        let mut field = bf_wal::FieldStatsIndex {
            documents: stats.documents,
            total_length: stats.total_length,
            chunks: Vec::new(),
        };
        let terms: Vec<_> = stats.document_frequencies.iter().collect();
        for chunk in terms.chunks(STATS_CHUNK_TERMS) {
            let mut builder = fst::MapBuilder::memory();
            for (term, df) in chunk {
                builder.insert(term, **df).map_err(segment::index_error)?;
            }
            let blob = builder.into_inner().map_err(segment::index_error)?;
            field.chunks.push((
                chunk[0].0.clone(),
                out.len() as u64,
                blob.len() as u32,
                crc32fast::hash(&blob),
            ));
            out.extend(blob);
        }
        index.fields.insert(name.clone(), field);
    }
    Ok((out, index))
}
/// A run of sorted `(term, document frequency)` pairs.
type Block = Vec<(Vec<u8>, u64)>;
/// Chunks decoded and re-encoded together by one merge step.
const MERGE_WINDOW_CHUNKS: usize = 32;
/// Encode the complete chunks of `pending` (all of it when `last`), in parallel, appending them to
/// `out` in term order and recording them in `field`.
fn write_full_chunks(
    pending: &mut Vec<(Vec<u8>, u64)>,
    out: &mut Vec<u8>,
    field: &mut bf_wal::FieldStatsIndex,
    last: bool,
) -> Result<(), ApiError> {
    let take = if last {
        pending.len()
    } else {
        pending.len() / STATS_CHUNK_TERMS * STATS_CHUNK_TERMS
    };
    if take == 0 {
        return Ok(());
    }
    let blocks: Vec<(Block, String)> = {
        let drained: Vec<(Vec<u8>, u64)> = pending.drain(..take).collect();
        let mut blocks = Vec::new();
        let mut it = drained.into_iter().peekable();
        while it.peek().is_some() {
            let block: Vec<_> = it.by_ref().take(STATS_CHUNK_TERMS).collect();
            let first = String::from_utf8(block[0].0.clone()).map_err(segment::index_error)?;
            blocks.push((block, first));
        }
        blocks
    };
    let built: Vec<Vec<u8>> = blocks
        .par_iter()
        .map(|(block, _)| {
            let mut builder = fst::MapBuilder::memory();
            for (term, df) in block {
                builder.insert(term, *df).map_err(segment::index_error)?;
            }
            builder.into_inner().map_err(segment::index_error)
        })
        .collect::<Result<_, ApiError>>()?;
    for ((_, first), blob) in blocks.into_iter().zip(built) {
        field.chunks.push((
            first,
            out.len() as u64,
            blob.len() as u32,
            crc32fast::hash(&blob),
        ));
        out.extend(blob);
    }
    Ok(())
}
/// Rewrite the out-of-line statistics object after a publication, without ever holding the term
/// map in memory: stream the previous object's FST chunks in term order, apply the batch's
/// term-frequency changes (`added` minus `removed`, both small and sorted), and write new chunks as
/// they fill. Memory is the previous object's bytes, the new object's bytes, and the two deltas,
/// whatever the vocabulary size. Returns the new object and its chunk index.
pub(crate) fn merge_text_stats(
    old: &[u8],
    index: &bf_wal::TextStatsIndex,
    added: &TextMap,
    removed: &TextMap,
) -> Result<(Vec<u8>, bf_wal::TextStatsIndex), ApiError> {
    let bad = || ApiError::new(500, "stored statistics are corrupt or unsupported");
    if old.len() < 8 || &old[..8] != CHUNKED_MAGIC {
        return Err(bad());
    }
    let mut out = CHUNKED_MAGIC.to_vec();
    out.reserve(old.len() + old.len() / 8);
    let mut merged = bf_wal::TextStatsIndex::default();
    let names: BTreeSet<&String> = index
        .fields
        .keys()
        .chain(added.keys())
        .chain(removed.keys())
        .collect();
    let empty = TextStats::default();
    let no_chunks = bf_wal::FieldStatsIndex::default();
    for name in names {
        let was = index.fields.get(name).unwrap_or(&no_chunks);
        let (add, rem) = (
            added.get(name).unwrap_or(&empty),
            removed.get(name).unwrap_or(&empty),
        );
        let mut field = bf_wal::FieldStatsIndex {
            documents: (was.documents + add.documents).saturating_sub(rem.documents),
            total_length: (was.total_length + add.total_length).saturating_sub(rem.total_length),
            chunks: Vec::new(),
        };
        // Signed changes per term, in term order.
        let mut delta: BTreeMap<&str, i64> = BTreeMap::new();
        for (term, df) in &add.document_frequencies {
            *delta.entry(term).or_default() += *df as i64;
        }
        for (term, df) in &rem.document_frequencies {
            *delta.entry(term).or_default() -= *df as i64;
        }
        let mut delta = delta.into_iter().filter(|(_, v)| *v != 0).peekable();
        let mut pending: Vec<(Vec<u8>, u64)> = Vec::new();
        // Old chunks are decoded and new chunks encoded in parallel, a window of chunks at a
        // time (the work is FST decoding and building, which is most of a merge's CPU); the
        // merge of the sorted streams stays sequential and cheap. Memory is one window.
        for window in was.chunks.chunks(MERGE_WINDOW_CHUNKS) {
            let decoded: Vec<Vec<(Vec<u8>, u64)>> = window
                .par_iter()
                .map(|(_, offset, len, crc)| {
                    let blob = old
                        .get(*offset as usize..*offset as usize + *len as usize)
                        .ok_or_else(bad)?;
                    if crc32fast::hash(blob) != *crc {
                        return Err(bad());
                    }
                    let map = fst::Map::new(blob).map_err(|_| bad())?;
                    let mut terms = Vec::with_capacity(STATS_CHUNK_TERMS);
                    let mut stream = fst::IntoStreamer::into_stream(&map);
                    while let Some((term, df)) = fst::Streamer::next(&mut stream) {
                        terms.push((term.to_vec(), df));
                    }
                    Ok(terms)
                })
                .collect::<Result<_, ApiError>>()?;
            for (term, df) in decoded.into_iter().flatten() {
                // New terms that sort before this one.
                while let Some((u, v)) = delta.next_if(|(u, _)| u.as_bytes() < term.as_slice()) {
                    if v > 0 {
                        pending.push((u.as_bytes().to_vec(), v as u64));
                    }
                }
                let change = delta
                    .next_if(|(u, _)| u.as_bytes() == term.as_slice())
                    .map_or(0, |(_, v)| v);
                let df = (df as i64 + change).max(0) as u64;
                if df > 0 {
                    pending.push((term, df));
                }
            }
            write_full_chunks(&mut pending, &mut out, &mut field, false)?;
        }
        for (u, v) in delta {
            if v > 0 {
                pending.push((u.as_bytes().to_vec(), v as u64));
            }
        }
        write_full_chunks(&mut pending, &mut out, &mut field, true)?;
        merged.fields.insert(name.clone(), field);
    }
    Ok((out, merged))
}
/// The legacy whole-object format (`MAGIC | header length | JSON header | FSTs | crc32`).
fn decode_text_stats(bytes: bytes::Bytes) -> Result<TextIndex, ApiError> {
    let bad = || ApiError::new(500, "stored statistics are corrupt or unsupported");
    if bytes.len() < 16 || &bytes[..8] != STATS_MAGIC {
        return Err(bad());
    }
    let (body, tail) = bytes.split_at(bytes.len() - 4);
    if crc32fast::hash(body) != u32::from_le_bytes(tail.try_into().unwrap()) {
        return Err(bad());
    }
    let header_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let header: BTreeMap<String, (u64, u64, usize)> =
        serde_json::from_slice(bytes.get(12..12 + header_len).ok_or_else(bad)?)
            .map_err(|_| bad())?;
    let mut at = 12 + header_len;
    let mut fields = BTreeMap::new();
    for (name, (documents, total_length, len)) in header {
        let blob = bytes.slice(at..(at + len).min(bytes.len() - 4));
        at += len;
        fields.insert(
            name,
            (
                documents,
                total_length,
                vec![fst::Map::new(blob).map_err(|_| bad())?],
            ),
        );
    }
    Ok(TextIndex {
        fields,
        bytes: bytes.len() as u64,
    })
}
/// Decode every chunk of a chunked object that was read whole.
fn decode_chunked(
    bytes: &bytes::Bytes,
    index: &bf_wal::TextStatsIndex,
) -> Result<TextIndex, ApiError> {
    let bad = || ApiError::new(500, "stored statistics are corrupt or unsupported");
    if bytes.len() < 8 || &bytes[..8] != CHUNKED_MAGIC {
        return Err(bad());
    }
    let mut fields = BTreeMap::new();
    for (name, field) in &index.fields {
        let mut maps = Vec::with_capacity(field.chunks.len());
        for (_, offset, len, crc) in &field.chunks {
            let blob = bytes
                .get(*offset as usize..(*offset as usize + *len as usize))
                .ok_or_else(bad)?;
            if crc32fast::hash(blob) != *crc {
                return Err(bad());
            }
            maps.push(
                fst::Map::new(bytes.slice(*offset as usize..*offset as usize + *len as usize))
                    .map_err(|_| bad())?,
            );
        }
        fields.insert(name.clone(), (field.documents, field.total_length, maps));
    }
    Ok(TextIndex {
        fields,
        bytes: bytes.len() as u64,
    })
}
/// The whole statistics object (the legacy format, and the indexer or slow paths with a shadowed
/// or unindexed tail), cached while the checkpoint is current.
async fn load_index(
    store: &Storage,
    file: &bf_wal::SegmentFile,
) -> Result<std::sync::Arc<TextIndex>, ApiError> {
    let key = format!("tstats:{}", file.key);
    if let Some(index) = crate::read_cache::get(&key, file.size) {
        return Ok(index);
    }
    let bytes = segment::read_whole(store, file).await?;
    let index = match &file.stats_index {
        Some(chunks) => decode_chunked(&bytes, chunks)?,
        None => decode_text_stats(bytes)?,
    };
    let resident = index.bytes;
    Ok(crate::read_cache::insert(&key, resident, index))
}
/// Rebuild the complete map (the indexer and the slow path with a shadowed or unindexed tail).
async fn load_text(store: &Storage, file: &bf_wal::SegmentFile) -> Result<TextMap, ApiError> {
    let index = load_index(store, file).await?;
    let mut out = TextMap::new();
    for (name, (documents, total_length, maps)) in &index.fields {
        let mut stats = TextStats {
            documents: *documents,
            total_length: *total_length,
            ..Default::default()
        };
        for map in maps {
            let mut stream = fst::IntoStreamer::into_stream(map);
            while let Some((term, df)) = fst::Streamer::next(&mut stream) {
                stats.document_frequencies.insert(
                    String::from_utf8(term.to_vec()).map_err(segment::index_error)?,
                    df,
                );
            }
        }
        out.insert(name.clone(), stats);
    }
    Ok(out)
}
/// Namespace-wide statistics of the checkpoint, including per-term text statistics.
pub(crate) async fn full_stats(
    store: &Storage,
    state: &NamespaceState,
) -> Result<NamespaceStats, ApiError> {
    let mut stats = (*state.stats).clone();
    if let Some(file) = &state.text_stats {
        stats.text = load_text(store, file).await?;
    }
    Ok(stats)
}
/// Query terms per text field, so corpus statistics carry only the frequencies a query uses.
pub(crate) type QueryTerms = BTreeMap<String, BTreeSet<String>>;
/// The checkpoint's corpus statistics per text field, restricted to the query's terms. A chunked
/// object reads only the chunks that hold the terms (ranged, in one batch).
async fn checkpoint_corpus_of(
    store: &Storage,
    state: &NamespaceState,
    terms: &QueryTerms,
) -> Result<std::sync::Arc<BTreeMap<String, CorpusStats>>, ApiError> {
    let Some(file) = &state.text_stats else {
        return Ok(state.corpus.clone());
    };
    corpus_for(store, file, terms).await
}
async fn corpus_for(
    store: &Storage,
    file: &bf_wal::SegmentFile,
    terms: &QueryTerms,
) -> Result<std::sync::Arc<BTreeMap<String, CorpusStats>>, ApiError> {
    let averaged = |documents: u64, total_length: u64| CorpusStats {
        total_docs: documents,
        average_doc_length: if documents == 0 {
            0.0
        } else {
            total_length as f64 / documents as f64
        },
        ..Default::default()
    };
    let Some(index) = &file.stats_index else {
        let index = load_index(store, file).await?;
        return Ok(index
            .fields
            .iter()
            .map(|(name, (documents, total_length, maps))| {
                let mut stats = averaged(*documents, *total_length);
                for term in terms.get(name).into_iter().flatten() {
                    if let Some(df) = maps.iter().find_map(|m| m.get(term)) {
                        stats.document_frequencies.insert(term.clone(), df);
                    }
                }
                (name.clone(), stats)
            })
            .collect::<BTreeMap<_, _>>()
            .into());
    };
    // Which chunk holds each query term.
    let mut wanted: BTreeMap<(&str, usize), Vec<&String>> = BTreeMap::new();
    for (name, field) in &index.fields {
        for term in terms.get(name).into_iter().flatten() {
            let at = field
                .chunks
                .partition_point(|c| c.0.as_str() <= term.as_str());
            if let Some(chunk) = at.checked_sub(1) {
                wanted.entry((name, chunk)).or_default().push(term);
            }
        }
    }
    let keys: Vec<_> = wanted.keys().copied().collect();
    let ranges = keys
        .iter()
        .map(|(name, c)| {
            let (_, offset, len, _) = &index.fields[*name].chunks[*c];
            *offset..*offset + *len as u64
        })
        .collect::<Vec<_>>();
    let blobs = if ranges.is_empty() {
        Vec::new()
    } else {
        segment::read_ranges(store, file, ranges)
            .await
            .map_err(segment::index_error)?
    };
    let mut out: BTreeMap<String, CorpusStats> = index
        .fields
        .iter()
        .map(|(name, f)| (name.clone(), averaged(f.documents, f.total_length)))
        .collect();
    for ((name, c), blob) in keys.iter().zip(blobs) {
        let (_, _, len, crc) = &index.fields[*name].chunks[*c];
        if blob.len() != *len as usize || crc32fast::hash(&blob) != *crc {
            return Err(ApiError::new(
                500,
                "stored statistics are corrupt or unsupported",
            ));
        }
        let map = fst::Map::new(blob).map_err(segment::index_error)?;
        let stats = out.get_mut(*name).expect("field present");
        for term in &wanted[&(*name, *c)] {
            if let Some(df) = map.get(term) {
                stats.document_frequencies.insert((*term).clone(), df);
            }
        }
    }
    Ok(out.into())
}
/// Fetch and cache the checkpoint's text statistics ahead of a BM25 query: the chunks holding
/// the query's terms for a chunked object (the manifest's chunk index says which), the whole
/// object for the legacy format.
pub(crate) async fn warm_corpus(store: &Storage, file: &bf_wal::SegmentFile, terms: &QueryTerms) {
    if file.stats_index.is_some() {
        let _ = corpus_for(store, file, terms).await;
    } else {
        let _ = load_index(store, file).await;
    }
}
/// Load out-of-line text statistics into `state`, for callers (the indexer and branch pins)
/// that carry or rewrite the complete statistics.
pub(crate) async fn hydrate(store: &Storage, state: &mut NamespaceState) -> Result<(), ApiError> {
    if state.text_stats.is_some() {
        let stats = full_stats(store, state).await?;
        state.corpus = checkpoint_corpus(&stats);
        state.stats = stats.into();
        state.text_stats = None;
    }
    Ok(())
}
pub(crate) fn reference_stats(state: &mut NamespaceState) {
    let mut stats = NamespaceStats::default();
    for doc in state.docs.values() {
        adjust(&mut stats, doc, &state.schema, true);
    }
    state.corpus = stats
        .text
        .iter()
        .map(|(name, s)| (name.clone(), corpus(s)))
        .collect::<BTreeMap<_, _>>()
        .into();
}
/// Start at the checkpoint's live totals, subtract shadowed segment rows, and add the tail.
pub(crate) async fn global_stats(
    store: &Storage,
    state: &NamespaceState,
    terms: &QueryTerms,
) -> Result<std::sync::Arc<BTreeMap<String, CorpusStats>>, ApiError> {
    let fields: BTreeSet<_> = state
        .schema
        .iter()
        .filter(|(_, s)| s.text_config().is_some())
        .map(|(n, _)| n.clone())
        .collect();
    if fields.is_empty() {
        return Ok(Default::default());
    }
    if state.segments.iter().any(|seg| {
        fields
            .iter()
            .any(|f| !segment::seg_text_matches(seg, &state.schema, f))
    }) {
        let mut stats = NamespaceStats::default();
        for seg in &state.segments {
            let live = crate::snapshot::allowed(store, state, seg, None).await?;
            for doc in segment::projected_documents(store, seg, &live, Some(&fields))
                .await?
                .values()
            {
                adjust(&mut stats, doc, &state.schema, true);
            }
        }
        for doc in state.docs.values() {
            adjust(&mut stats, doc, &state.schema, true);
        }
        return Ok(fields
            .into_iter()
            .map(|name| {
                let c = stats.text.get(&name).map(corpus).unwrap_or_default();
                (name, c)
            })
            .collect::<BTreeMap<_, _>>()
            .into());
    }
    if state.shadowed.is_empty() && state.docs.is_empty() {
        return checkpoint_corpus_of(store, state, terms).await;
    }
    let mut stats = full_stats(store, state).await?;
    if !state.shadowed.is_empty() {
        for seg in &state.segments {
            let live = segment::live(store, seg).await?;
            let shadowed = segment::lookup_many(store, seg, &state.shadowed).await? & live;
            for doc in segment::projected_documents(store, seg, &shadowed, Some(&fields))
                .await?
                .values()
            {
                adjust(&mut stats, doc, &state.schema, false);
            }
        }
    }
    for doc in state.docs.values() {
        adjust(&mut stats, doc, &state.schema, true);
    }
    Ok(fields
        .into_iter()
        .map(|name| {
            let c = stats.text.get(&name).map(corpus).unwrap_or_default();
            (name, c)
        })
        .collect::<BTreeMap<_, _>>()
        .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn immutable_corpus_statistics_are_shared_between_queries() {
        let store = Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let schema: Schema = serde_json::from_value(serde_json::json!({
            "text":{"type":"string","full_text_search":true}
        }))
        .unwrap();
        let mut stats = NamespaceStats::default();
        adjust(
            &mut stats,
            &serde_json::from_value(serde_json::json!({"text":"red fox"})).unwrap(),
            &schema,
            true,
        );
        let state = NamespaceState {
            schema,
            corpus: checkpoint_corpus(&stats),
            stats: stats.into(),
            ..Default::default()
        };
        let first = global_stats(&store, &state, &QueryTerms::new())
            .await
            .unwrap();
        let second = global_stats(&store, &state, &QueryTerms::new())
            .await
            .unwrap();
        assert!(std::sync::Arc::ptr_eq(&first, &second));
        assert_eq!(first["text"].total_docs, 1);
        assert_eq!(first["text"].document_frequencies["fox"], 1);
    }
    #[tokio::test]
    async fn out_of_line_text_statistics_round_trip_and_restrict_to_query_terms() {
        let store = Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let mut text = TextMap::new();
        let mut field = TextStats {
            documents: 3,
            total_length: 9,
            ..Default::default()
        };
        // 10,000 terms span three chunks.
        for i in 0..10_000u64 {
            field.document_frequencies.insert(format!("t{i:05}"), i + 1);
        }
        text.insert("text".into(), field);
        let (bytes, index) = encode_text_stats(&text).unwrap();
        assert_eq!(index.fields["text"].chunks.len(), 3);
        let mut file = segment::put_file(&store, "ns/t/segments/text-stats-x.bfs".into(), bytes, 0)
            .await
            .unwrap();
        file.stats_index = Some(index);
        let state = NamespaceState {
            text_stats: Some(file.clone()),
            ..Default::default()
        };
        assert_eq!(
            full_stats(&store, &state).await.unwrap().text["text"].document_frequencies,
            text["text"].document_frequencies
        );
        let terms = QueryTerms::from([(
            "text".into(),
            BTreeSet::from([
                "t00007".into(),
                "t05000".into(),
                "t09999".into(),
                "nope".into(),
            ]),
        )]);
        let corpus = checkpoint_corpus_of(&store, &state, &terms).await.unwrap();
        assert_eq!(corpus["text"].total_docs, 3);
        assert_eq!(corpus["text"].average_doc_length, 3.0);
        assert_eq!(corpus["text"].document_frequencies.len(), 3);
        assert_eq!(corpus["text"].document_frequencies["t05000"], 5001);
        assert_eq!(corpus["text"].document_frequencies["t09999"], 10_000);
    }
    #[test]
    fn streamed_statistics_merge_equals_the_in_memory_update() {
        // 3 chunks, then enough chunks (about 73) to span several merge windows.
        for terms in [10_000u64, 300_000] {
            streamed_merge_of(terms);
        }
    }
    fn streamed_merge_of(terms: u64) {
        let mut base = TextMap::new();
        let mut field = TextStats {
            documents: 100,
            total_length: 900,
            ..Default::default()
        };
        for i in 0..terms {
            field
                .document_frequencies
                .insert(format!("t{:05}", i * 2), (i % 7) + 1);
        }
        base.insert("text".into(), field);
        base.insert("title".into(), TextStats::default());
        let (bytes, index) = encode_text_stats(&base).unwrap();
        let mut added = TextMap::new();
        let mut removed = TextMap::new();
        let mut add = TextStats {
            documents: 5,
            total_length: 40,
            ..Default::default()
        };
        let mut rem = TextStats {
            documents: 2,
            total_length: 10,
            ..Default::default()
        };
        // New terms before, inside, and after the old range; bumps; and removals that hit zero.
        for t in [
            "a", "t00001", "t00003", "t05001", "t19999", "t99999", "zzz", "t00000", "t00004",
        ] {
            *add.document_frequencies.entry(t.to_string()).or_default() += 1;
        }
        for t in ["t00002", "t00004", "t00006", "t19998", "unknown"] {
            *rem.document_frequencies.entry(t.to_string()).or_default() += 9;
        }
        added.insert("text".into(), add.clone());
        removed.insert("text".into(), rem.clone());
        // A field that appears only in the batch.
        added.insert("body".into(), add.clone());
        let mut expected = base.clone();
        for (name, a) in &added {
            let f = expected.entry(name.clone()).or_default();
            f.documents += a.documents;
            f.total_length += a.total_length;
            for (t, d) in &a.document_frequencies {
                *f.document_frequencies.entry(t.clone()).or_default() += d;
            }
        }
        for (name, r) in &removed {
            let f = expected.get_mut(name).unwrap();
            f.documents -= r.documents;
            f.total_length -= r.total_length;
            for (t, d) in &r.document_frequencies {
                if let Some(c) = f.document_frequencies.get_mut(t) {
                    *c = c.saturating_sub(*d);
                    if *c == 0 {
                        f.document_frequencies.remove(t);
                    }
                }
            }
        }
        let (merged, merged_index) = merge_text_stats(&bytes, &index, &added, &removed).unwrap();
        let (want, want_index) = encode_text_stats(&expected).unwrap();
        assert_eq!(merged, want);
        assert_eq!(
            serde_json::to_string(&merged_index).unwrap(),
            serde_json::to_string(&want_index).unwrap()
        );
    }
}
