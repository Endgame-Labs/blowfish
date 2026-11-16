//! Shared analysis and deletion-adjusted, field-level corpus statistics.
use crate::{NamespaceState, segment};
use bf_storage::Storage;
use bf_text::{CorpusStats, FtsParams, Token};
use bf_types::{ApiError, Document, Schema};
use bf_wal::{NamespaceStats, TextStats};
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
/// Out-of-line checkpoint text statistics: per field, the document count and total length plus
/// an FST from term to document frequency. Queries look up only their own terms, directly in the
/// downloaded bytes, so there is nothing to decode or clone.
pub(crate) struct TextIndex {
    fields: BTreeMap<String, (u64, u64, fst::Map<bytes::Bytes>)>,
    bytes: u64,
}
/// Serialize per-term statistics (`MAGIC | header length | JSON header | FSTs | crc32`).
pub(crate) fn encode_text_stats(text: &TextMap) -> Result<Vec<u8>, ApiError> {
    let mut header = serde_json::Map::new();
    let mut blobs = Vec::new();
    for (name, stats) in text {
        let mut builder = fst::MapBuilder::memory();
        for (term, df) in &stats.document_frequencies {
            builder.insert(term, *df).map_err(segment::index_error)?;
        }
        let blob = builder.into_inner().map_err(segment::index_error)?;
        header.insert(
            name.clone(),
            serde_json::json!([stats.documents, stats.total_length, blob.len()]),
        );
        blobs.push(blob);
    }
    let header = serde_json::to_vec(&header).map_err(segment::index_error)?;
    let mut out = STATS_MAGIC.to_vec();
    out.extend((header.len() as u32).to_le_bytes());
    out.extend(header);
    for blob in blobs {
        out.extend(blob);
    }
    let crc = crc32fast::hash(&out);
    out.extend(crc.to_le_bytes());
    Ok(out)
}
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
                fst::Map::new(blob).map_err(|_| bad())?,
            ),
        );
    }
    Ok(TextIndex {
        fields,
        bytes: bytes.len() as u64,
    })
}
/// Per-term text statistics of the checkpoint (one GET, cached while the checkpoint is current).
async fn load_index(
    store: &Storage,
    file: &bf_wal::SegmentFile,
) -> Result<std::sync::Arc<TextIndex>, ApiError> {
    let key = format!("tstats:{}", file.key);
    if let Some(index) = crate::read_cache::get(&key, file.size) {
        return Ok(index);
    }
    let bytes = segment::read_whole(store, file).await?;
    let index = decode_text_stats(bytes)?;
    let resident = index.bytes;
    Ok(crate::read_cache::insert(&key, resident, index))
}
/// Rebuild the complete map (the indexer and the slow path with a shadowed or unindexed tail).
async fn load_text(store: &Storage, file: &bf_wal::SegmentFile) -> Result<TextMap, ApiError> {
    let index = load_index(store, file).await?;
    let mut out = TextMap::new();
    for (name, (documents, total_length, map)) in &index.fields {
        let mut stats = TextStats {
            documents: *documents,
            total_length: *total_length,
            ..Default::default()
        };
        let mut stream = fst::IntoStreamer::into_stream(map);
        while let Some((term, df)) = fst::Streamer::next(&mut stream) {
            stats.document_frequencies.insert(
                String::from_utf8(term.to_vec()).map_err(segment::index_error)?,
                df,
            );
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
/// The checkpoint's corpus statistics per text field, restricted to the query's terms.
async fn checkpoint_corpus_of(
    store: &Storage,
    state: &NamespaceState,
    terms: &QueryTerms,
) -> Result<std::sync::Arc<BTreeMap<String, CorpusStats>>, ApiError> {
    let Some(file) = &state.text_stats else {
        return Ok(state.corpus.clone());
    };
    let index = load_index(store, file).await?;
    Ok(index
        .fields
        .iter()
        .map(|(name, (documents, total_length, map))| {
            let mut stats = CorpusStats {
                total_docs: *documents,
                average_doc_length: if *documents == 0 {
                    0.0
                } else {
                    *total_length as f64 / *documents as f64
                },
                ..Default::default()
            };
            for term in terms.get(name).into_iter().flatten() {
                if let Some(df) = map.get(term) {
                    stats.document_frequencies.insert(term.clone(), df);
                }
            }
            (name.clone(), stats)
        })
        .collect::<BTreeMap<_, _>>()
        .into())
}
/// Fetch and cache the checkpoint's text statistics ahead of a BM25 query.
pub(crate) async fn warm_corpus(store: &Storage, file: &bf_wal::SegmentFile) {
    let _ = load_index(store, file).await;
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
        for (term, df) in [("fox", 2), ("quick", 1), ("zebra", 3)] {
            field.document_frequencies.insert(term.into(), df);
        }
        text.insert("text".into(), field);
        let file = segment::put_file(
            &store,
            "ns/t/segments/text-stats-x.bfs".into(),
            encode_text_stats(&text).unwrap(),
            0,
        )
        .await
        .unwrap();
        let state = NamespaceState {
            text_stats: Some(file.clone()),
            ..Default::default()
        };
        assert_eq!(
            full_stats(&store, &state).await.unwrap().text["text"].document_frequencies,
            text["text"].document_frequencies
        );
        let terms =
            QueryTerms::from([("text".into(), BTreeSet::from(["fox".into(), "nope".into()]))]);
        let corpus = checkpoint_corpus_of(&store, &state, &terms).await.unwrap();
        assert_eq!(corpus["text"].total_docs, 3);
        assert_eq!(corpus["text"].average_doc_length, 3.0);
        assert_eq!(corpus["text"].document_frequencies.len(), 1);
        assert_eq!(corpus["text"].document_frequencies["fox"], 2);
    }
}
