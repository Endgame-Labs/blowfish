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
        return Ok(state.corpus.clone());
    }
    let mut stats = (*state.stats).clone();
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
        let first = global_stats(&store, &state).await.unwrap();
        let second = global_stats(&store, &state).await.unwrap();
        assert!(std::sync::Arc::ptr_eq(&first, &second));
        assert_eq!(first["text"].total_docs, 1);
        assert_eq!(first["text"].document_frequencies["fox"], 1);
    }
}
