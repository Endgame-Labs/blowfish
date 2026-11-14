//! Per-segment candidate search, tail scoring, and reference-compatible result assembly.
use crate::{EngineConfig, NamespaceState, filter, query, segment, snapshot};
use bf_storage::Storage;
use bf_types::*;
use roaring::RoaringBitmap;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn rank_key(rank: &RankBy) -> String {
    rank.value().to_string()
}

pub(crate) async fn execute(
    store: &Storage,
    state: &NamespaceState,
    request: &QueryRequest,
    config: &EngineConfig,
    force_exact: bool,
) -> Result<QueryResponse, ApiError> {
    // Validate the complete request before touching index files, preserving M1 errors.
    let mut empty = state.clone();
    empty.docs.clear();
    query::execute(&empty, request)?;
    let mut candidates = state.clone();
    candidates.docs.clear();
    for q in request
        .queries
        .as_deref()
        .unwrap_or(std::slice::from_ref(request))
    {
        let part = prepare(store, state, q, config, force_exact).await?;
        candidates.docs.extend(part.docs);
        for (rank, scores) in part.scores {
            candidates.scores.entry(rank).or_default().extend(scores);
        }
    }
    query::execute(&candidates, request)
}
async fn prepare(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    config: &EngineConfig,
    force_exact: bool,
) -> Result<NamespaceState, ApiError> {
    let Some(
        rank @ RankBy::Vector {
            attribute,
            vector,
            exact,
            ..
        },
    ) = &q.rank_by
    else {
        return snapshot::materialize(store, state).await;
    };
    if state.metric == DistanceMetric::CosineDistance && vector.iter().all(|x| *x == 0.0) {
        return snapshot::materialize(store, state).await;
    }
    // Diversification can need arbitrarily deep candidates from each segment.
    let all = q.limit.as_ref().and_then(Limit::per).is_some();
    let k = q.total().unwrap_or(10) + q.offset;
    let mut out = state.clone();
    out.docs.clear();
    let mut scores = BTreeMap::new();
    let columns: BTreeSet<_> = state
        .schema
        .iter()
        .filter(|(name, cfg)| {
            cfg.data_type
                .as_ref()
                .is_none_or(|t| t.dimensions().is_none())
                || match &q.include_attributes {
                    Some(IncludeAttributes::All(true)) => true,
                    Some(IncludeAttributes::Names(names)) => names.contains(name),
                    _ => q
                        .exclude_attributes
                        .as_ref()
                        .is_some_and(|names| !names.contains(name)),
                }
        })
        .map(|(name, _)| name.clone())
        .collect();
    for seg in &state.segments {
        let allowed = snapshot::allowed(store, state, seg, q.filters.as_ref()).await?;
        if allowed.is_empty() {
            continue;
        }
        let Some(file) = seg.files.get(&format!("ann:{attribute}")) else {
            // Version-one document-only segments remain readable during rollout.
            let docs = segment::documents(store, seg, &allowed).await?;
            for (id, doc) in docs {
                if let Some(v) = doc.get(attribute) {
                    scores.insert(
                        id.clone(),
                        query::distance(&decode_vector(v)?, vector, state.metric),
                    );
                    out.docs.insert(id, doc);
                }
            }
            continue;
        };
        let reader = segment::ObjectReader {
            store: store.clone(),
            file: file.clone(),
        };
        let index = bf_vector::Index::open_with_metadata_len(reader, file.metadata_len)
            .await
            .map_err(segment::index_error)?;
        let top = if all { allowed.len() as usize } else { k };
        let hits = if *exact || force_exact || all {
            index.exact_search(vector, top, Some(&allowed)).await
        } else {
            index
                .search(
                    vector,
                    top,
                    bf_vector::SearchParams {
                        nprobe: config.ann_nprobe,
                        rerank_factor: config.ann_rerank_factor,
                    },
                    Some(&allowed),
                )
                .await
        }
        .map_err(segment::index_error)?;
        let ids = segment::ids(store, seg).await?;
        let rows: RoaringBitmap = hits.iter().map(|(row, _)| *row).collect();
        for (row, score) in hits {
            scores.insert(ids[row as usize].clone(), score as f64);
        }
        out.docs
            .extend(segment::projected_documents(store, seg, &rows, Some(&columns)).await?);
    }
    for (id, doc) in &state.docs {
        if q.filters
            .as_ref()
            .map(|f| filter::matches(f, Some(doc), None, &state.schema))
            .transpose()?
            .unwrap_or(true)
            && let Some(v) = doc.get(attribute)
        {
            scores.insert(
                id.clone(),
                query::distance(&decode_vector(v)?, vector, state.metric),
            );
            out.docs.insert(id.clone(), doc.clone());
        }
    }
    out.scores.insert(rank_key(rank), scores);
    Ok(out)
}

pub(crate) fn recall(ann: &QueryResponse, exact: &QueryResponse) -> f64 {
    let ann: BTreeSet<_> = ann
        .result
        .rows
        .iter()
        .flatten()
        .map(|row| row["id"].to_string())
        .collect();
    let exact: BTreeSet<_> = exact
        .result
        .rows
        .iter()
        .flatten()
        .map(|row| row["id"].to_string())
        .collect();
    if exact.is_empty() {
        1.0
    } else {
        ann.intersection(&exact).count() as f64 / exact.len() as f64
    }
}
