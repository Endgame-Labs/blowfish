//! Explain plans derived from the same schema, segments, and WAL overlay as search.
use crate::{EngineConfig, NamespaceState};
use bf_types::*;
use std::{collections::BTreeSet, fmt::Write};
fn rank_indexes(rank: &RankBy, out: &mut BTreeSet<String>, nprobe: usize) {
    match rank {
        RankBy::Vector {
            attribute, exact, ..
        } => {
            out.insert(if *exact {
                format!("exact dense-vector scan: {attribute}")
            } else {
                format!("IVF/RaBitQ ANN: {attribute}, nprobe={nprobe}")
            });
        }
        RankBy::MultiVector { attribute, .. } => {
            out.insert(format!(
                "exhaustive late-interaction vector scan: {attribute}"
            ));
        }
        RankBy::SparseVector { attribute, .. } => {
            out.insert(format!("sparse inverted postings: {attribute}"));
        }
        RankBy::Bm25 { attribute, .. } => {
            out.insert(format!("BM25 posting lists: {attribute}"));
        }
        RankBy::Filter(f) => filter_indexes(f, out),
        RankBy::Sum(xs) | RankBy::Max(xs) => {
            for x in xs {
                rank_indexes(x, out, nprobe)
            }
        }
        RankBy::Product { expr, .. }
        | RankBy::Saturate { expr, .. }
        | RankBy::Decay { expr, .. }
        | RankBy::Dist { expr, .. } => rank_indexes(expr, out, nprobe),
        RankBy::Order(keys) => {
            out.insert(format!(
                "scalar ordering: {}",
                keys.iter()
                    .map(|k| k.attribute.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        _ => {}
    }
}
fn filter_indexes(f: &Filter, out: &mut BTreeSet<String>) {
    match f {
        Filter::And(fs) | Filter::Or(fs) => {
            for f in fs {
                filter_indexes(f, out)
            }
        }
        Filter::Not(f) => filter_indexes(f, out),
        Filter::Compare { attribute, op, .. } => {
            out.insert(match op {
                FilterOp::Regex
                | FilterOp::Fuzzy
                | FilterOp::Glob
                | FilterOp::IGlob
                | FilterOp::NotGlob
                | FilterOp::NotIGlob => {
                    format!("pattern candidates + exact {op:?} verification: {attribute}")
                }
                FilterOp::ContainsAllTokens
                | FilterOp::ContainsAnyToken
                | FilterOp::ContainsTokenSequence => format!("text token postings: {attribute}"),
                _ => format!("filter bitmap/dictionary: {attribute}"),
            });
        }
    }
}
pub(crate) fn explain(
    state: &NamespaceState,
    request: &QueryRequest,
    config: &EngineConfig,
) -> String {
    let mut plan = format!(
        "Snapshot: manifest={}, indexed_through={}, WAL head={}\nSegments touched: {}\nWAL tail rows: {}, shadowed IDs: {}, tail bytes: {}\nANN nprobe={}, rerank_factor={}\n",
        state.manifest_seq,
        state.indexed_through,
        state.seq,
        state.segments.len(),
        state.docs.len(),
        state.shadowed.len(),
        state.tail_bytes,
        config.ann_nprobe,
        config.ann_rerank_factor
    );
    for (i, q) in request
        .queries
        .as_deref()
        .unwrap_or(std::slice::from_ref(request))
        .iter()
        .enumerate()
    {
        let mut indexes = BTreeSet::new();
        if let Some(rank) = &q.rank_by {
            rank_indexes(rank, &mut indexes, config.ann_nprobe)
        }
        if let Some(f) = &q.filters {
            filter_indexes(f, &mut indexes)
        }
        let _ = writeln!(
            plan,
            "Query {i}: {}",
            indexes.into_iter().collect::<Vec<_>>().join("; ")
        );
        let mut needed = BTreeSet::new();
        if let Some(rank) = &q.rank_by {
            rank_files(rank, &state.schema, &mut needed);
        }
        if let Some(f) = &q.filters {
            filter_files(f, &state.schema, &mut needed);
        }
        if let Some(RankBy::Filter(f)) = &q.rank_by {
            filter_files(f, &state.schema, &mut needed);
        }
        let sources = crate::vector_locator::Sources::new(state);
        for seg in &state.segments {
            let tiered: BTreeSet<String> = needed
                .iter()
                .filter_map(|file| file.strip_prefix("annv2:"))
                .filter(|a| sources.covers(seg, a))
                .map(|a| format!("annv2:{a}"))
                .collect();
            for file in &tiered {
                let _ = writeln!(
                    plan,
                    "  covering tier: {}",
                    file.trim_start_matches("annv2:")
                );
            }
            let used: Vec<_> = needed
                .iter()
                .filter(|file| seg.files.contains_key(*file))
                .cloned()
                .collect();
            let fallback: Vec<_> = needed
                .iter()
                .filter(|file| !seg.files.contains_key(*file) && !tiered.contains(*file))
                .cloned()
                .collect();
            let _ = writeln!(
                plan,
                "  Segment {}: {} rows, indexes used [{}], column-scan fallbacks [{}]",
                seg.id,
                seg.row_count,
                used.join(", "),
                fallback.join(", ")
            );
        }
        if q.compute_attributes.is_some() {
            plan.push_str("  Compute attributes after matching and ranking; Highlight ranks fragments independently\n");
        }
    }
    plan.push_str("Newest-wins merge: mask superseded segment rows, merge the WAL tail, apply offset/limit, and project attributes");
    plan
}

fn rank_files(rank: &RankBy, schema: &Schema, out: &mut BTreeSet<String>) {
    match rank {
        RankBy::Filter(f) => filter_files(f, schema, out),
        RankBy::Vector { attribute, .. } => {
            out.insert(format!("annv2:{attribute}"));
        }
        RankBy::SparseVector { attribute, .. } => {
            out.insert(format!("sparse:{attribute}"));
        }
        RankBy::Bm25 { attribute, .. } => {
            out.insert(format!("fts:{attribute}"));
        }
        RankBy::Sum(xs) | RankBy::Max(xs) => {
            for x in xs {
                rank_files(x, schema, out)
            }
        }
        RankBy::Product { expr, .. }
        | RankBy::Saturate { expr, .. }
        | RankBy::Decay { expr, .. }
        | RankBy::Dist { expr, .. } => rank_files(expr, schema, out),
        _ => {}
    }
}
fn filter_files(f: &Filter, schema: &Schema, out: &mut BTreeSet<String>) {
    match f {
        Filter::And(fs) | Filter::Or(fs) => {
            for f in fs {
                filter_files(f, schema, out)
            }
        }
        Filter::Not(f) => filter_files(f, schema, out),
        Filter::Compare { attribute, op, .. } => {
            out.insert(match op {
                FilterOp::Fuzzy | FilterOp::Regex => format!("trigram:{attribute}"),
                FilterOp::Glob | FilterOp::IGlob | FilterOp::NotGlob | FilterOp::NotIGlob
                    if schema.get(attribute).is_some_and(|c| c.glob == Some(true)) =>
                {
                    format!("trigram:{attribute}")
                }
                FilterOp::ContainsAllTokens
                | FilterOp::ContainsAnyToken
                | FilterOp::ContainsTokenSequence => format!("fts:{attribute}"),
                _ => "filters".into(),
            });
        }
    }
}
