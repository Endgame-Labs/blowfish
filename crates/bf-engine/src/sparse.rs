//! Immutable dimension postings and exact positive sparse dot-product scores.
use crate::{NamespaceState, segment};
use bf_storage::Storage;
use bf_types::*;
use bf_wal::{Segment, SegmentFile};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Default, Serialize, Deserialize)]
struct Inverted {
    postings: BTreeMap<String, Vec<(u32, f32)>>,
}
pub(crate) fn dot(value: Option<&Value>, query: &BTreeMap<String, f32>) -> f32 {
    query
        .iter()
        .map(|(dim, q)| {
            value
                .and_then(|v| v.get(dim))
                .and_then(Value::as_f64)
                .unwrap_or(0.0) as f32
                * q
        })
        .sum()
}
pub(crate) async fn build<D: std::borrow::Borrow<Document>>(
    store: &Storage,
    prefix: &str,
    schema: &Schema,
    docs: &BTreeMap<Id, D>,
    files: &mut BTreeMap<String, SegmentFile>,
) -> Result<(), ApiError> {
    for (column, (name, cfg)) in schema.iter().enumerate() {
        if cfg.data_type != Some(AttributeType::SparseVector) || cfg.sparse_knn.is_none() {
            continue;
        }
        let mut index = Inverted::default();
        for (row, doc) in docs.values().enumerate() {
            if let Some(value) = doc.borrow().get(name) {
                for (dim, weight) in decode_sparse(value)? {
                    index
                        .postings
                        .entry(dim)
                        .or_default()
                        .push((row as u32, weight));
                }
            }
        }
        files.insert(
            format!("sparse:{name}"),
            segment::put_value(store, format!("{prefix}/sparse.{column}.bfv"), &index).await?,
        );
    }
    Ok(())
}
fn clauses<'a>(rank: &'a RankBy, out: &mut Vec<&'a RankBy>) {
    match rank {
        RankBy::SparseVector { .. } => out.push(rank),
        RankBy::Sum(xs) | RankBy::Max(xs) => {
            for x in xs {
                clauses(x, out)
            }
        }
        RankBy::Product { expr, .. }
        | RankBy::Saturate { expr, .. }
        | RankBy::Decay { expr, .. }
        | RankBy::Dist { expr, .. } => clauses(expr, out),
        _ => {}
    }
}
pub(crate) async fn segment_scores(
    store: &Storage,
    seg: &Segment,
    rank: Option<&RankBy>,
    allowed: &RoaringBitmap,
    out: &mut NamespaceState,
) -> Result<(), ApiError> {
    let mut terms = vec![];
    if let Some(rank) = rank {
        clauses(rank, &mut terms)
    }
    if terms.is_empty() {
        return Ok(());
    }
    let ids = segment::ids(store, seg).await?;
    for rank in terms {
        let RankBy::SparseVector {
            attribute, vector, ..
        } = rank
        else {
            unreachable!()
        };
        let scores = out
            .scores
            .entry(crate::indexed::rank_key(rank))
            .or_default();
        if let Some(file) = seg.files.get(&format!("sparse:{attribute}")) {
            let index: std::sync::Arc<Inverted> = segment::get_value(store, file).await?;
            let mut accum = BTreeMap::<u32, f32>::new();
            for (dim, q) in vector {
                for (row, weight) in index.postings.get(dim).into_iter().flatten() {
                    if allowed.contains(*row) {
                        *accum.entry(*row).or_default() += q * weight;
                    }
                }
            }
            for (row, score) in accum {
                scores.insert(ids[row as usize].clone(), score as f64);
            }
        } else {
            let fields = std::collections::BTreeSet::from([attribute.clone()]);
            let docs =
                segment::projected_documents_from(store, None, seg, allowed, Some(&fields)).await?;
            for (id, doc) in docs {
                scores.insert(id, dot(doc.get(attribute), vector) as f64);
            }
        }
    }
    Ok(())
}

pub(crate) fn tail_scores<D: std::borrow::Borrow<Document>>(
    rank: Option<&RankBy>,
    docs: &BTreeMap<Id, D>,
    out: &mut NamespaceState,
) {
    let mut terms = vec![];
    if let Some(rank) = rank {
        clauses(rank, &mut terms)
    }
    for rank in terms {
        let RankBy::SparseVector {
            attribute, vector, ..
        } = rank
        else {
            unreachable!()
        };
        let scores = out
            .scores
            .entry(crate::indexed::rank_key(rank))
            .or_default();
        for (id, doc) in docs {
            scores.insert(id.clone(), dot(doc.borrow().get(attribute), vector) as f64);
        }
    }
}
