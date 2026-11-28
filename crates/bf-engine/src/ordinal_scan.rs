//! Choosing a segment's contenders for a score expression without building a document per row.
//!
//! The full scorer hydrates every allowed row (every column, text included), scores it, and sorts
//! them all. For the ranks that reach it (a boost that no list can bound, `limit.per`, and the
//! rest), this module scores rows by ordinal instead: each clause's BM25 hits are already in
//! hand, the columns the expression and the groups read are projected a chunk of rows at a time,
//! and a bounded heap keeps only the contenders. Only they are hydrated. The score of a row is
//! computed by `query::arithmetic`, the same code the full scorer runs, so totals carry its bits.
use crate::{NamespaceState, query, segment, sum_text::Scores};
use bf_storage::Storage;
use bf_types::*;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap, HashMap},
};

/// Rows scored per projected chunk: bounds the documents alive at once.
const CHUNK: usize = 1 << 15;

/// True when `rank` only combines BM25 clauses, constants, and numeric attributes, so a row's score
/// is a function of its clause scores and attribute values alone.
pub(crate) fn eligible(rank: &RankBy) -> bool {
    match rank {
        RankBy::Bm25 { .. } | RankBy::Constant(_) | RankBy::Attribute(_) => true,
        RankBy::Sum(xs) | RankBy::Max(xs) => xs.iter().all(eligible),
        RankBy::Product { expr, .. }
        | RankBy::Saturate { expr, .. }
        | RankBy::Decay { expr, .. }
        | RankBy::Dist { expr, .. } => eligible(expr),
        _ => false,
    }
}

/// True when a row that matches no clause scores 0 and so never ranks (every node maps 0 to 0).
fn zero_without_text(rank: &RankBy) -> bool {
    match rank {
        RankBy::Bm25 { .. } => true,
        RankBy::Sum(xs) | RankBy::Max(xs) => !xs.is_empty() && xs.iter().all(zero_without_text),
        RankBy::Product { expr, .. } | RankBy::Saturate { expr, .. } => zero_without_text(expr),
        _ => false,
    }
}

fn attributes(rank: &RankBy, out: &mut BTreeSet<String>) {
    match rank {
        RankBy::Attribute(name) => {
            out.insert(name.clone());
        }
        RankBy::Sum(xs) | RankBy::Max(xs) => xs.iter().for_each(|x| attributes(x, out)),
        RankBy::Product { expr, .. }
        | RankBy::Saturate { expr, .. }
        | RankBy::Decay { expr, .. }
        | RankBy::Dist { expr, .. } => attributes(expr, out),
        _ => {}
    }
}

/// One scored row, ordered best first: higher score, then lower ID. A max-heap of these keeps the
/// worst contender on top.
struct Contender<'a> {
    score: f64,
    id: &'a Id,
    row: u32,
}
impl PartialEq for Contender<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Contender<'_> {}
impl PartialOrd for Contender<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Contender<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .reverse()
            .then(self.id.cmp(other.id))
    }
}
fn offer<'a>(heap: &mut BinaryHeap<Contender<'a>>, c: Contender<'a>, cap: usize) {
    if heap.len() < cap {
        heap.push(c);
    } else if heap.peek().is_some_and(|worst| c < *worst) {
        heap.pop();
        heap.push(c);
    }
}

/// A row's score: a clause is its hit (0 when it has none), everything else is `query::arithmetic`.
fn score_row(
    schema: &Schema,
    doc: &Document,
    rank: &RankBy,
    clauses: &[&RankBy],
    scores: &[Scores],
    row: u32,
) -> Result<f64, ApiError> {
    if let RankBy::Bm25 { .. } = rank {
        let i = clauses
            .iter()
            .position(|c| std::ptr::eq(*c, rank))
            .expect("every clause of the rank is listed");
        return Ok(f64::from(scores[i].get(row)));
    }
    query::arithmetic(schema, doc, rank, &|x| {
        score_row(schema, doc, x, clauses, scores, row)
    })?
    .ok_or_else(|| ApiError::bad("expected a score expression"))
}

/// The segment's rows that can appear in the answer: its best `k` rows by score (ties by ID), or
/// each group's best `per.limit` rows under `limit.per`. `hits` holds each clause's hits (every
/// matching row) in `clauses` order. Rows scoring 0 or less never rank, as in the full scorer.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn select(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    seg: &bf_wal::Segment,
    clauses: &[&RankBy],
    hits: &[(Vec<u8>, Vec<bf_text::SearchHit>)],
    allowed: &RoaringBitmap,
    k: usize,
) -> Result<RoaringBitmap, ApiError> {
    let rank = q.rank_by.as_ref().expect("eligible rank");
    let started = std::time::Instant::now();
    let ids = segment::ids(store, seg).await?;
    crate::ann::stage(started, "ordinal ids");
    let per = q.limit.as_ref().and_then(Limit::per);
    // One scalar group attribute with a filter dictionary groups by integer code (see
    // `segment::group_codes`): no column is projected for it and no key is built.
    let codes = match per {
        Some(per) if per.attributes.len() == 1 => {
            segment::group_codes(store, seg, &state.schema, &per.attributes[0], ids.len()).await?
        }
        _ => None,
    };
    let mut columns = BTreeSet::new();
    attributes(rank, &mut columns);
    if let (Some(per), None) = (per, &codes) {
        columns.extend(per.attributes.iter().cloned());
    }
    columns.remove("id");
    let rows = if zero_without_text(rank) {
        let mut rows = RoaringBitmap::new();
        for (_, hits) in hits {
            let mut sorted: Vec<u32> = hits.iter().map(|h| h.row).collect();
            sorted.sort_unstable();
            rows |= RoaringBitmap::from_sorted_iter(sorted).expect("sorted rows");
        }
        rows & allowed
    } else {
        allowed.clone()
    };
    let rows_in_segment = rows.max().map_or(0, |m| m + 1);
    let scores: Vec<Scores> = hits
        .iter()
        .map(|(_, h)| Scores::new(h.clone(), rows_in_segment))
        .collect();
    let mut best: BinaryHeap<Contender> = BinaryHeap::new();
    let mut groups: HashMap<String, BinaryHeap<Contender>> = HashMap::new();
    let mut coded: Vec<BinaryHeap<Contender>> = Vec::new();
    coded.resize_with(codes.as_ref().map_or(0, |c| c.groups), Default::default);
    if crate::ann::trace_stages() {
        eprintln!("STAGE ordinal rows {} of {}", rows.len(), allowed.len());
    }
    let all: Vec<u32> = rows.iter().collect();
    for chunk in all.chunks(CHUNK) {
        let docs = if columns.is_empty() {
            Default::default()
        } else {
            let wanted = RoaringBitmap::from_sorted_iter(chunk.iter().copied())
                .expect("rows ascend within a chunk");
            segment::projected_rows(store, seg, &wanted, Some(&columns)).await?
        };
        let empty = Document::new();
        let scored: Vec<(u32, f64, Option<String>)> = chunk
            .par_iter()
            .map(|&row| {
                let doc = docs.get(&row).unwrap_or(&empty);
                let score = score_row(&state.schema, doc, rank, clauses, &scores, row)?;
                // Rows scoring 0 or less never rank (and a row that cannot lead its group is
                // dropped by the heap, so the key is built only under `limit.per`).
                let key = (score > 0.0 || score.is_nan())
                    .then_some(per)
                    .flatten()
                    .filter(|_| codes.is_none())
                    .map(|per| {
                        serde_json::to_string(
                            &per.attributes
                                .iter()
                                .map(|k| doc.get(k).unwrap_or(&serde_json::Value::Null))
                                .collect::<Vec<_>>(),
                        )
                        .expect("key serializes")
                    });
                Ok::<_, ApiError>((row, score, key))
            })
            .collect::<Result<_, _>>()?;
        for (row, score, key) in scored {
            if score <= 0.0 {
                continue;
            }
            let c = Contender {
                score,
                id: &ids[row as usize],
                row,
            };
            match (per, key, &codes) {
                (Some(per), _, Some(codes)) => {
                    offer(&mut coded[codes.codes[row as usize] as usize], c, per.limit)
                }
                (Some(per), Some(key), None) => offer(groups.entry(key).or_default(), c, per.limit),
                _ => offer(&mut best, c, k),
            }
        }
    }
    crate::ann::stage(started, "ordinal total");
    let mut selected = RoaringBitmap::new();
    for c in best
        .iter()
        .chain(groups.values().flat_map(|h| h.iter()))
        .chain(coded.iter().flat_map(|h| h.iter()))
    {
        selected.insert(c.row);
    }
    Ok(selected)
}
