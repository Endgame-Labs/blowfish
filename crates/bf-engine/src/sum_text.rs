//! Exact top-k of a `Sum` of plain BM25 clauses without scoring every allowed row.
//!
//! Each segment returns every clause's best `n` rows. The union of those rows is scored exactly
//! under every clause (a search masked to just the union), and the answer is accepted when the
//! `k`th union total is strictly above the sum of every clause's `n`th score: a row outside all of
//! the lists scores at most that sum, so it cannot reach the top `k`, not even on a tie. Otherwise
//! the lists double a bounded number of times, and then the caller runs the full scorer.
use crate::{NamespaceState, segment, snapshot};
use bf_storage::Storage;
use bf_types::*;
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// How `Sum` queries have been decided since the process started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SumOutcomes {
    /// The bound held at the first list length.
    pub first: u64,
    /// The bound held after the lists grew.
    pub grown: u64,
    /// No list length proved the answer, and the full scorer ran.
    pub fallback: u64,
}
static FIRST: AtomicU64 = AtomicU64::new(0);
static GROWN: AtomicU64 = AtomicU64::new(0);
static FALLBACK: AtomicU64 = AtomicU64::new(0);
/// Counts of `Sum` queries accepted at the first list length, accepted after growing, and sent
/// to the full scorer, for benchmarks that report how often the bound holds.
pub fn sum_outcomes() -> SumOutcomes {
    SumOutcomes {
        first: FIRST.load(Ordering::Relaxed),
        grown: GROWN.load(Ordering::Relaxed),
        fallback: FALLBACK.load(Ordering::Relaxed),
    }
}
/// Record one query's outcome: `Some(round)` for the round (0 first) that accepted, `None` for
/// the fallback. Also traced as a stage line when stage tracing is on.
pub(crate) fn record(round: Option<usize>) {
    let (counter, name) = match round {
        Some(0) => (&FIRST, "first"),
        Some(_) => (&GROWN, "grown"),
        None => (&FALLBACK, "fallback"),
    };
    counter.fetch_add(1, Ordering::Relaxed);
    if crate::ann::trace_stages() {
        eprintln!("STAGE sum outcome {name}");
    }
}

/// Largest per-clause list a `Sum` query will try before it scores everything.
const MAX_LIST: usize = 4096;
/// Smallest per-clause list.
const MIN_LIST: usize = 64;

/// Weights outside this range could overflow or underflow the scaled scores, and then two
/// different BM25 scores could map to one value, so such clauses keep the full scorer.
const WEIGHT_RANGE: std::ops::RangeInclusive<f64> = 1e-9..=1e9;
/// Deepest nest of `Product` constants around one clause that still takes the list paths.
const MAX_NEST: usize = 4;

/// The constant weights around a plain BM25 clause, innermost first, or `None` when `x` is
/// anything else (including a weight of 0 or one outside `WEIGHT_RANGE`). A positive weight
/// scales every score by a strictly increasing map, so the clause keeps its order and ties.
pub(crate) fn weights_of(x: &RankBy) -> Option<Vec<f64>> {
    match x {
        RankBy::Bm25 { .. } => Some(vec![]),
        RankBy::Product { weight, expr, .. } if WEIGHT_RANGE.contains(weight) => {
            let mut inner = weights_of(expr)?;
            inner.push(*weight);
            (inner.len() <= MAX_NEST).then_some(inner)
        }
        _ => None,
    }
}

/// A clause's score after its weights, multiplying innermost first in `f64` exactly as
/// `query::score` does (`weight * score`), so the result has the full scorer's bits.
pub(crate) fn scaled(weights: &[f64], score: f32) -> f64 {
    weights.iter().fold(f64::from(score), |s, w| w * s)
}

/// Each clause of a flat `Sum` or `Max` of weighted BM25 clauses, as its weights; `None` when
/// the rank is another shape.
pub(crate) fn clause_weights(rank: Option<&RankBy>) -> Option<Vec<Vec<f64>>> {
    match rank {
        Some(RankBy::Sum(xs) | RankBy::Max(xs)) if !xs.is_empty() => {
            xs.iter().map(weights_of).collect()
        }
        _ => None,
    }
}

/// `["Sum", [...]]` whose clauses are all plain BM25, each under constant positive weights.
pub(crate) fn sum_of_text(rank: Option<&RankBy>) -> bool {
    matches!(rank, Some(RankBy::Sum(_))) && clause_weights(rank).is_some()
}

/// First list length for a query that needs `k` rows, or `None` when the lists would be too long
/// to beat scoring everything.
pub(crate) fn first_list(k: usize) -> Option<usize> {
    (1..=MAX_LIST).contains(&k).then(|| k.max(MIN_LIST))
}

/// The next list length after `n`, or `None` once lists stop paying for themselves.
pub(crate) fn next_list(n: usize) -> Option<usize> {
    let next = n * 4;
    (next <= MAX_LIST).then_some(next)
}

/// One clause with its tokens, parameters, and corpus statistics.
pub(crate) struct Clause<'a> {
    pub attribute: &'a str,
    pub key: Vec<u8>,
    pub query: bf_text::Query,
    pub cfg: FullTextConfig,
    /// Constant weights around the clause, innermost first (empty for a plain clause).
    pub weights: Vec<f64>,
    pub stats: &'a bf_text::CorpusStats,
}

/// A segment ready to answer list searches: its allowed rows and one text index per clause.
pub(crate) struct SegmentSum {
    pub allowed: RoaringBitmap,
    pub readers: Vec<Arc<bf_text::SegmentReader>>,
}

/// Open a segment for `Sum` list searches. `None` when a clause has no usable text index in this
/// segment, in which case the caller scores everything the old way.
pub(crate) async fn open(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    seg: &bf_wal::Segment,
    clauses: &[Clause<'_>],
) -> Result<Option<SegmentSum>, ApiError> {
    for clause in clauses {
        if !segment::seg_text_matches(seg, &state.schema, clause.attribute) {
            return Ok(None);
        }
    }
    let readers = futures::future::try_join_all(clauses.iter().map(|clause| async move {
        match seg.files.get(&format!("fts:{}", clause.attribute)) {
            Some(file) => segment::text_index(store, file).await.map(Some),
            None => Ok(None),
        }
    }));
    let (allowed, readers) = tokio::try_join!(
        snapshot::allowed(store, state, seg, q.filters.as_ref()),
        readers
    )?;
    let Some(readers) = readers.into_iter().collect::<Option<Vec<_>>>() else {
        return Ok(None);
    };
    Ok(Some(SegmentSum { allowed, readers }))
}

/// One segment's union rows with exact totals, and the bound for rows outside every list.
pub(crate) struct Candidates {
    /// `(row, total)` for each row in some clause's list.
    pub rows: Vec<(u32, f64)>,
    /// Sum of every clause's `n`th score (0 for a clause whose list ended short).
    pub bound: f64,
    /// Every row with a positive score is in `rows`.
    pub exhaustive: bool,
}

/// Sum scaled scores in clause order, as `query::score` does, so totals are bit-identical.
pub(crate) fn total(scores: impl Iterator<Item = f64>) -> f64 {
    scores.sum()
}

impl SegmentSum {
    /// Each clause's best `n` rows, then the union scored exactly under every clause.
    pub(crate) async fn candidates(
        &self,
        clauses: &[Clause<'_>],
        n: usize,
    ) -> Result<Candidates, ApiError> {
        let mut union = RoaringBitmap::new();
        let mut cutoffs = Vec::with_capacity(clauses.len());
        let mut lists = Vec::with_capacity(clauses.len());
        for (clause, reader) in clauses.iter().zip(&self.readers) {
            let hits = reader
                .search_prefetched(
                    &clause.query,
                    clause.stats,
                    &clause.cfg,
                    n,
                    Some(&self.allowed),
                )
                .await
                .map_err(segment::index_error)?;
            let full = hits.len() >= n;
            cutoffs.push(if full { hits[n - 1].score } else { 0.0 });
            for hit in &hits {
                union.insert(hit.row);
            }
            lists.push((full, hits));
        }
        let bound = total(
            cutoffs
                .iter()
                .zip(clauses)
                .map(|(c, clause)| scaled(&clause.weights, *c)),
        );
        let exhaustive = lists.iter().all(|(full, _)| !full);
        let mut exact: Vec<HashMap<u32, f32>> = Vec::with_capacity(clauses.len());
        for ((clause, reader), (full, hits)) in clauses.iter().zip(&self.readers).zip(lists) {
            if !full {
                exact.push(hits.into_iter().map(|h| (h.row, h.score)).collect());
                continue;
            }
            let all = reader
                .search_prefetched(
                    &clause.query,
                    clause.stats,
                    &clause.cfg,
                    union.len() as usize,
                    Some(&union),
                )
                .await
                .map_err(segment::index_error)?;
            exact.push(all.into_iter().map(|h| (h.row, h.score)).collect());
        }
        let rows = union
            .iter()
            .map(|row| {
                let t = total(exact.iter().zip(clauses).map(|(m, clause)| {
                    scaled(&clause.weights, m.get(&row).copied().unwrap_or(0.0))
                }));
                (row, t)
            })
            .collect();
        Ok(Candidates {
            rows,
            bound,
            exhaustive,
        })
    }

    /// Per-clause hits for exactly `rows`, for hydration of the winners.
    pub(crate) async fn winners(
        &self,
        clauses: &[Clause<'_>],
        rows: &RoaringBitmap,
    ) -> Result<Vec<(Vec<u8>, Vec<bf_text::SearchHit>)>, ApiError> {
        let mut out = Vec::with_capacity(clauses.len());
        for (clause, reader) in clauses.iter().zip(&self.readers) {
            let hits = if rows.is_empty() {
                vec![]
            } else {
                reader
                    .search_prefetched(
                        &clause.query,
                        clause.stats,
                        &clause.cfg,
                        rows.len() as usize,
                        Some(rows),
                    )
                    .await
                    .map_err(segment::index_error)?
            };
            out.push((clause.key.clone(), hits));
        }
        Ok(out)
    }
}

/// Decide a round: the winners' score floor, or `None` when the lists were too short to prove it.
/// `totals` holds every candidate total (segment unions and tail documents), `bound` the largest
/// per-segment outside bound, and `exhaustive` whether every list ended short.
pub(crate) fn accept(totals: &mut [f64], k: usize, bound: f64, exhaustive: bool) -> Option<f64> {
    let positive = |t: &f64| *t > 0.0;
    let count = totals.iter().filter(|t| positive(t)).count();
    if count < k {
        // Fewer than k positive rows in the union: all of them win only when nothing is outside.
        return exhaustive.then_some(0.0);
    }
    totals.sort_unstable_by(|a, b| b.total_cmp(a));
    let kth = totals[k - 1];
    (exhaustive || kth > bound).then_some(kth)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_a_kth_total_strictly_above_the_bound() {
        // Strictly above: accepted, and the floor is the kth total.
        assert_eq!(accept(&mut [9.0, 7.0, 5.0], 2, 6.5, false), Some(7.0));
        // A tie with the bound could hide an outside row that wins on ID: not proven.
        assert_eq!(accept(&mut [9.0, 7.0, 5.0], 2, 7.0, false), None);
        // Fewer than k positive rows: only complete lists prove nothing is missing.
        assert_eq!(accept(&mut [9.0, 0.0], 2, 3.0, false), None);
        assert_eq!(accept(&mut [9.0, 0.0], 2, 0.0, true), Some(0.0));
        // Complete lists need no bound at all.
        assert_eq!(accept(&mut [2.0, 1.0], 1, 0.0, true), Some(2.0));
    }
    #[test]
    fn weights_nest_innermost_first_and_zero_or_extreme_weights_are_refused() {
        let rank = |v: serde_json::Value| -> RankBy { serde_json::from_value(v).unwrap() };
        let bm = || serde_json::json!(["t", "BM25", "x"]);
        assert_eq!(weights_of(&rank(bm())), Some(vec![]));
        assert_eq!(
            weights_of(&rank(serde_json::json!(["Product", 2.0, bm()]))),
            Some(vec![2.0])
        );
        assert_eq!(
            weights_of(&rank(serde_json::json!([
                "Product",
                ["Product", bm(), 3.0],
                2.0
            ]))),
            Some(vec![3.0, 2.0])
        );
        for w in [0.0, 1e-12, 1e12] {
            assert_eq!(
                weights_of(&rank(serde_json::json!(["Product", w, bm()]))),
                None
            );
        }
        // 2 * (3 * s) and (2 * 3) * s can differ in the last bit; the nest keeps the first.
        let s = 0.7f32;
        assert_eq!(scaled(&[3.0, 2.0], s), 2.0 * (3.0 * f64::from(s)));
    }
    #[test]
    fn list_lengths_grow_to_a_cap() {
        assert_eq!(first_list(0), None);
        assert_eq!(first_list(10), Some(64));
        assert_eq!(first_list(100), Some(100));
        assert_eq!(first_list(4097), None);
        assert_eq!(next_list(64), Some(256));
        assert_eq!(next_list(1024), Some(4096));
        assert_eq!(next_list(4096), None);
    }
}
