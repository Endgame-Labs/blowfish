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
/// `Saturate` midpoints and exponents outside these ranges could overflow `powf` or collapse
/// most scores to 0 or 1, so such clauses keep the full scorer.
const MIDPOINT_RANGE: std::ops::RangeInclusive<f64> = 1e-6..=1e12;
const EXPONENT_RANGE: std::ops::RangeInclusive<f64> = 0.05..=20.0;
/// Deepest nest of transforms around one clause that still takes the list paths.
const MAX_NEST: usize = 4;

/// One monotone transform applied to a clause's score by the full scorer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Step {
    /// `weight * score` (a constant `Product`, weight in `WEIGHT_RANGE`).
    Mul(f64),
    /// `Saturate`: `1 - 1 / (1 + (max(score, 0) / midpoint) ^ exponent)`.
    Saturate { midpoint: f64, exponent: f64 },
}
/// The transforms around a clause, innermost first. Each is non-decreasing in the score, so
/// the clause keeps its order (rounding may make unequal scores equal, never reverse them).
pub(crate) type Chain = Vec<Step>;

impl Step {
    fn apply(self, s: f64) -> f64 {
        match self {
            Step::Mul(w) => w * s,
            Step::Saturate { midpoint, exponent } => {
                // Exactly `query::score`: ratio first, then one minus it.
                let ratio = 1.0 / (1.0 + (s.max(0.0) / midpoint).powf(exponent));
                1.0 - ratio
            }
        }
    }
}
/// True when scaling can map two different scores to one (so ties need a transformed check).
pub(crate) fn lossy(chain: &[Step]) -> bool {
    chain.iter().any(|s| matches!(s, Step::Saturate { .. }))
}

/// The transforms around a plain BM25 clause, innermost first, or `None` when `x` is anything
/// else (including a weight of 0 or one outside its range, or odd `Saturate` parameters).
pub(crate) fn weights_of(x: &RankBy) -> Option<Chain> {
    match x {
        RankBy::Bm25 { .. } => Some(vec![]),
        RankBy::Product { weight, expr, .. } if WEIGHT_RANGE.contains(weight) => {
            let mut inner = weights_of(expr)?;
            inner.push(Step::Mul(*weight));
            (inner.len() <= MAX_NEST).then_some(inner)
        }
        RankBy::Saturate { expr, params } => {
            let midpoint = crate::query::parameter(params, "midpoint", 1.0).ok()?;
            let exponent = crate::query::parameter(params, "exponent", 1.0).ok()?;
            if !MIDPOINT_RANGE.contains(&midpoint) || !EXPONENT_RANGE.contains(&exponent) {
                return None;
            }
            let mut inner = weights_of(expr)?;
            inner.push(Step::Saturate { midpoint, exponent });
            (inner.len() <= MAX_NEST).then_some(inner)
        }
        _ => None,
    }
}

/// The transforms around a predicate, innermost first: constant positive `Product`s only.
fn bonus_of(x: &RankBy) -> Option<(&Filter, Chain)> {
    match x {
        RankBy::Filter(f) => Some((f, vec![])),
        RankBy::Product { weight, expr, .. } if WEIGHT_RANGE.contains(weight) => {
            let (f, mut inner) = bonus_of(expr)?;
            inner.push(Step::Mul(*weight));
            (inner.len() <= MAX_NEST).then_some((f, inner))
        }
        _ => None,
    }
}

/// A clause's score after its transforms, applied innermost first in `f64` exactly as
/// `query::score` does, so the result has the full scorer's bits.
pub(crate) fn scaled(chain: &[Step], score: f32) -> f64 {
    chain
        .iter()
        .fold(f64::from(score), |s, step| step.apply(s))
}

/// One term of a `Sum`, in rank order.
pub(crate) enum Term<'a> {
    /// The next plain BM25 clause (in `text_clauses` order) under its transforms.
    Text(Chain),
    /// A predicate that adds its transformed 1 to rows that match it, and 0 to the rest.
    Bonus(&'a Filter, Chain),
}

/// The terms of a rank that the list paths can answer: a flat `Sum` of text clauses and at most
/// `MAX_BONUSES` predicate bonuses (with at least one text clause), a flat `Max` of text
/// clauses, or one transformed text clause alone (treated as a one-term `Sum`).
pub(crate) fn terms_of(rank: Option<&RankBy>) -> Option<Vec<Term<'_>>> {
    match rank? {
        RankBy::Sum(xs) if !xs.is_empty() => {
            let mut bonuses = 0;
            let terms = xs
                .iter()
                .map(|x| {
                    weights_of(x).map(Term::Text).or_else(|| {
                        bonuses += 1;
                        bonus_of(x).map(|(f, c)| Term::Bonus(f, c))
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            (bonuses <= MAX_BONUSES && terms.iter().any(|t| matches!(t, Term::Text(_))))
                .then_some(terms)
        }
        RankBy::Max(xs) if !xs.is_empty() => xs
            .iter()
            .map(|x| weights_of(x).map(Term::Text))
            .collect(),
        x @ (RankBy::Product { .. } | RankBy::Saturate { .. }) => {
            weights_of(x).map(|c| vec![Term::Text(c)])
        }
        _ => None,
    }
}
/// Most predicate bonuses one `Sum` may carry (rows split into `2^n` classes, each searched).
const MAX_BONUSES: usize = 2;

/// The text clauses' transforms of a flat `Sum` or `Max` (or one transformed clause alone),
/// in `text_clauses` order; `None` when the rank is another shape.
pub(crate) fn clause_weights(rank: Option<&RankBy>) -> Option<Vec<Chain>> {
    Some(
        terms_of(rank)?
            .into_iter()
            .filter_map(|t| match t {
                Term::Text(c) => Some(c),
                Term::Bonus(..) => None,
            })
            .collect(),
    )
}

/// A `Sum` (possibly one transformed clause alone, possibly with predicate bonuses) the list
/// paths can answer.
pub(crate) fn sum_of_text(rank: Option<&RankBy>) -> bool {
    !matches!(rank, Some(RankBy::Max(_))) && terms_of(rank).is_some()
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
    /// Transforms around the clause, innermost first (empty for a plain clause).
    pub weights: Chain,
    pub stats: &'a bf_text::CorpusStats,
}

/// A predicate bonus: rows matching `filter` gain `weights` applied to 1.
pub(crate) struct Bonus<'a> {
    pub filter: &'a Filter,
    pub weights: Chain,
}

/// Where a term of the `Sum` sits: text clause `i`, or predicate bonus `j`.
#[derive(Clone, Copy)]
pub(crate) enum Slot {
    Text(usize),
    Bonus(usize),
}

/// A `Sum` ready to run: its text clauses, predicate bonuses, and their order in the rank (the
/// order matters because `f64` addition is not associative and totals must match the full
/// scorer's bits).
pub(crate) struct SumPlan<'a> {
    pub clauses: Vec<Clause<'a>>,
    pub bonuses: Vec<Bonus<'a>>,
    pub slots: Vec<Slot>,
}

impl SumPlan<'_> {
    /// The total of one row: `text(i)` is the transformed score of clause `i`, and `bonus[j]`
    /// the transformed value of bonus `j` for the row's class.
    pub(crate) fn total(&self, text: impl Fn(usize) -> f64, bonus: &[f64]) -> f64 {
        self.slots
            .iter()
            .map(|slot| match *slot {
                Slot::Text(i) => text(i),
                Slot::Bonus(j) => bonus[j],
            })
            .sum()
    }
}

/// Rows that share one match pattern over the bonus predicates, so one bonus value each.
pub(crate) struct Class {
    pub rows: RoaringBitmap,
    /// Transformed value of each bonus for these rows.
    pub bonus: Vec<f64>,
}

/// A segment ready to answer list searches: its allowed rows split into classes by which bonus
/// predicates they match (one class when there are none), and one text index per clause.
pub(crate) struct SegmentSum {
    pub classes: Vec<Class>,
    pub readers: Vec<Arc<bf_text::SegmentReader>>,
    /// Row order is ID order (proven by a validated ID list), needed to pick the lowest-ID rows
    /// among boosted rows that match no text.
    pub id_ordered: bool,
}

/// Open a segment for `Sum` list searches. `None` when a clause has no usable text index in this
/// segment, in which case the caller scores everything the old way.
pub(crate) async fn open(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    seg: &bf_wal::Segment,
    plan: &SumPlan<'_>,
) -> Result<Option<SegmentSum>, ApiError> {
    for clause in &plan.clauses {
        if !segment::seg_text_matches(seg, &state.schema, clause.attribute) {
            return Ok(None);
        }
    }
    let readers = futures::future::try_join_all(plan.clauses.iter().map(|clause| async move {
        match seg.files.get(&format!("fts:{}", clause.attribute)) {
            Some(file) => segment::text_index(store, file).await.map(Some),
            None => Ok(None),
        }
    }));
    let matches = futures::future::try_join_all(
        plan.bonuses
            .iter()
            .map(|b| segment::filter_live(store, seg, b.filter, &state.schema)),
    );
    let ordered = async {
        if plan.bonuses.is_empty() {
            Ok(false)
        } else {
            segment::ids(store, seg).await.map(|_| true)
        }
    };
    let (allowed, readers, matches, id_ordered) = tokio::try_join!(
        snapshot::allowed(store, state, seg, q.filters.as_ref()),
        readers,
        matches,
        ordered
    )?;
    let Some(readers) = readers.into_iter().collect::<Option<Vec<_>>>() else {
        return Ok(None);
    };
    let mut classes = vec![Class {
        rows: allowed,
        bonus: vec![],
    }];
    for (bonus, matching) in plan.bonuses.iter().zip(&matches) {
        let (yes, no) = (scaled(&bonus.weights, 1.0), scaled(&bonus.weights, 0.0));
        classes = classes
            .into_iter()
            .flat_map(|class| {
                let hit = &class.rows & &**matching;
                let miss = &class.rows - &**matching;
                let with = |rows: RoaringBitmap, v: f64| {
                    let mut bonus = class.bonus.clone();
                    bonus.push(v);
                    (!rows.is_empty()).then_some(Class { rows, bonus })
                };
                [with(hit, yes), with(miss, no)]
            })
            .flatten()
            .collect();
    }
    Ok(Some(SegmentSum {
        classes,
        readers,
        id_ordered,
    }))
}

/// One segment's union rows with exact totals, and the bound for rows outside every list.
pub(crate) struct Candidates {
    /// `(row, total)` for each row in some clause's list (and fill rows, see `candidates`).
    pub rows: Vec<(u32, f64)>,
    /// The largest total a row outside `rows` can reach.
    pub bound: f64,
    /// Every row with a positive score is in `rows`, or loses to `k` rows in it.
    pub exhaustive: bool,
}

impl SegmentSum {
    /// Each clause's best `n` rows per class, then the union scored exactly under every clause.
    /// A class whose bonus is positive also gets `k` fill rows when every list ended short: rows
    /// that match no text still total the bonus, and the lowest IDs among them are the ones that
    /// can win. `None` when that needs an ID order this segment cannot prove.
    pub(crate) async fn candidates(
        &self,
        plan: &SumPlan<'_>,
        n: usize,
        k: usize,
    ) -> Result<Option<Candidates>, ApiError> {
        let mut out = Candidates {
            rows: vec![],
            bound: 0.0,
            exhaustive: true,
        };
        for class in &self.classes {
            let Some(c) = self.class_candidates(plan, class, n, k).await? else {
                return Ok(None);
            };
            out.rows.extend(c.rows);
            out.bound = out.bound.max(c.bound);
            out.exhaustive &= c.exhaustive;
        }
        Ok(Some(out))
    }

    async fn class_candidates(
        &self,
        plan: &SumPlan<'_>,
        class: &Class,
        n: usize,
        k: usize,
    ) -> Result<Option<Candidates>, ApiError> {
        let clauses = &plan.clauses;
        let mut union = RoaringBitmap::new();
        let mut cutoffs = Vec::with_capacity(clauses.len());
        let mut lists = Vec::with_capacity(clauses.len());
        for (clause, reader) in clauses.iter().zip(&self.readers) {
            let hits = reader
                .search_prefetched(&clause.query, clause.stats, &clause.cfg, n, Some(&class.rows))
                .await
                .map_err(segment::index_error)?;
            let full = hits.len() >= n;
            cutoffs.push(if full { hits[n - 1].score } else { 0.0 });
            for hit in &hits {
                union.insert(hit.row);
            }
            lists.push((full, hits));
        }
        let bound = plan.total(
            |i| scaled(&clauses[i].weights, cutoffs[i]),
            &class.bonus,
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
        let mut rows: Vec<(u32, f64)> = union
            .iter()
            .map(|row| {
                let t = plan.total(
                    |i| scaled(&clauses[i].weights, exact[i].get(&row).copied().unwrap_or(0.0)),
                    &class.bonus,
                );
                (row, t)
            })
            .collect();
        if exhaustive {
            // Rows outside the union match no text at all and total the bare bonus.
            let bare = plan.total(|_| 0.0, &class.bonus);
            if bare > 0.0 {
                if !self.id_ordered {
                    return Ok(None);
                }
                rows.extend(
                    class
                        .rows
                        .iter()
                        .filter(|row| !union.contains(*row))
                        .take(k)
                        .map(|row| (row, bare)),
                );
            }
        }
        Ok(Some(Candidates {
            rows,
            bound,
            exhaustive,
        }))
    }

    /// Per-clause hits for exactly `rows`, for hydration of the winners.
    pub(crate) async fn winners(
        &self,
        plan: &SumPlan<'_>,
        rows: &RoaringBitmap,
    ) -> Result<Vec<(Vec<u8>, Vec<bf_text::SearchHit>)>, ApiError> {
        let mut out = Vec::with_capacity(plan.clauses.len());
        for (clause, reader) in plan.clauses.iter().zip(&self.readers) {
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
            Some(vec![Step::Mul(2.0)])
        );
        assert_eq!(
            weights_of(&rank(serde_json::json!([
                "Product",
                ["Product", bm(), 3.0],
                2.0
            ]))),
            Some(vec![Step::Mul(3.0), Step::Mul(2.0)])
        );
        for w in [0.0, 1e-12, 1e12] {
            assert_eq!(
                weights_of(&rank(serde_json::json!(["Product", w, bm()]))),
                None
            );
        }
        // 2 * (3 * s) and (2 * 3) * s can differ in the last bit; the nest keeps the first.
        let s = 0.7f32;
        assert_eq!(
            scaled(&[Step::Mul(3.0), Step::Mul(2.0)], s),
            2.0 * (3.0 * f64::from(s))
        );
    }
    #[test]
    fn saturate_never_reverses_the_order_of_adjacent_scores() {
        // The list paths rely on the transform being non-decreasing; sweep neighbors of f32 scores.
        for (midpoint, exponent) in [(3.0, 1.0), (1e-6, 20.0), (2.5, 0.3), (1e12, 0.05), (7.0, 2.0)] {
            let chain = [Step::Saturate { midpoint, exponent }];
            let mut x = 1e-3f32;
            while x < 500.0 {
                let mut prev = scaled(&chain, x);
                let mut v = x;
                for _ in 0..200 {
                    v = f32::from_bits(v.to_bits() + 1);
                    let next = scaled(&chain, v);
                    assert!(next >= prev, "{midpoint} {exponent} {v}");
                    prev = next;
                }
                x *= 1.37;
            }
        }
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
