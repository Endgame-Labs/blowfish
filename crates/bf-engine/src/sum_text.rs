//! Exact top-k of a monotone expression over plain BM25 clauses without scoring every allowed row.
//!
//! The expression is a tree of `Sum`, `Max`, constant `Product`, and `Saturate` over BM25 clauses,
//! constants, predicate bonuses, and row-wise terms with no BM25 (a recency decay, a saturated
//! attribute). Every node is non-decreasing in each child, so a row outside every clause's list
//! totals at most the expression at the clauses' cutoffs. A row-wise term is bounded by a static
//! upper bound and evaluated exactly for the listed rows only.
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
    chain.iter().fold(f64::from(score), |s, step| step.apply(s))
}

/// The transforms of each clause of a flat `Max` of text clauses (or of one transformed clause
/// alone), in `text_clauses` order; `None` when the rank is another shape. A flat `Max` takes
/// each clause's own list, with shared floors (see `Floors`).
pub(crate) fn clause_weights(rank: Option<&RankBy>) -> Option<Vec<Chain>> {
    match rank? {
        RankBy::Max(xs) if !xs.is_empty() => xs.iter().map(weights_of).collect(),
        _ => None,
    }
}
/// Most predicate bonuses one rank may carry (rows split into `2^n` classes, each searched).
const MAX_BONUSES: usize = 2;

/// A rank the bound-and-list path can answer: any monotone expression over BM25 clauses (see
/// `shape_of`), except a flat `Max` of clauses, which has its own, faster path.
pub(crate) fn sum_of_text(rank: Option<&RankBy>) -> bool {
    let flat_max = clause_weights(rank).is_some();
    !flat_max && shape_of(rank).is_some()
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
    pub stats: &'a bf_text::CorpusStats,
}

/// A predicate bonus: rows matching `filter` gain `weights` applied to 1.
pub(crate) struct Bonus<'a> {
    pub filter: &'a Filter,
    pub weights: Chain,
}

/// A node of the expression. Children are evaluated and combined exactly as `query::score` does
/// (`f64`, in rank order), so totals carry the full scorer's bits.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Expr {
    /// The raw BM25 score of text clause `i`.
    Text(usize),
    /// The transformed value of predicate bonus `j` for the row's class.
    Bonus(usize),
    /// Row-wise term `j` (no BM25), evaluated per listed row.
    Doc(usize),
    Const(f64),
    Sum(Vec<Expr>),
    /// `Max` starts from 0, as the full scorer's does.
    Max(Vec<Expr>),
    Mul(f64, Box<Expr>),
    Saturate {
        midpoint: f64,
        exponent: f64,
        expr: Box<Expr>,
    },
}

/// A term with no BM25 whose value depends on the row (attributes), with a static upper bound.
pub(crate) struct DocTerm<'a> {
    pub rank: &'a RankBy,
    /// No row's value exceeds this (as the full scorer computes it).
    pub upper: f64,
    /// Attributes the term reads.
    pub columns: std::collections::BTreeSet<String>,
}

/// A rank the list paths can answer: its expression and the pieces it refers to.
pub(crate) struct Shape<'a> {
    pub expr: Expr,
    pub bonuses: Vec<(&'a Filter, Chain)>,
    pub docs: Vec<DocTerm<'a>>,
    /// Text clauses, in `text_clauses` order.
    pub texts: usize,
}

fn has_text(x: &RankBy) -> bool {
    match x {
        RankBy::Bm25 { .. } => true,
        RankBy::Sum(xs) | RankBy::Max(xs) => xs.iter().any(has_text),
        RankBy::Product { expr, .. }
        | RankBy::Saturate { expr, .. }
        | RankBy::Decay { expr, .. }
        | RankBy::Dist { expr, .. } => has_text(expr),
        _ => false,
    }
}

/// `Saturate` and `Decay` parameters that keep the result in `[0, 1]`.
fn bounded_params(params: &serde_json::Value) -> Option<(f64, f64)> {
    let midpoint = crate::query::parameter(params, "midpoint", 1.0).ok()?;
    let exponent = crate::query::parameter(params, "exponent", 1.0).ok()?;
    (MIDPOINT_RANGE.contains(&midpoint) && EXPONENT_RANGE.contains(&exponent))
        .then_some((midpoint, exponent))
}

/// A static upper bound of a term with no BM25, or `None` when there is none (an attribute on its
/// own is unbounded). Bounds are computed with the same `f64` operations as the full scorer, which
/// are monotone, so no row's value exceeds them.
fn upper_of(x: &RankBy, columns: &mut std::collections::BTreeSet<String>) -> Option<f64> {
    match x {
        RankBy::Constant(n) => n.as_f64().filter(|v| v.is_finite() && *v >= 0.0),
        RankBy::Saturate { expr, params } | RankBy::Decay { expr, params } => {
            bounded_params(params)?;
            // The inner value must be computable per row; its own bound is not needed.
            inner_columns(expr, columns)?;
            Some(1.0)
        }
        RankBy::Product { weight, expr, .. } if weight.is_finite() => {
            Some(weight * upper_of(expr, columns)?)
        }
        RankBy::Sum(xs) if !xs.is_empty() => xs
            .iter()
            .map(|x| upper_of(x, columns))
            .collect::<Option<Vec<_>>>()
            .map(|v| v.into_iter().sum()),
        RankBy::Max(xs) if !xs.is_empty() => xs
            .iter()
            .map(|x| upper_of(x, columns))
            .collect::<Option<Vec<_>>>()
            .map(|v| v.into_iter().fold(0.0, f64::max)),
        _ => None,
    }
}

/// The attributes an unbounded inner term reads (`Attribute`, `Dist`, and arithmetic over them);
/// `None` for anything else.
fn inner_columns(x: &RankBy, columns: &mut std::collections::BTreeSet<String>) -> Option<()> {
    match x {
        RankBy::Constant(_) => Some(()),
        RankBy::Attribute(name) => {
            columns.insert(name.clone());
            Some(())
        }
        RankBy::Dist { expr, .. } => inner_columns(expr, columns),
        RankBy::Product { expr, .. }
        | RankBy::Saturate { expr, .. }
        | RankBy::Decay { expr, .. } => inner_columns(expr, columns),
        RankBy::Sum(xs) | RankBy::Max(xs) => xs.iter().try_for_each(|x| inner_columns(x, columns)),
        _ => None,
    }
}

struct Build<'a> {
    texts: usize,
    bonuses: Vec<(&'a Filter, Chain)>,
    docs: Vec<DocTerm<'a>>,
}

impl<'a> Build<'a> {
    fn expr(&mut self, x: &'a RankBy) -> Option<Expr> {
        if !has_text(x) {
            return self.rowwise(x);
        }
        Some(match x {
            RankBy::Bm25 { .. } => {
                self.texts += 1;
                Expr::Text(self.texts - 1)
            }
            RankBy::Sum(xs) if !xs.is_empty() => {
                Expr::Sum(xs.iter().map(|x| self.expr(x)).collect::<Option<_>>()?)
            }
            RankBy::Max(xs) if !xs.is_empty() => {
                Expr::Max(xs.iter().map(|x| self.expr(x)).collect::<Option<_>>()?)
            }
            RankBy::Product { weight, expr, .. } if WEIGHT_RANGE.contains(weight) => {
                Expr::Mul(*weight, Box::new(self.expr(expr)?))
            }
            RankBy::Saturate { expr, params } => {
                let (midpoint, exponent) = bounded_params(params)?;
                Expr::Saturate {
                    midpoint,
                    exponent,
                    expr: Box::new(self.expr(expr)?),
                }
            }
            _ => return None,
        })
    }
    /// A term with no BM25: a constant, a predicate bonus, or a row-wise term.
    fn rowwise(&mut self, x: &'a RankBy) -> Option<Expr> {
        if let RankBy::Constant(n) = x {
            return n
                .as_f64()
                .filter(|v| v.is_finite() && *v >= 0.0)
                .map(Expr::Const);
        }
        if let Some((filter, chain)) = bonus_of(x) {
            if self.bonuses.len() >= MAX_BONUSES {
                return None;
            }
            self.bonuses.push((filter, chain));
            return Some(Expr::Bonus(self.bonuses.len() - 1));
        }
        let mut columns = Default::default();
        let upper = upper_of(x, &mut columns)?;
        self.docs.push(DocTerm {
            rank: x,
            upper,
            columns,
        });
        Some(Expr::Doc(self.docs.len() - 1))
    }
}

/// The expression of a rank the list paths can answer, or `None` for another shape.
pub(crate) fn shape_of(rank: Option<&RankBy>) -> Option<Shape<'_>> {
    let rank = rank?;
    let mut build = Build {
        texts: 0,
        bonuses: vec![],
        docs: vec![],
    };
    let expr = build.expr(rank)?;
    (build.texts > 0).then_some(Shape {
        expr,
        bonuses: build.bonuses,
        docs: build.docs,
        texts: build.texts,
    })
}

/// A rank ready to run: its text clauses, predicate bonuses, row-wise terms, and expression.
pub(crate) struct SumPlan<'a> {
    pub clauses: Vec<Clause<'a>>,
    pub bonuses: Vec<Bonus<'a>>,
    pub docs: Vec<DocTerm<'a>>,
    pub expr: Expr,
}

impl SumPlan<'_> {
    /// The total of one row: `text(i)` is clause `i`'s raw score, `bonus[j]` the transformed value
    /// of bonus `j` for the row's class, and `doc(j)` row-wise term `j`.
    pub(crate) fn total(
        &self,
        text: &dyn Fn(usize) -> f64,
        bonus: &[f64],
        doc: &dyn Fn(usize) -> f64,
    ) -> f64 {
        fn go(e: &Expr, t: &dyn Fn(usize) -> f64, b: &[f64], d: &dyn Fn(usize) -> f64) -> f64 {
            match e {
                Expr::Text(i) => t(*i),
                Expr::Bonus(j) => b[*j],
                Expr::Doc(j) => d(*j),
                Expr::Const(c) => *c,
                Expr::Sum(xs) => xs.iter().map(|x| go(x, t, b, d)).sum(),
                Expr::Max(xs) => xs.iter().map(|x| go(x, t, b, d)).fold(0.0, f64::max),
                Expr::Mul(w, x) => w * go(x, t, b, d),
                Expr::Saturate {
                    midpoint,
                    exponent,
                    expr,
                } => {
                    let x = go(expr, t, b, d).max(0.0);
                    let ratio = 1.0 / (1.0 + (x / midpoint).powf(*exponent));
                    1.0 - ratio
                }
            }
        }
        go(&self.expr, text, bonus, doc)
    }
    /// The largest total a row can reach when no text clause scores above `cutoffs`.
    pub(crate) fn bound(&self, cutoffs: &[f32], bonus: &[f64]) -> f64 {
        self.total(&|i| f64::from(cutoffs[i]), bonus, &|j| self.docs[j].upper)
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
    store: Storage,
    seg: bf_wal::Segment,
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
        store: store.clone(),
        seg: seg.clone(),
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
        state: &NamespaceState,
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
            let Some(c) = self.class_candidates(state, plan, class, n, k).await? else {
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
        state: &NamespaceState,
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
                .search_prefetched(
                    &clause.query,
                    clause.stats,
                    &clause.cfg,
                    n,
                    Some(&class.rows),
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
        let bound = plan.bound(&cutoffs, &class.bonus);
        // Row-wise terms give rows that match no text a total of their own (not one shared value),
        // so only the bound can prove that nothing outside the lists wins.
        let exhaustive = plan.docs.is_empty() && lists.iter().all(|(full, _)| !full);
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
        let docs = self.row_terms(state, plan, &union).await?;
        let mut rows: Vec<(u32, f64)> = union
            .iter()
            .map(|row| {
                let t = plan.total(
                    &|i| f64::from(exact[i].get(&row).copied().unwrap_or(0.0)),
                    &class.bonus,
                    &|j| docs[&row][j],
                );
                (row, t)
            })
            .collect();
        if exhaustive {
            // Rows outside the union match no text at all and total the bare bonus.
            let bare = plan.total(&|_| 0.0, &class.bonus, &|_| 0.0);
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

    /// Each row's row-wise terms, evaluated exactly as the full scorer would on the row's document.
    async fn row_terms(
        &self,
        state: &NamespaceState,
        plan: &SumPlan<'_>,
        rows: &RoaringBitmap,
    ) -> Result<HashMap<u32, Vec<f64>>, ApiError> {
        if plan.docs.is_empty() || rows.is_empty() {
            return Ok(HashMap::new());
        }
        let columns: std::collections::BTreeSet<String> = plan
            .docs
            .iter()
            .flat_map(|d| d.columns.iter().cloned())
            .collect();
        let docs = segment::projected_rows(&self.store, &self.seg, rows, Some(&columns)).await?;
        let mut out = HashMap::with_capacity(docs.len());
        for row in rows {
            let doc = docs.get(&row).ok_or_else(segment::corrupt)?;
            let values = plan
                .docs
                .iter()
                .map(|d| crate::query::score(state, doc, d.rank))
                .collect::<Result<Vec<_>, _>>()?;
            out.insert(row, values);
        }
        Ok(out)
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
        for (midpoint, exponent) in [
            (3.0, 1.0),
            (1e-6, 20.0),
            (2.5, 0.3),
            (1e12, 0.05),
            (7.0, 2.0),
        ] {
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
