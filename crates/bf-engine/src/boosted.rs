//! `Sum(BM25, boost)` where the boost is a non-decreasing function of one numeric attribute, answered
//! by opening only the text windows that can still reach the `k`th total.
//!
//! The text index bounds every window of 128 rows (`bf_text::SegmentReader::window_bounds`). The
//! segment keeps one maximum of the attribute per window (read from the column the first time and
//! held in the shared read cache). A row's total is its BM25 score plus the boost of its value, so
//! no row of a window totals more than `text bound + boost(maximum)`: windows are opened in
//! descending order of that sum, each batch is scored exactly (the same arithmetic the ordinal
//! scorer runs, so totals carry its bits), and the walk stops at the first window whose bound is
//! strictly below the `k`th total. The strict test keeps windows that can only tie, because a
//! tied row with a lower ID would win. Windows that match no text are bounded by the boost alone,
//! so a row that matches no query term can still be found when its attribute is large.
//!
//! A segment where the attribute is missing or not a number in some row has no usable maxima, and
//! the ordinal scorer answers it.
use crate::{
    NamespaceState,
    ordinal_scan::{Contender, offer},
    segment,
    sum_text::{self, Scores},
};
use bf_storage::Storage;
use bf_types::*;
use roaring::RoaringBitmap;
use std::{
    collections::BinaryHeap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// Window counts of the boosted path since the process started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BoostOutcomes {
    /// Segments the path answered (a segment with no usable maxima is not counted).
    pub segments: u64,
    /// Windows with a bound above zero and an allowed row, in those segments.
    pub candidates: u64,
    /// Windows the path opened and scored.
    pub opened: u64,
}
static SEGMENTS: AtomicU64 = AtomicU64::new(0);
static CANDIDATES: AtomicU64 = AtomicU64::new(0);
static OPENED: AtomicU64 = AtomicU64::new(0);
/// How many windows the boosted path considered and opened, for tests and benchmarks.
pub fn boost_outcomes() -> BoostOutcomes {
    BoostOutcomes {
        segments: SEGMENTS.load(Ordering::Relaxed),
        candidates: CANDIDATES.load(Ordering::Relaxed),
        opened: OPENED.load(Ordering::Relaxed),
    }
}

/// A rank of the form `Sum(clause, term)`: one BM25 clause and one term that reads a single numeric
/// attribute and never decreases as its value grows (a non-negative weight, `Saturate`, and so on).
pub(crate) struct Boost<'a> {
    pub clause: &'a RankBy,
    term: &'a RankBy,
    attribute: String,
}

impl<'a> Boost<'a> {
    pub(crate) fn of(rank: &'a RankBy) -> Option<Self> {
        let RankBy::Sum(xs) = rank else {
            return None;
        };
        let [a, b] = xs.as_slice() else {
            return None;
        };
        let (clause, term) = match (a, b) {
            (RankBy::Bm25 { .. }, RankBy::Bm25 { .. }) => return None,
            (RankBy::Bm25 { .. }, _) => (a, b),
            (_, RankBy::Bm25 { .. }) => (b, a),
            _ => return None,
        };
        let attribute = sum_text::monotone_attribute(term)?;
        (attribute != "id").then_some(Self {
            clause,
            term,
            attribute,
        })
    }
    /// The attribute the boost reads.
    pub(crate) fn attribute(&self) -> &str {
        &self.attribute
    }
}

/// The term's value at an attribute value, with the full scorer's operations (the caller adds a
/// relative allowance for the order the total's terms are added in).
fn term_at(x: &RankBy, v: f64) -> f64 {
    match x {
        RankBy::Constant(n) => n.as_f64().unwrap_or(0.0),
        RankBy::Attribute(_) => v,
        RankBy::Product { weight, expr, .. } => weight * term_at(expr, v),
        RankBy::Sum(xs) => xs.iter().map(|x| term_at(x, v)).sum(),
        RankBy::Max(xs) => xs.iter().map(|x| term_at(x, v)).fold(0.0, f64::max),
        RankBy::Saturate { expr, params } => {
            let (midpoint, exponent) =
                sum_text::bounded_params(params).expect("a monotone term has bounded parameters");
            let x = term_at(expr, v).max(0.0);
            1.0 - 1.0 / (1.0 + (x / midpoint).powf(exponent))
        }
        _ => f64::INFINITY,
    }
}

/// An attribute's value in every row of a segment and its largest value in each window; `None` when
/// some row has no numeric value for it (kept so the column is not read again to learn that).
pub(crate) struct WindowMaxima(Option<Column>);
struct Column {
    values: Vec<f64>,
    maxima: Vec<f64>,
}

/// The maxima of `attribute` over each window of the segment, read from the column once per
/// segment and kept in the shared read cache.
async fn maxima(
    store: &Storage,
    state: &NamespaceState,
    seg: &bf_wal::Segment,
    attribute: &str,
) -> Result<Arc<WindowMaxima>, ApiError> {
    if !matches!(
        state
            .schema
            .get(attribute)
            .and_then(|a| a.data_type.as_ref()),
        Some(AttributeType::Scalar(
            ScalarType::Int | ScalarType::Uint | ScalarType::Float
        ))
    ) {
        return Ok(Arc::new(WindowMaxima(None)));
    }
    let Some(docs) = seg.files.get("docs") else {
        return Ok(Arc::new(WindowMaxima(None)));
    };
    let rows = seg.row_count as usize;
    let windows = rows.div_ceil(bf_text::WINDOW);
    let key = format!("{}:{attribute}:window-maxima", docs.key);
    let bytes = (rows + windows) as u64 * 8 + 64;
    if let Some(found) = crate::read_cache::get::<WindowMaxima>(&key, bytes) {
        return Ok(found);
    }
    // A column that would take more than an eighth of the read cache would evict what other queries
    // share, and could not stay resident: the ordinal scorer answers such a segment.
    if bytes > crate::read_cache::capacity() / MAX_CACHE_SHARE {
        return Ok(Arc::new(WindowMaxima(None)));
    }
    let unusable = || crate::read_cache::insert(&key, bytes, WindowMaxima(None));
    let Some(values) = segment::numeric_column(store, seg, attribute).await? else {
        return Ok(unusable());
    };
    let mut best = vec![f64::NEG_INFINITY; windows];
    for (row, &v) in values.iter().enumerate() {
        let w = &mut best[row / bf_text::WINDOW];
        *w = w.max(v);
    }
    Ok(crate::read_cache::insert(
        &key,
        bytes,
        WindowMaxima(Some(Column {
            values,
            maxima: best,
        })),
    ))
}

/// The segment's contenders: the rows to hydrate, and the clause's hits among them.
pub(crate) struct Selection {
    pub rows: RoaringBitmap,
    pub hits: Vec<bf_text::SearchHit>,
}

/// A segment's cached values may take at most `1 / MAX_CACHE_SHARE` of the read cache.
const MAX_CACHE_SHARE: u64 = 8;
/// Windows in the first batch; each later batch is four times larger, up to `MAX_BATCH`.
const FIRST_BATCH: usize = 16;
const MAX_BATCH: usize = 512;
/// Relative allowance for the order a total's two terms are added in (the bound adds in `f64`).
const SLACK: f64 = 1e-9;

/// The best `k` rows of the segment by total (ties by ID), or `None` when this segment has no
/// usable maxima and the ordinal scorer must answer it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn select(
    store: &Storage,
    state: &NamespaceState,
    seg: &bf_wal::Segment,
    reader: &bf_text::SegmentReader,
    boost: &Boost<'_>,
    query: &bf_text::Query,
    stats: &bf_text::CorpusStats,
    cfg: &bf_types::FullTextConfig,
    allowed: &RoaringBitmap,
    k: usize,
) -> Result<Option<Selection>, ApiError> {
    let started = std::time::Instant::now();
    let maxima = maxima(store, state, seg, boost.attribute()).await?;
    let Some(column) = maxima.0.as_ref() else {
        return Ok(None);
    };
    crate::ann::stage(started, "boost maxima");
    let text = reader
        .window_bounds(query, stats, cfg)
        .await
        .map_err(segment::index_error)?;
    crate::ann::stage(started, "boost text bounds");
    let ids = segment::ids(store, seg).await?;
    let rows = seg.row_count as usize;
    let mut order: Vec<(f64, usize)> = Vec::new();
    for (w, &m) in column.maxima.iter().enumerate() {
        let (start, end) = (
            (w * bf_text::WINDOW) as u32,
            ((w + 1) * bf_text::WINDOW).min(rows) as u32,
        );
        let bound = (text.get(w).copied().unwrap_or(0.0) + term_at(boost.term, m)) * (1.0 + SLACK);
        if bound > 0.0 && allowed.range(start..end).next().is_some() {
            order.push((bound, w));
        }
    }
    order.sort_unstable_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut best: BinaryHeap<Contender> = BinaryHeap::new();
    let mut all_hits: Vec<bf_text::SearchHit> = Vec::new();
    let (mut next, mut batch, mut opened) = (0, FIRST_BATCH, 0);
    while next < order.len() && k > 0 {
        let floor = (best.len() == k)
            .then(|| best.peek().map(|c| c.score))
            .flatten();
        let mut mask = RoaringBitmap::new();
        let mut taken = 0;
        while next < order.len() && taken < batch {
            let (bound, w) = order[next];
            // Bounds descend and the floor only rises: nothing after this window can reach it.
            if floor.is_some_and(|f| bound < f) {
                next = order.len();
                break;
            }
            next += 1;
            taken += 1;
            let (start, end) = (
                (w * bf_text::WINDOW) as u32,
                ((w + 1) * bf_text::WINDOW).min(rows) as u32,
            );
            mask.extend(allowed.range(start..end));
        }
        if mask.is_empty() {
            break;
        }
        opened += taken;
        let hits = reader
            .search_prefetched(query, stats, cfg, mask.len() as usize, Some(&mask))
            .await
            .map_err(segment::index_error)?;
        let scores = Scores::new(hits.clone(), seg.row_count);
        for row in &mask {
            // The full scorer's arithmetic: the clause's `f32` score as `f64`, plus the term.
            let score =
                f64::from(scores.get(row)) + term_at(boost.term, column.values[row as usize]);
            if score <= 0.0 {
                continue;
            }
            offer(
                &mut best,
                Contender {
                    score,
                    id: &ids[row as usize],
                    row,
                },
                k,
            );
        }
        all_hits.extend(hits);
        batch = (batch * 4).min(MAX_BATCH);
    }
    SEGMENTS.fetch_add(1, Ordering::Relaxed);
    CANDIDATES.fetch_add(order.len() as u64, Ordering::Relaxed);
    OPENED.fetch_add(opened as u64, Ordering::Relaxed);
    if crate::ann::trace_stages() {
        let ideal = best.peek().map_or(0, |worst| {
            order
                .iter()
                .filter(|(bound, _)| *bound >= worst.score)
                .count()
        });
        eprintln!(
            "STAGE boost windows {} candidates {} opened {opened} ideal {ideal}",
            column.maxima.len(),
            order.len()
        );
    }
    let mut selected = RoaringBitmap::new();
    for c in &best {
        selected.insert(c.row);
    }
    all_hits.retain(|h| selected.contains(h.row));
    crate::ann::stage(started, "boost total");
    Ok(Some(Selection {
        rows: selected,
        hits: all_hits,
    }))
}
