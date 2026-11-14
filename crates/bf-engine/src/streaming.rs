//! Bounded scalar scans: one column block, top-k rows, or aggregate states at a time.
use crate::{NamespaceState, filter, query, segment, snapshot};
use bf_storage::Storage;
use bf_types::*;
use serde_json::{Value, json};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap},
};

#[derive(Clone)]
struct RankKey {
    values: Vec<(Value, bool)>,
    id: Id,
}
impl PartialEq for RankKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for RankKey {}
impl PartialOrd for RankKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RankKey {
    fn cmp(&self, other: &Self) -> Ordering {
        for ((a, desc), (b, _)) in self.values.iter().zip(&other.values) {
            let cmp = filter::compare(a, b);
            let cmp = if *desc { cmp.reverse() } else { cmp };
            if !cmp.is_eq() {
                return cmp;
            }
        }
        self.id.cmp(&other.id)
    }
}
struct Ranked {
    key: RankKey,
    doc: Document,
    group: String,
}
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
impl std::borrow::Borrow<RankKey> for Ranked {
    fn borrow(&self) -> &RankKey {
        &self.key
    }
}

/// With diversification, both the global winners and each group contain at most k entries.
#[derive(Default)]
struct Winners {
    heap: BinaryHeap<Ranked>,
    ranked: BTreeSet<Ranked>,
    groups: HashMap<String, BTreeSet<RankKey>>,
}
impl Winners {
    fn insert(&mut self, row: Ranked, k: usize, per: Option<usize>) {
        let Some(per) = per else {
            if self.heap.len() < k {
                self.heap.push(row);
            } else if self.heap.peek().is_some_and(|worst| row < *worst) {
                self.heap.pop();
                self.heap.push(row);
            }
            return;
        };
        if let Some(group) = self.groups.get(&row.group)
            && group.len() >= per
        {
            let worst = group.last().expect("nonempty group").clone();
            if row.key >= worst {
                return;
            }
            let old = self.ranked.take(&worst).expect("group winner");
            self.remove_group(&old);
        }
        if self.ranked.len() == k {
            if self.ranked.last().is_some_and(|worst| row >= *worst) {
                return;
            }
            let old = self.ranked.pop_last().expect("full winners");
            self.remove_group(&old);
        }
        self.groups
            .entry(row.group.clone())
            .or_default()
            .insert(row.key.clone());
        self.ranked.insert(row);
    }
    fn remove_group(&mut self, row: &Ranked) {
        let group = self.groups.get_mut(&row.group).expect("winner group");
        group.remove(&row.key);
        if group.is_empty() {
            self.groups.remove(&row.group);
        }
    }
    fn documents(self) -> BTreeMap<Id, Document> {
        self.heap
            .into_iter()
            .chain(self.ranked)
            .map(|r| (r.key.id, r.doc))
            .collect()
    }
}
#[derive(Default)]
struct Accumulator {
    count: u64,
    integer: i128,
    integer_overflow: bool,
    float: f64,
    has_float: bool,
    extremum: Option<Value>,
}
impl Accumulator {
    fn add(&mut self, doc: &Document, agg: &Aggregate) {
        let value = agg
            .attribute
            .as_ref()
            .and_then(|a| doc.get(a))
            .filter(|v| !v.is_null());
        if agg.attribute.is_none() || value.is_some() {
            self.count += 1;
        }
        let Some(value) = value else {
            return;
        };
        match agg.op {
            AggregateOp::Count => {}
            AggregateOp::Sum => {
                self.has_float |= value.as_number().is_some_and(|v| v.is_f64());
                self.float += value.as_f64().unwrap_or(0.0);
                let n = value
                    .as_i64()
                    .map(i128::from)
                    .or_else(|| value.as_u64().map(i128::from))
                    .unwrap_or(0);
                if let Some(sum) = self.integer.checked_add(n) {
                    self.integer = sum;
                } else {
                    self.integer_overflow = true;
                }
            }
            AggregateOp::Min | AggregateOp::Max => {
                if self.extremum.as_ref().is_none_or(|old| {
                    let cmp = filter::compare(value, old);
                    if agg.op == AggregateOp::Min {
                        cmp.is_lt()
                    } else {
                        cmp.is_gt()
                    }
                }) {
                    self.extremum = Some(value.clone());
                }
            }
        }
    }
    fn finish(self, agg: &Aggregate) -> Result<Value, ApiError> {
        Ok(match agg.op {
            AggregateOp::Count => json!(self.count),
            AggregateOp::Min | AggregateOp::Max => self.extremum.unwrap_or(Value::Null),
            AggregateOp::Sum if self.has_float => {
                if !self.float.is_finite() {
                    return Err(ApiError::bad("aggregation sum overflow"));
                }
                json!(self.float)
            }
            AggregateOp::Sum => {
                if self.integer_overflow {
                    return Err(ApiError::bad("aggregation sum overflow"));
                }
                if let Ok(n) = i64::try_from(self.integer) {
                    json!(n)
                } else if let Ok(n) = u64::try_from(self.integer) {
                    json!(n)
                } else {
                    return Err(ApiError::bad("aggregation sum overflow"));
                }
            }
        })
    }
}
#[derive(Clone)]
struct GroupKey(Vec<Value>, String);
impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for GroupKey {}
impl PartialOrd for GroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for GroupKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .iter()
            .zip(&other.0)
            .map(|(a, b)| filter::compare(a, b))
            .find(|c| !c.is_eq())
            .unwrap_or_else(|| self.1.cmp(&other.1))
    }
}
struct Scan<'a> {
    q: &'a QueryRequest,
    winners: Winners,
    groups: BTreeMap<GroupKey, Vec<Accumulator>>,
}
impl Scan<'_> {
    fn row(&mut self, id: Id, doc: Document) {
        let k = self.q.total().unwrap_or(10000) + self.q.offset;
        if let Some(RankBy::Order(keys)) = &self.q.rank_by {
            let key = RankKey {
                id,
                values: keys
                    .iter()
                    .map(|key| {
                        (
                            doc.get(&key.attribute).cloned().unwrap_or(Value::Null),
                            key.direction == Direction::Desc,
                        )
                    })
                    .collect(),
            };
            let per = self.q.limit.as_ref().and_then(Limit::per);
            let group = per
                .map(|p| {
                    serde_json::to_string(
                        &p.attributes
                            .iter()
                            .map(|a| doc.get(a).unwrap_or(&Value::Null))
                            .collect::<Vec<_>>(),
                    )
                    .expect("group serializes")
                })
                .unwrap_or_default();
            self.winners
                .insert(Ranked { key, doc, group }, k, per.map(|p| p.limit));
        } else {
            self.group_keys(&doc, 0, &mut Vec::new(), k);
        }
    }
    // Generate each array-group product incrementally; never materialize its Cartesian product.
    fn group_keys(&mut self, doc: &Document, depth: usize, key: &mut Vec<Value>, k: usize) {
        let groups = self.q.group_by.as_deref().unwrap_or_default();
        if depth == groups.len() {
            let key = GroupKey(
                key.clone(),
                serde_json::to_string(key).expect("group serializes"),
            );
            if !self.groups.contains_key(&key) && self.groups.len() == k {
                if self
                    .groups
                    .last_key_value()
                    .is_some_and(|(worst, _)| key >= *worst)
                {
                    return;
                }
                self.groups.pop_last();
            }
            let aggregates = self.q.aggregate_by.as_ref().expect("aggregate scan");
            let accumulators = self.groups.entry(key).or_insert_with(|| {
                (0..aggregates.len())
                    .map(|_| Accumulator::default())
                    .collect()
            });
            for (acc, agg) in accumulators.iter_mut().zip(aggregates.values()) {
                acc.add(doc, agg);
            }
            return;
        }
        let values = match &groups[depth] {
            GroupBy::Attribute(name) => vec![doc.get(name).cloned().unwrap_or(Value::Null)],
            GroupBy::ForEachUnique { attribute, .. } => {
                let mut values = doc
                    .get(attribute)
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_else(|| vec![Value::Null]);
                values.sort_by(filter::compare);
                values.dedup();
                values
            }
        };
        for value in values {
            key.push(value);
            self.group_keys(doc, depth + 1, key, k);
            key.pop();
        }
    }
}
pub(crate) fn supports(q: &QueryRequest) -> bool {
    q.queries.is_none() && (q.aggregate_by.is_some() || matches!(q.rank_by, Some(RankBy::Order(_))))
}
async fn scan<'q>(
    store: &Storage,
    state: &NamespaceState,
    q: &'q QueryRequest,
) -> Result<Scan<'q>, ApiError> {
    let mut scan = Scan {
        q,
        winners: Winners::default(),
        groups: BTreeMap::new(),
    };
    // Scalars stay bounded to one 256-row block. Requested vectors remain opt-in.
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
        let mut rows = allowed.iter().peekable();
        while let Some(first) = rows.peek().copied() {
            let block = first / 256;
            let mut selected = roaring::RoaringBitmap::new();
            while rows.peek().is_some_and(|row| *row / 256 == block) {
                selected.insert(rows.next().expect("peeked row"));
            }
            for (id, mut doc) in
                segment::projected_documents(store, seg, &selected, Some(&columns)).await?
            {
                doc.retain(|name, _| state.schema.contains_key(name));
                scan.row(id, doc);
            }
        }
    }
    for (id, doc) in &state.docs {
        if q.filters
            .as_ref()
            .map(|f| filter::matches(f, Some(doc), None, &state.schema))
            .transpose()?
            .unwrap_or(true)
        {
            scan.row(id.clone(), doc.clone());
        }
    }
    Ok(scan)
}
/// Scalar subqueries feeding RRF also retain only competitive rows.
pub(crate) async fn order_candidates(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
) -> Result<NamespaceState, ApiError> {
    let mut candidates = state.clone();
    candidates.docs = scan(store, state, q).await?.winners.documents();
    Ok(candidates)
}
pub(crate) async fn execute(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
) -> Result<QueryResponse, ApiError> {
    let mut scan = scan(store, state, q).await?;
    if q.rank_by.is_some() {
        let mut candidates = state.clone();
        candidates.docs = scan.winners.documents();
        return query::execute(&candidates, q);
    }
    let aggregates = q.aggregate_by.as_ref().expect("aggregate scan");
    if q.group_by.is_none() && scan.groups.is_empty() {
        scan.groups.insert(
            GroupKey(vec![], "[]".into()),
            (0..aggregates.len())
                .map(|_| Accumulator::default())
                .collect(),
        );
    }
    let mut rows = Vec::new();
    for (key, accumulators) in scan.groups {
        let mut row = Document::new();
        for (group, value) in q.group_by.iter().flatten().zip(key.0) {
            let label = match group {
                GroupBy::Attribute(name) => name,
                GroupBy::ForEachUnique { label, .. } => label,
            };
            row.insert(label.clone(), value);
        }
        for ((label, agg), acc) in aggregates.iter().zip(accumulators) {
            row.insert(label.clone(), acc.finish(agg)?);
        }
        rows.push(row);
    }
    Ok(QueryResponse {
        result: if q.group_by.is_some() {
            SingleQueryResult {
                aggregation_groups: Some(rows),
                ..Default::default()
            }
        } else {
            SingleQueryResult {
                aggregations: rows.pop().map(|row| row.into_iter().collect()),
                ..Default::default()
            }
        },
        results: None,
        performance: Default::default(),
        billing: Default::default(),
    })
}
