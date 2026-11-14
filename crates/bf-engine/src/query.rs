use crate::{Engine, NamespaceState, filter, text};
use bf_types::*;
use serde_json::{Value, json};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

pub(crate) fn execute(
    state: &NamespaceState,
    request: &QueryRequest,
) -> Result<QueryResponse, ApiError> {
    if request.rerank_by.as_ref().is_some_and(Value::is_object) {
        return Err(ApiError::bad(
            "model rerank_by is not supported by blowfish yet",
        ));
    }
    let (result, results) = if let Some(queries) = &request.queries {
        if queries.is_empty() || queries.len() > 16 {
            return Err(ApiError::bad("multi-query requires one to sixteen queries"));
        }
        if request.rank_by.is_some()
            || request.filters.is_some()
            || request.aggregate_by.is_some()
            || request.group_by.is_some()
            || request.top_k.is_some()
            || request.include_attributes.is_some()
            || request.exclude_attributes.is_some()
        {
            return Err(ApiError::bad(
                "queries cannot be combined with single-query fields",
            ));
        }
        let mut results = vec![];
        for q in queries {
            if q.queries.is_some() || q.rerank_by.is_some() {
                return Err(ApiError::bad(
                    "nested multi-query and subquery reranking are not supported",
                ));
            }
            results.push(single(state, q, request.vector_encoding)?);
        }
        if let Some(rerank) = &request.rerank_by {
            results = vec![rrf(state, request, rerank, results)?];
        } else if request.limit.is_some() || request.offset != 0 {
            return Err(ApiError::bad(
                "multi-query limit and offset require rerank_by",
            ));
        }
        (SingleQueryResult::default(), Some(results))
    } else {
        if request.rerank_by.is_some() {
            return Err(ApiError::bad(
                "model rerank_by is not supported by blowfish yet",
            ));
        }
        (single(state, request, request.vector_encoding)?, None)
    };
    Ok(QueryResponse {
        result,
        results,
        performance: QueryPerformance::default(),
        billing: QueryBilling::default(),
    })
}
fn limits(q: &QueryRequest, required: bool) -> Result<usize, ApiError> {
    if q.top_k.is_some() && q.limit.is_some() {
        return Err(ApiError::bad("top_k and limit are mutually exclusive"));
    }
    let total = q
        .total()
        .or((!required).then_some(10000))
        .ok_or_else(|| ApiError::bad("ranked queries require top_k or limit"))?;
    if total == 0 || total > 10000 || q.offset.checked_add(total).is_none_or(|n| n > 10000) {
        return Err(ApiError::bad(
            "limit and offset must fit within 10000 results",
        ));
    }
    if let Some(per) = q.limit.as_ref().and_then(Limit::per)
        && (per.limit == 0 || per.attributes.is_empty())
    {
        return Err(ApiError::bad(
            "limit.per requires attributes and a positive limit",
        ));
    }
    Ok(total)
}
fn single(
    state: &NamespaceState,
    q: &QueryRequest,
    encoding: VectorEncoding,
) -> Result<SingleQueryResult, ApiError> {
    if !q.extra.is_empty() {
        return Err(ApiError::bad(format!(
            "{} is not supported by blowfish yet",
            q.extra.keys().next().expect("nonempty")
        )));
    }
    if q.rank_by.is_some() == q.aggregate_by.is_some() {
        return Err(ApiError::bad(
            "provide exactly one of rank_by and aggregate_by",
        ));
    }
    if q.include_attributes.is_some() && q.exclude_attributes.is_some() {
        return Err(ApiError::bad(
            "include_attributes and exclude_attributes are mutually exclusive",
        ));
    }
    if q.group_by.is_some() && q.aggregate_by.is_none() {
        return Err(ApiError::bad("group_by requires aggregate_by"));
    }
    if q.distance_metric.is_some_and(|m| m != state.metric) {
        return Err(ApiError::bad(
            "query distance metric does not match namespace",
        ));
    }
    if let Some(IncludeAttributes::Names(names)) = &q.include_attributes {
        for name in names {
            attribute(state, name)?;
        }
    }
    if let Some(per) = q.limit.as_ref().and_then(Limit::per) {
        for name in &per.attributes {
            attribute(state, name)?;
        }
    }
    if let Some(f) = &q.filters {
        filter::validate(f, &state.schema, false)?;
        crate::pattern::ready(f, &state.schema, &state.segments)?;
    }
    if let Some(rank) = &q.rank_by {
        validate_rank(state, rank, false)?;
        if let RankBy::Vector { exact: true, .. } | RankBy::MultiVector { exact: true, .. } = rank
            && q.filters.is_none()
        {
            return Err(ApiError::bad("kNN requires filters"));
        }
    }
    crate::highlight::validate(state, q)?;
    let mut docs = vec![];
    for (id, doc) in &state.docs {
        if q.filters
            .as_ref()
            .map(|f| filter::matches(f, Some(doc), None, &state.schema))
            .transpose()?
            .unwrap_or(true)
        {
            docs.push((id, doc));
        }
    }
    if let Some(aggregates) = &q.aggregate_by {
        if q.include_attributes.is_some() || q.exclude_attributes.is_some() || q.offset != 0 {
            return Err(ApiError::bad(
                "aggregations cannot include attributes or use offset",
            ));
        }
        if aggregates.is_empty() || aggregates.len() > 8 {
            return Err(ApiError::bad("provide one to eight aggregate functions"));
        }
        for agg in aggregates.values() {
            if let Some(name) = &agg.attribute {
                let cfg = attribute(state, name)?;
                match (agg.op, cfg.data_type.as_ref()) {
                    (AggregateOp::Count, _) => {}
                    (
                        AggregateOp::Sum,
                        Some(AttributeType::Scalar(
                            ScalarType::Int | ScalarType::Uint | ScalarType::Float,
                        )),
                    ) => {}
                    (AggregateOp::Min | AggregateOp::Max, Some(AttributeType::Scalar(s)))
                        if *s != ScalarType::Bytes => {}
                    _ => {
                        return Err(ApiError::bad(
                            "aggregation requires a compatible scalar attribute",
                        ));
                    }
                }
            }
        }
        if let Some(groups) = &q.group_by {
            let total = limits(q, false)?;
            if groups.is_empty() {
                return Err(ApiError::bad("group_by cannot be empty"));
            }
            let mut labels = vec![];
            for group in groups {
                let (label, name, array) = match group {
                    GroupBy::Attribute(s) => (s, s, false),
                    GroupBy::ForEachUnique { label, attribute } => (label, attribute, true),
                };
                if labels.contains(label) || aggregates.contains_key(label) {
                    return Err(ApiError::bad(
                        "group labels must be unique and distinct from aggregation labels",
                    ));
                }
                labels.push(label.clone());
                let t = attribute(state, name)?.data_type.as_ref();
                if array != matches!(t, Some(AttributeType::Array(_))) {
                    return Err(ApiError::bad("array grouping requires ForEachUnique"));
                }
            }
            let mut grouped: HashMap<String, (Vec<Value>, Vec<&Document>)> = HashMap::new();
            for (_, doc) in docs {
                let mut keys = vec![vec![]];
                for group in groups {
                    let values = match group {
                        GroupBy::Attribute(name) => {
                            vec![doc.get(name).cloned().unwrap_or(Value::Null)]
                        }
                        GroupBy::ForEachUnique { attribute, .. } => {
                            let mut v = doc
                                .get(attribute)
                                .and_then(Value::as_array)
                                .cloned()
                                .unwrap_or_else(|| vec![Value::Null]);
                            v.sort_by(filter::compare);
                            v.dedup();
                            v
                        }
                    };
                    keys = keys
                        .into_iter()
                        .flat_map(|key| {
                            values.iter().map(move |v| {
                                let mut k = key.clone();
                                k.push(v.clone());
                                k
                            })
                        })
                        .collect();
                }
                for key in keys {
                    grouped
                        .entry(serde_json::to_string(&key).expect("key serializes"))
                        .or_insert_with(|| (key, vec![]))
                        .1
                        .push(doc);
                }
            }
            let mut grouped: Vec<_> = grouped.into_values().collect();
            grouped.sort_by(|(a, _), (b, _)| compare_keys(a, b));
            let mut rows = vec![];
            for (key, docs) in grouped.into_iter().take(total) {
                let mut row: Document = labels.iter().cloned().zip(key).collect();
                row.extend(aggregate(&docs, aggregates)?);
                rows.push(row);
            }
            return Ok(SingleQueryResult {
                aggregation_groups: Some(rows),
                ..Default::default()
            });
        }
        return Ok(SingleQueryResult {
            aggregations: Some(aggregate(
                &docs.into_iter().map(|(_, d)| d).collect::<Vec<_>>(),
                aggregates,
            )?),
            ..Default::default()
        });
    }
    let total = limits(q, true)?;
    let rank = q.rank_by.as_ref().expect("validated rank");
    let mut ranked = vec![];
    for (id, doc) in docs {
        let score = match rank {
            RankBy::Order(_) => None,
            RankBy::MultiVector {
                attribute, vectors, ..
            } => {
                let Some(values) = doc
                    .get(attribute)
                    .and_then(Value::as_array)
                    .filter(|vs| !vs.is_empty())
                else {
                    continue;
                };
                let docs = values
                    .iter()
                    .map(decode_vector)
                    .collect::<Result<Vec<_>, _>>()?;
                Some(
                    vectors
                        .iter()
                        .map(|q| {
                            docs.iter()
                                .map(|d| distance(d, q, state.metric))
                                .fold(f64::INFINITY, f64::min)
                        })
                        .sum(),
                )
            }
            RankBy::Vector {
                attribute, vector, ..
            } => {
                if let Some(scores) = state.scores.get(&crate::indexed::rank_key(rank)) {
                    let Some(score) = scores.get(id) else {
                        continue;
                    };
                    Some(*score)
                } else {
                    let Some(value) = doc.get(attribute) else {
                        continue;
                    };
                    Some(distance(&decode_vector(value)?, vector, state.metric))
                }
            }
            _ => {
                let score = score(state, doc, rank)?;
                if score <= 0.0 {
                    continue;
                }
                Some(score)
            }
        };
        ranked.push((id, doc, score));
    }
    ranked.sort_by(|(a_id, a, a_score), (b_id, b, b_score)| {
        let order = match rank {
            RankBy::Order(keys) => {
                let mut cmp = Ordering::Equal;
                for key in keys {
                    cmp = filter::compare(
                        a.get(&key.attribute).unwrap_or(&Value::Null),
                        b.get(&key.attribute).unwrap_or(&Value::Null),
                    );
                    if key.direction == Direction::Desc {
                        cmp = cmp.reverse();
                    }
                    if cmp != Ordering::Equal {
                        break;
                    }
                }
                cmp
            }
            RankBy::Vector { .. } | RankBy::MultiVector { .. } => a_score
                .expect("distance")
                .total_cmp(&b_score.expect("distance")),
            _ => b_score.expect("score").total_cmp(&a_score.expect("score")),
        };
        order.then(a_id.cmp(b_id))
    });
    let mut counts = HashMap::new();
    let mut rows = vec![];
    let mut accepted = 0;
    for (_, doc, dist) in ranked {
        if let Some(per) = q.limit.as_ref().and_then(Limit::per) {
            let key = serde_json::to_string(
                &per.attributes
                    .iter()
                    .map(|k| doc.get(k).unwrap_or(&Value::Null))
                    .collect::<Vec<_>>(),
            )
            .expect("key serializes");
            let count = counts.entry(key).or_insert(0);
            if *count >= per.limit {
                continue;
            }
            *count += 1;
        }
        if accepted < q.offset {
            accepted += 1;
            continue;
        }
        rows.push(project(state, doc, q, encoding, dist)?);
        if rows.len() == total {
            break;
        }
    }
    Ok(SingleQueryResult {
        rows: Some(rows),
        ..Default::default()
    })
}
fn attribute<'a>(state: &'a NamespaceState, name: &str) -> Result<&'a AttributeSchema, ApiError> {
    state
        .schema
        .get(name)
        .ok_or_else(|| ApiError::bad(format!("unknown attribute '{name}'")))
}
pub(crate) fn validate_rank(
    state: &NamespaceState,
    rank: &RankBy,
    guarded: bool,
) -> Result<(), ApiError> {
    match rank {
        RankBy::UnsupportedEmbed { .. } => {
            return Err(ApiError::bad(
                "native Embed expressions are not supported by blowfish yet",
            ));
        }
        RankBy::Constant(_) => {}
        RankBy::Order(keys) => {
            for key in keys {
                if !matches!(
                    attribute(state, &key.attribute)?.data_type,
                    Some(AttributeType::Scalar(_))
                ) {
                    return Err(ApiError::bad("order-by requires scalar attributes"));
                }
            }
        }
        RankBy::MultiVector {
            attribute: name,
            vectors,
            ..
        } => {
            let Some(AttributeType::MultiVector { dimensions }) =
                attribute(state, name)?.data_type.as_ref()
            else {
                return Err(ApiError::bad(
                    "multi-vector ANN requires a [][N]f32 attribute with late_interaction enabled",
                ));
            };
            if vectors.is_empty() || vectors.iter().any(|v| v.len() != *dimensions) {
                return Err(ApiError::bad("invalid multi-vector query dimensions"));
            }
        }
        RankBy::Vector {
            attribute: name,
            vector,
            ..
        } => {
            if matches!(
                attribute(state, name)?.data_type,
                Some(AttributeType::MultiVector { .. })
            ) {
                return Err(ApiError::bad(
                    "late-interaction ANN requires an array of query vectors",
                ));
            }
            let dims = attribute(state, name)?
                .data_type
                .as_ref()
                .and_then(AttributeType::dimensions)
                .ok_or_else(|| ApiError::bad("ANN requires a dense-vector attribute"))?;
            if matches!(
                attribute(state, name)?.data_type,
                Some(AttributeType::VectorI8 { .. })
            ) {
                crate::write::normalize_value(
                    &json!(vector),
                    &AttributeType::VectorI8 { dimensions: dims },
                )?;
            }
            if vector.len() != dims {
                return Err(ApiError::bad(format!(
                    "Invalid query dimensions! Query has dimensions={}, but index has dimensions={dims}",
                    vector.len()
                )));
            }
        }
        RankBy::SparseVector {
            attribute: name, ..
        } => {
            let cfg = attribute(state, name)?;
            if cfg.data_type != Some(AttributeType::SparseVector) || cfg.sparse_knn.is_none() {
                return Err(ApiError::bad(
                    "SparseKNN requires a {}f16 attribute with sparse_knn enabled",
                ));
            }
        }
        RankBy::Bm25 {
            attribute: name,
            query,
            ..
        } => {
            let cfg = attribute(state, name)?.text_config().ok_or_else(|| {
                ApiError::bad(format!("full_text_search is not enabled for '{name}'"))
            })?;
            if serde_json::to_string(query)
                .expect("query serializes")
                .len()
                > 8194
            {
                return Err(ApiError::bad("full-text query exceeds 8192 bytes"));
            }
            if cfg.tokenizer == bf_text::Tokenizer::PreTokenizedArray && !query.is_array() {
                return Err(ApiError::bad("pre_tokenized_array requires an array query"));
            }
        }
        RankBy::Sum(xs) | RankBy::Max(xs) => {
            for x in xs {
                if matches!(
                    x,
                    RankBy::Order(_) | RankBy::Vector { .. } | RankBy::MultiVector { .. }
                ) {
                    return Err(ApiError::bad(
                        "score expressions cannot contain order-by or vector distance ranking",
                    ));
                }
                validate_rank(state, x, guarded || matches!(rank, RankBy::Max(_)))?;
            }
        }
        RankBy::Product { expr, .. } => {
            validate_score_child(state, expr, guarded)?;
        }
        RankBy::Filter(f) => {
            filter::validate(f, &state.schema, false)?;
            crate::pattern::ready(f, &state.schema, &state.segments)?;
        }
        RankBy::Attribute(name) => match attribute(state, name)?.data_type.as_ref() {
            Some(AttributeType::Scalar(ScalarType::Uint)) => {}
            Some(AttributeType::Scalar(ScalarType::Int | ScalarType::Float)) if guarded => {}
            _ => {
                return Err(ApiError::bad(
                    "Attribute scores require uint or signed numeric types wrapped in Max, Saturate, or Decay",
                ));
            }
        },
        RankBy::Saturate { expr, params } | RankBy::Decay { expr, params } => {
            validate_score_child(state, expr, true)?;
            let midpoint = parameter(params, "midpoint", 1.0)?;
            let exponent = parameter(params, "exponent", 1.0)?;
            if midpoint <= 0.0 || exponent <= 0.0 {
                return Err(ApiError::bad("midpoint and exponent must be positive"));
            }
        }
        RankBy::Dist { expr, origin } => {
            if let RankBy::Attribute(name) = &**expr {
                match attribute(state, name)?.data_type.as_ref() {
                    Some(AttributeType::Scalar(ScalarType::Datetime)) => {
                        datetime_millis(origin.as_str().ok_or_else(|| {
                            ApiError::bad("datetime origin must be an ISO string")
                        })?)?;
                    }
                    Some(AttributeType::Scalar(
                        ScalarType::Int | ScalarType::Uint | ScalarType::Float,
                    )) => {
                        if !origin.is_number() {
                            return Err(ApiError::bad("numeric Dist requires a numeric origin"));
                        }
                    }
                    _ => {
                        return Err(ApiError::bad(
                            "Dist requires numeric or datetime attributes",
                        ));
                    }
                }
            } else {
                validate_score_child(state, expr, true)?;
                if !origin.is_number() {
                    return Err(ApiError::bad("Dist requires a numeric origin"));
                }
            }
        }
    }
    Ok(())
}
fn validate_score_child(
    state: &NamespaceState,
    rank: &RankBy,
    guarded: bool,
) -> Result<(), ApiError> {
    if matches!(
        rank,
        RankBy::Order(_) | RankBy::Vector { .. } | RankBy::MultiVector { .. }
    ) {
        return Err(ApiError::bad("expected a score expression"));
    }
    validate_rank(state, rank, guarded)
}
fn parameter(params: &Value, name: &str, default: f64) -> Result<f64, ApiError> {
    let Some(v) = params.get(name) else {
        return Ok(default);
    };
    if let Some(n) = v.as_f64() {
        return Ok(n);
    }
    if name == "midpoint"
        && let Some(s) = v.as_str()
    {
        let (num, factor) = if let Some(s) = s.strip_suffix('s') {
            (s, 1000.0)
        } else if let Some(s) = s.strip_suffix('m') {
            (s, 60000.0)
        } else if let Some(s) = s.strip_suffix('h') {
            (s, 3600000.0)
        } else if let Some(s) = s.strip_suffix('d') {
            (s, 86400000.0)
        } else if let Some(s) = s.strip_suffix('w') {
            (s, 604800000.0)
        } else {
            return Err(ApiError::bad("invalid duration midpoint"));
        };
        return num
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite())
            .map(|n| n * factor)
            .ok_or_else(|| ApiError::bad("invalid duration midpoint"));
    }
    Err(ApiError::bad("score parameter must be numeric"))
}
pub(crate) fn score(
    state: &NamespaceState,
    doc: &Document,
    rank: &RankBy,
) -> Result<f64, ApiError> {
    Ok(match rank {
        RankBy::Constant(n) => n.as_f64().expect("JSON number fits f64"),
        RankBy::SparseVector {
            attribute, vector, ..
        } => {
            if let Some(scores) = state.scores.get(&crate::indexed::rank_key(rank)) {
                let id: Id = serde_json::from_value(doc["id"].clone()).expect("validated ID");
                *scores.get(&id).unwrap_or(&0.0)
            } else {
                crate::sparse::dot(doc.get(attribute), vector) as f64
            }
        }
        RankBy::Bm25 {
            attribute,
            query,
            params,
        } => {
            if let Some(scores) = state.scores.get(&crate::indexed::rank_key(rank)) {
                let id: Id = serde_json::from_value(doc["id"].clone()).expect("validated ID");
                *scores.get(&id).unwrap_or(&0.0)
            } else {
                let cfg = state.schema[attribute]
                    .text_config()
                    .expect("validated FTS");
                let query =
                    bf_text::Query::new(text::tokens(query, &cfg), text::prefix(params.as_ref()));
                let fallback;
                let stats = if let Some(stats) = state.corpus.get(attribute) {
                    stats
                } else {
                    fallback = bf_text::CorpusStats::from_documents(
                        &state
                            .docs
                            .values()
                            .filter_map(|d| d.get(attribute))
                            .filter(|v| !v.is_null())
                            .map(|v| text::tokens(v, &cfg))
                            .collect::<Vec<_>>(),
                    );
                    &fallback
                };
                bf_text::score_document(
                    &text::tokens(doc.get(attribute).unwrap_or(&Value::Null), &cfg),
                    &query,
                    stats,
                    &cfg,
                ) as f64
            }
        }
        RankBy::Sum(xs) => xs
            .iter()
            .map(|x| score(state, doc, x))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum(),
        RankBy::Max(xs) => xs
            .iter()
            .map(|x| score(state, doc, x))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .fold(0.0, f64::max),
        RankBy::Product { weight, expr, .. } => weight * score(state, doc, expr)?,
        RankBy::Filter(f) => {
            if filter::matches(f, Some(doc), None, &state.schema)? {
                1.0
            } else {
                0.0
            }
        }
        RankBy::Attribute(name) => doc.get(name).and_then(Value::as_f64).unwrap_or(0.0),
        RankBy::Saturate { expr, params } | RankBy::Decay { expr, params } => {
            let x = score(state, doc, expr)?.max(0.0);
            let m = parameter(params, "midpoint", 1.0)?;
            let e = parameter(params, "exponent", 1.0)?;
            let ratio = 1.0 / (1.0 + (x / m).powf(e));
            if matches!(rank, RankBy::Decay { .. }) {
                ratio
            } else {
                1.0 - ratio
            }
        }
        RankBy::Dist { expr, origin } => {
            if let RankBy::Attribute(name) = &**expr
                && state.schema[name].data_type == Some(AttributeType::Scalar(ScalarType::Datetime))
            {
                let value = doc
                    .get(name)
                    .and_then(Value::as_str)
                    .map(datetime_millis)
                    .transpose()?
                    .unwrap_or(0);
                (value as f64 - datetime_millis(origin.as_str().expect("validated origin"))? as f64)
                    .abs()
            } else {
                (score(state, doc, expr)? - origin.as_f64().expect("validated origin")).abs()
            }
        }
        _ => return Err(ApiError::bad("expected a score expression")),
    })
}
pub(crate) fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f64 {
    if metric == DistanceMetric::CosineDistance
        && (a.iter().all(|x| *x == 0.0) || b.iter().all(|x| *x == 0.0))
    {
        return 1.0;
    }
    crate::segment::vector_metric(metric)
        .distance(a, b)
        .expect("validated vectors") as f64
}
fn project(
    state: &NamespaceState,
    doc: &Document,
    q: &QueryRequest,
    encoding: VectorEncoding,
    dist: Option<f64>,
) -> Result<Document, ApiError> {
    let mut row = Document::new();
    row.insert("id".into(), doc["id"].clone());
    let attrs: Vec<_> = match &q.include_attributes {
        Some(IncludeAttributes::All(true)) => doc.keys().cloned().collect(),
        Some(IncludeAttributes::Names(names)) => names.clone(),
        _ if q.exclude_attributes.is_some() => doc
            .keys()
            .filter(|k| !q.exclude_attributes.as_ref().expect("present").contains(k))
            .cloned()
            .collect(),
        _ => vec![],
    };
    for name in attrs {
        if let Some(value) = doc.get(&name) {
            let value = if encoding == VectorEncoding::Base64 {
                match state.schema.get(&name).and_then(|s| s.data_type.as_ref()) {
                    Some(AttributeType::Vector { f16, .. }) => {
                        json!(encode_vector(&decode_vector(value)?, *f16))
                    }
                    Some(AttributeType::MultiVector { .. }) => {
                        return Err(ApiError::bad(
                            "base64 output is not supported for multi-vector arrays",
                        ));
                    }
                    Some(AttributeType::VectorI8 { .. }) => {
                        use base64::Engine as _;
                        json!(
                            base64::prelude::BASE64_STANDARD.encode(
                                decode_vector(value)?
                                    .iter()
                                    .map(|x| *x as i8 as u8)
                                    .collect::<Vec<_>>()
                            )
                        )
                    }
                    _ => value.clone(),
                }
            } else {
                value.clone()
            };
            row.insert(name, value);
        }
    }
    if let Some(dist) = dist {
        if !dist.is_finite() {
            return Err(ApiError::bad(
                "score expression produced a non-finite result",
            ));
        }
        row.insert("$dist".into(), json!(dist));
    }
    crate::highlight::project(state, doc, q, &mut row)?;
    Ok(row)
}
fn compare_keys(a: &[Value], b: &[Value]) -> Ordering {
    for (a, b) in a.iter().zip(b) {
        let c = filter::compare(a, b);
        if c != Ordering::Equal {
            return c;
        }
    }
    a.len().cmp(&b.len())
}
fn aggregate(
    docs: &[&Document],
    aggregates: &BTreeMap<String, Aggregate>,
) -> Result<BTreeMap<String, Value>, ApiError> {
    let mut out = BTreeMap::new();
    for (label, agg) in aggregates {
        let values: Vec<_> = docs
            .iter()
            .filter_map(|d| agg.attribute.as_ref().and_then(|a| d.get(a)))
            .filter(|v| !v.is_null())
            .collect();
        let value = match agg.op {
            AggregateOp::Count => json!(if agg.attribute.is_none() {
                docs.len()
            } else {
                values.len()
            }),
            AggregateOp::Min => values
                .iter()
                .min_by(|a, b| filter::compare(a, b))
                .map(|v| (*v).clone())
                .unwrap_or(Value::Null),
            AggregateOp::Max => values
                .iter()
                .max_by(|a, b| filter::compare(a, b))
                .map(|v| (*v).clone())
                .unwrap_or(Value::Null),
            AggregateOp::Sum => {
                if values
                    .iter()
                    .any(|v| v.as_number().is_some_and(|n| n.is_f64()))
                {
                    let sum: f64 = values.iter().filter_map(|v| v.as_f64()).sum();
                    if !sum.is_finite() {
                        return Err(ApiError::bad("aggregation sum overflow"));
                    }
                    json!(sum)
                } else {
                    let sum = values
                        .iter()
                        .map(|v| {
                            v.as_i64()
                                .map(i128::from)
                                .or_else(|| v.as_u64().map(i128::from))
                                .unwrap_or(0)
                        })
                        .try_fold(0_i128, |a, b| a.checked_add(b))
                        .ok_or_else(|| ApiError::bad("aggregation sum overflow"))?;
                    if let Ok(n) = i64::try_from(sum) {
                        json!(n)
                    } else if let Ok(n) = u64::try_from(sum) {
                        json!(n)
                    } else {
                        return Err(ApiError::bad("aggregation sum overflow"));
                    }
                }
            }
        };
        out.insert(label.clone(), value);
    }
    Ok(out)
}
fn rrf(
    state: &NamespaceState,
    q: &QueryRequest,
    rerank: &Value,
    results: Vec<SingleQueryResult>,
) -> Result<SingleQueryResult, ApiError> {
    let a = rerank
        .as_array()
        .ok_or_else(|| ApiError::bad("model rerank_by is not supported by blowfish yet"))?;
    if a.first() != Some(&json!("RRF")) || a.len() > 2 {
        return Err(ApiError::bad("invalid rerank_by expression"));
    }
    if results.len() < 2 || results.iter().any(|r| r.rows.is_none()) {
        return Err(ApiError::bad(
            "RRF requires at least two non-aggregate subqueries",
        ));
    }
    let params = a.get(1);
    if params.is_some_and(|p| !p.is_object()) {
        return Err(ApiError::bad("RRF parameters must be an object"));
    }
    let constant = params
        .and_then(|p| p.get("rank_constant"))
        .map(|v| {
            v.as_u64()
                .filter(|n| *n >= 1)
                .ok_or_else(|| ApiError::bad("rank_constant must be a positive integer"))
        })
        .transpose()?
        .unwrap_or(60) as f64;
    let weights = if let Some(weights) = params.and_then(|p| p.get("weights")) {
        let weights = weights
            .as_array()
            .ok_or_else(|| ApiError::bad("RRF weights must be an array"))?;
        if weights.len() != results.len() {
            return Err(ApiError::bad("RRF requires one weight per subquery"));
        }
        weights
            .iter()
            .map(|v| {
                v.as_f64()
                    .filter(|n| *n > 0.0)
                    .ok_or_else(|| ApiError::bad("RRF weights must be positive"))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![1.0; results.len()]
    };
    let total = limits(q, true)?;
    let mut fused: BTreeMap<Id, (f64, Document)> = BTreeMap::new();
    for (i, result) in results.into_iter().enumerate() {
        for (rank, mut row) in result.rows.expect("validated rows").into_iter().enumerate() {
            let id: Id = serde_json::from_value(row["id"].clone()).expect("valid ID");
            row.remove("$dist");
            let entry = fused.entry(id).or_insert((0.0, Document::new()));
            entry.0 += weights[i] / (constant + (rank + 1) as f64);
            entry.1.extend(row);
        }
    }
    let mut rows: Vec<_> = fused.into_iter().collect();
    rows.sort_by(|(a, (sa, _)), (b, (sb, _))| sb.total_cmp(sa).then(a.cmp(b)));
    let mut counts = HashMap::new();
    let mut accepted = 0;
    let mut out = vec![];
    for (id, (score, mut row)) in rows {
        if let Some(per) = q.limit.as_ref().and_then(Limit::per) {
            for name in &per.attributes {
                attribute(state, name)?;
            }
            let doc = &state.docs[&id];
            let key = serde_json::to_string(
                &per.attributes
                    .iter()
                    .map(|n| doc.get(n).unwrap_or(&Value::Null))
                    .collect::<Vec<_>>(),
            )
            .expect("key serializes");
            let count = counts.entry(key).or_insert(0);
            if *count >= per.limit {
                continue;
            }
            *count += 1;
        }
        if accepted < q.offset {
            accepted += 1;
            continue;
        }
        row.insert("$dist".into(), json!(score));
        out.push(row);
        if out.len() == total {
            break;
        }
    }
    Ok(SingleQueryResult {
        rows: Some(out),
        ..Default::default()
    })
}
pub(crate) async fn recall(engine: &Engine, ns: &str, request: Value) -> Result<Value, ApiError> {
    let (state, _) = engine.snapshot(ns, &Consistency::Strong).await?;

    let top = request.get("top_k").and_then(Value::as_u64).unwrap_or(10) as usize;
    let num = request.get("num").and_then(Value::as_u64).unwrap_or(25) as usize;
    if top == 0 || top > 10000 || num == 0 || num > 10000 {
        return Err(ApiError::bad("invalid recall sample count or top_k"));
    }
    let explicit = request.get("rank_by").filter(|v| !v.is_null());
    if explicit.is_some() && request.get("num").is_some_and(|v| !v.is_null() && v != 1) {
        return Err(ApiError::bad("recall with rank_by requires num=1"));
    }
    let filters = request
        .get("filters")
        .filter(|v| !v.is_null())
        .map(|v| Filter::parse(v.clone()))
        .transpose()?;
    let mut ranks = vec![];
    if let Some(rank) = explicit {
        ranks.push(RankBy::parse(rank.clone())?);
    } else if let Some((name, _)) = state.schema.iter().find(|(_, s)| {
        s.data_type
            .as_ref()
            .is_some_and(|t| t.dimensions().is_some())
    }) {
        let mut samples: Vec<_> = state.docs.values().take(num).cloned().collect();
        for seg in &state.segments {
            if samples.len() >= num {
                break;
            }
            let live = crate::snapshot::allowed(engine.storage(), &state, seg, None).await?;
            let rows = live.iter().take(num - samples.len()).collect();
            samples.extend(
                crate::segment::documents(engine.storage(), seg, &rows)
                    .await?
                    .into_values(),
            );
        }
        for doc in samples
            .iter()
            .cycle()
            .take(if samples.is_empty() { 0 } else { num })
        {
            if let Some(vector) = doc.get(name) {
                ranks.push(RankBy::parse(json!([name, "ANN", vector]))?);
            }
        }
    }
    let mut count = 0;
    let mut ann_count = 0;
    let mut recall_sum = 0.0;
    let mut truth = vec![];
    for rank in &ranks {
        let q = QueryRequest {
            rank_by: Some(rank.clone()),
            top_k: Some(top),
            filters: filters.clone(),
            include_attributes: Some(IncludeAttributes::All(
                request.get("include_ground_truth") == Some(&json!(true)),
            )),
            ..Default::default()
        };
        if !matches!(rank, RankBy::Vector { .. }) {
            return Err(ApiError::bad("recall requires vector ranking"));
        }
        let ann =
            crate::indexed::execute(engine.storage(), &state, &q, &engine.0.config, false).await?;
        let exact =
            crate::indexed::execute(engine.storage(), &state, &q, &engine.0.config, true).await?;
        recall_sum += crate::indexed::recall(&ann, &exact);
        ann_count += ann.result.rows.as_ref().map_or(0, Vec::len);
        let rows = exact.result.rows.expect("rank rows");
        count += rows.len();
        if let RankBy::Vector { vector, .. } = rank {
            truth.push(json!({"query_vector":vector,"nearest_neighbors":rows}));
        }
    }
    let avg = if ranks.is_empty() {
        0.0
    } else {
        count as f64 / ranks.len() as f64
    };
    let avg_recall = if ranks.is_empty() {
        1.0
    } else {
        recall_sum / ranks.len() as f64
    };
    let avg_ann = if ranks.is_empty() {
        0.0
    } else {
        ann_count as f64 / ranks.len() as f64
    };
    let mut response =
        json!({"avg_recall":avg_recall,"avg_exhaustive_count":avg,"avg_ann_count":avg_ann});
    if request.get("include_ground_truth") == Some(&json!(true)) {
        response
            .as_object_mut()
            .expect("object")
            .insert("ground_truth".into(), json!(truth));
    }
    Ok(response)
}
