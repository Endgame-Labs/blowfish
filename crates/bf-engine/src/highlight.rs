//! Computed projection and independently ranked text fragments.
use crate::{NamespaceState, filter, query, text};
use bf_types::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, ops::Range};

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Params {
    fragment_by: bf_text::FragmentBy,
    rank_fragments_by: Option<RankBy>,
    fragment_limit: usize,
    include_offsets: Option<String>,
}
impl Default for Params {
    fn default() -> Self {
        Self {
            fragment_by: Default::default(),
            rank_fragments_by: None,
            fragment_limit: 3,
            include_offsets: None,
        }
    }
}
fn parameters(expr: &Value) -> Result<(&str, Params), ApiError> {
    let a = expr
        .as_array()
        .ok_or_else(|| ApiError::bad("computed expression must be an array"))?;
    if !(2..=3).contains(&a.len()) {
        return Err(ApiError::bad(
            "Highlight requires an attribute and optional parameters",
        ));
    }
    let name = a[1]
        .as_str()
        .ok_or_else(|| ApiError::bad("Highlight requires an attribute name"))?;
    let params: Params = serde_json::from_value(a.get(2).cloned().unwrap_or(json!({})))
        .map_err(|e| ApiError::bad(format!("invalid Highlight parameters: {e}")))?;
    if !(1..=1024).contains(&params.fragment_limit) {
        return Err(ApiError::bad("fragment_limit must be between 1 and 1024"));
    }
    if params
        .include_offsets
        .as_deref()
        .is_some_and(|u| !matches!(u, "utf-8" | "utf-16" | "codepoints"))
    {
        return Err(ApiError::bad(
            "include_offsets must be utf-8, utf-16, or codepoints",
        ));
    }
    Ok((name, params))
}
fn is_highlight(v: &Value) -> bool {
    v.get(0).is_some_and(|v| v == "Highlight")
}
fn expression(v: &Value) -> Result<RankBy, ApiError> {
    if v.get(1).is_some_and(|v| v == "VectorDist") {
        let mut v = v.clone();
        v[1] = json!("ANN");
        RankBy::parse(v)
    } else {
        RankBy::parse(v.clone())
    }
}
fn inferred(rank: &RankBy, name: &str) -> Option<RankBy> {
    match rank {
        RankBy::Bm25 { attribute, .. } if attribute == name => Some(rank.clone()),
        RankBy::Filter(Filter::Compare {
            attribute,
            op: FilterOp::Fuzzy,
            ..
        }) if attribute == name => Some(rank.clone()),
        RankBy::Sum(xs) | RankBy::Max(xs) => {
            let xs: Vec<_> = xs.iter().filter_map(|x| inferred(x, name)).collect();
            if xs.is_empty() {
                None
            } else if matches!(rank, RankBy::Sum(_)) {
                Some(RankBy::Sum(xs))
            } else {
                Some(RankBy::Max(xs))
            }
        }
        RankBy::Product {
            weight,
            expr,
            weight_first,
        } => inferred(expr, name).map(|expr| RankBy::Product {
            weight: *weight,
            expr: Box::new(expr),
            weight_first: *weight_first,
        }),
        RankBy::Saturate { expr, params } => inferred(expr, name).map(|expr| RankBy::Saturate {
            expr: Box::new(expr),
            params: params.clone(),
        }),
        _ => None,
    }
}
fn fragment_rank(q: &QueryRequest, name: &str, params: &Params) -> Result<RankBy, ApiError> {
    params.rank_fragments_by.clone().or_else(||q.rank_by.as_ref().and_then(|rank|inferred(rank,name)))
        .ok_or_else(||ApiError::bad(format!("cannot infer rank_fragments_by: query rank_by has no supported highlighting clause over '{name}'; provide rank_fragments_by explicitly")))
}
fn fragment_state(state: &NamespaceState, name: &str) -> Result<NamespaceState, ApiError> {
    let cfg = state
        .schema
        .get(name)
        .ok_or_else(|| ApiError::bad(format!("unknown highlight attribute '{name}'")))?;
    if !matches!(
        cfg.data_type,
        Some(AttributeType::Scalar(ScalarType::String) | AttributeType::Array(ScalarType::String))
    ) {
        return Err(ApiError::bad("Highlight requires string or []string"));
    }
    if cfg
        .text_config()
        .is_some_and(|c| c.tokenizer == bf_text::Tokenizer::PreTokenizedArray)
    {
        return Err(ApiError::bad(
            "Highlight does not support pre_tokenized_array",
        ));
    }
    let mut out = state.clone();
    let mut cfg = cfg.clone();
    cfg.data_type = Some(AttributeType::Scalar(ScalarType::String));
    out.schema.insert("$fragment".into(), cfg.clone());
    out.schema.insert(name.into(), cfg);
    out.docs.clear();
    out.scores.clear();
    out.corpus.clear();
    Ok(out)
}
pub(crate) fn validate(state: &NamespaceState, q: &QueryRequest) -> Result<(), ApiError> {
    let Some(exprs) = &q.compute_attributes else {
        return Ok(());
    };
    if exprs.len() > 256 {
        return Err(ApiError::bad(
            "compute_attributes accepts at most 256 expressions",
        ));
    }
    if q.aggregate_by.is_some() {
        return Err(ApiError::bad(
            "compute_attributes cannot be combined with aggregations",
        ));
    }
    for (label, v) in exprs {
        if label.starts_with('$') || label.len() > 128 || label.is_empty() {
            return Err(ApiError::bad("invalid computed attribute name"));
        }
        if is_highlight(v) {
            let (name, params) = parameters(v)?;
            let fragments = fragment_state(state, name)?;
            let rank = fragment_rank(q, name, &params)?;
            query::validate_rank(&fragments, &rank, false)?;
            if inferred(&rank, name)
                .or_else(|| inferred(&rank, "$fragment"))
                .is_none()
            {
                return Err(ApiError::bad(
                    "rank_fragments_by requires BM25 or Fuzzy over the highlighted attribute or $fragment",
                ));
            }
        } else {
            let rank = expression(v)?;
            if matches!(rank, RankBy::Order(_)) {
                return Err(ApiError::bad("computed values require a score expression"));
            }
            query::validate_rank(state, &rank, false)?;
        }
    }
    Ok(())
}
fn matches(
    rank: &RankBy,
    fragment: &str,
    name: &str,
    state: &NamespaceState,
    out: &mut Vec<Range<usize>>,
) -> Result<(), ApiError> {
    match rank {
        RankBy::Bm25 {
            attribute,
            query,
            params,
        } if attribute == name || attribute == "$fragment" => {
            let cfg = state.schema[attribute]
                .text_config()
                .expect("validated text config");
            let terms = text::tokens(query, &cfg);
            let prefix = text::prefix(params.as_ref());
            for (term, range) in bf_text::tokenize_spans(fragment, &cfg) {
                if terms.iter().enumerate().any(|(i, t)| {
                    term == t.text || (prefix && i + 1 == terms.len() && term.starts_with(&t.text))
                }) {
                    out.push(range)
                }
            }
        }
        RankBy::Filter(Filter::Compare {
            attribute,
            op: FilterOp::Fuzzy,
            value,
            params,
        }) if attribute == name || attribute == "$fragment" => {
            let cfg = filter::fuzzy_params(params.as_ref())?;
            if let Some(distance) = cfg.distance(value.as_str().expect("validated string")) {
                out.extend(bf_text::substring_matches(
                    fragment,
                    value.as_str().unwrap(),
                    distance,
                    cfg.case_sensitive,
                ));
            }
        }
        RankBy::Sum(xs) | RankBy::Max(xs) => {
            for x in xs {
                matches(x, fragment, name, state, out)?
            }
        }
        RankBy::Product { expr, .. }
        | RankBy::Saturate { expr, .. }
        | RankBy::Decay { expr, .. } => matches(expr, fragment, name, state, out)?,
        _ => {}
    }
    Ok(())
}
fn highlights(
    state: &NamespaceState,
    doc: &Document,
    q: &QueryRequest,
    expr: &Value,
) -> Result<Value, ApiError> {
    let (name, params) = parameters(expr)?;
    let rank = fragment_rank(q, name, &params)?;
    let mut fragments = fragment_state(state, name)?;
    let value = doc.get(name).unwrap_or(&Value::Null);
    let array = value.is_array();
    let strings: Vec<_> = if let Some(s) = value.as_str() {
        vec![s]
    } else {
        value
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect()
    };
    let mut candidates = vec![];
    for (element, s) in strings.iter().enumerate() {
        for range in bf_text::fragment_ranges(s, params.fragment_by) {
            let fragment = &s[range.clone()];
            let mut spans = vec![];
            matches(&rank, fragment, name, &fragments, &mut spans)?;
            let mut fragment_doc = doc.clone();
            fragment_doc.insert(name.into(), json!(fragment));
            fragment_doc.insert("$fragment".into(), json!(fragment));
            let id = Id::Uint(fragments.docs.len() as u64);
            fragment_doc.insert("id".into(), id.value());
            fragments.docs.insert(id, fragment_doc.clone());
            candidates.push((element, range, spans, fragment_doc));
        }
    }
    let mut ranked = vec![];
    for (element, range, mut spans, fragment_doc) in candidates {
        if spans.is_empty() {
            continue;
        }
        spans.sort_by_key(|r| (r.start, r.end));
        spans.dedup();
        let score = query::score(&fragments, &fragment_doc, &rank)?;
        let mut row =
            BTreeMap::from([("text".to_owned(), json!(&strings[element][range.clone()]))]);
        if let Some(unit) = &params.include_offsets {
            let s = strings[element];
            row.insert(
                "fragment_range".into(),
                json!([
                    bf_text::offset(s, range.start, unit),
                    bf_text::offset(s, range.end, unit)
                ]),
            );
            row.insert(
                "match_ranges".into(),
                json!(
                    spans
                        .iter()
                        .map(|r| [
                            bf_text::offset(s, range.start + r.start, unit),
                            bf_text::offset(s, range.start + r.end, unit)
                        ])
                        .collect::<Vec<_>>()
                ),
            );
        }
        if array {
            row.insert("array_index".into(), json!(element));
        }
        ranked.push((score, element, range.start, row));
    }
    ranked.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    Ok(json!(
        ranked
            .into_iter()
            .take(params.fragment_limit)
            .map(|(_, _, _, row)| row)
            .collect::<Vec<_>>()
    ))
}
pub(crate) fn project(
    state: &NamespaceState,
    doc: &Document,
    q: &QueryRequest,
    row: &mut Document,
) -> Result<(), ApiError> {
    for (name, expr) in q.compute_attributes.iter().flatten() {
        let value = if is_highlight(expr) {
            highlights(state, doc, q, expr)?
        } else {
            let rank = expression(expr)?;
            if let RankBy::Vector {
                attribute, vector, ..
            } = &rank
            {
                doc.get(attribute)
                    .map(|value| {
                        decode_vector(value)
                            .map(|v| json!(query::distance(&v, vector, state.metric)))
                    })
                    .transpose()?
                    .unwrap_or(Value::Null)
            } else {
                json!(query::score(state, doc, &rank)?)
            }
        };
        row.insert(name.clone(), value);
    }
    Ok(())
}
