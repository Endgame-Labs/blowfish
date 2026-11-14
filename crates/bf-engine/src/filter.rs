use crate::{text, write::normalize_value};
use bf_types::*;
use serde_json::Value;
use std::cmp::Ordering;

pub(crate) fn validate(f: &Filter, schema: &Schema, allow_refs: bool) -> Result<(), ApiError> {
    match f {
        Filter::And(fs) | Filter::Or(fs) => {
            for f in fs {
                validate(f, schema, allow_refs)?;
            }
        }
        Filter::Not(f) => validate(f, schema, allow_refs)?,
        Filter::Compare {
            attribute,
            op,
            value,
            params,
        } => {
            use FilterOp::*;
            let cfg = schema
                .get(attribute)
                .ok_or_else(|| ApiError::bad(format!("unknown attribute '{attribute}'")))?;
            let t = cfg.data_type.as_ref().expect("normalized schema type");
            if *op == Fuzzy {
                if cfg.fuzzy != Some(true) {
                    return Err(ApiError::bad("fuzzy is not enabled for this attribute"));
                }
                if !value.is_string() {
                    return Err(ApiError::bad("Fuzzy requires a string"));
                }
                fuzzy_params(params.as_ref())?;
            } else if matches!(
                op,
                ContainsAllTokens | ContainsAnyToken | ContainsTokenSequence
            ) {
                if cfg.text_config().is_none() {
                    return Err(ApiError::bad(format!(
                        "full_text_search is not enabled for '{attribute}'"
                    )));
                }
                if !value.is_string()
                    && !value
                        .as_array()
                        .is_some_and(|a| a.iter().all(Value::is_string))
                {
                    return Err(ApiError::bad(
                        "token query must be a string or string array",
                    ));
                }
            } else if *op == Regex {
                if cfg.regex != Some(true) {
                    return Err(ApiError::bad("regex is not enabled for this attribute"));
                }
                regex::Regex::new(
                    value
                        .as_str()
                        .ok_or_else(|| ApiError::bad("Regex requires a string"))?,
                )
                .map_err(|_| ApiError::bad("invalid regular expression"))?;
            } else if matches!(op, Glob | NotGlob | IGlob | NotIGlob) {
                if !matches!(
                    t,
                    AttributeType::Scalar(ScalarType::String)
                        | AttributeType::Array(ScalarType::String)
                ) {
                    return Err(ApiError::bad("Glob requires string or []string attributes"));
                }

                if cfg.glob != Some(true) && cfg.filterable != Some(true) {
                    return Err(ApiError::bad("glob is not enabled for this attribute"));
                }
                globset::GlobBuilder::new(
                    value
                        .as_str()
                        .ok_or_else(|| ApiError::bad("Glob requires a string"))?,
                )
                .case_insensitive(matches!(op, IGlob | NotIGlob))
                .build()
                .map_err(|_| ApiError::bad("invalid glob pattern"))?;
            } else {
                if attribute != "id" && cfg.filterable != Some(true) {
                    return Err(ApiError::bad(format!(
                        "attribute '{attribute}' is not filterable"
                    )));
                }
                if value.get("$ref_new").is_some() {
                    if !allow_refs
                        || value.as_object().is_none_or(|o| o.len() != 1)
                        || !value["$ref_new"].is_string()
                    {
                        return Err(ApiError::bad(
                            "$ref_new is only supported in write conditions",
                        ));
                    }
                    return Ok(());
                }
                let operand_type = if matches!(
                    op,
                    Contains
                        | NotContains
                        | ContainsAny
                        | NotContainsAny
                        | AnyLt
                        | AnyLte
                        | AnyGt
                        | AnyGte
                ) {
                    match t {
                        AttributeType::Array(s) => AttributeType::Scalar(*s),
                        _ => {
                            return Err(ApiError::bad(
                                "array operator requires an array attribute",
                            ));
                        }
                    }
                } else {
                    t.clone()
                };
                if matches!(op, In | NotIn | ContainsAny | NotContainsAny) {
                    for v in value
                        .as_array()
                        .ok_or_else(|| ApiError::bad("set operand must be an array"))?
                    {
                        normalize_value(v, &operand_type)?;
                    }
                } else {
                    normalize_value(value, &operand_type)?;
                }
            }
            if *op == ContainsTokenSequence && text::prefix(params.as_ref()) {
                return Err(ApiError::bad(
                    "ContainsTokenSequence does not support last_as_prefix",
                ));
            }
            if let Some(params) = params
                && params
                    .get("last_as_prefix")
                    .is_some_and(|v| !v.is_boolean())
            {
                return Err(ApiError::bad("last_as_prefix must be a boolean"));
            }
        }
    }
    Ok(())
}
/// Compare JSON scalars with exact integer ordering and null as the least value.
pub(crate) fn compare(a: &Value, b: &Value) -> Ordering {
    if a.is_null() {
        return if b.is_null() {
            Ordering::Equal
        } else {
            Ordering::Less
        };
    }
    if b.is_null() {
        return Ordering::Greater;
    }
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                return a.cmp(&b);
            }
            if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
                return a.cmp(&b);
            }
            if a.is_i64() && b.is_u64() {
                return Ordering::Less;
            }
            if a.is_u64() && b.is_i64() {
                return Ordering::Greater;
            }
            a.as_f64()
                .expect("number")
                .total_cmp(&b.as_f64().expect("number"))
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Array(a), Value::Array(b)) => {
            for (a, b) in a.iter().zip(b) {
                let c = compare(a, b);
                if c != Ordering::Equal {
                    return c;
                }
            }
            a.len().cmp(&b.len())
        }
        _ => a.to_string().cmp(&b.to_string()),
    }
}
fn equal(a: &Value, b: &Value) -> bool {
    compare(a, b) == Ordering::Equal
}
fn canonical(value: &Value, t: Option<&AttributeType>) -> Result<Value, ApiError> {
    if let Some(t) = t {
        normalize_value(value, t)
    } else {
        Ok(value.clone())
    }
}

pub(crate) fn matches(
    f: &Filter,
    doc: Option<&Document>,
    new: Option<&Document>,
    schema: &Schema,
) -> Result<bool, ApiError> {
    match f {
        Filter::And(fs) => {
            for f in fs {
                if !matches(f, doc, new, schema)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Filter::Or(fs) => {
            for f in fs {
                if matches(f, doc, new, schema)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Filter::Not(f) => Ok(!matches(f, doc, new, schema)?),
        Filter::Compare {
            attribute,
            op,
            value,
            params,
        } => {
            use FilterOp::*;
            let actual = doc.and_then(|d| d.get(attribute)).unwrap_or(&Value::Null);
            let operand = if let Some(name) = value.get("$ref_new").and_then(Value::as_str) {
                new.and_then(|d| d.get(name)).unwrap_or(&Value::Null)
            } else {
                value
            };
            let cfg = schema.get(attribute);
            let t = cfg.and_then(|s| s.data_type.as_ref());
            let inner = t.and_then(|t| match t {
                AttributeType::Array(s) => Some(AttributeType::Scalar(*s)),
                _ => None,
            });
            let operand = if matches!(op, Eq | NotEq | Lt | Lte | Gt | Gte) {
                canonical(operand, t)?
            } else if matches!(op, Contains | NotContains | AnyLt | AnyLte | AnyGt | AnyGte) {
                canonical(operand, inner.as_ref())?
            } else {
                operand.clone()
            };
            let scalar = |a: &Value, b: &Value, op: FilterOp| {
                let c = compare(a, b);
                match op {
                    Eq => c == Ordering::Equal,
                    NotEq => c != Ordering::Equal,
                    Lt | AnyLt => c == Ordering::Less,
                    Lte | AnyLte => c != Ordering::Greater,
                    Gt | AnyGt => c == Ordering::Greater,
                    Gte | AnyGte => c != Ordering::Less,
                    _ => false,
                }
            };
            Ok(match op {
                Eq | NotEq | Lt | Lte | Gt | Gte => scalar(actual, &operand, *op),
                In | NotIn => {
                    let mut found = false;
                    for item in operand
                        .as_array()
                        .ok_or_else(|| ApiError::bad("In requires an array"))?
                    {
                        if equal(actual, &canonical(item, t)?) {
                            found = true;
                            break;
                        }
                    }
                    if *op == NotIn { !found } else { found }
                }
                Contains | NotContains => {
                    let found = actual
                        .as_array()
                        .is_some_and(|a| a.iter().any(|v| equal(v, &operand)));
                    if *op == NotContains { !found } else { found }
                }
                ContainsAny | NotContainsAny => {
                    let mut found = false;
                    if let Some(actual) = actual.as_array() {
                        for item in operand
                            .as_array()
                            .ok_or_else(|| ApiError::bad("ContainsAny requires an array"))?
                        {
                            let item = canonical(item, inner.as_ref())?;
                            if actual.iter().any(|v| equal(v, &item)) {
                                found = true;
                                break;
                            }
                        }
                    }
                    if *op == NotContainsAny { !found } else { found }
                }
                AnyLt | AnyLte | AnyGt | AnyGte => actual
                    .as_array()
                    .is_some_and(|a| a.iter().any(|v| scalar(v, &operand, *op))),
                Glob | NotGlob | IGlob | NotIGlob => {
                    let glob = globset::GlobBuilder::new(
                        operand
                            .as_str()
                            .ok_or_else(|| ApiError::bad("Glob requires a string"))?,
                    )
                    .case_insensitive(matches!(op, IGlob | NotIGlob))
                    .build()
                    .map_err(|_| ApiError::bad("invalid glob pattern"))?
                    .compile_matcher();
                    let found = strings(actual).iter().any(|s| glob.is_match(s));
                    if matches!(op, NotGlob | NotIGlob) {
                        !found
                    } else {
                        found
                    }
                }
                Regex => {
                    let regex = regex::Regex::new(
                        operand
                            .as_str()
                            .ok_or_else(|| ApiError::bad("Regex requires a string"))?,
                    )
                    .map_err(|_| ApiError::bad("invalid regex"))?;
                    strings(actual).iter().any(|s| regex.is_match(s))
                }
                ContainsAllTokens | ContainsAnyToken | ContainsTokenSequence => {
                    let config = cfg
                        .and_then(AttributeSchema::text_config)
                        .unwrap_or_default();
                    let query = bf_text::Query::new(
                        text::tokens(&operand, &config),
                        text::prefix(params.as_ref()),
                    );
                    let tokens = text::tokens(actual, &config);
                    match op {
                        ContainsAllTokens => {
                            bf_text::matches_all_tokens(&tokens, &query, config.tokenizer)
                        }
                        ContainsAnyToken => {
                            bf_text::matches_any_token(&tokens, &query, config.tokenizer)
                        }
                        ContainsTokenSequence => bf_text::matches_token_sequence(&tokens, &query),
                        _ => unreachable!(),
                    }
                }
                Fuzzy => {
                    let config = fuzzy_params(params.as_ref())?;
                    strings(actual).iter().any(|s| {
                        config.matches(s, operand.as_str().expect("validated fuzzy query"))
                    })
                }
            })
        }
    }
}
fn strings(value: &Value) -> Vec<&str> {
    if let Some(s) = value.as_str() {
        vec![s]
    } else if let Some(a) = value.as_array() {
        a.iter().filter_map(Value::as_str).collect()
    } else {
        vec![]
    }
}

pub(crate) fn fuzzy_params(params: Option<&Value>) -> Result<bf_text::FuzzyParams, ApiError> {
    let params =
        params.ok_or_else(|| ApiError::bad("Fuzzy requires max_edit_distance parameters"))?;
    let config: bf_text::FuzzyParams = serde_json::from_value(params.clone())
        .map_err(|e| ApiError::bad(format!("invalid Fuzzy parameters: {e}")))?;
    config.validate().map_err(ApiError::bad)?;
    Ok(config)
}
