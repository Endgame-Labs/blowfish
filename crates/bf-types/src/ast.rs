use crate::{ApiError, decode_vector};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Every v2 comparison and token operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOp {
    Eq,
    NotEq,
    In,
    NotIn,
    Lt,
    Lte,
    Gt,
    Gte,
    AnyLt,
    AnyLte,
    AnyGt,
    AnyGte,
    Contains,
    NotContains,
    ContainsAny,
    NotContainsAny,
    Glob,
    NotGlob,
    IGlob,
    NotIGlob,
    Regex,
    Fuzzy,
    ContainsAllTokens,
    ContainsAnyToken,
    ContainsTokenSequence,
}
/// A parsed filter. Operands retain `$ref_new` expressions for conditional writes.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    Compare {
        attribute: String,
        op: FilterOp,
        value: Value,
        params: Option<Value>,
    },
}
impl Filter {
    /// Parse the JSON tuple grammar, rejecting unknown operators with 422.
    pub fn parse(v: Value) -> Result<Self, ApiError> {
        let a = v
            .as_array()
            .ok_or_else(|| ApiError::malformed("filter must be a JSON array"))?;
        let name = a
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::malformed("invalid filter attribute or operator"))?;
        if matches!(name, "And" | "Or") && a.len() == 2 {
            let fs = a[1]
                .as_array()
                .ok_or_else(|| ApiError::malformed("logical filter requires an array of filters"))?
                .iter()
                .cloned()
                .map(Self::parse)
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(if name == "And" {
                Self::And(fs)
            } else {
                Self::Or(fs)
            });
        }
        if name == "Not" && a.len() == 2 {
            return Ok(Self::Not(Box::new(Self::parse(a[1].clone())?)));
        }
        if !(3..=4).contains(&a.len()) {
            return Err(ApiError::malformed(
                "filter requires attribute, operator, and operand",
            ));
        }
        let op: FilterOp = serde_json::from_value(a[1].clone())
            .map_err(|_| ApiError::malformed("unknown filter operator"))?;
        if a.len() == 4 && !a[3].is_object() {
            return Err(ApiError::malformed("filter parameters must be an object"));
        }
        if matches!(
            op,
            FilterOp::In | FilterOp::NotIn | FilterOp::ContainsAny | FilterOp::NotContainsAny
        ) && !a[2].is_array()
            && a[2].get("$ref_new").is_none()
        {
            return Err(ApiError::malformed("set filter requires an array operand"));
        }
        Ok(Self::Compare {
            attribute: name.into(),
            op,
            value: a[2].clone(),
            params: a.get(3).cloned(),
        })
    }
    /// Convert back to the public tuple representation.
    pub fn value(&self) -> Value {
        match self {
            Self::And(fs) => json!(["And", fs.iter().map(Self::value).collect::<Vec<_>>()]),
            Self::Or(fs) => json!(["Or", fs.iter().map(Self::value).collect::<Vec<_>>()]),
            Self::Not(f) => json!(["Not", f.value()]),
            Self::Compare {
                attribute,
                op,
                value,
                params,
            } => {
                let mut a = vec![json!(attribute), json!(op), value.clone()];
                if let Some(p) = params {
                    a.push(p.clone());
                }
                Value::Array(a)
            }
        }
    }
}
/// Sorting direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Asc,
    Desc,
}
/// One key in a lexicographic attribute ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderKey {
    pub attribute: String,
    pub direction: Direction,
}
/// The exhaustive ranking expression AST.
#[derive(Debug, Clone, PartialEq)]
pub enum RankBy {
    /// A literal numeric score, including the zero floor in `Max` expressions.
    Constant(serde_json::Number),
    Vector {
        attribute: String,
        exact: bool,
        vector: Vec<f32>,
        wire_vector: Value,
    },
    /// Positive sparse-vector dot-product score, composable in score expressions.
    SparseVector {
        attribute: String,
        vector: std::collections::BTreeMap<String, f32>,
        wire_vector: Value,
    },
    Bm25 {
        attribute: String,
        query: Value,
        params: Option<Value>,
    },
    Sum(Vec<RankBy>),
    Max(Vec<RankBy>),
    Product {
        weight: f64,
        expr: Box<RankBy>,
        weight_first: bool,
    },
    Filter(Filter),
    Attribute(String),
    Saturate {
        expr: Box<RankBy>,
        params: Value,
    },
    Decay {
        expr: Box<RankBy>,
        params: Value,
    },
    Dist {
        expr: Box<RankBy>,
        origin: Value,
    },
    Order(Vec<OrderKey>),
}
impl RankBy {
    /// Parse all M1 rank tuples, including either Product operand order.
    pub fn parse(v: Value) -> Result<Self, ApiError> {
        if let Value::Number(n) = v {
            return Ok(Self::Constant(n));
        }
        let a = v
            .as_array()
            .ok_or_else(|| ApiError::malformed("rank_by must be an array"))?;
        if a.first().is_some_and(Value::is_array) {
            if a.is_empty() || a.len() > 8 {
                return Err(ApiError::bad("order-by accepts one to eight keys"));
            }
            let mut keys = Vec::new();
            for key in a {
                match Self::parse(key.clone())? {
                    Self::Order(mut k) if k.len() == 1 => keys.append(&mut k),
                    _ => return Err(ApiError::malformed("invalid multi-key order-by")),
                }
            }
            return Ok(Self::Order(keys));
        }
        let name = a
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::malformed("empty rank_by"))?;
        let need = |n| {
            if a.len() == n {
                Ok(())
            } else {
                Err(ApiError::malformed("invalid rank_by tuple length"))
            }
        };
        match name {
            "Sum" | "Max" => {
                need(2)?;
                let terms = a[1]
                    .as_array()
                    .ok_or_else(|| ApiError::malformed("score terms must be an array"))?
                    .iter()
                    .cloned()
                    .map(Self::parse)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(if name == "Sum" {
                    Self::Sum(terms)
                } else {
                    Self::Max(terms)
                })
            }
            "Product" => {
                need(3)?;
                let (weight, expr, weight_first) = if let Some(w) = a[1].as_f64() {
                    (w, &a[2], true)
                } else {
                    (
                        a[2].as_f64().ok_or_else(|| {
                            ApiError::malformed("Product requires a numeric weight")
                        })?,
                        &a[1],
                        false,
                    )
                };
                if weight < 0.0 {
                    return Err(ApiError::bad("Product weight must be non-negative"));
                }
                Ok(Self::Product {
                    weight,
                    expr: Box::new(Self::parse(expr.clone())?),
                    weight_first,
                })
            }
            "Attribute" => {
                need(2)?;
                Ok(Self::Attribute(
                    a[1].as_str()
                        .ok_or_else(|| ApiError::malformed("Attribute requires a name"))?
                        .into(),
                ))
            }
            "Saturate" | "Decay" => {
                need(3)?;
                if !a[2].is_object() {
                    return Err(ApiError::malformed("score parameters must be an object"));
                }
                let expr = Box::new(Self::parse(a[1].clone())?);
                Ok(if name == "Saturate" {
                    Self::Saturate {
                        expr,
                        params: a[2].clone(),
                    }
                } else {
                    Self::Decay {
                        expr,
                        params: a[2].clone(),
                    }
                })
            }
            "Dist" => {
                need(3)?;
                Ok(Self::Dist {
                    expr: Box::new(Self::parse(a[1].clone())?),
                    origin: a[2].clone(),
                })
            }
            _ => match a.get(1).and_then(Value::as_str) {
                Some("asc" | "desc") => {
                    need(2)?;
                    Ok(Self::Order(vec![OrderKey {
                        attribute: name.into(),
                        direction: serde_json::from_value(a[1].clone()).expect("known direction"),
                    }]))
                }
                Some("ANN" | "kNN") => {
                    need(3)?;
                    Ok(Self::Vector {
                        attribute: name.into(),
                        exact: a[1] == "kNN",
                        vector: decode_vector(&a[2])?,
                        wire_vector: a[2].clone(),
                    })
                }
                Some("BM25") => {
                    if !(3..=4).contains(&a.len())
                        || !(a[2].is_string()
                            || a[2]
                                .as_array()
                                .is_some_and(|a| a.iter().all(Value::is_string)))
                    {
                        return Err(ApiError::malformed("invalid BM25 tuple"));
                    }
                    Ok(Self::Bm25 {
                        attribute: name.into(),
                        query: a[2].clone(),
                        params: a.get(3).cloned(),
                    })
                }
                Some("SparseKNN") => {
                    need(3)?;
                    Ok(Self::SparseVector {
                        attribute: name.into(),
                        vector: crate::decode_sparse(&a[2])?,
                        wire_vector: a[2].clone(),
                    })
                }
                _ => Ok(Self::Filter(Filter::parse(v)?)),
            },
        }
    }
    /// Serialize the AST as API tuples.
    pub fn value(&self) -> Value {
        match self {
            Self::Constant(n) => Value::Number(n.clone()),
            Self::Vector {
                attribute,
                exact,
                wire_vector,
                ..
            } => json!([attribute, if *exact { "kNN" } else { "ANN" }, wire_vector]),
            Self::SparseVector {
                attribute,
                wire_vector,
                ..
            } => json!([attribute, "SparseKNN", wire_vector]),
            Self::Bm25 {
                attribute,
                query,
                params,
            } => {
                let mut a = vec![json!(attribute), json!("BM25"), query.clone()];
                if let Some(p) = params {
                    a.push(p.clone());
                }
                Value::Array(a)
            }
            Self::Sum(xs) => json!(["Sum", xs.iter().map(Self::value).collect::<Vec<_>>()]),
            Self::Max(xs) => json!(["Max", xs.iter().map(Self::value).collect::<Vec<_>>()]),
            Self::Product {
                weight,
                expr,
                weight_first,
            } => {
                if *weight_first {
                    json!(["Product", weight, expr.value()])
                } else {
                    json!(["Product", expr.value(), weight])
                }
            }
            Self::Filter(f) => f.value(),
            Self::Attribute(s) => json!(["Attribute", s]),
            Self::Saturate { expr, params } => json!(["Saturate", expr.value(), params]),
            Self::Decay { expr, params } => json!(["Decay", expr.value(), params]),
            Self::Dist { expr, origin } => json!(["Dist", expr.value(), origin]),
            Self::Order(keys) => {
                let a: Vec<_> = keys
                    .iter()
                    .map(|k| json!([k.attribute, k.direction]))
                    .collect();
                if a.len() == 1 { a[0].clone() } else { json!(a) }
            }
        }
    }
}
macro_rules! tuple_serde {
    ($t:ty) => {
        impl Serialize for $t {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.value().serialize(s)
            }
        }
        impl<'de> Deserialize<'de> for $t {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                Self::parse(Value::deserialize(d)?).map_err(serde::de::Error::custom)
            }
        }
    };
}
tuple_serde!(Filter);
tuple_serde!(RankBy);

/// A named aggregation function.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct Aggregate {
    pub op: AggregateOp,
    pub attribute: Option<String>,
}
/// Exhaustive aggregation operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOp {
    Count,
    Sum,
    Min,
    Max,
}
impl TryFrom<Vec<String>> for Aggregate {
    type Error = ApiError;
    fn try_from(a: Vec<String>) -> Result<Self, Self::Error> {
        let op = match a.first().map(String::as_str) {
            Some("Count") => AggregateOp::Count,
            Some("Sum") => AggregateOp::Sum,
            Some("Min") => AggregateOp::Min,
            Some("Max") => AggregateOp::Max,
            _ => return Err(ApiError::malformed("unknown aggregation")),
        };
        if a.is_empty() || a.len() > 2 || (op != AggregateOp::Count && a.len() != 2) {
            return Err(ApiError::malformed("invalid aggregation tuple"));
        }
        Ok(Self {
            op,
            attribute: a.get(1).cloned(),
        })
    }
}
impl From<Aggregate> for Vec<String> {
    fn from(a: Aggregate) -> Self {
        let mut v = vec![
            match a.op {
                AggregateOp::Count => "Count",
                AggregateOp::Sum => "Sum",
                AggregateOp::Min => "Min",
                AggregateOp::Max => "Max",
            }
            .into(),
        ];
        v.extend(a.attribute);
        v
    }
}
/// A group key or an array-expanding ForEachUnique expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Value", into = "Value")]
pub enum GroupBy {
    Attribute(String),
    ForEachUnique { label: String, attribute: String },
}
impl TryFrom<Value> for GroupBy {
    type Error = ApiError;
    fn try_from(v: Value) -> Result<Self, Self::Error> {
        if let Some(s) = v.as_str() {
            return Ok(Self::Attribute(s.into()));
        }
        if let Some(o) = v.as_object()
            && o.len() == 1
        {
            let (label, v) = o.iter().next().expect("one element");
            if let Some(a) = v.as_array()
                && a.len() == 2
                && a[0] == "ForEachUnique"
                && let Some(attribute) = a[1].as_str()
            {
                return Ok(Self::ForEachUnique {
                    label: label.clone(),
                    attribute: attribute.into(),
                });
            }
        }
        Err(ApiError::malformed("invalid group_by key"))
    }
}
impl From<GroupBy> for Value {
    fn from(g: GroupBy) -> Self {
        match g {
            GroupBy::Attribute(s) => json!(s),
            GroupBy::ForEachUnique { label, attribute } => {
                json!({label:["ForEachUnique",attribute]})
            }
        }
    }
}
