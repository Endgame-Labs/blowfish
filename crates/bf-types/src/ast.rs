use crate::{ApiError, decode_vector};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// Validate before the recursive parser clones any subtree. This also covers callers
// deserializing from a Value, where serde_json's text recursion limit does not apply.
fn check_expression(value: &Value) -> Result<(), ApiError> {
    let mut pending = vec![(value, 0usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if depth > 64 || nodes > 65536 {
            return Err(ApiError::malformed("expression exceeds complexity limits"));
        }
        match value {
            Value::Array(values) => {
                if pending.len() + values.len() > 65536 {
                    return Err(ApiError::malformed("expression exceeds complexity limits"));
                }
                pending.extend(values.iter().map(|v| (v, depth + 1)));
            }
            Value::Object(values) => {
                if pending.len() + values.len() > 65536 {
                    return Err(ApiError::malformed("expression exceeds complexity limits"));
                }
                pending.extend(values.values().map(|v| (v, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(())
}

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
        check_expression(&v)?;
        Self::parse_inner(v)
    }
    fn parse_inner(v: Value) -> Result<Self, ApiError> {
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
                .map(Self::parse_inner)
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(if name == "And" {
                Self::And(fs)
            } else {
                Self::Or(fs)
            });
        }
        if name == "Not" && a.len() == 2 {
            return Ok(Self::Not(Box::new(Self::parse_inner(a[1].clone())?)));
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
        /// The vector as sent, for [`RankBy::value`]. `Null` when it was a plain number array:
        /// the request parser keeps only `vector` then, and `value` writes it back as numbers.
        wire_vector: Value,
    },
    /// Hosted embedding syntax is preserved so execution can return a clear 400.
    UnsupportedEmbed {
        attribute: String,
        exact: bool,
        expression: Value,
    },
    /// Sum of the minimum document-vector distances for each query vector.
    MultiVector {
        attribute: String,
        exact: bool,
        vectors: Vec<Vec<f32>>,
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
        check_expression(&v)?;
        Self::parse_inner(v)
    }
    fn parse_inner(v: Value) -> Result<Self, ApiError> {
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
                match Self::parse_inner(key.clone())? {
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
                    .map(Self::parse_inner)
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
                    expr: Box::new(Self::parse_inner(expr.clone())?),
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
                let expr = Box::new(Self::parse_inner(a[1].clone())?);
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
                    expr: Box::new(Self::parse_inner(a[1].clone())?),
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
                    if a[2].get(0).is_some_and(|v| v == "Embed") {
                        return Ok(Self::UnsupportedEmbed {
                            attribute: name.into(),
                            exact: a[1] == "kNN",
                            expression: a[2].clone(),
                        });
                    }
                    if a[2]
                        .as_array()
                        .is_some_and(|xs| xs.first().is_some_and(Value::is_array))
                    {
                        let vectors = a[2]
                            .as_array()
                            .expect("array")
                            .iter()
                            .map(decode_vector)
                            .collect::<Result<Vec<_>, _>>()?;
                        return Ok(Self::MultiVector {
                            attribute: name.into(),
                            exact: a[1] == "kNN",
                            vectors,
                            wire_vector: a[2].clone(),
                        });
                    }
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
                vector,
                wire_vector,
            } => {
                let wire = if wire_vector.is_null() {
                    // Each f32's shortest decimal form, as the client most likely sent it
                    // (`0.1`, not the widened `0.10000000149011612`).
                    Value::Array(
                        vector
                            .iter()
                            .map(|x| {
                                serde_json::Number::from_f64(
                                    x.to_string().parse().expect("f32 prints as f64"),
                                )
                                .map_or(Value::Null, Value::Number)
                            })
                            .collect(),
                    )
                } else {
                    wire_vector.clone()
                };
                json!([attribute, if *exact { "kNN" } else { "ANN" }, wire])
            }
            Self::UnsupportedEmbed {
                attribute,
                exact,
                expression,
            } => json!([attribute, if *exact { "kNN" } else { "ANN" }, expression]),
            Self::MultiVector {
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

impl Serialize for RankBy {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.value().serialize(s)
    }
}
/// A request's `rank_by`. A dense vector query (`[attr, "ANN" | "kNN", [numbers...]]`, nearly
/// every query) is read straight into `f32`s, without a `serde_json::Value` per number; every
/// other form is read as a `Value` and goes through [`RankBy::parse`], so both give the same
/// results and errors.
impl<'de> Deserialize<'de> for RankBy {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(RankVisitor)
    }
}
struct RankVisitor;
impl RankVisitor {
    fn parse<E: serde::de::Error>(v: Value) -> Result<RankBy, E> {
        RankBy::parse(v).map_err(E::custom)
    }
}
impl<'de> serde::de::Visitor<'de> for RankVisitor {
    type Value = RankBy;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a rank_by expression")
    }
    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<RankBy, E> {
        Self::parse(Value::Bool(v))
    }
    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<RankBy, E> {
        Self::parse(Value::from(v))
    }
    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<RankBy, E> {
        Self::parse(Value::from(v))
    }
    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<RankBy, E> {
        Self::parse(Value::from(v))
    }
    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<RankBy, E> {
        Self::parse(Value::String(v.into()))
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<RankBy, E> {
        Self::parse(Value::Null)
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<RankBy, E> {
        Self::parse(Value::Null)
    }
    fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<RankBy, D::Error> {
        d.deserialize_any(self)
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<RankBy, A::Error> {
        Self::parse(Value::deserialize(
            serde::de::value::MapAccessDeserializer::new(map),
        )?)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<RankBy, A::Error> {
        let mut items: Vec<Value> = Vec::with_capacity(3);
        let rest = |mut items: Vec<Value>, mut seq: A| -> Result<RankBy, A::Error> {
            while let Some(v) = seq.next_element::<Value>()? {
                items.push(v);
            }
            Self::parse(Value::Array(items))
        };
        let Some(first) = seq.next_element::<Value>()? else {
            return rest(items, seq);
        };
        let named = first.is_string();
        items.push(first);
        if !named {
            return rest(items, seq);
        }
        let Some(second) = seq.next_element::<Value>()? else {
            return rest(items, seq);
        };
        let exact = second == "kNN";
        let vector = exact || second == "ANN";
        items.push(second);
        if !vector {
            return rest(items, seq);
        }
        let Some(arg) = seq.next_element::<VectorArg>()? else {
            return rest(items, seq);
        };
        let floats = match arg {
            VectorArg::Floats(floats) => floats,
            VectorArg::Other(v) => {
                items.push(v);
                return rest(items, seq);
            }
        };
        match seq.next_element::<Value>()? {
            // Within `check_expression`'s node limit, a plain number array parses exactly as
            // `RankBy::parse` would: the tuple has three elements, and `decode_vector`'s checks.
            None if floats.len() <= MAX_FAST_VECTOR => {
                let Value::String(attribute) = items.swap_remove(0) else {
                    unreachable!("checked above")
                };
                let vector = crate::check_vector(floats).map_err(serde::de::Error::custom)?;
                Ok(RankBy::Vector {
                    attribute,
                    exact,
                    vector,
                    wire_vector: Value::Null,
                })
            }
            extra => {
                items.push(json!(floats));
                items.extend(extra);
                rest(items, seq)
            }
        }
    }
}
/// The longest number array the fast path takes; a longer one goes through `check_expression`
/// (and fails its node limit there, as it always has).
const MAX_FAST_VECTOR: usize = 65_000;

/// The third element of a vector rank tuple: numbers read as `f32` (through `f64`, as
/// `decode_vector` reads them), or anything else as a `Value`.
enum VectorArg {
    Floats(Vec<f32>),
    Other(Value),
}
impl<'de> Deserialize<'de> for VectorArg {
    /// With `serde_json` reading JSON text, the argument's raw text goes to [`fastvec::parse`]; any
    /// other deserializer, and any text that is not a plain number array it can read exactly,
    /// takes [`VectorArg::generic`].
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Raw;
        impl<'de> serde::de::Visitor<'de> for Raw {
            type Value = VectorArg;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a vector")
            }
            // `serde_json`'s raw value: one entry holding the value's text.
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<VectorArg, A::Error> {
                map.next_key::<serde::de::IgnoredAny>()?;
                map.next_value_seed(RawText)
            }
            fn visit_newtype_struct<D2: serde::Deserializer<'de>>(
                self,
                d: D2,
            ) -> Result<VectorArg, D2::Error> {
                VectorArg::generic(d)
            }
        }
        struct RawText;
        impl<'de> serde::de::DeserializeSeed<'de> for RawText {
            type Value = VectorArg;
            fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<VectorArg, D::Error> {
                struct Text;
                impl serde::de::Visitor<'_> for Text {
                    type Value = VectorArg;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str("a vector")
                    }
                    fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<VectorArg, E> {
                        if let Some(floats) = crate::fastvec::parse(text) {
                            return Ok(VectorArg::Floats(floats));
                        }
                        VectorArg::generic(&mut serde_json::Deserializer::from_str(text))
                            .map_err(E::custom)
                    }
                }
                d.deserialize_str(Text)
            }
        }
        d.deserialize_newtype_struct("$serde_json::private::RawValue", Raw)
    }
}
impl VectorArg {
    /// Read any JSON value, numbers as `f32` through `f64`.
    fn generic<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = VectorArg;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a vector")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<VectorArg, A::Error> {
                let mut floats = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
                loop {
                    match seq.next_element::<Element>()? {
                        None => return Ok(VectorArg::Floats(floats)),
                        Some(Element::Number(x)) => floats.push(x as f32),
                        Some(Element::Other(v)) => {
                            // Not a plain number array (a multi-vector, an `Embed` call, or an
                            // error): keep it whole for `RankBy::parse`. Numbers before this
                            // element only matter to an error message, so `f32` is close enough.
                            let mut all: Vec<Value> = floats.iter().map(|x| json!(x)).collect();
                            all.push(v);
                            while let Some(v) = seq.next_element::<Value>()? {
                                all.push(v);
                            }
                            return Ok(VectorArg::Other(Value::Array(all)));
                        }
                    }
                }
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<VectorArg, E> {
                Ok(VectorArg::Other(Value::String(v.into())))
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<VectorArg, E> {
                Ok(VectorArg::Other(Value::Bool(v)))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<VectorArg, E> {
                Ok(VectorArg::Other(Value::from(v)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<VectorArg, E> {
                Ok(VectorArg::Other(Value::from(v)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<VectorArg, E> {
                Ok(VectorArg::Other(Value::from(v)))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<VectorArg, E> {
                Ok(VectorArg::Other(Value::Null))
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<VectorArg, E> {
                Ok(VectorArg::Other(Value::Null))
            }
            fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<VectorArg, D::Error> {
                d.deserialize_any(self)
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<VectorArg, A::Error> {
                Ok(VectorArg::Other(Value::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?))
            }
        }
        d.deserialize_any(V)
    }
}
/// One element of a vector array: a number, or anything else as a `Value`.
enum Element {
    Number(f64),
    Other(Value),
}
impl<'de> Deserialize<'de> for Element {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Element;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a vector element")
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Element, E> {
                Ok(Element::Number(v))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Element, E> {
                Ok(Element::Number(v as f64))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Element, E> {
                Ok(Element::Number(v as f64))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Element, E> {
                Ok(Element::Other(Value::String(v.into())))
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Element, E> {
                Ok(Element::Other(Value::Bool(v)))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Element, E> {
                Ok(Element::Other(Value::Null))
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Element, E> {
                Ok(Element::Other(Value::Null))
            }
            fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<Element, D::Error> {
                d.deserialize_any(self)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, seq: A) -> Result<Element, A::Error> {
                Ok(Element::Other(Value::deserialize(
                    serde::de::value::SeqAccessDeserializer::new(seq),
                )?))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Element, A::Error> {
                Ok(Element::Other(Value::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?))
            }
        }
        d.deserialize_any(V)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    /// The direct reader and `RankBy::parse` agree on every input, valid or not.
    #[test]
    fn rank_by_reader_matches_parse() {
        let long: Vec<f32> = (0..65_100).map(|i| i as f32).collect();
        let cases = [
            json!(["vector", "ANN", [0.1, 0.2, -3, 4e-3, 1e10]]),
            json!(["vector", "kNN", [1, 2, 3]]),
            json!(["vector", "ANN", "AACAPwAAAAA="]),
            json!(["vector", "ANN", []]),
            json!(["vector", "ANN", [1, "x"]]),
            json!(["vector", "ANN", [1, null]]),
            json!(["vector", "ANN", [1e300, 2]]),
            json!(["vector", "ANN", [[1, 2], [3, 4]]]),
            json!(["vector", "ANN", ["Embed", {"text": "hi"}]]),
            json!(["vector", "ANN", [1, 2], 3]),
            json!(["vector", "ANN"]),
            json!(["vector", "ANN", 5]),
            json!(["vector", "ANN", {"a": 1}]),
            json!(["vector", "ANN", long]),
            json!(["text", "BM25", "hello"]),
            json!(["Sum", [["text", "BM25", "a"], ["vector", "ANN", [1, 2]]]]),
            json!(["Product", 2, ["text", "BM25", "a"]]),
            json!([["a", "asc"], ["b", "desc"]]),
            json!(["a", "desc"]),
            json!([]),
            json!([1]),
            json!(1.5),
            json!("x"),
            json!({"a": 1}),
            json!(true),
        ];
        for case in cases {
            let text = case.to_string();
            let read = serde_json::from_str::<RankBy>(&text).map_err(|e| e.to_string());
            let parsed = RankBy::parse(case.clone()).map_err(|e| e.to_string());
            match (&read, &parsed) {
                (Ok(a), Ok(b)) => {
                    // The direct reader drops the wire copy of a number array.
                    let strip = |r: &RankBy| match r.clone() {
                        RankBy::Vector {
                            attribute,
                            exact,
                            vector,
                            wire_vector,
                        } => RankBy::Vector {
                            attribute,
                            exact,
                            vector,
                            wire_vector: if wire_vector.is_array() {
                                Value::Null
                            } else {
                                wire_vector
                            },
                        },
                        other => other,
                    };
                    assert_eq!(strip(a), strip(b), "{text:.200}");
                }
                (Err(a), Err(b)) => assert!(a.contains(b.as_str()), "{a} / {b}"),
                _ => panic!("{text:.200}: {read:?} against {parsed:?}"),
            }
        }
        // A number array is written back in its shortest form.
        let read: RankBy = serde_json::from_str(r#"["v","ANN",[0.1,0.25]]"#).unwrap();
        assert_eq!(read.value(), json!(["v", "ANN", [0.1, 0.25]]));
    }
}
