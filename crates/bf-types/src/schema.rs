use crate::{ApiError, decode_vector};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt, str::FromStr};

/// Supported scalar attribute kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    String,
    Int,
    Uint,
    Float,
    Bool,
    Uuid,
    Datetime,
    Bytes,
}
impl ScalarType {
    fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int => "int",
            Self::Uint => "uint",
            Self::Float => "float",
            Self::Bool => "bool",
            Self::Uuid => "uuid",
            Self::Datetime => "datetime",
            Self::Bytes => "bytes",
        }
    }
}
/// A schema type, including nullable scalar arrays and dense vectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeType {
    Scalar(ScalarType),
    Array(ScalarType),
    Vector {
        dimensions: usize,
        f16: bool,
    },
    /// Dense vector stored as signed eight-bit integers.
    VectorI8 {
        dimensions: usize,
    },
    /// Variable-length arrays of f32 vectors for late interaction.
    MultiVector {
        dimensions: usize,
    },
    /// Sparse string-keyed positive f16 weights.
    SparseVector,
}
impl AttributeType {
    /// Dense-vector dimensions, if this is a vector.
    pub fn dimensions(&self) -> Option<usize> {
        match self {
            Self::Vector { dimensions, .. }
            | Self::VectorI8 { dimensions }
            | Self::MultiVector { dimensions } => Some(*dimensions),
            _ => None,
        }
    }
    /// Whether this is a dense, sparse, or multi-vector column.
    pub fn is_vector(&self) -> bool {
        matches!(
            self,
            Self::Vector { .. }
                | Self::VectorI8 { .. }
                | Self::MultiVector { .. }
                | Self::SparseVector
        )
    }
    /// Infer a type from the first non-null value.
    pub fn infer(name: &str, value: &Value) -> Result<Self, ApiError> {
        if name == "vector" {
            return Ok(Self::Vector {
                dimensions: decode_vector(value)?.len(),
                f16: false,
            });
        }
        Ok(match value {
            Value::String(_) => Self::Scalar(ScalarType::String),
            Value::Bool(_) => Self::Scalar(ScalarType::Bool),
            Value::Number(n) if n.is_i64() || n.is_u64() => Self::Scalar(ScalarType::Int),
            Value::Number(_) => Self::Scalar(ScalarType::Float),
            Value::Array(a) => {
                let first = a.iter().find(|v| !v.is_null()).ok_or_else(|| {
                    ApiError::bad(format!(
                        "cannot infer type of empty array '{name}'; declare it in schema"
                    ))
                })?;
                match Self::infer("", first)? {
                    Self::Scalar(s) if s != ScalarType::Bytes => Self::Array(s),
                    _ => {
                        return Err(ApiError::bad(
                            "nested arrays require an explicit vector type",
                        ));
                    }
                }
            }
            _ => return Err(ApiError::bad(format!("cannot infer type of '{name}'"))),
        })
    }
}
impl fmt::Display for AttributeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MultiVector { dimensions } => write!(f, "[][{dimensions}]f32"),
            Self::VectorI8 { dimensions } => write!(f, "[{dimensions}]i8"),
            Self::SparseVector => f.write_str("{}f16"),
            Self::Scalar(t) => f.write_str(t.name()),
            Self::Array(t) => write!(f, "[]{}", t.name()),
            Self::Vector { dimensions, f16 } => {
                write!(f, "[{dimensions}]{}", if *f16 { "f16" } else { "f32" })
            }
        }
    }
}
impl FromStr for AttributeType {
    type Err = ApiError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(dense) = s.strip_prefix("[][") {
            let dense = format!("[{dense}");
            if let Ok(Self::Vector {
                dimensions,
                f16: false,
            }) = dense.parse()
            {
                return Ok(Self::MultiVector { dimensions });
            }
        }
        if s == "{}f16" {
            return Ok(Self::SparseVector);
        }
        let (array, name) = s.strip_prefix("[]").map_or((false, s), |s| (true, s));
        let scalar = match name {
            "string" => Some(ScalarType::String),
            "int" => Some(ScalarType::Int),
            "uint" => Some(ScalarType::Uint),
            "float" => Some(ScalarType::Float),
            "bool" => Some(ScalarType::Bool),
            "uuid" => Some(ScalarType::Uuid),
            "datetime" => Some(ScalarType::Datetime),
            "bytes" => Some(ScalarType::Bytes),
            _ => None,
        };
        if let Some(t) = scalar {
            return Ok(if array {
                Self::Array(t)
            } else {
                Self::Scalar(t)
            });
        }
        if !array
            && let Some((dims, element)) = s.strip_prefix('[').and_then(|s| s.split_once(']'))
            && matches!(element, "f32" | "f16" | "i8")
            && let Ok(dimensions) = dims.parse::<usize>()
            && (1..=10752).contains(&dimensions)
        {
            return Ok(if element == "i8" {
                Self::VectorI8 { dimensions }
            } else {
                Self::Vector {
                    dimensions,
                    f16: element == "f16",
                }
            });
        }
        Err(ApiError::bad(format!(
            "attribute type '{s}' is not supported by blowfish yet"
        )))
    }
}
impl Serialize for AttributeType {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}
impl<'de> Deserialize<'de> for AttributeType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Validated tokenizer and BM25 settings shared with the text-index implementation.
pub type FullTextConfig = bf_text::FtsParams;
/// Schema configuration. Optional settings retain their wire representation until normalization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttributeSchema {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub data_type: Option<AttributeType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filterable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_text_search: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glob: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fuzzy: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embed: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sparse_knn: Option<Value>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub drop: bool,
}
impl AttributeSchema {
    /// New schema inferred from an attribute type.
    pub fn new(t: AttributeType) -> Self {
        Self {
            data_type: Some(t),
            filterable: None,
            full_text_search: None,
            ann: None,
            regex: None,
            glob: None,
            fuzzy: None,
            embed: None,
            sparse_knn: None,
            drop: false,
        }
    }
    /// Full-text configuration, when enabled.
    pub fn text_config(&self) -> Option<FullTextConfig> {
        match self.full_text_search.as_ref()? {
            Value::Bool(true) => Some(FullTextConfig::default()),
            v if v.is_object() => serde_json::from_value(v.clone()).ok(),
            _ => None,
        }
    }
    /// Expand defaults for durable state and metadata output.
    pub fn normalize(&mut self, name: &str, metric: DistanceMetric) -> Result<(), ApiError> {
        let t = self
            .data_type
            .as_ref()
            .ok_or_else(|| ApiError::bad(format!("schema type is required for '{name}'")))?;
        if name == "id" {
            if !matches!(
                t,
                AttributeType::Scalar(ScalarType::Uint | ScalarType::String | ScalarType::Uuid)
            ) {
                return Err(ApiError::bad("id type must be uint, uuid, or string"));
            }
            self.filterable = None;
            return Ok(());
        }
        if self
            .full_text_search
            .as_ref()
            .is_some_and(|v| v != false && !v.is_null())
            && !matches!(
                t,
                AttributeType::Scalar(ScalarType::String)
                    | AttributeType::Array(ScalarType::String)
            )
        {
            return Err(ApiError::bad(
                "full_text_search requires string or []string",
            ));
        }
        if matches!(t, AttributeType::MultiVector { .. })
            && metric != DistanceMetric::CosineDistance
        {
            return Err(ApiError::bad(
                "ANN over vector arrays requires distance_metric cosine_distance",
            ));
        }
        if *t == AttributeType::SparseVector {
            if self.filterable == Some(true) {
                return Err(ApiError::bad("sparse vectors cannot be filterable"));
            }
            self.filterable = Some(false);
            if let Some(config) = &self.sparse_knn
                && (config.get("distance_metric").and_then(Value::as_str) != Some("dot_product")
                    || config.as_object().is_none_or(|o| o.len() != 1))
            {
                return Err(ApiError::bad(
                    "sparse_knn requires distance_metric dot_product",
                ));
            }
        } else if t.dimensions().is_some() {
            self.ann = Some(if matches!(t, AttributeType::MultiVector { .. }) {
                json!({"distance_metric":metric,"late_interaction":true})
            } else {
                json!({"distance_metric":metric})
            });
            self.filterable = if matches!(t, AttributeType::VectorI8 { .. }) {
                Some(false)
            } else {
                None
            };
        } else {
            if self
                .full_text_search
                .as_ref()
                .is_some_and(|v| v != false && !v.is_null())
            {
                let config: FullTextConfig =
                    serde_json::from_value(self.full_text_search.clone().expect("present"))
                        .map_err(|e| {
                            ApiError::bad(format!("invalid full_text_search configuration: {e}"))
                        })?;
                if !matches!(
                    t,
                    AttributeType::Scalar(ScalarType::String)
                        | AttributeType::Array(ScalarType::String)
                ) {
                    return Err(ApiError::bad(
                        "full_text_search requires string or []string",
                    ));
                }
                if config.tokenizer == bf_text::Tokenizer::PreTokenizedArray
                    && *t != AttributeType::Array(ScalarType::String)
                {
                    return Err(ApiError::bad("pre_tokenized_array requires []string"));
                }
                config.validate().map_err(ApiError::bad)?;
                // String serialization preserves the shortest decimal f32 representation.
                self.full_text_search = Some(
                    serde_json::from_str(
                        &serde_json::to_string(&config).expect("text config serializes"),
                    )
                    .expect("valid JSON"),
                );
            }
            let indexed = self.text_config().is_some()
                || self.regex == Some(true)
                || self.glob == Some(true)
                || self.fuzzy == Some(true);
            self.filterable
                .get_or_insert(!indexed && *t != AttributeType::Scalar(ScalarType::Bytes));
        }
        if (self.regex == Some(true) || self.glob == Some(true) || self.fuzzy == Some(true))
            && !matches!(
                t,
                AttributeType::Scalar(ScalarType::String)
                    | AttributeType::Array(ScalarType::String)
            )
        {
            return Err(ApiError::bad(
                "regex, glob, and fuzzy indexes require string attributes",
            ));
        }
        if self.regex == Some(true) && *t != AttributeType::Scalar(ScalarType::String) {
            return Err(ApiError::bad("regex requires a string attribute"));
        }
        if self.sparse_knn.is_some() && *t != AttributeType::SparseVector {
            return Err(ApiError::bad("sparse_knn requires a {}f16 attribute"));
        }
        if self.embed.as_ref().is_some_and(|v| !v.is_null()) {
            return Err(ApiError::bad(
                "native Embed configuration is not supported by blowfish yet",
            ));
        }
        Ok(())
    }
    /// Shape returned by the legacy schema endpoint (different from metadata).
    pub fn schema_value(&self) -> Value {
        let mut v = serde_json::to_value(self).expect("schema serializes");
        let o = v.as_object_mut().expect("object");
        // The legacy endpoint predates sparse ranking; metadata retains this setting.
        o.remove("sparse_knn");
        o.entry("filterable").or_insert(Value::Null);
        o.entry("full_text_search").or_insert(Value::Null);
        if self
            .data_type
            .as_ref()
            .is_some_and(|t| t.dimensions().is_some())
        {
            o.insert("ann".into(), Value::Bool(true));
        }
        v
    }
}
/// Shorthand or expanded schema input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SchemaInput {
    Type(AttributeType),
    Config(AttributeSchema),
}
impl SchemaInput {
    /// Expand a shorthand type.
    pub fn expanded(&self) -> AttributeSchema {
        match self {
            Self::Type(t) => AttributeSchema::new(t.clone()),
            Self::Config(c) => c.clone(),
        }
    }
}
/// User-supplied schema changes.
pub type SchemaUpdate = BTreeMap<String, SchemaInput>;
/// Normalized durable namespace schema.
pub type Schema = BTreeMap<String, AttributeSchema>;
/// Namespace-wide dense-vector metric.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    #[default]
    CosineDistance,
    EuclideanSquared,
}
