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
    Vector { dimensions: usize, f16: bool },
}
impl AttributeType {
    /// Dense-vector dimensions, if this is a vector.
    pub fn dimensions(&self) -> Option<usize> {
        match self {
            Self::Vector { dimensions, .. } => Some(*dimensions),
            _ => None,
        }
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
            && matches!(element, "f32" | "f16")
            && let Ok(dimensions) = dims.parse::<usize>()
            && (1..=10752).contains(&dimensions)
        {
            return Ok(Self::Vector {
                dimensions,
                f16: element == "f16",
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

/// Complete full-text settings, with persistent defaults.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FullTextConfig {
    pub k1: f64,
    pub b: f64,
    pub k3: f64,
    pub ascii_folding: bool,
    pub case_sensitive: bool,
    pub language: String,
    pub stemming: bool,
    pub remove_stopwords: bool,
    pub tokenizer: String,
    pub max_token_length: usize,
}
impl Default for FullTextConfig {
    fn default() -> Self {
        Self {
            k1: 1.2,
            b: 0.75,
            k3: 8.0,
            ascii_folding: false,
            case_sensitive: false,
            language: "english".into(),
            stemming: false,
            remove_stopwords: false,
            tokenizer: "word_v4".into(),
            max_token_length: 39,
        }
    }
}
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
        if t.dimensions().is_some() {
            self.ann = Some(json!({"distance_metric":metric}));
            self.filterable = None;
        } else {
            if let Some(config) = self.text_config() {
                if !matches!(
                    t,
                    AttributeType::Scalar(ScalarType::String)
                        | AttributeType::Array(ScalarType::String)
                ) {
                    return Err(ApiError::bad(
                        "full_text_search requires string or []string",
                    ));
                }
                if config.k1 <= 0.0
                    || !(0.0..=1.0).contains(&config.b)
                    || config.k3 <= 0.0
                    || !(1..=254).contains(&config.max_token_length)
                {
                    return Err(ApiError::bad("invalid full_text_search parameters"));
                }
                self.full_text_search =
                    Some(serde_json::to_value(config).expect("text config serializes"));
            } else if self
                .full_text_search
                .as_ref()
                .is_some_and(|v| !v.is_boolean() && !v.is_null())
            {
                return Err(ApiError::bad("invalid full_text_search configuration"));
            }
            let indexed = self.text_config().is_some()
                || self.regex == Some(true)
                || self.glob == Some(true)
                || self.fuzzy == Some(true);
            self.filterable
                .get_or_insert(!indexed && *t != AttributeType::Scalar(ScalarType::Bytes));
        }
        if self.embed.as_ref().is_some_and(|v| !v.is_null()) || self.sparse_knn.is_some() {
            return Err(ApiError::bad(
                "native embedding and sparse vectors are not supported by blowfish yet",
            ));
        }
        Ok(())
    }
    /// Shape returned by the legacy schema endpoint (different from metadata).
    pub fn schema_value(&self) -> Value {
        let mut v = serde_json::to_value(self).expect("schema serializes");
        let o = v.as_object_mut().expect("object");
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
