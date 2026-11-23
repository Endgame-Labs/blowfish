//! Shared data model and turbopuffer wire contract.
mod ast;
mod schema;
mod wire;
pub use ast::*;
pub use schema::*;
pub use wire::*;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt;

/// A document identifier. UUIDs are canonicalized by namespace schema validation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Id {
    /// Unsigned integer ID.
    Uint(u64),
    /// String ID, including a schema-declared UUID.
    String(String),
}
impl Id {
    /// Validate the wire size of an ID.
    pub fn validate(&self) -> Result<(), ApiError> {
        if matches!(self, Self::String(s) if s.len() > 64) {
            return Err(ApiError::bad("document ID exceeds 64 bytes"));
        }
        Ok(())
    }
    /// JSON representation, preserving numeric IDs.
    pub fn value(&self) -> Value {
        serde_json::to_value(self).expect("ID serializes")
    }
}
/// Flat document, including its required `id` attribute.
pub type Document = serde_json::Map<String, Value>;
/// Nullable JSON attribute value; validation is driven by the namespace schema.
pub type AttributeValue = Value;

/// One error representation used by every layer and HTTP endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// HTTP status code.
    pub status: u16,
    /// Public error message (never credentials or backend URLs).
    pub message: String,
    /// How long the client should wait before retrying, in milliseconds. The HTTP layer sends it
    /// as `Retry-After` and `retry-after-ms` on 429 and 503 (with a default when unset).
    pub retry_after_ms: Option<u64>,
    /// Explicit `x-should-retry` verdict; when unset, the HTTP layer chooses by status.
    pub should_retry: Option<bool>,
}
impl ApiError {
    /// Construct an error with an explicit HTTP status.
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            retry_after_ms: None,
            should_retry: None,
        }
    }
    /// Suggest a retry delay in milliseconds (sent as `Retry-After` and `retry-after-ms`).
    pub fn with_retry_after_ms(mut self, ms: u64) -> Self {
        self.retry_after_ms = Some(ms);
        self
    }
    /// Mark the error transient (`true`) or permanent (`false`) for the SDKs' retry logic.
    pub fn with_should_retry(mut self, retry: bool) -> Self {
        self.should_retry = Some(retry);
        self
    }
    /// Semantic validation failure.
    pub fn bad(message: impl Into<String>) -> Self {
        Self::new(400, message)
    }
    /// Invalid JSON or tuple grammar.
    pub fn malformed(message: impl Into<String>) -> Self {
        Self::new(422, message)
    }
    /// A missing namespace.
    pub fn not_found(ns: &str) -> Self {
        Self::new(404, format!("namespace '{ns}' was not found"))
    }
    /// Safe error body.
    pub fn body(&self) -> Value {
        json!({"status":"error", "error":self.message})
    }
}
impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for ApiError {}

/// Validate a namespace before using it in an object key.
pub fn validate_namespace(ns: &str) -> Result<(), ApiError> {
    if ns.is_empty()
        || ns.len() > 128
        || ns == "."
        || ns == ".."
        || !ns
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(ApiError::bad("invalid namespace name"));
    }
    Ok(())
}

/// Decode a numeric vector or base64 little-endian f32 vector, rejecting NaN and infinity.
pub fn decode_vector(value: &Value) -> Result<Vec<f32>, ApiError> {
    if value.get(0).is_some_and(|v| v == "Embed") {
        return Err(ApiError::bad(
            "native Embed expressions are not supported by blowfish yet",
        ));
    }

    use base64::Engine;
    let values = if let Some(s) = value.as_str() {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|_| ApiError::bad("invalid base64 vector"))?;
        if bytes.len() % 4 != 0 {
            return Err(ApiError::bad(
                "base64 vector length must be a multiple of four",
            ));
        }
        bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect()
    } else if let Some(a) = value.as_array() {
        a.iter()
            .map(|x| {
                x.as_f64()
                    .map(|n| n as f32)
                    .ok_or_else(|| ApiError::bad("vector elements must be numbers"))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        return Err(ApiError::bad("expected a vector array or base64 string"));
    };
    check_vector(values)
}
/// The checks [`decode_vector`] applies to every decoded vector.
pub(crate) fn check_vector(values: Vec<f32>) -> Result<Vec<f32>, ApiError> {
    if values.is_empty() || values.iter().any(|n| !n.is_finite()) {
        return Err(ApiError::bad(
            "vectors must contain finite numbers and at least one dimension",
        ));
    }
    Ok(values)
}
/// Encode a vector in its declared element width for responses.
pub fn encode_vector(values: &[f32], f16: bool) -> String {
    use base64::Engine;
    let bytes: Vec<u8> = if f16 {
        values
            .iter()
            .flat_map(|x| half::f16::from_f32(*x).to_le_bytes())
            .collect()
    } else {
        values.iter().flat_map(|x| x.to_le_bytes()).collect()
    };
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
/// ISO-8601 dates and datetimes normalized to UTC milliseconds.
pub fn datetime_millis(value: &str) -> Result<i64, ApiError> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime};
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_millis());
    }
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|d| d.and_utc().timestamp_millis())
        .ok_or_else(|| ApiError::bad("invalid datetime"))
}

/// Decode sparse wire weights, rounding to f16 and rejecting zero, negative,
/// underflowed, or overflowing values, as required by SparseKNN.
pub fn decode_sparse(
    value: &serde_json::Value,
) -> Result<std::collections::BTreeMap<String, f32>, ApiError> {
    let weights = value
        .as_object()
        .ok_or_else(|| ApiError::bad("sparse vector must be an object"))?;
    if weights.len() > 4096 {
        return Err(ApiError::bad(
            "sparse vectors cannot exceed 4096 dimensions",
        ));
    }
    weights
        .iter()
        .map(|(dim, value)| {
            let weight = value
                .as_f64()
                .ok_or_else(|| ApiError::bad("sparse vector requires positive f16 weights"))?;
            let weight = half::f16::from_f64(weight).to_f32();
            if !weight.is_finite() || weight <= 0.0 {
                return Err(ApiError::bad(
                    "sparse vector weights must be positive and finite after f16 conversion",
                ));
            }
            Ok((dim.clone(), weight))
        })
        .collect()
}
