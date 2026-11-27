use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// A filter-based patch, executed before explicit row operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatchByFilter {
    pub patch: Document,
    pub filters: Filter,
}
/// Copy within the deployment, with SDK overload nesting.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CopyFrom {
    Name(String),
    Config {
        source_namespace: String,
        #[serde(flatten)]
        options: BTreeMap<String, Value>,
    },
}
/// Atomic namespace write input.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WriteRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upsert_rows: Option<Vec<Document>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upsert_columns: Option<Document>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_rows: Option<Vec<Document>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_columns: Option<Document>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletes: Option<Vec<Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_by_filter: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_by_filter: Option<PatchByFilter>,
    pub delete_by_filter_allow_partial: bool,
    pub patch_by_filter_allow_partial: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upsert_condition: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_condition: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_condition: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_metric: Option<DistanceMetric>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaUpdate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copy_from_namespace: Option<CopyFrom>,
    /// Create an independent namespace sharing immutable source segments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_from_namespace: Option<CopyFrom>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_namespace: Option<bool>,
    pub return_affected_ids: bool,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
/// Query limit and optional diversification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Limit {
    Total(usize),
    Config {
        total: usize,
        #[serde(default)]
        per: Option<LimitPer>,
    },
}
impl Limit {
    /// Total number of results requested.
    pub fn total(&self) -> usize {
        match self {
            Self::Total(n) => *n,
            Self::Config { total, .. } => *total,
        }
    }
    /// Diversification constraints.
    pub fn per(&self) -> Option<&LimitPer> {
        match self {
            Self::Config { per, .. } => per.as_ref(),
            _ => None,
        }
    }
}
/// Maximum number of results for each tuple of attribute values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitPer {
    pub attributes: Vec<String>,
    pub limit: usize,
}
/// Attribute projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IncludeAttributes {
    All(bool),
    Names(Vec<String>),
}
/// Strong or bounded eventual consistency.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "level", rename_all = "lowercase", deny_unknown_fields)]
pub enum Consistency {
    #[default]
    Strong,
    Eventual {
        #[serde(default = "default_staleness")]
        max_staleness_seconds: u64,
    },
}
fn default_staleness() -> u64 {
    60
}
/// Dense-vector response encoding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VectorEncoding {
    #[default]
    Float,
    Base64,
}
/// Single query or a multi-query envelope. Envelope validation occurs before execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank_by: Option<RankBy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<Limit>,
    pub offset: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_attributes: Option<IncludeAttributes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_attributes: Option<Vec<String>>,
    /// Computed values projected onto returned documents (maximum 256).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_attributes: Option<BTreeMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_by: Option<BTreeMap<String, Aggregate>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_by: Option<Vec<GroupBy>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queries: Option<Vec<QueryRequest>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerank_by: Option<Value>,
    pub consistency: Consistency,
    pub vector_encoding: VectorEncoding,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_metric: Option<DistanceMetric>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
impl QueryRequest {
    /// Effective row limit.
    pub fn total(&self) -> Option<usize> {
        self.limit.as_ref().map(Limit::total).or(self.top_k)
    }
}
/// Zero-priced local query billing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryBilling {
    pub billable_logical_bytes_queried: u64,
    pub billable_logical_bytes_returned: u64,
}
/// Measured query work and timing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryPerformance {
    pub cache_hit_ratio: f64,
    pub cache_temperature: String,
    pub server_total_ms: u64,
    pub query_execution_ms: u64,
    pub exhaustive_search_count: u64,
    pub approx_namespace_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_included_write_at: Option<String>,
    /// Tokens sent to an embedding provider for this query (native embedding only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_tokens: Option<u64>,
    /// Time spent in embedding calls (native embedding only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_ms: Option<u64>,
    /// Time spent in the model reranking call (blowfish extension).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rerank_ms: Option<u64>,
}
/// Result payload shared by single and multi-query responses.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SingleQueryResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Document>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<BTreeMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregation_groups: Option<Vec<Document>>,
}
/// Complete query response, with required telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    #[serde(flatten)]
    pub result: SingleQueryResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<Vec<SingleQueryResult>>,
    pub performance: QueryPerformance,
    pub billing: QueryBilling,
}
impl QueryResponse {
    /// Append the JSON body of an ID-only ranked response to `out`: the same bytes as serializing
    /// a [`QueryResponse`] whose rows are `{"id": id, "$dist": distance}` (keys in map order,
    /// numbers written by `serde_json`) and whose billing is zero, without building the rows as
    /// documents first. A non-finite distance has no JSON number, so it returns `false` with
    /// `out` truncated back; the general path reports that case as an error.
    pub fn write_id_rows(
        out: &mut Vec<u8>,
        rows: &[(crate::IdRef, f64)],
        performance: &QueryPerformance,
    ) -> bool {
        let start = out.len();
        out.extend_from_slice(b"{\"rows\":[");
        for (i, (id, distance)) in rows.iter().enumerate() {
            if !distance.is_finite() {
                out.truncate(start);
                return false;
            }
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(b"{\"$dist\":");
            serde_json::to_writer(&mut *out, distance).expect("a number serializes");
            out.extend_from_slice(b",\"id\":");
            match id {
                crate::IdRef::Uint(id) => {
                    serde_json::to_writer(&mut *out, id).expect("a number serializes")
                }
                crate::IdRef::Uuid(id) => {
                    out.push(b'"');
                    out.extend_from_slice(&crate::IdRef::uuid_text(*id));
                    out.push(b'"');
                }
            }
            out.push(b'}');
        }
        out.extend_from_slice(b"],\"performance\":");
        serde_json::to_writer(&mut *out, performance).expect("performance serializes");
        out.extend_from_slice(b",\"billing\":");
        serde_json::to_writer(&mut *out, &QueryBilling::default()).expect("billing serializes");
        out.push(b'}');
        true
    }
}
/// Write billing, optionally including conditional-write scans.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WriteBilling {
    pub billable_logical_bytes_written: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<QueryBilling>,
}
/// Measured server write time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WritePerformance {
    pub server_total_ms: u64,
    /// Tokens sent to an embedding provider for this write (native embedding only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_tokens: Option<u64>,
    /// Time spent in embedding calls (native embedding only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_ms: Option<u64>,
}
/// Atomic write result; operation counters are omitted when zero, matching the oracle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResponse {
    pub status: String,
    pub message: String,
    pub rows_affected: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_upserted: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_patched: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_deleted: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_remaining: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub upserted_ids: Vec<Id>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub patched_ids: Vec<Id>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub deleted_ids: Vec<Id>,
    pub billing: WriteBilling,
    pub performance: WritePerformance,
}
impl Default for WriteResponse {
    fn default() -> Self {
        Self {
            status: "OK".into(),
            message: "documents committed successfully".into(),
            rows_affected: 0,
            rows_upserted: None,
            rows_patched: None,
            rows_deleted: None,
            rows_remaining: None,
            upserted_ids: vec![],
            patched_ids: vec![],
            deleted_ids: vec![],
            billing: WriteBilling::default(),
            performance: WritePerformance::default(),
        }
    }
}
