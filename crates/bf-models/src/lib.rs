//! Model registry and provider clients for native embedding and model reranking.
//!
//! The operator maps model names (including turbopuffer's, such as `qwen/qwen3-embedding-0p6b`)
//! to a provider in the `[models]` configuration table. Two provider protocols exist:
//!
//! - `openai_embeddings`: `POST {base_url}/v1/embeddings`, the OpenAI shape, spoken by llama.cpp's
//!   `llama-server --embedding`, Ollama, vLLM, TEI, Fireworks, OpenRouter, and OpenAI.
//! - `rerank`: `POST {base_url}/v1/rerank` with `{model, query, documents, top_n}` answering
//!   `results[{index, relevance_score}]`, spoken by llama.cpp's `--reranking`, vLLM, TEI, Jina,
//!   Cohere, and Voyage.
//!
//! Calls share a bounded per-model concurrency pool and retry on 429 and 5xx. Nothing is cached
//! across writes; query embeddings use a small in-memory LRU.
use bf_types::ApiError;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;

/// Which wire protocol a model speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// OpenAI-compatible `/v1/embeddings`.
    OpenaiEmbeddings,
    /// Cohere/Jina-style `/v1/rerank`.
    Rerank,
    /// A GGUF model run in-process through llama.cpp (cargo feature `local-models`). Set `task`.
    Local,
}
/// What a model does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Task {
    Embedding,
    Rerank,
}

/// One `[models."name"]` table.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub provider: Provider,
    /// Provider base URL without the path, for example `http://127.0.0.1:8081`. Not used by
    /// `local`.
    #[serde(default)]
    pub base_url: String,
    /// `local` only: whether the model embeds or reranks.
    #[serde(default)]
    pub task: Option<Task>,
    /// `local` only: Hugging Face repository holding the GGUF file, for example
    /// `ggml-org/embeddinggemma-2-GGUF`.
    #[serde(default)]
    pub hf_repo: Option<String>,
    /// `local` only: file within `hf_repo`, for example `embeddinggemma-2-Q8_0.gguf`.
    #[serde(default)]
    pub hf_file: Option<String>,
    /// `local` only: a GGUF file on disk, used instead of `hf_repo`.
    #[serde(default)]
    pub path: Option<String>,
    /// `local` only: expected SHA-256 of the file, checked after a download.
    #[serde(default)]
    pub sha256: Option<String>,
    /// `local` only: inference threads for this model (default: half the cores).
    #[serde(default)]
    pub threads: Option<usize>,
    /// `local` only: concurrent inference contexts; `threads` are shared between them.
    #[serde(default)]
    pub workers: Option<usize>,
    /// `local` only: requests that may wait for a worker; further requests get a 429.
    #[serde(default)]
    pub queue: Option<usize>,
    /// Name of an environment variable holding the provider's bearer key. The key itself never
    /// appears in configuration.
    #[serde(default)]
    pub api_key_env: Option<String>,
    /// The provider's own model ID. Defaults to the registry name.
    #[serde(default)]
    pub model: Option<String>,
    /// Native (default) output dimensions. Required for embedding models.
    #[serde(default)]
    pub dims: Option<usize>,
    /// Other output sizes a client may request (Matryoshka truncation). Empty means only `dims`.
    #[serde(default)]
    pub allowed_dims: Vec<usize>,
    /// Send `dimensions` to the provider instead of truncating and renormalizing here.
    #[serde(default)]
    pub send_dimensions: bool,
    /// Inputs longer than this many tokens are truncated (estimated at three characters per token).
    #[serde(default = "default_max_tokens")]
    pub max_input_tokens: usize,
    /// Inputs per provider request.
    #[serde(default)]
    pub batch_size: Option<usize>,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    /// Concurrent provider requests for this model.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Retries after a 429, a 5xx, or a connection error.
    #[serde(default = "default_retries")]
    pub retries: u32,
    /// Prepended to query text (embedding queries, and the rerank query).
    #[serde(default)]
    pub query_prefix: String,
    /// Prepended to documents (embedded rows, and rerank candidates).
    #[serde(default)]
    pub document_prefix: String,
    /// HTTP providers: endpoint path override, for example `/rerank` or `/embed`.
    #[serde(default)]
    pub endpoint_path: Option<String>,
}
impl ModelConfig {
    fn kind(&self) -> Option<Task> {
        match self.provider {
            Provider::OpenaiEmbeddings => Some(Task::Embedding),
            Provider::Rerank => Some(Task::Rerank),
            Provider::Local => self.task,
        }
    }
}

/// Where local models are cached and downloaded from.
#[derive(Debug, Clone)]
pub struct ModelOptions {
    /// Directory for downloaded GGUF files (`BLOWFISH_MODELS_CACHE_DIR`, else `~/.cache/blowfish/models`).
    pub cache_dir: std::path::PathBuf,
    /// Hugging Face endpoint (`HF_ENDPOINT`, else `https://huggingface.co`).
    pub hf_endpoint: String,
}
impl Default for ModelOptions {
    fn default() -> Self {
        let cache_dir = std::env::var_os("BLOWFISH_MODELS_CACHE_DIR")
            .map(Into::into)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                    .join(".cache/blowfish/models")
            });
        Self {
            cache_dir,
            hf_endpoint: std::env::var("HF_ENDPOINT")
                .unwrap_or_else(|_| "https://huggingface.co".into()),
        }
    }
}
fn default_max_tokens() -> usize {
    8192
}
fn default_timeout() -> u64 {
    60_000
}
fn default_concurrency() -> usize {
    4
}
fn default_retries() -> u32 {
    3
}

/// The `[models]` table: model name to provider settings.
pub type ModelsConfig = BTreeMap<String, ModelConfig>;

/// Merge `BLOWFISH_MODELS_JSON` (a JSON object of the same shape as the TOML table) over `base`.
pub fn apply_env(base: &mut ModelsConfig, json: &str) -> Result<(), String> {
    let extra: ModelsConfig =
        serde_json::from_str(json).map_err(|e| format!("invalid BLOWFISH_MODELS_JSON: {e}"))?;
    base.extend(extra);
    Ok(())
}

/// Whether an embedding is for a document being stored or a query being run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Document,
    Query,
}

/// Embedding results with the tokens the provider reported (or an estimate).
#[derive(Debug, Clone, Default)]
pub struct Embedded {
    pub vectors: Vec<Vec<f32>>,
    pub tokens: u64,
}

/// Rerank scores, one per input document, with token usage.
#[derive(Debug, Clone, Default)]
pub struct Reranked {
    pub scores: Vec<f32>,
    pub tokens: u64,
}

/// What a caller needs to know about an embedding model.
#[derive(Debug, Clone)]
pub struct EmbeddingInfo {
    pub dims: usize,
    pub allowed_dims: Vec<usize>,
}

struct Entry {
    local: Option<local::Local>,
    config: ModelConfig,
    key: Option<String>,
    pool: Semaphore,
}

const QUERY_CACHE_CAP: usize = 1024;
#[derive(Default)]
struct Lru {
    map: HashMap<(String, usize, String), Vec<f32>>,
    order: VecDeque<(String, usize, String)>,
}

/// The configured models and an HTTP client.
pub struct Models {
    http: reqwest::Client,
    entries: HashMap<String, Entry>,
    cache: Mutex<Lru>,
}
impl std::fmt::Debug for Models {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<_> = self.entries.keys().collect();
        names.sort();
        f.debug_struct("Models").field("names", &names).finish()
    }
}

impl Models {
    /// Validate the configuration with default cache settings.
    pub fn new(config: ModelsConfig) -> Result<Arc<Self>, String> {
        Self::with_options(config, ModelOptions::default())
    }
    /// Validate the configuration and read API keys from the environment.
    pub fn with_options(config: ModelsConfig, options: ModelOptions) -> Result<Arc<Self>, String> {
        let mut entries = HashMap::new();
        for (name, c) in config {
            let kind = c.kind().ok_or_else(|| {
                format!("model '{name}': local models need task = \"embedding\" or \"rerank\"")
            })?;
            if c.provider != Provider::Local && c.base_url.trim().is_empty() {
                return Err(format!("model '{name}': base_url is required"));
            }
            if kind == Task::Embedding {
                let dims = c
                    .dims
                    .ok_or_else(|| format!("model '{name}': dims is required"))?;
                if dims == 0 || c.allowed_dims.contains(&0) {
                    return Err(format!("model '{name}': dims must be positive"));
                }
            }
            if c.concurrency == 0 || c.max_input_tokens == 0 || c.batch_size == Some(0) {
                return Err(format!(
                    "model '{name}': concurrency, max_input_tokens, and batch_size must be positive"
                ));
            }
            let key = match &c.api_key_env {
                Some(var) => Some(std::env::var(var).map_err(|_| {
                    format!("model '{name}': environment variable {var} is not set")
                })?),
                None => None,
            };
            let local = if c.provider == Provider::Local {
                Some(local::Local::new(&name, &c, options.clone())?)
            } else {
                None
            };
            entries.insert(
                name,
                Entry {
                    local,
                    pool: Semaphore::new(c.concurrency),
                    config: c,
                    key,
                },
            );
        }
        Ok(Arc::new(Self {
            http: reqwest::Client::builder()
                .build()
                .map_err(|e| e.to_string())?,
            entries,
            cache: Mutex::default(),
        }))
    }
    /// Configured embedding model names and rerank model names.
    pub fn names(&self) -> (Vec<String>, Vec<String>) {
        let mut e = vec![];
        let mut r = vec![];
        for (n, x) in &self.entries {
            match x.config.kind() {
                Some(Task::Embedding) => e.push(n.clone()),
                _ => r.push(n.clone()),
            }
        }
        e.sort();
        r.sort();
        (e, r)
    }
    /// Dimensions of an embedding model, or a 400 for an unknown or non-embedding model.
    pub fn embedding_info(&self, model: &str) -> Result<EmbeddingInfo, ApiError> {
        let e = self.entry(model, Task::Embedding, "embedding")?;
        Ok(EmbeddingInfo {
            dims: e.config.dims.expect("validated"),
            allowed_dims: e.config.allowed_dims.clone(),
        })
    }
    /// Whether a rerank model is configured under this name.
    pub fn has_rerank(&self, model: &str) -> bool {
        self.entries
            .get(model)
            .is_some_and(|e| e.config.kind() == Some(Task::Rerank))
    }
    fn entry(&self, model: &str, want: Task, what: &str) -> Result<&Entry, ApiError> {
        match self.entries.get(model) {
            Some(e) if e.config.kind() == Some(want) => Ok(e),
            Some(_) => Err(ApiError::bad(format!(
                "model '{model}' is not an {what} model"
            ))),
            None => Err(ApiError::bad(format!(
                "unknown or unconfigured {what} model '{model}'"
            ))),
        }
    }

    /// Embed `texts` with the model, at `dims` dimensions (the model's default when `None`).
    /// Returned vectors are in input order. Prefixes are added and over-long inputs truncated here.
    pub async fn embed(
        &self,
        model: &str,
        texts: &[String],
        kind: InputKind,
        dims: Option<usize>,
    ) -> Result<Embedded, ApiError> {
        let e = self.entry(model, Task::Embedding, "embedding")?;
        let native = e.config.dims.expect("validated");
        let dims = dims.unwrap_or(native);
        if dims != native && !e.config.allowed_dims.contains(&dims) {
            return Err(ApiError::bad(format!(
                "dims {dims} is not supported by model '{model}' (allowed: {})",
                allowed_list(native, &e.config.allowed_dims)
            )));
        }
        let prefix = match kind {
            InputKind::Document => &e.config.document_prefix,
            InputKind::Query => &e.config.query_prefix,
        };
        let limit = e.config.max_input_tokens.saturating_mul(CHARS_PER_TOKEN);
        let inputs: Vec<String> = texts
            .iter()
            .map(|t| format!("{prefix}{}", truncate(t, limit)))
            .collect();
        // Single-query embeddings are cached; documents never are.
        let cache_key = (kind == InputKind::Query && inputs.len() == 1)
            .then(|| (model.to_string(), dims, inputs[0].clone()));
        if let Some(k) = &cache_key
            && let Some(v) = self.cache.lock().expect("cache lock").map.get(k)
        {
            return Ok(Embedded {
                vectors: vec![v.clone()],
                tokens: 0,
            });
        }
        let batch = e.config.batch_size.unwrap_or(32);
        let chunks: Vec<&[String]> = inputs.chunks(batch).collect();
        let calls = chunks.iter().map(|c| self.embed_batch(e, model, c, dims));
        let mut out = Embedded::default();
        for r in futures::future::join_all(calls).await {
            let r = r?;
            out.vectors.extend(r.vectors);
            out.tokens += r.tokens;
        }
        if let Some(k) = cache_key {
            let mut lru = self.cache.lock().expect("cache lock");
            if lru.map.insert(k.clone(), out.vectors[0].clone()).is_none() {
                lru.order.push_back(k);
                if lru.order.len() > QUERY_CACHE_CAP
                    && let Some(old) = lru.order.pop_front()
                {
                    lru.map.remove(&old);
                }
            }
        }
        Ok(out)
    }

    async fn embed_batch(
        &self,
        e: &Entry,
        model: &str,
        inputs: &[String],
        dims: usize,
    ) -> Result<Embedded, ApiError> {
        let native = e.config.dims.expect("validated");
        if let Some(local) = &e.local {
            let out = local.embed(inputs).await?;
            if out.vectors.iter().any(|v| v.len() != native) {
                return Err(provider_error(
                    model,
                    &format!("produced a different size than dims = {native}; fix 'dims'"),
                ));
            }
            let vectors = out.vectors.into_iter().map(|v| fit_dims(v, dims)).collect();
            return Ok(Embedded {
                vectors,
                tokens: out.tokens,
            });
        }
        let mut body = json!({
            "model": e.config.model.as_deref().unwrap_or(model),
            "input": inputs,
            "encoding_format": "float",
        });
        if e.config.send_dimensions {
            body["dimensions"] = json!(dims);
        }
        let reply = self.post(e, model, "/v1/embeddings", &body).await?;
        let data = reply["data"]
            .as_array()
            .filter(|d| d.len() == inputs.len())
            .ok_or_else(|| provider_error(model, "response has the wrong number of embeddings"))?;
        let mut vectors = vec![Vec::new(); inputs.len()];
        for (pos, item) in data.iter().enumerate() {
            let index = item["index"].as_u64().map_or(pos, |i| i as usize);
            let v: Vec<f32> = item["embedding"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_f64().map(|f| f as f32))
                        .collect()
                })
                .unwrap_or_default();
            let slot = vectors
                .get_mut(index)
                .ok_or_else(|| provider_error(model, "response index is out of range"))?;
            let expected = if e.config.send_dimensions {
                dims
            } else {
                native
            };
            if v.len() != expected {
                return Err(provider_error(
                    model,
                    &format!(
                        "returned {} dimensions, expected {expected}; check 'dims' for this model",
                        v.len()
                    ),
                ));
            }
            if v.iter().any(|x| !x.is_finite()) {
                return Err(provider_error(model, "returned a non-finite embedding"));
            }
            *slot = fit_dims(v, dims);
        }
        let tokens = reply["usage"]["prompt_tokens"]
            .as_u64()
            .or_else(|| reply["usage"]["total_tokens"].as_u64())
            .unwrap_or_else(|| estimate_tokens(inputs));
        Ok(Embedded { vectors, tokens })
    }

    /// Score each document against the query; higher is more relevant. Scores come back in
    /// document order.
    pub async fn rerank(
        &self,
        model: &str,
        query: &str,
        documents: &[String],
    ) -> Result<Reranked, ApiError> {
        let e = self.entry(model, Task::Rerank, "rerank")?;
        if documents.is_empty() {
            return Ok(Reranked::default());
        }
        let budget = e.config.max_input_tokens.saturating_mul(CHARS_PER_TOKEN);
        let q = format!("{}{}", e.config.query_prefix, truncate(query, budget / 2));
        let doc_budget = budget.saturating_sub(q.len()).max(256);
        let docs: Vec<String> = documents
            .iter()
            .map(|d| format!("{}{}", e.config.document_prefix, truncate(d, doc_budget)))
            .collect();
        let batch = e.config.batch_size.unwrap_or(64);
        let starts: Vec<usize> = (0..docs.len()).step_by(batch).collect();
        let calls = starts.iter().map(|&s| {
            let slice = &docs[s..(s + batch).min(docs.len())];
            self.rerank_batch(e, model, &q, slice)
        });
        let mut out = Reranked::default();
        for r in futures::future::join_all(calls).await {
            let r = r?;
            out.scores.extend(r.scores);
            out.tokens += r.tokens;
        }
        Ok(out)
    }

    async fn rerank_batch(
        &self,
        e: &Entry,
        model: &str,
        query: &str,
        docs: &[String],
    ) -> Result<Reranked, ApiError> {
        if let Some(local) = &e.local {
            let out = local.rerank(query, docs).await?;
            let scores = out.vectors.iter().map(|v| v[0]).collect();
            return Ok(Reranked {
                scores,
                tokens: out.tokens,
            });
        }
        let body = json!({
            "model": e.config.model.as_deref().unwrap_or(model),
            "query": query,
            "documents": docs,
            "top_n": docs.len(),
        });
        let reply = self.post(e, model, "/v1/rerank", &body).await?;
        let results = reply["results"]
            .as_array()
            .or_else(|| reply["data"].as_array())
            .ok_or_else(|| provider_error(model, "response has no results"))?;
        let mut scores = vec![f32::NEG_INFINITY; docs.len()];
        for r in results {
            let i = r["index"].as_u64().map(|i| i as usize);
            let s = r["relevance_score"]
                .as_f64()
                .or_else(|| r["score"].as_f64());
            match (i, s) {
                (Some(i), Some(s)) if i < docs.len() && s.is_finite() => scores[i] = s as f32,
                _ => return Err(provider_error(model, "malformed rerank result")),
            }
        }
        if scores.contains(&f32::NEG_INFINITY) {
            return Err(provider_error(model, "response omitted some documents"));
        }
        let tokens = reply["usage"]["total_tokens"]
            .as_u64()
            .unwrap_or_else(|| estimate_tokens(docs) + estimate_tokens(&[query.to_string()]));
        Ok(Reranked { scores, tokens })
    }

    /// One JSON POST through the model's concurrency pool, retrying 429, 5xx, and transport errors.
    async fn post(
        &self,
        e: &Entry,
        model: &str,
        default_path: &str,
        body: &Value,
    ) -> Result<Value, ApiError> {
        let path = e.config.endpoint_path.as_deref().unwrap_or(default_path);
        let url = format!("{}{path}", e.config.base_url.trim_end_matches('/'));
        let _permit = e
            .pool
            .acquire()
            .await
            .map_err(|_| ApiError::new(503, "model pool is closed"))?;
        let mut attempt = 0u32;
        loop {
            let mut req = self
                .http
                .post(&url)
                .timeout(Duration::from_millis(e.config.timeout_ms))
                .json(body);
            if let Some(key) = &e.key {
                req = req.bearer_auth(key);
            }
            let (retryable, error) = match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    return resp
                        .json::<Value>()
                        .await
                        .map_err(|_| provider_error(model, "response is not valid JSON"));
                }
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let retry_after = resp
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok());
                    let detail = resp.text().await.unwrap_or_default();
                    tracing::warn!(model, status, detail = %truncate(&detail, 300), "model provider error");
                    if let Some(s) = retry_after
                        && (status == 429 || status == 503)
                        && attempt < e.config.retries
                    {
                        tokio::time::sleep(Duration::from_secs(s.min(10))).await;
                        attempt += 1;
                        continue;
                    }
                    match status {
                        429 => (
                            true,
                            ApiError::new(
                                429,
                                format!("model '{model}' provider rate limit exceeded"),
                            ),
                        ),
                        500..=599 => (
                            true,
                            ApiError::new(
                                502,
                                format!("model '{model}' provider failed with status {status}"),
                            ),
                        ),
                        _ => (
                            false,
                            ApiError::new(
                                502,
                                format!(
                                    "model '{model}' provider rejected the request with status {status}"
                                ),
                            ),
                        ),
                    }
                }
                Err(err) if err.is_timeout() => (
                    true,
                    ApiError::new(504, format!("model '{model}' provider timed out")),
                ),
                Err(err) => {
                    tracing::warn!(model, error = %err, "model provider unreachable");
                    (
                        true,
                        ApiError::new(503, format!("model '{model}' provider is unreachable")),
                    )
                }
            };
            if !retryable || attempt >= e.config.retries {
                return Err(error);
            }
            tokio::time::sleep(Duration::from_millis(100 << attempt.min(6))).await;
            attempt += 1;
        }
    }
}

const CHARS_PER_TOKEN: usize = 3;

fn allowed_list(native: usize, allowed: &[usize]) -> String {
    let mut all: Vec<usize> = std::iter::once(native)
        .chain(allowed.iter().copied())
        .collect();
    all.sort_unstable();
    all.dedup();
    all.iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
fn provider_error(model: &str, what: &str) -> ApiError {
    ApiError::new(502, format!("model '{model}' provider {what}"))
}
fn estimate_tokens(texts: &[String]) -> u64 {
    texts.iter().map(|t| t.len().div_ceil(4) as u64).sum()
}
/// Cut to at most `max` bytes on a character boundary.
fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}
/// Matryoshka truncation: keep the leading `dims` values and renormalize to unit length.
pub fn fit_dims(mut v: Vec<f32>, dims: usize) -> Vec<f32> {
    if v.len() > dims {
        v.truncate(dims);
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            v.iter_mut().for_each(|x| *x /= norm);
        }
    }
    v
}
mod local;
#[cfg(test)]
mod tests;
