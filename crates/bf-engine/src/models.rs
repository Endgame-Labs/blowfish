//! Native embedding and model reranking: the async steps around the synchronous engine.
//!
//! Embedding happens before a write reaches the commit pipeline (so rows are embedded before
//! `upsert_condition` is evaluated) and before a query reaches the search (an `Embed` expression
//! is replaced by the vector it produces). Model reranking runs the first-stage queries without
//! `rerank_by`, scores the candidates with the provider, and reorders them.
use crate::{Engine, QueryDiagnostics};
use bf_models::{InputKind, Models};
use bf_types::*;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Instant};

/// Most upsert rows in one write to a namespace that has embedded attributes.
const MAX_EMBED_ROWS: usize = 256;
const MAX_EMBEDDED_ATTRIBUTES: usize = 4;

/// Embedding and reranking work done for one request.
#[derive(Default)]
pub(crate) struct ModelStats {
    tokens: u64,
    embed_ms: u64,
    embedded: bool,
    rerank_ms: Option<u64>,
}
impl ModelStats {
    fn query(&self, p: &mut QueryPerformance) {
        if self.embedded {
            p.embedding_tokens = Some(self.tokens);
            p.embedding_ms = Some(self.embed_ms);
        }
        p.rerank_ms = self.rerank_ms;
    }
}

struct Target {
    source: String,
    model: String,
    dims: usize,
    target: String,
}

impl Engine {
    /// Schema of an existing namespace, or `None` when it does not exist yet.
    async fn schema_of(&self, ns: &str) -> Result<Option<Schema>, ApiError> {
        match self.snapshot(ns, &Consistency::Strong).await {
            Ok((state, _)) => Ok(state.exists.then(|| state.schema.clone())),
            Err(e) if e.status == 404 => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Fill in vectors for rows whose embedded attribute is set and whose target vector is absent,
    /// and create the generated vector columns that `embed` settings need. Returns the embedding
    /// cost when any provider call was made.
    pub(crate) async fn embed_for_write(
        &self,
        ns: &str,
        request: &mut WriteRequest,
    ) -> Result<Option<(u64, u64)>, ApiError> {
        let adds_embed = request.schema.iter().flatten().any(|(_, c)| {
            matches!(c, SchemaInput::Config(a) if a.embed.as_ref().is_some_and(|v| !v.is_null()))
        });
        let Some(models) = self.0.config.models.clone() else {
            if adds_embed {
                return Err(ApiError::bad(
                    "native embedding is not configured on this server (no [models] entries)",
                ));
            }
            return Ok(None);
        };
        let has_rows = request.upsert_rows.as_ref().is_some_and(|r| !r.is_empty())
            || request.upsert_columns.is_some();
        if !has_rows && !adds_embed {
            return Ok(None);
        }
        let current = self.schema_of(ns).await?;
        let mut schema = current.clone().unwrap_or_default();
        for (name, change) in request.schema.iter().flatten() {
            let cfg = change.expanded();
            if cfg.drop {
                schema.remove(name);
            } else if let Some(old) = schema.get_mut(name) {
                match cfg.embed {
                    Some(Value::Null) => old.embed = None,
                    Some(v) => old.embed = Some(v),
                    None => {}
                }
            } else {
                schema.insert(name.clone(), cfg);
            }
        }
        let targets = plan_targets(&models, &mut schema, request, current.is_none())?;
        if targets.is_empty() {
            return Ok(None);
        }
        if let Some(columns) = request.upsert_columns.take() {
            let rows = crate::write::columns_to_rows(&columns, &schema)?;
            request
                .upsert_rows
                .get_or_insert_with(Vec::new)
                .extend(rows);
        }
        let rows = request.upsert_rows.get_or_insert_with(Vec::new);
        if rows.len() > MAX_EMBED_ROWS {
            return Err(ApiError::bad(format!(
                "upserts to a namespace with embedded attributes are limited to {MAX_EMBED_ROWS} rows"
            )));
        }
        // Unique texts per (model, dims), so repeated text is embedded once per write.
        let mut jobs: BTreeMap<(String, usize), BTreeMap<String, Vec<f32>>> = BTreeMap::new();
        for row in rows.iter() {
            for t in &targets {
                if row.get(&t.target).is_none_or(Value::is_null)
                    && let Some(text) = row.get(&t.source).and_then(Value::as_str)
                {
                    jobs.entry((t.model.clone(), t.dims))
                        .or_default()
                        .insert(text.to_string(), Vec::new());
                }
            }
        }
        if jobs.is_empty() {
            return Ok(None);
        }
        let start = Instant::now();
        let mut tokens = 0;
        for ((model, dims), texts) in &mut jobs {
            let keys: Vec<String> = texts.keys().cloned().collect();
            let out = models
                .embed(model, &keys, InputKind::Document, Some(*dims))
                .await?;
            tokens += out.tokens;
            for (k, v) in keys.into_iter().zip(out.vectors) {
                texts.insert(k, v);
            }
        }
        for row in rows.iter_mut() {
            for t in &targets {
                if row.get(&t.target).is_none_or(Value::is_null)
                    && let Some(text) = row.get(&t.source).and_then(Value::as_str)
                {
                    let v = &jobs[&(t.model.clone(), t.dims)][text];
                    row.insert(t.target.clone(), json!(v));
                }
            }
        }
        Ok(Some((tokens, start.elapsed().as_millis() as u64)))
    }

    /// Replace every `Embed` rank expression with the vector it produces.
    async fn resolve_embeds(
        &self,
        models: &Arc<Models>,
        ns: &str,
        request: &mut QueryRequest,
        stats: &mut ModelStats,
    ) -> Result<(), ApiError> {
        let mut found = vec![];
        visit_ranks(request, &mut |r| {
            if let RankBy::UnsupportedEmbed {
                attribute,
                expression,
                ..
            } = r
            {
                found.push((attribute.clone(), expression.clone()));
            }
        });
        if found.is_empty() {
            return Ok(());
        }
        let schema = self
            .schema_of(ns)
            .await?
            .ok_or_else(|| ApiError::not_found(ns))?;
        let start = Instant::now();
        let mut vectors = std::collections::VecDeque::new();
        for (attribute, expression) in &found {
            let (target, model, dims, text) = resolve_expression(&schema, attribute, expression)?;
            let out = models
                .embed(&model, &[text], InputKind::Query, Some(dims))
                .await?;
            stats.tokens += out.tokens;
            vectors.push_back((target, out.vectors.into_iter().next().expect("one vector")));
        }
        stats.embedded = true;
        stats.embed_ms += start.elapsed().as_millis() as u64;
        visit_ranks(request, &mut |r| {
            if let RankBy::UnsupportedEmbed { exact, .. } = r {
                let (attribute, vector) = vectors.pop_front().expect("one vector per expression");
                *r = RankBy::Vector {
                    attribute,
                    exact: *exact,
                    wire_vector: json!(vector),
                    vector,
                };
            }
        });
        Ok(())
    }

    /// Query entry point: embed, run, and rerank.
    pub(crate) async fn query_with_models(
        &self,
        models: Arc<Models>,
        ns: &str,
        mut request: QueryRequest,
    ) -> Result<(QueryResponse, QueryDiagnostics), ApiError> {
        let mut stats = ModelStats::default();
        self.resolve_embeds(&models, ns, &mut request, &mut stats)
            .await?;
        let rerank = request
            .rerank_by
            .as_ref()
            .filter(|v| v.is_object())
            .cloned();
        let Some(rerank) = rerank else {
            let (mut response, diag) = self.query_inner(ns, request).await?;
            stats.query(&mut response.performance);
            return Ok((response, diag));
        };
        let spec = RerankSpec::parse(&rerank, &models)?;
        if request.aggregate_by.is_some() || request.group_by.is_some() {
            return Err(ApiError::bad(
                "aggregation queries are not supported with model reranking",
            ));
        }
        let multi = request.queries.is_some();
        let mut inner = request.clone();
        inner.rerank_by = None;
        let wanted = if multi {
            // The root limit is optional; without one, every deduplicated candidate is returned.
            let total = request.total().unwrap_or(10_000);
            if total == 0 || total > 10_000 || request.offset.saturating_add(total) > 10_000 {
                return Err(ApiError::bad(
                    "limit and offset must fit within 10000 results",
                ));
            }
            inner.limit = None;
            inner.offset = 0;
            let mut any = false;
            for q in inner.queries.iter_mut().flatten() {
                any |= expose(q, &spec.attribute);
            }
            any
        } else {
            expose(&mut inner, &spec.attribute)
        };
        let (mut response, diag) = self.query_inner(ns, inner).await?;
        let candidates: Vec<Document> = if multi {
            let mut seen = std::collections::HashSet::new();
            let mut merged = vec![];
            for result in response.results.take().into_iter().flatten() {
                for row in result.rows.unwrap_or_default() {
                    if seen.insert(row.get("id").map(Value::to_string)) {
                        merged.push(row);
                    }
                }
            }
            merged
        } else {
            response.result.rows.take().unwrap_or_default()
        };
        let start = Instant::now();
        let mut rows = spec.rerank(&models, candidates).await?;
        stats.rerank_ms = Some(start.elapsed().as_millis() as u64);
        if multi {
            rows = rows
                .into_iter()
                .skip(request.offset)
                .take(request.total().unwrap_or(10_000))
                .collect();
        }
        if !wanted {
            for r in &mut rows {
                r.remove(&spec.attribute);
            }
        }
        let result = SingleQueryResult {
            rows: Some(rows),
            ..Default::default()
        };
        if multi {
            response.results = Some(vec![result]);
        } else {
            response.result = result;
        }
        stats.query(&mut response.performance);
        Ok((response, diag))
    }
}

/// A parsed model `rerank_by`.
struct RerankSpec {
    model: String,
    attribute: String,
    query: String,
}
impl RerankSpec {
    fn parse(v: &Value, models: &Models) -> Result<Self, ApiError> {
        let o = v.as_object().expect("object");
        if let Some(k) = o
            .keys()
            .find(|k| !matches!(k.as_str(), "model" | "attribute" | "query"))
        {
            return Err(ApiError::bad(format!("unknown rerank_by field '{k}'")));
        }
        let get = |k: &str| {
            o.get(k)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| ApiError::bad(format!("rerank_by requires a string '{k}'")))
        };
        let model = get("model")?;
        if !models.has_rerank(&model) {
            return Err(ApiError::bad(format!(
                "unknown or unconfigured rerank model '{model}'"
            )));
        }
        Ok(Self {
            model,
            attribute: get("attribute")?,
            query: get("query")?,
        })
    }

    /// Score candidates and sort them by score, highest first. Candidates with a missing or null
    /// attribute are not sent to the model and score zero. `$dist` becomes the score.
    async fn rerank(
        &self,
        models: &Models,
        mut rows: Vec<Document>,
    ) -> Result<Vec<Document>, ApiError> {
        let mut docs = vec![];
        let mut at = vec![];
        for (i, r) in rows.iter().enumerate() {
            if let Some(t) = r.get(&self.attribute).and_then(Value::as_str) {
                docs.push(t.to_string());
                at.push(i);
            }
        }
        let scored = models.rerank(&self.model, &self.query, &docs).await?;
        let mut scores = vec![0.0f64; rows.len()];
        for (i, s) in at.into_iter().zip(scored.scores) {
            scores[i] = f64::from(s);
        }
        let mut order: Vec<usize> = (0..rows.len()).collect();
        order.sort_by(|a, b| scores[*b].total_cmp(&scores[*a]));
        let mut out = Vec::with_capacity(rows.len());
        for i in order {
            let mut row = std::mem::take(&mut rows[i]);
            row.insert("$dist".into(), json!(scores[i]));
            out.push(row);
        }
        Ok(out)
    }
}

/// Make a query return `attribute`; true when the caller already asked for it.
fn expose(q: &mut QueryRequest, attribute: &str) -> bool {
    let excluded = q
        .exclude_attributes
        .as_ref()
        .is_some_and(|e| e.iter().any(|a| a == attribute));
    let wanted = !excluded
        && match &q.include_attributes {
            Some(IncludeAttributes::All(all)) => *all,
            Some(IncludeAttributes::Names(n)) => n.iter().any(|a| a == attribute),
            None => false,
        };
    if let Some(e) = &mut q.exclude_attributes {
        e.retain(|a| a != attribute);
    }
    match &mut q.include_attributes {
        Some(IncludeAttributes::All(true)) => {}
        Some(IncludeAttributes::Names(n)) => {
            if !n.iter().any(|a| a == attribute) {
                n.push(attribute.into());
            }
        }
        other => *other = Some(IncludeAttributes::Names(vec![attribute.into()])),
    }
    wanted
}

/// Apply `f` to each rank expression in a query or its subqueries.
fn visit_ranks(request: &mut QueryRequest, f: &mut dyn FnMut(&mut RankBy)) {
    fn walk(r: &mut RankBy, f: &mut dyn FnMut(&mut RankBy)) {
        f(r);
        match r {
            RankBy::Sum(v) | RankBy::Max(v) => v.iter_mut().for_each(|x| walk(x, f)),
            RankBy::Product { expr, .. }
            | RankBy::Saturate { expr, .. }
            | RankBy::Decay { expr, .. }
            | RankBy::Dist { expr, .. } => walk(expr, f),
            _ => {}
        }
    }
    if let Some(r) = &mut request.rank_by {
        walk(r, f);
    }
    for q in request.queries.iter_mut().flatten() {
        visit_ranks(q, f);
    }
}

/// Work out which vector attribute, model, and dimensions an `Embed` expression uses, and the
/// text to embed.
fn resolve_expression(
    schema: &Schema,
    attribute: &str,
    expression: &Value,
) -> Result<(String, String, usize, String), ApiError> {
    let parts = expression.as_array().map(Vec::as_slice).unwrap_or_default();
    let text = match parts {
        [_, Value::String(text)] | [_, Value::String(text), _] => text.clone(),
        _ => return Err(ApiError::malformed("Embed requires a query string")),
    };
    let explicit = match parts.get(2) {
        None => None,
        Some(o) => {
            let o = o
                .as_object()
                .filter(|o| o.keys().all(|k| k == "model"))
                .ok_or_else(|| ApiError::malformed("Embed options accept only 'model'"))?;
            Some(
                o.get("model")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ApiError::malformed("Embed model must be a string"))?
                    .to_string(),
            )
        }
    };
    let attr = schema
        .get(attribute)
        .ok_or_else(|| ApiError::bad(format!("unknown attribute '{attribute}'")))?;
    let vector_dims = |name: &str| match schema.get(name).and_then(|s| s.data_type.clone()) {
        Some(AttributeType::Vector { dimensions, .. }) => Ok(dimensions),
        _ => Err(ApiError::bad(format!(
            "'{name}' is not a dense vector attribute"
        ))),
    };
    if let Some(spec) = attr.embed_spec() {
        if explicit.as_ref().is_some_and(|m| *m != spec.model) {
            return Err(ApiError::bad(format!(
                "attribute '{attribute}' is embedded with '{}'; Embed cannot use a different model",
                spec.model
            )));
        }
        let target = spec.target(attribute);
        let dims = vector_dims(&target)?;
        return Ok((target, spec.model, dims, text));
    }
    let dims = vector_dims(attribute).map_err(|_| {
        ApiError::bad(format!(
            "Embed needs a vector attribute or a string attribute with an embed setting, but '{attribute}' is neither"
        ))
    })?;
    let model = explicit
        .or_else(|| {
            schema.iter().find_map(|(name, s)| {
                let spec = s.embed_spec()?;
                (spec.target(name) == attribute).then_some(spec.model)
            })
        })
        .ok_or_else(|| ApiError::bad("Embed on a vector attribute requires a model"))?;
    Ok((attribute.into(), model, dims, text))
}

/// Validate the embed settings in `schema`, add any missing target vector columns to both
/// `schema` and the write, and list what to embed.
fn plan_targets(
    models: &Models,
    schema: &mut Schema,
    request: &mut WriteRequest,
    new_namespace: bool,
) -> Result<Vec<Target>, ApiError> {
    let specs: Vec<(String, EmbedSpec)> = schema
        .iter()
        .filter_map(|(n, a)| a.embed_spec().map(|s| (n.clone(), s)))
        .collect();
    if specs.len() > MAX_EMBEDDED_ATTRIBUTES {
        return Err(ApiError::bad(format!(
            "a namespace may have at most {MAX_EMBEDDED_ATTRIBUTES} embedded attributes"
        )));
    }
    let mut out = vec![];
    let mut created = false;
    for (source, spec) in specs {
        let info = models.embedding_info(&spec.model)?;
        let dims = spec.dims.unwrap_or(info.dims);
        if dims != info.dims && !info.allowed_dims.contains(&dims) {
            return Err(ApiError::bad(format!(
                "dims {dims} is not supported by model '{}'",
                spec.model
            )));
        }
        let target = spec.target(&source);
        match schema.get(&target).and_then(|s| s.data_type.as_ref()) {
            Some(AttributeType::Vector { dimensions, .. }) if *dimensions == dims => {}
            Some(AttributeType::Vector { dimensions, .. }) => {
                return Err(ApiError::bad(format!(
                    "embed target '{target}' has {dimensions} dimensions but model '{}' produces {dims}",
                    spec.model
                )));
            }
            Some(_) => {
                return Err(ApiError::bad(format!(
                    "embed target '{target}' must be a dense vector attribute"
                )));
            }
            None => {
                let ty = AttributeType::Vector {
                    dimensions: dims,
                    f16: spec.dtype == EmbedDtype::F16,
                };
                schema.insert(target.clone(), AttributeSchema::new(ty.clone()));
                request
                    .schema
                    .get_or_insert_with(Default::default)
                    .insert(target.clone(), SchemaInput::Type(ty));
                created = true;
            }
        }
        out.push(Target {
            source,
            model: spec.model,
            dims,
            target,
        });
    }
    if created && new_namespace && request.distance_metric.is_none() {
        request.distance_metric = Some(DistanceMetric::CosineDistance);
    }
    Ok(out)
}
