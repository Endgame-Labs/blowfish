use crate::{NamespaceState, filter};
use bf_types::*;
use bf_wal::WriteBatch;
use chrono::Utc;
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub(crate) fn prepare(
    current: &NamespaceState,
    request: &WriteRequest,
    copy: Option<&NamespaceState>,
) -> Result<(WriteBatch, WriteResponse), ApiError> {
    for name in request.extra.keys() {
        if matches!(
            name.as_str(),
            "branch_from_namespace" | "encryption" | "sharding" | "disable_backpressure"
        ) {
            return Err(ApiError::bad(format!(
                "{name} is not supported by blowfish yet"
            )));
        }
    }
    if copy.is_some()
        && (request.schema.is_some()
            || request.upsert_rows.is_some()
            || request.upsert_columns.is_some()
            || request.patch_rows.is_some()
            || request.patch_columns.is_some()
            || request.deletes.is_some()
            || request.delete_by_filter.is_some()
            || request.patch_by_filter.is_some())
    {
        return Err(ApiError::bad(
            "copy cannot be combined with writes or schema changes",
        ));
    }
    if let Some(source) = copy {
        if current.exists {
            return Err(ApiError::bad("destination namespace already exists"));
        }
        let now = Utc::now();
        let batch = WriteBatch {
            upserts: source.docs.values().cloned().collect(),
            deletes: vec![],
            schema: source.schema.clone(),
            distance_metric: source.metric,
            created_at: now,
            updated_at: now,
        };
        let response = WriteResponse {
            message: "namespace cloned successfully".into(),
            rows_affected: batch.upserts.len(),
            ..Default::default()
        };
        return Ok((batch, response));
    }
    if !current.exists && request.create_namespace == Some(false) {
        return Err(ApiError::new(404, "namespace was not found"));
    }
    let mut state = current.clone();
    if let Some(metric) = request.distance_metric {
        if current.exists
            && current.metric != metric
            && current.schema.values().any(|s| {
                s.data_type
                    .as_ref()
                    .is_some_and(|t| t.dimensions().is_some())
            })
        {
            return Err(ApiError::bad("distance metric cannot change"));
        }
        state.metric = metric;
    }
    if let Some(changes) = &request.schema {
        for (name, change) in changes {
            validate_name(name)?;
            let mut cfg = change.expanded();
            if cfg.drop {
                if name == "id" {
                    return Err(ApiError::bad("cannot drop id"));
                }
                if state.schema.get(name).is_some_and(|s| {
                    s.data_type
                        .as_ref()
                        .is_some_and(|t| t.dimensions().is_some())
                }) {
                    return Err(ApiError::bad("cannot drop a vector column"));
                }
                state.schema.remove(name);
                for doc in state.docs.values_mut() {
                    doc.remove(name);
                }
                continue;
            }
            if let Some(old) = state.schema.get(name) {
                if cfg.data_type.is_some() && cfg.data_type != old.data_type {
                    return Err(ApiError::bad(format!("cannot change type of '{name}'")));
                }
                let mut merged = old.clone();
                macro_rules! merge{($($field:ident),*)=>{$(if cfg.$field.is_some(){merged.$field=cfg.$field.take();})*};}
                merge!(
                    data_type,
                    filterable,
                    full_text_search,
                    ann,
                    regex,
                    glob,
                    fuzzy,
                    embed,
                    sparse_knn
                );
                cfg = merged;
            }
            cfg.normalize(name, state.metric)?;
            state.schema.insert(name.clone(), cfg);
        }
    }
    let mut upserts = request.upsert_rows.clone().unwrap_or_default();
    let mut patches = request.patch_rows.clone().unwrap_or_default();
    if let Some(columns) = &request.upsert_columns {
        upserts.extend(columns_to_rows(columns, &state.schema)?);
    }
    if let Some(columns) = &request.patch_columns {
        patches.extend(columns_to_rows(columns, &state.schema)?);
    }
    if !current.exists && upserts.is_empty() && request.create_namespace != Some(true) {
        return Err(ApiError::bad(
            "first write must contain documents or create_namespace: true",
        ));
    }
    if request.create_namespace == Some(true)
        && !state.schema.contains_key("id")
        && upserts.is_empty()
    {
        return Err(ApiError::bad(
            "create_namespace requires the id type in schema",
        ));
    }
    let mut seen = BTreeSet::new();
    for row in upserts.iter_mut().chain(patches.iter_mut()) {
        let id = normalize_id(
            row.get("id")
                .ok_or_else(|| ApiError::bad("row is missing id"))?,
            &mut state.schema,
        )?;
        if !seen.insert(id.clone()) {
            return Err(ApiError::bad("duplicate document ID in write request"));
        }
        row.insert("id".into(), id.value());
    }
    let mut deletes = Vec::new();
    for id in request.deletes.as_deref().unwrap_or_default() {
        let id = normalize_id(&id.value(), &mut state.schema)?;
        if !seen.insert(id.clone()) {
            return Err(ApiError::bad("duplicate document ID in write request"));
        }
        deletes.push(id);
    }
    // Infer all types before validating rows, so an empty array can use a later non-empty value.
    for row in upserts
        .iter()
        .chain(patches.iter())
        .chain(request.patch_by_filter.iter().map(|p| &p.patch))
    {
        for (name, value) in row {
            validate_name(name)?;
            if value.is_null() || state.schema.contains_key(name) {
                continue;
            }
            if value.as_array().is_some_and(Vec::is_empty) && name != "vector" {
                continue;
            }
            let mut cfg = AttributeSchema::new(AttributeType::infer(name, value)?);
            cfg.normalize(name, state.metric)?;
            state.schema.insert(name.clone(), cfg);
        }
    }
    for cfg in state.schema.values() {
        if let Some(config) = cfg.text_config() {
            config.validate().map_err(ApiError::bad)?;
        }
    }
    if !current.exists
        && request.distance_metric.is_none()
        && state.schema.values().any(|s| {
            s.data_type
                .as_ref()
                .is_some_and(|t| t.dimensions().is_some())
        })
    {
        return Err(ApiError::bad(
            "distance_metric is required when creating vector columns",
        ));
    }
    if state.schema.len() > 1024 {
        return Err(ApiError::bad("namespace exceeds 1024 attributes"));
    }
    if state
        .schema
        .values()
        .filter(|s| {
            s.data_type
                .as_ref()
                .is_some_and(|t| t.dimensions().is_some())
        })
        .count()
        > 8
    {
        return Err(ApiError::bad("namespace exceeds eight vector columns"));
    }
    if current.exists {
        for (name, cfg) in &state.schema {
            if cfg
                .data_type
                .as_ref()
                .is_some_and(|t| t.dimensions().is_some())
                && !current.schema.contains_key(name)
            {
                return Err(ApiError::bad(
                    "vector columns must be declared at namespace creation",
                ));
            }
        }
    }
    for row in &mut upserts {
        row.retain(|_, v| !v.is_null());
        normalize_document(row, &state.schema)?;
        for (name, cfg) in &state.schema {
            if cfg
                .data_type
                .as_ref()
                .is_some_and(|t| t.dimensions().is_some())
                && row.get(name).is_none_or(Value::is_null)
            {
                return Err(ApiError::bad(format!(
                    "upsert is missing vector attribute '{name}'"
                )));
            }
        }
    }
    for row in &mut patches {
        reject_vector_patch(row, &state.schema)?;
        normalize_document(row, &state.schema)?;
    }
    let patch_by_filter = if let Some(p) = &request.patch_by_filter {
        if p.patch.contains_key("id") {
            return Err(ApiError::bad("patch_by_filter cannot change id"));
        }
        let mut patch = p.patch.clone();
        reject_vector_patch(&patch, &state.schema)?;
        normalize_document(&mut patch, &state.schema)?;
        Some((patch, &p.filters))
    } else {
        None
    };
    for f in [
        &request.delete_by_filter,
        &request.upsert_condition,
        &request.patch_condition,
        &request.delete_condition,
    ]
    .into_iter()
    .flatten()
    {
        filter::validate(f, &state.schema, true)?;
    }
    if let Some((_, f)) = &patch_by_filter {
        filter::validate(f, &state.schema, false)?;
    }
    let mut response = WriteResponse::default();
    let mut upserted = vec![];
    let mut patched = vec![];
    let mut deleted = vec![];
    let mut remaining = false;
    // Explicit write phases follow turbopuffer's ordering.
    if let Some(f) = &request.delete_by_filter {
        let ids = matching_ids(&state, f)?;
        if ids.len() > 5_000_000 && !request.delete_by_filter_allow_partial {
            return Err(ApiError::bad(
                "delete_by_filter exceeds five million rows; allow_partial is required",
            ));
        }
        remaining |= ids.len() > 5_000_000;
        for id in ids.into_iter().take(5_000_000) {
            state.docs.remove(&id);
            deleted.push(id);
        }
    }
    if let Some((patch, f)) = patch_by_filter {
        let ids = matching_ids(&state, f)?;
        if ids.len() > 50_000 && !request.patch_by_filter_allow_partial {
            return Err(ApiError::bad(
                "patch_by_filter exceeds 50000 rows; allow_partial is required",
            ));
        }
        remaining |= ids.len() > 50_000;
        for id in ids.into_iter().take(50_000) {
            let doc = state.docs.get_mut(&id).expect("matched row");
            apply_patch(doc, &patch);
            patched.push(id);
        }
    }
    for row in upserts {
        let id: Id = serde_json::from_value(row["id"].clone()).expect("validated ID");
        let allowed = match (state.docs.get(&id), &request.upsert_condition) {
            (Some(old), Some(f)) => filter::matches(f, Some(old), Some(&row), &state.schema)?,
            _ => true,
        };
        if allowed {
            state.docs.insert(id.clone(), row);
            upserted.push(id);
        }
    }
    for patch in patches {
        let id: Id = serde_json::from_value(patch["id"].clone()).expect("validated ID");
        if let Some(old) = state.docs.get(&id) {
            let allowed = if let Some(f) = &request.patch_condition {
                filter::matches(f, Some(old), Some(&patch), &state.schema)?
            } else {
                true
            };
            if allowed {
                apply_patch(state.docs.get_mut(&id).expect("existing row"), &patch);
                patched.push(id);
            }
        }
    }
    for id in deletes {
        if let Some(old) = state.docs.get(&id) {
            let allowed = if let Some(f) = &request.delete_condition {
                filter::matches(f, Some(old), None, &state.schema)?
            } else {
                true
            };
            if allowed {
                state.docs.remove(&id);
                deleted.push(id);
            }
        }
    }
    response.rows_affected = upserted.len() + patched.len() + deleted.len();
    response.rows_upserted = (!upserted.is_empty()).then_some(upserted.len());
    response.rows_patched = (!patched.is_empty()).then_some(patched.len());
    response.rows_deleted = (!deleted.is_empty()).then_some(deleted.len());
    response.rows_remaining = remaining.then_some(true);
    if request.upsert_condition.is_some()
        || request.patch_condition.is_some()
        || request.delete_condition.is_some()
        || request.delete_by_filter.is_some()
        || request.patch_by_filter.is_some()
    {
        response.billing.query = Some(QueryBilling::default());
    }
    if request.return_affected_ids {
        response.upserted_ids = upserted;
        response.patched_ids = patched;
        response.deleted_ids = deleted;
    }
    let now = Utc::now();
    let batch = WriteBatch {
        upserts: state
            .docs
            .iter()
            .filter(|(id, doc)| {
                current.docs.get(id).is_none_or(|old| {
                    // A schema drop hides stored columns; it must not duplicate every
                    // surviving vector into a giant schema-only WAL entry.
                    old.iter()
                        .filter(|(name, _)| state.schema.contains_key(*name))
                        .ne(doc.iter())
                })
            })
            .map(|(_, doc)| doc.clone())
            .collect(),
        deletes: current
            .docs
            .keys()
            .filter(|id| !state.docs.contains_key(id))
            .cloned()
            .collect(),
        schema: state.schema,
        distance_metric: state.metric,
        created_at: if current.exists {
            current.created_at
        } else {
            now
        },
        updated_at: now,
    };
    Ok((batch, response))
}
fn matching_ids(state: &NamespaceState, f: &Filter) -> Result<Vec<Id>, ApiError> {
    let mut out = vec![];
    for (id, doc) in &state.docs {
        if filter::matches(f, Some(doc), None, &state.schema)? {
            out.push(id.clone());
        }
    }
    Ok(out)
}
fn apply_patch(doc: &mut Document, patch: &Document) {
    for (k, v) in patch {
        if k == "id" {
            continue;
        }
        if v.is_null() {
            doc.remove(k);
        } else {
            doc.insert(k.clone(), v.clone());
        }
    }
}
fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > 128 || name.starts_with('$') {
        return Err(ApiError::bad(
            "attribute names must contain 1–128 bytes and cannot start with $",
        ));
    }
    Ok(())
}
fn normalize_id(value: &Value, schema: &mut Schema) -> Result<Id, ApiError> {
    let mut id: Id = serde_json::from_value(value.clone())
        .map_err(|_| ApiError::bad("id must be a uint, UUID, or string"))?;
    id.validate()?;
    if !schema.contains_key("id") {
        let t = match id {
            Id::Uint(_) => ScalarType::Uint,
            Id::String(_) => ScalarType::String,
        };
        schema.insert("id".into(), AttributeSchema::new(AttributeType::Scalar(t)));
    }
    match (&id, schema["id"].data_type.as_ref()) {
        (Id::Uint(_), Some(AttributeType::Scalar(ScalarType::Uint)))
        | (Id::String(_), Some(AttributeType::Scalar(ScalarType::String))) => {}
        (Id::String(s), Some(AttributeType::Scalar(ScalarType::Uuid))) => {
            id = Id::String(
                uuid::Uuid::parse_str(s)
                    .map_err(|_| ApiError::bad("invalid UUID id"))?
                    .to_string(),
            );
        }
        _ => {
            return Err(ApiError::bad(
                "namespace ID type does not match; mixed ID types are not allowed",
            ));
        }
    }
    Ok(id)
}
fn normalize_document(doc: &mut Document, schema: &Schema) -> Result<(), ApiError> {
    for (name, value) in doc {
        if value.is_null() {
            continue;
        }
        let cfg = schema.get(name).ok_or_else(|| {
            ApiError::bad(format!(
                "cannot infer type of '{name}'; declare it in schema"
            ))
        })?;
        *value = normalize_value(
            value,
            cfg.data_type.as_ref().expect("normalized schema type"),
        )?;
    }
    Ok(())
}
/// Validate and canonicalize a non-null attribute or filter operand.
pub(crate) fn normalize_value(value: &Value, t: &AttributeType) -> Result<Value, ApiError> {
    use base64::Engine as _;
    if value.is_null() {
        return Ok(Value::Null);
    }
    let mismatch = || ApiError::bad(format!("attribute value does not match type {t}"));
    match t {
        AttributeType::SparseVector => Ok(serde_json::from_str(
            &serde_json::to_string(&decode_sparse(value)?).expect("sparse vector serializes"),
        )
        .expect("valid JSON")),
        AttributeType::Vector { dimensions, f16 } => {
            let mut v = decode_vector(value)?;
            if v.len() != *dimensions {
                return Err(ApiError::bad(format!(
                    "invalid vector dimensions: expected {dimensions}, got {}",
                    v.len()
                )));
            }
            if *f16 {
                for n in &mut v {
                    *n = half::f16::from_f32(*n).to_f32();
                    if !n.is_finite() {
                        return Err(ApiError::bad("vector value exceeds f16 range"));
                    }
                }
            }
            Ok(json!(v))
        }
        AttributeType::Array(s) => {
            let values = value.as_array().ok_or_else(mismatch)?;
            Ok(Value::Array(
                values
                    .iter()
                    .map(|v| {
                        if v.is_null() {
                            Err(mismatch())
                        } else {
                            normalize_value(v, &AttributeType::Scalar(*s))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        AttributeType::Scalar(s) => Ok(match s {
            ScalarType::String => json!(value.as_str().ok_or_else(mismatch)?),
            ScalarType::Int => json!(value.as_i64().ok_or_else(mismatch)?),
            ScalarType::Uint => json!(value.as_u64().ok_or_else(mismatch)?),
            ScalarType::Float => json!(value.as_f64().ok_or_else(mismatch)?),
            ScalarType::Bool => json!(value.as_bool().ok_or_else(mismatch)?),
            ScalarType::Uuid => json!(
                uuid::Uuid::parse_str(value.as_str().ok_or_else(mismatch)?)
                    .map_err(|_| mismatch())?
                    .to_string()
            ),
            ScalarType::Datetime => {
                let ms = datetime_millis(value.as_str().ok_or_else(mismatch)?)?;
                json!(
                    chrono::DateTime::from_timestamp_millis(ms)
                        .ok_or_else(mismatch)?
                        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
                )
            }
            ScalarType::Bytes => {
                let v = value.as_str().ok_or_else(mismatch)?;
                base64::prelude::BASE64_STANDARD
                    .decode(v)
                    .map_err(|_| mismatch())?;
                json!(v)
            }
        }),
    }
}
fn columns_to_rows(columns: &Document, schema: &Schema) -> Result<Vec<Document>, ApiError> {
    let ids = columns
        .get("id")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::bad("columnar write requires an id array"))?;
    let count = ids.len();
    let mut rows: Vec<_> = ids
        .iter()
        .map(|id| {
            let mut d = Document::new();
            d.insert("id".into(), id.clone());
            d
        })
        .collect();
    for (name, values) in columns {
        if name == "id" {
            continue;
        }
        let dims = schema
            .get(name)
            .and_then(|s| s.data_type.as_ref())
            .and_then(AttributeType::dimensions);
        let flat = values
            .as_array()
            .is_some_and(|a| a.first().is_some_and(Value::is_number));
        if (dims.is_some() || name == "vector") && (values.is_string() || flat) {
            let vector = decode_vector(values)?;
            let width = dims
                .or_else(|| (count > 0 && vector.len() % count == 0).then(|| vector.len() / count))
                .ok_or_else(|| ApiError::bad("flat vector column has invalid dimensions"))?;
            if width == 0 || vector.len() != width * count {
                return Err(ApiError::bad(
                    "flat vector column length does not match id column",
                ));
            }
            for (row, chunk) in rows.iter_mut().zip(vector.chunks(width)) {
                row.insert(name.clone(), json!(chunk));
            }
            continue;
        }
        let values = values
            .as_array()
            .ok_or_else(|| ApiError::bad("columns must be arrays"))?;
        if values.len() != count {
            return Err(ApiError::bad("column lengths must match id column"));
        }
        for (row, value) in rows.iter_mut().zip(values) {
            row.insert(name.clone(), value.clone());
        }
    }
    Ok(rows)
}

fn reject_vector_patch(patch: &Document, schema: &Schema) -> Result<(), ApiError> {
    if patch.keys().any(|name| {
        schema
            .get(name)
            .and_then(|s| s.data_type.as_ref())
            .is_some_and(|t| t.dimensions().is_some())
    }) {
        return Err(ApiError::bad("vector attributes cannot be patched"));
    }
    Ok(())
}
