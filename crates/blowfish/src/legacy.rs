//! Translation of the pre-v2 wire protocol into the current engine API.
use super::*;
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use std::collections::BTreeSet;

fn object(value: Value) -> Result<serde_json::Map<String, Value>, HttpError> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::malformed("request must be an object").into())
}
fn deserialize<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, HttpError> {
    serde_json::from_value(value).map_err(|_| {
        ApiError::malformed("failed to deserialize the JSON body into the target type").into()
    })
}
fn filter(value: Value) -> Result<Value, HttpError> {
    let Value::Object(attributes) = value else {
        return Ok(value);
    };
    let mut terms = Vec::new();
    for (name, conditions) in attributes {
        for condition in conditions
            .as_array()
            .ok_or_else(|| ApiError::malformed("legacy filter conditions must be arrays"))?
        {
            let condition = condition
                .as_array()
                .filter(|a| a.len() == 2)
                .ok_or_else(|| {
                    ApiError::malformed("legacy filter condition requires an operator and value")
                })?;
            terms.push(json!([name, condition[0], condition[1]]));
        }
    }
    Ok(json!(["And", terms]))
}
fn flatten_row(value: Value) -> Result<Document, HttpError> {
    let mut row = object(value)?;
    let attributes = row.remove("attributes");
    if let Some(attributes) = attributes.filter(|v| !v.is_null()) {
        for (name, value) in object(attributes)? {
            if name == "id" || name == "vector" {
                return Err(ApiError::bad("legacy attributes cannot contain id or vector").into());
            }
            row.insert(name, value);
        }
    }
    Ok(row.into_iter().collect())
}
fn translate_write(value: Value) -> Result<WriteRequest, HttpError> {
    let mut input = object(value)?;
    let mut rows = Vec::new();
    let mut deletes = Vec::new();
    if let Some(ids) = input.remove("ids") {
        let ids = ids
            .as_array()
            .ok_or_else(|| ApiError::malformed("ids must be an array"))?;
        let vectors = input.remove("vectors").unwrap_or(Value::Null);
        let vectors = vectors.as_array().ok_or_else(|| {
            ApiError::bad("vectors must contain one value per id; use null entries for deletes")
        })?;
        if ids.len() != vectors.len() {
            return Err(ApiError::bad("column lengths must match ids").into());
        }
        let attributes = input
            .remove("attributes")
            .filter(|v| !v.is_null())
            .map(object)
            .transpose()?
            .unwrap_or_default();
        for column in attributes.values() {
            if column.as_array().is_none_or(|v| v.len() != ids.len()) {
                return Err(ApiError::bad("column lengths must match ids").into());
            }
        }
        if attributes.contains_key("id") || attributes.contains_key("vector") {
            return Err(ApiError::bad("legacy attributes cannot contain id or vector").into());
        }
        for (index, (id, vector)) in ids.iter().zip(vectors).enumerate() {
            if vector.is_null() {
                deletes.push(id.clone());
                continue;
            }
            let mut row: Document = attributes
                .iter()
                .map(|(k, v)| (k.clone(), v[index].clone()))
                .collect();
            row.insert("id".into(), id.clone());
            row.insert("vector".into(), vector.clone());
            rows.push(row);
        }
    }
    if let Some(upserts) = input.remove("upserts") {
        for row in upserts
            .as_array()
            .ok_or_else(|| ApiError::malformed("upserts must be an array"))?
        {
            let row = flatten_row(row.clone())?;
            if row.get("vector").is_some_and(Value::is_null) {
                deletes.push(
                    row.get("id")
                        .cloned()
                        .ok_or_else(|| ApiError::bad("row is missing id"))?,
                );
            } else {
                rows.push(row);
            }
        }
    }
    if !rows.is_empty() {
        input.insert("upsert_rows".into(), json!(rows));
    }
    if !deletes.is_empty() {
        input.insert("deletes".into(), json!(deletes));
    }
    if let Some(value) = input.remove("delete_by_filter").filter(|v| !v.is_null()) {
        input.insert("delete_by_filter".into(), filter(value)?);
    }
    deserialize(Value::Object(input))
}
pub(super) async fn write(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
    Extension(p): Extension<Principal>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<WriteResponse>, HttpError> {
    let request = translate_write(body(input)?)?;
    operations::authorize_copy(&s, &p, &request)?;
    Ok(Json(s.engine.write(&ns, request).await?))
}
fn translate_query(value: Value) -> Result<(QueryRequest, bool, bool), HttpError> {
    let mut input = object(value)?;
    // The old SDK serializes absent optional fields as null.
    input.retain(|_, v| !v.is_null());
    let include_vectors = input
        .remove("include_vectors")
        .map(|v| {
            v.as_bool()
                .ok_or_else(|| ApiError::malformed("include_vectors must be a boolean"))
        })
        .transpose()?
        .unwrap_or(false);
    if let Some(encoding) = input.remove("vector_encoding_format") {
        input.insert("vector_encoding".into(), encoding);
    }
    if let Some(vector) = input.remove("vector") {
        input
            .entry("rank_by")
            .or_insert(json!(["vector", "ANN", vector]));
    }
    input.entry("rank_by").or_insert(json!(["id", "asc"]));
    input.entry("top_k").or_insert(json!(10));
    if let Some(value) = input.remove("filters") {
        input.insert("filters".into(), filter(value)?);
    }
    let original = input.remove("include_attributes").unwrap_or(json!(false));
    let include_attributes = original != json!(false);
    match original {
        Value::Bool(true) => {
            input.insert("include_attributes".into(), json!(true));
        }
        Value::Bool(false) => {
            input.insert(
                "include_attributes".into(),
                json!(if include_vectors {
                    vec!["vector"]
                } else {
                    vec![]
                }),
            );
        }
        Value::Array(mut names) => {
            // The vector flag controls the reserved vector column independently of attributes.
            names.retain(|v| v != "vector");
            if include_vectors {
                names.push(json!("vector"));
            }
            input.insert("include_attributes".into(), Value::Array(names));
        }
        _ => {
            return Err(
                ApiError::malformed("include_attributes must be a boolean or array").into(),
            );
        }
    }
    Ok((
        deserialize(Value::Object(input))?,
        include_vectors,
        include_attributes,
    ))
}
fn nested_rows(rows: Vec<Document>, vectors: bool, attributes: bool) -> Vec<Value> {
    rows.into_iter()
        .map(|mut row| {
            let mut result = serde_json::Map::new();
            result.insert("id".into(), row.remove("id").expect("engine row id"));
            if let Some(distance) = row.remove("$dist") {
                result.insert("dist".into(), distance);
            }
            let vector = row.remove("vector");
            if vectors && let Some(vector) = vector {
                result.insert("vector".into(), vector);
            }
            if attributes {
                result.insert("attributes".into(), json!(row));
            }
            Value::Object(result)
        })
        .collect()
}
pub(super) async fn query(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, HttpError> {
    let (request, vectors, attributes) = translate_query(body(input)?)?;
    let response = s.engine.query(&ns, request).await?;
    Ok(Json(json!(nested_rows(
        response.result.rows.unwrap_or_default(),
        vectors,
        attributes
    ))))
}
#[derive(Default, Deserialize)]
pub(super) struct ExportParams {
    cursor: Option<String>,
    #[serde(default = "export_page_size")]
    page_size: usize,
}
fn export_page_size() -> usize {
    1000
}
pub(super) async fn export(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
    params: Result<Query<ExportParams>, QueryRejection>,
) -> Result<Json<Value>, HttpError> {
    let Query(params) = params.map_err(|_| ApiError::bad("invalid export parameters"))?;
    if !(1..=1000).contains(&params.page_size) {
        return Err(ApiError::bad("page_size must be between 1 and 1000").into());
    }
    let after = params
        .cursor
        .map(|cursor| {
            let bytes = BASE64_URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ApiError::bad("invalid export cursor"))?;
            let (namespace, id): (String, Id) = serde_json::from_slice(&bytes)
                .map_err(|_| ApiError::bad("invalid export cursor"))?;
            if namespace != ns {
                return Err(ApiError::bad("invalid export cursor"));
            }
            Ok(id)
        })
        .transpose()?;
    let request = deserialize(
        json!({"rank_by":["id","asc"],"top_k":params.page_size+1,"include_attributes":true,"filters":after.map(|id|json!(["id","Gt",id]))}),
    )?;
    let mut rows = s
        .engine
        .query(&ns, request)
        .await?
        .result
        .rows
        .unwrap_or_default();
    let next = if rows.len() > params.page_size {
        rows.pop();
        Some(
            BASE64_URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&json!([ns, rows.last().expect("nonempty page")["id"]]))
                    .expect("cursor serializes"),
            ),
        )
    } else {
        None
    };
    let names: BTreeSet<_> = rows
        .iter()
        .flat_map(|r| r.keys())
        .filter(|k| !matches!(k.as_str(), "id" | "vector" | "$dist"))
        .cloned()
        .collect();
    let attributes: serde_json::Map<_, _> = names
        .into_iter()
        .map(|name| {
            let column = rows
                .iter()
                .map(|row| row.get(&name).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>();
            (name, json!(column))
        })
        .collect();
    Ok(Json(
        json!({"ids":rows.iter().map(|r| &r["id"]).collect::<Vec<_>>(),"vectors":rows.iter().map(|r| r.get("vector").cloned().unwrap_or(Value::Null)).collect::<Vec<_>>(),"attributes":attributes,"next_cursor":next,"message":null}),
    ))
}
pub(super) async fn head(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
) -> Result<Response, HttpError> {
    let metadata = s.engine.metadata(&ns).await?;
    let dimensions = metadata["schema"]["vector"]["type"]
        .as_str()
        .and_then(|t| {
            t.strip_prefix('[')?
                .split(']')
                .next()?
                .parse::<usize>()
                .ok()
        })
        .unwrap_or(0);
    let mut response = StatusCode::OK.into_response();
    for (name, value) in [
        ("x-turbopuffer-dimensions", dimensions.to_string()),
        (
            "x-turbopuffer-approx-num-vectors",
            metadata["approx_row_count"].to_string(),
        ),
        (
            "x-turbopuffer-created-at",
            metadata["created_at"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        ),
        ("x-turbopuffer-encryption", "sse".into()),
        ("x-turbopuffer-index-version", "None".into()),
    ] {
        response.headers_mut().insert(
            name,
            HeaderValue::from_str(&value).expect("metadata header"),
        );
    }
    Ok(response)
}
pub(super) async fn delete(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<Value>, HttpError> {
    s.engine.delete_namespace(&ns).await?;
    Ok(Json(json!({"status":"ok"})))
}
