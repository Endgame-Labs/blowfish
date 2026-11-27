//! Seek ID predicates in the sorted document column instead of decoding a bitmap per ID.
use super::*;
use bf_types::FilterOp;

/// ID rows are in ascending order. A scalar comparison therefore selects one row interval
/// (or its complement), which can be intersected with the caller's live rows directly.
pub(super) async fn evaluate(
    store: &Storage,
    segment: &Segment,
    op: FilterOp,
    value: &Value,
    schema: &Schema,
    live: &RoaringBitmap,
) -> Result<Option<RoaringBitmap>, ApiError> {
    use FilterOp::*;
    if !matches!(op, Eq | NotEq | Lt | Lte | Gt | Gte) {
        return Ok(None);
    }
    let operand = if let Some(t) = schema.get("id").and_then(|s| s.data_type.as_ref()) {
        crate::write::normalize_value(value, t)?
    } else {
        value.clone()
    };
    if live.is_empty() {
        return Ok(Some(RoaringBitmap::new()));
    }
    let bounds = bounds(store, segment).await?;
    // Only the boundary that the operator needs is sought. Min/max checks eliminate whole
    // segments before their docs directory, ID column, or ID dictionary is opened.
    let low = if matches!(op, Eq | NotEq | Lt | Gte) {
        boundary(store, segment, &operand, false, bounds.as_ref()).await?
    } else {
        0
    };
    let high = if matches!(op, Eq | NotEq | Lte | Gt) {
        boundary(store, segment, &operand, true, bounds.as_ref()).await?
    } else {
        segment.row_count
    };
    let range = match op {
        Eq | NotEq => low..high,
        Lt => 0..low,
        Lte => 0..high,
        Gt => high..segment.row_count,
        Gte => low..segment.row_count,
        _ => unreachable!(),
    };
    let mut rows = RoaringBitmap::new();
    rows.insert_range(range);
    Ok(Some(if op == NotEq {
        live - rows
    } else {
        rows & live
    }))
}

/// Read just the existing Bloom object's range header; the Bloom bits are irrelevant to ranges.
async fn bounds(store: &Storage, segment: &Segment) -> Result<Option<(Value, Value)>, ApiError> {
    let Some(file) = segment.files.get("idfilter") else {
        return Ok(None);
    };
    let key = format!("id-bounds:{}", file.key);
    if let Some(bounds) = read_cache::get::<(Value, Value)>(&key, 0) {
        return Ok(Some((*bounds).clone()));
    }
    let reader = ObjectReader::new(store, file);
    let header = reader
        .read_batch_sparse(std::iter::once(0..file.size.min(13)).collect())
        .await
        .map_err(index_error)?;
    let header = &header[0];
    if header.len() != 13 || &header[..8] != ID_FILTER_MAGIC {
        return Err(corrupt());
    }
    let len = u32::from_le_bytes(header[9..13].try_into().map_err(index_error)?) as u64;
    let end = 13u64
        .checked_add(len)
        .filter(|n| *n <= file.size)
        .ok_or_else(corrupt)?;
    let bytes = reader
        .read_batch_sparse(std::iter::once(13..end).collect())
        .await
        .map_err(index_error)?;
    let (min, max): (Id, Id) = serde_json::from_slice(&bytes[0]).map_err(index_error)?;
    if min > max {
        return Err(corrupt());
    }
    let bounds = (min.value(), max.value());
    read_cache::insert(&key, len + 128, bounds.clone());
    Ok(Some(bounds))
}

/// First row >= operand (or > operand for an inclusive upper boundary). Document hydration
/// validates CRCs and caches decoded blocks, so seeks within one block do no further I/O.
async fn boundary(
    store: &Storage,
    segment: &Segment,
    operand: &Value,
    inclusive: bool,
    bounds: Option<&(Value, Value)>,
) -> Result<u32, ApiError> {
    let before = |v: &Value| {
        let cmp = filter::compare(v, operand);
        cmp.is_lt() || (inclusive && cmp.is_eq())
    };
    if let Some((min, max)) = bounds {
        if !before(min) {
            return Ok(0);
        }
        if before(max) {
            return Ok(segment.row_count);
        }
    }
    let columns = std::collections::BTreeSet::from(["id".to_string()]);
    let (mut low, mut high) = (0, segment.row_count);
    while low < high {
        let mid = low + (high - low) / 2;
        let docs = projected_rows(
            store,
            segment,
            &RoaringBitmap::from_iter([mid]),
            Some(&columns),
        )
        .await?;
        let value = docs
            .get(&mid)
            .and_then(|d| d.get("id"))
            .ok_or_else(corrupt)?;
        if before(value) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn seeks_match_document_evaluation_for_every_id_type_and_live_holes() {
        for kind in ["uint", "string", "uuid"] {
            let store = Storage::open(&bf_storage::StorageConfig {
                url: "memory://".into(),
                ..Default::default()
            })
            .await
            .unwrap();
            let schema: Schema = serde_json::from_value(json!({"id": {"type":kind}})).unwrap();
            let values: Vec<Value> = (0..1100u64)
                .map(|i| match kind {
                    "uint" => json!(u64::MAX - 4000 + i * 3),
                    "string" => json!(format!("doc-{:04}-é", i * 3)),
                    _ => json!(uuid::Uuid::from_u128((i * 3) as u128).to_string()),
                })
                .collect();
            let docs: BTreeMap<Id, Document> = values
                .iter()
                .map(|v| {
                    (
                        serde_json::from_value(v.clone()).unwrap(),
                        Document::from_iter([("id".into(), v.clone())]),
                    )
                })
                .collect();
            let mut segment = build_with(
                &store,
                "ids",
                &schema,
                bf_types::DistanceMetric::CosineDistance,
                &docs,
                BTreeMap::new(),
                1,
                1,
            )
            .await
            .unwrap();
            // The implementation must not need either full dictionary.
            segment.files.remove("ids");
            segment.files.remove("filters");
            let live: RoaringBitmap = (0..1100).filter(|i| i % 7 != 0).collect();
            for legacy in [false, true] {
                if legacy {
                    segment.files.remove("idfilter");
                }
                for operand in [
                    Value::Null,
                    values[0].clone(),
                    values[255].clone(),
                    values[256].clone(),
                    values[1099].clone(),
                ] {
                    for op in ["Eq", "NotEq", "Lt", "Lte", "Gt", "Gte"] {
                        let f: Filter = serde_json::from_value(json!(["id", op, operand])).unwrap();
                        let got = filter_rows(&store, &segment, &f, &schema, &live)
                            .await
                            .unwrap();
                        let expected: RoaringBitmap = docs
                            .values()
                            .enumerate()
                            .filter_map(|(i, doc)| {
                                (live.contains(i as u32)
                                    && filter::matches(&f, Some(doc), None, &schema).unwrap())
                                .then_some(i as u32)
                            })
                            .collect();
                        assert_eq!(got, expected, "{kind} {op} {operand} legacy={legacy}");
                    }
                }
            }
        }
    }
}
