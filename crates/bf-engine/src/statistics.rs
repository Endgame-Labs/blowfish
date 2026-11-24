//! Bounded scans of overwritten rows. Dense float vectors have a fixed canonical JSON width;
//! a dense ANN membership map proves their presence without reading their payloads.
use crate::{NamespaceState, read_cache, segment, snapshot, text};
use bf_storage::Storage;
use bf_types::{ApiError, AttributeType, Schema};
use bf_wal::{NamespaceStats, Segment};
use futures::StreamExt;
use roaring::RoaringBitmap;
use std::collections::BTreeSet;
use std::sync::Arc;

/// The aggregate row bound across concurrent overwritten-document scans.
const SCAN_ROWS: usize = 4096;

/// Prepare live masks and positive removal statistics. Independent segment scans use at
/// most two blocking workers, sharing the original row bound instead of multiplying it.
pub(crate) async fn removals(
    store: &Storage,
    state: &NamespaceState,
    concurrency: usize,
) -> Result<(Vec<(RoaringBitmap, RoaringBitmap)>, NamespaceStats), ApiError> {
    if state.segments.is_empty() {
        return Ok((Vec::new(), NamespaceStats::default()));
    }
    let workers = concurrency.clamp(1, 2);
    let base = Arc::new(state.without_docs());
    let context = read_cache::context();
    let parts =
        futures::stream::iter(state.segments.iter().cloned().enumerate().map(|(i, seg)| {
            let store = store.clone();
            let base = base.clone();
            let context = context.clone();
            async move {
                let runtime = tokio::runtime::Handle::current();
                tokio::task::spawn_blocking(move || {
                    let scan = async {
                        let live = segment::live(&store, &seg).await?;
                        let keep = snapshot::allowed(&store, &base, &seg, None).await?;
                        let deleted = &live - &keep;
                        let mut removed = NamespaceStats::default();
                        adjust_bounded(
                            &store,
                            &seg,
                            &deleted,
                            &base.schema,
                            &mut removed,
                            true,
                            true,
                            SCAN_ROWS / workers,
                        )
                        .await?;
                        Ok::<_, ApiError>((i, live, keep, removed))
                    };
                    match context {
                        Some(context) => runtime.block_on(read_cache::scope(context, scan)),
                        None => runtime.block_on(scan),
                    }
                })
                .await
                .map_err(segment::index_error)?
            }
        }))
        .buffer_unordered(workers);
    futures::pin_mut!(parts);
    let mut masks = vec![(RoaringBitmap::new(), RoaringBitmap::new()); state.segments.len()];
    let mut removed = NamespaceStats::default();
    let mut failure = None;
    // Drain before returning an error: a running blocking scan cannot be cancelled, and
    // abandoning it would let a retry exceed the aggregate scan bound.
    while let Some(part) = parts.next().await {
        match part {
            Ok((i, live, keep, stats)) => {
                masks[i] = (live, keep);
                merge(&mut removed, stats, true);
            }
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok((masks, removed))
}

/// Combine already-counted documents without decoding or tokenizing them again.
pub(crate) fn merge(into: &mut NamespaceStats, from: NamespaceStats, add: bool) {
    let combine = |a: &mut u64, b: u64| {
        *a = if add {
            a.saturating_add(b)
        } else {
            a.saturating_sub(b)
        };
    };
    combine(&mut into.rows, from.rows);
    combine(&mut into.logical_bytes, from.logical_bytes);
    for (name, counts) in from.text {
        let field = into.text.entry(name).or_default();
        combine(&mut field.documents, counts.documents);
        combine(&mut field.total_length, counts.total_length);
        for (term, n) in counts.document_frequencies {
            if add {
                *field.document_frequencies.entry(term).or_default() += n;
            } else if let Some(count) = field.document_frequencies.get_mut(&term) {
                *count = count.saturating_sub(n);
                if *count == 0 {
                    field.document_frequencies.remove(&term);
                }
            }
        }
    }
}

pub(crate) async fn rows(
    store: &Storage,
    seg: &Segment,
    rows: &RoaringBitmap,
    schema: &Schema,
    with_text: bool,
) -> Result<NamespaceStats, ApiError> {
    let mut stats = NamespaceStats::default();
    adjust(store, seg, rows, schema, &mut stats, true, with_text).await?;
    Ok(stats)
}

pub(crate) async fn adjust(
    store: &Storage,
    seg: &Segment,
    rows: &RoaringBitmap,
    schema: &Schema,
    stats: &mut NamespaceStats,
    add: bool,
    with_text: bool,
) -> Result<(), ApiError> {
    adjust_bounded(store, seg, rows, schema, stats, add, with_text, SCAN_ROWS).await
}

#[allow(clippy::too_many_arguments)]
async fn adjust_bounded(
    store: &Storage,
    seg: &Segment,
    rows: &RoaringBitmap,
    schema: &Schema,
    stats: &mut NamespaceStats,
    add: bool,
    with_text: bool,
    chunk_rows: usize,
) -> Result<(), ApiError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut columns: BTreeSet<String> = seg.schema.keys().cloned().collect();
    let mut vector_bytes = 0u64;
    for (name, cfg) in &seg.schema {
        let Some(AttributeType::Vector { dimensions, .. }) = &cfg.data_type else {
            continue;
        };
        let Some(file) = seg.files.get(&format!("annv2:{name}")) else {
            continue;
        };
        let dir = segment::projection_directory(store, seg, file, true).await?;
        let d = &dir.segments()[0];
        // Sparse and flat indexes can omit rows. They use the ordinary, bounded decoder.
        if d.membership.is_none() || d.rows != seg.row_count as usize {
            continue;
        }
        columns.remove(name);
        // Comma, quoted field name, colon, quotes, and base64 of canonical f32 bytes.
        vector_bytes += 1
            + serde_json::to_vec(name)
                .map_err(segment::index_error)?
                .len() as u64
            + 1
            + 2
            + (dimensions * 4).div_ceil(3) as u64 * 4;
    }
    let mut remaining = rows.iter();
    loop {
        let chunk: RoaringBitmap = remaining.by_ref().take(chunk_rows).collect();
        if chunk.is_empty() {
            break;
        }
        let docs = segment::projected_rows_with(store, seg, &chunk, Some(&columns), true).await?;
        for doc in docs.values() {
            if with_text {
                text::adjust(stats, doc, schema, add);
            }
            let bytes =
                serde_json::to_vec(doc).map_err(segment::index_error)?.len() as u64 + vector_bytes;
            if add {
                stats.rows += 1;
                stats.logical_bytes += bytes;
            } else {
                stats.rows = stats.rows.saturating_sub(1);
                stats.logical_bytes = stats.logical_bytes.saturating_sub(bytes);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bf_storage::StorageConfig;
    use bf_types::{DistanceMetric, Document, Id};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parallel_removal_counts_and_masks_equal_sequential_subtraction() {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let schema:Schema=serde_json::from_value(json!({"id":{"type":"uint"},"text":{"type":"string","full_text_search":true},"vector":{"type":"[3]f16"}})).unwrap();
        let mut state = NamespaceState {
            schema: schema.clone(),
            ..Default::default()
        };
        let mut expected = NamespaceStats::default();
        let mut original = NamespaceStats::default();
        for part in 0..3u64 {
            let docs:BTreeMap<Id,Document>=(0..4200).map(|row| {
                let id=part*4200+row;
                (Id::Uint(id),serde_json::from_value(json!({"id":id,"text":format!("fish café {}",row%17),"vector":bf_types::encode_vector(&[1.0,0.5,0.25],false)})).unwrap())
            }).collect();
            let seg = segment::build_with(
                &store,
                "removals",
                &schema,
                DistanceMetric::CosineDistance,
                &docs,
                BTreeMap::new(),
                0,
                1,
            )
            .await
            .unwrap();
            for (id, doc) in &docs {
                text::adjust(&mut original, doc, &schema, true);
                original.rows += 1;
                original.logical_bytes += serde_json::to_vec(doc).unwrap().len() as u64;
                if matches!(id,Id::Uint(n) if n%3!=0) {
                    state.shadowed.insert(id.clone());
                    text::adjust(&mut expected, doc, &schema, true);
                    expected.rows += 1;
                    expected.logical_bytes += serde_json::to_vec(doc).unwrap().len() as u64;
                }
            }
            state.segments.push(seg);
        }
        for workers in [1, 2, 8] {
            let (masks, actual) = removals(&store, &state, workers).await.unwrap();
            assert_eq!(
                serde_json::to_value(&actual).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            for (part, (live, keep)) in masks.iter().enumerate() {
                assert_eq!(live.len(), 4200);
                let wanted: RoaringBitmap = (0..4200)
                    .filter(|row| (part as u32 * 4200 + row).is_multiple_of(3))
                    .collect();
                assert_eq!(*keep, wanted);
            }
            let mut remaining = original.clone();
            merge(&mut remaining, actual, false);
            let mut round_trip = remaining;
            merge(&mut round_trip, expected.clone(), true);
            assert_eq!(
                serde_json::to_value(round_trip).unwrap(),
                serde_json::to_value(&original).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn statistics_match_materialized_documents_for_dense_sparse_and_legacy_vectors() {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let schema: Schema = serde_json::from_value(json!({
            "id":{"type":"uint"},"text":{"type":"string","full_text_search":true},
            "f16":{"type":"[3]f16"},"f32":{"type":"[3]f32"},
            "missing":{"type":"[3]f32"},"i8":{"type":"[3]i8"}
        }))
        .unwrap();
        let docs: BTreeMap<Id,Document> = (0..9000).map(|i| {
            let mut d: Document = serde_json::from_value(json!({"id":i,"text":format!("fish café {}",i%7),"f16":[i as f32*0.1,-0.0,1.001],"f32":[i as f32*0.1,0.,1.001],"i8":[-128,i%100,127]})).unwrap();
            if i%3 == 0 {d.insert("missing".into(),json!([1.,0.,1.]));}
            (Id::Uint(i as u64),d)
        }).collect();
        let current = segment::build_with(
            &store,
            "stats",
            &schema,
            DistanceMetric::CosineDistance,
            &docs,
            BTreeMap::new(),
            0,
            1,
        )
        .await
        .unwrap();
        let dense =
            segment::projection_directory(&store, &current, &current.files["annv2:f16"], true)
                .await
                .unwrap();
        assert!(
            dense.segments()[0].membership.is_some(),
            "exercise fixed-width accounting on a dense index"
        );
        let sparse =
            segment::projection_directory(&store, &current, &current.files["annv2:missing"], true)
                .await
                .unwrap();
        assert!(
            sparse.segments()[0].membership.is_none(),
            "exercise bounded decoding when rows omit a vector"
        );
        let mut legacy = current.clone();
        segment::upgrade::make_legacy(&store, &mut legacy)
            .await
            .unwrap();
        for seg in [&current, &legacy] {
            for selected in [
                (0..9000).collect::<RoaringBitmap>(),
                (0..9000).filter(|r| r % 7 == 0).collect(),
                RoaringBitmap::new(),
            ] {
                let materialized = segment::documents(&store, seg, &selected).await.unwrap();
                let mut expected = NamespaceStats::default();
                for doc in materialized.values() {
                    text::adjust(&mut expected, doc, &schema, true);
                    expected.rows += 1;
                    expected.logical_bytes += serde_json::to_vec(doc).unwrap().len() as u64;
                }
                let actual = rows(&store, seg, &selected, &schema, true).await.unwrap();
                assert_eq!(
                    serde_json::to_value(&actual).unwrap(),
                    serde_json::to_value(&expected).unwrap()
                );
                let mut subtracted = expected.clone();
                adjust(
                    &store,
                    seg,
                    &selected,
                    &schema,
                    &mut subtracted,
                    false,
                    true,
                )
                .await
                .unwrap();
                assert_eq!((subtracted.rows, subtracted.logical_bytes), (0, 0));
                assert!(subtracted.text.values().all(|f| f.documents == 0
                    && f.total_length == 0
                    && f.document_frequencies.is_empty()));
                let without_text = rows(&store, seg, &selected, &schema, false).await.unwrap();
                assert_eq!(
                    (without_text.rows, without_text.logical_bytes),
                    (expected.rows, expected.logical_bytes)
                );
                assert!(without_text.text.is_empty());
            }
        }
    }
}
