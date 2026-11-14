//! Consistent segment checkpoints plus a bounded, newest-wins WAL overlay.
use crate::{NamespaceState, Operation, filter, log_error, segment};
use bf_storage::Storage;
use bf_types::*;
use bf_wal::{Pending, Wal};
use roaring::RoaringBitmap;
use std::{collections::BTreeSet, time::Instant};

pub(crate) async fn catch_up(
    wal: &Wal,
    state: &mut NamespaceState,
    cap: Option<u64>,
) -> Result<(), ApiError> {
    // Always refresh the manifest before reading a potentially reclaimed tail.
    // A concurrent GC can still race this read; one fresh checkpoint retry resolves it.
    for attempt in 0..3 {
        if let Some((generation, m)) = wal.latest_manifest().await.map_err(log_error)? {
            if m.format_version != 1 {
                return Err(ApiError::new(500, "unsupported manifest format"));
            }
            if m.indexed_through > 0 && generation > state.manifest_seq {
                *state = if m.deleted {
                    NamespaceState {
                        seq: m.indexed_through,
                        indexed_through: m.indexed_through,
                        manifest_seq: generation,
                        ..Default::default()
                    }
                } else {
                    NamespaceState {
                        seq: m.indexed_through,
                        exists: true,
                        schema: m.schema,
                        metric: m.distance_metric,
                        created_at: m.created_at,
                        updated_at: m.updated_at.unwrap_or(m.created_at),
                        segments: m.segments,
                        indexed_through: m.indexed_through,
                        manifest_seq: generation,
                        stats: m.stats,
                        ..Default::default()
                    }
                };
            }
            state.manifest_seq = generation;
        }
        match wal
            .tail_bounded(
                state.seq,
                cap.unwrap_or(u64::MAX).saturating_sub(state.tail_bytes),
            )
            .await
        {
            Ok(entries) => {
                for (seq, entry) in entries {
                    let size = serde_json::to_vec(&entry).expect("WAL serializes").len() as u64;
                    if cap.is_some_and(|cap| state.tail_bytes.saturating_add(size) > cap) {
                        break;
                    }
                    state.replay(seq, &entry);
                }
                state.refreshed = Instant::now();
                return Ok(());
            }
            Err(bf_wal::Error::Pruned | bf_wal::Error::Format) if attempt < 2 => continue,
            Err(e) => return Err(log_error(e)),
        }
    }
    unreachable!()
}
/// Remove tail-shadowed IDs before searching any immutable segment.
pub(crate) async fn allowed(
    store: &Storage,
    state: &NamespaceState,
    seg: &bf_wal::Segment,
    f: Option<&Filter>,
) -> Result<RoaringBitmap, ApiError> {
    let mut live = segment::live(store, seg).await?;
    if !state.shadowed.is_empty() {
        let ids = segment::ids(store, seg).await?;
        for row in live.clone() {
            if state.shadowed.contains(&ids[row as usize]) {
                live.remove(row);
            }
        }
    }
    if let Some(f) = f {
        filter::validate(f, &state.schema, false)?;
        live = segment::filter_rows(store, seg, f, &state.schema, &live).await?;
    }
    Ok(live)
}
pub(crate) async fn materialize(
    store: &Storage,
    state: &NamespaceState,
) -> Result<NamespaceState, ApiError> {
    let mut full = state.clone();
    for seg in &state.segments {
        let live = allowed(store, state, seg, None).await?;
        full.docs
            .extend(segment::documents(store, seg, &live).await?);
    }
    // Schema drops apply immediately even while older segment columns remain on disk.
    for doc in full.docs.values_mut() {
        doc.retain(|name, _| state.schema.contains_key(name));
    }
    Ok(full)
}
/// Point writes fetch their current IDs; by-filter and schema-drop writes use a scan.
pub(crate) async fn for_writes(
    store: &Storage,
    state: &NamespaceState,
    pending: &[Pending<Operation, Result<WriteResponse, ApiError>>],
) -> Result<NamespaceState, ApiError> {
    let mut wanted = BTreeSet::new();
    for p in pending {
        let Operation::Write(req, _) = &p.request else {
            continue;
        };
        if req.delete_by_filter.is_some() || req.patch_by_filter.is_some() {
            return materialize(store, state).await;
        }
        for row in req
            .upsert_rows
            .iter()
            .flatten()
            .chain(req.patch_rows.iter().flatten())
        {
            if let Some(value) = row.get("id")
                && let Ok(id) = serde_json::from_value::<Id>(value.clone())
            {
                wanted.insert(id);
            }
        }
        for columns in req.upsert_columns.iter().chain(req.patch_columns.iter()) {
            if let Some(ids) = columns.get("id").and_then(serde_json::Value::as_array) {
                for value in ids {
                    if let Ok(id) = serde_json::from_value::<Id>(value.clone()) {
                        wanted.insert(id);
                    }
                }
            }
        }
        wanted.extend(req.deletes.iter().flatten().cloned());
    }
    if state.schema.get("id").and_then(|s| s.data_type.as_ref())
        == Some(&AttributeType::Scalar(ScalarType::Uuid))
    {
        wanted = wanted
            .into_iter()
            .map(|id| match id {
                Id::String(s) => uuid::Uuid::parse_str(&s)
                    .map(|id| Id::String(id.to_string()))
                    .unwrap_or(Id::String(s)),
                id => id,
            })
            .collect();
    }
    let mut full = state.clone();
    for seg in &state.segments {
        let live = segment::live(store, seg).await?;
        let wanted = wanted.difference(&state.shadowed).cloned().collect();
        let rows = segment::lookup_many(store, seg, &wanted).await? & live;
        full.docs
            .extend(segment::documents(store, seg, &rows).await?);
    }
    for doc in full.docs.values_mut() {
        doc.retain(|name, _| state.schema.contains_key(name));
    }
    Ok(full)
}

/// Exact row and logical-byte totals without materializing unaffected segment rows.
pub(crate) async fn statistics(
    store: &Storage,
    state: &NamespaceState,
) -> Result<bf_wal::NamespaceStats, ApiError> {
    let mut stats = state.stats.clone();
    if !state.shadowed.is_empty() {
        for seg in &state.segments {
            let rows = segment::lookup_many(store, seg, &state.shadowed).await?
                & segment::live(store, seg).await?;
            let old = segment::documents(store, seg, &rows).await?;
            stats.rows = stats.rows.saturating_sub(old.len() as u64);
            let bytes: u64 = old
                .values()
                .map(|d| serde_json::to_vec(d).expect("document serializes").len() as u64)
                .sum();
            stats.logical_bytes = stats.logical_bytes.saturating_sub(bytes);
        }
    }
    stats.rows += state.docs.len() as u64;
    stats.logical_bytes += state
        .docs
        .values()
        .map(|d| serde_json::to_vec(d).expect("document serializes").len() as u64)
        .sum::<u64>();
    Ok(stats)
}
