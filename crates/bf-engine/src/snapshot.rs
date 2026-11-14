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
                *state = NamespaceState {
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
        if req.delete_by_filter.is_some()
            || req.patch_by_filter.is_some()
            || req
                .schema
                .as_ref()
                .is_some_and(|s| s.values().any(|v| v.expanded().drop))
        {
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
    let mut full = state.clone();
    for seg in &state.segments {
        let live = segment::live(store, seg).await?;
        let mut rows = RoaringBitmap::new();
        for id in &wanted {
            if !state.shadowed.contains(id)
                && let Some(row) = segment::lookup(store, seg, id).await?
                && live.contains(row)
            {
                rows.insert(row);
            }
        }
        full.docs
            .extend(segment::documents(store, seg, &rows).await?);
    }
    Ok(full)
}
