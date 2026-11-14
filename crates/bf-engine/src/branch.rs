//! Durable copy-on-write segment pins, committed through the destination WAL.
//!
//! A v8 write UUID distinguishes copy commits from ordinary v4 WAL entries.
//! The immutable pin and commit descriptor are durable before the WAL CAS. Thus
//! a crash cannot acknowledge a branch whose inherited segments cannot replay.
use crate::{Engine, NamespaceState, log_error, snapshot, storage_error};
use bf_storage::{CreateResult, Storage};
use bf_types::*;
use bf_wal::{NamespaceStats, Segment, Wal, WalEntry};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

pub(crate) struct Source {
    pub state: NamespaceState,
    pub rows: usize,
    pin: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Lineage {
    pub parent: String,
    pub depth: u64,
}
#[derive(Serialize, Deserialize)]
struct Pin {
    format_version: u32,
    source_namespace: String,
    created_at: DateTime<Utc>,
    segments: Vec<Segment>,
    stats: NamespaceStats,
    #[serde(default)]
    lineage: Option<Lineage>,
}
#[derive(Serialize, Deserialize)]
struct Commit {
    format_version: u32,
    pin: String,
    #[serde(default)]
    created_at: Option<DateTime<Utc>>,
}
fn commit_key(wal: &Wal, id: Uuid) -> String {
    let namespace = wal.key(0);
    let namespace = namespace.split('/').nth(1).expect("validated namespace");
    format!("ns/{namespace}/branch-commits/{id}.json")
}
fn install(state: &mut NamespaceState, pin: &Pin) {
    state.segments = pin.segments.clone();
    state.stats = pin.stats.clone();
}
pub(crate) fn install_source(state: &mut NamespaceState, source: &Source) {
    state.segments = source.state.segments.clone();
    state.stats = source.state.stats.clone();
}
impl Engine {
    pub(crate) async fn copy_source(
        &self,
        source: &str,
        dest: &str,
        is_branch: bool,
    ) -> Result<Source, ApiError> {
        let wal = Wal::new(self.0.store.clone(), source).map_err(log_error)?;
        for _ in 0..16 {
            let state = self.snapshot(source, &Consistency::Strong).await?.0;
            let pin = Pin {
                format_version: 1,
                source_namespace: source.into(),
                created_at: Utc::now(),
                segments: state.segments.clone(),
                stats: state.stats.clone(),
                lineage: if is_branch {
                    Some(Lineage {
                        parent: source.into(),
                        depth: lineage(&self.0.store, source, state.created_at)
                            .await?
                            .map_or(1, |l| l.depth.saturating_add(1)),
                    })
                } else {
                    None
                },
            };
            let key = format!("ns/{dest}/branch-pins/{}.json", Uuid::new_v4());
            if self
                .0
                .store
                .create(&key, serde_json::to_vec(&pin).expect("pin serializes"))
                .await
                .map_err(storage_error)?
                == CreateResult::Conflict
            {
                continue;
            }
            // GC claims a new source manifest epoch before deleting anything and
            // then rereads pins. Either it sees this pin, or this check retries.
            let generation = wal
                .latest_manifest()
                .await
                .map_err(log_error)?
                .map_or(0, |(g, _)| g);
            if generation != state.manifest_seq {
                continue;
            }
            let rows = snapshot::statistics(&self.0.store, &state).await?.rows as usize;
            return Ok(Source {
                state,
                rows,
                pin: key,
            });
        }
        Err(ApiError::new(
            409,
            "source namespace changed while preparing its segment references; retry the copy",
        ))
    }
}
pub(crate) async fn stage(
    store: &Storage,
    wal: &Wal,
    entry: &mut WalEntry,
    source: &Source,
) -> Result<(), ApiError> {
    let mut bytes = *entry.write_id.as_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    entry.write_id = Uuid::from_bytes(bytes);
    let descriptor = Commit {
        format_version: 1,
        pin: source.pin.clone(),
        created_at: entry.batches.first().map(|b| b.created_at),
    };
    if store
        .create(
            &commit_key(wal, entry.write_id),
            serde_json::to_vec(&descriptor).expect("copy descriptor serializes"),
        )
        .await
        .map_err(storage_error)?
        == CreateResult::Conflict
    {
        return Err(ApiError::new(503, "copy commit descriptor already exists"));
    }
    Ok(())
}
pub(crate) async fn replay(
    store: &Storage,
    wal: &Wal,
    seq: u64,
    entry: &WalEntry,
    state: &mut NamespaceState,
) -> Result<(), ApiError> {
    if entry.write_id.get_version_num() == 8 {
        let descriptor: Commit = serde_json::from_slice(
            &store
                .get(&commit_key(wal, entry.write_id))
                .await
                .map_err(storage_error)?,
        )
        .map_err(crate::segment::index_error)?;
        let pin: Pin =
            serde_json::from_slice(&store.get(&descriptor.pin).await.map_err(storage_error)?)
                .map_err(crate::segment::index_error)?;
        if descriptor.format_version != 1
            || pin.format_version != 1
            || entry.tombstone
            || entry.batches.len() != 1
        {
            return Err(ApiError::new(500, "unsupported copy commit descriptor"));
        }
        install(state, &pin);
    }
    state.replay(seq, entry);
    Ok(())
}
/// Pins conservatively outlive both branches and in-flight copies. Keeping an
/// obsolete pin can retain storage, but cannot erase data still used by a branch.
pub(crate) async fn protected(store: &Storage) -> Result<BTreeSet<String>, ApiError> {
    let mut protected = BTreeSet::new();
    for object in store.list("ns/").await.map_err(storage_error)? {
        if !object.key.contains("/branch-pins/") {
            continue;
        }
        let pin: Pin =
            serde_json::from_slice(&store.get(&object.key).await.map_err(storage_error)?)
                .map_err(crate::segment::index_error)?;
        if pin.format_version != 1 {
            return Err(ApiError::new(500, "unsupported segment pin"));
        }
        for segment in pin.segments {
            protected.extend(
                segment
                    .files
                    .into_values()
                    .chain(std::iter::once(segment.live))
                    .map(|f| f.key),
            );
        }
    }
    Ok(protected)
}

/// Lineage belongs to the destination incarnation, independent of its parent.
pub(crate) async fn lineage(
    store: &Storage,
    ns: &str,
    created_at: DateTime<Utc>,
) -> Result<Option<Lineage>, ApiError> {
    for object in store
        .list(&format!("ns/{ns}/branch-commits/"))
        .await
        .map_err(storage_error)?
    {
        let descriptor: Commit =
            serde_json::from_slice(&store.get(&object.key).await.map_err(storage_error)?)
                .map_err(crate::segment::index_error)?;
        if descriptor.created_at != Some(created_at) {
            continue;
        }
        let pin: Pin =
            serde_json::from_slice(&store.get(&descriptor.pin).await.map_err(storage_error)?)
                .map_err(crate::segment::index_error)?;
        return Ok(pin.lineage);
    }
    Ok(None)
}
