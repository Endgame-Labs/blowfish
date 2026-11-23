//! Memory-budgeted batches of immutable text rewrites under the indexer's epoch claim.
use crate::{segment, text_keys};
use bf_storage::Storage;
use bf_types::ApiError;
use bf_wal::Segment;

const MAX_SEGMENTS: usize = 32;

fn pending(seg: &Segment) -> bool {
    text_keys::needs(seg)
        || seg
            .files
            .iter()
            .any(|(role, file)| role.starts_with("fts:") && segment::text_needs_upgrade(file))
}
fn working_bytes(seg: &Segment) -> u64 {
    let mut largest = 0;
    for (role, file) in &seg.files {
        if role.starts_with("fts:") && segment::text_needs_upgrade(file) {
            // Input, output, term directories, and builder allocations for pre-v4 fallback.
            let docs = seg
                .files
                .get("docs")
                .map_or(0, |f| f.size.saturating_mul(8));
            largest = largest.max(file.size.saturating_mul(6).saturating_add(docs));
        }
    }
    largest
        .saturating_add(u64::from(seg.row_count) * 128)
        .max(1 << 20)
}

pub(crate) async fn upgrade(
    store: &Storage,
    ns: &str,
    segments: &mut [Segment],
) -> Result<Vec<String>, ApiError> {
    let budget = (bf_alloc::memory_limit_bytes() / 8).clamp(16 << 20, 1 << 30);
    let concurrency = std::thread::available_parallelism().map_or(1, usize::from);
    let jobs: Vec<_> = segments
        .iter()
        .enumerate()
        .filter(|(_, s)| pending(s))
        .take(MAX_SEGMENTS)
        .map(|(i, s)| (i, working_bytes(s)))
        .collect();
    let mut retired = Vec::new();
    let mut remaining = jobs.as_slice();
    while !remaining.is_empty() {
        let mut bytes = 0u64;
        let mut count = 0;
        for (_, cost) in remaining.iter().take(concurrency) {
            // An indivisible oversized field runs alone, so a second cannot multiply its peak.
            if count > 0 && bytes.saturating_add(*cost) > budget {
                break;
            }
            bytes = bytes.saturating_add(*cost);
            count += 1;
        }
        let group = remaining[..count]
            .iter()
            .map(|(i, _)| (*i, segments[*i].clone()))
            .collect::<Vec<_>>();
        // Blocking conversion work cannot be cancelled by dropping its future. Drain launched
        // peers on error before another pass can spend the same working-set budget.
        let converted =
            futures::future::join_all(group.into_iter().map(|(i, mut seg)| async move {
                let mut old = Vec::new();
                while let Some(key) = segment::upgrade_text(store, ns, &mut seg).await? {
                    old.push(key);
                }
                text_keys::backfill(store, ns, &mut seg).await?;
                Ok::<_, ApiError>((i, seg, old))
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        for (i, seg, old) in converted {
            segments[i] = seg;
            retired.extend(old);
        }
        remaining = &remaining[count..];
    }
    Ok(retired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Engine, EngineConfig};
    use bf_storage::StorageConfig;
    use bf_wal::Wal;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_group_drains_launched_peers_without_adopting_partial_results() {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let engine = Engine::with_config(
            store.clone(),
            EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                ..Default::default()
            },
        );
        engine.write("failure",serde_json::from_value(json!({"schema":{"text":{"type":"string","full_text_search":true}},"upsert_rows":[{"id":1,"text":"fish and water"}]})).unwrap()).await.unwrap();
        engine.index_namespace("failure", true).await.unwrap();
        let (_, m) = Wal::new(store.clone(), "failure")
            .unwrap()
            .latest_manifest()
            .await
            .unwrap()
            .unwrap();
        let mut segments = vec![m.segments[0].clone(), m.segments[0].clone()];
        segments[0].id = "broken".into();
        segments[1].id = "peer".into();
        for s in &mut segments {
            s.files.get_mut("fts:text").unwrap().text_header = None;
        }
        segments[0].files.get_mut("fts:text").unwrap().key = "ns/failure/missing-text.bft".into();
        let before: Vec<_> = segments
            .iter()
            .map(|s| s.files["fts:text"].key.clone())
            .collect();
        assert!(upgrade(&store, "failure", &mut segments).await.is_err());
        assert_eq!(
            segments
                .iter()
                .map(|s| s.files["fts:text"].key.clone())
                .collect::<Vec<_>>(),
            before
        );
        if std::thread::available_parallelism().is_ok_and(|n| n.get() >= 2) {
            let peer = store.list("ns/failure/segments/peer/").await.unwrap();
            assert_eq!(
                peer.len(),
                1,
                "the peer must finish its upload before the failed group returns"
            );
        }
    }
}
