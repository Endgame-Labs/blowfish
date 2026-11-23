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
        let converted =
            futures::future::try_join_all(group.into_iter().map(|(i, mut seg)| async move {
                let mut old = Vec::new();
                while let Some(key) = segment::upgrade_text(store, ns, &mut seg).await? {
                    old.push(key);
                }
                text_keys::backfill(store, ns, &mut seg).await?;
                Ok::<_, ApiError>((i, seg, old))
            }))
            .await?;
        for (i, seg, old) in converted {
            segments[i] = seg;
            retired.extend(old);
        }
        remaining = &remaining[count..];
    }
    Ok(retired)
}
