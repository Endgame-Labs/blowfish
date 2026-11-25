//! Round BW16 kill-test instrument (env `BF_BW16_ORACLE=1`): an out-of-band thread resolves each
//! published state's shadow masks ahead of the query, so a query that finds them pays nothing for
//! shadow-ID resolution. This is the best any ID-to-row map could do, and it is not shipped.
use crate::{NamespaceState, Published, segment};
use bf_storage::Storage;
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};

type Key = (u64, u64, usize, usize);
type Masks = Arc<BTreeMap<String, RoaringBitmap>>;

static MEMO: OnceLock<Mutex<BTreeMap<Key, Masks>>> = OnceLock::new();
pub static HITS: AtomicU64 = AtomicU64::new(0);
pub static MISSES: AtomicU64 = AtomicU64::new(0);

pub fn enabled() -> bool {
    std::env::var_os("BF_BW16_ORACLE").is_some()
}
fn key(s: &NamespaceState) -> Key {
    (s.seq, s.manifest_seq, s.shadowed.len(), s.segments.len())
}
/// The precomputed `lookup_many & live` row set per segment, if the poller has this state.
pub(crate) fn get(s: &NamespaceState) -> Option<Masks> {
    let found = MEMO.get()?.lock().unwrap().get(&key(s)).cloned();
    let counter = if found.is_some() { &HITS } else { &MISSES };
    let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
    if n % 20 == 0 {
        eprintln!(
            "BW16 oracle hits={} misses={}",
            HITS.load(Ordering::Relaxed),
            MISSES.load(Ordering::Relaxed)
        );
    }
    found
}
pub(crate) fn spawn(
    state: Arc<Published<NamespaceState>>,
    store: Storage,
    ctx: crate::read_cache::ReadContext,
) {
    MEMO.get_or_init(Default::default);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(crate::read_cache::scope(ctx, async move {
            let mut last = None;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                let s = state.load();
                if s.shadowed.is_empty() || Some(key(&s)) == last {
                    continue;
                }
                last = Some(key(&s));
                let mut masks = BTreeMap::new();
                let mut ok = true;
                for seg in s.segments.iter() {
                    let r = async {
                        let rows = segment::lookup_many(&store, seg, &s.shadowed).await?;
                        let live = segment::live(&store, seg).await?;
                        Ok::<_, bf_types::ApiError>(rows & live)
                    }
                    .await;
                    match r {
                        Ok(rows) => {
                            masks.insert(seg.id.clone(), rows);
                        }
                        Err(_) => {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    let mut m = MEMO.get().unwrap().lock().unwrap();
                    m.insert(key(&s), Arc::new(masks));
                    while m.len() > 64 {
                        m.pop_first();
                    }
                }
            }
        }));
    });
}
