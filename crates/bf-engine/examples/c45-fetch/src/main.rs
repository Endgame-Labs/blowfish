//! Round C45: how long does S3 take to return a set of objects (or their first N bytes) as parallel ranged GETs?
//!
//! usage: c45-fetch BUCKET REGION REPS INFLIGHT WARM GROUP [-- GROUP ...]
//!        GROUP = NAME PART_BYTES KEY:LEN [KEY:LEN ...]
//!
//! One run of a group issues every part of every object at once (at most INFLIGHT at a time; 0 means all), the way a query
//! would read one scoring pack per text segment right after the manifest, and prints one JSON line: the time to
//! the last byte of the whole set, of each object, and the per-part first-byte and last-byte percentiles.
//! Each rep runs every group once, back to back, in an order that rotates by rep, so groups are compared within the same
//! seconds of S3 weather (paired).
//! C45_HEDGE_MS (environment, default 0 = off) duplicates a part still unfinished after that many milliseconds and keeps whichever
//! copy finishes first, the way the server's hedged reads do; the JSON line counts the duplicates.
//! WARM idle connections are opened first (HEADs of a missing key), as the server's connection warmer does.
//! LEN of 0 means the whole object. Keys are physical (the 24-byte create header counts). Read only.
use futures::future::join_all;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path,
};
use std::{
    sync::{Arc, atomic::AtomicUsize},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 * p) as usize).min(v.len() - 1)]
}

struct Group {
    name: String,
    part: u64,
    objects: Vec<(Path, u64)>,
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (bucket, region) = (&a[1], &a[2]);
    let (reps, inflight, warm): (usize, usize, usize) = (
        a[3].parse().unwrap(),
        a[4].parse().unwrap(),
        a[5].parse().unwrap(),
    );
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_client_options(
                object_store::ClientOptions::new().with_pool_idle_timeout(Duration::from_secs(600)),
            )
            .build()
            .unwrap(),
    );
    let mut groups: Vec<Group> = Vec::new();
    for words in a[6..].split(|w| w == "--") {
        let mut objects = Vec::new();
        for spec in &words[2..] {
            let (key, len) = spec.rsplit_once(':').unwrap();
            let path = Path::from(key);
            let len: u64 = len.parse().unwrap();
            let size = store.head(&path).await.unwrap().size;
            objects.push((path, if len == 0 { size } else { len.min(size) }));
        }
        groups.push(Group {
            name: words[0].clone(),
            part: words[1].parse().unwrap(),
            objects,
        });
    }
    let hedge_ms: u64 = std::env::var("C45_HEDGE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let hedges = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(if inflight == 0 {
        1 << 20
    } else {
        inflight
    }));
    let missing = Path::from("c45/none");
    for rep in 0..reps {
        for turn in 0..groups.len() {
            let g = &groups[(turn + rep) % groups.len()];
            join_all((0..warm).map(|_| store.head(&missing))).await;
            let total: u64 = g.objects.iter().map(|o| o.1).sum();
            let t0 = Instant::now();
            let mut jobs = Vec::new();
            for (oi, (path, len)) in g.objects.iter().enumerate() {
                let mut off = 0;
                while off < *len {
                    let end = (off + g.part).min(*len);
                    let (store, path, gate, hedges) =
                        (store.clone(), path.clone(), gate.clone(), hedges.clone());
                    jobs.push(tokio::spawn(async move {
                        let _permit = gate.acquire().await.unwrap();
                        let start = Instant::now();
                        let one = |store: Arc<dyn ObjectStore>, path: Path| async move {
                            let opts = GetOptions {
                                range: Some(GetRange::Bounded(off..end)),
                                ..Default::default()
                            };
                            let r = store.get_opts(&path, opts).await.unwrap();
                            let first = start.elapsed().as_secs_f64() * 1e3;
                            let body = r.bytes().await.unwrap();
                            assert_eq!(body.len() as u64, end - off);
                            first
                        };
                        let primary = one(store.clone(), path.clone());
                        tokio::pin!(primary);
                        let first = if hedge_ms == 0 {
                            primary.await
                        } else {
                            tokio::select! {
                                f = &mut primary => f,
                                _ = tokio::time::sleep(Duration::from_millis(hedge_ms)) => {
                                    hedges.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    let second = one(store.clone(), path.clone());
                                    tokio::select! { f = &mut primary => f, f = second => f }
                                }
                            }
                        };
                        (
                            oi,
                            first,
                            start.elapsed().as_secs_f64() * 1e3,
                            t0.elapsed().as_secs_f64() * 1e3,
                        )
                    }));
                    off = end;
                }
            }
            let done: Vec<_> = join_all(jobs)
                .await
                .into_iter()
                .map(|j| j.unwrap())
                .collect();
            let wall = t0.elapsed().as_secs_f64() * 1e3;
            let mut per_object = vec![0f64; g.objects.len()];
            for d in &done {
                per_object[d.0] = per_object[d.0].max(d.3);
            }
            let (mut fb, mut tot): (Vec<f64>, Vec<f64>) = (
                done.iter().map(|d| d.1).collect(),
                done.iter().map(|d| d.2).collect(),
            );
            println!(
                "{}",
                serde_json::json!({"rep": rep, "name": g.name, "parts": done.len(), "bytes": total,
                    "wall_ms": wall, "gbps": total as f64 / wall / 1e6,
                    "hedges": hedges.swap(0, std::sync::atomic::Ordering::Relaxed),
                    "per_object_ms": per_object,
                    "part_first_p50": pct(&mut fb, 0.5), "part_first_p90": pct(&mut fb, 0.9),
                    "part_total_p50": pct(&mut tot, 0.5), "part_total_p90": pct(&mut tot, 0.9),
                    "part_total_max": pct(&mut tot, 1.0)})
            );
        }
    }
}
