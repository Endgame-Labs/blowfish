//! Full-corpus offline prerequisite; enabled explicitly, and never run in ordinary test suites.
use super::*;

#[tokio::test]
#[ignore = "requires an owned full-NQ source store and a new RR3_COPY under .scratch"]
async fn full_nq_recluster_copy() {
    let source = std::path::PathBuf::from(std::env::var_os("RR3_STORE").expect("RR3_STORE"));
    let output = std::path::PathBuf::from(std::env::var_os("RR3_COPY").expect("RR3_COPY"));
    assert!(output.components().any(|c| c.as_os_str() == ".scratch"));
    assert!(!output.exists(), "the benchmark copy must be new");
    assert!(
        std::process::Command::new("cp")
            .args(["-a", "--reflink=always"])
            .arg(&source)
            .arg(&output)
            .status()
            .unwrap()
            .success()
    );
    let store = bf_storage::Storage::open(&bf_storage::StorageConfig {
        url: format!("file://{}", output.display()),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store,
        crate::EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            decoded_cache_bytes: 1 << 30,
            ..Default::default()
        },
    );
    let scratch = output.join(".scratch");
    let report = engine
        .recluster(
            "nq",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 32,
                cluster_rows: 0,
                max_slab_rows: 250_000,
                scratch: scratch.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(report.rows, 2_681_468);
    assert_eq!(std::fs::read_dir(scratch).unwrap().count(), 0);
    println!(
        "RR3 copied tier rows={} leaves={} seconds={:.3}",
        report.rows, report.clusters, report.seconds
    );
}

pub(super) fn visit_order(present: &[u32]) -> Option<Vec<usize>> {
    let root = std::path::PathBuf::from(std::env::var_os("RR3_SPILL")?);
    let offsets = spill_offsets(&root);
    let mut order: Vec<_> = (0..present.len()).collect();
    order.sort_unstable_by_key(|i| offsets[present[*i] as usize]);
    Some(order)
}

fn spill_offsets(root: &std::path::Path) -> Vec<u32> {
    std::fs::read(root.join("offsets.u32"))
        .unwrap()
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| u32::from_le_bytes(*b))
        .collect()
}

#[tokio::test]
#[ignore = "requires a completed full-NQ store in RR3_STORE, and an output RR3_SPILL"]
async fn full_nq_prepare_spill() {
    use std::io::Write;
    let root = std::path::PathBuf::from(std::env::var_os("RR3_STORE").expect("RR3_STORE"));
    let output = std::path::PathBuf::from(std::env::var_os("RR3_SPILL").expect("RR3_SPILL"));
    assert!(output.components().any(|c| c.as_os_str() == ".scratch"));
    std::fs::create_dir_all(&output).unwrap();
    let store = bf_storage::Storage::open(&bf_storage::StorageConfig {
        url: format!("file://{}", root.display()),
        read_only: true,
        ..Default::default()
    })
    .await
    .unwrap();
    let (_, manifest) = Wal::new(store.clone(), "nq")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    let mut file =
        std::io::BufWriter::new(std::fs::File::create(output.join("vectors.raw")).unwrap());
    let mut offsets = vec![u32::MAX; 2_681_468];
    let mut at = 0u32;
    for source in &manifest.segments {
        let ids = segment::ids(&store, source).await.unwrap();
        let index = segment_index(&store, source, "annv2:vector").await.unwrap();
        assert_eq!(index.vector_row_bytes(), 2048);
        for c in 0..index.cluster_count() {
            let (rows, raw) = index.read_cluster_rows(c).await.unwrap();
            file.write_all(&raw).unwrap();
            for (i, g) in rows.iter().enumerate() {
                let bf_types::Id::Uint(id) = ids[*g as usize] else {
                    panic!("numeric NQ IDs")
                };
                assert_eq!(offsets[id as usize], u32::MAX);
                offsets[id as usize] = at + i as u32;
            }
            at += rows.len() as u32;
        }
    }
    assert_eq!(at, 2_681_468);
    assert!(!offsets.contains(&u32::MAX));
    file.flush().unwrap();
    let mut positions =
        std::io::BufWriter::new(std::fs::File::create(output.join("offsets.u32")).unwrap());
    for at in offsets {
        positions.write_all(&at.to_le_bytes()).unwrap();
    }
    println!("RR3 engine spill rows={at}");
}

#[tokio::test]
#[ignore = "requires a completed disposable full-NQ store in RR3_STORE"]
async fn full_nq_layout_fingerprint() {
    use std::io::Write;
    let root = std::path::PathBuf::from(std::env::var_os("RR3_STORE").expect("RR3_STORE"));
    let output = std::path::PathBuf::from(std::env::var_os("RR3_OUTPUT").expect("RR3_OUTPUT"));
    assert!(output.components().any(|c| c.as_os_str() == ".scratch"));
    let store = bf_storage::Storage::open(&bf_storage::StorageConfig {
        url: format!("file://{}", root.display()),
        read_only: true,
        ..Default::default()
    })
    .await
    .unwrap();
    let (_, manifest) = Wal::new(store.clone(), "nq")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    let tier = &manifest.ann_tiers["vector"][0];
    assert_eq!(manifest.ann_tiers["vector"].len(), 1);
    let mut global_ids = vec![usize::MAX; tier.covered.iter().map(|c| c.rows as usize).sum()];
    let placement = crate::lineage::place(&store, &manifest.segments, &manifest.lineage, tier)
        .await
        .unwrap()
        .unwrap();
    for (cover, place) in tier.covered.iter().zip(placement.places) {
        match place {
            crate::lineage::Place::Here(at) => {
                for (row, id) in segment::ids(&store, &manifest.segments[at])
                    .await
                    .unwrap()
                    .iter()
                    .enumerate()
                {
                    let bf_types::Id::Uint(id) = id else {
                        panic!("numeric NQ IDs")
                    };
                    global_ids[cover.base as usize + row] = *id as usize;
                }
            }
            crate::lineage::Place::Moved(parts) => {
                for (at, map) in parts {
                    let ids = segment::ids(&store, &manifest.segments[at]).await.unwrap();
                    for (from, to) in map.pairs() {
                        let bf_types::Id::Uint(id) = &ids[to as usize] else {
                            panic!("numeric NQ IDs")
                        };
                        global_ids[(cover.base + from) as usize] = *id as usize;
                    }
                }
            }
        }
    }
    assert_eq!(global_ids.len(), 2_681_468);
    assert!(!global_ids.contains(&usize::MAX));
    let mut leaf_of = vec![u32::MAX; global_ids.len()];
    let mut centers = Vec::new();
    let mut base = 0;
    for slab in &tier.slabs {
        let index = segment_index(&store, slab, "annv2:vector").await.unwrap();
        for seed in 0..8 {
            let probe: Vec<_> = (0..index.dims())
                .map(|d| ((seed * 7919 + d * 104729) as f64).sin() as f32)
                .collect();
            centers.extend(index.center_distances(&probe).unwrap());
        }
        for c in 0..index.cluster_count() {
            for g in index.read_cluster_rows(c).await.unwrap().0 {
                let id = global_ids[g as usize];
                assert_eq!(leaf_of[id], u32::MAX, "a live row appears twice");
                leaf_of[id] = base + c as u32;
            }
        }
        base += index.cluster_count() as u32;
    }
    assert!(!leaf_of.contains(&u32::MAX));
    let mut assignments = std::fs::File::create(output.with_extension("assignments")).unwrap();
    for leaf in leaf_of {
        assignments.write_all(&leaf.to_le_bytes()).unwrap();
    }
    let mut fingerprints = std::fs::File::create(output.with_extension("centers")).unwrap();
    for distance in centers {
        fingerprints.write_all(&distance.to_le_bytes()).unwrap();
    }
    std::fs::write(
        output.with_extension("coarse"),
        store.get(&tier.coarse.as_ref().unwrap().key).await.unwrap(),
    )
    .unwrap();
    println!(
        "RR3 fingerprint rows={} leaves={base} sources={}",
        global_ids.len(),
        tier.covered.len()
    );
}

fn raw_f32(path: &std::path::Path, dims: usize) -> Vec<Vec<f32>> {
    std::fs::read(path)
        .unwrap()
        .chunks_exact(dims * 4)
        .map(|row| {
            row.as_chunks::<4>()
                .0
                .iter()
                .map(|v| f32::from_le_bytes(*v))
                .collect()
        })
        .collect()
}

#[test]
#[ignore = "requires the public full-NQ corpus and exact truth in RR3_DATA"]
fn full_nq_assignment_prerequisite() {
    let data = std::path::PathBuf::from(std::env::var_os("RR3_DATA").expect("RR3_DATA"));
    let algorithm = std::env::var("RR3_ALGORITHM").unwrap_or_else(|_| "baseline".into());
    let spill = std::env::var_os("RR3_SPILL").map(std::path::PathBuf::from);
    let file = std::fs::File::open(
        spill
            .as_ref()
            .map_or_else(|| data.join("corpus.f16"), |p| p.join("vectors.raw")),
    )
    .unwrap();
    let positions = spill.as_ref().map(|p| spill_offsets(p));
    let offset = if spill.is_some() {
        0
    } else {
        let mut header = [0u8; 10];
        file.read_exact_at(&mut header, 0).unwrap();
        assert_eq!(&header[..8], b"\x93NUMPY\x01\x00");
        10 + u16::from_le_bytes([header[8], header[9]]) as u64
    };
    let width = 1024 * 2;
    let count = ((file.metadata().unwrap().len() - offset) / width) as usize;
    assert_eq!(count, 2_681_468);
    let target_rows = super::target_leaf_rows(count as u64);
    let read_row = |g: u32, buf: &mut Vec<u8>| {
        buf.resize(width as usize, 0);
        let position = positions.as_ref().map_or(g, |p| p[g as usize]);
        file.read_exact_at(buf, offset + u64::from(position) * width)
            .unwrap();
        let v: Vec<_> = buf
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| bf_vector::f16::from_bits(u16::from_le_bytes(*b)).to_f32())
            .collect();
        bf_vector::prepare_vector(&v, bf_vector::Metric::CosineDistance)
    };
    let present: Vec<u32> = (0..count as u32).collect();
    let started = std::time::Instant::now();
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let picks: Vec<u32> = (0..19_200)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            present[state as usize % present.len()]
        })
        .collect();
    let sample: Vec<_> = picks
        .par_iter()
        .map_init(Vec::new, |buf, g| read_row(*g, buf))
        .collect();
    let refs: Vec<_> = sample.iter().map(Vec::as_slice).collect();
    let coarse = math::kmeans(&refs, 32, 8, 7);
    let packed = math::Centers::new(coarse.iter().map(Vec::as_slice));
    let cells: Vec<usize> = present
        .par_chunks(128)
        .flat_map_iter(|chunk| {
            let mut buf = Vec::new();
            let vectors: Vec<_> = chunk.iter().map(|g| read_row(*g, &mut buf)).collect();
            let refs: Vec<_> = vectors.iter().map(Vec::as_slice).collect();
            packed.nearest(&refs)
        })
        .collect();
    let mut members = vec![Vec::new(); 32];
    for (&g, cell) in present.iter().zip(cells) {
        members[cell].push(g);
    }
    println!("RR3 coarse seconds={:.3}", started.elapsed().as_secs_f64());
    let started = std::time::Instant::now();
    let jobs = if algorithm == "projection" {
        projection(members, &read_row, target_rows)
    } else {
        assign_leaves(
            &present,
            &read_row,
            &coarse,
            members,
            target_rows,
            250_000,
            &|| false,
        )
        .unwrap()
    };
    println!(
        "RR3 {algorithm} leaves seconds={:.3}",
        started.elapsed().as_secs_f64()
    );
    let mut centers = Vec::new();
    let mut leaf_of = vec![0usize; count];
    for job in jobs {
        let base = centers.len();
        centers.extend(job.centers);
        for (g, l) in job.rows.iter().zip(job.assignment) {
            leaf_of[*g as usize] = base + l;
        }
    }
    let queries: Vec<_> = raw_f32(&data.join("queries.f32"), 1024)
        .into_iter()
        .map(|v| bf_vector::prepare_vector(&v, bf_vector::Metric::CosineDistance))
        .collect();
    assert_eq!(queries.len(), 3452);
    let truth: Vec<_> = std::fs::read(data.join("truth.u32"))
        .unwrap()
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| u32::from_le_bytes(*b))
        .collect();
    let packed = math::Centers::new(centers.iter().map(Vec::as_slice));
    let hits: Vec<(usize, usize)> = queries
        .par_chunks(64)
        .enumerate()
        .flat_map_iter(|(chunk, rows)| {
            let refs: Vec<_> = rows.iter().map(Vec::as_slice).collect();
            let norms: Vec<_> = rows.iter().map(|r| norm2(r)).collect();
            packed
                .distances(&refs, &norms)
                .chunks(centers.len())
                .enumerate()
                .map(|(local, distances)| {
                    let mut order: Vec<_> =
                        distances.iter().enumerate().map(|(i, d)| (*d, i)).collect();
                    order.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                    let query = chunk * 64 + local;
                    let hits = |probes: f64| {
                        let n = (probes * (centers.len() as f64 / (count as f64).sqrt()).max(1.))
                            .ceil() as usize;
                        let selected: std::collections::BTreeSet<_> =
                            order.iter().take(n).map(|(_, i)| *i).collect();
                        truth[query * 100..query * 100 + 10]
                            .iter()
                            .filter(|g| selected.contains(&leaf_of[**g as usize]))
                            .count()
                    };
                    (hits(48.), hits(64.))
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let (a, b) = hits.iter().fold((0, 0), |(a, b), (x, y)| (a + x, b + y));
    println!(
        "RR3 {algorithm} leaves={} ideal_routing_hits={a}/{b} ideal_routing_recall={:.8}/{:.8}",
        centers.len(),
        a as f64 / 34520.,
        b as f64 / 34520.
    );
}

fn projection(
    cells: Vec<Vec<u32>>,
    read_row: &(dyn Fn(u32, &mut Vec<u8>) -> Vec<f32> + Sync),
    target: usize,
) -> Vec<SlabJob> {
    cells
        .into_iter()
        .filter(|c| !c.is_empty())
        .map(|ids| {
            let vectors: Vec<_> = ids
                .par_iter()
                .map_init(Vec::new, |buf, g| read_row(*g, buf))
                .collect();
            let mut leaves = Vec::new();
            let mut assignment = vec![0; ids.len()];
            fn split(
                order: &mut [usize],
                vectors: &[Vec<f32>],
                target: usize,
                leaves: &mut Vec<Vec<f32>>,
                assignment: &mut [usize],
            ) {
                if order.len() <= target {
                    let mut center = vec![0f64; vectors[0].len()];
                    // Restore canonical row order for every sum.
                    order.sort_unstable();
                    for &i in order.iter() {
                        assignment[i] = leaves.len();
                        for (s, x) in center.iter_mut().zip(&vectors[i]) {
                            *s += f64::from(*x);
                        }
                    }
                    leaves.push(
                        center
                            .iter()
                            .map(|s| (*s / order.len() as f64) as f32)
                            .collect(),
                    );
                    return;
                }
                let farthest = |seed: usize| {
                    *order
                        .iter()
                        .max_by(|a, b| {
                            bf_vector::squared_distance(&vectors[seed], &vectors[**a])
                                .total_cmp(&bf_vector::squared_distance(
                                    &vectors[seed],
                                    &vectors[**b],
                                ))
                                .then(a.cmp(b))
                        })
                        .unwrap()
                };
                let a = farthest(order[0]);
                let b = farthest(a);
                let direction: Vec<_> = vectors[a]
                    .iter()
                    .zip(&vectors[b])
                    .map(|(a, b)| a - b)
                    .collect();
                let mut projected: Vec<_> = order
                    .iter()
                    .map(|&i| {
                        (
                            bf_vector::dots_x4([vectors[i].as_slice(); 4], &direction)[0],
                            i,
                        )
                    })
                    .collect();
                let mid = projected.len() / 2;
                projected
                    .select_nth_unstable_by(mid, |a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                for (i, (_, row)) in order.iter_mut().zip(projected) {
                    *i = row;
                }
                let (left, right) = order.split_at_mut(mid);
                split(left, vectors, target, leaves, assignment);
                split(right, vectors, target, leaves, assignment);
            }
            let mut order: Vec<_> = (0..ids.len()).collect();
            split(&mut order, &vectors, target, &mut leaves, &mut assignment);
            SlabJob {
                rows: ids,
                centers: leaves,
                assignment,
            }
        })
        .collect()
}
