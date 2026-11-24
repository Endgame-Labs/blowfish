//! Resident execution must preserve estimates and exact scores, and allocate nothing after warm-up.
use bf_vector::{
    BuildParams, Bytes, Candidate, Directory, Fragment, Index, IndexBuilder, Metric, ResidentIndex,
    ResidentQueryScratch, VectorType,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

struct CountAlloc;
thread_local! {
    static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}
// SAFETY: all allocation operations delegate unchanged to System. The thread-local counter has
// a const initializer and never allocates or accesses the allocated memory.
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        COUNT.with(|count| {
            if let Some(n) = count.get() {
                count.set(Some(n + 1));
            }
        });
        // SAFETY: the caller supplies a valid allocation layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout came from System.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        COUNT.with(|count| {
            if let Some(n) = count.get() {
                count.set(Some(n + 1));
            }
        });
        // SAFETY: the caller supplies a live allocation and a valid new size.
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOC: CountAlloc = CountAlloc;

fn rows(offset: u32, n: usize, dims: usize) -> Vec<(u32, Vec<f32>)> {
    (0..n)
        .map(|i| {
            (
                offset + i as u32,
                (0..dims)
                    .map(|j| {
                        if i % 71 == 0 {
                            0.
                        } else {
                            (((i * 7919 + j * 1013 + offset as usize) % 1009) as f32 - 500.) / 50.
                        }
                    })
                    .collect(),
            )
        })
        .collect()
}

async fn compile(
    metric: Metric,
    dtype: VectorType,
) -> (Directory, Vec<ResidentIndex>, Vec<Vec<Bytes>>) {
    let mut sources = Vec::new();
    let mut blobs = Vec::new();
    for s in 0..3 {
        let blob = IndexBuilder::new(64, metric)
            .unwrap()
            .vector_type(dtype)
            .params(BuildParams {
                flat_threshold: 16,
                clusters: Some(12),
                ..BuildParams::default()
            })
            .build(&rows(s * 1000, 700, 64))
            .unwrap();
        let (ann, meta) = Fragment::build_ann(&blob).unwrap();
        sources.push((
            format!("s{s}"),
            format!("k{s}"),
            format!("k{s}"),
            Fragment::decode(&ann[..meta as usize]).unwrap(),
        ));
        blobs.push(Bytes::from(ann));
    }
    let dir = Directory::decode(&Directory::build(&sources).unwrap().unwrap()).unwrap();
    let mut residents = Vec::new();
    let mut code_blocks = Vec::new();
    for (s, blob) in blobs.iter().enumerate() {
        let mut partial = dir.partial_index(s, Bytes::new()).unwrap();
        let mut codes = vec![Bytes::new(); dir.segments()[s].clusters];
        let mut vectors = codes.clone();
        for e in dir.segment_entries(s) {
            let slice = |r: std::ops::Range<u64>| blob.slice(r.start as usize..r.end as usize);
            partial.install(e.cluster, &slice(e.blob_range())).unwrap();
            codes[e.cluster] = slice(e.codes_range());
            vectors[e.cluster] = slice(e.vectors_range());
        }
        code_blocks.push(codes.clone());
        residents.push(
            partial
                .into_index()
                .into_resident(codes, Some(vectors))
                .unwrap(),
        );
    }
    (dir, residents, code_blocks)
}

#[tokio::test]
async fn identical_scans_distances_and_zero_allocations_after_warmup() {
    for metric in [Metric::CosineDistance, Metric::EuclideanSquared] {
        for dtype in [VectorType::F16, VectorType::F32] {
            let (dir, residents, codes) = compile(metric, dtype).await;
            let queries: Vec<_> = rows(19, 12, 64)
                .into_iter()
                .map(|(_, v)| v)
                .chain([vec![0.; 64]])
                .collect();
            let mut scratch = ResidentQueryScratch::default();
            let mut scan_scratch = bf_vector::ScanScratch::default();
            for query in &queries {
                let rotated = dir.rotate_query(query).unwrap();
                for (s, resident) in residents.iter().enumerate() {
                    let clusters: Vec<_> =
                        dir.segment_entries(s).iter().map(|e| e.cluster).collect();
                    let ordinary = resident
                        .index()
                        .scan_blocks_bounded_rotated(
                            &rotated,
                            &clusters,
                            clusters.iter().map(|&c| codes[s][c].clone()).collect(),
                            None,
                            10,
                        )
                        .unwrap();
                    let warm = resident
                        .scan(&rotated, &clusters, None, 10, &mut scan_scratch)
                        .unwrap();
                    assert_eq!(warm.len(), ordinary.len());
                    for (a, b) in warm.iter().zip(&ordinary) {
                        assert_eq!(
                            (a.row, a.distance, a.error, a.cluster, a.ordinal),
                            (b.row, b.distance, b.error, b.cluster, b.ordinal)
                        );
                    }
                }
                // A complete probe covers every row; bounded rerank must match exact f16/f32 truth.
                let mut truth = Vec::new();
                for s in 0..3 {
                    for (id, mut v) in rows(s * 1000, 700, 64) {
                        if dtype == VectorType::F16 {
                            for x in &mut v {
                                *x = bf_vector::f16::from_f32(*x).to_f32();
                            }
                        }
                        truth.push((id, metric.distance(query, &v).unwrap()));
                    }
                }
                truth.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                truth.truncate(10);
                assert_eq!(
                    dir.search_resident(&residents, query, 1000, 1000, 10, &mut scratch)
                        .unwrap(),
                    truth
                );
            }
            for _ in 0..2 {
                for query in &queries {
                    dir.search_resident(&residents, query, 1000, 1000, 10, &mut scratch)
                        .unwrap();
                }
            }
            COUNT.with(|count| count.set(Some(0)));
            for query in &queries {
                std::hint::black_box(
                    dir.search_resident(&residents, query, 1000, 1000, 10, &mut scratch)
                        .unwrap(),
                );
            }
            let allocations = COUNT.with(|count| count.replace(None).unwrap());
            assert_eq!(
                allocations, 0,
                "resident query allocated for {metric:?}/{dtype:?}"
            );
        }
    }
}

#[tokio::test]
async fn resident_admission_rejects_corruption_and_shape_errors() {
    let (dir, residents, _) = compile(Metric::CosineDistance, VectorType::F16).await;
    let mut scratch = ResidentQueryScratch::default();
    assert!(
        dir.search_resident(&residents, &[f32::NAN; 64], 48, 32, 10, &mut scratch)
            .is_err()
    );
    let blob = Bytes::from(
        IndexBuilder::new(64, Metric::CosineDistance)
            .unwrap()
            .params(BuildParams {
                flat_threshold: 16,
                ..BuildParams::default()
            })
            .build(&rows(0, 700, 64))
            .unwrap(),
    );
    let index = Index::open(blob).await.unwrap();
    assert!(index.into_resident(Vec::new(), None).is_err());
    let blob = IndexBuilder::new(64, Metric::CosineDistance)
        .unwrap()
        .params(BuildParams {
            flat_threshold: 16,
            ..Default::default()
        })
        .build(&rows(0, 700, 64))
        .unwrap();
    let (ann, meta) = Fragment::build_ann(&blob).unwrap();
    let ann = Bytes::from(ann);
    let fragment = Fragment::decode(&ann[..meta as usize]).unwrap();
    let one = Directory::decode(
        &Directory::build(&[("s".into(), "k".into(), "k".into(), fragment)])
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    for corrupt_vectors in [false, true] {
        let mut partial = one.partial_index(0, Bytes::new()).unwrap();
        let mut codes = vec![Bytes::new(); one.segments()[0].clusters];
        let mut vectors = codes.clone();
        for e in one.entries() {
            let slice = |r: std::ops::Range<u64>| ann.slice(r.start as usize..r.end as usize);
            partial.install(e.cluster, &slice(e.blob_range())).unwrap();
            codes[e.cluster] = slice(e.codes_range());
            vectors[e.cluster] = slice(e.vectors_range());
        }
        let target = if corrupt_vectors {
            &mut vectors
        } else {
            &mut codes
        };
        let bytes = target.iter_mut().find(|b| !b.is_empty()).unwrap();
        let mut corrupt = bytes.to_vec();
        corrupt[0] ^= 1;
        *bytes = corrupt.into();
        assert!(
            partial
                .into_index()
                .into_resident(codes, Some(vectors))
                .is_err()
        );
    }
    // Candidate addresses from callers cannot escape the vector arena.
    assert!(
        residents[0]
            .distance(
                &[0.; 64],
                &Candidate {
                    row: 0,
                    distance: 0.,
                    error: 0.,
                    cluster: u32::MAX,
                    ordinal: 0
                },
                &mut Vec::new()
            )
            .is_err()
    );
}
