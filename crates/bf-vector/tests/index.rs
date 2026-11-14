use std::{
    io,
    ops::Range,
    sync::{Arc, Mutex},
};

use bf_vector::{
    BuildParams, Bytes, Index, IndexBuilder, Metric, RangeRead, RoaringBitmap, SearchParams,
    VectorType, exact_search, metadata_len, recall_at_k,
};
use proptest::prelude::*;

struct Counted {
    bytes: Bytes,
    batches: Mutex<Vec<Vec<Range<u64>>>>,
}
impl RangeRead for Counted {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        self.bytes.read(range).await
    }
    async fn read_batch(&self, ranges: Vec<Range<u64>>) -> io::Result<Vec<Bytes>> {
        self.batches.lock().unwrap().push(ranges.clone());
        let mut result = Vec::new();
        for range in ranges {
            result.push(self.read(range).await?);
        }
        Ok(result)
    }
}

fn clustered(n: usize, dims: usize) -> Vec<(u32, Vec<f32>)> {
    (0..n)
        .map(|i| {
            let c = i % 32;
            let v = (0..dims)
                .map(|j| {
                    let center = ((c * 7919 + j * 1013) % 1009) as f32 / 50. - 10.;
                    let noise =
                        (((i + 1) * 104729 + (j + 1) * 15485863) % 10007) as f32 / 10007. - 0.5;
                    center + noise
                })
                .collect();
            ((i as u32) * 7 + 3, v)
        })
        .collect()
}

#[tokio::test]
async fn round_trip_types_metrics_and_empty() {
    for metric in [Metric::CosineDistance, Metric::EuclideanSquared] {
        for dtype in [VectorType::F32, VectorType::F16] {
            let mut data = clustered(100, 17);
            data.push((u32::MAX, vec![0.; 17]));
            let blob = IndexBuilder::new(17, metric)
                .unwrap()
                .vector_type(dtype)
                .build(&data)
                .unwrap();
            let index = Index::open(Bytes::from(blob)).await.unwrap();
            assert_eq!(index.len(), 101);
            assert!(index.is_flat());
            assert_eq!(index.vector_type(), dtype);
            let stored: Vec<_> = data
                .iter()
                .map(|(r, v)| {
                    (
                        *r,
                        v.iter()
                            .map(|x| {
                                if dtype == VectorType::F16 {
                                    bf_vector::f16::from_f32(*x).to_f32()
                                } else {
                                    *x
                                }
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect();
            for q in [&data[3].1, &vec![0.; 17]] {
                assert_eq!(
                    index
                        .search(q, 10, SearchParams::default(), None)
                        .await
                        .unwrap(),
                    exact_search(&stored, q, 10, metric, None).unwrap()
                );
            }
            let empty: Vec<(u32, Vec<f32>)> = vec![];
            let blob = IndexBuilder::new(17, metric)
                .unwrap()
                .build(&empty)
                .unwrap();
            let index = Index::open(blob).await.unwrap();
            assert!(index.is_empty());
            assert!(
                index
                    .search(&[0.; 17], 10, SearchParams::default(), None)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }
}

#[tokio::test]
async fn ivf_recall_filtering_and_determinism() {
    let data = clustered(6000, 64);
    for metric in [Metric::CosineDistance, Metric::EuclideanSquared] {
        let builder = IndexBuilder::new(64, metric).unwrap();
        let blob = builder.build(&data).unwrap();
        assert_eq!(blob, builder.build(&data).unwrap());
        let index = Index::open(blob).await.unwrap();
        assert!(!index.is_flat());
        let mut recall = 0.;
        for i in 0..20 {
            let q: Vec<_> = data[i * 17]
                .1
                .iter()
                .enumerate()
                .map(|(j, x)| x + ((j % 3) as f32 - 1.) * 0.15)
                .collect();
            let ann = index
                .search(&q, 10, SearchParams::default(), None)
                .await
                .unwrap();
            let exact = exact_search(&data, &q, 10, metric, None).unwrap();
            recall += recall_at_k(&ann, &exact);
            for (row, distance) in ann {
                assert_eq!(
                    distance,
                    metric
                        .distance(&q, &data[(row as usize - 3) / 7].1)
                        .unwrap()
                );
            }
        }
        assert!(recall / 20. >= 0.95, "recall {}", recall / 20.);
        for stride in [1, 2, 31, 1000] {
            let mut filter: RoaringBitmap = data.iter().step_by(stride).map(|x| x.0).collect();
            filter.insert(0); // Unknown row IDs must not affect search.
            let q = &data[543].1;
            let ann = index
                .search(
                    q,
                    10,
                    SearchParams {
                        nprobe: 2,
                        rerank_factor: 64,
                    },
                    Some(&filter),
                )
                .await
                .unwrap();
            let exact = exact_search(&data, q, 10, metric, Some(&filter)).unwrap();
            assert_eq!(ann.len(), exact.len());
            assert!(
                recall_at_k(&ann, &exact) >= 0.95,
                "stride {stride}, metric {metric:?}"
            );
            if stride >= 31 {
                assert_eq!(ann, exact);
            }
        }
    }
}

#[tokio::test]
async fn tree_and_single_round_metadata() {
    let data = clustered(5000, 32);
    let params = BuildParams {
        flat_threshold: 0,
        two_level_threshold: 4000,
        ..Default::default()
    };
    let blob = IndexBuilder::new(32, Metric::EuclideanSquared)
        .unwrap()
        .params(params)
        .build(&data)
        .unwrap();
    let len = metadata_len(&blob).unwrap();
    let source = Arc::new(Counted {
        bytes: Bytes::from(blob),
        batches: Mutex::new(vec![]),
    });
    let index = Index::open_with_metadata_len(source.clone(), len)
        .await
        .unwrap();
    assert!(index.parent_count() > 0);
    assert_eq!(source.batches.lock().unwrap().len(), 1);
    assert_eq!(source.batches.lock().unwrap()[0].len(), 2);
    let q = &data[123].1;
    let got = index
        .search(q, 10, SearchParams::default(), None)
        .await
        .unwrap();
    assert!(
        recall_at_k(
            &got,
            &exact_search(&data, q, 10, Metric::EuclideanSquared, None).unwrap()
        ) >= 0.95
    );
    assert_eq!(source.batches.lock().unwrap().len(), 3); // Metadata, codes, and rerank.
    let filter: RoaringBitmap = [data[11].0, data[101].0].into_iter().collect();
    let before = source.batches.lock().unwrap().len();
    assert_eq!(
        index
            .search(q, 10, SearchParams::default(), Some(&filter))
            .await
            .unwrap(),
        exact_search(&data, q, 10, Metric::EuclideanSquared, Some(&filter)).unwrap()
    );
    let batches = source.batches.lock().unwrap();
    assert_eq!(batches.len(), before + 1); // Selective exact search reads no codes.
    let bytes: u64 = batches
        .last()
        .unwrap()
        .iter()
        .map(|r| r.end - r.start)
        .sum();
    assert_eq!(bytes, 2 * 32 * 4);
}

#[tokio::test]
async fn full_probes_and_rerank_are_exact() {
    let data = clustered(3000, 19);
    let params = BuildParams {
        flat_threshold: 0,
        clusters: Some(31),
        ..Default::default()
    };
    let blob = IndexBuilder::new(19, Metric::CosineDistance)
        .unwrap()
        .params(params)
        .build(&data)
        .unwrap();
    let index = Index::open(blob).await.unwrap();
    let got = index
        .search(
            &data[777].1,
            12,
            SearchParams {
                nprobe: usize::MAX,
                rerank_factor: usize::MAX,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        got,
        exact_search(&data, &data[777].1, 12, Metric::CosineDistance, None).unwrap()
    );
}

#[tokio::test]
async fn corruption_and_invalid_inputs() {
    let data = clustered(20, 8);
    let b = IndexBuilder::new(8, Metric::CosineDistance).unwrap();
    assert!(b.build(&[(1, vec![0.; 8]), (1, vec![1.; 8])]).is_err());
    assert!(b.build(&[(1, vec![f32::NAN; 8])]).is_err());
    assert!(b.build(&[(1, vec![1.; 7])]).is_err());
    assert!(
        b.clone()
            .vector_type(VectorType::F16)
            .build(&[(1, vec![f32::MAX; 8])])
            .is_err()
    );
    let blob = b.build(&data).unwrap();
    for len in [0, 1, 63, blob.len() - 1] {
        assert!(Index::open(blob[..len].to_vec()).await.is_err());
    }
    let mut corrupt = blob.clone();
    corrupt[12] ^= 1;
    assert!(Index::open(corrupt).await.is_err());
    let mut corrupt = blob.clone();
    let end = corrupt.len() - 4;
    corrupt[end] ^= 1;
    assert!(Index::open(corrupt).await.is_err());
    let mut corrupt = blob.clone();
    let meta = metadata_len(&blob).unwrap() as usize;
    corrupt[meta] ^= 1;
    let index = Index::open(corrupt).await.unwrap();
    assert!(index.exact_search(&data[0].1, 10, None).await.is_err());
    let index = Index::open(blob).await.unwrap();
    assert!(
        index
            .search(&[0.; 7], 10, SearchParams::default(), None)
            .await
            .is_err()
    );
    assert!(
        index
            .search(
                &[0.; 8],
                10,
                SearchParams {
                    nprobe: 0,
                    rerank_factor: 1
                },
                None
            )
            .await
            .is_err()
    );
}

struct Short;
impl RangeRead for Short {
    fn len(&self) -> u64 {
        1000
    }
    async fn read(&self, _range: Range<u64>) -> io::Result<Bytes> {
        Ok(Bytes::new())
    }
}
#[tokio::test]
async fn short_reads_are_errors() {
    assert!(Index::open(Short).await.is_err());
    assert!(Index::open_with_metadata_len(Short, 100).await.is_err());
    assert!(vec![0u8; 8].read(9..10).await.is_err());
    assert!(
        Bytes::from_static(&[0; 8])
            .read(Range { start: 7, end: 1 })
            .await
            .is_err()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn exact_index_matches_independent_naive(
        data in prop::collection::vec(prop::array::uniform5(-30f32..30.), 0..80),
        query in prop::array::uniform5(-30f32..30.), k in 0usize..100, use_filter in any::<bool>(),
    ) {
        futures::executor::block_on(async {
            let rows: Vec<_> = data.into_iter().enumerate().map(|(i,v)|((i as u32)*3,v)).collect();
            let filter: RoaringBitmap = rows.iter().step_by(3).map(|x|x.0).collect();
            let filter = use_filter.then_some(&filter);
            let blob = IndexBuilder::new(5,Metric::EuclideanSquared).unwrap().build(&rows).unwrap();
            let index = Index::open(blob).await.unwrap();
            let got = index.exact_search(&query,k,filter).await.unwrap();
            let mut naive: Vec<_> = rows.iter().filter(|(r,_)|filter.is_none_or(|f|f.contains(*r))).map(|(r,v)| {
                (*r,v.iter().zip(query).map(|(a,b)|(*a as f64-b as f64).powi(2)).sum::<f64>() as f32)
            }).collect();
            naive.sort_unstable_by(|a,b|a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            naive.truncate(k);
            assert_eq!(got.iter().map(|x|x.0).collect::<Vec<_>>(),naive.iter().map(|x|x.0).collect::<Vec<_>>());
            for (a,b) in got.iter().zip(&naive) {assert!((a.1-b.1).abs()<=1e-4*b.1.max(1.));}
        });
    }
}

#[tokio::test]
async fn half_ivf_and_identical_vectors() {
    let input = clustered(2300, 33);
    let half_rows: Vec<_> = input
        .iter()
        .map(|(r, v)| {
            (
                *r,
                v.iter()
                    .map(|x| bf_vector::f16::from_f32(*x))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let builder = IndexBuilder::new(33, Metric::CosineDistance)
        .unwrap()
        .vector_type(VectorType::F16);
    let blob = builder.build_f16(&half_rows).unwrap();
    let stored: Vec<_> = half_rows
        .iter()
        .map(|(r, v)| (*r, v.iter().map(|x| x.to_f32()).collect::<Vec<_>>()))
        .collect();
    let index = Index::open(blob).await.unwrap();
    let exact = exact_search(&stored, &input[317].1, 10, Metric::CosineDistance, None).unwrap();
    let ann = index
        .search(&input[317].1, 10, SearchParams::default(), None)
        .await
        .unwrap();
    assert!(recall_at_k(&ann, &exact) >= 0.95);
    let identical: Vec<_> = (0..2100).map(|r| (r, vec![1.; 3])).collect();
    let blob = IndexBuilder::new(3, Metric::EuclideanSquared)
        .unwrap()
        .build(&identical)
        .unwrap();
    let index = Index::open(blob).await.unwrap();
    let result = index
        .search(&[1.; 3], 10, SearchParams::default(), None)
        .await
        .unwrap();
    assert_eq!(result, (0..10).map(|r| (r, 0.)).collect::<Vec<_>>());
}

/// Supply raw row-major f32-LE files from a public embedding corpus. This is
/// opt-in so CI neither downloads data nor silently substitutes synthetic data.
#[test]
#[ignore = "requires BF_VECTOR_REAL_VECTORS and BF_VECTOR_REAL_QUERIES, both raw 100-dimensional f32-LE"]
fn real_embedding_recall() {
    fn load(name: &str) -> Vec<(u32, Vec<f32>)> {
        let bytes = std::fs::read(std::env::var(name).expect(name)).unwrap();
        assert_eq!(bytes.len() % 400, 0);
        bytes
            .chunks(400)
            .enumerate()
            .map(|(i, chunk)| {
                (
                    i as u32,
                    chunk
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .map(|b| f32::from_le_bytes(*b))
                        .collect(),
                )
            })
            .collect()
    }
    let rows = load("BF_VECTOR_REAL_VECTORS");
    let queries = load("BF_VECTOR_REAL_QUERIES");
    assert!(rows.len() >= 100_000);
    assert!(queries.len() >= 100);
    futures::executor::block_on(async {
        let blob = IndexBuilder::new(100, Metric::CosineDistance)
            .unwrap()
            .build(&rows)
            .unwrap();
        let index = Index::open(blob).await.unwrap();
        let mut recall = 0.;
        for (_, q) in queries.iter().take(100) {
            let ann = index
                .search(q, 10, SearchParams::default(), None)
                .await
                .unwrap();
            let exact = exact_search(&rows, q, 10, Metric::CosineDistance, None).unwrap();
            recall += recall_at_k(&ann, &exact);
        }
        let recall = recall / 100.;
        eprintln!("public embedding recall@10: {recall:.4}");
        assert!(recall >= 0.95, "recall@10 {recall:.4} is below 0.95");
    });
}

#[tokio::test]
async fn corrupt_codes_and_footer_version() {
    let data = clustered(2100, 8);
    let builder = IndexBuilder::new(8, Metric::EuclideanSquared).unwrap();
    let blob = builder.build(&data).unwrap();
    let meta = metadata_len(&blob).unwrap() as usize;
    let mut corrupt = blob.clone();
    corrupt[meta] ^= 1;
    let index = Index::open(corrupt).await.unwrap();
    assert!(
        index
            .search(
                &data[9].1,
                10,
                SearchParams {
                    nprobe: usize::MAX,
                    rerank_factor: 1
                },
                None
            )
            .await
            .is_err()
    );
    let mut future_version = blob;
    let offset = future_version.len() - 64 + 8;
    future_version[offset..offset + 4].copy_from_slice(&2u32.to_le_bytes());
    assert!(Index::open(future_version).await.is_err());
}

#[tokio::test]
async fn signed_i8_storage_roundtrip_and_exact_scores() {
    use bf_vector::{Bytes, Index, IndexBuilder, Metric, VectorType};
    let data = vec![
        (0, vec![-128.0, 127.0]),
        (1, vec![1.0, 2.0]),
        (2, vec![3.0, 4.0]),
    ];
    let builder = IndexBuilder::new(2, Metric::EuclideanSquared)
        .unwrap()
        .vector_type(VectorType::I8);
    let bytes = builder.build(&data).unwrap();
    let index = Index::open(Bytes::from(bytes)).await.unwrap();
    assert_eq!(index.vector_type(), VectorType::I8);
    assert_eq!(
        index.exact_search(&[1.0, 2.0], 3, None).await.unwrap(),
        vec![(1, 0.0), (2, 8.0), (0, 32266.0)]
    );
    for bad in [-129.0, 128.0, 0.5] {
        assert!(builder.build(&[(0, vec![bad, 0.0])]).is_err());
    }
}
