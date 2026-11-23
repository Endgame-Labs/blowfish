//! The single ANN object preserves codes, vectors, filters, and exact results.
use bf_vector::{
    BuildParams, Bytes, Directory, Fragment, Index, IndexBuilder, Membership, Metric,
    RoaringBitmap, SearchParams, VectorType,
};

fn rows(n: usize, dims: usize, sparse: bool) -> Vec<(u32, Vec<f32>)> {
    (0..n)
        .map(|i| {
            (
                if sparse { i as u32 * 17 + 7 } else { i as u32 },
                (0..dims)
                    .map(|d| ((i * 7919 + d * 104729) as f64).sin() as f32)
                    .collect(),
            )
        })
        .collect()
}

#[tokio::test]
async fn single_object_preserves_payloads_and_filtered_results() {
    for metric in [Metric::CosineDistance, Metric::EuclideanSquared] {
        for dtype in [VectorType::F32, VectorType::F16, VectorType::I8] {
            let mut data = rows(1500, 32, false);
            if dtype == VectorType::I8 {
                for (_, v) in &mut data {
                    for x in v {
                        *x = (*x * 100.).round();
                    }
                }
            }
            let legacy = IndexBuilder::new(32, metric)
                .unwrap()
                .params(BuildParams {
                    flat_threshold: 64,
                    ..Default::default()
                })
                .vector_type(dtype)
                .build(&data)
                .unwrap();
            let old = Index::open(Bytes::from(legacy.clone())).await.unwrap();
            let (object, meta) = Fragment::build_ann(&legacy).unwrap();
            assert_eq!(bf_vector::metadata_len(&object).unwrap(), meta);
            assert_eq!(&object[..8], b"BFANN002");
            let new = Index::open_with_metadata_len(Bytes::from(object.clone()), meta)
                .await
                .unwrap();
            assert_eq!(old.len(), new.len());
            let dir = Directory::decode(
                &Directory::build(&[(
                    "s".into(),
                    "ann.bfa".into(),
                    "ann.bfa".into(),
                    Fragment::decode(&object[..meta as usize]).unwrap(),
                )])
                .unwrap()
                .unwrap(),
            )
            .unwrap();
            let seg = &dir.segments()[0];
            let member = seg.membership.clone().unwrap();
            assert!(member.end < meta, "membership is resident");
            let membership = Membership::decode(
                &object[member.start as usize..member.end as usize],
                seg.rows,
                seg.clusters,
            )
            .unwrap();
            let allowed: RoaringBitmap = (0..1500).filter(|r| r % 7 == 3).collect();
            assert_eq!(
                membership.counts(&allowed, seg.clusters),
                old.cluster_counts(Some(&allowed))
            );
            let partial = dir.partial_index(0, Bytes::from(object.clone())).unwrap();
            for cid in 0..old.cluster_count() {
                assert_eq!(
                    old.read_cluster_rows(cid).await.unwrap(),
                    new.read_cluster_rows(cid).await.unwrap()
                );
            }
            for e in dir.entries() {
                let old_codes = old.read_codes(&[e.cluster]).await.unwrap();
                let range = e.codes_range();
                assert_eq!(
                    old_codes[0].as_ref(),
                    &object[range.start as usize..range.end as usize]
                );
                assert_eq!(e.blob_range().end, e.codes_range().start);
                assert!(e.vectors_range().start >= meta);
            }
            for q in [1, 723] {
                for filter in [None, Some(&allowed)] {
                    let params = SearchParams {
                        nprobe: 16,
                        ..Default::default()
                    };
                    assert_eq!(
                        old.search(&data[q].1, 10, params, filter).await.unwrap(),
                        new.search(&data[q].1, 10, params, filter).await.unwrap()
                    );
                }
                assert_eq!(
                    old.exact_search(&data[q].1, 10, Some(&allowed))
                        .await
                        .unwrap(),
                    partial
                        .exact_at(&data[q].1, 10, membership.locations(&allowed, |_| true))
                        .await
                        .unwrap()
                );
            }
            let mut broken = object.clone();
            broken[dir.entries()[0].blob_range().start as usize] ^= 1;
            assert!(
                Index::open(Bytes::from(broken)).await.is_err(),
                "leaf blob CRC"
            );
            let mut broken = object.clone();
            broken[meta as usize - 5] ^= 1;
            assert!(Index::open(Bytes::from(broken)).await.is_err());
            let mut broken = object;
            let end = broken.len();
            broken[end - 1] ^= 1;
            assert!(Index::open(Bytes::from(broken)).await.is_err());
        }
    }
}

#[tokio::test]
async fn flat_empty_and_sparse_global_rows_round_trip() {
    for (n, sparse) in [(0, false), (10, false), (1500, true)] {
        let data = rows(n, 17, sparse);
        let legacy = IndexBuilder::new(17, Metric::EuclideanSquared)
            .unwrap()
            .build(&data)
            .unwrap();
        let old = Index::open(Bytes::from(legacy.clone())).await.unwrap();
        let (object, _) = Fragment::build_ann(&legacy).unwrap();
        let new = Index::open(Bytes::from(object)).await.unwrap();
        assert_eq!(old.len(), new.len());
        for c in 0..old.cluster_count() {
            assert_eq!(
                old.read_cluster_rows(c).await.unwrap(),
                new.read_cluster_rows(c).await.unwrap()
            );
        }
        assert_eq!(
            old.search(&[0.; 17], 10, SearchParams::default(), None)
                .await
                .unwrap(),
            new.search(&[0.; 17], 10, SearchParams::default(), None)
                .await
                .unwrap()
        );
    }
}
