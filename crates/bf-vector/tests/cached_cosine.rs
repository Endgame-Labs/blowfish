use bf_vector::{
    BuildParams, Bytes, CosineNorm, CosineQuery, Index, IndexBuilder, Metric, VectorType,
};

#[tokio::test]
async fn cached_norm_rerank_preserves_order_bits_checksums_and_bounds() {
    for dims in [7, 16, 65, 1024] {
        let vectors = (0..80u32)
            .map(|row| {
                (
                    row * 3 + 19,
                    (0..dims)
                        .map(|d| {
                            half::f16::from_f32(((row as usize * 37 + d * 53) as f32 * 0.17).sin())
                                .to_f32()
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let blob = IndexBuilder::new(dims, Metric::CosineDistance)
            .unwrap()
            .vector_type(VectorType::F16)
            .params(BuildParams {
                flat_threshold: 0,
                clusters: Some(4),
                ..Default::default()
            })
            .build(&vectors)
            .unwrap();
        let index = Index::open(Bytes::from(blob.clone())).await.unwrap();
        let mut candidates = Vec::new();
        for (cid, ordinal) in index.locations(None) {
            let (rows, _) = index.read_cluster_rows(cid as usize).await.unwrap();
            let id = rows[ordinal as usize];
            let vector = &vectors.iter().find(|v| v.0 == id).unwrap().1;
            candidates.push((
                cid as usize,
                ordinal as usize,
                CosineNorm::new(vector).unwrap(),
            ));
        }
        // Reversed locations and duplicate candidates retain the ordinary rerank semantics.
        candidates.reverse();
        candidates.push(candidates[0]);
        for scale in [0., 1., f32::MAX, f32::from_bits(1)] {
            let query = vectors[13].1.iter().map(|x| x * scale).collect::<Vec<_>>();
            let prepared = CosineQuery::new(&query).unwrap();
            let expected = index
                .rerank_candidates(
                    &query,
                    30,
                    candidates
                        .iter()
                        .map(|c| (c.0 as u32, c.1 as u32))
                        .collect(),
                )
                .await
                .unwrap();
            let got = index
                .rerank_cosine_candidates(&prepared, 30, candidates.clone())
                .await
                .unwrap();
            assert_eq!(
                got.iter()
                    .map(|(r, d)| (*r, d.to_bits()))
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|(r, d)| (*r, d.to_bits()))
                    .collect::<Vec<_>>()
            );
            assert!(
                index
                    .rerank_cosine_candidates(&prepared, 10, vec![(usize::MAX, 0, candidates[0].2)])
                    .await
                    .is_err()
            );
            assert!(
                index
                    .rerank_cosine_candidates(&prepared, 10, vec![(0, usize::MAX, candidates[0].2)])
                    .await
                    .is_err()
            );
        }
        // A checksum failure is still surfaced before using the cached norm.
        let (rows, bytes) = index.read_cluster_rows(0).await.unwrap();
        let begin = blob
            .windows(bytes.len())
            .position(|b| b == bytes.as_ref())
            .unwrap();
        let mut corrupt = blob;
        corrupt[begin] ^= 1;
        let bad = Index::open(Bytes::from(corrupt)).await.unwrap();
        let query = CosineQuery::new(&vectors[0].1).unwrap();
        let norm = CosineNorm::new(&vectors.iter().find(|v| v.0 == rows[0]).unwrap().1).unwrap();
        assert!(
            bad.rerank_cosine_candidates(&query, 1, vec![(0, 0, norm)])
                .await
                .is_err()
        );
    }
}
