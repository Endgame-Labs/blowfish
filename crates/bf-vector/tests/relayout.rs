//! Upgrading an index to the fast-scan layout changes only its codes: the result matches a fresh
//! build (up to the last bit of a recovered `alpha`), and queries answer the same.
use bf_vector::{
    Bytes, CodeLayout, Index, IndexBuilder, Metric, SearchParams, VectorType, code_layout,
    downgrade_layout, upgrade_layout,
};

fn rows(n: usize, dims: usize) -> Vec<(u32, Vec<f32>)> {
    (0..n)
        .map(|i| {
            let c = i % 24;
            let v = (0..dims)
                .map(|j| {
                    let center = ((c * 7919 + j * 1013) % 1009) as f32 / 50. - 10.;
                    let noise =
                        (((i + 1) * 104729 + (j + 1) * 15485863) % 10007) as f32 / 10007. - 0.5;
                    center + noise
                })
                .collect();
            (i as u32, v)
        })
        .collect()
}

#[tokio::test]
async fn upgrade_matches_a_fresh_build_and_answers_the_same() {
    for (dtype, metric, dims) in [
        (VectorType::F32, Metric::EuclideanSquared, 32),
        (VectorType::F16, Metric::CosineDistance, 40),
    ] {
        let mut data = rows(5000, dims);
        // The all-zero input is a special case in the codes.
        data.push((9999, vec![0.; dims]));
        let new = IndexBuilder::new(dims, metric)
            .unwrap()
            .vector_type(dtype)
            .build(&data)
            .unwrap();
        assert_eq!(code_layout(&new[..64]).unwrap(), CodeLayout::FastScan);
        assert!(upgrade_layout(&new).unwrap().is_none(), "already upgraded");
        let old = downgrade_layout(&new).unwrap().unwrap();
        assert_eq!(code_layout(&old[..64]).unwrap(), CodeLayout::RowMajor);
        assert_ne!(old.len(), new.len());
        assert!(downgrade_layout(&old).unwrap().is_none());
        let upgraded = upgrade_layout(&old).unwrap().unwrap();
        // The recovered alpha can differ from the original in its last bit; nothing else changes.
        assert_eq!(upgraded.len(), new.len());
        let differ = new.iter().zip(&upgraded).filter(|(a, b)| a != b).count();
        assert!(
            differ < new.len() / 20,
            "upgrade changes few bytes ({differ})"
        );
        let a = Index::open(Bytes::from(old)).await.unwrap();
        let b = Index::open(Bytes::from(upgraded)).await.unwrap();
        for q in [3usize, 777, 4000] {
            let query = &data[q].1;
            let p = SearchParams::default();
            let ra = a.search(query, 10, p, None).await.unwrap();
            let rb = b.search(query, 10, p, None).await.unwrap();
            assert_eq!(
                ra.iter().map(|r| r.0).collect::<Vec<_>>(),
                rb.iter().map(|r| r.0).collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn flat_indexes_are_left_alone() {
    let flat = IndexBuilder::new(8, Metric::EuclideanSquared)
        .unwrap()
        .build(&rows(50, 8))
        .unwrap();
    assert_eq!(code_layout(&flat[..64]).unwrap(), CodeLayout::Other);
    assert!(upgrade_layout(&flat).unwrap().is_none());
}
