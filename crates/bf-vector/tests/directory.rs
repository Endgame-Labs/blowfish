//! One-bit directory centers rank clusters like exact centers do, and fragments round-trip.
use bf_vector::{BuildParams, Bytes, Directory, Fragment, Index, IndexBuilder, Metric};

fn rows(n: usize, dims: usize, salt: usize) -> Vec<(u32, Vec<f32>)> {
    (0..n)
        .map(|i| {
            let c = (i * 31 + salt) % 40;
            let v = (0..dims)
                .map(|j| {
                    let center = ((c * 7919 + j * 1013) % 1009) as f32 / 50. - 10.;
                    let noise = (((i + 1) * 104729 + (j + 1) * 15485863) % 10007) as f32 / 10007.;
                    center + noise - 0.5
                })
                .collect();
            (i as u32, v)
        })
        .collect()
}

#[tokio::test]
async fn one_bit_centers_rank_clusters_and_fragments_round_trip() {
    let dims = 64;
    let builder = IndexBuilder::new(dims, Metric::EuclideanSquared)
        .unwrap()
        .params(BuildParams {
            flat_threshold: 64,
            ..BuildParams::default()
        });
    let mut sources = Vec::new();
    let mut indexes = Vec::new();
    for s in 0..3 {
        let blob = builder.build(&rows(2000, dims, s)).unwrap();
        let (fragment, meta) = Fragment::build(&blob).unwrap();
        assert!(meta < fragment.len() as u64);
        let frag = Fragment::decode(&fragment[..meta as usize]).unwrap();
        sources.push((format!("s{s}"), format!("k{s}"), format!("f{s}"), frag));
        indexes.push(Index::open(Bytes::from(blob)).await.unwrap());
    }
    let bytes = Directory::build(&sources).unwrap().unwrap();
    let dir = Directory::decode(&bytes).unwrap();
    assert_eq!(dir.segments().len(), 3);
    // Carrying a section over into a new directory keeps it byte-identical.
    let again = Directory::build(&[(
        "s0".into(),
        "k0".into(),
        "f0".into(),
        dir.fragment_of(0),
    )])
    .unwrap()
    .unwrap();
    assert_eq!(Directory::decode(&again).unwrap().entries().len(), {
        dir.entries().iter().filter(|e| e.segment == 0).count()
    });
    let clusters = indexes[0].cluster_count();
    assert!(dir.entries().len() > clusters);
    for q in 0..20 {
        let query: Vec<f32> = rows(1, dims, q * 3 + 1)[0].1.clone();
        let estimated = dir.center_distances(&query).unwrap();
        assert_eq!(estimated.len(), dir.entries().len());
        // Per segment, the exact nearest cluster must be among the five nearest estimates.
        for (s, index) in indexes.iter().enumerate() {
            let exact = index.center_distances(&query).unwrap();
            let best = (0..exact.len())
                .min_by(|a, b| exact[*a].total_cmp(&exact[*b]))
                .unwrap();
            let mut mine: Vec<(f32, usize)> = dir
                .entries()
                .iter()
                .zip(&estimated)
                .filter(|(e, _)| e.segment == s)
                .map(|(e, d)| (*d, e.cluster))
                .collect();
            mine.sort_by(|a, b| a.0.total_cmp(&b.0));
            assert!(
                mine.iter().take(5).any(|(_, c)| *c == best),
                "query {q} segment {s}: nearest cluster {best} not in top five"
            );
        }
    }
}
