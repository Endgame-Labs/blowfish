//! One-bit directory centers rank clusters like exact centers do, and fragments round-trip.
use bf_vector::{
    BuildParams, Bytes, Directory, Fragment, Index, IndexBuilder, Membership, Metric, RoaringBitmap,
};

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
    let again = Directory::build(&[("s0".into(), "k0".into(), "f0".into(), dir.fragment_of(0))])
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

#[tokio::test]
async fn membership_gives_exact_cluster_counts_and_vector_locations() {
    let dims = 64;
    let builder = IndexBuilder::new(dims, Metric::EuclideanSquared)
        .unwrap()
        .params(BuildParams {
            flat_threshold: 64,
            ..BuildParams::default()
        });
    let data = rows(3000, dims, 5);
    let blob = builder.build(&data).unwrap();
    let (fragment, meta) = Fragment::build(&blob).unwrap();
    let frag = Fragment::decode(&fragment[..meta as usize]).unwrap();
    let bytes = Directory::build(&[("s".into(), "k".into(), "f".into(), frag)])
        .unwrap()
        .unwrap();
    let dir = Directory::decode(&bytes).unwrap();
    let seg = &dir.segments()[0];
    let range = seg.membership.clone().expect("map for a clustered segment");
    let membership = Membership::decode(
        &fragment[range.start as usize..range.end as usize],
        seg.rows,
        seg.clusters,
    )
    .unwrap();
    let index = Index::open(Bytes::from(blob.clone())).await.unwrap();
    let allowed: RoaringBitmap = (0..3000u32).filter(|r| r % 7 == 3).collect();
    let counts = membership.counts(&allowed, seg.clusters);
    assert_eq!(counts.iter().map(|c| *c as u64).sum::<u64>(), allowed.len());
    assert_eq!(counts, index.cluster_counts(Some(&allowed)));
    // Exact ranking from locations equals the index's own exact search over the same rows.
    let query = rows(1, dims, 11)[0].1.clone();
    let expected = index
        .exact_search(&query, 10, Some(&allowed))
        .await
        .unwrap();
    let partial = dir.partial_index(0, Bytes::from(blob)).unwrap();
    let locations = membership.locations(&allowed, |_| true);
    assert_eq!(locations.len() as u64, allowed.len());
    let got = partial.exact_at(&query, 10, locations).await.unwrap();
    assert_eq!(got, expected);
}

#[tokio::test]
async fn block_fragments_hold_blob_and_codes_together_and_scan_like_the_index() {
    let dims = 64;
    let builder = IndexBuilder::new(dims, Metric::CosineDistance)
        .unwrap()
        .params(BuildParams {
            flat_threshold: 64,
            ..BuildParams::default()
        });
    let blob = builder.build(&rows(4000, dims, 9)).unwrap();
    let index = Index::open(Bytes::from(blob.clone())).await.unwrap();
    let (fragment, meta) = Fragment::build_blocks(&blob).unwrap();
    assert!(meta < fragment.len() as u64);
    let frag = Fragment::decode(&fragment[..meta as usize]).unwrap();
    let bytes = Directory::build(&[("s".into(), "k".into(), "f".into(), frag)])
        .unwrap()
        .unwrap();
    let dir = Directory::decode(&bytes).unwrap();
    let seg = &dir.segments()[0];
    assert!(seg.codes_in_fragment);
    assert!(seg.membership.is_none());
    // Each block is the blob followed by the codes, so one range covers both.
    for e in dir.entries() {
        assert_eq!(e.blob_range().end, e.codes_range().start);
    }
    // A plain fragment and a block fragment cannot share a directory.
    let (plain, plain_meta) = Fragment::build(&blob).unwrap();
    let mixed = Directory::build(&[
        (
            "a".into(),
            "k".into(),
            "f".into(),
            Fragment::decode(&fragment[..meta as usize]).unwrap(),
        ),
        (
            "b".into(),
            "k".into(),
            "f".into(),
            Fragment::decode(&plain[..plain_meta as usize]).unwrap(),
        ),
    ])
    .unwrap();
    assert!(mixed.is_none());
    // Probing clusters through the fragment gives the index's own estimates.
    let query: Vec<f32> = rows(1, dims, 5)[0].1.clone();
    let wanted: Vec<usize> = dir.entries().iter().take(6).map(|e| e.cluster).collect();
    let mut partial = dir.partial_index(0, Bytes::from(blob)).unwrap();
    let mut codes = Vec::new();
    for e in dir.entries().iter().take(6) {
        let b = e.blob_range();
        partial
            .install(
                e.cluster,
                &Bytes::copy_from_slice(&fragment[b.start as usize..b.end as usize]),
            )
            .unwrap();
        let c = e.codes_range();
        codes.push(Bytes::copy_from_slice(
            &fragment[c.start as usize..c.end as usize],
        ));
    }
    let got = partial
        .index()
        .scan_blocks(&query, &wanted, codes, None)
        .unwrap();
    let expected = index.scan_clusters(&query, &wanted, None).await.unwrap();
    assert_eq!(got.len(), expected.len());
    for (a, b) in got.iter().zip(&expected) {
        assert_eq!((a.row, a.cluster, a.ordinal), (b.row, b.cluster, b.ordinal));
        assert_eq!(a.distance, b.distance);
    }
}

#[tokio::test]
async fn paged_directory_round_trips_and_selects_the_nearest_cells() {
    let dims = 64;
    let builder = IndexBuilder::new(dims, Metric::EuclideanSquared)
        .unwrap()
        .params(BuildParams {
            flat_threshold: 64,
            ..BuildParams::default()
        });
    let mut sources = Vec::new();
    for s in 0..3 {
        let blob = builder.build(&rows(2000, dims, s)).unwrap();
        let (fragment, meta) = Fragment::build_blocks(&blob).unwrap();
        let frag = Fragment::decode(&fragment[..meta as usize]).unwrap();
        sources.push((format!("s{s}"), format!("k{s}"), format!("f{s}"), frag));
    }
    let dir = Directory::decode(&Directory::build(&sources).unwrap().unwrap()).unwrap();
    let total = dir.entries().len();
    let (object, header_len) = dir.build_paged(25, 7).unwrap();
    assert!(bf_vector::PagedDirectory::is_paged(&object));
    let paged = bf_vector::PagedDirectory::decode(&object[..header_len as usize]).unwrap();
    assert_eq!(paged.segments().len(), 3);
    assert_eq!(paged.totals().1 as usize, total);
    let page = |c: usize| {
        let r = paged.page_range(c);
        std::sync::Arc::new(
            paged
                .decode_page(&object[r.start as usize..r.end as usize])
                .unwrap(),
        )
    };
    // Every cell together reproduce the flat directory: same entries, same estimated distances.
    let query: Vec<f32> = rows(1, dims, 4)[0].1.clone();
    let all = paged.select_cells(&query, usize::MAX).unwrap();
    assert_eq!(all.len(), paged.cell_count());
    let pages: Vec<_> = all.iter().map(|&c| page(c)).collect();
    let whole = paged.assemble(&pages);
    assert_eq!(whole.entries().len(), total);
    assert_eq!(whole.segment_totals(), dir.segment_totals());
    let key = |e: &bf_vector::DirEntry| (e.segment, e.cluster);
    let mut want: Vec<_> = dir
        .entries()
        .iter()
        .zip(dir.center_distances(&query).unwrap())
        .map(|(e, d)| (key(e), d))
        .collect();
    let mut got: Vec<_> = whole
        .entries()
        .iter()
        .zip(whole.center_distances(&query).unwrap())
        .map(|(e, d)| (key(e), d))
        .collect();
    want.sort_by_key(|x| x.0);
    got.sort_by_key(|x| x.0);
    assert_eq!(want, got);
    // A small request reads a few cells, and the nearest cluster is among their entries.
    let few = paged.select_cells(&query, 1).unwrap();
    assert_eq!(few.len(), 1);
    let nearest = want
        .iter()
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .map(|x| x.0)
        .unwrap();
    let wide = paged.select_cells(&query, total / 2).unwrap();
    let partial = paged.assemble(&wide.iter().map(|&c| page(c)).collect::<Vec<_>>());
    assert!(partial.entries().len() < total);
    assert!(partial.entries().iter().any(|e| key(e) == nearest));
    assert_eq!(partial.segment_totals(), dir.segment_totals());
    // Corruption is detected.
    let mut bad = object[..header_len as usize].to_vec();
    bad[20] ^= 1;
    assert!(bf_vector::PagedDirectory::decode(&bad).is_err());
}
