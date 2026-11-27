//! `QueryResponse::write_id_rows` writes the bytes serde produces for the same response.
use bf_types::{Document, QueryBilling, QueryPerformance, QueryResponse, SingleQueryResult};
use serde_json::json;

fn general(rows: &[(u64, f64)], performance: &QueryPerformance) -> Vec<u8> {
    let rows = rows
        .iter()
        .map(|(id, dist)| {
            let mut row = Document::new();
            row.insert("id".into(), json!(id));
            row.insert("$dist".into(), json!(dist));
            row
        })
        .collect();
    serde_json::to_vec(&QueryResponse {
        result: SingleQueryResult {
            rows: Some(rows),
            ..Default::default()
        },
        results: None,
        performance: performance.clone(),
        billing: QueryBilling::default(),
    })
    .unwrap()
}

#[test]
fn id_rows_are_the_bytes_of_the_general_response() {
    let performance = QueryPerformance {
        cache_hit_ratio: 0.0,
        cache_temperature: "cold".into(),
        server_total_ms: 3,
        query_execution_ms: 2,
        exhaustive_search_count: 0,
        approx_namespace_size: 2_681_468,
        last_included_write_at: Some("2026-11-24T22:27:02.845363436Z".into()),
        ..Default::default()
    };
    let distances = [
        0.0,
        -0.0,
        1.0,
        0.27615076303482056,
        f64::from(0.1f32),
        f64::from(f32::MIN_POSITIVE),
        f64::from(f32::MAX),
        1e-7,
        1e21,
        123456789.125,
        2.0,
    ];
    for take in 0..=distances.len() {
        let rows: Vec<(u64, f64)> = distances[..take]
            .iter()
            .enumerate()
            .map(|(i, d)| {
                (
                    [0, 7, u64::MAX, 1_821_785, 9_007_199_254_740_993][i % 5],
                    *d,
                )
            })
            .collect();
        let mut out = Vec::new();
        assert!(QueryResponse::write_id_rows(&mut out, &rows, &performance));
        assert_eq!(out, general(&rows, &performance), "{take} rows");
    }
    let mut without = performance.clone();
    without.last_included_write_at = None;
    let mut out = Vec::new();
    assert!(QueryResponse::write_id_rows(
        &mut out,
        &[(1, 0.5)],
        &without
    ));
    assert_eq!(out, general(&[(1, 0.5)], &without));
}

#[test]
fn a_non_finite_distance_writes_nothing() {
    let mut out = b"keep".to_vec();
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(!QueryResponse::write_id_rows(
            &mut out,
            &[(1, 0.5), (2, bad)],
            &QueryPerformance::default()
        ));
        assert_eq!(out, b"keep");
    }
}
