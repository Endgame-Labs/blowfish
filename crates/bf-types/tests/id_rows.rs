//! `QueryResponse::write_id_rows` writes the bytes serde produces for the same response.
use bf_types::{Document, IdRef, QueryBilling, QueryPerformance, QueryResponse, SingleQueryResult};
use serde_json::json;

fn general(rows: &[(IdRef, f64)], performance: &QueryPerformance) -> Vec<u8> {
    let rows = rows
        .iter()
        .map(|(id, dist)| {
            let mut row = Document::new();
            row.insert("id".into(), id.to_id().value());
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
        let rows: Vec<(IdRef, f64)> = distances[..take]
            .iter()
            .enumerate()
            .map(|(i, d)| {
                (
                    match i % 7 {
                        5 => IdRef::Uuid(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
                        6 => IdRef::Uuid(u128::MAX),
                        n => IdRef::Uint([0, 7, u64::MAX, 1_821_785, 9_007_199_254_740_993][n]),
                    },
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
        &[(IdRef::Uint(1), 0.5)],
        &without
    ));
    assert_eq!(out, general(&[(IdRef::Uint(1), 0.5)], &without));
}

#[test]
fn a_non_finite_distance_writes_nothing() {
    let mut out = b"keep".to_vec();
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(!QueryResponse::write_id_rows(
            &mut out,
            &[(IdRef::Uint(1), 0.5), (IdRef::Uuid(2), bad)],
            &QueryPerformance::default()
        ));
        assert_eq!(out, b"keep");
    }
}

#[test]
fn uuid_text_round_trips_and_orders_like_the_text() {
    let ids = [
        0u128,
        1,
        0x0123_4567_89ab_cdef_0123_4567_89ab_cdef,
        u128::MAX,
    ];
    for id in ids {
        let text = String::from_utf8(IdRef::uuid_text(id).to_vec()).unwrap();
        assert_eq!(text, uuid::Uuid::from_u128(id).to_string());
        assert_eq!(IdRef::parse_uuid(&text), Some(id));
    }
    for bad in [
        "0123456789abcdef0123456789abcdef",
        "01234567-89AB-cdef-0123-456789abcdef",
        "01234567-89ab-cdef-0123-456789abcde",
        "01234567_89ab-cdef-0123-456789abcdef",
        "0123456g-89ab-cdef-0123-456789abcdef",
        "",
    ] {
        assert_eq!(IdRef::parse_uuid(bad), None, "{bad}");
    }
    assert!(IdRef::Uint(u64::MAX) < IdRef::Uuid(0));
}
