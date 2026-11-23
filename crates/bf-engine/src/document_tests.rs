use super::*;
use bf_storage::StorageConfig;
use bf_types::DistanceMetric;
use serde_json::json;
use std::collections::BTreeSet;

pub(super) fn input(n: u32) -> (Schema, BTreeMap<Id, Document>) {
    let schema = serde_json::from_value(json!({
        "id":{"type":"uint"}, "text":{"type":"string"}, "tags":{"type":"[]string"},
        "f32":{"type":"[3]f32"}, "f16":{"type":"[3]f16"}, "i8":{"type":"[3]i8"},
        "empty":{"type":"[3]f32"}
    }))
    .unwrap();
    let docs = (0..n)
        .map(|id| {
            let mut doc: Document = serde_json::from_value(json!({
                "id":id, "text":format!("🐡 café, fish, and water {}", id % 3).repeat(30),
                "tags":["élan", "🐠", "same"]
            }))
            .unwrap();
            if id % 5 != 1 {
                doc.insert("f32".into(), json!([id as f32 * 0.1, -0.0, 1.001]));
                doc.insert("f16".into(), json!([id as f32 * 0.1, -0.0, 1.001]));
                doc.insert("i8".into(), json!([-128, id % 100, 127]));
            }
            (Id::Uint(id as u64), doc)
        })
        .collect();
    (schema, docs)
}

#[tokio::test]
async fn vectors_are_absent_from_documents_and_projection_is_byte_identical_to_v1() {
    for n in [7, 1400] {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let (schema, docs) = input(n);
        let current = build_with(
            &store,
            "t",
            &schema,
            DistanceMetric::CosineDistance,
            &docs,
            BTreeMap::new(),
            0,
            1,
        )
        .await
        .unwrap();
        let dir = directory(&ObjectReader::new(&store, &current.files["docs"]))
            .await
            .unwrap();
        assert_eq!(dir.version, 2);
        assert_eq!(dir.vectors.len(), 4);
        assert_eq!(dir.columns.len(), 3);
        assert!(dir.columns["text"].compressed);
        assert!(dir.columns["tags"].compressed);
        assert!(!dir.columns["id"].compressed);
        assert!(
            dir.columns["text"]
                .blocks
                .iter()
                .all(|b| b.uncompressed_length.unwrap() > b.length as usize * 4)
        );
        let mut legacy = current.clone();
        upgrade::make_legacy(&store, &mut legacy).await.unwrap();
        let rows = (0..n).filter(|r| r % 7 != 3).collect();
        for columns in [
            None,
            Some(BTreeSet::from(["f16".into(), "i8".into(), "empty".into()])),
        ] {
            let before = projected_rows(&store, &legacy, &rows, columns.as_ref())
                .await
                .unwrap();
            let after = projected_rows(&store, &current, &rows, columns.as_ref())
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_vec(&before).unwrap(),
                serde_json::to_vec(&after).unwrap()
            );
        }
        let strip = BTreeSet::from(["f16".into(), "f32".into(), "i8".into(), "empty".into()]);
        let (compacted_docs, parts) = compaction_rows(&store, &current, &rows, &strip)
            .await
            .unwrap();
        for name in &strip {
            let old = upgrade::read_packed(&store, &legacy, &rows, name)
                .await
                .unwrap();
            assert_eq!(
                read_packed(&store, &current, &rows, name).await.unwrap(),
                old
            );
        }
        let parts = parts.into_iter().map(|(k, v)| (k, vec![v])).collect();
        let packed = merge_packed(&schema, &compacted_docs, parts).unwrap();
        let merged = build_with(
            &store,
            "t",
            &schema,
            DistanceMetric::CosineDistance,
            &compacted_docs,
            packed,
            0,
            1,
        )
        .await
        .unwrap();
        let merged_rows = (0..merged.row_count).collect();
        let actual = documents(&store, &merged, &merged_rows).await.unwrap();
        assert_eq!(actual, documents(&store, &legacy, &rows).await.unwrap());
        let rewritten = upgrade::rewrite(&store, "t", &legacy).await.unwrap();
        for (role, file) in rewritten {
            legacy.files.insert(role, file);
        }
        assert!(!upgrade::needs_upgrade(&store, &legacy).await.unwrap());
        assert_eq!(actual, documents(&store, &legacy, &rows).await.unwrap());
        let value: Arc<Value> = get_value(&store, &legacy.files["ids"]).await.unwrap();
        assert!(value.get("bloom").is_none());
    }
}

#[tokio::test]
async fn idfilter_rejects_absent_ids_without_opening_the_id_list() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let (schema, docs) = input(700);
    let mut segment = build_with(
        &store,
        "t",
        &schema,
        DistanceMetric::CosineDistance,
        &docs,
        BTreeMap::new(),
        0,
        1,
    )
    .await
    .unwrap();
    let wanted = BTreeSet::from([Id::Uint(2000)]);
    segment.files.get_mut("ids").unwrap().key = "deliberately-unreadable-ids".into();
    assert!(
        lookup_many(&store, &segment, &wanted)
            .await
            .unwrap()
            .is_empty()
    );
    // An ID inside the range but absent from the filter must also avoid opening ids.bfi.
    let filter = IdFilter::build(&[Id::Uint(0), Id::Uint(1000)]);
    let inside = (1..1000)
        .map(Id::Uint)
        .find(|id| filter.candidates(&BTreeSet::from([id.clone()])).is_empty())
        .unwrap();
    segment.files.insert(
        "idfilter".into(),
        put_file(&store, "different-filter".into(), filter.to_bytes(), 0)
            .await
            .unwrap(),
    );
    assert!(
        lookup_many(&store, &segment, &BTreeSet::from([inside]))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn compressed_block_corruption_fails_before_decode() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let (schema, docs) = input(700);
    let mut segment = build_with(
        &store,
        "t",
        &schema,
        DistanceMetric::CosineDistance,
        &docs,
        BTreeMap::new(),
        0,
        1,
    )
    .await
    .unwrap();
    let file = &segment.files["docs"];
    let dir = directory(&ObjectReader::new(&store, file)).await.unwrap();
    let mut bytes = read_whole(&store, file).await.unwrap().to_vec();
    bytes[dir.columns["text"].blocks[0].offset as usize] ^= 1;
    let file = put_file(&store, "corrupt-documents".into(), bytes, file.metadata_len)
        .await
        .unwrap();
    segment.files.insert("docs".into(), file);
    assert!(
        projected_rows(
            &store,
            &segment,
            &RoaringBitmap::from_iter([0]),
            Some(&BTreeSet::from(["text".into()]))
        )
        .await
        .is_err()
    );
}
