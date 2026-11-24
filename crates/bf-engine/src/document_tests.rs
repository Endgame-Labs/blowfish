use super::*;
use bf_storage::StorageConfig;
use bf_types::DistanceMetric;
use serde_json::json;
use std::collections::BTreeSet;

#[test]
fn selected_column_decode_matches_full_values_and_rejects_malformed_skipped_values() {
    let raw=serde_json::to_vec(&json!([null,"café 🐡","quote \" and slash \\", [1,null,2], {"nested":[true,"é"]},false,3.25,"line\nend"])).unwrap();
    for compressed in [false, true] {
        let bytes = if compressed {
            zstd::bulk::compress(&raw, 3).unwrap()
        } else {
            raw.clone()
        };
        let mut body = Vec::new();
        let mut descriptor = block(&mut body, &bytes);
        if compressed {
            descriptor.uncompressed_length = Some(raw.len());
        }
        let column = Column {
            blocks: vec![descriptor.clone()],
            vector: None,
            vector_i8: false,
            compressed,
        };
        let selected: RoaringBitmap = [100, 101, 103, 104, 107].into_iter().collect();
        let full = decode_column(&bytes, &column, &descriptor).unwrap();
        let expected: Vec<_> = full
            .into_iter()
            .enumerate()
            .filter_map(|(i, value)| {
                let row = 100 + i as u32;
                selected
                    .contains(row)
                    .then_some(value.map(|v| (row, v)))
                    .flatten()
            })
            .collect();
        assert_eq!(
            decode_selected_column(&bytes, &column, &descriptor, 100, 8, &selected).unwrap(),
            expected
        );
    }
    let selected = RoaringBitmap::from_iter([0]);
    for invalid in [
        b"[1,{,3]".as_slice(),
        b"[1,2]",
        b"[1,2,3,4]",
        b"[1,2,3] false",
    ] {
        let mut body = Vec::new();
        let descriptor = block(&mut body, invalid);
        let column = Column {
            blocks: vec![descriptor.clone()],
            vector: None,
            vector_i8: false,
            compressed: false,
        };
        assert!(decode_selected_column(invalid, &column, &descriptor, 0, 3, &selected).is_err());
    }
}

#[tokio::test]
async fn small_wide_vector_flushes_remain_exact_and_preserve_top_k() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let schema: Schema =
        serde_json::from_value(json!({"id":{"type":"uint"},"vector":{"type":"[1024]f16"}}))
            .unwrap();
    let mut docs: BTreeMap<Id, Document> = BTreeMap::new();
    for id in 0..2560u64 {
        let mut vector = vec![0.0; 1024];
        vector[0] = 1.0;
        vector[1] = id as f32 / 128.0;
        docs.insert(
            Id::Uint(id),
            serde_json::from_value(
                json!({"id":id,"vector":bf_types::encode_vector(&vector,false)}),
            )
            .unwrap(),
        );
    }
    let mut segments = Vec::new();
    for first in (0..2560u64).step_by(512) {
        let part: BTreeMap<Id, Document> = docs
            .range(Id::Uint(first)..Id::Uint(first + 512))
            .map(|(id, doc)| (id.clone(), doc.clone()))
            .collect();
        let seg = build_with(
            &store,
            "wide",
            &schema,
            DistanceMetric::EuclideanSquared,
            &part,
            BTreeMap::new(),
            0,
            1,
        )
        .await
        .unwrap();
        let index = vector_index(&store, &seg.files["annv2:vector"])
            .await
            .unwrap();
        assert!(
            index.is_flat(),
            "small flushes retain the established exact index plan"
        );
        segments.push(seg);
    }
    let mut vector = vec![0.0; 1024];
    vector[0] = 1.0;
    let mut state = crate::NamespaceState {
        schema,
        metric: DistanceMetric::EuclideanSquared,
        segments: segments.into(),
        ..Default::default()
    };
    let directory = crate::ann_dir::build(
        &store,
        "wide",
        &state.segments,
        "vector",
        crate::ann_dir::Layout::Segments,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    state.ann_dirs.insert("annv2:vector".into(), directory);
    let request: bf_types::QueryRequest =
        serde_json::from_value(json!({"rank_by":["vector","ANN",vector],"top_k":10})).unwrap();
    for hidden in [0, 1, 31, 96] {
        state.docs.clear();
        state.shadowed.clear();
        let mut reference = docs.clone();
        for id in 0..hidden {
            state.shadowed.insert(Id::Uint(id));
            if id % 2 == 0 {
                reference.remove(&Id::Uint(id));
            } else {
                let mut replacement = reference[&Id::Uint(id)].clone();
                let mut vector = vec![0.0; 1024];
                vector[0] = 1.0;
                vector[1] = 100.0 + id as f32;
                replacement.insert(
                    "vector".into(),
                    json!(bf_types::encode_vector(&vector, false)),
                );
                reference.insert(Id::Uint(id), replacement.clone());
                state
                    .docs
                    .insert(Id::Uint(id), std::sync::Arc::new(replacement));
            }
        }
        let actual = crate::indexed::execute(
            &store,
            &state,
            &request,
            &crate::EngineConfig::default(),
            false,
        )
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
        let mut exhaustive = state.clone();
        exhaustive.docs = crate::shared(reference);
        let expected = crate::query::execute(&exhaustive, &request)
            .unwrap()
            .result
            .rows
            .unwrap();
        assert_eq!(
            actual, expected,
            "{hidden} nearest indexed rows shadowed by replacements and deletes"
        );
    }
}

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
        let rewritten = upgrade::rewrite(&store, "t", &legacy, None).await.unwrap();
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

#[tokio::test]
async fn document_upgrade_rejects_corrupt_source_blocks() {
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
    upgrade::make_legacy(&store, &mut segment).await.unwrap();
    let file = &segment.files["docs"];
    let dir = directory(&ObjectReader::new(&store, file)).await.unwrap();
    let mut bytes = read_whole(&store, file).await.unwrap().to_vec();
    bytes[dir.columns["text"].blocks[0].offset as usize] ^= 1;
    let file = put_file(
        &store,
        "corrupt-legacy-documents".into(),
        bytes,
        file.metadata_len,
    )
    .await
    .unwrap();
    segment.files.insert("docs".into(), file);
    assert!(upgrade::rewrite(&store, "t", &segment, None).await.is_err());
}
