//! Document-v1 readers, used only while the automatic rewrite is pending.
use super::*;

pub(super) fn decode_vectors(
    data: &[u8],
    count: usize,
    dims: usize,
    f16: bool,
    i8: bool,
) -> Result<Vec<Option<Value>>, ApiError> {
    let mut pos = 0;
    let mut values = vec![];
    for _ in 0..count {
        let present = *data.get(pos).ok_or_else(corrupt)?;
        pos += 1;
        if present == 0 {
            values.push(None);
            continue;
        }
        if present != 1 {
            return Err(corrupt());
        }
        let mut v = vec![];
        for _ in 0..dims {
            let width = if i8 {
                1
            } else if f16 {
                2
            } else {
                4
            };
            let bytes = data.get(pos..pos + width).ok_or_else(corrupt)?;
            let x = if i8 {
                bytes[0] as i8 as f32
            } else if f16 {
                half::f16::from_le_bytes(bytes.try_into().unwrap()).to_f32()
            } else {
                f32::from_le_bytes(bytes.try_into().unwrap())
            };
            v.push(x);
            pos += width;
        }
        values.push(Some(if i8 {
            serde_json::json!(v.iter().map(|x| *x as i8).collect::<Vec<_>>())
        } else {
            Value::String(bf_types::encode_vector(&v, false))
        }));
    }
    if pos != data.len() {
        return Err(corrupt());
    }
    Ok(values)
}

/// Survey only document metadata. Version 1 is accepted solely until this rewrite publishes.
pub(crate) async fn needs_upgrade(store: &Storage, segment: &Segment) -> Result<bool, ApiError> {
    let file = segment.files.get("docs").ok_or_else(corrupt)?;
    Ok(directory(&ObjectReader::new(store, file)).await?.version == 1)
}

/// Copy non-vector columns into document-v2, without decoding a namespace or retraining ANN.
/// ANN must already be current, and its object remains referenced by the same segment.
pub(crate) async fn rewrite(
    store: &Storage,
    ns: &str,
    segment: &Segment,
) -> Result<Vec<(String, SegmentFile)>, ApiError> {
    let file = segment.files.get("docs").ok_or_else(corrupt)?;
    let reader = ObjectReader::new(store, file).bypass_cache();
    let old = directory(&reader).await?;
    let mut dir = Directory::default();
    let mut body = Vec::new();
    for (name, column) in &old.columns {
        if let Some((dims, f16)) = column.vector {
            if !segment.files.contains_key(&format!("annv2:{name}")) {
                return Err(index_error(
                    "document upgrade requires the current ANN object",
                ));
            }
            dir.vectors
                .insert(name.clone(), (dims, f16, column.vector_i8));
            continue;
        }
        let compressed = segment.schema.get(name).is_some_and(|c| {
            matches!(
                c.data_type,
                Some(
                    AttributeType::Scalar(bf_types::ScalarType::String)
                        | AttributeType::Array(bf_types::ScalarType::String)
                )
            )
        });
        let mut blocks = Vec::with_capacity(column.blocks.len());
        for old in &column.blocks {
            let data = read_block(&reader, old).await?;
            let b = if compressed {
                let bytes = zstd::bulk::compress(&data, 3).map_err(index_error)?;
                let mut b = block(&mut body, &bytes);
                b.uncompressed_length = Some(data.len());
                b
            } else {
                block(&mut body, &data)
            };
            blocks.push(b);
        }
        dir.columns.insert(
            name.clone(),
            Column {
                blocks,
                vector: None,
                vector_i8: false,
                compressed,
            },
        );
    }
    let (bytes, meta) = finish_version(body, &dir, 2)?;
    let prefix = format!("ns/{ns}/segments/{}", segment.id);
    let docs = put_file(
        store,
        format!("{prefix}/docs.{}.bfc", uuid::Uuid::new_v4()),
        bytes,
        meta,
    )
    .await?;
    let index = ids(store, segment).await?;
    let ids = put_value(
        store,
        format!("{prefix}/ids.{}.bfi", uuid::Uuid::new_v4()),
        &*index,
    )
    .await?;
    Ok(vec![("docs".into(), docs), ("ids".into(), ids)])
}

pub(super) async fn read_packed(
    store: &Storage,
    segment: &Segment,
    rows: &RoaringBitmap,
    attribute: &str,
) -> Result<(Vec<u32>, Vec<u8>), ApiError> {
    let reader =
        ObjectReader::new(store, segment.files.get("docs").ok_or_else(corrupt)?).bypass_cache();
    let dir = directory(&reader).await?;
    let col = dir.columns.get(attribute).ok_or_else(corrupt)?;
    let (dims, f16) = col.vector.ok_or_else(corrupt)?;
    let width = dims
        * if col.vector_i8 {
            1
        } else if f16 {
            2
        } else {
            4
        };
    let blocks: std::collections::BTreeSet<_> =
        rows.iter().map(|row| row as usize / BLOCK_ROWS).collect();
    let indices: Vec<_> = blocks.into_iter().collect();
    let (mut present, mut out) = (Vec::new(), Vec::new());
    for batch in indices.chunks(16) {
        let descriptors = batch
            .iter()
            .map(|i| col.blocks.get(*i).ok_or_else(corrupt))
            .collect::<Result<Vec<_>, _>>()?;
        let data = reader
            .read_batch(
                descriptors
                    .iter()
                    .map(|b| b.offset..b.offset + b.length)
                    .collect(),
            )
            .await
            .map_err(index_error)?;
        for ((&i, b), data) in batch.iter().zip(descriptors).zip(data) {
            if crc32fast::hash(&data) != b.crc {
                return Err(corrupt());
            }
            let first = (i * BLOCK_ROWS) as u32;
            let last = first
                .saturating_add(BLOCK_ROWS as u32)
                .min(segment.row_count);
            let mut pos = 0;
            for row in first..last {
                let flag = *data.get(pos).ok_or_else(corrupt)?;
                pos += 1;
                match flag {
                    0 => {}
                    1 => {
                        let bytes = data.get(pos..pos + width).ok_or_else(corrupt)?;
                        if rows.contains(row) {
                            present.push(row);
                            out.extend_from_slice(bytes);
                        }
                        pos += width;
                    }
                    _ => return Err(corrupt()),
                }
            }
            if pos != data.len() {
                return Err(corrupt());
            }
        }
    }
    Ok((present, out))
}

/// Reconstruct the exact D1 document and ID formats for migration tests and measurements.
/// No production path writes these objects.
#[cfg(test)]
pub(crate) async fn make_legacy(store: &Storage, segment: &mut Segment) -> Result<(), ApiError> {
    let reader = ObjectReader::new(store, &segment.files["docs"]).bypass_cache();
    let current = directory(&reader).await?;
    let mut body = Vec::new();
    let mut dir = Directory::default();
    for (name, col) in &current.columns {
        let mut blocks = Vec::new();
        for b in &col.blocks {
            let raw = read_block(&reader, b).await?;
            let raw = if col.compressed {
                zstd::bulk::decompress(&raw, b.uncompressed_length.ok_or_else(corrupt)?)
                    .map_err(index_error)?
            } else {
                raw.to_vec()
            };
            blocks.push(block(&mut body, &raw));
        }
        dir.columns.insert(
            name.clone(),
            Column {
                blocks,
                vector: None,
                vector_i8: false,
                compressed: false,
            },
        );
    }
    let all: RoaringBitmap = (0..segment.row_count).collect();
    for (name, &(dims, f16, i8)) in &current.vectors {
        let width = dims
            * if i8 {
                1
            } else if f16 {
                2
            } else {
                4
            };
        let (present, packed) = super::read_packed(store, segment, &all, name)
            .await
            .expect("fixture ANN vectors");
        let mut next = 0;
        let mut blocks = Vec::new();
        for first in (0..segment.row_count).step_by(BLOCK_ROWS) {
            let mut data = Vec::new();
            for row in first
                ..first
                    .saturating_add(BLOCK_ROWS as u32)
                    .min(segment.row_count)
            {
                if present.get(next) == Some(&row) {
                    data.push(1);
                    data.extend_from_slice(&packed[next * width..(next + 1) * width]);
                    next += 1;
                } else {
                    data.push(0);
                }
            }
            blocks.push(block(&mut body, &data));
        }
        dir.columns.insert(
            name.clone(),
            Column {
                blocks,
                vector: Some((dims, f16)),
                vector_i8: i8,
                compressed: false,
            },
        );
    }
    let (bytes, metadata) = finish(body, &dir)?;
    let key = format!("test/legacy-docs-{}.bfc", uuid::Uuid::new_v4());
    segment
        .files
        .insert("docs".into(), put_file(store, key, bytes, metadata).await?);
    let ids = ids(store, segment).await.expect("fixture IDs");
    let mut bloom = vec![0u8; (ids.len() * 10).div_ceil(8).max(1)];
    for id in ids.iter() {
        let data = serde_json::to_vec(id).expect("ID serializes");
        let h = data.iter().fold(0xcbf29ce484222325u64, |h, b| {
            (h ^ *b as u64).wrapping_mul(0x100000001b3)
        });
        for bit in [h, h.rotate_left(21), h.rotate_left(42)].map(|h| h as usize % (bloom.len() * 8))
        {
            bloom[bit / 8] |= 1 << (bit % 8);
        }
    }
    let legacy = serde_json::json!({"ids":ids.ids,"bloom":bloom});
    let key = format!("test/legacy-ids-{}.bfi", uuid::Uuid::new_v4());
    segment
        .files
        .insert("ids".into(), put_value(store, key, &legacy).await?);
    Ok(())
}
