//! Rewrite an index's quantized codes in the fast-scan layout, without retraining.
//!
//! A cluster's centers, row membership, and full-precision vectors stay byte for byte; only each
//! cluster's code block changes (row-major records to the blocked fast-scan layout, or back), and
//! with it the metadata's code ranges, checksums, and the layout flag. Converting a row-major
//! index forward gives the bytes a fresh fast-scan build of the same clustering would write.
//! Converting back recovers each row's residual norm and `alpha` from the fast-scan factors
//! (`norm^2` and `norm / alpha`), which is exact up to one rounding in `alpha`.

use crate::{
    Result,
    fastscan::{self, BLOCK, RowCode},
    format::{
        self, FLAG_COMPACT, FLAG_FASTSCAN, FLAG_ROTATED, FOOTER_SIZE, MAGIC, Metadata, bad,
        decode_footer, decode_metadata, encode_metadata, mean_center,
    },
};

/// How an index stores its quantized codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodeLayout {
    /// One record per row: residual norm, signed alpha, and the sign bits.
    RowMajor,
    /// Blocks of 32 rows, transposed, with per-row factors (see the `fastscan` module).
    FastScan,
    /// A flat index (no codes), or one in a format older than the compact one.
    Other,
}

/// The code layout recorded in an index's metadata. `header` is the object's first 48 bytes or
/// more (the metadata's fixed fields), so an operator can survey a namespace with one small
/// ranged read per index.
pub fn code_layout(header: &[u8]) -> Result<CodeLayout> {
    if header.starts_with(crate::directory::ANN_MAGIC) && header.len() >= 32 {
        return Ok(
            if u32::from_le_bytes(header[28..32].try_into().unwrap()) != 0 {
                CodeLayout::Other
            } else if u32::from_le_bytes(header[8..12].try_into().unwrap()) <= 4 {
                CodeLayout::RowMajor
            } else {
                CodeLayout::FastScan
            },
        );
    }
    if header.len() < 48 || &header[..8] != b"BFVIDX01" {
        return Err(bad("bad metadata magic"));
    }
    let flat = u32::from_le_bytes(header[32..36].try_into().unwrap());
    let flags = u32::from_le_bytes(header[44..48].try_into().unwrap());
    Ok(layout_of(flat != 0, flags))
}

fn layout_of(flat: bool, flags: u32) -> CodeLayout {
    if flat || flags & FLAG_COMPACT == 0 || flags & FLAG_ROTATED == 0 {
        CodeLayout::Other
    } else if flags & FLAG_FASTSCAN != 0 {
        CodeLayout::FastScan
    } else {
        CodeLayout::RowMajor
    }
}

/// Rewrite a whole `.bfv` blob in the fast-scan layout. Returns `None` when the index already is
/// in it, is flat, or uses compact codes below eight padded dimensions. Never changes the input.
pub fn upgrade_layout(blob: &[u8]) -> Result<Option<Vec<u8>>> {
    convert(blob, CodeLayout::FastScan)
}

/// Rewrite a fast-scan `.bfv` blob in the older row-major layout, so tests can make the files an
/// older release wrote. Not part of any release build: only the `legacy-fixtures` feature has it.
#[cfg(feature = "legacy-fixtures")]
pub fn downgrade_layout(blob: &[u8]) -> Result<Option<Vec<u8>>> {
    if blob.starts_with(crate::directory::ANN_MAGIC) {
        let mut index = crate::Index::metadata_only(blob, bytes::Bytes::new())?;
        let padded = index.dims().next_power_of_two();
        if index.is_flat() || !padded.is_multiple_of(8) {
            return Ok(None);
        }
        // Single-object directories omit empty leaf payloads. The legacy metadata table
        // requires a center for every slot, including leaves with no rows.
        for cluster in &mut index.metadata_mut().clusters {
            if cluster.data.is_none() {
                cluster.data = Some(std::sync::Arc::new(format::ClusterData {
                    center: vec![0.; padded],
                    ..Default::default()
                }));
            }
        }
        let codes = index
            .metadata()
            .clusters
            .iter()
            .map(|c| blob[c.codes.start as usize..c.codes.end as usize].to_vec())
            .collect();
        let legacy = rebuild(blob, index.metadata_mut(), codes, CodeLayout::FastScan)?;
        return convert(&legacy, CodeLayout::RowMajor);
    }
    convert(blob, CodeLayout::RowMajor)
}

fn convert(blob: &[u8], target: CodeLayout) -> Result<Option<Vec<u8>>> {
    if blob.len() < FOOTER_SIZE as usize {
        return Err(bad("missing footer"));
    }
    let len = blob.len() as u64;
    let (meta_len, crc) = decode_footer(&blob[blob.len() - FOOTER_SIZE as usize..], len)?;
    let mut metadata = decode_metadata(&blob[..meta_len as usize], crc, len - FOOTER_SIZE)?;
    let current = layout_of(metadata.flat, metadata.flags);
    if current == target || metadata.flat {
        return Ok(None);
    }
    let padded = metadata.dims.next_power_of_two();
    let compact = metadata.flags & FLAG_COMPACT != 0;
    let target = if !padded.is_multiple_of(8) {
        if compact && metadata.flags & FLAG_ROTATED != 0 {
            return Ok(None);
        }
        CodeLayout::RowMajor
    } else {
        target
    };
    if metadata.flags & FLAG_ROTATED == 0 {
        let rq = crate::RaBitQ::new(metadata.dims, metadata.seed)?;
        let rotate = |c: &[f32]| rq.rotate(c).into_iter().map(|x| x as f32).collect();
        metadata.parents = metadata.parents.iter().map(|c| rotate(c)).collect();
        for c in &mut metadata.clusters {
            std::sync::Arc::get_mut(c.data.as_mut().expect("decoded cluster"))
                .expect("fresh cluster")
                .center = rotate(&c.center);
        }
    }
    let reference = mean_center(
        metadata
            .clusters
            .iter()
            .filter(|c| !c.rows.is_empty())
            .map(|c| c.center.as_slice()),
        padded,
    );
    let root = (padded as f64).sqrt();
    let width = padded.div_ceil(8);
    let mut all_codes: Vec<Vec<u8>> = Vec::with_capacity(metadata.clusters.len());
    for cluster in &metadata.clusters {
        let n = cluster.rows.len() as usize;
        let old = &blob[cluster.codes.start as usize..cluster.codes.end as usize];
        let codes = match target {
            _ if current != CodeLayout::FastScan => {
                let rows: Vec<RowCode> = (0..n)
                    .map(|i| {
                        let head = if compact { 8 } else { 24 };
                        let rec = &old[i * (head + width)..(i + 1) * (head + width)];
                        let (norm, signed) = if compact {
                            (
                                f32::from_le_bytes(rec[..4].try_into().unwrap()),
                                f32::from_le_bytes(rec[4..8].try_into().unwrap()),
                            )
                        } else {
                            let norm = f64::from_le_bytes(rec[..8].try_into().unwrap()) as f32;
                            let alpha = f64::from_le_bytes(rec[8..16].try_into().unwrap()) as f32;
                            let zero = u64::from_le_bytes(rec[16..24].try_into().unwrap()) != 0;
                            (norm, if zero { -alpha } else { alpha })
                        };
                        let bits = rec[head..].to_vec();
                        let cb = cluster
                            .center
                            .iter()
                            .zip(&reference)
                            .enumerate()
                            .map(|(d, (c, g))| {
                                let x = *c as f64 - *g as f64;
                                if bits[d / 8] & (1 << (d % 8)) != 0 {
                                    x
                                } else {
                                    -x
                                }
                            })
                            .sum::<f64>()
                            / root;
                        RowCode {
                            bits,
                            norm,
                            alpha: signed.abs(),
                            cb: cb as f32,
                            zero: signed < 0.,
                        }
                    })
                    .collect();
                if target == CodeLayout::FastScan {
                    fastscan::encode(&rows, padded)
                } else {
                    let mut out = Vec::new();
                    for row in rows {
                        out.extend(row.norm.to_le_bytes());
                        out.extend((if row.zero { -row.alpha } else { row.alpha }).to_le_bytes());
                        out.extend(row.bits);
                    }
                    out
                }
            }
            CodeLayout::RowMajor => {
                let f32_at = |array: usize, i: usize| {
                    let at = (array * n + i) * 4;
                    f32::from_le_bytes(old[at..at + 4].try_into().unwrap())
                };
                let blocks = &old[16 * n..];
                let mut out = Vec::with_capacity(n * (8 + width));
                for i in 0..n {
                    let norm = f32_at(0, i).max(0.).sqrt();
                    let k = f32_at(1, i);
                    let zero = k.is_sign_negative();
                    // `k` is `norm / alpha`; a row at its center (norm 0) has no alpha to recover,
                    // and its estimate does not depend on it.
                    let alpha = if k == 0. || !k.is_finite() {
                        1.
                    } else {
                        (norm / k.abs()).clamp(1e-6, 1.)
                    };
                    out.extend(norm.to_le_bytes());
                    out.extend((if zero { -alpha } else { alpha }).to_le_bytes());
                    let start = (i / BLOCK) * width * BLOCK;
                    for t in 0..width {
                        out.push(blocks[start + t * BLOCK + i % BLOCK]);
                    }
                }
                out
            }
            _ => unreachable!(),
        };
        all_codes.push(codes);
    }
    metadata.flags |= FLAG_COMPACT | FLAG_ROTATED;
    rebuild(blob, &mut metadata, all_codes, target).map(Some)
}

fn rebuild(
    blob: &[u8],
    metadata: &mut Metadata,
    all_codes: Vec<Vec<u8>>,
    target: CodeLayout,
) -> Result<Vec<u8>> {
    let old_vectors: Vec<std::ops::Range<u64>> = metadata
        .clusters
        .iter()
        .map(|c| c.vectors.clone())
        .collect();
    metadata.flags = if target == CodeLayout::FastScan {
        metadata.flags | FLAG_FASTSCAN
    } else {
        metadata.flags & !FLAG_FASTSCAN
    };
    for (cluster, codes) in metadata.clusters.iter_mut().zip(&all_codes) {
        cluster.codes = 0..codes.len() as u64;
        cluster.codes_crc = crc32fast::hash(codes);
    }
    let meta_len = encode_metadata(metadata)?.len() as u64;
    let mut offset = meta_len;
    for cluster in &mut metadata.clusters {
        let n = cluster.codes.end;
        cluster.codes = offset..offset + n;
        offset += n;
    }
    for (cluster, old) in metadata.clusters.iter_mut().zip(&old_vectors) {
        let n = old.end - old.start;
        cluster.vectors = offset..offset + n;
        offset += n;
    }
    let mut out = Vec::with_capacity(offset as usize + FOOTER_SIZE as usize);
    out.extend(encode_metadata(metadata)?);
    let meta_crc = crc32fast::hash(&out);
    for codes in all_codes {
        out.extend(codes);
    }
    for old in old_vectors {
        out.extend_from_slice(&blob[old.start as usize..old.end as usize]);
    }
    let mut footer = vec![0u8; FOOTER_SIZE as usize];
    footer[..8].copy_from_slice(MAGIC);
    footer[8..12].copy_from_slice(&format::FORMAT_VERSION.to_le_bytes());
    footer[16..24].copy_from_slice(&meta_len.to_le_bytes());
    footer[24..32].copy_from_slice(&(offset + FOOTER_SIZE).to_le_bytes());
    footer[32..36].copy_from_slice(&meta_crc.to_le_bytes());
    let checksum = crc32fast::hash(&footer[..60]);
    footer[60..].copy_from_slice(&checksum.to_le_bytes());
    out.extend(footer);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Bytes, Index, IndexBuilder, Metric, SearchParams};

    #[tokio::test]
    async fn precompact_headers_and_raw_zero_centers_upgrade() {
        for dims in [2usize, 8] {
            let n = 3000;
            let mut data = Vec::new();
            for i in 0..n * dims {
                data.extend(((i as f64).sin() as f32).to_le_bytes());
            }
            let blob = IndexBuilder::new(dims, Metric::EuclideanSquared)
                .unwrap()
                .build_packed_assigned(
                    (0..n as u32).collect(),
                    data,
                    vec![vec![0.; dims]],
                    vec![0; n],
                )
                .unwrap();
            let compact = downgrade_layout(&blob).unwrap().unwrap_or(blob);
            let mut index = Index::metadata_only(&compact, Bytes::new()).unwrap();
            let mut codes = Vec::new();
            for c in &index.metadata().clusters {
                let mut old = Vec::new();
                for row in compact[c.codes.start as usize..c.codes.end as usize]
                    .chunks_exact(8 + dims.div_ceil(8))
                {
                    let norm = f32::from_le_bytes(row[..4].try_into().unwrap()) as f64;
                    let alpha = f32::from_le_bytes(row[4..8].try_into().unwrap()) as f64;
                    old.extend(norm.to_le_bytes());
                    old.extend(alpha.abs().to_le_bytes());
                    old.extend(u64::from(alpha.is_sign_negative()).to_le_bytes());
                    old.extend_from_slice(&row[8..]);
                }
                codes.push(old);
            }
            // Zero centers are identical before and after rotation, so this also exercises
            // the oldest center representation without introducing a second transform.
            index.metadata_mut().flags &= !(FLAG_COMPACT | FLAG_ROTATED | FLAG_FASTSCAN);
            let legacy =
                rebuild(&compact, index.metadata_mut(), codes, CodeLayout::RowMajor).unwrap();
            let (object, _) = crate::Fragment::build_ann(&legacy).unwrap();
            let old = Index::open(Bytes::from(legacy)).await.unwrap();
            let new = Index::open(Bytes::from(object)).await.unwrap();
            assert_eq!(
                old.read_cluster_rows(0).await.unwrap(),
                new.read_cluster_rows(0).await.unwrap()
            );
            let params = SearchParams {
                nprobe: 1,
                rerank_factor: n,
            };
            assert_eq!(
                old.search(&vec![0.1; dims], 10, params, None)
                    .await
                    .unwrap(),
                new.search(&vec![0.1; dims], 10, params, None)
                    .await
                    .unwrap()
            );
        }
    }
}
