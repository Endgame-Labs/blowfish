//! Repack existing leaves without assigning rows or quantizing vectors again.
use crate::{
    Bytes, Index, RangeRead, Result,
    format::{self, Cluster, ClusterData, Metadata, bad, check_crc, encode_metadata},
};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::sync::Arc;

/// One validated leaf's existing payload, with its rows mapped to namespace addresses.
/// Constructed by [`Index::packing_leaf`], and consumed by [`pack_leaves`].
pub struct PackedLeaf {
    shape: (usize, crate::Metric, crate::VectorType, u64),
    cluster: Cluster,
    codes: Vec<u8>,
    vectors: Bytes,
}
impl PackedLeaf {
    /// Number of live rows retained in this leaf.
    pub fn len(&self) -> usize {
        self.cluster.rows.len() as usize
    }
    /// Whether all source rows were masked out.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<R: RangeRead> Index<R> {
    /// Existing leaf centers in the original coordinate space, for spatial packing.
    /// Requires a clustered fast-scan index. Stored centers are never changed by packing;
    /// inverse rotation here is only used to plan the layout and train future coarse routing.
    pub fn packing_centers(&self) -> Result<Vec<Vec<f32>>> {
        if self.is_flat() || self.metadata.flags & format::FLAG_FASTSCAN == 0 {
            return Err(bad("leaf packing requires fast-scan leaves"));
        }
        Ok(self
            .metadata
            .clusters
            .iter()
            .map(|c| {
                let mut v: Vec<f64> = c.center.iter().map(|x| f64::from(*x)).collect();
                self.rq.inverse_in_place(&mut v);
                v.truncate(self.dims());
                v.into_iter().map(|x| x as f32).collect()
            })
            .collect())
    }

    /// Read and validate an existing leaf, retain `live` rows, and add `base` to their IDs.
    /// Row order, vector bytes, sign bits, and the first three fast-scan factors are preserved.
    /// A later [`pack_leaves`] updates the reference-dependent fourth factor.
    pub async fn packing_leaf(
        &self,
        cid: usize,
        base: u32,
        live: Option<&RoaringBitmap>,
    ) -> Result<PackedLeaf> {
        self.packing_compatible()?;
        let c = self
            .metadata
            .clusters
            .get(cid)
            .ok_or_else(|| bad("leaf outside index"))?;
        let (rows, vectors) = self.read_cluster_rows(cid).await?;
        let codes = self
            .read_codes(&[cid])
            .await?
            .pop()
            .ok_or_else(|| bad("missing leaf codes"))?;
        check_crc(&codes, c.codes_crc)?;
        crate::fastscan::check_factors(&codes, rows.len(), self.rq.padded)?;
        let selected: Vec<usize> = rows
            .iter()
            .enumerate()
            .filter_map(|(i, r)| live.is_none_or(|l| l.contains(*r)).then_some(i))
            .collect();
        let ids = selected
            .iter()
            .map(|i| {
                rows[*i]
                    .checked_add(base)
                    .ok_or_else(|| bad("global row overflow"))
            })
            .collect::<Result<Vec<_>>>()?;
        let width = self.vector_row_bytes();
        let (codes, vectors) = if selected.len() == rows.len() {
            (codes.to_vec(), vectors)
        } else {
            let n = selected.len();
            let mut out = vec![0; crate::fastscan::block_len(n, self.rq.padded)];
            for (dst, src) in selected.iter().enumerate() {
                for factor in 0..4 {
                    let from = (factor * rows.len() + src) * 4;
                    let to = (factor * n + dst) * 4;
                    out[to..to + 4].copy_from_slice(&codes[from..from + 4]);
                }
                for t in 0..self.rq.padded / 8 {
                    let from = 16 * rows.len()
                        + (src / 32) * (self.rq.padded / 8) * 32
                        + t * 32
                        + src % 32;
                    let to = 16 * n + (dst / 32) * (self.rq.padded / 8) * 32 + t * 32 + dst % 32;
                    out[to] = codes[from];
                }
            }
            let mut raw = Vec::with_capacity(n * width);
            for i in &selected {
                raw.extend_from_slice(&vectors[i * width..(i + 1) * width]);
            }
            (out, Bytes::from(raw))
        };
        let mut cluster = c.clone();
        cluster.data = Some(Arc::new(ClusterData {
            center: c.center.clone(),
            rows: ids.iter().copied().collect(),
            row_crcs: selected.iter().map(|i| c.row_crcs[*i]).collect(),
            ids: Some(ids),
            ..Default::default()
        }));
        cluster.parent = u32::MAX;
        Ok(PackedLeaf {
            shape: (
                self.dims(),
                self.metric(),
                self.vector_type(),
                self.metadata.seed,
            ),
            cluster,
            codes,
            vectors,
        })
    }

    fn packing_compatible(&self) -> Result<()> {
        if self.is_flat() || self.metadata.flags & format::FLAG_FASTSCAN == 0 {
            return Err(bad("leaf packing requires fast-scan leaves"));
        }
        Ok(())
    }
}

/// Assemble existing leaves into a current ANN object, returning its resident metadata length.
/// No vector is decoded or requantized. Only row maps, offsets, checksums, and the fast-scan
/// reference correction change. Inputs must share dimensions, metric, precision, and rotation,
/// and have disjoint global row addresses. Empty leaves are omitted.
pub fn pack_leaves(mut leaves: Vec<PackedLeaf>) -> Result<(Vec<u8>, u64)> {
    leaves.retain(|l| !l.is_empty());
    let first = leaves
        .first()
        .ok_or_else(|| bad("no live leaves to pack"))?;
    let shape = first.shape;
    if leaves.iter().any(|l| l.shape != shape) {
        return Err(bad("incompatible leaves"));
    }
    let mut seen = RoaringBitmap::new();
    for leaf in &leaves {
        if !seen.is_disjoint(&leaf.cluster.rows) {
            return Err(bad("duplicate packed row"));
        }
        seen |= &leaf.cluster.rows;
    }
    let padded = shape.0.next_power_of_two();
    let reference = format::mean_center(leaves.iter().map(|l| l.cluster.center.as_slice()), padded);
    // Recompute only <b, c - g>/sqrt(D), using existing transposed signs. The other factors
    // and the sign bytes are immutable, including the sign of a zero vector's scale.
    leaves.par_iter_mut().for_each(|leaf| {
        let n = leaf.len();
        let root = (padded as f64).sqrt();
        let offset: Vec<f64> = leaf
            .cluster
            .center
            .iter()
            .zip(&reference)
            .map(|(c, g)| f64::from(*c) - f64::from(*g))
            .collect();
        // One small exact table per sign byte avoids touching every dimension for every row.
        let tables: Vec<[f64; 256]> = offset
            .as_chunks::<8>()
            .0
            .iter()
            .map(|dims| {
                std::array::from_fn(|b| {
                    dims.iter()
                        .enumerate()
                        .map(|(i, x)| if b & (1 << i) != 0 { *x } else { -*x })
                        .sum()
                })
            })
            .collect();
        for row in 0..n {
            let cb = tables
                .iter()
                .enumerate()
                .map(|(t, table)| {
                    let at = 16 * n + (row / 32) * (padded / 8) * 32 + t * 32 + row % 32;
                    table[leaf.codes[at] as usize]
                })
                .sum::<f64>()
                / root;
            let at = (3 * n + row) * 4;
            leaf.codes[at..at + 4].copy_from_slice(&(cb as f32).to_le_bytes());
        }
        leaf.cluster.codes = 0..leaf.codes.len() as u64;
        leaf.cluster.codes_crc = crc32fast::hash(&leaf.codes);
        leaf.cluster.vectors = 0..leaf.vectors.len() as u64;
        leaf.cluster.vectors_crc = crc32fast::hash(&leaf.vectors);
    });
    let mut metadata = Metadata {
        dims: shape.0,
        count: seen.len() as u32,
        metric: shape.1,
        dtype: shape.2,
        seed: shape.3,
        flat: false,
        flags: format::FLAG_ROTATED | format::FLAG_COMPACT | format::FLAG_FASTSCAN,
        parents: Vec::new(),
        clusters: leaves
            .iter()
            .map(|l| l.cluster.clone())
            .collect::<Vec<_>>()
            .into(),
    };
    let meta_len = encode_metadata(&metadata)?.len() as u64;
    let mut offset = meta_len;
    for c in &mut metadata.clusters {
        let n = c.codes.end;
        c.codes = offset..offset + n;
        offset += n;
    }
    for c in &mut metadata.clusters {
        let n = c.vectors.end;
        c.vectors = offset..offset + n;
        offset += n;
    }
    let mut blob = Vec::with_capacity(offset as usize + format::FOOTER_SIZE as usize);
    blob.extend(encode_metadata(&metadata)?);
    let meta_crc = crc32fast::hash(&blob);
    for l in &leaves {
        blob.extend_from_slice(&l.codes);
    }
    for l in &leaves {
        blob.extend_from_slice(&l.vectors);
    }
    let mut footer = [0u8; 64];
    footer[..8].copy_from_slice(format::MAGIC);
    footer[8..12].copy_from_slice(&format::FORMAT_VERSION.to_le_bytes());
    footer[16..24].copy_from_slice(&meta_len.to_le_bytes());
    footer[24..32].copy_from_slice(&(offset + 64).to_le_bytes());
    footer[32..36].copy_from_slice(&meta_crc.to_le_bytes());
    let crc = crc32fast::hash(&footer[..60]);
    footer[60..].copy_from_slice(&crc.to_le_bytes());
    blob.extend(footer);
    crate::Fragment::build_ann(&blob)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BuildParams, IndexBuilder, Metric, SearchParams, VectorType};

    async fn source(dims: usize, shift: usize, seed: u64) -> Index<Bytes> {
        let rows: Vec<_> = (0..99)
            .map(|i| {
                (
                    i,
                    (0..dims)
                        .map(|d| {
                            if i == 0 {
                                0.
                            } else {
                                ((i as usize * 13 + d * 7 + shift) as f32).sin()
                            }
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        let b = IndexBuilder::new(dims, Metric::CosineDistance)
            .unwrap()
            .vector_type(VectorType::F16)
            .params(BuildParams {
                clusters: Some(4),
                flat_threshold: 0,
                seed,
                ..Default::default()
            })
            .build(&rows)
            .unwrap();
        let (blob, _) = crate::Fragment::build_ann(&b).unwrap();
        Index::open(Bytes::from(blob)).await.unwrap()
    }

    #[tokio::test]
    async fn packing_preserves_centers_codes_vectors_and_global_rows() {
        for dims in [7, 64] {
            let a = source(dims, 0, 42).await;
            let b = source(dims, 111, 42).await;
            let live: RoaringBitmap = (0..99).filter(|i| i % 7 != 0).collect();
            let mut leaves = Vec::new();
            for (index, base) in [(&a, 0), (&b, 1000)] {
                let centers = index.packing_centers().unwrap();
                for (c, raw) in centers.iter().enumerate() {
                    let rot = index.rq.rotate(raw);
                    for (got, want) in rot.iter().zip(index.metadata.clusters[c].center.iter()) {
                        assert!((*got - f64::from(*want)).abs() < 1e-6);
                    }
                    leaves.push(index.packing_leaf(c, base, Some(&live)).await.unwrap());
                }
            }
            let originals: Vec<_> = leaves
                .iter()
                .map(|l| {
                    (
                        l.cluster.center.clone(),
                        l.codes.clone(),
                        l.vectors.clone(),
                        l.cluster.rows.clone(),
                    )
                })
                .collect();
            let (blob, _) = pack_leaves(leaves).unwrap();
            let packed = Index::open(Bytes::from(blob)).await.unwrap();
            assert_eq!(packed.len(), live.len() as usize * 2);
            for (cid, (center, codes, vectors, rows)) in originals.iter().enumerate() {
                let cluster = &packed.metadata.clusters[cid];
                assert_eq!(&cluster.center, center);
                let (got_rows, got_vectors) = packed.read_cluster_rows(cid).await.unwrap();
                assert_eq!(got_rows, rows.iter().collect::<Vec<_>>());
                assert_eq!(&got_vectors, vectors);
                let got_codes = packed.read_codes(&[cid]).await.unwrap().pop().unwrap();
                let n = rows.len() as usize;
                assert_eq!(&got_codes[..12 * n], &codes[..12 * n]);
                assert_eq!(&got_codes[16 * n..], &codes[16 * n..]);
                for row in 0..n {
                    let expected = center
                        .iter()
                        .zip(packed.reference.as_ref().unwrap().iter())
                        .enumerate()
                        .map(|(d, (c, g))| {
                            let byte = codes[16 * n
                                + (row / 32) * (dims.next_power_of_two() / 8) * 32
                                + (d / 8) * 32
                                + row % 32];
                            let x = f64::from(*c) - f64::from(*g);
                            if byte & (1 << (d % 8)) != 0 { x } else { -x }
                        })
                        .sum::<f64>()
                        / (dims.next_power_of_two() as f64).sqrt();
                    let at = (3 * n + row) * 4;
                    let got = f32::from_le_bytes(got_codes[at..at + 4].try_into().unwrap());
                    assert!((f64::from(got) - expected).abs() < 1e-6);
                }
            }
            let q = vec![0.1; dims];
            let exact = packed.exact_search(&q, 10, None).await.unwrap();
            let ann = packed
                .search(
                    &q,
                    10,
                    SearchParams {
                        nprobe: 8,
                        rerank_factor: 100,
                    },
                    None,
                )
                .await
                .unwrap();
            assert_eq!(ann, exact);
        }
    }

    #[tokio::test]
    async fn packing_rejects_duplicate_rows_mixed_rotations_and_address_overflow() {
        let a = source(8, 0, 42).await;
        let b = source(8, 0, 43).await;
        assert!(
            pack_leaves(vec![
                a.packing_leaf(0, 0, None).await.unwrap(),
                a.packing_leaf(0, 0, None).await.unwrap()
            ])
            .is_err()
        );
        assert!(
            pack_leaves(vec![
                a.packing_leaf(0, 0, None).await.unwrap(),
                b.packing_leaf(0, 1000, None).await.unwrap()
            ])
            .is_err()
        );
        assert!(a.packing_leaf(0, u32::MAX, None).await.is_err());
        assert!(pack_leaves(vec![]).is_err());
        let empty = RoaringBitmap::new();
        assert!(pack_leaves(vec![a.packing_leaf(0, 0, Some(&empty)).await.unwrap()]).is_err());
    }
}
