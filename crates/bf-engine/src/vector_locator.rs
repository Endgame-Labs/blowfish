//! Stored-precision vectors addressed through immutable tier coverage and reverse lineage.
use crate::{NamespaceState, lineage, read_cache, segment};
use bf_storage::Storage;
use bf_types::ApiError;
use bf_vector::RoaringBitmap;
use bf_wal::{Lineage, Segment, SlabSet};
use std::{collections::BTreeMap, sync::Arc};

/// Snapshot-owned sources: readers never resolve a row against a newer publication.
#[derive(Clone)]
#[allow(dead_code)] // Wired into projection in the next ordered step.
pub(crate) struct Sources {
    tiers: Arc<BTreeMap<String, Vec<SlabSet>>>,
    lineage: Arc<Vec<Lineage>>,
}
#[allow(dead_code)] // Wired into projection in the next ordered step.
impl Sources {
    pub fn new(state: &NamespaceState) -> Self {
        Self {
            tiers: state.ann_tiers.clone(),
            lineage: state.lineage.clone(),
        }
    }

    /// Resolve requested segment rows through coverage, reverse lineage, and the resident
    /// ascending slab row lists. Slabs address vectors by global row; their list ordinal is
    /// the slab-local row. Missing vectors remain absent, including sparse columns.
    pub async fn read(
        &self,
        store: &Storage,
        seg: &Segment,
        rows: &RoaringBitmap,
        attr: &str,
        bypass: bool,
    ) -> Result<(Vec<u32>, Vec<u8>), ApiError> {
        let sets = self
            .tiers
            .get(attr)
            .ok_or_else(|| segment::index_error("vector source has no covering tier"))?;
        let (dims, dtype) = seg
            .schema
            .get(attr)
            .and_then(segment::dense_type)
            .ok_or_else(segment::corrupt)?;
        let width = dims * dtype.width();
        let mut found: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        let mut addressed = RoaringBitmap::new();
        for set in sets {
            let mut global_to_row = BTreeMap::new();
            for cover in &set.covered {
                if cover.segment == seg.id {
                    for row in rows {
                        if row >= cover.rows {
                            return Err(segment::corrupt());
                        }
                        global_to_row.insert(
                            cover.base.checked_add(row).ok_or_else(segment::corrupt)?,
                            row,
                        );
                        addressed.insert(row);
                    }
                } else if let Some(entry) = self.lineage.iter().find(|l| l.source == cover.segment)
                {
                    for part in entry.parts.iter().filter(|part| part.target == seg.id) {
                        let map = lineage::load(store, part).await?;
                        for row in rows & &map.to {
                            let source = map.back(row).ok_or_else(segment::corrupt)?;
                            if source >= cover.rows {
                                return Err(segment::corrupt());
                            }
                            global_to_row.insert(
                                cover
                                    .base
                                    .checked_add(source)
                                    .ok_or_else(segment::corrupt)?,
                                row,
                            );
                            addressed.insert(row);
                        }
                    }
                }
            }
            if global_to_row.is_empty() {
                continue;
            }
            for slab in &set.slabs {
                let file = crate::upgrade::files(slab, attr)
                    .ok_or_else(segment::corrupt)?
                    .0;
                let key = format!("slab-rows:{}", file.key);
                let resident: Arc<Vec<u32>> = if let Some(rows) = read_cache::get(&key, 0) {
                    rows
                } else {
                    let index = segment::vector_index(store, file).await?;
                    if index.dims() != dims || index.vector_type() != dtype {
                        return Err(segment::corrupt());
                    }
                    let rows = index.row_ids();
                    if rows.len() != slab.row_count as usize {
                        return Err(segment::corrupt());
                    }
                    read_cache::insert(&key, (rows.len() * 4) as u64, rows)
                };
                let wanted: RoaringBitmap = global_to_row
                    .keys()
                    .filter(|r| resident.binary_search(r).is_ok())
                    .copied()
                    .collect();
                if wanted.is_empty() {
                    continue;
                }
                // Slab schemas are empty in the manifest; the covered segment supplies shape.
                let mut source = slab.clone();
                source.schema = seg.schema.clone();
                let (present, packed) =
                    segment::read_own_ann_packed(store, &source, &wanted, attr, bypass).await?;
                if packed.len() != present.len() * width {
                    return Err(segment::corrupt());
                }
                for (global, bytes) in present.into_iter().zip(packed.chunks_exact(width)) {
                    let row = *global_to_row.get(&global).ok_or_else(segment::corrupt)?;
                    if found.insert(row, bytes.to_vec()).is_some() {
                        return Err(segment::index_error("vector row has multiple tier sources"));
                    }
                }
            }
        }
        if rows - &addressed != RoaringBitmap::new() {
            return Err(segment::index_error("vector rows are not fully covered"));
        }
        let mut present = Vec::with_capacity(found.len());
        let mut packed = Vec::with_capacity(found.len() * width);
        for (row, bytes) in found {
            present.push(row);
            packed.extend(bytes);
        }
        Ok((present, packed))
    }
}

#[cfg(test)]
#[path = "vector_locator_tests.rs"]
mod tests;
