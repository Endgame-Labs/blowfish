//! Exact vectors for a slab, read back from their source segments.
//!
//! The recluster spills 8-bit codes (see `sq8`), so a slab's own vectors come from where the
//! copy pass found them: each row remembers its input, leaf, and place in the leaf. A slab reads
//! each leaf it touches once, with at most [`LEAVES_IN_FLIGHT`] leaves (and their checks) in
//! flight, and checks every vector against its spilled row before it is used.
use super::sq8::Sq8;
use crate::{segment, vector_locator::inputs::Input};
use bf_types::ApiError;
use futures::{StreamExt, TryStreamExt};
use std::{
    collections::HashMap,
    os::unix::fs::FileExt,
    sync::{Arc, atomic::AtomicU32, atomic::Ordering},
};

/// Source leaves read at once. A leaf is about 1.3 MB at 640 rows of 1,024 f16 dimensions.
const LEAVES_IN_FLIGHT: usize = 64;
/// Attempts for one leaf's read before the build fails (a slab then restarts with the next build).
const ATTEMPTS: u32 = 3;
/// Open source indexes kept between slabs. Past this, the cache starts over.
const OPEN_INDEXES: usize = 64;

/// Where each global row's exact vector is, filled by the copy pass.
pub(super) struct Locations {
    input: Vec<AtomicU32>,
    cluster: Vec<AtomicU32>,
    ordinal: Vec<AtomicU32>,
}

impl Locations {
    pub fn new(rows: usize) -> Self {
        let column = || (0..rows).map(|_| AtomicU32::new(0)).collect();
        Self {
            input: column(),
            cluster: column(),
            ordinal: column(),
        }
    }

    pub fn set(&self, row: u32, input: u32, cluster: u32, ordinal: u32) {
        let at = row as usize;
        self.input[at].store(input, Ordering::Relaxed);
        self.cluster[at].store(cluster, Ordering::Relaxed);
        self.ordinal[at].store(ordinal, Ordering::Relaxed);
    }

    fn freeze(self) -> Vec<[u32; 3]> {
        (0..self.input.len())
            .map(|i| {
                [
                    self.input[i].load(Ordering::Relaxed),
                    self.cluster[i].load(Ordering::Relaxed),
                    self.ordinal[i].load(Ordering::Relaxed),
                ]
            })
            .collect()
    }
}

/// Reads and checks the exact vectors of a slab's rows.
pub(super) struct Source {
    store: bf_storage::Storage,
    inputs: Arc<Vec<Input>>,
    locations: Vec<[u32; 3]>,
    spill: Arc<std::fs::File>,
    spill_offsets: Arc<Vec<u32>>,
    sq8: Arc<Sq8>,
    dims: usize,
    dtype: bf_vector::VectorType,
    open: tokio::sync::Mutex<HashMap<u32, Arc<bf_vector::Index<segment::ObjectReader>>>>,
}

/// One leaf's wanted rows: `(global row, place in the leaf, slot in the slab)`.
type Wanted = Vec<(u32, u32, usize)>;

impl Source {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: bf_storage::Storage,
        inputs: Arc<Vec<Input>>,
        locations: Locations,
        spill: Arc<std::fs::File>,
        spill_offsets: Arc<Vec<u32>>,
        sq8: Arc<Sq8>,
        dims: usize,
        dtype: bf_vector::VectorType,
    ) -> Self {
        Self {
            store,
            inputs,
            locations: locations.freeze(),
            spill,
            spill_offsets,
            sq8,
            dims,
            dtype,
            open: Default::default(),
        }
    }

    fn width(&self) -> usize {
        self.dims * self.dtype.width()
    }

    async fn index(
        &self,
        input: u32,
    ) -> Result<Arc<bf_vector::Index<segment::ObjectReader>>, ApiError> {
        let mut open = self.open.lock().await;
        if let Some(index) = open.get(&input) {
            return Ok(index.clone());
        }
        let file = &self.inputs[input as usize].file;
        let index = Arc::new(
            bf_vector::Index::open_with_metadata_len(
                segment::ObjectReader::new(&self.store, file).bypass_cache(),
                file.metadata_len,
            )
            .await
            .map_err(segment::index_error)?,
        );
        if index.dims() != self.dims || index.vector_type() != self.dtype {
            return Err(segment::corrupt());
        }
        if open.len() >= OPEN_INDEXES {
            open.clear();
        }
        open.insert(input, index.clone());
        Ok(index)
    }

    /// The stored bytes of `rows`, in order (`width` bytes each at the stored precision).
    pub async fn rows(&self, rows: &[u32]) -> Result<Vec<u8>, ApiError> {
        let width = self.width();
        // Group by leaf, in source order, so each leaf is read once.
        let mut order: Vec<(u32, u32, u32, u32, usize)> = rows
            .iter()
            .enumerate()
            .map(|(slot, g)| {
                let [input, cluster, ordinal] = self.locations[*g as usize];
                (input, cluster, ordinal, *g, slot)
            })
            .collect();
        order.sort_unstable();
        let mut leaves: Vec<(u32, u32, Wanted)> = Vec::new();
        for (input, cluster, ordinal, g, slot) in order {
            match leaves.last_mut() {
                Some((i, c, wanted)) if (*i, *c) == (input, cluster) => {
                    wanted.push((g, ordinal, slot));
                }
                _ => leaves.push((input, cluster, vec![(g, ordinal, slot)])),
            }
        }
        let mut data = vec![0u8; rows.len() * width];
        let mut stream = futures::stream::iter(
            leaves
                .into_iter()
                .map(|(input, cluster, wanted)| self.leaf(input, cluster, wanted)),
        )
        .buffer_unordered(LEAVES_IN_FLIGHT);
        while let Some((slots, packed)) = stream.try_next().await? {
            for (slot, bytes) in slots.into_iter().zip(packed.chunks_exact(width)) {
                data[slot * width..(slot + 1) * width].copy_from_slice(bytes);
            }
        }
        Ok(data)
    }

    /// One leaf: read it (again, on a failed read), check each wanted row, and pack them.
    async fn leaf(
        &self,
        input: u32,
        cluster: u32,
        wanted: Wanted,
    ) -> Result<(Vec<usize>, Vec<u8>), ApiError> {
        let mut attempt = 1;
        let (ids, bytes) = loop {
            let read = async {
                self.index(input)
                    .await?
                    .read_cluster_rows(cluster as usize)
                    .await
                    .map_err(segment::index_error)
            };
            match read.await {
                Ok(leaf) => break leaf,
                Err(error) if attempt < ATTEMPTS => {
                    tracing::warn!(%error, attempt, "recluster: source leaf read failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(200 * u64::from(attempt)))
                        .await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        };
        let (inputs, spill, offsets, sq8) = (
            self.inputs.clone(),
            self.spill.clone(),
            self.spill_offsets.clone(),
            self.sq8.clone(),
        );
        let (dims, dtype, width) = (self.dims, self.dtype, self.width());
        tokio::task::spawn_blocking(move || {
            if bytes.len() != ids.len() * width {
                return Err(segment::corrupt());
            }
            let mut slots = Vec::with_capacity(wanted.len());
            let mut packed = Vec::with_capacity(wanted.len() * width);
            let mut spilled = vec![0u8; dims];
            for (g, ordinal, slot) in wanted {
                let at = ordinal as usize;
                let local = *ids.get(at).ok_or_else(segment::corrupt)?;
                // The row at this place is the row the copy pass saw there.
                if inputs[input as usize].global(local)? != Some(g) {
                    return Err(segment::corrupt());
                }
                let exact = &bytes[at * width..(at + 1) * width];
                spill
                    .read_exact_at(&mut spilled, u64::from(offsets[g as usize]) * dims as u64)
                    .map_err(segment::index_error)?;
                sq8.verify(dtype, exact, &spilled)?;
                packed.extend_from_slice(exact);
                slots.push(slot);
            }
            Ok((slots, packed))
        })
        .await
        .map_err(segment::index_error)?
    }
}
