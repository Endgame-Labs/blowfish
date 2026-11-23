use rayon::prelude::*;

use crate::{Error, Metric, Result, distance::validate, rabitq::Rng};

/// Stored full-precision column type. F16 reranking measures the stored values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorType {
    /// Four-byte IEEE float.
    F32,
    /// Two-byte IEEE half float.
    F16,
    /// One-byte signed integer; inputs must be integral and in [-128, 127].
    I8,
}
impl VectorType {
    /// Bytes per stored element.
    pub fn width(self) -> usize {
        match self {
            Self::F32 => 4,
            Self::F16 => 2,
            Self::I8 => 1,
        }
    }
}

/// Deterministic index training parameters.
#[derive(Clone, Debug)]
pub struct BuildParams {
    /// Seed for training samples and the orthogonal quantization transform.
    pub seed: u64,
    /// Inputs smaller than this count use exact, flat storage.
    pub flat_threshold: usize,
    /// Number of leaf clusters; `None` uses ceil(sqrt(rows)).
    pub clusters: Option<usize>,
    /// Lloyd iterations on the training sample.
    pub iterations: usize,
    /// Maximum training sample size per requested leaf cluster.
    pub samples_per_cluster: usize,
    /// Build a two-level centroid tree at this row count.
    pub two_level_threshold: usize,
    /// Store quantized codes in the blocked fast-scan layout (the default; `BF_ANN_FASTSCAN=0`
    /// turns it off) rather than row by row. Readers handle both.
    pub fastscan: bool,
}
impl Default for BuildParams {
    fn default() -> Self {
        Self {
            seed: 42,
            flat_threshold: 2048,
            clusters: None,
            iterations: 8,
            samples_per_cluster: 32,
            two_level_threshold: 262_144,
            fastscan: crate::format::fastscan_builds(),
        }
    }
}

/// Builder for immutable, little-endian `.bfv` blobs.
#[derive(Clone, Debug)]
pub struct IndexBuilder {
    pub(crate) dims: usize,
    pub(crate) metric: Metric,
    pub(crate) dtype: VectorType,
    pub(crate) params: BuildParams,
}
impl IndexBuilder {
    /// Create a builder. Defaults to f32 storage and deterministic training.
    pub fn new(dims: usize, metric: Metric) -> Result<Self> {
        crate::RaBitQ::new(dims, 0)?;
        Ok(Self {
            dims,
            metric,
            dtype: VectorType::F32,
            params: BuildParams::default(),
        })
    }
    /// Set the full-precision column type. F32 inputs are rounded before training
    /// when storing f16; overflow to infinity is rejected.
    pub fn vector_type(mut self, dtype: VectorType) -> Self {
        self.dtype = dtype;
        self
    }
    /// Replace training parameters.
    pub fn params(mut self, params: BuildParams) -> Self {
        self.params = params;
        self
    }
    /// Build an immutable blob. IDs must be unique; vectors must be finite and
    /// have the declared dimension. Empty indexes are supported.
    pub fn build<V: AsRef<[f32]>>(&self, rows: &[(u32, V)]) -> Result<Vec<u8>> {
        let mut ids = Vec::with_capacity(rows.len());
        let mut data = Vec::with_capacity(rows.len() * self.row_bytes());
        for (row, v) in rows {
            ids.push(*row);
            self.dtype.pack(v.as_ref(), self.dims, &mut data)?;
        }
        self.build_packed(ids, data)
    }
    /// Like [`IndexBuilder::build`], but consumes the rows (kept for callers that already own them).
    pub fn build_owned(&self, rows: Vec<(u32, Vec<f32>)>) -> Result<Vec<u8>> {
        self.build(&rows)
    }
    /// Bytes of one packed row at the builder's stored precision.
    pub fn row_bytes(&self) -> usize {
        self.dims * self.dtype.width()
    }
    /// Build from rows already packed at the stored precision: `data` holds `ids.len()` rows of
    /// [`IndexBuilder::row_bytes`] little-endian bytes each (see [`VectorType::pack`]). Rows stay in
    /// that form for the whole build: training and quantization normalize each one on the fly, so
    /// the peak is about the packed input plus the output blob (4 KB per 1,024-dimension f16 row)
    /// instead of several full-precision copies. The blob is identical to what [`IndexBuilder::build`]
    /// produces for the same rows.
    pub fn build_packed(&self, ids: Vec<u32>, data: Vec<u8>) -> Result<Vec<u8>> {
        if ids.len() > u32::MAX as usize
            || self.params.iterations == 0
            || self.params.samples_per_cluster == 0
            || self.params.clusters == Some(0)
            || data.len() != ids.len() * self.row_bytes()
        {
            return Err(Error::InvalidInput(
                "invalid build parameters or too many rows".into(),
            ));
        }
        let order = if ids.windows(2).all(|w| w[0] <= w[1]) {
            Vec::new()
        } else {
            let mut order: Vec<u32> = (0..ids.len() as u32).collect();
            order.sort_unstable_by_key(|i| ids[*i as usize]);
            order
        };
        let packed = Packed {
            dims: self.dims,
            dtype: self.dtype,
            metric: self.metric,
            ids,
            data,
            order,
        };
        if (0..packed.len().saturating_sub(1)).any(|i| packed.row_id(i) == packed.row_id(i + 1)) {
            return Err(Error::InvalidInput("duplicate row ID".into()));
        }
        let k = self
            .params
            .clusters
            .unwrap_or_else(|| (packed.len() as f64).sqrt().ceil() as usize)
            .min(packed.len());
        let flat = packed.len() < self.params.flat_threshold || k <= 1;
        let (centers, parents, parent_ids, assignments) = if flat {
            (
                vec![vec![0.; self.dims]],
                vec![],
                vec![u32::MAX],
                vec![0; packed.len()],
            )
        } else {
            self.cluster(&packed, k)?
        };
        crate::format::build_blob(
            self,
            &packed,
            centers,
            parents,
            parent_ids,
            assignments,
            flat,
        )
    }
    /// Build from packed rows (as [`IndexBuilder::build_packed`]) that the caller has already
    /// clustered: `centers` are the leaf centers, prepared for the metric (unit length for
    /// cosine), and `assignment[i]` is the cluster of the `i`-th input row. A namespace-wide
    /// recluster assigns rows against every slab's centers, then builds each slab from its share.
    pub fn build_packed_assigned(
        &self,
        ids: Vec<u32>,
        data: Vec<u8>,
        centers: Vec<Vec<f32>>,
        assignment: Vec<usize>,
    ) -> Result<Vec<u8>> {
        if ids.len() > u32::MAX as usize
            || data.len() != ids.len() * self.row_bytes()
            || assignment.len() != ids.len()
            || centers.is_empty()
            || centers.len() > 65_536
            || centers.iter().any(|c| c.len() != self.dims)
            || assignment.iter().any(|c| *c >= centers.len())
        {
            return Err(Error::InvalidInput(
                "invalid rows, centers, or assignment".into(),
            ));
        }
        let order = if ids.windows(2).all(|w| w[0] <= w[1]) {
            Vec::new()
        } else {
            let mut order: Vec<u32> = (0..ids.len() as u32).collect();
            order.sort_unstable_by_key(|i| ids[*i as usize]);
            order
        };
        // Assignments in ID order, as the blob writer walks the rows.
        let assignment = if order.is_empty() {
            assignment
        } else {
            order.iter().map(|i| assignment[*i as usize]).collect()
        };
        let packed = Packed {
            dims: self.dims,
            dtype: self.dtype,
            metric: self.metric,
            ids,
            data,
            order,
        };
        if (0..packed.len().saturating_sub(1)).any(|i| packed.row_id(i) == packed.row_id(i + 1)) {
            return Err(Error::InvalidInput("duplicate row ID".into()));
        }
        let k = centers.len();
        crate::format::build_blob(
            self,
            &packed,
            centers,
            vec![],
            vec![u32::MAX; k],
            assignment,
            false,
        )
    }
    /// Build from already-half-precision vectors, preserving their values.
    pub fn build_f16<V: AsRef<[half::f16]>>(&self, rows: &[(u32, V)]) -> Result<Vec<u8>> {
        if self.dtype != VectorType::F16 {
            return Err(Error::InvalidInput("build_f16 requires F16 storage".into()));
        }
        let rows: Vec<_> = rows
            .iter()
            .map(|(r, v)| {
                (
                    *r,
                    v.as_ref().iter().map(|x| x.to_f32()).collect::<Vec<_>>(),
                )
            })
            .collect();
        self.build(&rows)
    }

    fn cluster(&self, data: &Packed, k: usize) -> Result<Clustering> {
        if k > 65_536 {
            return Err(Error::InvalidInput(
                "at most 65536 leaf clusters are supported".into(),
            ));
        }
        let mut rng = Rng(self.params.seed);
        let mut sample: Vec<usize> = (0..data.len()).collect();
        let count = k
            .saturating_mul(self.params.samples_per_cluster)
            .min(data.len());
        for i in 0..count {
            let j = i + rng.next() as usize % (data.len() - i);
            sample.swap(i, j);
        }
        sample.truncate(count);
        // Only the sample is held normalized in full precision.
        let prepared: Vec<Vec<f32>> = sample.par_iter().map(|i| data.prepared(*i)).collect();
        drop(sample);
        let samples: Vec<&[f32]> = prepared.iter().map(|v| v.as_slice()).collect();
        if data.len() < self.params.two_level_threshold || k < 16 {
            let centers = train(&samples, k, self.params.iterations, &mut rng);
            let assignments = (0..data.len())
                .into_par_iter()
                .map(|i| nearest(&data.prepared(i), &centers))
                .collect();
            return Ok((centers, vec![], vec![u32::MAX; k], assignments));
        }
        let p = (k as f64).sqrt().ceil() as usize;
        let parents = train(&samples, p, self.params.iterations, &mut rng);
        let mut groups = vec![Vec::new(); p];
        for sample in samples {
            groups[nearest(sample, &parents)].push(sample);
        }
        let mut centers = Vec::new();
        let mut parent_ids = Vec::new();
        let mut ranges = Vec::new();
        for (parent, group) in groups.iter().enumerate() {
            let start = centers.len();
            let requested = k / p + usize::from(parent < k % p);
            if !group.is_empty() {
                let children = train(
                    group,
                    requested.min(group.len()),
                    self.params.iterations,
                    &mut rng,
                );
                parent_ids.extend(std::iter::repeat_n(parent as u32, children.len()));
                centers.extend(children);
            } else {
                centers.push(parents[parent].clone());
                parent_ids.push(parent as u32);
            }
            ranges.push(start..centers.len());
        }
        drop(groups);
        drop(prepared);
        let assignments = (0..data.len())
            .into_par_iter()
            .map(|i| {
                let v = data.prepared(i);
                let range = &ranges[nearest(&v, &parents)];
                range.start + nearest(&v, &centers[range.clone()])
            })
            .collect();
        Ok((centers, parents, parent_ids, assignments))
    }
}

/// Rows held at the stored precision (little-endian bytes), optionally behind a sort order.
pub(crate) struct Packed {
    pub dims: usize,
    pub dtype: VectorType,
    pub metric: Metric,
    ids: Vec<u32>,
    data: Vec<u8>,
    /// Row positions sorted by ID; empty when `ids` is already sorted.
    order: Vec<u32>,
}
impl Packed {
    pub fn len(&self) -> usize {
        self.ids.len()
    }
    fn at(&self, i: usize) -> usize {
        if self.order.is_empty() {
            i
        } else {
            self.order[i] as usize
        }
    }
    /// The row ID of the `i`th row in ID order.
    pub fn row_id(&self, i: usize) -> u32 {
        self.ids[self.at(i)]
    }
    /// The stored bytes of the `i`th row in ID order.
    pub fn raw(&self, i: usize) -> &[u8] {
        let w = self.dims * self.dtype.width();
        let at = self.at(i) * w;
        &self.data[at..at + w]
    }
    /// The `i`th row decoded to f32 and normalized for the metric.
    pub fn prepared(&self, i: usize) -> Vec<f32> {
        let raw = self.raw(i);
        let v: Vec<f32> = match self.dtype {
            VectorType::F32 => raw
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect(),
            VectorType::I8 => raw.iter().map(|b| *b as i8 as f32).collect(),
            VectorType::F16 => {
                let mut out = Vec::with_capacity(raw.len() / 2);
                crate::index::extend_f16(raw, &mut out);
                out
            }
        };
        prepare_owned(v, self.metric)
    }
}

impl VectorType {
    /// Append one vector to `out` at this stored precision, validating it first: finite and
    /// `dims` long; integral and within [-128, 127] for I8; finite after rounding for F16.
    pub fn pack(self, v: &[f32], dims: usize, out: &mut Vec<u8>) -> Result<()> {
        validate(v, dims)?;
        match self {
            Self::F32 => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Self::I8 => {
                if v.iter()
                    .any(|x| x.fract() != 0.0 || !(-128.0..=127.0).contains(x))
                {
                    return Err(Error::InvalidInput("an invalid i8 value was passed".into()));
                }
                out.extend(v.iter().map(|x| *x as i8 as u8));
            }
            Self::F16 => {
                for x in v {
                    let h = half::f16::from_f32(*x);
                    if !h.is_finite() {
                        return Err(Error::InvalidInput(
                            "expected matching, nonempty, finite vectors".into(),
                        ));
                    }
                    out.extend_from_slice(&h.to_le_bytes());
                }
            }
        }
        Ok(())
    }
}

fn prepare_owned(mut v: Vec<f32>, metric: Metric) -> Vec<f32> {
    if metric == Metric::EuclideanSquared {
        return v;
    }
    let norm = v.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    if norm == 0. {
        return v;
    }
    for x in v.iter_mut() {
        *x = (*x as f64 / norm) as f32;
    }
    v
}

type Clustering = (Vec<Vec<f32>>, Vec<Vec<f32>>, Vec<u32>, Vec<usize>);

pub(crate) fn prepare(v: &[f32], metric: Metric) -> Vec<f32> {
    if metric == Metric::EuclideanSquared {
        return v.to_vec();
    }
    let norm = v.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    if norm == 0. {
        return v.to_vec();
    }
    v.iter().map(|x| (*x as f64 / norm) as f32).collect()
}

pub(crate) fn nearest(v: &[f32], centers: &[Vec<f32>]) -> usize {
    centers
        .iter()
        .enumerate()
        .map(|(i, c)| (i, Metric::EuclideanSquared.distance_unchecked(v, c)))
        .min_by(|(ia, a), (ib, b)| a.total_cmp(b).then(ia.cmp(ib)))
        .unwrap()
        .0
}

fn train(data: &[&[f32]], k: usize, iterations: usize, rng: &mut Rng) -> Vec<Vec<f32>> {
    let dims = data[0].len();
    let mut ids: Vec<_> = (0..data.len()).collect();
    for i in 0..k {
        let j = i + rng.next() as usize % (data.len() - i);
        ids.swap(i, j);
    }
    let mut centers: Vec<_> = ids[..k].iter().map(|i| data[*i].to_vec()).collect();
    for _ in 0..iterations {
        let assignments: Vec<_> = data.par_iter().map(|v| nearest(v, &centers)).collect();
        // Fixed-order accumulation makes the blob independent of rayon scheduling.
        let mut sums = vec![vec![0f64; dims]; k];
        let mut counts = vec![0usize; k];
        for (v, c) in data.iter().zip(assignments) {
            counts[c] += 1;
            for (sum, x) in sums[c].iter_mut().zip(*v) {
                *sum += *x as f64;
            }
        }
        for c in 0..k {
            if counts[c] == 0 {
                centers[c] = data[rng.next() as usize % data.len()].to_vec();
            } else {
                for d in 0..dims {
                    centers[c][d] = (sums[c][d] / counts[c] as f64) as f32;
                }
            }
        }
    }
    centers
}

/// Lloyd k-means on `data` (rows already prepared for the metric), for coarse partitions such as
/// the slab cells of a recluster. Deterministic for a seed.
pub fn kmeans(data: &[&[f32]], k: usize, iterations: usize, seed: u64) -> Vec<Vec<f32>> {
    train(data, k.min(data.len()), iterations, &mut Rng(seed))
}

/// Index of the center nearest to `v` (squared Euclidean distance).
pub fn nearest_center(v: &[f32], centers: &[Vec<f32>]) -> usize {
    nearest(v, centers)
}

/// Squared Euclidean distance between two vectors of equal length, unvalidated (for hot loops
/// over vectors already checked, such as rows against k-means centers).
pub fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    Metric::EuclideanSquared.distance_unchecked(a, b)
}

/// Unit-normalize `v` for cosine metrics; a copy for Euclidean ones.
pub fn prepare_vector(v: &[f32], metric: Metric) -> Vec<f32> {
    prepare(v, metric)
}
