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
    pub(crate) fn width(self) -> usize {
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
        if rows.len() > u32::MAX as usize
            || self.params.iterations == 0
            || self.params.samples_per_cluster == 0
            || self.params.clusters == Some(0)
        {
            return Err(Error::InvalidInput(
                "invalid build parameters or too many rows".into(),
            ));
        }
        let mut stored = Vec::with_capacity(rows.len());
        for (row, v) in rows {
            validate(v.as_ref(), self.dims)?;
            if self.dtype == VectorType::I8
                && v.as_ref()
                    .iter()
                    .any(|x| x.fract() != 0.0 || !(-128.0..=127.0).contains(x))
            {
                return Err(Error::InvalidInput("an invalid i8 value was passed".into()));
            }
            let v: Vec<f32> = v
                .as_ref()
                .iter()
                .map(|x| match self.dtype {
                    VectorType::F32 | VectorType::I8 => *x,
                    VectorType::F16 => half::f16::from_f32(*x).to_f32(),
                })
                .collect();
            validate(&v, self.dims)?;
            stored.push((*row, v));
        }
        stored.sort_unstable_by_key(|x| x.0);
        if stored.windows(2).any(|w| w[0].0 == w[1].0) {
            return Err(Error::InvalidInput("duplicate row ID".into()));
        }
        let training: Vec<Vec<f32>> = stored
            .par_iter()
            .map(|(_, v)| prepare(v, self.metric))
            .collect();
        let k = self
            .params
            .clusters
            .unwrap_or_else(|| (rows.len() as f64).sqrt().ceil() as usize)
            .min(rows.len());
        let flat = rows.len() < self.params.flat_threshold || k <= 1;
        let (centers, parents, parent_ids, assignments) = if flat {
            (
                vec![vec![0.; self.dims]],
                vec![],
                vec![u32::MAX],
                vec![0; rows.len()],
            )
        } else {
            self.cluster(&training, k)?
        };
        crate::format::build_blob(
            self,
            &stored,
            &training,
            centers,
            parents,
            parent_ids,
            assignments,
            flat,
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

    fn cluster(&self, data: &[Vec<f32>], k: usize) -> Result<Clustering> {
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
        let samples: Vec<&[f32]> = sample.iter().map(|i| data[*i].as_slice()).collect();
        if data.len() < self.params.two_level_threshold || k < 16 {
            let centers = train(&samples, k, self.params.iterations, &mut rng);
            let assignments = data.par_iter().map(|v| nearest(v, &centers)).collect();
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
        let assignments = data
            .par_iter()
            .map(|v| {
                let range = &ranges[nearest(v, &parents)];
                range.start + nearest(v, &centers[range.clone()])
            })
            .collect();
        Ok((centers, parents, parent_ids, assignments))
    }
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

/// Unit-normalize `v` for cosine metrics; a copy for Euclidean ones.
pub fn prepare_vector(v: &[f32], metric: Metric) -> Vec<f32> {
    prepare(v, metric)
}
