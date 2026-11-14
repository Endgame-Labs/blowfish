# bf-vector 🐡

Standalone f32/f16 distance kernels, deterministic IVF with corrected one-bit
residual codes, exact reranking, and an immutable range-readable `.bfv` format.
There are no dependencies on other blowfish crates.

```rust
use bf_vector::{Bytes, Index, IndexBuilder, Metric, SearchParams, metadata_len};

# async fn example() -> bf_vector::Result<()> {
let vectors = vec![(7, vec![1.0, 0.0]), (19, vec![0.0, 1.0])];
let blob = IndexBuilder::new(2, Metric::CosineDistance)?.build(&vectors)?;
let metadata_bytes = metadata_len(&blob)?;
// Record metadata_bytes alongside the immutable object key in the manifest.
let index = Index::open_with_metadata_len(Bytes::from(blob), metadata_bytes).await?;
let nearest = index.search(&[1.0, 0.0], 10, SearchParams::default(), None).await?;
assert_eq!(nearest[0], (7, 0.0));
# Ok(())
# }
```

`IndexBuilder::vector_type(VectorType::F16)` rounds f32 inputs to f16 before
training. `build_f16` accepts half-precision inputs directly. Exact reranking
always measures the stored representation. `Metric::distance_f16` converts half
values exactly to f32 before calling the runtime-dispatched NumKong kernels.
This conversion is portable, but does allocate. Distances reject non-finite
inputs and mismatched dimensions. Cosine is `1 - cosine_similarity` in `[0, 2]`;
squared Euclidean is the sum of squared differences. As an explicit zero-vector
policy, two zero vectors have cosine distance zero, and one zero vector has
cosine distance one. This policy is not yet verified against the upstream oracle.

`exact_search` measures supplied `(u32, vector)` pairs, and `Index::exact_search`
measures stored vectors. Results sort by distance, then row ID. Filters are
allowlists of row IDs, expressed as `RoaringBitmap`; unknown rows are ignored.
`recall_at_k` compares result sets without counting duplicates.

Training uses seeded sampling, eight Lloyd iterations, parallel assignments,
and fixed-order f64 centroid accumulation. The default is ceil(sqrt(n)) leaf
clusters, with a two-level tree from 262,144 rows. Empty clusters are retained;
empty indexes are valid. Inputs below 2,048 rows skip codes and use exact scans.
Dimensions from 1 to 65,536 are supported, with at most 65,536 leaf clusters.

The RaBitQ estimator retains both the residual norm and the normalized
residual/code inner product, rather than assuming the sign code is the vector.
Three seeded signed Hadamard transforms supply an orthogonal rotation in padded
power-of-two dimensions. This is a fast structured rotation, rather than the
paper's dense Haar rotation: the public `DistanceEstimate` exposes a conservative,
deterministic Cauchy–Schwarz error bound, including a numerical margin. The
paper's sharper probabilistic guarantee is not claimed. Query-local lookup tables
sum eight sign-weighted coordinates per byte without quantizing the query.
Reference: https://arxiv.org/abs/2405.12497.

`SearchParams` controls leaf probing and the candidate rerank budget. The defaults
are 64 probes and 64 times k reranking. Small filtered sets (at most 2,048 matching
rows) use exact vector reads without codes. Other filters skip empty clusters,
scale probe counts inversely with selectivity, and widen until the candidate
budget is met. No ANN parameter setting guarantees recall for every distribution;
probe all clusters and rerank all candidates for exact behavior.

The version-1 format uses explicit little-endian fields, with no Rust memory
layouts on disk:

1. Front metadata: metric, dtype, dimensions, seed, parent and leaf centroids,
   cluster offsets, Roaring row membership, and per-vector CRC32 checksums.
2. Each contiguous cluster: residual codes and correction factors, followed by
   full vectors in the declared f32 or f16 type. Ordinals follow sorted row IDs.
3. A 64-byte footer: magic, format version, metadata length, total object length,
   metadata CRC32, reserved bytes, and footer CRC32. Code blocks and full-vector
   blocks also have CRC32 checksums in the cluster directory.

Implement `RangeRead` for the engine's cached object store. Reads must return the
exact requested range or an error; batch responses must retain request order.
The default `read_batch` runs up to 32 reads concurrently; backends can override
it for coalescing. `Vec<u8>`, `Bytes` (zero-copy slices), and `Arc<R>` are supported.
A normal `Index::open` reads the footer, then metadata. Persist `metadata_len`
in the segment manifest and use `open_with_metadata_len` to fetch both in one
parallel batch. Search then batches codes, followed by selected vectors: three
logical sequential stages including opening. Dense rerank selections fetch whole
clusters when at least a third of their vectors are needed; adjacent sparse
vector ranges coalesce. Metadata stays resident; cache codes and vectors in the
reader. The async API performs CPU scoring inline; the engine should run it on
its dedicated CPU pool or a blocking executor.

Run `cargo test -p bf-vector` for kernel properties, exact-vs-naive properties,
round trips, corruption rejection, filtered recall, deterministic builds, tree
routing, and range-read stage checks. CRC32 detects accidental corruption; it is
not an authenticity mechanism.
