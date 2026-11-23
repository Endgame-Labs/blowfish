# bf-vector 🐡

Standalone f32/f16 distance kernels, deterministic IVF with corrected one-bit
residual codes, exact reranking, and an immutable range-readable `.bfa` object format.
There are no dependencies on other blowfish crates.

```rust
use bf_vector::{Bytes, Index, IndexBuilder, Metric, SearchParams, Fragment};

# async fn example() -> bf_vector::Result<()> {
let vectors = vec![(7, vec![1.0, 0.0]), (19, vec![0.0, 1.0])];
let blob = IndexBuilder::new(2, Metric::CosineDistance)?.build(&vectors)?;
let (blob, metadata_bytes) = Fragment::build_ann(&blob)?;
// Record metadata_bytes alongside the immutable object key in the manifest.
let index = Index::open_with_metadata_len(Bytes::from(blob), metadata_bytes).await?;
let nearest = index.search(&[1.0, 0.0], 10, SearchParams::default(), None).await?;
assert_eq!(nearest[0], (7, 0.0));
# Ok(())
# }
```

`IndexBuilder::vector_type(VectorType::F16)` rounds f32 inputs to f16 before
training. `build_f16` accepts half-precision inputs directly. Exact reranking
always measures the stored representation. `Metric::distance_f16` calls native, runtime-dispatched NumKong f16 kernels
without converting or allocating. Distances reject non-finite
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
are 128 probes and 64 times k reranking. Small filtered sets (at most 2,048 matching
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

The benchmark binary defaults to 1M synthetic vectors of dimension 768, 100
queries, cosine distance, and nprobe values 16, 128, and 256:

```bash
RAYON_NUM_THREADS=8 cargo run -p bf-vector --release --bin vector-bench
```

Use `--f16`, `--rerank-factor`, `--seed`, `--rows`, `--dims`, `--queries`, and
`--nprobe` to vary the workload. `--vectors` and `--query-vectors` accept raw
row-major little-endian f32 files with no header. Synthetic data consists of
256 separated noisy clusters; queries perturb stored vectors. It is an easy
cluster-routing workload, so results should not be generalized to arbitrary
embeddings. File-based queries below are held-out GloVe vectors.

Real-data reproduction uses the `train` and `test` Parquet files from:

https://huggingface.co/datasets/open-vdb/glove-100-angular

Export the first 100,000 training rows and the first 100 test rows' `emb` column
as row-major f32-LE files. Keep downloads under a git-ignored `data.local/`
directory. Then run:

```bash
cargo run -p bf-vector --release --bin vector-bench -- \
  --rows 100000 --dims 100 --queries 100 --nprobe 16,64,128 \
  --vectors crates/bf-vector/data.local/glove-train.f32 \
  --query-vectors crates/bf-vector/data.local/glove-test.f32

BF_VECTOR_REAL_VECTORS="$(pwd)/crates/bf-vector/data.local/glove-train.f32" \
BF_VECTOR_REAL_QUERIES="$(pwd)/crates/bf-vector/data.local/glove-test.f32" \
  cargo test -p bf-vector --release --test index real_embedding_recall -- --ignored --nocapture
```

The regular test suite leaves this download-dependent test ignored; the command
above explicitly requires 100,000 corpus rows, 100 held-out queries, and mean
recall@10 of at least 0.95 with the default search parameters.

Measured on 2026-11-14 with eight rayon threads on x86-64 (AMD EPYC 9354P),
release mode, a `Bytes` reader, three warm-up queries, and 100 timed queries.
Each query runs alone; timings include code scoring, CRC verification, range
planning, and exact reranking. These measurements exclude network and cache-miss
latency. p99 is the 99th sorted sample among 100 queries, not a long-duration
service tail estimate. All runs use f32 storage, seed 42, and rerank factor 64.

| Corpus | Build time | Index bytes | Metadata bytes | Leaf / parent centroids |
|---|---:|---:|---:|---:|
| Synthetic, 1M × 768 | 20.645 s | 3,233,335,768 | 9,335,704 | 1000 / 32 |
| GloVe, 100k × 100 | 0.384 s | 44,751,004 | 750,940 | 317 / 0 |

| Corpus | nprobe | Recall@10 | p50 | p99 |
|---|---:|---:|---:|---:|
| Synthetic, 1M × 768 | 16 | 0.99800 | 4.829 ms | 8.298 ms |
| Synthetic, 1M × 768 | 128 | 0.99800 | 19.974 ms | 26.429 ms |
| Synthetic, 1M × 768 | 256 | 0.99800 | 35.468 ms | 49.975 ms |
| GloVe, 100k × 100 | 16 | 0.82000 | 0.451 ms | 0.667 ms |
| GloVe, 100k × 100 | 64 | 0.94600 | 1.158 ms | 1.505 ms |
| GloVe, 100k × 100 | 128 | 0.97300 | 2.072 ms | 2.797 ms |

The 128-probe default reaches mean recall@10 of 0.973 on this held-out GloVe
slice; 64 probes reached 0.946, which is why the default was raised. Recall on
different corpora still needs measurement. The million-vector synthetic result
is encouraging for routing and throughput, but its well-separated clusters
make it easier than broad real-world embedding distributions.

`Fragment::build_ann` packs a builder's index into one version-2 ANN object. I keep the resident
directory entries and dense row-to-cluster map at the front, put each leaf's exact center, row set,
row checksums, and codes together in tour order, and follow them with the vectors and footer.
Fast-scan codes and stored vectors are copied byte for byte. Sparse global slab IDs retain their
row sets without allocating a dense map over the namespace. `Fragment::decode` reads just the
resident bytes for directory publication; `Index::open` also loads the leaf blobs for full-index
operations such as recluster. Legacy `.bfv` objects are accepted as upgrade input, and no `.bff`
or `.bfb` fragment writer remains.
