# Round B5 offline kill tests

This example embeds the unchanged text reader to inspect private metadata. It adds
no library API, production query option, or text format. Build with
`cargo build --release -p bf-text --example b5`.

Prepare the first million passages as `corpus.jsonl`, using the streaming `lines`
function in `bench/scale/msmarco_load.py`. Keep all fixtures and logs in an owned
`.scratch/` directory. Set `BF_TEXT_EXPERIMENT` to that directory, and run
`cargo test --release -p bf-text prepare_msmarco -- --ignored --nocapture`.
Copy `bench/scale/data/bm25-msmarco.json` there as `workload.json`.

Run `b5 FIXTURES MODE`, where MODE is `baseline`, `bmp`, `baseline-single`,
`bmp-single`, `baseline-1000`, `bmp-1000`, or `footprint`. Each search verifies all
ordered rows and f32 scores against the production reader. The first pass warms
the workload. Optional `BF_TEXT_PERF_CONTROL` and `BF_TEXT_PERF_ACK` FIFOs delimit
only the second pass for `perf stat --delay=-1 --control=fifo:CONTROL,ACK`.
Use a paired baseline on the same machine and in the same release binary.

BMP uses 64-document forward blocks after a deterministic lexical sort by the
rarest term, second-rarest term, length, and original row. Block saturation maxima
are rounded upward into u8 values. Query contributions accumulate in lexical term
order, bounds sort globally across segments, and exact scoring preserves original
row ties. This deliberately optimistic prototype scores predecoded forward lists
and rebuilds bounds for the current namespace statistics outside counters. A
failure rejects the proposed cheap clustering even before charging decompression
or handling changes to namespace normalization. Serialized bytes are codec
accounting, including retained phrase bytes, rather than an implemented format.

The footprint mode round-trips every partial posting block using actual-length
unsigned varints, accounts for variable-width block records and implied offsets,
and validates each phrase stream. It retains existing dictionary overhead in its
estimate. A passing estimate still requires an actual serialized format, reader
validation, and automatic migration before shipping.

The cold prototype is a disposable checkpoint experiment. The packer groups 128
(default) or `B5_PACK_TERMS=256` global lexical terms. Each group includes corpus
DF, every segment's unchanged block directory, and postings for lists of at most
32 blocks. Long postings and positions retain their original objects and offsets.
File fingerprints are checked for collisions in the fixture. The existing
statistics index carries group addresses in the experimental manifest; the normal
compact reader-copy threshold and all resulting reads are charged.

To reproduce it, build the original format-6 server from the first B5 commit,
load one owned local `b5-msmarco` namespace with `msmarco_load.py`, wait until its
checkpoint covers the last WAL entry, and **stop its writer**. Put that local store
under `FIXTURES/checkpoint`. Run these commands with an owned `.scratch/` fixture:

```
uv run crates/bf-text/examples/b5/prepare_cold.py --scratch FIXTURES inspect
b5 FIXTURES pack
uv run crates/bf-text/examples/b5/prepare_cold.py --scratch FIXTURES repack
```

Only in a disposable source copy, run `uv run
crates/bf-text/examples/b5/patch_cold.py`, and build the scale server using its
standalone manifest. The patch loads statistics and term metadata together,
overlaps independent document-length reads, waits for shared term data before
installing query readers, and satisfies
short-posting reads from the fetched group. It supports this single-field,
fresh-process workload; it is not a production API or publication implementation.
Restore the source copy after saving the candidate binary. Replay with:

```
uv run crates/bf-text/examples/b5/cold_facade.py --scratch RESULTS \
  --baseline BASE_SERVER --candidate PACK_SERVER \
  --baseline-store FIXTURES/checkpoint --candidate-store FIXTURES/checkpoint-pack \
  --candidate-env BF_B5_PACK=1
```

The driver alternates arm order, asserts complete response equality, starts a
fresh process with an empty disk cache per query, and closes its children and
cache on exit. Each facade delays requests by 25 ms. Sum `get` and `get_range`
metrics; counting only `get` misses posting payload reads. Inspect per-query traces
with `bench/scale/waves.py`. Repeat packing at 256 terms before the next replay.
For the production codec control, automatically upgrade a separate hard-linked
checkpoint copy with the unpatched candidate server, stop its writer, and run the
same replay without `--candidate-env`.

`cargo fmt` does not discover every `include!` body. Also run
`rustfmt --edition 2024 --check crates/bf-text/examples/b5/*.rs` when changing
these prototypes. All fixture data, binary copies, logs, and temporary source
copies must be removed after copying the useful measurements into the report.
