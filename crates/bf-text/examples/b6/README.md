# Round B6 offline kill tests

This disposable example embeds the current reader for private metadata access.
It changes no public API, query option, or persisted format. `bands_search.rs`
is a copy of `search_internal`, with bitmap bounds applied before posting reads
and the competitive-row mask. Keep the baseline copy synchronized when replaying.

Prepare the first million MS MARCO passages with the `lines` iterator in
`bench/scale/msmarco_load.py`, and run the ignored `prepare_msmarco` experiment
with `BF_TEXT_EXPERIMENT=FIXTURES`. Copy `bm25-msmarco.json` to `workload.json`.
Generate `truth.json` with the independent `exact_bm25` example and the corpus.
Keep all fixtures and outputs in an owned `.scratch/` directory, and remove them
when the run finishes.

Build `cargo build --release -p bf-text --example b6`. Run
`b6 FIXTURES baseline|ta|bands4|bands8|footprint`. Every measured mode first
checks the production reader against exhaustive truth, warms one full workload,
and measures the next pass. Ordered rows and every f32 score bit must agree.
The workload uses consecutive passage IDs, checked against exhaustive truth.

TA sorts query-term postings by exact saturation under current namespace
statistics, packs 128 absolute row IDs with BitPacker4x, and stores block maxima.
Each surfaced row is completed once from a row-offset-indexed forward TF slice.
Forward entries use lexical term-ID deltas, fold tf=1 into the delta's low bit,
and encode other TFs as unsigned LEB128. The byte count includes offsets. A
frequency-ranked term-ID alternative is also sized, including its ID mapping.
TA shares one top-10 heap across consecutive segments and terminates only on a
strictly lower canonical-order f32 bound, retaining score ties.

Bitmap modes build four or eight disjoint upward-rounded saturation bands for
the namespace's 32 most frequent terms. During window selection, they combine
frequent-term row bounds with other dense-term window bounds and exact sparse
partials. Empty windows are removed before posting prefetch; surviving rows keep
the normal scorer. There is no query-truth seed floor. Normalization-specific
impact order, maxima, and bands are prepared outside counters, so this is an
optimistic warm-cache experiment, without lifecycle or cold-I/O claims.

Use the B5 perf protocol: `BF_TEXT_PERF_CONTROL` and `BF_TEXT_PERF_ACK` identify
FIFOs passed to `perf stat --delay=-1 --control=fifo:CONTROL,ACK`. Only the second
pass is counted. `BF_TEXT_CYCLES=1` also opens a user-space hardware cycle event
for per-query p90 cycles. Its two reads per query are charged to outer counters.
Run baseline and candidates pinned to one CPU on the same owned Graviton host.

Run `cargo fmt`, explicit `rustfmt --edition 2024` for included files, clippy,
`cargo test -p bf-text`, and `cargo test -p bf-text --example b6` before committing.
