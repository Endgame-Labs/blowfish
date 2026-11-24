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
