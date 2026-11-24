# bf-text 🐡

Independent text analysis, BM25 scoring, and immutable full-text segments. No other
blowfish crate is a dependency.

`FtsParams` accepts `true` or the full-text schema object, expands the defaults,
and validates incompatible parameters. Expanded metadata preserves null language
for CJK/pre-tokenized fields and null token-length limits for pre-tokenized fields.
`tokenize` analyzes a string, and
`tokenize_array` keeps ordinary array elements separate for phrases. With
`pre_tokenized_array`, each supplied element is a literal token in one sequence.
`Token` retains its normalized text, position, and array element number.

`word_v4` uses the pinned `alyze` analyzer published by turbopuffer. This includes
its Unicode 17 segmentation, ASCII folding, Snowball stemmers, stopword lists,
and optional dictionary-based Chinese, Japanese, and Korean analyzers. The CJK
features download checksum-verified dictionaries during the dependency's build;
consult alyze's offline-build instructions for disconnected builds. `word_v3`
uses the same current UAX #29 implementation. The historical `word_v0`,
`word_v1`, and `word_v2` splitters approximate the documented rules using current
Unicode tables rather than shipping old Unicode databases.

## Engine integration

1. Analyze documents and queries with the field's persisted parameters.
2. Build `Query::new(tokens, last_as_prefix)`. Query repetitions are preserved.
3. Feed `SegmentBuilder::with_tokenizer(include_positions, params.tokenizer)` in
   row order. `add_document(&tokens)` includes present but empty fields;
   `add_missing_document()` reserves a row for absent/null fields.
4. `finish()` returns immutable bytes. Store them under the segment's text key.
5. Implement `RangeRead` over storage/cache, and call `SegmentReader::open`.
6. Merge `reader.stats()` from disjoint, live collections and statistics for the
   WAL tail. Supply the namespace-wide `CorpusStats` to every segment search and
   to `score_document`. Missing fields are excluded; empty fields count as a
   normalization length of one. The engine must adjust counts for deleted or
   superseded documents.
7. Pass a `RoaringBitmap` of live/allowed segment rows to `search`. Merge the
   returned `SearchHit` values and the WAL's scores by global document ID.

Results sort by descending positive score, with smaller row ordinals winning
segment-local ties. Zero-score rows are omitted. An absent corpus DF contributes
zero, so callers must supply query-term statistics. Prefix expansion contributes
one flat point per matching document, including when the final token is an
exact match, regardless of the number of expanded terms.

The boolean helpers and reader methods cover `ContainsAllTokens`,
`ContainsAnyToken`, and `ContainsTokenSequence`. An empty all/sequence query
matches all rows, and an empty any query matches none. Boolean UAX #29 matching
also recognizes punctuation-delimited subwords (`don` in `don't`, and `3` in
`3.14`); underscores are retained. BM25 and phrases retain the original terms.
Phrase positions preserve gaps, repeated terms, and input element boundaries.
Sequence queries reject prefix matching, and indexes without positions return
`io::ErrorKind::Unsupported` for sequence queries.

## Segment format (version 5)

All integers are little-endian. The fixed 72-byte footer records the format,
row count, tokenizer, normalization length, region offsets, and checksums.
Posting blocks contain at most 128 rows, with bit-packed row deltas and term
frequencies. Block directories store first/last rows, maximum TF, minimum
length, and offsets, lengths, and checksums for postings and phrase positions.
Independent TF and length extrema stay conservative when corpus statistics change.

The dictionary is split into bundles of at most 128 consecutive terms, with a
32 KiB payload budget. A large term directory occupies its own bundle. Each bundle
has an FST, checksummed term directories, and the posting payloads for terms with at
most 32 blocks. Its encoding is `FST length (u32) | reserved bytes (u32) | FST |
padding | term directories and short postings`. The dictionary region is reserved
before assigning absolute directory offsets, avoiding a circular dependency on
FST size. Directories retain the version-4 layout:
`DF (u32) | block count (u32) | 52-byte block records | crc32`. A bundle CRC
protects the complete range. Phrase positions remain independent, and document
lengths are packed in blocks of 128 rows. Zero length denotes a missing field.

`text_header` extracts the footer and bundle index for the manifest.
`SegmentReader::open_with_header` opens without I/O; lengths and query bundles
can then load concurrently. A query reads term directories and short postings
from those resident bundles without another backend wave, even with an uncached
`RangeRead`. Bundle, term, and decoded-posting memos are bounded. Decoded posting
payloads use separate 8 MiB allowances for sparse and dense lists, charged to the
engine metadata cache; changing statistics or allowed rows never reuses scores.
Sparse lists receive cache priority. Their known term scores are summed at the same row
before taking window maxima, and dense terms skip rows whose remaining bound
cannot reach the heap threshold. The rounding margin covers all query terms. Larger posting lists still use exact window bounds, reading promising windows before any remaining competitive
windows. Scoring precomputes term weights, caches exact f64 saturation factors for
ordinary lengths and term frequencies, and searches only each window's slice of a
decoded posting block. The factor table refreshes when average length, k1, or b
changes; uncommon values retain the reference arithmetic.

Dense decoded frames are probed after window pruning, so rejected windows do not
retain frames or refresh their CLOCK-cache priority. Query-independent normalization
bounds share a separate 8 MiB allowance: term-window summaries reuse frame maxima and
refresh when newly decoded frames can tighten them. Scores still use the
reference f32 addition order, including repetitions, prefixes, and ties.

Versions 1 through 4 remain readable during upgrades. New writes always use
version 5. The engine automatically rebuilds old indexes from immutable document
columns, one field per fenced publication; there is no layout-selection setting.
The manifest schema is unchanged. The writer repacks the intermediate builder
output into one copy of each posting and directory. Long postings and phrase positions precede the dictionary bundles.

## Validation and compatibility limits

`cargo test -p bf-text` uses offline oracle fixtures, corruption checks, range-read
checks, phrase and array tests, cross-segment statistics tests, explicit pruning
checks, and 96 randomized index-versus-brute-force cases.

The 2026-11-14 live oracle run used tiny throwaway namespaces and approximately
150 requests. Every namespace created was deleted and checked with a list call.
The committed fixture has 37 score cases and 678 filter cases, covering every
documented tokenizer, all 18 languages, punctuation, apostrophes, hyphens,
numbers, Unicode, CJK, emoji, case, length limits, stemming, stopwords, query
repetition, prefixes, empty text, and custom BM25 parameters. Nonexception score
cases agree within 1%; ordinary English cases agree to floating-point precision.

Explicitly recorded exceptions:

- The live Greek stemmer differs from the published Snowball implementation for
  `ανθρώπων` versus `ανθρωπ`. This affects one score case and one boolean case.
- With English stopword removal and stemming, the live boolean filter matches
  `t` inside the removed word `The`; analyzed postings do not retain it. This
  affects one boolean case. The engine can retain raw text for a residual check
  if this edge case needs exact compatibility.
- Historical tokenizer Unicode tables and `word_v3` edge cases are approximated.
- Token length is measured in Unicode scalar values, as confirmed by the oracle,
  despite older API research describing byte limits.

## Benchmark

Run `cargo run --release -p bf-text --example benchmark -- 1000000 200`.
The example builds a deterministic 1M-document corpus with 20M tokens and 4,096
terms, then measures 200 two-term top-10 queries after 10 warm-up queries. It uses
in-memory immutable bytes and omits positions. It reports index bytes, build time,
p50/p95 latency, posting blocks read, and row windows pruned. It does not measure
network latency, indexing tokenization, or engine-level caching.

Historical version-1 result on an 8-vCPU Linux worker: **39,401,769 bytes** (39.40 bytes/document),
**14.285 seconds** to build, **3.863 ms p50**, and **6.543 ms p95**. The mean query
read 66.5 blocks and pruned 69.8 row windows. Results depend on hardware, corpus,
query selectivity, and concurrency.

`SegmentReader::prefetch_plan(query, allowed)` warms the query's dictionary
bundles and directories, and computes posting ranges without reading phrase
positions. `search_prefetched` uses exact window pruning and batched posting
reads. Override `RangeRead::read_batch` to coalesce remote reads. `search_prefetched_with_metrics` also reports posting reads, pruned windows, and
scored rows. Both batched and demand-driven searches return exact results.
