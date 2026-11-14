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

## Version-1 format

All integers are little-endian. There are no architecture-dependent structs or
external crate serialization formats in the container except the versioned FST.

| Region | Contents |
| --- | --- |
| Posting payloads | At most 128 postings per block; first row is in metadata, row deltas and frequencies are padded to 128 values and independently bit-packed with `BitPacker4x`; two one-byte bit widths precede the packed data |
| Position payloads | Optional independently fetched payload per block; one `(element: u32, position: u32)` pair for each occurrence, in posting order |
| Document lengths | One `u32` per row; zero denotes missing, and present empty text has length one |
| Term directory | Term count, then each term's DF, block count, and 52-byte block records |
| Dictionary | `fst::Map`, mapping UTF-8 terms to directory ordinals |
| Footer | Fixed 72 bytes: magic, version, flags, row count, tokenizer ID, total normalization length, three region offsets, three region CRC32 checksums, and footer CRC32 |

Block records contain first/last row IDs, posting count, maximum TF, minimum
normalization length, posting offset/size/CRC32, and position offset/size/CRC32.
The independent TF and length extrema produce conservative score bounds for
arbitrary external statistics and scoring parameters. Storing fixed BM25 scores
would make them unsafe when corpus statistics change.

Opening reads the final 64 KiB and, if needed, the missing directory/length region
in one additional read. Search fetches only selected posting blocks; phrases
fetch positions independently. The reader validates checksums, ranges, ordering,
counts, and packed widths before decoding. It returns I/O errors for corrupt or
unsupported formats. Storage encryption/authentication is the engine's concern.

Search uses block-max MAXSCORE over 128-row windows, evaluates promising windows
first, caches decoded posting blocks within a query, and prunes windows below
the current top-k threshold. Independent block reads within a window run
concurrently. Dictionary-wide subword expansion and many sequential remote
windows remain optimization opportunities; use the engine's range cache for
remote objects. This is an exact segment search, with property tests against
brute-force scoring.

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

Observed on an 8-vCPU Linux worker: **39,401,769 bytes** (39.40 bytes/document),
**14.285 seconds** to build, **3.863 ms p50**, and **6.543 ms p95**. The mean query
read 66.5 blocks and pruned 69.8 row windows. Results depend on hardware, corpus,
query selectivity, and concurrency.
