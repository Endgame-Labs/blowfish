# Testing notes

## Round T2: four load-sensitive test failures, each with a root cause

Several full-workspace gates failed once on tests that pass alone. Each failure below was reproduced
under load (the test or its whole binary in a loop, niced, pinned to four cores, beside busy loops
and other workers' builds), its cause was captured, and the fix removes the assumption instead of
widening a timeout. None of the fixes changes what the server does.

### 1. `warm_graph::tests::filtered_scan_hides_shadowed_rows_inside_the_allowed_set` ("graph never admitted")

**Cause (a test-only accounting error).** Every `Engine` charges its caches' *nominal* budgets
(`decoded_cache_bytes` plus the store's memory cache, each capped at the memory limit) to a
process-wide `FOREGROUND` counter, and a warm graph is admitted only if its reservation fits in
`0.8 * limit - FOREGROUND`. A server has one engine, so the sum is its real cache budget. The unit
tests of one process each build their own engines, most with the default budgets (a quarter of
memory each), and they stay alive while they run. Under load the tests are slow, so many engines
overlap: in a full `bf-engine` lib run at 12 test threads, `FOREGROUND` reached 46.8 to 59.5 GB
against a 33.6 GB limit. `cap - foreground` saturates to zero, no reservation can succeed, and
`admitted_entry` polled for its 120 seconds and failed. Six warm-graph tests failed together in
that run (`filtered_scan_hides_shadowed_rows...`, `selective_filters_scan...`,
`shadowed_rows_stay_traversable...`, `recall_trials_admit_sparse_graph_reads...`,
`source_identity_masks_wal_fallback...`, and `typed_ids_path_is_byte_identical...`); the diagnostics
showed `building=false slot=false reserved=0 foreground=47-59 GB`, so no other build held anything.
The test's own exclusive gate cannot help: the engines that sum past the cap belong to tests that
do not take it.

**Fix.** Under `cfg(test)`, `Foreground::new` charges one engine at most 256 MiB. A test engine
holds a few megabytes, so the nominal budget carried no information in tests. The production
accounting is unchanged, and the pure `overcommitted_at` arithmetic keeps its own tests.

### 2. `auto_recluster` `a_delta_builds_beside_a_base_rebuild_and_the_base_keeps_it` (`next.len()` was 1, not 2)

**Cause (a second indexer left running beside the engine under test).** The test makes its
read-only checks with `Engine::new(store)`, which is `EngineConfig::from_env()`: background
indexing on, every 250 ms. That engine (`fresh`) stays alive to the end of the test, and once it
has served a query it indexes namespace `t` too. At the end the test writes 200 rows and calls
`index_namespace("t", true)`. If `fresh`'s indexer publishes in between, the forced pass finds the
manifest moved (`old_sequence != state.manifest_seq`) and returns `Ok(false)` *without flushing the
rows*, which is its documented behavior with two indexers. The next `recluster` then reads a
manifest with no rows outside the tiers, `forced_kind` sees `uncovered_rows == 0` and picks a
base, and the namespace ends with one tier of 9,300 rows (the 200-row segment appears afterward,
from `fresh`). The window is a few milliseconds, which is why the failure took a busy gate to show
(three times in the ledger, each in a normal-length 12 to 14 second run).
Forcing the race (`BF_INDEX_INTERVAL_MS=2 BF_INDEX_AGE_MS=1 BF_INDEX_ROWS=10`, which `from_env`
reads) failed it 1 run in 8 and printed exactly that manifest: tiers `[(9300)]`, and a last segment
of 200 rows with an ANN file but no tier.

**Fix.** The read-only engines use a `reader` helper (`indexing: false`), so only the engine under
test indexes the namespace. The assertions are unchanged.

### 3. `segment::prefetch_tests::stalled_nonwinning_projection_is_canceled_after_winner_hydration`

**Cause (the hypothesis in the ideas ledger, confirmed).** The test needs a speculative document
read to *start*, then asserts the query answers without waiting for it. Speculation is best-effort:
the read runs in a task spawned into a join set that the query aborts as soon as the winners are
hydrated. If the task is first polled after the winners finish, the read is never issued and the
test waits for `started` until its 10 second timeout. Delaying the speculative task by 200 ms made
the test fail every time at that wait (30 ms did not; 1 second also failed before the fix). The
same race could also hit the fixture search that picks which block to stall, which reads the
backend's read log from a normal query.

**Fix.** The test backend gets a gate: while armed, ranged reads of the documents file before its
footer (the winners' column blocks and the speculative ones alike) wait until the gate opens. The
blocked read opens it when it starts, so the query cannot finish and abort speculation first; a
10 second limit makes a speculation that never starts fail the test, not hang it. The fixture
search holds the same gate for a 500 ms head start per candidate query, so the read log shows what
the planner speculates. The assertions that matter are unchanged and still meaningful: with the
stalled read in flight, the response must arrive (so an unneeded speculative read does not hold
it), and the read must be canceled afterward. With the delay injected at 0, 200 ms, and 1,000 ms,
the fixed test passes.

### 4. `a_bulk_load_flush_is_kept_in_the_manifest_so_a_restart_still_waits` (found along the way)

**Cause.** A flush counts as a bulk load only if its rows arrived at 2,000 a second or faster
(`age = now - tail_since`, clamped to at least one second). The test writes 4,950 rows in one call
and indexes, so the call and the index pass must finish within 2.5 seconds. In a debug build on a
busy machine they do not, and the flush is correctly treated as a steady stream: no `bulk_load_at`.
It failed in two of three first runs when three copies of the file started together.

**Fix.** The rate is now `EngineConfig::bulk_load_rows_per_sec` (default 2,000, unchanged), and
the test sets 1 row a second. The row-count condition (a quarter of `bulk_flush_rows`) still
separates the 50-row write, which must not be a load, from the 4,950-row one, which must.

### Method notes

- `stress-ng` is not installed here; busy loops pinned to cores (`taskset -c`, `nice`) plus the
  test binaries' own parallelism were enough. A full `bf-engine` lib run takes about 7 minutes
  alone and about 21 minutes under that load, and reproduced cause 1 on its first run.
- Look at the process-wide statics before blaming time: causes 1 and 2 are both state shared
  between tests in one process (a counter summed over every live engine, and a second indexer on
  one namespace).
