# Exhaustive reference engine

`Engine::new(Storage)` creates an independent node. `write` validates a request,
submits it to the namespace's group-commit queue, and acknowledges only after its
immutable WAL slot is durable. On a competing CAS, the node replays the intervening
entries, revalidates every queued request, and retries. Atomic write batches contain
resolved document mutations, schema, metric, and timestamps; replay does not rerun
conditions.

`query` takes one snapshot for a single query or every subquery in a multi-query.
Strong reads catch up through the first absent WAL slot. Eventual reads reuse the
cached snapshot until its configured staleness expires. Every query scans documents;
there are no persistent text or vector indexes yet. `text_fallback` owns the temporary
word tokenizer and corpus-wide BM25 calculation, independently of the query planner.

The remaining public methods implement metadata, schema updates, namespace listing,
namespace deletion, cache warming, exact recall evaluation, and query explanation.
`storage()` provides the configured store for operational inspection.

Namespace deletion commits an immutable tombstone, clears cached state, and removes
non-WAL objects. WAL objects remain as fences: removing an old slot would allow a
stale writer to recreate it. Physical WAL reclamation belongs with the generation
and retention protocol in the GC milestone. Recreating the namespace continues its
sequence after the tombstone and can use a different schema or ID type.

This is deliberately an in-memory reference implementation. Copying a namespace
copies its materialized documents into a new WAL batch, and each commit currently
publishes a schema-only manifest. A cold node replays the WAL from the beginning;
manifest checkpoints and shared immutable segments come with the indexer.

The fallback tokenizer approximates the upstream Unicode word tokenizer. Its default
BM25 scores match the recorded three-document oracle. CJK dictionary tokenizers are
rejected. Complete language-specific stopword lists and version-specific word
tokenizer behavior need bf-text.
