# turbopuffer compatibility suite 🐡

Run commands from the repository root. Each Python scenario uses a fresh namespace and deletes it in a `finally` block. Reserve the `blowfish-oracle-` prefix for this suite on the paid oracle account.

```sh
export COMPAT_ORACLE_BASE_URL=https://your-oracle-endpoint
uv run --directory compat compat record
uv run --directory compat compat check --base-url http://localhost:8000
uv run --directory compat compat check --base-url http://localhost:8000 -k filter
uv run --directory compat compat diff-live --base-url http://localhost:8000
uv run --directory compat compat cleanup
uv run --directory compat python -m unittest discover -s tests
compat/smoke-ts/run.sh http://localhost:8000
```

`--api-key` is optional for the target, and `COMPAT_API_KEY` sets the TypeScript smoke test's key. The oracle uses a dummy SDK key and expects an integration that injects authentication. `cleanup` deletes **all** namespaces beginning with `blowfish-oracle-`, including those from other runs. Recording and cleanup use a paid service.

Fixtures retain response structure and results while dropping performance measurements, billing values, timestamps, and trace IDs. Query and write responses must include object-valued `performance` and `billing` with correctly typed recognized fields. The oracle omits `performance` from successful copy responses, so copy checks require `billing` and validate `performance` if present. Error comparisons use the HTTP status and a coarse semantic category, rather than exact wording. `$dist` uses relative tolerance 0.00001, or 0.02 for BM25, and an absolute tolerance of 0.000001. Equal-score ties and the explicit attribute-order tie scenario compare by ID within tied groups.

The seeded ANN scenario writes 2,000 16-dimensional vectors, runs 20 queries, and requires at least 0.9 recall@10 for each query compared with the oracle's recorded top ten. It compares membership, not ranking. The suite also covers namespace pagination, copying, cache warming, recall, export pagination, explicit type round trips, and raw HTTP. Explain accepts either a 400 with a string `error` (the oracle before an index exists) or a 200 with a string `plan_text` (blowfish's exhaustive plan). A failed scenario does not prevent later scenarios from running.

The TypeScript smoke test installs its locked dependencies with npm and exercises write, ANN, BM25, and metadata using the official SDK. It uses Node from `~/.local/node/bin` when present, or Node on `PATH` otherwise. Its namespace is deleted in a `finally` block.

Metadata `approx_*` fields must be present, non-negative integers. Their values are normalized to zero: turbopuffer's index-derived estimates can lag a write, while blowfish reports exact current counts.

Async-copy, metadata PATCH, and legacy v1 coverage is in `operations_legacy.py`, with 15 additional oracle fixtures (111 scenarios total). The oracle served all tested legacy endpoints when recorded. Explicit async copies assert a 202 token and same-origin `Location`, poll through running to finished, and check both success and failure. Copy writes also exercise the SDK's transparent polling; ordinary writes remain synchronous. Metadata scenarios check read-only writes and schema rejection, clearing read-only, pinning defaults, persistence, and clearing pinning. Legacy scenarios cover column and row writes, null-vector deletes, object filters, copying, base64 query rows, export, and metadata headers. Rust integration tests additionally force export pagination, test independent nodes, test expiration, and check authorization.

Additional SDK checks:

```sh
uv run --directory compat python sdk_operations_smoke.py http://localhost:8000
uv run --with 'turbopuffer==0.1.37' python compat/legacy_smoke.py http://localhost:8000
compat/smoke-ts/run.sh http://localhost:8000
```

The current Python smoke covers explicit start/poll, synchronous and asyncio transparent polling, and propagation of operation errors. The TypeScript smoke covers explicit start/poll, transparent polling, and metadata updates. The old Python SDK check covers column writes, base64 query responses, float query responses, object filters, HEAD metadata, export, copy, and deletion. Version 0.1.37 leaves base64 query vectors as strings when the response omits `vector_encoding_format`, matching the recorded oracle behavior.
