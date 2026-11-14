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

The seeded ANN scenario writes 2,000 16-dimensional vectors, runs 20 queries, and requires at least 0.9 recall@10 for each query compared with the oracle's recorded top ten. It compares membership, not ranking. The suite also covers namespace pagination, copying, cache warming, recall, export pagination, explicit type round trips, and raw HTTP. Explain responses compare only HTTP status and whether `plan_text` is a string; the oracle may return an error before an index exists. A failed scenario does not prevent later scenarios from running.

The TypeScript smoke test installs its locked dependencies with npm and exercises write, ANN, BM25, and metadata using the official SDK. It uses Node from `~/.local/node/bin` when present, or Node on `PATH` otherwise. Its namespace is deleted in a `finally` block.
