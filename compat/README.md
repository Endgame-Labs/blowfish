# turbopuffer compatibility suite 🐡

Run commands from the repository root. The suite uses the official Python SDK, and each scenario gets a fresh namespace, which is deleted in a `finally` block. Use only an oracle account where the `blowfish-oracle-` prefix is reserved for these tests.

```sh
export COMPAT_ORACLE_BASE_URL=https://your-oracle-endpoint
uv run --directory compat compat record
uv run --directory compat compat check --base-url http://localhost:8000
uv run --directory compat compat check --base-url http://localhost:8000 -k filter
uv run --directory compat compat diff-live --base-url http://localhost:8000
uv run --directory compat compat cleanup
uv run --directory compat python -m unittest discover -s tests
```

`--api-key` is optional for the target. The oracle uses a dummy SDK key and expects an integration that injects authentication. `cleanup` deletes **all** namespaces beginning with `blowfish-oracle-`, including those from other runs. Record and cleanup use a paid service; keep data tiny.

Fixtures retain response structure and results while dropping `performance`, `billing`, timestamps, and trace IDs. Query and write responses must contain object-valued `performance` and `billing`. Errors retain the exact message for inspection, but comparisons use status and a coarse message category. `$dist` uses relative tolerance 0.00001, or 0.02 for BM25, and an absolute tolerance of 0.000001. Equal-score row ties are compared by ID; other lists compare in order, so scenarios use deterministic ordering where possible. Small vector datasets exercise exact-sized ANN results; this suite does not yet implement large-dataset ANN recall comparison or ties in attribute ordering. A failed scenario does not prevent later scenarios from running.
