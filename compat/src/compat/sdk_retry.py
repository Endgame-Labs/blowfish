"""Backpressure and retry scenarios for the official SDKs.

They run only against a server started with a tiny unindexed-WAL limit and a slow indexer (set
`COMPAT_BACKPRESSURE_URL`; `scripts/compat_both.sh` starts one). The first write to a namespace
fills the limit, so the next write is answered 429 until the indexer publishes. Each scenario
records every request the SDK makes (time, status, headers) through an httpx hook, then checks
that the SDK retried, succeeded, and waited about what the server's headers said.
"""
import asyncio
import os
import time
import uuid

import httpx2 as httpx
import turbopuffer


class Recorder:
    """httpx hooks that log (monotonic time, status, headers) per response."""

    def __init__(self):
        self.events = []
        self.sent = []

    def on_request(self, request):
        self.sent.append(time.monotonic())

    def on_response(self, response):
        self.events.append((self.sent[-1], response.status_code, dict(response.headers)))

    async def a_on_request(self, request):
        self.on_request(request)

    async def a_on_response(self, response):
        self.on_response(response)

    def statuses(self):
        return [status for _, status, _ in self.events]


def _write(ns, i):
    return ns.write(upsert_rows=[{"id": i, "text": "backpressure"}])


def _check_waits(rec):
    """Each retry after a 429 must wait about the header's retry-after-ms (no jitter applies)."""
    saw = 0
    for (t0, status, headers), t1 in zip(rec.events, rec.sent[1:]):
        if status != 429:
            continue
        ms = int(headers["retry-after-ms"])
        assert 500 <= ms <= 30000, f"retry-after-ms out of range: {ms}"
        assert int(headers["retry-after"]) == max(1, -(-ms // 1000)), headers
        assert headers["x-should-retry"] == "true", headers
        waited = (t1 - t0) * 1000
        assert 0.85 * ms <= waited <= ms + 1500, f"waited {waited:.0f} ms, header said {ms} ms"
        saw += 1
    assert saw >= 1, "no 429 was observed"


def retries_sync(url, key, ns_name):
    rec = Recorder()
    http = httpx.Client(event_hooks={"request": [rec.on_request], "response": [rec.on_response]})
    ns = turbopuffer.Turbopuffer(base_url=url, api_key=key, max_retries=6, http_client=http).namespace(ns_name)
    _write(ns, 1)
    assert rec.statuses() == [200], rec.statuses()
    result = _write(ns, 2)
    assert result.rows_affected == 1
    statuses = rec.statuses()
    assert statuses[1] == 429 and statuses[-1] == 200 and set(statuses[1:-1]) <= {429}, statuses
    _check_waits(rec)
    assert len(ns.query(rank_by=["id", "asc"], top_k=10).rows) == 2


def retries_async(url, key, ns_name):
    async def go():
        rec = Recorder()
        http = httpx.AsyncClient(event_hooks={"request": [rec.a_on_request], "response": [rec.a_on_response]})
        client = turbopuffer.AsyncTurbopuffer(base_url=url, api_key=key, max_retries=6, http_client=http)
        ns = client.namespace(ns_name)
        await ns.write(upsert_rows=[{"id": 1, "text": "x"}])
        r = await ns.write(upsert_rows=[{"id": 2, "text": "x"}])
        assert r.rows_affected == 1
        assert 429 in rec.statuses() and rec.statuses()[-1] == 200, rec.statuses()
        _check_waits(rec)
        await ns.delete_all()
        await client.close()
    asyncio.run(go())


def no_retries_raises_rate_limit(url, key, ns_name):
    rec = Recorder()
    http = httpx.Client(event_hooks={"request": [rec.on_request], "response": [rec.on_response]})
    ns = turbopuffer.Turbopuffer(base_url=url, api_key=key, max_retries=0, http_client=http).namespace(ns_name)
    _write(ns, 1)
    try:
        _write(ns, 2)
    except turbopuffer.RateLimitError as exc:
        assert exc.status_code == 429 and exc.body["status"] == "error", exc.body
        assert "backpressure" in exc.body["error"], exc.body
    else:
        raise AssertionError("expected RateLimitError")
    assert rec.statuses() == [200, 429], rec.statuses()
    _, _, headers = rec.events[-1]
    assert "retry-after" in headers and "retry-after-ms" in headers and headers["x-should-retry"] == "true", headers


def reads_not_blocked(url, key, ns_name):
    """Backpressure only rejects writes: queries and metadata still answer 200 while it is on."""
    ns = turbopuffer.Turbopuffer(base_url=url, api_key=key, max_retries=0).namespace(ns_name)
    _write(ns, 1)
    assert len(ns.query(rank_by=["id", "asc"], top_k=10).rows) == 1
    assert ns.metadata().approx_row_count == 1


SCENARIOS = [
    ("python_retries_on_429", retries_sync),
    ("python_async_retries_on_429", retries_async),
    ("python_no_retry_raises_rate_limit", no_retries_raises_rate_limit),
    ("reads_not_blocked_by_backpressure", reads_not_blocked),
]


def run(url, key=None):
    """Run every scenario against the backpressure server; returns (passed, failed)."""
    key = key or "dummy"
    admin = turbopuffer.Turbopuffer(base_url=url, api_key=key, max_retries=0)
    passed = failed = 0
    for name, fn in SCENARIOS:
        ns_name = "blowfish-oracle-compat-sdk-retry-" + name.replace("_", "-") + "-" + uuid.uuid4().hex[:8]
        try:
            fn(url, key, ns_name)
            print(f"PASS sdk_retry_{name}", flush=True)
            passed += 1
        except Exception as exc:
            print(f"FAIL sdk_retry_{name}: {type(exc).__name__}: {exc}", flush=True)
            failed += 1
        finally:
            try:
                admin.namespace(ns_name).delete_all()
            except turbopuffer.NotFoundError:
                pass
    print(f"sdk-retry: {passed}/{passed + failed} passed")
    return passed, failed
