# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1"]
# ///
"""Bounded async HTTP query runner for generated scale workloads.

Usage: uv run scale_api.py WORKLOAD.json BASE_URL NAMESPACE CONCURRENCY [warmup]
Outputs one JSON measurement per query, and an aggregate QPS record on stderr.
BF_SCALE_TIMEOUT_SECONDS overrides the default 1800-second request timeout.
"""
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx


async def main():
    inputs = json.loads(Path(sys.argv[1]).read_text())
    base, namespace, concurrency = sys.argv[2], sys.argv[3], int(sys.argv[4])
    semaphore = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    timeout = float(os.environ.get("BF_SCALE_TIMEOUT_SECONDS", "1800"))
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        url = f"{base}/v2/namespaces/{namespace}/query"
        if len(sys.argv) > 5 and sys.argv[5] == "warmup":
            for item in inputs:
                response = await client.post(url, json=item["request"])
                response.raise_for_status()

        async def one(item):
            async with semaphore:
                start = time.monotonic()
                response = await client.post(url, json=item["request"])
                response.raise_for_status()
                print(json.dumps({"query_id": item["query_id"], "kind": item["kind"],
                                  "ms": (time.monotonic() - start) * 1000,
                                  "response": response.json()}), flush=True)

        start = time.monotonic()
        await asyncio.gather(*(one(item) for item in inputs))
        elapsed = time.monotonic() - start
        print(json.dumps({"queries": len(inputs), "seconds": elapsed,
                          "qps": len(inputs) / elapsed, "concurrency": concurrency}), file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
