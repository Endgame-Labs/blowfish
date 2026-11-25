# /// script
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Paced ordinary NQ heat for graph_compare.py; a pause file stops query traffic.

Usage: uv run graph_heat.py PROFILE_DIRECTORY PORT QUERIES_PER_SECOND.
PROFILE_DIRECTORY/fixture/data contains queries.npy. The caller owns the process,
logs, directory, and cleanup. Status snapshots are written every 30 seconds.
"""

import asyncio, json, pathlib, sys, time
import numpy as np
import httpx

root = pathlib.Path(sys.argv[1])
port = int(sys.argv[2])
rate = int(sys.argv[3])
queries = np.load(root / "fixture/data/queries.npy")
bodies = [
    json.dumps(
        {
            "rank_by": ["vector", "ANN", q.tolist()],
            "top_k": 10,
            "include_attributes": False,
        },
        separators=(",", ":"),
    ).encode()
    for q in queries
]


async def main():
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}",
        timeout=300,
        limits=httpx.Limits(max_connections=20),
    ) as client:

        async def work(worker):
            i = worker
            deadline = time.monotonic()
            while True:
                while (root / "pause").exists():
                    await asyncio.sleep(0.1)
                    deadline = time.monotonic()
                response = await client.post(
                    "/v2/namespaces/nq/query",
                    content=bodies[i % len(bodies)],
                    headers={"content-type": "application/json"},
                )
                response.raise_for_status()
                i += 16
                deadline += 16 / rate
                await asyncio.sleep(max(0, deadline - time.monotonic()))

        async def status():
            while True:
                r = await client.get("/measure/nq/status")
                r.raise_for_status()
                value = {"time": time.time(), "status": r.json()}
                with (root / "status.jsonl").open("a") as f:
                    f.write(json.dumps(value) + "\n")
                print(json.dumps(value), flush=True)
                await asyncio.sleep(30)

        await asyncio.gather(*(work(i) for i in range(16)), status())


asyncio.run(main())
