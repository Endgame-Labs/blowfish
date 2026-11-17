# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Sequential upsert latency for batches of about 512 KB of JSON (text plus a 768-dim vector per row).

Usage: uv run upsert_bench.py BASE_URL NAMESPACE [--writes 300] [--target-kb 512] [--delete]
Each write is acknowledged once durable in object storage; the client time is request start to response.
Rows are synthetic (pseudo-random words for text, random f32 vectors as JSON arrays); ids are unique per row.
The batch row count is chosen so one request body is about --target-kb. Prints one JSON line with
p50/p90/p99 over all writes plus the first-write and 429 counts; --delete drops the namespace afterwards.
"""
import argparse, json, random, time
import httpx
import numpy as np

DIM = 768


def make_rows(rng, nprng, start, n, words):
    vecs = nprng.standard_normal((n, DIM)).astype("f4")
    vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
    return [{"id": start + i, "text": " ".join(rng.choices(words, k=rng.randint(30, 70))),
             "category": rng.choice(["a", "b", "c", "d"]), "vector": [round(float(x), 5) for x in vecs[i]]}
            for i in range(n)]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("base_url"); p.add_argument("namespace")
    p.add_argument("--writes", type=int, default=300); p.add_argument("--target-kb", type=int, default=512)
    p.add_argument("--delete", action="store_true")
    a = p.parse_args()
    rng, nprng = random.Random(1), np.random.default_rng(1)
    words = ["".join(rng.choices("abcdefghijklmnopqrstuvwxyz", k=rng.randint(3, 9))) for _ in range(20000)]
    url = f"{a.base_url}/v2/namespaces/{a.namespace}"
    schema = {"text": {"type": "string", "full_text_search": True}, "category": "string",
              "vector": {"type": f"[{DIM}]f32", "ann": True}}
    probe = make_rows(rng, nprng, 0, 20, words)
    per_row = len(json.dumps({"upsert_rows": probe})) / 20
    n = int(a.target_kb * 1024 / per_row)
    print(json.dumps({"rows_per_batch": n, "approx_body_kb": round(n * per_row / 1024)}), flush=True)
    ms, retries, sizes = [], 0, []
    with httpx.Client(timeout=300) as c:
        for w in range(a.writes):
            body = {"upsert_rows": make_rows(rng, nprng, w * n, n, words)}
            if w == 0:
                body.update(distance_metric="cosine_distance", schema=schema)
            raw = json.dumps(body).encode()
            sizes.append(len(raw))
            while True:
                t = time.monotonic()
                r = c.post(url, content=raw, headers={"content-type": "application/json"})
                dt = (time.monotonic() - t) * 1000
                if r.status_code == 429:
                    retries += 1; time.sleep(0.5); continue
                r.raise_for_status(); break
            ms.append(dt)
        if a.delete:
            c.delete(url)
    q = np.percentile(ms, [50, 90, 99])
    print(json.dumps({"writes": len(ms), "p50": round(q[0]), "p90": round(q[1]), "p99": round(q[2]), "mean": round(float(np.mean(ms))),
                      "first_ms": round(ms[0]), "retries_429": retries, "avg_body_kb": round(float(np.mean(sizes)) / 1024),
                      "by_decile_p50": [round(float(np.median(ms[i * len(ms) // 10:(i + 1) * len(ms) // 10]))) for i in range(10)]}))


if __name__ == "__main__":
    main()
