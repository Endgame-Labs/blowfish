# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1"]
# ///
"""Round BW29: load a local MS MARCO corpus.jsonl (id, docid, text) with a `cat` column (id % 8, filterable).

Usage: bw29_load.py CORPUS_JSONL BASE_URL NAMESPACE [--rows 1000000]
"""
import argparse, json, time
from concurrent.futures import ThreadPoolExecutor
import httpx


def send(client, url, body):
    delay = 0.25
    while True:
        try:
            r = client.post(url, json=body)
        except httpx.TransportError:
            time.sleep(delay); delay = min(10, delay * 1.5); continue
        if r.status_code == 429:
            time.sleep(delay); delay = min(10, delay * 1.5); continue
        r.raise_for_status()
        return


def main():
    p = argparse.ArgumentParser()
    p.add_argument("corpus"); p.add_argument("base_url"); p.add_argument("namespace")
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--inflight", type=int, default=4)
    a = p.parse_args()
    url = f"{a.base_url}/v2/namespaces/{a.namespace}"
    schema = {"text": {"type": "string", "full_text_search": True},
              "cat": {"type": "int", "filterable": True}}
    pool = ThreadPoolExecutor(a.inflight)
    pending, batch, rows, t0 = [], [], 0, time.monotonic()
    with httpx.Client(timeout=300) as cl, open(a.corpus) as f:
        def flush(first):
            nonlocal batch
            body = {"upsert_rows": batch}
            if first:
                body["schema"] = schema
            batch = []
            while len(pending) >= a.inflight:
                pending.pop(0).result()
            pending.append(pool.submit(send, cl, url, body))
            if first:
                pending.pop(0).result()
        for line in f:
            d = json.loads(line)
            i = int(d["docid"])
            batch.append({"id": i, "text": d["text"], "cat": i % 8})
            rows += 1
            if len(batch) == a.batch:
                flush(rows == a.batch)
                if rows % 100_000 == 0:
                    print(rows, round(time.monotonic() - t0), flush=True)
            if rows >= a.rows:
                break
        if batch:
            flush(False)
        for x in pending:
            x.result()
    print("done", rows, round(time.monotonic() - t0), flush=True)


if __name__ == "__main__":
    main()
