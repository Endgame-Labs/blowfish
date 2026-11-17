# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx==0.28.1"]
# ///
"""Stream the first N MS MARCO passages into a running scale server (text only, one BM25 attribute).

Usage: uv run msmarco_load.py --base-url http://127.0.0.1:8300 --namespace NS --rows 1000000 [--hub-endpoint URL]
The corpus (Tevatron/msmarco-passage-corpus, corpus.jsonl.gz) is streamed and gunzipped on the fly; nothing is
kept on disk, and the download stops after N rows. Document id is the passage's docid. Writes retry on 429 and
transport errors; the schema rides on the first write. Warning: a writable server indexes, compacts, and collects garbage in EVERY
namespace under its storage prefix, so point it at a prefix that holds only the namespace being loaded. No credentials are read (the Hub endpoint may be a proxy).
"""
import argparse, json, time, zlib
from concurrent.futures import ThreadPoolExecutor

import httpx

PATH = "/datasets/Tevatron/msmarco-passage-corpus/resolve/main/corpus.jsonl.gz"


def lines(client, endpoint):
    dec = zlib.decompressobj(16 + zlib.MAX_WBITS)
    tail = b""
    with client.stream("GET", endpoint + PATH, follow_redirects=True) as r:
        r.raise_for_status()
        for chunk in r.iter_bytes(1 << 20):
            parts = (tail + dec.decompress(chunk)).split(b"\n")
            tail = parts.pop()
            yield from parts


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
    p.add_argument("--base-url", default="http://127.0.0.1:8300")
    p.add_argument("--hub-endpoint", default="https://huggingface.co")
    p.add_argument("--namespace", required=True)
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--inflight", type=int, default=4)
    a = p.parse_args()
    url = f"{a.base_url}/v2/namespaces/{a.namespace}"
    pool = ThreadPoolExecutor(a.inflight)
    pending, rows, nbytes, t0, batch = [], 0, 0, time.monotonic(), []
    with httpx.Client(timeout=300) as dl, httpx.Client(timeout=300) as cl:
        def flush(first):
            nonlocal batch
            body = {"upsert_rows": batch}
            if first:
                body["schema"] = {"text": {"type": "string", "full_text_search": True}}
            batch = []
            while len(pending) >= a.inflight:
                pending.pop(0).result()
            pending.append(pool.submit(send, cl, url, body))
            if first:
                pending.pop(0).result()
        for line in lines(dl, a.hub_endpoint):
            d = json.loads(line)
            batch.append({"id": int(d["docid"]), "text": d["text"]})
            nbytes += len(d["text"].encode())
            rows += 1
            if len(batch) == a.batch:
                flush(rows == a.batch)
                if rows % 50_000 == 0:
                    print(json.dumps({"rows": rows, "text_bytes": nbytes, "seconds": round(time.monotonic() - t0)}), flush=True)
            if rows >= a.rows:
                break
        if batch:
            flush(False)
        for f in pending:
            f.result()
    print(json.dumps({"done": True, "rows": rows, "text_bytes": nbytes, "avg_bytes": nbytes / rows,
                      "seconds": round(time.monotonic() - t0)}), flush=True)


if __name__ == "__main__":
    main()
