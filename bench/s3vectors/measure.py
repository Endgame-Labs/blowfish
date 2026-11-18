# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["numpy==2.5.4", "orjson==3.10.18", "requests", "boto3"]
# ///
"""Query measurements against the loaded S3 Vectors index: recall, warm latency, and idle-gap cold latency.

  recall   200 oracle queries, sequential, top-10 vs ann_ids
  warm     N queries at concurrency 1 (cycling the 200 oracle vectors)
  cold     K rounds: sleep --idle seconds with no traffic to the index, then one query (a distinct
           oracle vector per round), then --after immediate follow-ups with the same vector
Each query is one QueryVectors call timed on the client (the instance is in the same region).
"""
import argparse, json, sys, time
import numpy as np
import s3v


def pct(xs, p):
    return float(np.percentile(xs, p))


def summary(xs):
    xs = [x * 1000 for x in xs]
    return dict(n=len(xs), p50=round(pct(xs, 50), 1), p90=round(pct(xs, 90), 1), p99=round(pct(xs, 99), 1),
                mean=round(float(np.mean(xs)), 1), min=round(min(xs), 1), max=round(max(xs), 1))


def q(c, vec):
    for attempt in range(6):
        st, out, dt = c.query(vec)
        if st == 200:
            return [int(v["key"]) for v in out["vectors"]], dt
        print(json.dumps({"event": "query_error", "status": st, "body": str(out)[:200]}), flush=True)
        time.sleep(1 + attempt)
    raise RuntimeError("query failing")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=("recall", "warm", "cold"))
    p.add_argument("--oracle", default="oracle-10m.npz")
    p.add_argument("--n", type=int, default=1000)
    p.add_argument("--rounds", type=int, default=20)
    p.add_argument("--idle", type=float, default=1800)
    p.add_argument("--after", type=int, default=2)
    p.add_argument("--offset", type=int, default=0, help="first oracle query index used by cold rounds")
    a = p.parse_args()
    d = np.load(a.oracle)
    vecs, truth = d["vector"].astype(np.float32), d["ann_ids"]
    c = s3v.Client()
    if a.mode == "recall":
        rec, lat = [], []
        for i in range(len(vecs)):
            ids, dt = q(c, vecs[i])
            rec.append(len(set(ids) & set(truth[i].tolist())) / 10); lat.append(dt)
        print(json.dumps(dict(mode="recall", recall_at_10=round(float(np.mean(rec)), 4),
                              min_recall=min(rec), latency_ms=summary(lat))))
    elif a.mode == "warm":
        lat = []
        for i in range(a.n):
            lat.append(q(c, vecs[i % len(vecs)])[1])
        print(json.dumps(dict(mode="warm", latency_ms=summary(lat))))
    else:
        first, follow = [], []
        for r in range(a.rounds):
            time.sleep(a.idle)
            i = (a.offset + r) % len(vecs)
            ids, dt = q(c, vecs[i]); first.append(dt)
            rec = len(set(ids) & set(truth[i].tolist())) / 10
            f = [q(c, vecs[i])[1] for _ in range(a.after)]; follow.extend(f)
            print(json.dumps(dict(mode="cold_round", round=r, idle_s=a.idle, first_ms=round(dt * 1000, 1),
                                  follow_ms=[round(x * 1000, 1) for x in f], recall=rec)), flush=True)
        print(json.dumps(dict(mode="cold_summary", idle_s=a.idle, first_ms=summary(first),
                              follow_ms=summary(follow))), flush=True)


if __name__ == "__main__":
    main()
