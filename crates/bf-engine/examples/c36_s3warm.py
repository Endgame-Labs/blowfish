# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.0", "numpy==2.5.4"]
# ///
"""Round C36: does a GET of one byte range warm a disjoint adjacent range of the same S3 object?

Reads (never writes) large ANN objects of a fixture under $BF_S3_BUCKET/$C36_PREFIX. Every wave
runs about 35 concurrent 16 KB reads (the production rerank fan-out) on offsets never touched
before in this process, and the conditions rotate wave by wave so drift hits all of them:

  far      no prime: random offsets (the C34 "first read": about 72 ms p50)
  adj256k  a 256 KB GET of [p, p+256K) finished just before; the read is [p+256K, +16K)
  adj8m    an 8 MB GET (32 coalesced blocks) of [p, p+8M); the read is [p+8M, +16K)
  gap1m    a 256 KB GET of [p, p+256K); the read is 1 MB past its end
  same     a 256 KB GET of [p, p+256K); the read is inside it (positive control: about 28 ms)

Usage: c36_s3warm.py [--waves 20] [--fanout 35]. Prints one JSON line per condition and a verdict.
"""

import argparse
import json
import os
import random
import statistics as st
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import numpy as np
from botocore.config import Config

P = argparse.ArgumentParser()
P.add_argument("--waves", type=int, default=20, help="waves per condition")
P.add_argument("--fanout", type=int, default=35)
P.add_argument("--objects", type=int, default=20, help="largest ANN objects to draw from")
P.add_argument("--seed", type=int, default=36)
a = P.parse_args()
KB, MB = 1 << 10, 1 << 20
bucket = os.environ["BF_S3_BUCKET"]
prefix = os.environ.get("C36_PREFIX", "tmp/c36/10m/ns/wikipedia-10m/")
s3 = boto3.client("s3", region_name="us-west-2", config=Config(max_pool_connections=128, retries={"max_attempts": 1}))
objs = []
for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
    objs += [(o["Key"], o["Size"]) for o in pg.get("Contents", []) if o["Key"].endswith((".bfa", ".bfb"))]
objs.sort(key=lambda x: -x[1])
objs = objs[: a.objects]
print(len(objs), "objects, smallest", objs[-1][1] // MB, "MB", flush=True)
rng = random.Random(a.seed)
used = {k: [] for k, _ in objs}  # [lo, hi) windows already touched


def get(key, off, n):
    t = time.perf_counter()
    s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={off}-{off + n - 1}")["Body"].read()
    return (time.perf_counter() - t) * 1000


def fresh(span):
    """A random (key, offset) whose whole [offset, offset+span) window nothing has touched yet."""
    while True:
        key, size = rng.choice(objs)
        off = rng.randrange(0, size - span - 1)
        if all(off + span <= lo or off >= hi for lo, hi in used[key]):
            used[key].append((off, off + span))
            return key, off


with ThreadPoolExecutor(a.fanout) as ex:  # warm connections
    list(ex.map(lambda i: get(objs[0][0], 0, 100), range(a.fanout * 2)))
# condition -> (prime bytes, read offset from the prime's start, window span to reserve)
SPEC = {
    "far": (0, None, 16 * KB),
    "adj256k": (256 * KB, 256 * KB, 256 * KB + 16 * KB),
    "adj8m": (8 * MB, 8 * MB, 8 * MB + 16 * KB),
    "gap1m": (256 * KB, 256 * KB + MB, 256 * KB + MB + 16 * KB),
    "same": (256 * KB, 100 * KB, 256 * KB),
}
res = {c: [] for c in SPEC}
wave_medians = {c: [] for c in SPEC}
names = list(SPEC)
with ThreadPoolExecutor(a.fanout) as ex:
    for w in range(a.waves):
        order = names[w % len(names) :] + names[: w % len(names)]
        if (w // len(names)) % 2:
            order.reverse()
        for c in order:
            prime, read_at, span = SPEC[c]
            picks = [fresh(span) for _ in range(a.fanout)]
            if prime:
                list(ex.map(lambda p: get(p[0], p[1], prime), picks))
            lat = list(ex.map(lambda p: get(p[0], p[1] + (read_at if read_at is not None else 0), 16 * KB), picks))
            res[c] += lat
            wave_medians[c].append(st.median(lat))
        print("wave", w, flush=True)
out = {}
for c, lat in res.items():
    q = np.percentile(lat, [10, 50, 90])
    out[c] = {"n": len(lat), "p10": round(q[0]), "p50": round(q[1]), "p90": round(q[2]), "max": round(max(lat)),
              "wave_median_mean": round(float(np.mean(wave_medians[c])), 1)}
    print(json.dumps({c: out[c]}), flush=True)
rg = np.random.default_rng(36)
for c in ("adj256k", "adj8m", "gap1m", "same"):
    d = np.array(wave_medians["far"]) - np.array(wave_medians[c])  # positive: the primed read is faster
    boot = [np.median(rg.choice(d, len(d))) for _ in range(5000)]
    lo, hi = np.percentile(boot, [2.5, 97.5])
    drop = out["far"]["p50"] - out[c]["p50"]
    print(json.dumps({"vs_far": c, "p50_drop_ms": drop, "wave_median_drop_ms": round(float(np.median(d)), 1),
                      "ci95": [round(float(lo), 1), round(float(hi), 1)], "meets_49ms": bool(drop >= 49)}), flush=True)
