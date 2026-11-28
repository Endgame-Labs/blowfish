# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.0"]
# ///
"""Round C46 physics: does a small priming GET warm a later 16 KiB range read near it, in AGED objects?

Reads only. Objects are the large ANN objects of a retained fixture ($BF_S3_BUCKET, $C46_PREFIX, default
tmp/c5/10m/). Every wave is 35 concurrent reads (the rerank fan-out) at offsets never touched before:
  far     a 16 KiB read, nothing primed (control)
  p<size>_d<delta>_t<delay>  35 primes of <size> bytes at offset p finish, wait <delay> ms, then 16 KiB reads at p+delta
Conditions rotate wave by wave. Output: one JSON line per read to OUT, and a table (summary).
usage: c46_physics.py run OUT [--waves 12]   |   c46_physics.py summary OUT
"""
import json, os, random, sys, time, statistics
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.config import Config

KiB, MiB = 1024, 1 << 20
FAN = 35
S3 = boto3.client("s3", region_name="us-west-2", config=Config(max_pool_connections=128, retries={"max_attempts": 1}))
SIZES = {"1B": 1, "4K": 4 * KiB, "16K": 16 * KiB}
DELTAS = {"-512K": -512 * KiB, "+512K": 512 * KiB, "+1M": MiB, "+2M": 2 * MiB}
DELAYS = [50, 150, 300]


def get(bucket, key, off, n):
    t = time.perf_counter()
    S3.get_object(Bucket=bucket, Key=key, Range=f"bytes={off}-{off + n - 1}")["Body"].read()
    return (time.perf_counter() - t) * 1000


def run(out, waves):
    bucket, prefix = os.environ["BF_S3_BUCKET"], os.environ.get("C46_PREFIX", "tmp/c5/10m/")
    objs = []
    for pg in S3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        objs += [(o["Key"], o["Size"]) for o in pg.get("Contents", []) if o["Size"] >= 64 * MiB]
    print(len(objs), "objects", flush=True)
    rng = random.Random(int(os.environ.get("C46_SEED", 46)))
    used = {}  # key -> offsets touched; no two points within 3 MiB, so no condition warms another's bytes

    def fresh():
        while True:
            k, sz = rng.choice(objs)
            p = rng.randrange(4 * MiB, sz - 8 * MiB) // 4096 * 4096
            if not any(abs(p - u) < 3 * MiB for u in used.get(k, ())):
                used.setdefault(k, []).append(p); return k, p

    conds = ["far"] + [f"p{s}_d{d}_t{t}" for s in SIZES for d in DELTAS for t in DELAYS]
    f = open(out, "a")
    with ThreadPoolExecutor(FAN) as ex:
        for w in range(waves):
            order = conds[w % len(conds):] + conds[:w % len(conds)]  # rotate the start; every condition once per wave
            for c in order:
                pts = [fresh() for _ in range(FAN)]
                if c == "far":
                    ms = list(ex.map(lambda kp: get(bucket, kp[0], kp[1], 16 * KiB), pts))
                else:
                    s, d, t = c[1:].split("_")
                    n, delta, delay = SIZES[s], DELTAS[d[1:]], int(t[1:])
                    list(ex.map(lambda kp: get(bucket, kp[0], kp[1], n), pts))
                    time.sleep(delay / 1000)
                    ms = list(ex.map(lambda kp: get(bucket, kp[0], kp[1] + delta, 16 * KiB), pts))
                for m in ms:
                    f.write(json.dumps({"c": c, "ms": round(m, 2)}) + "\n")
                f.flush()
                time.sleep(0.05)
            print("wave", w, "done", flush=True)


def q(v, p):
    v = sorted(v); return v[min(len(v) - 1, int(p * len(v)))]


def summary(out):
    rows = [json.loads(l) for l in open(out)]
    by = {}
    for r in rows:
        by.setdefault(r["c"], []).append(r["ms"])
    far = by["far"]
    print(f"far (nothing primed): n={len(far)} p50 {q(far,.5):.0f} p90 {q(far,.9):.0f}\n")
    print("| Prime | Follow-up at | Delay ms | n | p50 | p90 |\n| --- | --- | ---: | ---: | ---: | ---: |")
    for c, v in by.items():
        if c != "far":
            s, d, t = c[1:].split("_")
            print(f"| {s} | {d[1:]} | {t[1:]} | {len(v)} | {q(v,.5):.0f} | {q(v,.9):.0f} |")


if __name__ == "__main__":
    if sys.argv[1] == "run":
        run(sys.argv[2], int(sys.argv[sys.argv.index("--waves") + 1]) if "--waves" in sys.argv else 12)
    else:
        summary(sys.argv[2])
