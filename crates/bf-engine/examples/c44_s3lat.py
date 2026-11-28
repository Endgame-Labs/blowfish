# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.0"]
# ///
"""Round C44: first-byte latency of S3 reads for each rerank-page layout.

usage:
  c44_s3lat.py write  BUCKET PREFIX SET           write one set of test objects under PREFIX/SET/
  c44_s3lat.py read   BUCKET PREFIX SET LABEL OUT read one set (fresh offsets, never read before), append JSON lines to OUT
  c44_s3lat.py aged   BUCKET LABEL OUT            read the retained fixtures' old objects (GETs only)
  c44_s3lat.py clean  BUCKET PREFIX               delete everything under PREFIX/ (only keys this script wrote)
  c44_s3lat.py summary OUT                        print the table for every label in OUT

A set holds, written together: 96 x 64 MiB objects ("big", ranges of 16 KiB), 50,000 x 16 KiB, 12,500 x 64 KiB,
and 3,200 x 256 KiB whole objects (the three small layouts span the same 800 MB), so a set read once is "cold"
at one age. Reads run in waves of 35 at once (the rerank fan-out), the layouts rotated wave by wave, with
15 waves (525 reads) per layout. Every read uses an offset or object nothing in the set has read before.
"near" reads a first range, then (after the first wave finishes) a second range DELTA bytes away in the same object.
Bucket names come from the arguments; nothing here is hardcoded.
"""
import json, os, random, zlib, statistics as st, sys, time
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.config import Config

KiB, MiB = 1024, 1 << 20
S3 = boto3.client("s3", region_name="us-west-2", config=Config(max_pool_connections=160, retries={"max_attempts": 1}))
WAVE, WAVES = 35, 15
N_BIG, BIG = 96, 64 * MiB  # far reads use objects 0-63, near reads 64-95
SMALL = {"s16k": (16 * KiB, 50_000), "s64k": (64 * KiB, 12_500), "s256k": (256 * KiB, 3_200)}


def put(bucket, key, body):
    S3.put_object(Bucket=bucket, Key=key, Body=body)


def cmd_write(bucket, prefix, s):
    base = f"{prefix.rstrip('/')}/{s}"
    t = time.time()
    with ThreadPoolExecutor(96) as ex:
        jobs = [ex.submit(put, bucket, f"{base}/big/{i:03d}", os.urandom(BIG)) for i in range(N_BIG)]
        for name, (sz, n) in SMALL.items():
            blob = os.urandom(sz * 64)  # slices of one random block: distinct bytes per object, cheap to make
            for i in range(n):
                off = (i % 64) * sz
                jobs.append(ex.submit(put, bucket, f"{base}/{name}/{i:06d}", blob[off:off + sz] if i % 64 else os.urandom(sz)))
        for j in jobs:
            j.result()
    print(json.dumps({"written": s, "seconds": round(time.time() - t), "end_epoch": time.time()}))


def get(bucket, key, lo, n):
    t = time.perf_counter()
    kw = {"Range": f"bytes={lo}-{lo + n - 1}"} if lo is not None else {}
    r = S3.get_object(Bucket=bucket, Key=key, **kw)
    fb = (time.perf_counter() - t) * 1000
    data = r["Body"].read()
    tot = (time.perf_counter() - t) * 1000
    assert len(data) == n, (key, len(data), n)
    return fb, tot


def run_wave(ex, bucket, reqs):
    futs = [ex.submit(get, bucket, k, lo, n) for k, lo, n in reqs]
    return [f.result() for f in futs]


def record(out, label, layout, rows, extra=None):
    with open(out, "a") as f:
        for fb, tot in rows:
            f.write(json.dumps({"label": label, "layout": layout, "fb": round(fb, 2), "tot": round(tot, 2), **(extra or {})}) + "\n")


def cmd_read(bucket, prefix, s, label, out):
    base = f"{prefix.rstrip('/')}/{s}"
    rng = random.Random(zlib.crc32(f"{s}{label}".encode()))
    used = set()
    pools = {n: rng.sample(range(c), c) for n, (_, c) in SMALL.items()}
    cursor = {n: 0 for n in SMALL}

    def far_big():
        while True:
            i, off = rng.randrange(0, 64), rng.randrange(0, BIG // (16 * KiB)) * 16 * KiB
            if (i, off) not in used:
                used.add((i, off)); return f"{base}/big/{i:03d}", off, 16 * KiB

    def small(name):
        sz = SMALL[name][0]
        i = pools[name][cursor[name]]; cursor[name] += 1
        return f"{base}/{name}/{i:06d}", None, sz

    def near_pair(delta):
        key, off = None, None
        while True:
            i, off = rng.randrange(64, 96), rng.randrange(0, (BIG - 8 * MiB) // (16 * KiB)) * 16 * KiB
            if (i, off) not in used:
                used.add((i, off)); return f"{base}/big/{i:03d}", off

    layouts = ["big16k", "s16k", "s64k", "s256k"]
    with ThreadPoolExecutor(WAVE) as ex:
        for w in range(WAVES):
            order = layouts[w % 4:] + layouts[:w % 4]
            for name in order:
                reqs = [far_big() for _ in range(WAVE)] if name == "big16k" else [small(name) for _ in range(WAVE)]
                record(out, label, name, run_wave(ex, bucket, reqs))
                time.sleep(0.1)
            # near reads: first range, then a second range delta away in the same object (after the first wave ended)
            for delta, tag in ((64 * KiB, "near+64KiB"), (1 * MiB, "near+1MiB")):
                firsts = [near_pair(delta) for _ in range(WAVE)]
                record(out, label, "near-first", run_wave(ex, bucket, [(k, o, 16 * KiB) for k, o in firsts]), {"delta": tag})
                record(out, label, "near-second", run_wave(ex, bucket, [(k, o + delta, 16 * KiB) for k, o in firsts]), {"delta": tag})
                time.sleep(0.1)
            # 256 KiB object, a first 16 KiB range then a second 16 KiB range 128 KiB further in the same object
            ks = [small("s256k") for _ in range(WAVE)]
            record(out, label, "s256k-range-first", run_wave(ex, bucket, [(k, 0, 16 * KiB) for k, _, _ in ks]))
            record(out, label, "s256k-range-second", run_wave(ex, bucket, [(k, 128 * KiB, 16 * KiB) for k, _, _ in ks]))
    print(json.dumps({"read": s, "label": label, "end_epoch": time.time()}))


def cmd_aged(bucket, label, out):
    """GETs only, on the retained fixtures: 16 KiB ranges in the largest objects, whole small objects by size bin."""
    rng = random.Random(44)
    objs = []
    for pre in ("tmp/nq-fixture-v019/", "tmp/c5/10m/", "tmp/msmarco-1m-v021/"):
        for pg in S3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre):
            objs += [(o["Key"], o["Size"], o["LastModified"].timestamp()) for o in pg.get("Contents", [])]
    now = time.time()
    big = [o for o in objs if o[0].startswith("tmp/c5/10m/") and o[1] >= 64 * MiB]
    bins = {"aged-whole-8-32K": (8 * KiB, 32 * KiB), "aged-whole-32-128K": (32 * KiB, 128 * KiB), "aged-whole-128K-1M": (128 * KiB, MiB)}
    plan = {"aged-big16k": [(k, rng.randrange(0, (sz - 16 * KiB) // 4096) * 4096, 16 * KiB) for k, sz, _ in rng.choices(big, k=WAVE * WAVES)]}
    for name, (lo, hi) in bins.items():
        sel = [o for o in objs if lo <= o[1] < hi]
        rng.shuffle(sel)
        plan[name] = [(k, None, sz) for k, sz, _ in sel[:WAVE * WAVES]]
    ages = {n: round(min((now - lm) / 86400 for k, sz, lm in objs if lo <= sz < hi), 1) for n, (lo, hi) in bins.items()}
    with ThreadPoolExecutor(WAVE) as ex:
        for name, reqs in plan.items():
            for w in range(0, len(reqs) - len(reqs) % WAVE or len(reqs), WAVE):
                chunk = reqs[w:w + WAVE]
                record(out, label, name, run_wave(ex, bucket, chunk))
                time.sleep(0.1)
    print(json.dumps({"aged": {n: len(r) for n, r in plan.items()}, "min_age_days": ages}))


def cmd_clean(bucket, prefix):
    n = 0
    for pg in S3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        keys = [{"Key": o["Key"]} for o in pg.get("Contents", [])]
        if keys:
            S3.delete_objects(Bucket=bucket, Delete={"Objects": keys}); n += len(keys)
    print(json.dumps({"deleted": n}))


def q(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(p * len(v)))]


def cmd_summary(out):
    rows = [json.loads(l) for l in open(out)]
    keys = sorted({(r["label"], r["layout"], r.get("delta", "")) for r in rows})
    print("| Label | Layout | n | First byte p50 / p90 / p99, ms | Total p50 / p90 / p99, ms |\n| --- | --- | ---: | --- | --- |")
    for label, layout, delta in keys:
        sel = [r for r in rows if (r["label"], r["layout"], r.get("delta", "")) == (label, layout, delta)]
        fb, tot = [r["fb"] for r in sel], [r["tot"] for r in sel]
        print(f"| {label} | {layout} {delta} | {len(sel)} | {q(fb,.5):.0f} / {q(fb,.9):.0f} / {q(fb,.99):.0f} | {q(tot,.5):.0f} / {q(tot,.9):.0f} / {q(tot,.99):.0f} |")


if __name__ == "__main__":
    c, *a = sys.argv[1:]
    {"write": cmd_write, "read": cmd_read, "aged": cmd_aged, "clean": cmd_clean, "summary": cmd_summary}[c](*a)
