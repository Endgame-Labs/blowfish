# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.40.0"]
# ///
"""Sol 06:25 #4: 200 range GETs of 16 KiB inside one large object vs 200 whole-object GETs of separate 16 KiB objects.
usage: side.py BUCKET PREFIX  (fresh keys each run; deletes everything it wrote)"""
import boto3, os, random, statistics as st, sys, time
from botocore.config import Config
bucket, prefix = sys.argv[1], sys.argv[2].rstrip("/") + "/side-" + str(int(time.time()))
s3 = boto3.client("s3", region_name="us-west-2", config=Config(max_pool_connections=8, retries={"max_attempts": 1}))
N, SZ, BIG = 200, 16384, 256 * 2**20
big = prefix + "/big"
body = os.urandom(1 << 20) * (BIG >> 20)
s3.put_object(Bucket=bucket, Key=big, Body=body)
small = [f"{prefix}/small-{i:03d}" for i in range(N)]
for k in small: s3.put_object(Bucket=bucket, Key=k, Body=os.urandom(SZ))
time.sleep(60)
rng = random.Random(41)
offs = [rng.randrange(0, BIG // SZ) * SZ for _ in range(N)]
def get(key, rng_hdr=None):
    t = time.perf_counter()
    r = s3.get_object(Bucket=bucket, Key=key, **({"Range": rng_hdr} if rng_hdr else {}))
    fb = time.perf_counter() - t
    data = r["Body"].read(); tot = time.perf_counter() - t
    assert len(data) == SZ
    return fb * 1000, tot * 1000
for _ in range(5): get(small[0]); get(big, "bytes=0-16383")   # warm connections
res = {"range-in-large": [], "whole-small": []}
order = list(range(N)); rng.shuffle(order)
for i in order:  # interleaved, alternating which goes first
    a = (lambda: res["range-in-large"].append(get(big, f"bytes={offs[i]}-{offs[i]+SZ-1}")))
    b = (lambda: res["whole-small"].append(get(small[i])))
    (a, b) if i % 2 == 0 else (b, a)
    for f in ((a, b) if i % 2 == 0 else (b, a)): f()
def q(v, p): v = sorted(v); return v[min(len(v) - 1, int(p * len(v)))]
for name, rows in res.items():
    fb, tot = [r[0] for r in rows], [r[1] for r in rows]
    print(f"{name:15s} n={len(rows)} first-byte p50 {q(fb,.5):.1f} p90 {q(fb,.9):.1f} ms  total p50 {q(tot,.5):.1f} p90 {q(tot,.9):.1f} ms")
s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in small + [big]]})
