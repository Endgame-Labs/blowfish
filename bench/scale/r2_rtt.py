# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4", "boto3==1.40.0"]
# ///
"""RTT from this VM to the S3 endpoint in bench config: TCP connect, and a 4 KB ranged GET (warm connection).

Usage: uv run r2_rtt.py CONFIG_JSON KEY [--n 200]   (KEY: any object of at least 4 KB under the storage prefix's bucket)
Reads config.local (storage_url, endpoint, profile). Prints p50/p90 of TCP connect, TLS handshake, and the 4 KB GET.
"""
import argparse, json, socket, ssl, time, urllib.parse
import boto3
import numpy as np

p = argparse.ArgumentParser(); p.add_argument("config"); p.add_argument("key"); p.add_argument("--n", type=int, default=200)
a = p.parse_args()
c = json.load(open(a.config))
bucket = urllib.parse.urlparse(c["storage_url"]).netloc
host = urllib.parse.urlparse(c["endpoint"]).hostname
tcp, tls = [], []
for _ in range(a.n):
    t = time.monotonic(); s = socket.create_connection((host, 443), timeout=10); tcp.append((time.monotonic() - t) * 1000)
    t = time.monotonic(); ssl.create_default_context().wrap_socket(s, server_hostname=host); tls.append((time.monotonic() - t) * 1000); s.close()
cl = boto3.Session(profile_name=c["profile"]).client("s3", endpoint_url=c["endpoint"], region_name="auto")
get = []
for i in range(a.n + 5):
    t = time.monotonic(); cl.get_object(Bucket=bucket, Key=a.key, Range="bytes=0-4095")["Body"].read()
    if i >= 5: get.append((time.monotonic() - t) * 1000)
f = lambda x: {"p50": round(float(np.percentile(x, 50)), 1), "p90": round(float(np.percentile(x, 90)), 1)}
print(json.dumps({"tcp_connect_ms": f(tcp), "tls_handshake_ms": f(tls), "get_4kb_ms": f(get)}))
