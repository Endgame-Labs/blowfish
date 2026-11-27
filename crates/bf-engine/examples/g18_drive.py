# /// script
# dependencies = ["numpy==2.5.4"]
# ///
"""Round G18 load driver: 16 client processes, one keep-alive connection each, IDs only.

Usage: uv run g18_drive.py DATA PORT bf|qdrant K SECONDS [EF] [OUT_JSON] [SERVER_PID]
DATA holds queries.npy and truth.npy. Request bodies are encoded before the clock starts. One quality
pass (all queries, serially) gives recall@K and a SHA-256 over every response's rows, for byte-identity
checks between binaries; then the timed pass runs for SECONDS and reports q/s, p50, and p99. Server CPU
per query is read by the caller from the server's PID (`ps -o time`), as in Round G17.
"""
import hashlib
import http.client
import json
import multiprocessing as mp
import resource
import subprocess
import sys
import time

import numpy as np

PROCESSES = 16


def cpu_seconds(pid):
    text = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    days, _, rest = text.rpartition("-")
    return float(days or 0) * 86400 + sum(float(p) * 60 ** i for i, p in enumerate(reversed(rest.split(":"))))


def children_cpu():
    r = resource.getrusage(resource.RUSAGE_CHILDREN)
    return r.ru_utime + r.ru_stime


def encode(engine, k, ef, q):
    if engine == "bf":
        body = {"rank_by": ["vector", "ANN", q.tolist()], "top_k": k, "include_attributes": False}
    else:
        body = {"vector": q.tolist(), "limit": k, "with_payload": False, "with_vector": False,
                "params": {"hnsw_ef": ef}}
    return json.dumps(body, separators=(",", ":")).encode()


def post(connection, route, body):
    connection.request("POST", route, body, {"content-type": "application/json"})
    response = connection.getresponse()
    data = response.read()
    if response.status != 200:
        raise RuntimeError((response.status, data[:300]))
    return data


def client(index, port, route, bodies, seconds, barrier, out):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    connection.connect()
    latencies = []
    barrier.wait()
    end = time.perf_counter() + seconds
    i = index
    while time.perf_counter() < end:
        t = time.perf_counter()
        post(connection, route, bodies[i % len(bodies)])
        latencies.append(time.perf_counter() - t)
        i += PROCESSES
    out.put(latencies)


def main():
    data, port, engine, k, seconds = sys.argv[1:6]
    ef = int(sys.argv[6]) if len(sys.argv) > 6 else 0
    out_path = sys.argv[7] if len(sys.argv) > 7 and sys.argv[7] else None
    server_pid = int(sys.argv[8]) if len(sys.argv) > 8 else None
    port, k, seconds = int(port), int(k), float(seconds)
    queries = np.load(f"{data}/queries.npy")
    truth = np.load(f"{data}/truth.npy")[:, :k]
    route = "/v2/namespaces/nq/query" if engine == "bf" else "/collections/nq/points/search"
    bodies = [encode(engine, k, ef, q) for q in queries]
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    digest, overlap = hashlib.sha256(), 0
    for i, body in enumerate(bodies):
        answer = json.loads(post(connection, route, body))
        rows = answer["rows"] if engine == "bf" else answer["result"]
        digest.update(json.dumps(rows, separators=(",", ":")).encode())
        overlap += len({int(row["id"]) for row in rows} & set(map(int, truth[i])))
    connection.close()
    barrier, out = mp.Barrier(PROCESSES + 1), mp.Queue()
    procs = [mp.Process(target=client, args=(i, port, route, bodies, seconds, barrier, out))
             for i in range(PROCESSES)]
    for p in procs:
        p.start()
    barrier.wait()
    server0 = cpu_seconds(server_pid) if server_pid else None
    start = time.perf_counter()
    lat = np.concatenate([np.array(out.get(timeout=seconds + 120)) for _ in procs])
    elapsed = time.perf_counter() - start
    server_cpu = cpu_seconds(server_pid) - server0 if server_pid else None
    for p in procs:
        p.join()
    client_cpu = children_cpu()  # the 16 client processes only (the quality pass ran in this process)
    result = {"driver": "python", "engine": engine, "k": k, "ef": ef, "queries": int(len(lat)), "seconds": elapsed,
              "qps": len(lat) / elapsed, "p50_ms": float(np.percentile(lat, 50) * 1e3),
              "p99_ms": float(np.percentile(lat, 99) * 1e3),
              "recall": overlap / (len(queries) * k), "rows_sha256": digest.hexdigest(),
              "client_cpu_cores": client_cpu / elapsed, "client_cpu_us_per_query": client_cpu / len(lat) * 1e6,
              "server_cpu_cores": server_cpu and server_cpu / elapsed,
              "server_cpu_us_per_query": server_cpu and server_cpu / len(lat) * 1e6}
    print(json.dumps(result), flush=True)
    if out_path:
        open(out_path, "w").write(json.dumps(result) + "\n")


if __name__ == "__main__":
    mp.set_start_method("fork")
    main()
