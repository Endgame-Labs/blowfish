"""Compare request shapes or fresh-process cold queries against two frozen Linux binaries.

Run with uv --project bench/nq. All outputs must be under .scratch. Cold mode uses
bench/scale/local_s3.py with 25 ms delay, rather than claiming to reproduce S3 tails.
"""

import argparse
import contextlib
import http.client
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import time

import numpy as np

ROOT = Path(__file__).resolve().parents[3]


def request(namespace, body):
    client = http.client.HTTPConnection("127.0.0.1", 18100, timeout=600)
    try:
        start = time.perf_counter()
        client.request(
            "POST",
            f"/v2/namespaces/{namespace}/query",
            json.dumps(body).encode(),
            {"Content-Type": "application/json"},
        )
        response = client.getresponse()
        raw = response.read()
        elapsed = (time.perf_counter() - start) * 1000
        result = json.loads(raw)
        result.pop("performance", None)
        return response.status, result, elapsed
    finally:
        client.close()


@contextlib.contextmanager
def server(binary, args, label, probes=48, cold=False):
    log_path = args.out_dir / f"{label}.server.log"
    env = dict(
        os.environ,
        TOKIO_WORKER_THREADS="8",
        RAYON_NUM_THREADS="8",
        BF_DECODED_CACHE_BYTES=str(4 << 30),
        TMPDIR=str(args.out_dir),
    )
    env.pop("BF_QUERY_CONCURRENCY", None)
    command = [
        "taskset",
        "-c",
        "0-7",
        str(binary),
        "serve",
        "--listen",
        "127.0.0.1:18100",
        "--read-only",
        "--indexing",
        "false",
        "--recall-sample-rate",
        "0",
        "--ann-nprobe",
        str(probes),
        "--cache-memory-bytes",
        str(4 << 30),
        "--cache-disk-bytes",
        "0",
    ]
    if cold:
        command += [
            "--storage-url",
            "s3://local",
            "--storage-endpoint",
            "http://127.0.0.1:18324",
            "--storage-region",
            "us-east-1",
        ]
        # Dummy credentials for the read-only loopback facade; no vendor account is used.
        env.update(
            AWS_ACCESS_KEY_ID="local",
            AWS_SECRET_ACCESS_KEY="local",
            AWS_EC2_METADATA_DISABLED="true",
            BF_TRACE_READS="1",
        )
    else:
        command += ["--storage-url", args.store.as_uri()]
    with log_path.open("ab", buffering=0) as log:
        child = subprocess.Popen(command, env=env, stdout=log, stderr=log)
        try:
            for _ in range(300):
                if child.poll() is not None:
                    raise RuntimeError(f"server exited; inspect {log_path}")
                client = http.client.HTTPConnection("127.0.0.1", 18100, timeout=1)
                try:
                    client.request("GET", "/")
                    if client.getresponse().status == 200:
                        break
                except OSError:
                    pass
                finally:
                    client.close()
                time.sleep(0.1)
            else:
                raise TimeoutError("server readiness")
            log.write(b"Q2_QUERY_START\n")
            yield log, log_path
        finally:
            child.terminate()
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


def shapes(args, vectors):
    rng = random.Random(27)
    bodies = []
    for i in range(96):
        vector = vectors[rng.randrange(len(vectors))].tolist()
        body = {
            "rank_by": ["vector", "ANN", vector],
            "top_k": 10,
            "include_attributes": ["id"],
            "offset": rng.randrange(4),
        }
        case = i % 12
        if case == 1:
            body["include_attributes"] = ["category", "label"]
        elif case == 2:
            body.update(top_k=100, include_attributes=["category", "label"])
        elif case == 3:
            body.pop("include_attributes")
            body["exclude_attributes"] = ["vector"]
        elif case == 4:
            body["filters"] = ["category", "Eq", rng.randrange(8)]
        elif case == 5:
            body["consistency"] = {"level": "eventual", "max_staleness_seconds": 3600}
        elif case == 6:
            body = {"aggregate_by": {"count": ["Count"]}, "group_by": ["category"]}
        elif case == 7:
            body = {
                "queries": [
                    {
                        "rank_by": ["vector", "ANN", vector],
                        "top_k": 10,
                        "include_attributes": ["id"],
                    },
                    {
                        "rank_by": [
                            "vector",
                            "ANN",
                            vectors[(i + 1) % len(vectors)].tolist(),
                        ],
                        "top_k": 10,
                        "include_attributes": ["category", "label"],
                    },
                ],
                "rerank_by": ["RRF"],
                "limit": 10,
            }
        elif case == 8:
            body.update(
                vector_encoding="base64", include_attributes=["vector", "label"]
            )
        elif case == 9:
            body["rank_by"] = ["category", "asc"]
        elif case == 10:
            body["top_k"] = 0
        elif case == 11:
            body["include_attributes"] = ["unknown_attribute"]
        bodies.append(body)
    got = {}
    for label, binary in [("base", args.baseline), ("candidate", args.candidate)]:
        with server(binary, args, "shapes-" + label):
            for _ in range(3):
                results = [request("small", body)[:2] for body in bodies]
            got[label] = results
    assert got["base"] == got["candidate"], "request-shape response mismatch"
    summary = {
        "cases": len(bodies),
        "successes": sum(s == 200 for s, _ in got["base"]),
        "expected_errors": sum(s != 200 for s, _ in got["base"]),
        "identical": True,
    }
    (args.out_dir / "shapes.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary), flush=True)


def trace(path):
    text = path.read_text().split("Q2_QUERY_START\n", 1)[1]
    reads = []
    for line in text.splitlines():
        if line.startswith("TRACE_READ "):
            _, _, _, _, size, end, elapsed = line.split()
            reads.append((int(end) - int(elapsed), int(end), int(size)))
    waves, previous = 0, -1
    for start, end, _ in sorted(reads):
        if start >= previous:
            waves += 1
        previous = max(previous, end)
    return {"reads": len(reads), "bytes": sum(r[2] for r in reads), "waves": waves}


def cold(args, vectors):
    rows = []
    log = (args.out_dir / "facade.log").open("wb")
    facade = subprocess.Popen(
        [
            "taskset",
            "-c",
            "8-15",
            sys.executable,
            str(ROOT / "bench/scale/local_s3.py"),
            str(args.store),
            "--port",
            "18324",
            "--delay-ms",
            "25",
        ],
        stdout=log,
        stderr=log,
    )
    try:
        time.sleep(1)
        for probes in [48, 64]:
            for pair in range(0, args.queries, 2):
                for qid, label in [
                    (pair, "base"),
                    (pair, "candidate"),
                    (pair + 1, "candidate"),
                    (pair + 1, "base"),
                ]:
                    binary = args.baseline if label == "base" else args.candidate
                    name = f"cold-{probes}-{qid}-{label}"
                    with server(binary, args, name, probes, cold=True) as (_, path):
                        status, result, ms = request(
                            "nq",
                            {
                                "rank_by": ["vector", "ANN", vectors[qid].tolist()],
                                "top_k": 10,
                                "include_attributes": ["id"],
                            },
                        )
                        assert status == 200, result
                        time.sleep(
                            0.1
                        )  # Let speculative reads finish before counting them.
                    rows.append(
                        {
                            "probes": probes,
                            "query": qid,
                            "arm": label,
                            "ms": ms,
                            "rows": result["rows"],
                            **trace(path),
                        }
                    )
                    (args.out_dir / "cold-rows.json").write_text(json.dumps(rows))
            summary = {"probes": probes}
            for label in ["base", "candidate"]:
                selected = [
                    r for r in rows if r["probes"] == probes and r["arm"] == label
                ]
                summary[label] = {
                    "p50": float(np.percentile([r["ms"] for r in selected], 50)),
                    "p90": float(np.percentile([r["ms"] for r in selected], 90)),
                    **{
                        k: float(np.mean([r[k] for r in selected]))
                        for k in ["reads", "bytes", "waves"]
                    },
                }
            deltas = []
            for qid in range(args.queries):
                paired = {
                    r["arm"]: r
                    for r in rows
                    if r["probes"] == probes and r["query"] == qid
                }
                assert paired["base"]["rows"] == paired["candidate"]["rows"], (
                    "cold result mismatch"
                )
                deltas.append(paired["candidate"]["ms"] - paired["base"]["ms"])
            rng = np.random.default_rng(27)
            sampled = rng.integers(0, len(deltas), (20000, len(deltas)))
            boot = np.median(np.asarray(deltas)[sampled], axis=1)
            paired = {
                arm: np.asarray(
                    [
                        next(
                            r["ms"]
                            for r in rows
                            if r["probes"] == probes
                            and r["query"] == qid
                            and r["arm"] == arm
                        )
                        for qid in range(args.queries)
                    ]
                )
                for arm in ["base", "candidate"]
            }
            tail = np.percentile(
                paired["candidate"][sampled], 90, axis=1
            ) - np.percentile(paired["base"][sampled], 90, axis=1)
            summary.update(
                paired_median_ms=float(np.median(deltas)),
                paired_median_ci95=np.quantile(boot, [0.025, 0.975]).tolist(),
                paired_p90_change_ms=summary["candidate"]["p90"]
                - summary["base"]["p90"],
                paired_p90_ci95=np.quantile(tail, [0.025, 0.975]).tolist(),
                identical=True,
            )
            (args.out_dir / f"cold-{probes}.json").write_text(
                json.dumps(summary, indent=2)
            )
            print(json.dumps(summary), flush=True)
    finally:
        facade.terminate()
        facade.wait(timeout=10)
        log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["shapes", "cold"], required=True)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--queries", type=int, default=30)
    args = parser.parse_args()
    for field in ["store", "data", "baseline", "candidate", "out_dir"]:
        setattr(args, field, getattr(args, field).resolve())
    if (
        not args.out_dir.is_relative_to(ROOT / ".scratch")
        or args.queries <= 0
        or args.queries % 2
    ):
        parser.error("use a .scratch output directory and a positive, even query count")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    vectors = np.load(args.data / "queries.npy")
    if args.queries > len(vectors):
        parser.error("not enough queries")
    if args.mode == "shapes":
        shapes(args, vectors)
    else:
        cold(args, vectors)
