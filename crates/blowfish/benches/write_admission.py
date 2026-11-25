"""Trace the repository's prepared-body ingest loader without changing its retry policy.

Run with uv: uv run python -B write_admission.py BODIES --trace RUN/requests.jsonl.
Prepare BODIES with crates/bf-wal/benches/http_ingest.py. This wrapper keeps its
schema-first load, concurrency, advertised retry sleeps, and acknowledgment checks.
Keep BODIES, RUN, and other outputs under .scratch/. Each trace row is one
logical request, including every attempt and retry sleep. Per-request timing
starts after the body file read, as in the loader; total load time includes reads.
The original loader summary is followed by the timing summary. Use --stages with
a fresh, auth-disabled disposable server to include individual engine and WAL
stage timings. Stage intervals can overlap. Removing sleeps is an
accounting estimate, not a prediction of a different admission policy.
"""

import argparse
import http.client
import importlib.util
import json
from pathlib import Path
import re
import threading
import time
from types import SimpleNamespace
from urllib.request import urlopen


class Recorder:
    """Collect attempt, worker, and retry timings using a clock local to the client."""

    def __init__(self, trace):
        self.local = threading.local()
        self.lock = threading.Lock()
        self.records = []
        self.connections = []
        self.trace = trace

    def connection_type(self):
        recorder = self

        class Connection(http.client.HTTPConnection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                with recorder.lock:
                    recorder.connections.append(self)

            def request(self, method, url, body=None, headers=None, **kwargs):
                started = time.monotonic()
                if not hasattr(recorder.local, "current"):
                    match = re.search(rb'"id"\s*:\s*\[\s*(\d+)', body[:256])
                    recorder.local.current = {
                        "id": int(match[1]) if match else None,
                        "started": started,
                        "attempts": [],
                        "retry_sleep_ms": 0.0,
                    }
                recorder.local.attempt_started = started
                return super().request(method, url, body, headers or {}, **kwargs)

            def getresponse(self):
                response = super().getresponse()
                return Response(response, recorder)

        return Connection

    def sleep(self, seconds):
        started = time.monotonic()
        time.sleep(seconds)
        self.local.current["retry_sleep_ms"] += (time.monotonic() - started) * 1000

    def finish_attempt(self, response, data):
        now = time.monotonic()
        current = self.local.current
        attempt = {
            "status": response.status,
            "http_ms": (now - self.local.attempt_started) * 1000,
            "retry_after_ms": response.getheader("retry-after-ms"),
        }
        if response.status == 200:
            # This is a commit-group duration, which can start before this request joins.
            attempt["worker_ms"] = json.loads(data).get("performance", {}).get("server_total_ms")
        current["attempts"].append(attempt)
        if response.status == 200:
            current["total_ms"] = (now - current.pop("started")) * 1000
            with self.lock:
                self.records.append(current)
                self.trace.write(json.dumps(current) + "\n")
                self.trace.flush()
            del self.local.current

    def close(self):
        for connection in self.connections:
            connection.close()


class Response:
    """Record the response after its complete body has arrived."""

    def __init__(self, response, recorder):
        self.response = response
        self.recorder = recorder
        self.status = response.status

    def getheader(self, name, default=None):
        return self.response.getheader(name, default)

    def read(self):
        data = self.response.read()
        self.recorder.finish_attempt(self.response, data)
        return data


def percentile(values, fraction):
    """Use the ingest loader's lower order statistic, including at p90."""
    return sorted(values)[int((len(values) - 1) * fraction)]


def summarize(records):
    """Separate retries from successful work, including an optimistic no-sleep bound."""
    totals = [r["total_ms"] for r in records]
    without_sleep = [r["total_ms"] - r["retry_sleep_ms"] for r in records]
    threshold = percentile(totals, 0.9)
    tail = [r for r in records if r["total_ms"] >= threshold]
    return {
        "writes": len(records),
        "retried_writes": sum(len(r["attempts"]) > 1 for r in records),
        "retries": sum(len(r["attempts"]) - 1 for r in records),
        "latency_ms": {name: percentile(totals, q) for name, q in
                       (("p50", 0.5), ("p90", 0.9), ("p99", 0.99), ("max", 1))},
        "without_retry_sleep_ms": {name: percentile(without_sleep, q) for name, q in
                                  (("p90", 0.9), ("p99", 0.99), ("max", 1))},
        "successful_attempt_ms": {name: percentile([r["attempts"][-1]["http_ms"] for r in records], q)
                                  for name, q in (("p90", 0.9), ("p99", 0.99), ("max", 1))},
        "retry_sleep_ms": sum(r["retry_sleep_ms"] for r in records),
        "p90_tail_writes": len(tail),
        "p90_tail_retried_writes": sum(len(r["attempts"]) > 1 for r in tail),
    }


def stage_samples(text):
    """Read only the engine and WAL stage summaries from Prometheus text."""
    samples = {}
    pattern = re.compile(r'^(blowfish_(?:commit|wal)_stage_seconds)(?:_(count|sum))?\{([^}]+)\} (\S+)$')
    for line in text.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        metric, kind, labels, value = match.groups()
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', labels))
        if "stage" not in labels:
            continue
        key = (metric, labels["stage"])
        field = kind or {"0.5": "p50", "0.9": "p90"}.get(labels.get("quantile"))
        if field:
            samples.setdefault(key, {})[field] = float(value)
    return samples


def stage_summary(before, after):
    """Report per-stage run means; fresh-process quantiles describe individual stages.

    Concurrent stage times overlap, and their percentiles must not be added. A
    process with previous samples cannot provide run-specific quantiles.
    """
    rows = {}
    for (metric, stage), values in after.items():
        old = before.get((metric, stage), {})
        count = values.get("count", 0) - old.get("count", 0)
        if count <= 0:
            continue
        row = {"samples": int(count),
               "mean_ms": 1000 * (values["sum"] - old.get("sum", 0)) / count}
        if not old.get("count", 0):
            row.update({f"{q}_ms": 1000 * values[q] for q in ("p50", "p90") if q in values})
        rows[f"{metric.removeprefix('blowfish_').removesuffix('_stage_seconds')}.{stage}"] = row
    return rows


def read_stages(host, port):
    """Scrape an auth-disabled disposable benchmark server, outside the timed load."""
    with urlopen(f"http://{host}:{port}/metrics", timeout=30) as response:
        return stage_samples(response.read().decode())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bodies", type=Path)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18100)
    parser.add_argument("--namespace", default="w2")
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--stages", action="store_true",
                        help="include engine and WAL stage metrics from a fresh disposable server")
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[2] / "bf-wal/benches/http_ingest.py"
    spec = importlib.util.spec_from_file_location("ingest", source)
    ingest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ingest)
    with args.trace.open("w") as trace:
        recorder = Recorder(trace)
        ingest.http = SimpleNamespace(client=SimpleNamespace(HTTPConnection=recorder.connection_type()))
        ingest.time = SimpleNamespace(monotonic=time.monotonic, sleep=recorder.sleep)
        try:
            before = read_stages(args.host, args.port) if args.stages else {}
            ingest.load(args)
            summary = summarize(recorder.records)
            if args.stages:
                summary["stages"] = stage_summary(before, read_stages(args.host, args.port))
            print(json.dumps(summary), flush=True)
        finally:
            recorder.close()


if __name__ == "__main__":
    main()
