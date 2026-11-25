"""Integration checks for the ingest timing wrapper. Run with uv and unittest."""

from contextlib import redirect_stdout
import http.server
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

MODULE = Path(__file__).with_name("write_admission.py")
spec = importlib.util.spec_from_file_location("write_admission", MODULE)
tracer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tracer)


class WriteAdmissionTests(unittest.TestCase):
    def test_real_loader_retries_and_records_each_acknowledged_request(self):
        seen = {}

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                count = sum(value == 2 for value in seen.values())
                data = (f'blowfish_wal_stage_seconds_sum{{stage="compress"}} {count / 10}\n'
                        f'blowfish_wal_stage_seconds_count{{stage="compress"}} {count}\n').encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                first = body["upsert_columns"]["id"][0]
                if first not in seen:
                    seen[first] = 1
                    data, status = b'{"status":"error","error":"backpressure"}', 429
                else:
                    seen[first] += 1
                    data, status = b'{"rows_upserted":1,"performance":{"server_total_ms":7}}', 200
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("retry-after-ms", "10")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        scratch = MODULE.resolve().parents[3] / ".scratch"
        scratch.mkdir(exist_ok=True)
        try:
            with tempfile.TemporaryDirectory(prefix="w2-test-", dir=scratch) as directory:
                run = Path(directory)
                (run / "shape.json").write_text('{"rows":3,"batch":1}')
                for first in range(3):
                    (run / f"{first:09}.json").write_text(json.dumps({"upsert_columns": {"id": [first]}}))
                trace = run / "requests.jsonl"
                args = [str(MODULE), str(run), "--trace", str(trace), "--port", str(server.server_port), "--stages"]
                output = io.StringIO()
                with patch.object(sys, "argv", args), redirect_stdout(output):
                    tracer.main()
                records = [json.loads(row) for row in trace.read_text().splitlines()]
                summary = json.loads(output.getvalue().splitlines()[-1])
                self.assertEqual(seen, {0: 2, 1: 2, 2: 2})
                self.assertEqual({r["id"] for r in records}, {0, 1, 2})
                self.assertEqual(summary["retries"], 3)
                self.assertEqual(summary["stages"]["wal.compress"], {"samples": 3, "mean_ms": 100})
                for record in records:
                    self.assertEqual([a["status"] for a in record["attempts"]], [429, 200])
                    self.assertEqual(record["attempts"][0]["retry_after_ms"], "10")
                    self.assertEqual(record["attempts"][-1]["worker_ms"], 7)
                    self.assertGreaterEqual(record["retry_sleep_ms"], 9)
                    self.assertGreaterEqual(record["total_ms"], record["retry_sleep_ms"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_stage_metrics_use_run_deltas_and_only_fresh_quantiles(self):
        text = '\n'.join([
            'blowfish_wal_stage_seconds{stage="compress",quantile="0.5"} 0.2',
            'blowfish_wal_stage_seconds{quantile="0.9",stage="compress"} 0.4',
            'blowfish_wal_stage_seconds_sum{stage="compress"} 0.6',
            'blowfish_wal_stage_seconds_count{stage="compress"} 2',
            'blowfish_commit_stage_seconds_sum{stage="prepare"} 0.1',
            'blowfish_commit_stage_seconds_count{stage="prepare"} 1',
            'unrelated_metric{stage="prepare"} 999',
        ])
        after = tracer.stage_samples(text)
        fresh = tracer.stage_summary({}, after)
        self.assertEqual(fresh["wal.compress"],
                         {"samples": 2, "mean_ms": 300, "p50_ms": 200, "p90_ms": 400})
        old = tracer.stage_samples(text.replace(' 0.6', ' 0.2').replace('} 2', '} 1'))
        delta = tracer.stage_summary(old, after)
        self.assertAlmostEqual(delta["wal.compress"]["mean_ms"], 400)
        self.assertEqual(delta["wal.compress"]["samples"], 1)
        self.assertNotIn("p90_ms", delta["wal.compress"])
        self.assertNotIn("commit.prepare", delta)

    def test_rare_retries_do_not_set_p90(self):
        records = [{"total_ms": 100, "retry_sleep_ms": 0, "attempts": [{"http_ms": 100}]} for _ in range(97)]
        records.extend({"total_ms": 16_100, "retry_sleep_ms": 16_000, "attempts": [{"http_ms": 0}, {"http_ms": 100}]}
                       for _ in range(3))
        summary = tracer.summarize(records)
        self.assertEqual(summary["latency_ms"]["p90"], 100)
        self.assertEqual(summary["without_retry_sleep_ms"]["p90"], 100)
        self.assertEqual(summary["latency_ms"]["p99"], 16_100)
        self.assertEqual(summary["without_retry_sleep_ms"]["p99"], 100)


if __name__ == "__main__":
    unittest.main()
