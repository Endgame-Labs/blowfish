"""Measurement regressions: uv run python -m unittest discover -s bench/nq -p test_measure.py."""

import contextlib
import io
import multiprocessing as mp
import os
import sys
import time
import types
import unittest
from unittest.mock import patch

import measure


class ErrorClient:
    def __init__(self, *args):
        pass

    def query(self, body):
        raise RuntimeError("simulated server loss")


class KilledClient(ErrorClient):
    def query(self, body):
        os._exit(9)


class MeasureTests(unittest.TestCase):
    def setUp(self):
        self.args = types.SimpleNamespace(base_url="http://localhost:1", namespace="nq", cpus=set())

    def test_first_pass_preserves_the_first_query_and_has_no_warmup(self):
        calls = []

        class Client(ErrorClient):
            def query(self, body):
                calls.append(body)

        for warmup, expected in [(False, 2), (True, 4)]:
            with self.subTest(warmup=warmup):
                calls.clear()
                with patch.object(measure, "Client", Client), patch.object(
                    measure.time, "perf_counter", side_effect=[0, .001, .001, .003]
                ):
                    result = measure.latency(self.args, [[0.], [1.]], warmup=warmup)
                self.assertEqual(len(calls), expected)
                self.assertEqual(result["p90"], 1.9)
                if warmup:
                    self.assertNotIn("first_query", result)
                else:
                    self.assertEqual(result["first_query"], 1.0)

    def test_first_pass_rejects_prior_cache_warming(self):
        argv = ["measure.py", "unused", "--tag", "invalid", "--first-pass", "--parts", "quality,latency"]
        with patch.object(sys, "argv", argv), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as caught:
                measure.main()
        self.assertEqual(caught.exception.code, 2)

    def test_failed_and_killed_workers_abort_and_are_reaped(self):
        for client, message in [(ErrorClient, "simulated server loss"), (KilledClient, "without results")]:
            with self.subTest(client=client.__name__):
                started = time.monotonic()
                with patch.object(measure, "Client", client):
                    with self.assertRaisesRegex(RuntimeError, message):
                        measure.throughput(self.args, [[1.], [2.]], 2)
                self.assertLess(time.monotonic() - started, 10)
                self.assertFalse(mp.active_children())


if __name__ == "__main__":
    unittest.main()
