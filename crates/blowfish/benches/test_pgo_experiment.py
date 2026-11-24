"""Evidence-admission checks for the PGO experiment, run with uv and unittest."""

import copy
import tempfile
import unittest
from pathlib import Path

import pgo_experiment as pgo


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.scratch = pgo.h1.ROOT / ".scratch"
        self.scratch.mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(
            dir=self.scratch, prefix="pgo-test-"
        )
        self.addCleanup(self.directory.cleanup)

    def test_every_warm_response_must_be_eligible(self):
        good = (
            0,
            0,
            1,
            {
                "performance": {
                    "cache_hit_ratio": 1,
                    "exhaustive_search_count": 0,
                    "approx_namespace_size": 2681468,
                }
            },
        )
        pgo.validate_warm([good, good], 2)
        for key, value in (
            ("cache_hit_ratio", 0.99),
            ("exhaustive_search_count", 1),
            ("approx_namespace_size", 10000),
        ):
            bad = copy.deepcopy(good)
            bad[3]["performance"][key] = value
            with self.assertRaises(RuntimeError):
                pgo.validate_warm([good, bad], 2)
        with self.assertRaises(RuntimeError):
            pgo.validate_warm([good, good], 1)

    def test_cpu_wide_counts_are_normalized_only_with_full_coverage(self):
        path = Path(self.directory.name) / "perf.csv"
        path.write_text(
            "# comment\n2000,,r01:u,1000000000,100.00,,\n"
            "4000,,r23:u,1000000000,100.00,,\n"
        )
        counts = pgo.read_perf(path, 100, ["r01:u", "r23:u"])
        self.assertEqual(counts["r01:u"]["per_query"], 20)
        self.assertEqual(counts["r23:u"]["per_query"], 40)
        for line in (
            "<not supported>,,r01:u,0,100.00,,\n",
            "2000,,r01:u,1000000000,98.00,,\n",
            "0,,r01:u,1000000000,100.00,,\n",
            "",
        ):
            path.write_text(line)
            with self.assertRaises(RuntimeError):
                pgo.read_perf(path, 100, ["r01:u"])


if __name__ == "__main__":
    unittest.main()
