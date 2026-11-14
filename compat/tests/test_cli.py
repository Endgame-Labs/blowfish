import unittest

from compat.cli import compare, error_category, normalize, register, SCENARIOS


class ComparisonTests(unittest.TestCase):
    def test_normalization_preserves_results_and_validates_metrics(self):
        self.assertEqual(normalize({"rows": [{"id": 1}], "performance": {}, "billing": {}}), {"rows": [{"id": 1}]})
        with self.assertRaises(ValueError):
            normalize({"billing": None})

    def test_distances_and_error_messages(self):
        self.assertEqual(compare({"$dist": 1.0}, {"$dist": 1.01}, tol=0.02), [])
        self.assertTrue(compare({"$dist": 1.0}, {"$dist": 1.1}, tol=0.02))
        self.assertEqual(compare({"error": {"status": 400, "message": "one"}}, {"error": {"status": 400, "message": "two"}}), [])
        self.assertEqual(error_category("Invalid query dimensions"), "dimensions")

    def test_equal_score_rows_can_change_order(self):
        rows = [{"id": 1, "$dist": 1.0}, {"id": 2, "$dist": 1.0}]
        self.assertEqual(compare(rows, list(reversed(rows))), [])

    def test_attribute_ties_only_reorder_equal_keys(self):
        expected = [{"id": 1, "n": 1}, {"id": 2, "n": 1}, {"id": 3, "n": 2}]
        self.assertEqual(compare(expected, [expected[1], expected[0], expected[2]], tie_attrs=("n",)), [])
        self.assertTrue(compare(expected, [expected[2], expected[1], expected[0]], tie_attrs=("n",)))

    def test_ann_recall_threshold(self):
        expected = {"recall_queries": [[{"id": i} for i in range(10)]]}
        passing = {"recall_queries": [[{"id": i} for i in range(1, 11)]]}
        failing = {"recall_queries": [[{"id": i} for i in range(2, 12)]]}
        self.assertEqual(compare(expected, passing), [])
        self.assertTrue(compare(expected, failing))

    def test_missing_response_metrics(self):
        with self.assertRaises(ValueError):
            normalize({"rows": []})
        with self.assertRaises(ValueError):
            normalize({"rows": [], "performance": {"server_total_ms": "slow"}, "billing": {}})

    def test_scenarios_registered(self):
        register()
        self.assertGreaterEqual(len(SCENARIOS), 80)


if __name__ == "__main__":
    unittest.main()
