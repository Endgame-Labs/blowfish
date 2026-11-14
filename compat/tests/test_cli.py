import unittest

from compat.cli import compare, compare_scenario, error_category, normalize, register, SCENARIOS


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

    def test_metadata_estimates_validate_presence_and_integer_type(self):
        expected = {"approx_row_count": 0, "approx_logical_bytes": 0}
        self.assertEqual(normalize({"approx_row_count": 4, "approx_logical_bytes": 123}), expected)
        for invalid in (-1, 1.5, True, None, "4"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                normalize({"approx_row_count": invalid})
        self.assertTrue(compare(expected, normalize({"approx_row_count": 4})))

    def test_explain_accepts_an_unindexed_error_or_a_string_plan(self):
        oracle = {"status": 400, "object": True, "plan_text_is_string": False, "error_is_string": True}
        blowfish = {"status": 200, "object": True, "plan_text_is_string": True, "error_is_string": False}
        for expected in (oracle, blowfish):
            for actual in (oracle, blowfish):
                self.assertEqual(compare_scenario("explain_query", expected, actual), [])
        for invalid in (
            {**blowfish, "plan_text_is_string": False},
            {**oracle, "error_is_string": False},
            {**oracle, "status": 500},
            {**blowfish, "object": False},
        ):
            self.assertTrue(compare_scenario("explain_query", oracle, invalid))


if __name__ == "__main__":
    unittest.main()
