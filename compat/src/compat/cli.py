"""Run the official SDK against isolated compatibility scenarios."""
import argparse
import fnmatch
import json
import math
import os
from pathlib import Path
import re
import sys
import uuid

import turbopuffer

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
ROWS = [
    {"id": 1, "vector": [1.0, 0.0, 0.0], "n": 1, "name": "alpha", "tags": ["red", "blue"], "text": "quick brown fox"},
    {"id": 2, "vector": [0.0, 1.0, 0.0], "n": 2, "name": "beta", "tags": ["blue"], "text": "quick dog"},
    {"id": 3, "vector": [0.0, 0.0, 1.0], "n": 3, "name": "gamma", "tags": [], "text": "slow fox"},
    {"id": 4, "vector": [0.5, 0.5, 0.0], "n": 4, "name": "delta", "text": "brown dog"},
]
SCENARIOS = {}


def scenario(name, method, params, *, setup=True, expected_error=False, bm25=False):
    """Register a named SDK operation on a fresh namespace."""
    def run(ns):
        """Execute this scenario's SDK operation."""
        if setup:
            normalize(ns.write(upsert_rows=ROWS, distance_metric="cosine_distance", schema={"text": {"type": "string", "full_text_search": True}}))
        try:
            value = getattr(ns, method)(**params)
        except turbopuffer.APIStatusError as exc:
            if not expected_error:
                raise
            body = exc.body if isinstance(exc.body, dict) else {}
            message = str(body.get("error", ""))
            return {"error": {"status": exc.status_code, "category": error_category(message), "message": message}}
        if expected_error:
            raise ValueError("expected an API error, but the request succeeded")
        result = value if isinstance(value, dict) else value.model_dump(mode="json", exclude_unset=True)
        if method in ("query", "multi_query", "write"):
            for key in ("performance", "billing"):
                if key not in result or not isinstance(result[key], dict):
                    raise ValueError(f"{method} response missing object {key}")
        if method == "write" and setup:
            result = {"write": normalize(result), "after": normalize(ns.query(rank_by=["id", "asc"], top_k=10, include_attributes=True))}
        return normalize(result)
    SCENARIOS[name] = (run, bm25)


def error_category(message):
    m = message.lower()
    for category, terms in [("missing_namespace", ("not found", "does not exist", "no such namespace", "unknown namespace")), ("dimensions", ("dimension", "vector length", "vector size")), ("id_type", ("id type", "mix id", "mixed id", "identifier type")), ("deserialize", ("deserialize", "variant", "unknown operator", "invalid operator")), ("limit", ("limit", "top_k")), ("type", ("type",))]:
        if any(term in m for term in terms):
            return category
    return "other"


def normalize(value, depth=0, require_metrics=True):
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json", exclude_unset=True)
    if isinstance(value, list):
        return [normalize(v, depth + 1) for v in value]
    if isinstance(value, dict):
        if require_metrics and depth == 0 and any(key in value for key in ("rows", "results", "rows_affected")):
            for key in ("performance", "billing"):
                if not isinstance(value.get(key), dict):
                    raise ValueError(f"response missing object {key}")
        for key in ("performance", "billing"):
            if key in value and not isinstance(value[key], dict):
                raise ValueError(f"{key} must be an object")
        perf = value.get("performance", {})
        for key, item in perf.items():
            if key in {"server_total_ms", "query_execution_ms", "exhaustive_search_count", "approx_namespace_size", "cache_hit_ratio"} and (not isinstance(item, (int, float)) or isinstance(item, bool)):
                raise ValueError(f"performance.{key} must be numeric")
            if key in {"cache_temperature", "last_included_write_at"} and not isinstance(item, str):
                raise ValueError(f"performance.{key} must be a string")
        for key, item in value.get("billing", {}).items():
            if key.startswith("billable_") and (not isinstance(item, (int, float)) or isinstance(item, bool)):
                raise ValueError(f"billing.{key} must be numeric")
        for key, item in value.items():
            if key.startswith("approx_") and (type(item) is not int or item < 0):
                raise ValueError(f"{key} must be a non-negative integer")
        # Index-derived estimates can lag writes; retain presence and validate
        # the integer contract while allowing exact counts from blowfish.
        value = {k: 0 if k.startswith("approx_") else v for k, v in value.items()}
        return {k: normalize(v, depth + 1) for k, v in value.items() if k not in {"performance", "billing", "created_at", "updated_at", "last_write_at", "last_included_write_at", "trace_id", "request_id"}}
    return value


def compare(expected, actual, path="", tol=1e-5, tie_attrs=()):
    if path.endswith(".message") and ".error." in path:
        return []
    if path.endswith(".$dist") and isinstance(expected, (float, int)) and isinstance(actual, (float, int)):
        return [] if math.isclose(expected, actual, rel_tol=tol, abs_tol=1e-6) else [f"{path}: expected {expected}, got {actual}"]
    if type(expected) is not type(actual):
        return [f"{path}: expected {expected!r}, got {actual!r}"]
    if isinstance(expected, dict):
        if "recall_queries" in expected and "recall_queries" in actual:
            left, right = expected["recall_queries"], actual["recall_queries"]
            if len(left) != len(right):
                return [f"{path}: query count differs"]
            issues = []
            for i, (a, b) in enumerate(zip(left, right)):
                ids = {row["id"] for row in a}
                recall = len(ids & {row["id"] for row in b}) / len(ids) if ids else 1.0
                if len(a) != 10 or len(b) != 10 or recall < 0.9:
                    issues.append(f"{path}[{i}]: recall@10 {recall:.2f}, lengths {len(a)}, {len(b)}")
            return issues
        issues = []
        for k in expected.keys() | actual.keys():
            if k not in expected or k not in actual:
                issues.append(f"{path}.{k}: missing from {'fixture' if k not in expected else 'response'}")
            else:
                issues.extend(compare(expected[k], actual[k], f"{path}.{k}", tol, tie_attrs))
        return issues
    if isinstance(expected, list):
        # Ranking ties have no specified order. Only rearrange runs whose scores
        # are equal, and keep the ordering of distinct scores intact.
        def sort_ties(rows):
            rows = list(rows)
            start = 0
            while start < len(rows):
                row = rows[start]
                if not isinstance(row, dict) or "id" not in row:
                    start += 1
                    continue
                keys = ("$dist",) if "$dist" in row else tie_attrs
                if not keys or any(k not in row for k in keys):
                    start += 1
                    continue
                end = start + 1
                while end < len(rows) and isinstance(rows[end], dict) and "id" in rows[end] and all(k in rows[end] and rows[end][k] == row[k] for k in keys):
                    end += 1
                rows[start:end] = sorted(rows[start:end], key=lambda item: (type(item["id"]).__name__, str(item["id"])))
                start = end
            return rows
        expected, actual = sort_ties(expected), sort_ties(actual)
        if len(expected) != len(actual):
            return [f"{path}: expected {len(expected)} items, got {len(actual)}"]
        return [issue for i, (a, b) in enumerate(zip(expected, actual)) for issue in compare(a, b, f"{path}[{i}]", tol, tie_attrs)]
    return [] if expected == actual else [f"{path}: expected {expected!r}, got {actual!r}"]


def compare_scenario(name, expected, actual):
    """Compare a scenario while allowing the documented exhaustive explain plan."""
    if name == "explain_query":
        valid = (
            {"status": 400, "object": True, "plan_text_is_string": False, "error_is_string": True},
            {"status": 200, "object": True, "plan_text_is_string": True, "error_is_string": False},
        )
        if expected in valid and actual in valid:
            return []
        return [f"explain_query: expected an unindexed 400 error or a 200 string plan_text, got {actual!r}"]
    return compare(expected, actual, tol=0.02 if SCENARIOS[name][1] else 1e-5, tie_attrs=("n",) if name == "order_ties" else ())


def register():
    def q(name, **kw):
        params = {"rank_by": ["id", "asc"], "top_k": 10, "include_attributes": True, **kw}
        if "limit" in kw:
            params.pop("top_k", None)
        if "exclude_attributes" in kw:
            params.pop("include_attributes", None)
        scenario(name, "query", params)
    for op, value in [("Eq", 2), ("NotEq", 2), ("In", [1, 3]), ("NotIn", [1, 3]), ("Lt", 3), ("Lte", 3), ("Gt", 2), ("Gte", 2)]:
        q("filter_" + op.lower(), filters=["n", op, value])
    for op, value in [("Contains", "blue"), ("NotContains", "blue"), ("ContainsAny", ["red", "blue"]), ("NotContainsAny", ["red"])]:
        q("filter_" + op.lower(), filters=["tags", op, value])
    for op, value in [("Glob", "*a*"), ("NotGlob", "*a*"), ("IGlob", "*A*"), ("NotIGlob", "*A*")]:
        q("filter_" + op.lower(), filters=["name", op, value])
    for op, value in [("ContainsAllTokens", "quick fox"), ("ContainsAnyToken", "quick fox"), ("ContainsTokenSequence", "quick brown")]:
        q("filter_" + op.lower(), filters=["text", op, value])
    for op in ("AnyLt", "AnyLte", "AnyGt", "AnyGte"):
        q("filter_" + op.lower(), filters=["tags", op, "blue"])
    for name, f in [("and", ["And", [["n", "Gt", 1], ["n", "Lt", 4]]]), ("or", ["Or", [["n", "Eq", 1], ["n", "Eq", 3]]]), ("not", ["Not", ["n", "Eq", 1]]), ("null", ["tags", "Eq", None]), ("not_null", ["tags", "NotEq", None])]:
        q("filter_" + name, filters=f)
    for name, params in {
        "order_asc": {"rank_by": ["n", "asc"]}, "order_desc": {"rank_by": ["n", "desc"]},
        "order_multiple": {"rank_by": [["n", "asc"], ["id", "desc"]]}, "offset": {"offset": 1, "top_k": 2},
        "include": {"include_attributes": ["name"]}, "exclude": {"exclude_attributes": ["text"], "include_attributes": True},
        "base64": {"vector_encoding": "base64"}, "strong": {"consistency": {"level": "strong"}},
        "eventual": {"consistency": {"level": "eventual", "max_staleness_seconds": 1}},
        "limit_per": {"limit": {"total": 4, "per": {"attributes": ["name"], "limit": 1}}, "top_k": 4},
        "export_page": {"filters": ["id", "Gt", 2]},
    }.items(): q(name, **params)
    for name, rank in [("ann", ["vector", "ANN", [1.0, 0.2, 0.1]]), ("knn", ["vector", "kNN", [1.0, 0.2, 0.1]]), ("bm25_single", ["text", "BM25", "fox"]), ("bm25_multi", ["text", "BM25", "quick fox"]), ("bm25_prefix", ["text", "BM25", "qui", {"last_as_prefix": True}]), ("score_sum", ["Sum", [["n", "Eq", 1], ["n", "Eq", 2]]]), ("score_max", ["Max", [["n", "Eq", 1], ["n", "Eq", 2]]]), ("score_product", ["Product", 2.0, ["n", "Eq", 1]]), ("score_attribute", ["Max", [0, ["Attribute", "n"]]]), ("score_saturate", ["Saturate", ["Attribute", "n"], {"midpoint": 2}]), ("score_decay", ["Decay", ["Attribute", "n"], {"midpoint": 2}]), ("score_dist", ["Decay", ["Dist", ["Attribute", "n"], 2], {"midpoint": 2}])]:
        params = {"rank_by": rank, "top_k": 4}
        if name == "knn": params["filters"] = ["n", "Gte", 1]
        scenario(name, "query", params, bm25="bm25" in name)
    for name, agg, group in [("count", {"c": ["Count"]}, []), ("sum", {"s": ["Sum", "n"]}, []), ("min", {"m": ["Min", "n"]}, []), ("max", {"m": ["Max", "n"]}, []), ("group", {"c": ["Count"]}, ["n"]), ("foreachunique", {"c": ["Count"]}, [{"tag": ["ForEachUnique", "tags"]}])]:
        scenario("aggregate_" + name, "query", {"aggregate_by": agg, **({"group_by": group, "top_k": 10} if group else {})})
    for name, params in {
        "upsert_rows": {"upsert_rows": [{"id": 5, "n": 5, "vector": [1.0, 0.0, 0.0]}]}, "upsert_columns": {"upsert_columns": {"id": [5], "n": [5], "vector": [[1.0, 0.0, 0.0]]}},
        "patch_rows": {"patch_rows": [{"id": 1, "n": 10}]}, "patch_columns": {"patch_columns": {"id": [1], "n": [10]}},
        "deletes": {"deletes": [1]}, "delete_by_filter": {"delete_by_filter": ["n", "Eq", 1]},
        "patch_by_filter": {"patch_by_filter": {"filters": ["n", "Eq", 1], "patch": {"n": 10}}},
        "upsert_condition": {"upsert_rows": [{"id": 1, "n": 10, "vector": [1.0, 0.0, 0.0]}], "upsert_condition": ["n", "Lt", {"$ref_new": "n"}]},
        "patch_condition": {"patch_rows": [{"id": 1, "n": 10}], "patch_condition": ["n", "Eq", 1]},
        "delete_condition": {"deletes": [1], "delete_condition": ["n", "Eq", 1]},
        "write_order": {"delete_by_filter": ["n", "Eq", 1], "upsert_rows": [{"id": 1, "n": 5, "vector": [1.0, 0.0, 0.0]}], "deletes": [3], "patch_rows": [{"id": 2, "n": 8}]},
    }.items(): scenario("write_" + name, "write", params)
    scenario("type_string_id", "write", {"upsert_rows": [{"id": "alpha", "value": "hello"}]}, setup=False)
    scenario("type_uuid_id", "write", {"upsert_rows": [{"id": "a8098c1a-f86e-11da-bd1a-00112444be1e", "value": 1}]}, setup=False)
    scenario("type_scalars", "write", {"upsert_rows": [{"id": 1, "s": "hello", "i": -1, "f": 1.5, "b": True, "a": [1, 2]}], "schema": {"f": {"type": "float"}}}, setup=False)
    scenario("schema", "schema", {})
    scenario("metadata", "metadata", {})
    scenario("delete_namespace", "delete_all", {})
    scenario("multi_query", "multi_query", {"queries": [{"rank_by": ["id", "asc"], "top_k": 2}, {"rank_by": ["id", "desc"], "top_k": 2}]})
    scenario("multi_rrf", "multi_query", {"queries": [{"rank_by": ["id", "asc"], "top_k": 2}, {"rank_by": ["id", "desc"], "top_k": 2}], "rerank_by": ["RRF"], "limit": 2})
    scenario("error_missing", "query", {"rank_by": ["id", "asc"], "top_k": 1}, setup=False, expected_error=True)
    scenario("error_dimensions", "query", {"rank_by": ["vector", "ANN", [1.0]], "top_k": 1}, expected_error=True)
    scenario("error_filter", "query", {"rank_by": ["id", "asc"], "top_k": 1, "filters": ["n", "BadOperator", 1]}, expected_error=True)
    scenario("error_mixed_ids", "write", {"upsert_rows": [{"id": "wrong", "n": 1}]}, expected_error=True)
    scenario("error_limit", "query", {"rank_by": ["id", "asc"], "top_k": 10001}, expected_error=True)


EXTRA_SCENARIOS = ("types_roundtrip", "ann_recall", "copy_string", "copy_object", "list_pagination", "export_full", "raw_invalid_json", "raw_content_type", "raw_unknown_route", "raw_gzip", "raw_unknown_params", "hint_cache_warm", "debug_recall", "explain_query", "order_ties")


def client(url, key):
    return turbopuffer.Turbopuffer(base_url=url, api_key=key or "dummy", max_retries=0, timeout=15)


def execute(c, name):
    namespace = "blowfish-oracle-compat-" + name.replace("_", "-") + "-" + uuid.uuid4().hex[:12]
    ns = c.namespace(namespace)
    try:
        if name in EXTRA_SCENARIOS:
            from compat.extra import run
            return run(c, namespace, name)
        return SCENARIOS[name][0](ns)
    finally:
        try:
            ns.delete_all()
        except turbopuffer.NotFoundError:
            pass


def cleanup(c):
    names = [n.id for n in c.namespaces(prefix="blowfish-oracle-")]
    for name in names:
        c.namespace(name).delete_all()
    print(f"Deleted {len(names)} oracle namespaces")


def main():
    register()
    SCENARIOS.update({name: (None, False) for name in EXTRA_SCENARIOS})
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for cmd in ("record", "check", "diff-live", "cleanup"):
        p = sub.add_parser(cmd)
        if cmd in ("check", "diff-live"):
            p.add_argument("--base-url", required=True)
            p.add_argument("--api-key")
        if cmd != "cleanup": p.add_argument("-k", default="*", help="scenario name substring or glob")
    args = parser.parse_args()
    oracle_url = os.getenv("COMPAT_ORACLE_BASE_URL")
    if args.command in ("record", "diff-live", "cleanup") and not oracle_url:
        parser.error("COMPAT_ORACLE_BASE_URL is required")
    if args.command == "cleanup":
        cleanup(client(oracle_url, None)); return
    target = client(args.base_url, args.api_key) if args.command in ("check", "diff-live") else None
    oracle = client(oracle_url, None) if args.command in ("record", "diff-live") else None
    pattern = args.k if any(c in args.k for c in "*?[") else "*" + args.k + "*"
    selected = [n for n in SCENARIOS if fnmatch.fnmatch(n, pattern)]
    if not selected:
        print("No scenarios matched", file=sys.stderr); sys.exit(2)
    FIXTURES.mkdir(exist_ok=True)
    failed = 0
    for name in selected:
        try:
            path = FIXTURES / (name + ".json")
            expected = execute(oracle, name) if oracle else json.loads(path.read_text())
            if args.command == "record":
                path.write_text(json.dumps(expected, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
                issues = []
            else:
                actual = execute(target, name)
                issues = compare_scenario(name, expected, actual)
            if issues:
                failed += 1; print(f"FAIL {name}: " + "; ".join(issues[:8]), flush=True)
            else: print(f"PASS {name}", flush=True)
        except Exception as exc:
            failed += 1; print(f"FAIL {name}: {type(exc).__name__}: {exc}", flush=True)
    print(f"{len(selected)-failed}/{len(selected)} passed")
    if failed: sys.exit(1)


if __name__ == "__main__": main()
