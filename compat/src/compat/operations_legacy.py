"""Oracle scenarios for async copying, namespace settings, and pre-v2 clients."""
import time
from urllib.parse import urljoin, urlsplit

import httpx


SCENARIOS = (
    "async_copy", "async_copy_failure", "async_prefer_copy", "async_prefer_write",
    "metadata_read_only", "metadata_pinning", "legacy_columns", "legacy_rows",
    "legacy_deletes", "legacy_delete_filter", "legacy_copy", "legacy_query",
    "legacy_object_filter", "legacy_export", "legacy_head",
)


def run(c, name, kind):
    """Exercise a fresh namespace, with every auxiliary namespace deleted in finally."""
    from compat.cli import normalize

    ns = c.namespace(name)
    base = str(c.base_url).rstrip("/")
    v1, v2 = f"/v1/namespaces/{name}", f"/v2/namespaces/{name}"
    rows = [{"id": 1, "vector": [1.0, 0.0], "label": "a"}, {"id": 2, "vector": [0.0, 1.0]}]
    with httpx.Client(base_url=base, headers={"Authorization": f"Bearer {c.api_key}"}, timeout=60) as http:
        def request(method, path, expected=200, **kw):
            response = http.request(method, path, **kw)
            if response.status_code != expected:
                raise ValueError(f"{kind}: expected {expected}, got {response.status_code}: {response.text}")
            return response

        def body(method, path, **kw):
            return request(method, path, **kw).json()

        def seed():
            ns.write(upsert_rows=rows, distance_metric="cosine_distance")

        def after():
            return normalize(ns.query(rank_by=["id", "asc"], top_k=10, include_attributes=True))

        def poll(location):
            absolute = urljoin(base + "/", location)
            if urlsplit(absolute).netloc != urlsplit(base).netloc or urlsplit(absolute).scheme != urlsplit(base).scheme:
                raise ValueError("operation Location must have the same origin")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                operation = body("GET", absolute)
                if not isinstance(operation.get("start_time"), str):
                    raise ValueError("operation missing start_time")
                if operation["status"] == "finished":
                    if not isinstance(operation.get("finish_time"), str):
                        raise ValueError("finished operation missing finish_time")
                    result = operation["result"]
                    if set(result) not in ({"success"}, {"error"}):
                        raise ValueError("operation must have exactly one result")
                    if "error" in result:
                        return {"error_status": result["error"]["status_code"], "error_shape": result["error"]["detail"]["status"]}
                    return normalize(result["success"], require_metrics=False)
                if operation["status"] != "running":
                    raise ValueError("unknown operation status")
                time.sleep(0.1)
            raise TimeoutError("copy did not finish")

        if kind in ("async_copy", "async_copy_failure", "async_prefer_copy", "legacy_copy"):
            source = name + "-source"
            try:
                c.namespace(source).write(upsert_rows=rows, distance_metric="cosine_distance")
                if kind == "async_copy_failure":
                    seed()
                if kind == "legacy_copy":
                    result = body("POST", v1, json={"copy_from_namespace": source})
                elif kind == "async_prefer_copy":
                    # The current SDK transparently polls only when Preference-Applied is set.
                    result = ns.write(copy_from_namespace=source).model_dump(mode="json", exclude_unset=True)
                else:
                    response = request("POST", v2 + "/async", expected=202, json={"copy_from_namespace": {"source_namespace": source}})
                    if "preference-applied" in response.headers:
                        raise ValueError("explicit async endpoint must not trigger transparent polling")
                    if not isinstance(response.json().get("token"), str):
                        raise ValueError("async response missing token")
                    result = poll(response.headers["location"])
                return {"copy": normalize(result, require_metrics=False), "after": after()}
            finally:
                c.namespace(source).delete_all()
        if kind in ("legacy_columns", "legacy_rows"):
            payload = {"ids": [1, 2], "vectors": ["AACAPwAAAAA=", [0.0, 1.0]], "attributes": {"label": ["a", None]}} if kind == "legacy_columns" else {"upserts": [{"id": 1, "vector": [1.0, 0.0], "attributes": {"label": "a"}}, {"id": 2, "vector": [0.0, 1.0]}]}
            result = body("POST", v1, json={**payload, "distance_metric": "cosine_distance"})
            return {"write": normalize(result), "after": after()}
        seed()
        if kind == "async_prefer_write":
            response = request("POST", v2, json={"deletes": [2]}, headers={"prefer": "respond-async"})
            return {"status": response.status_code, "body": normalize(response.json()), "after": after()}
        if kind == "metadata_read_only":
            patched = body("PATCH", v1 + "/metadata", json={"read_only": True})
            persisted = body("GET", v2 + "/metadata")
            denied = request("POST", v2, expected=400, json={"deletes": [1]}).json()
            schema_denied = request("POST", v1 + "/schema", expected=400, json={"label": {"type": "string"}}).json()
            cleared = body("PATCH", v1 + "/metadata", json={"read_only": False})
            body("POST", v2, json={"deletes": [2]})
            return {"patched": patched["read_only"], "persisted": persisted["read_only"], "denied": denied, "schema_denied": schema_denied, "cleared": "read_only" not in cleared, "after": after()}
        if kind == "metadata_pinning":
            try:
                default = body("PATCH", v1 + "/metadata", json={"pinning": True})["pinning"]["replicas"]
                replicas = body("PATCH", v1 + "/metadata", json={"pinning": {"replicas": 2}})["pinning"]["replicas"]
                persisted = body("GET", v2 + "/metadata")["pinning"]["replicas"]
                cleared = "pinning" not in body("PATCH", v1 + "/metadata", json={"pinning": None})
                return {"default": default, "replicas": replicas, "persisted": persisted, "cleared": cleared}
            finally:
                body("PATCH", v1 + "/metadata", json={"pinning": False})
        if kind in ("legacy_deletes", "legacy_delete_filter"):
            payload = {"ids": [2], "vectors": [None]} if kind == "legacy_deletes" else {"delete_by_filter": {"label": [["Eq", "a"]]}}
            return {"write": normalize(body("POST", v1, json=payload)), "after": after()}
        if kind in ("legacy_query", "legacy_object_filter"):
            filters = {"label": [["Eq", "a"]]} if kind == "legacy_object_filter" else ["id", "Gte", 1]
            return body("POST", v1 + "/query", json={"vector": [1.0, 0.0], "top_k": 10, "include_vectors": True, "include_attributes": True, "filters": filters, "vector_encoding_format": "base64"})
        if kind == "legacy_export":
            page = body("GET", v1)
            # The cursor is opaque, and the oracle chooses its own export page size.
            result = []
            while True:
                for i, identifier in enumerate(page["ids"]):
                    result.append({"id": identifier, "vector": page["vectors"][i], "attributes": {key: values[i] for key, values in page["attributes"].items() if values[i] is not None}})
                if not page.get("next_cursor"):
                    break
                page = body("GET", v1, params={"cursor": page["next_cursor"]})
            return sorted(result, key=lambda row: row["id"])
        if kind == "legacy_head":
            response = request("HEAD", v1)
            return {"dimensions": int(response.headers["x-turbopuffer-dimensions"]), "count": int(response.headers["x-turbopuffer-approx-num-vectors"]), "created_at": bool(response.headers["x-turbopuffer-created-at"])}
        raise ValueError(kind)
