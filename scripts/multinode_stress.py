"""Exercise acknowledged writes and conditional serialization across three nodes."""
import concurrent.futures
import json
import subprocess
import time
import urllib.request
import uuid

BASES = [f"http://127.0.0.1:{port}" for port in (8101, 8102, 8103)]
NS = "multinode-stress-" + uuid.uuid4().hex


def request(node, path, body=None, method="POST"):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASES[node] + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)


def write(i):
    return request(i % 3, f"/v2/namespaces/{NS}",
                   {"upsert_rows": [{"id": i + 10, "value": i}]})


def conditional(i):
    return request(i % 3, f"/v2/namespaces/{NS}",
                   {"patch_rows": [{"id": 1, "value": i + 1}],
                    "patch_condition": ["value", "Eq", 0]})["rows_affected"]


try:
    request(0, f"/v2/namespaces/{NS}", {"upsert_rows": [{"id": 1, "value": 0}]})
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        assert sum(pool.map(conditional, range(18))) == 1, "conditional writes were not serialized"
        futures = [pool.submit(write, i) for i in range(90)]
        time.sleep(0.1)
        subprocess.run(["docker", "compose", "--profile", "multinode", "kill", "node3"], check=True)
        subprocess.run(["docker", "compose", "--profile", "multinode", "start", "node3"], check=True)
        acknowledged = []
        for i, future in enumerate(futures):
            try:
                future.result()
                acknowledged.append(i)
            except Exception as error:
                print(f"write {i} was not acknowledged: {error}")
    assert acknowledged, "no writes were acknowledged"
    result = request(0, f"/v2/namespaces/{NS}/query", {
        "rank_by": ["id", "asc"], "top_k": 1000, "include_attributes": True,
        "consistency": {"level": "strong"}})
    rows = {row["id"]: row for row in result["rows"]}
    for i in acknowledged:
        assert rows[i + 10]["value"] == i, f"acknowledged write {i} lost"
    print(f"Verified {len(acknowledged)} acknowledged writes and conditional serialization")
finally:
    request(0, f"/v2/namespaces/{NS}", method="DELETE")
