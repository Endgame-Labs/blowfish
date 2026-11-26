# /// script
# dependencies = ["numpy==2.5.4"]
# ///
"""Round G11 anomaly probe: paced top_k queries against a promoted graph, logging the graph's status.

Usage: g11_lull.py DATA PORT K QPS SECONDS OUT.jsonl
One client at QPS requests a second, `top_k` K, IDs only. Every 5 seconds it writes whether a graph is
present, whether it is usable, the answered counter, and the calibration window.
"""
import http.client, json, sys, time
import numpy as np

data, port, k, qps, seconds, out = sys.argv[1:]
k, qps, seconds, port = int(k), float(qps), float(seconds), int(port)
queries = np.load(f"{data}/queries.npy")
conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)


def status():
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    c.request("GET", "/_blowfish/v1/status")
    r = json.loads(c.getresponse().read())["hot_graphs"].get("nq", {"graphs": []})
    c.close()
    return r


t0 = time.monotonic()
next_status = 0.
i = 0
with open(out, "w") as f:
    while time.monotonic() - t0 < seconds:
        due = t0 + i / qps
        time.sleep(max(0., due - time.monotonic()))
        body = json.dumps({"rank_by": ["vector", "ANN", queries[i % len(queries)].tolist()], "top_k": k,
                           "include_attributes": False})
        conn.request("POST", "/v2/namespaces/nq/query", body, {"content-type": "application/json"})
        conn.getresponse().read()
        i += 1
        if time.monotonic() - t0 >= next_status:
            next_status += 5
            s = status()
            g = s["graphs"][0] if s["graphs"] else None
            cal = g and g["calibration"]
            f.write(json.dumps({"t": round(time.monotonic() - t0), "queries": i, "graph": bool(g),
                                "answered": s.get("answered"), "usable": bool(cal and cal["usable"]),
                                "samples": cal and cal["samples"], "ef": cal and cal["ef"],
                                "ratio": cal and cal.get("graph_cost_ratio")}) + "\n")
            f.flush()
