# /// script
# dependencies = ["numpy==2.5.4", "httpx==0.28.1"]
# ///
"""Round G10: dump the production IVF answers for every NQ query to int32 npy files.

Usage: uv run g10_ivf_dump.py DATA_DIR PORT PROBES OUT_DIR
Needs a blowfish server on PORT serving namespace `nq` with ann_nprobe = PROBES and no graph
(a server nobody heats never promotes one). Writes OUT_DIR/ivf{PROBES}_k{10,100}.npy,
shape [queries, k], corpus IDs, -1 padded. The caller owns OUT_DIR.
"""
import concurrent.futures, pathlib, sys
import httpx, numpy as np

data, port, probes, out = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3], pathlib.Path(sys.argv[4])
queries = np.load(data / "queries.npy")
out.mkdir(parents=True, exist_ok=True)
client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=300,
                      limits=httpx.Limits(max_connections=16))


def ask(args):
    q, k = args
    r = client.post("/v2/namespaces/nq/query", json={
        "rank_by": ["vector", "ANN", q.tolist()], "top_k": k, "include_attributes": False})
    r.raise_for_status()
    return [int(row["id"]) for row in r.json()["rows"]]


for k in (10, 100):
    with concurrent.futures.ThreadPoolExecutor(16) as pool:
        rows = list(pool.map(ask, ((q, k) for q in queries)))
    m = np.full((len(rows), k), -1, dtype="<i4")
    for i, r in enumerate(rows):
        m[i, :len(r)] = r
    np.save(out / f"ivf{probes}_k{k}.npy", m)
    print("wrote", k, m.shape, flush=True)
