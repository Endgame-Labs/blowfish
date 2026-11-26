# /// script
# dependencies = ["numpy==2.5.4"]
# ///
"""Round G11: Qdrant 1.20.0 on the M4 for the head-to-head.

Usage:
  g11_qdrant.py RUN_DIR prepare            start Qdrant on RUN_DIR/qdrant/store, load NQ (g3/qdrant_load.py), stop it
  g11_qdrant.py RUN_DIR sweep K EF [EF...] start Qdrant, then g11_http.py (16 client processes) at each ef
                                           (ef 0 omits hnsw_ef: Qdrant's default); results RUN_DIR/qdrant-k{K}-ef{EF}.json
RUN_DIR holds fixture/data, qdrant/qdrant (the official macOS arm64 1.20.0 binary), and src/ (a repository copy).
The caller owns RUN_DIR and its cleanup; the Qdrant server is always stopped.
"""
import http.client
import os
import pathlib
import signal
import subprocess
import sys
import time

run = pathlib.Path(sys.argv[1]).resolve()
mode, rest = sys.argv[2], sys.argv[3:]
here = pathlib.Path(__file__).resolve().parent
q = run / "qdrant"
(q / "config.yaml").write_text(
    f"telemetry_disabled: true\nstorage:\n  storage_path: {q}/store\n"
    "service:\n  host: 127.0.0.1\n  http_port: 18446\n  grpc_port: 18447\n")
server = None
try:
    log = (q / "server.log").open("ab")
    server = subprocess.Popen([str(q / "qdrant"), "--config-path", str(q / "config.yaml")],
                              stdout=log, stderr=log, stdin=subprocess.DEVNULL, start_new_session=True)
    for _ in range(300):
        if server.poll() is not None:
            raise SystemExit("qdrant exited")
        try:
            c = http.client.HTTPConnection("127.0.0.1", 18446, timeout=5)
            c.request("GET", "/readyz")
            r = c.getresponse()
            r.read()
            if r.status == 200:
                break
        except OSError:
            time.sleep(1)
    if mode == "prepare":
        subprocess.run(["uv", "run", "--no-project", "--with", "numpy==2.5.4", "--with", "qdrant-client==1.20.0",
                        "python", str(run / "src/crates/bf-vector/examples/g3/qdrant_load.py"),
                        str(run / "fixture/data"), "18446", "18447"], check=True)
        print("QDRANT_FIXTURE_COMPLETE", flush=True)
    else:
        k, efs = rest[0], rest[1:]
        for ef in efs:
            e = dict(os.environ, G3_CLIENT_PROCESSES="16", G3_CLIENT_THREADS="1", G8_K=k)
            subprocess.run(["uv", "run", "--python", "3.12", "--no-project", "--with", "numpy", "python",
                            str(here / "g11_http.py"), str(run / "fixture/data"), "qdrant", "18446", ef,
                            os.environ.get("G8_REPEATS", "3"), str(run / f"qdrant-k{k}-ef{ef}"),
                            str(server.pid)], env=e, check=True)
finally:
    if server is not None and server.poll() is None:
        os.killpg(server.pid, signal.SIGTERM)
        try:
            server.wait(timeout=60)
        except subprocess.TimeoutExpired:
            os.killpg(server.pid, signal.SIGKILL)
