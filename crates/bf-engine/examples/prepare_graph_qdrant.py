"""Prepare the persisted Qdrant 1.20 fixture used by graph_compare.py.

Run from an owned measurement directory containing src/, .scratch/fixture/data,
and the official native .scratch/qdrant/qdrant binary. This calls the existing G3
loader with float16 RAM vectors, M=16, ef_construct=100, and one merged segment.
The server is stopped after indexing; the caller records and removes all artifacts.
"""

import http.client, json, pathlib, subprocess, time, os

root = pathlib.Path.cwd()
out = root / ".scratch/qdrant"
out.mkdir(exist_ok=True)
assert (
    subprocess.check_output([str(out / "qdrant"), "--version"]).decode().strip()
    == "qdrant 1.20.0"
)
config = out / "config.yaml"
config.write_text(
    f"telemetry_disabled: true\nstorage:\n  storage_path: {out}/store\nservice:\n  host: 127.0.0.1\n  http_port: 18446\n  grpc_port: 18447\n"
)
with (out / "server.log").open("ab") as log:
    p = subprocess.Popen(
        [str(out / "qdrant"), "--config-path", str(config)],
        stdout=log,
        stderr=log,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    (out / "pid").write_text(str(p.pid))
    try:
        for _ in range(120):
            if p.poll() is not None:
                raise RuntimeError("qdrant exited")
            try:
                c = http.client.HTTPConnection("127.0.0.1", 18446, timeout=5)
                c.request("GET", "/readyz")
                r = c.getresponse()
                r.read()
                c.close()
                if r.status == 200:
                    break
            except OSError:
                pass
            time.sleep(1)
        subprocess.run(
            [
                "uv",
                "run",
                "--no-project",
                "--with",
                "numpy==2.5.4",
                "--with",
                "qdrant-client==1.20.0",
                "python",
                "src/crates/bf-vector/examples/g3/qdrant_load.py",
                ".scratch/fixture/data",
                "18446",
                "18447",
            ],
            check=True,
        )
    finally:
        p.terminate()
        try:
            p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
        (out / "pid").unlink(missing_ok=True)
print("QDRANT_FIXTURE_COMPLETE", flush=True)
