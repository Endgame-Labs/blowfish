"""Count one Linux filtered_bench arm after 2,000 prewarm requests.

The supplied storage config sets the cache budgets. Both the result prefix and all temporary
FIFOs must be under the repository .scratch/. Hardware counters cover 1,000 serial requests
and 2,000 requests at sixteen clients. Process CPU ticks independently check thread coverage.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import signal
import sys
import threading


def measure(binary, config, workload, output, probes):
    output.parent.mkdir(parents=True, exist_ok=True)
    control, ack = Path(str(output) + ".control"), Path(str(output) + ".ack")
    for path in [control, ack]:
        path.unlink(missing_ok=True)
        os.mkfifo(path)
    env = dict(os.environ, BF_ANN_NPROBE=str(probes), BF_ANN_RERANK_FACTOR="32",
               BF_DECODED_CACHE_BYTES="4108168192", BF_RETENTION_SECS="3600",
               RAYON_NUM_THREADS="6", TOKIO_WORKER_THREADS="6",
               BF_FILTER_BENCH_PERF_CONTROL=str(control), BF_FILTER_BENCH_PERF_ACK=str(ack))
    clocks, errors = {}, []

    def ticks():
        pid = Path(str(output) + ".pid").read_text().strip()
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return int(fields[11]) + int(fields[12])

    try:
        command = ["perf", "stat", "--delay=-1", f"--control=fifo:{control},{ack}",
                   "-e", "instructions:u,cycles:u,task-clock", "-x", ",", "-o", str(output) + ".perf",
                   "--", sys.executable, str(Path(__file__).resolve()), "__exec", str(output) + ".pid",
                   "taskset", "-c", "0-5", str(binary), str(config), "warm", str(output) + ".json", str(workload)]
        with open(str(output) + ".log", "w") as log:
            process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True, bufsize=1, start_new_session=True)

            def capture():
                try:
                    for line in process.stdout:
                        log.write(line)
                        log.flush()
                        if line.strip() == "Events enabled":
                            clocks["start"] = ticks()
                        elif line.strip() == "Events disabled" and "start" in clocks:
                            clocks["end"] = ticks()
                except Exception as error:
                    errors.append(error)

            reader = threading.Thread(target=capture, daemon=True)
            reader.start()
            try:
                status = process.wait(timeout=900)
            except BaseException:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
            reader.join(timeout=10)
            if status:
                raise subprocess.CalledProcessError(status, command)
        if errors:
            raise RuntimeError("process CPU observation failed") from errors[0]
        result = json.loads(Path(str(output) + ".json").read_text())
        if result["counted_queries"] != 3000:
            raise RuntimeError("expected exactly 3,000 measured requests")
        events, coverage = {}, []
        for line in Path(str(output) + ".perf").read_text().splitlines():
            fields = line.split(",")
            if len(fields) < 5:
                continue
            try:
                events[fields[2]] = float(fields[0])
                coverage.append(float(fields[4]))
            except ValueError:
                continue
        if events.get("instructions:u", 0) <= 0 or events.get("task-clock", 0) <= 0:
            raise RuntimeError("hardware counters were disabled or unavailable")
        if not coverage or min(coverage) < 99.9:
            raise RuntimeError(f"insufficient PMU coverage: {coverage}")
        cpu = events["task-clock"] / 3000
        observed = (clocks["end"] - clocks["start"]) / os.sysconf("SC_CLK_TCK") * 1000 / 3000
        if abs(observed / cpu - 1) >= 0.02:
            raise RuntimeError(f"process CPU disagrees with perf: {observed} vs {cpu} ms/query")
        summary = {"cpu_ms": cpu, "process_cpu_ms": observed,
                   "instructions": events["instructions:u"] / 3000,
                   "cycles": events["cycles:u"] / 3000, "coverage_percent": min(coverage),
                   "qps16": result["qps16"],
                   "rows": result["concurrent_diagnostics"]["ann_rows_per_query"]}
        Path(str(output) + ".metrics.json").write_text(json.dumps(summary))
        return summary
    finally:
        control.unlink(missing_ok=True)
        ack.unlink(missing_ok=True)


if __name__ == "__main__":
    if sys.argv[1:2] == ["__exec"]:
        Path(sys.argv[2]).write_text(str(os.getpid()))
        os.execvpe(sys.argv[3], sys.argv[3:], os.environ)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--probes", type=int, default=48)
    args = parser.parse_args()
    if not args.out.resolve().is_relative_to(Path(__file__).resolve().parents[3] / ".scratch"):
        parser.error("--out must be under the repository .scratch/")
    print(json.dumps(measure(args.binary, args.config, args.workload, args.out, args.probes)), flush=True)
