"""Round G11 variant of g8_http.py: adds per-request latency (p50/p90/p99), process CPU seconds, and a Qdrant default-ef mode (ef 0 omits hnsw_ef).

Round G8 variant of g3/http_measure.py: top_k from env G8_K (default 100); macOS counters.

Same-host HTTP quality and throughput, with disjoint client/server CPUs.

Run with uv --with numpy. DATA contains queries.npy and truth.npy. Every quality pass
uses all queries at top_k=10. Timed passes reuse pre-encoded requests and consume full
responses. Eight client processes, each with eight connections, occupy CPUs 24-31;
run the server on CPUs 0-23. Perf attaches to the server only, after warm-up.
Usage: http_measure.py DATA bf|qdrant PORT EF REPEATS OUTPUT_STEM SERVER_PID
"""
import concurrent.futures
import http.client
import json
import multiprocessing as mp
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time

import numpy as np


def mac_counters(pid):
    # Stable RUSAGE_INFO_V4 ABI from Apple's sys/resource.h: UUID (16 bytes),
    # followed by u64 fields. This reads all threads of the target task.
    import ctypes
    values = (ctypes.c_uint64 * 64)()
    library = ctypes.CDLL('/usr/lib/libproc.dylib')
    library.proc_pid_rusage.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_void_p)
    if library.proc_pid_rusage(pid, 4, ctypes.byref(values)) != 0:
        return None
    return {'instructions': values[31], 'cycles': values[32],
            'rss_bytes': values[8], 'peak_footprint_bytes': values[30],
            'cpu_user_ns': values[2], 'cpu_system_ns': values[3]}


def request(connection, body):
    connection.request('POST', ROUTE, body, {'content-type': 'application/json'})
    response = connection.getresponse()
    data = response.read()
    if response.status != 200:
        raise RuntimeError((response.status, data[:300]))
    return data


def worker(process, repeats, barrier):
    # Round GM1: G3_CLIENT_CPU_BASE=-1 skips pinning (a 10-CPU guest has no CPUs 24 and up).
    base = int(os.environ.get("G3_CLIENT_CPU_BASE", "24"))
    if base >= 0 and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {base + process})
    count = len(BODIES) * repeats

    def thread(index):
        connection = http.client.HTTPConnection('127.0.0.1', PORT, timeout=120)
        connection.connect()
        barrier.wait()
        lat = []
        for i in range(process * THREADS + index, count, PROCESSES * THREADS):
            t = time.perf_counter()
            request(connection, BODIES[i % len(BODIES)])
            lat.append(time.perf_counter() - t)
        connection.close()
        return lat

    with concurrent.futures.ThreadPoolExecutor(THREADS) as executor:
        return [x for part in executor.map(thread, range(THREADS)) for x in part]


def run(repeats):
    barrier = mp.Barrier(PROCESSES * THREADS + 1)
    result = mp.Queue()

    def target(i):
        try:
            result.put((True, worker(i, repeats, barrier)))
        except Exception as e:
            result.put((False, repr(e)))
            barrier.abort()

    processes = [mp.Process(target=target, args=(i,)) for i in range(PROCESSES)]
    for p in processes:
        p.start()
    start = time.perf_counter()
    barrier.wait(timeout=120)
    start = time.perf_counter()
    results = [result.get(timeout=600) for _ in processes]
    elapsed = time.perf_counter() - start
    for p in processes:
        p.join()
    if any(not ok for ok, _ in results):
        raise RuntimeError(results)
    lat = np.sort(np.array([x for _, part in results for x in part])) * 1000
    count = len(lat)
    pct = lambda p: float(lat[min(count - 1, int(p * count))])
    return {'queries': count, 'seconds': elapsed, 'qps': count / elapsed,
            'p50_ms': pct(0.50), 'p90_ms': pct(0.90), 'p99_ms': pct(0.99)}


if __name__ == '__main__':
    data, engine, port, ef, repeats, stem, pid = sys.argv[1:]
    mp.set_start_method("fork")
    PROCESSES = int(os.environ.get("G3_CLIENT_PROCESSES", "8"))
    THREADS = int(os.environ.get("G3_CLIENT_THREADS", "8"))
    PORT = int(port)
    ef, repeats, pid = int(ef), int(repeats), int(pid)
    stem = Path(stem).resolve()
    stem.parent.mkdir(parents=True, exist_ok=True)
    queries = np.load(Path(data) / 'queries.npy')
    K = int(os.environ.get('G8_K', '100'))
    ROUTE = '/v2/namespaces/nq/query' if engine == 'bf' else '/collections/nq/points/search'
    # G12_MIX=1: one query in ten asks for 100 rows, the rest for K (a mixed load for p99).
    KS = [100 if os.environ.get('G12_MIX') and i % 10 == 0 else K for i in range(len(queries))]
    truth = np.load(Path(data) / 'truth.npy')
    BODIES = [json.dumps(
        {'rank_by': ['vector', 'ANN', q.tolist()], 'top_k': KS[i], 'include_attributes': False}
        if engine == 'bf' else
        {'vector': q.tolist(), 'limit': KS[i], 'with_payload': False, 'with_vector': False,
         **({'params': {'hnsw_ef': ef}} if ef else {})}, separators=(',', ':')).encode()
        for i, q in enumerate(queries)]
    connection = http.client.HTTPConnection('127.0.0.1', PORT, timeout=120)
    overlap = 0
    for i, body in enumerate(BODIES):
        answer = json.loads(request(connection, body))
        rows = answer['rows'] if engine == 'bf' else answer['result']
        overlap += len({int(row['id']) for row in rows} & set(map(int, truth[i][:KS[i]])))
    connection.close()
    quality = overlap / sum(KS)
    print(json.dumps({'k': K, 'recall_at_k': quality, 'warmup': run(1)}), flush=True)
    if sys.platform == 'darwin':
        before = mac_counters(pid)
        measured = run(repeats)
        after = mac_counters(pid)
        measured['cpu_seconds_per_query'] = ((after['cpu_user_ns'] + after['cpu_system_ns'])
                                             - (before['cpu_user_ns'] + before['cpu_system_ns'])) / 1e9 / measured['queries']
        if before is not None and after is not None:
            measured.update(after)
            for key in ('instructions', 'cycles'):
                measured[key] -= before[key]
                measured[key + '_per_query'] = measured[key] / measured['queries']
        measured.update(recall_at_k=quality, engine=engine, ef=ef, server_pid=pid)
        Path(str(stem) + '.json').write_text(json.dumps(measured, indent=2) + '\n')
        print(json.dumps(measured), flush=True)
        raise SystemExit(0)
    if os.environ.get('G11_NO_PERF'):
        # Round GM1: no perf in the Lima guest; the same timed passes without instruction counters.
        measured = run(repeats)
        measured.update(recall_at_k=quality, engine=engine, ef=ef, server_pid=pid)
        Path(str(stem) + '.json').write_text(json.dumps(measured, indent=2) + '\n')
        print(json.dumps(measured), flush=True)
        raise SystemExit(0)
    control, ack = [Path(str(stem) + suffix) for suffix in ('.ctl', '.ack')]
    perf = None
    try:
        for f in (control, ack):
            os.mkfifo(f)
        perf = subprocess.Popen(['perf', 'stat', '-p', str(pid), '-D', '-1', '--control',
                                 f'fifo:{control},{ack}', '-e', 'instructions,cycles', '-x', ',',
                                 '-o', str(stem) + '.perf'])
        with control.open('w') as ctl, ack.open('r') as response:
            ctl.write('enable\n'); ctl.flush(); response.readline()
            measured = run(repeats)
            ctl.write('disable\n'); ctl.flush(); response.readline()
        perf.send_signal(signal.SIGINT)
        perf.wait(timeout=10)
        if perf.returncode not in (0, -signal.SIGINT, 128 + signal.SIGINT):
            raise RuntimeError('perf failed')
        status = Path(f'/proc/{pid}/status').read_text()
        measured['rss_bytes'] = int(next(line for line in status.splitlines() if line.startswith('VmRSS:')).split()[1]) * 1024
        measured.update(recall_at_k=quality, engine=engine, ef=ef, server_pid=pid,
                        client_cpu_seconds=resource.getrusage(resource.RUSAGE_CHILDREN).ru_utime)
        Path(str(stem) + '.json').write_text(json.dumps(measured, indent=2) + '\n')
        print(json.dumps(measured), flush=True)
    finally:
        if perf is not None and perf.poll() is None:
            perf.terminate(); perf.wait()
        for f in (control, ack):
            f.unlink(missing_ok=True)
