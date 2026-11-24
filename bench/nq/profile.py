r"""Profile a warm, read-only NQ server on Linux, with exact recall and owned child cleanup.

Run from the repository root under timeout. Example:
  timeout 600 uv run --project bench/nq python bench/nq/profile.py \
    --binary target/release/blowfish --store .scratch/nq/store \
    --data .scratch/nq/data --out .scratch/nq/profile/p64.json \
    --probes 64 --quality --warm 3452 --clients 4 --perf stat \
    --perf-path /path/to/the/kernel-matching/perf

Outputs must live under .scratch/. The JSON summary is accompanied by result IDs, server logs,
and optional perf counters or samples. Repeat the same warmed query IDs to measure cache hits;
wall time on a shared machine is not a throughput claim. See measure.py for throughput.
"""
import argparse
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import signal
import shutil
import sys
import subprocess
import threading
import time

import numpy as np

p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
p.add_argument('--binary', required=True)
p.add_argument('--store', required=True)
p.add_argument('--data', required=True)
p.add_argument('--out', required=True)
p.add_argument('--probes', type=int, default=64)
p.add_argument('--port', type=int, default=18442)
p.add_argument('--count', type=int, default=1000)
p.add_argument('--clients', type=int, default=16)
p.add_argument('--warm', type=int, default=1000)
p.add_argument('--trace', action='store_true')
p.add_argument('--quality', action='store_true')
p.add_argument('--perf', choices=['stat', 'record'])
p.add_argument('--perf-path', default=shutil.which('perf'))
p.add_argument('--cpus', default='6-7')
p.add_argument('--threads', type=int, default=2)
a = p.parse_args()
signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
out = Path(a.out)
if not out.resolve().is_relative_to(Path(__file__).resolve().parents[2] / '.scratch'):
    p.error('--out must be under the repository .scratch/ directory')
if min(a.clients, a.count, a.threads) < 1 or a.warm < 0:
    p.error('clients, count, and threads must be positive; warm must be nonnegative')
if a.perf and not a.perf_path:
    p.error('--perf requires a working perf executable')
out.parent.mkdir(parents=True, exist_ok=True)
q = np.load(Path(a.data) / 'queries.npy')
truth = np.load(Path(a.data) / 'truth.npy')
if a.warm > len(q):
    p.error('--warm cannot exceed the number of queries')
bodies = [
    json.dumps({'rank_by': ['vector', 'ANN', v.tolist()], 'top_k': 10,
                'include_attributes': ['id']}).encode()
    for v in q
]
local = threading.local()


def query(i):
    if not hasattr(local, 'conn'):
        local.conn = http.client.HTTPConnection('127.0.0.1', a.port, timeout=60)
    c = local.conn
    start = time.perf_counter()
    c.request('POST', '/v2/namespaces/nq/query', bodies[i], {'Content-Type': 'application/json'})
    r = c.getresponse()
    raw = r.read()
    if r.status != 200:
        raise RuntimeError((r.status, raw[:200]))
    return i, (time.perf_counter() - start) * 1000, raw

env = dict(os.environ, RAYON_NUM_THREADS=str(a.threads),
           TOKIO_WORKER_THREADS=str(a.threads), TMPDIR=str(out.parent / 'tmp'))
Path(env['TMPDIR']).mkdir(exist_ok=True)
if a.trace:
    env['BF_TRACE_STAGES'] = '1'
log = open(str(out) + '.server.log', 'wb', buffering=0)
command = [
    'taskset', '-c', a.cpus, 'nice', '-n', '19', a.binary, 'serve',
    '--listen', f'127.0.0.1:{a.port}',
    '--storage-url', Path(a.store).resolve().as_uri(),
    '--indexing', 'false', '--read-only', '--recall-sample-rate', '0',
    '--ann-nprobe', str(a.probes), '--cache-memory-bytes', '4294967296',
    '--cache-disk-bytes', '0',
]
server = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
profiler = None
perf_log = None
try:
    deadline = time.monotonic() + 30
    while True:
        if server.poll() is not None:
            raise RuntimeError('server exited')
        try:
            c = http.client.HTTPConnection('127.0.0.1', a.port, timeout=1)
            c.request('GET', '/')
            c.getresponse().read()
            c.close()
            break
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(.2)
    with concurrent.futures.ThreadPoolExecutor(a.clients) as pool:
        list(pool.map(query, range(a.warm)))
        log.write(b'NQ_MEASURE_BEGIN\n')
        if a.perf:
            perf = a.perf_path
            args = [perf, a.perf, '-p', str(server.pid)]
            if a.perf == 'stat':
                args += ['-e', 'instructions:u,cycles:u,task-clock', '-x', ',',
                         '-o', str(out) + '.perf']
            else:
                args += ['-F', '199', '-g', '--call-graph', 'dwarf,8192', '-o', str(out) + '.perf.data']
            perf_log = open(str(out) + '.perf.log', 'wb')
            profiler = subprocess.Popen(
                args, env=dict(os.environ, PERF_BUILDID_DIR=str(out.parent / 'buildid')),
                stdout=subprocess.DEVNULL, stderr=perf_log,
            )
            time.sleep(.3)
            if profiler.poll() is not None:
                raise RuntimeError('perf exited before measurement; inspect the .perf.log output')
        started = time.perf_counter()
        count = len(q) if a.quality else a.count
        results = list(pool.map(query, [i % len(q) for i in range(count)]))
        elapsed = time.perf_counter() - started
        log.write(b'NQ_MEASURE_END\n')
        if profiler:
            profiler.send_signal(signal.SIGINT)
            profiler.wait(timeout=15)
            profiler = None
    latencies = [r[1] for r in results]
    answer = {
        'binary': a.binary, 'cpus': a.cpus, 'threads': a.threads,
        'warm_queries': a.warm, 'probes': a.probes, 'queries': count,
        'clients': a.clients, 'seconds': elapsed, 'qps': count / elapsed,
        'latency_ms': {f'p{p}': float(np.percentile(latencies, p)) for p in [50, 90, 99]},
    }
    decoded = [json.loads(r[2]) for r in results]
    answer['cache_hit_ratio'] = float(np.mean([
        r['performance']['cache_hit_ratio'] for r in decoded
    ]))
    answer['exhaustive_search_count'] = sorted({
        r['performance']['exhaustive_search_count'] for r in decoded
    })
    answer['namespace_rows'] = sorted({
        r['performance']['approx_namespace_size'] for r in decoded
    })
    ids = [[int(row['id']) for row in r['rows']] for r in decoded]
    answer['recall10'] = float(np.mean([
        len(set(got) & set(truth[i % len(q), :10].tolist())) / 10
        for i, got in enumerate(ids)
    ]))
    status = Path(f'/proc/{server.pid}/status').read_text()
    peak_kib = next(line.split()[1] for line in status.splitlines() if line.startswith('VmHWM:'))
    answer['peak_rss_bytes'] = int(peak_kib) * 1024
    out.write_text(json.dumps(answer) + '\n')
    Path(str(out) + '.ids').write_text(json.dumps(ids))
    print(json.dumps(answer), flush=True)
finally:
    if profiler and profiler.poll() is None:
        profiler.send_signal(signal.SIGINT)
        profiler.wait(timeout=15)
    if server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=20)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=10)
    if perf_log:
        perf_log.close()
    log.close()
