"""Run one query-only perf measurement. Run with uv; outputs must be under .scratch/.

Usage: measure.py OUTPUT_STEM CPU_LIST THREADS BINARY MODE DATA ARTIFACT [mode arguments...]
Set G3_QUIET_GRANTED=1 only after receiving the coordinator's q/s window.
"""
import os
from pathlib import Path
import subprocess
import sys

stem, cpus, threads, binary, *args = sys.argv[1:]
stem = Path(stem).resolve()
if '.scratch' not in stem.parts:
    raise SystemExit('outputs must be under .scratch/')
quiet = Path(__file__).resolve().parents[4].parent / 'QUIET'
if quiet.exists() and os.environ.get('G3_QUIET_GRANTED') != '1':
    raise SystemExit(f'quiet window active: {quiet.read_text()}')
stem.parent.mkdir(parents=True, exist_ok=True)
control, ack = (Path(str(stem) + suffix) for suffix in ['.ctl', '.ack'])
try:
    for fifo in (control, ack):
        os.mkfifo(fifo)
    env = dict(os.environ, RAYON_NUM_THREADS=threads, G3_PERF_CTL=str(control), G3_PERF_ACK=str(ack))
    command = ['nice', '-n', '19', 'taskset', '-c', cpus, os.environ.get('G3_PERF_BIN', 'perf'), 'stat', '-D', '-1',
               '--control', f'fifo:{control},{ack}', '-e', 'instructions,cycles', '-x', ',',
               '-o', str(stem) + '.perf', binary, *args]
    with Path(str(stem) + '.log').open('w') as log:
        subprocess.run(command, env=env, stdout=log, stderr=log, check=True)
    print(Path(str(stem) + '.log').read_text())
    print(Path(str(stem) + '.perf').read_text())
finally:
    for fifo in (control, ack):
        fifo.unlink(missing_ok=True)
