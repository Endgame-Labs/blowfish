"""Millisecond timeline of one cold query from a server log (BF_TRACE_READS=1, BF_TRACE_STAGES=1).

Usage: python3 timeline.py SERVER_LOG [--all]
Prints every read (start and end offset in ms from the first read, bytes, object kind, range), the stage lines
(`STAGE`, `QUERY`, `T` lines with epoch-ms stamps) and a per-wave summary. Reads that begin after every earlier read
has finished start a new wave. `--all` lists each read; the default lists the 40 slowest plus waves.
"""
import re, sys, collections

reads, marks = [], []
for line in open(sys.argv[1]):
    if line.startswith("TRACE_READ "):
        _, path, a, b, n, end, el = line.split()
        reads.append((int(end) - int(el), int(end), int(n), path, int(a), int(b)))
    elif line.startswith("T "):  # T <label> <epoch_ms>
        _, label, ms = line.split()[:3]
        marks.append((int(ms), label))
reads.sort()
if not reads:
    sys.exit("no reads")
t0 = min([reads[0][0]] + [m[0] for m in marks])
kind = lambda p: re.sub(r"[0-9A-Z]{26}|[0-9a-f]{8}-[0-9a-f-]{27}|\d{6,}", "*", p).split("ns/")[-1]
print(f"{len(reads)} reads, {sum(r[2] for r in reads)/1e6:.2f} MB")
for ms, label in sorted(marks):
    print(f"  mark +{ms - t0:5d} ms  {label}")
waves, cur, cur_end = [], [], 0
for r in reads:
    if cur and r[0] >= cur_end:
        waves.append(cur); cur, cur_end = [], 0
    cur.append(r); cur_end = max(cur_end, r[1])
waves.append(cur)
for i, w in enumerate(waves):
    el = sorted(r[1] - r[0] for r in w)
    kinds = collections.Counter(kind(r[3]).split("/")[-1].split(".")[-1] if "." in kind(r[3]) else kind(r[3]) for r in w)
    print(f"wave {i}: {len(w):4d} req {sum(r[2] for r in w)/1e6:6.2f} MB  +{min(r[0] for r in w)-t0:5d}..{max(r[1] for r in w)-t0:5d} ms  "
          f"req p50 {el[len(el)//2]} max {el[-1]}  {dict(kinds.most_common(5))}")
sel = reads if "--all" in sys.argv else sorted(reads, key=lambda r: r[0] - r[1])[:40]
for r in sorted(sel):
    print(f"  +{r[0]-t0:5d}..{r[1]-t0:5d}  {r[1]-r[0]:4d} ms  {r[2]:8d} B  {kind(r[3])} [{r[4]}..{r[5]}]")
