# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Round C44 step 2: replay a 10M cold trace's rerank wave with each layout's measured S3 latency.

usage: c44_model.py RES.jsonl TRACE.jsonl [TRACE.jsonl ...]
RES.jsonl is c44_s3lat.py's output; each TRACE is a cold_ab.py rows.jsonl (BF_TRACE_READS and BF_TRACE_STAGES).

Per query: the rerank wave is the `slabs` reads (the trace's offset and limit are the byte range's start and end) that start after the `scan` stage ends and end by the `rerank vectors` stage's end.
Modeled query time = measured time - (rerank-vectors end - scan end) + modeled wave, where the modeled wave is
(rerank-vectors end - last observed byte) + max over the layout's GETs of (the read's start offset from the scan end + a latency draw).
A latency draw resamples one whole measured wave of 35 reads (so the in-wave correlation stays), taking the draws of several waves when
a query has more than 35 GETs. Layouts turn each traced read into GETs: ranges (the traced reads as they are), 16 KiB objects (one GET per
16 KiB page), 64 KiB and 256 KiB objects (one GET per distinct group touched). Every draw is last-byte time (`tot`).
"""
import json, sys
import numpy as np

KiB = 1024
PAGE = 16 * KiB
rng = np.random.default_rng(44)
res = [json.loads(l) for l in open(sys.argv[1])]


def waves(label, layout, delta=None):
    rows = [r["tot"] for r in res if r["label"] == label and r["layout"] == layout and r.get("delta") == delta]
    return [np.array(rows[i:i + 35]) for i in range(0, len(rows), 35) if rows[i:i + 35]]


def draw(ws, n):
    out = []
    while len(out) < n:
        out.extend(ws[rng.integers(len(ws))])
    return np.array(out[:n])[rng.permutation(n)] if n else np.array([])


# layout name -> (label, measured layout, group bytes or None)
AGES = {
    "1 min": {"ranges in 64 MiB objects": ("fresh-1min", "big16k", None), "16 KiB objects": ("fresh-1min", "s16k", PAGE),
              "64 KiB objects": ("fresh-1min", "s64k", 64 * KiB), "256 KiB objects": ("fresh-1min", "s256k", 256 * KiB)},
}
for lab in sorted({r["label"] for r in res}):
    if lab.startswith("age-"):
        AGES[lab[4:]] = {"ranges in 64 MiB objects": (lab, "big16k", None), "16 KiB objects": (lab, "s16k", PAGE),
                         "64 KiB objects": (lab, "s64k", 64 * KiB), "256 KiB objects": (lab, "s256k", 256 * KiB)}
AGES["4 days (fixtures)"] = {"ranges in 64 MiB objects": ("aged-4d", "aged-big16k", None), "16 KiB objects": ("aged-4d", "aged-whole-8-32K", PAGE),
                             "64 KiB objects": ("aged-4d", "aged-whole-32-128K", 64 * KiB), "256 KiB objects": ("aged-4d", "aged-whole-128K-1M", 256 * KiB)}


def load(path):
    out = []
    for line in open(path):
        r = json.loads(line)
        if r["arm"] != "base":
            continue
        t0 = r["request_start_us"]
        st = {s["stage"]: s["end_us"] for s in r["stage_timeline"]}
        scan, rv = st["scan"], st["rerank vectors"]
        wave = [e for e in r["read_timeline"] if e["kind"] == "slabs" and e["start_us"] >= scan - 1000 and e["end_us"] <= rv + 1000]
        out.append(dict(ms=r["ms"], scan=scan, rv=rv, wave=wave))
    return out


def gets_for(wave, group):
    """List of (start offset from scan end in ms is added by the caller, bytes) for the layout."""
    if group is None:
        return [(e["start_us"], e["bytes"]) for e in wave]
    seen = {}
    for e in wave:
        for p in range(e["offset"] // PAGE, (e["limit"] - 1) // PAGE + 1):
            k = (e["object"], (p * PAGE) // group)
            seen[k] = min(seen.get(k, 1 << 62), e["start_us"])
    return [(s, group) for s in seen.values()]


def model(queries, spec, reps=20):
    label, layout, group = spec
    ws = waves(label, layout)
    new, gets, byt, wv = [], [], [], []
    for q in queries:
        g = gets_for(q["wave"], group)
        gets.append(len(g)); byt.append(sum(b for _, b in g))
        if not g:
            new.append([q["ms"]] * reps); continue
        last_end = max(e["end_us"] for e in q["wave"])
        tail = (q["rv"] - last_end) / 1000
        base_ms = q["ms"] - (q["rv"] - q["scan"]) / 1000
        starts = np.array([(s - q["scan"]) / 1000 for s, _ in g])
        w = [tail + (starts + draw(ws, len(g))).max() for _ in range(reps)]
        wv.append(w)
        new.append([base_ms + x for x in w])
    flat = np.concatenate(new)
    return np.percentile(flat, [50, 90]), np.mean(gets), np.mean(byt) / 1e6, np.percentile(np.concatenate(wv), [50, 90])


def observed(queries):
    ms = [q["ms"] for q in queries]
    span = [(max(e["end_us"] for e in q["wave"]) - q["scan"]) / 1000 for q in queries if q["wave"]]
    return np.percentile(ms, [50, 90]), np.percentile(span, [50, 90]), np.mean([len(q["wave"]) for q in queries])


PUT_USD_PER_K, ROWS_PER_PAGE = 0.005, 8  # 1,024-d f16 = 2 KiB per row, 8 rows per 16 KiB page
per_million_pages = 1_000_000 / ROWS_PER_PAGE
OBJS = {"ranges in 64 MiB objects": 1_000_000 * 2048 / (64 * 2**20), "16 KiB objects": per_million_pages,
        "64 KiB objects": per_million_pages / 4, "256 KiB objects": per_million_pages / 16}
for path in sys.argv[2:]:
    qs = load(path)
    (p50, p90), (s50, s90), nreads = observed(qs)
    print(f"\n### {path}: {len(qs)} queries; traced p50 {p50:.0f} / p90 {p90:.0f} ms; traced wave (scan end to last byte) p50 {s50:.0f} / p90 {s90:.0f} ms; {nreads:.0f} reads per wave\n")
    print("| Age | Layout | Modeled wave p50 / p90, ms | Modeled query p50 / p90, ms | Ratio to traced (p50, p90) | GETs per query | MB per query | Objects per million rows | PUT cost per million rows |")
    print("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for age, layouts in AGES.items():
        for name, spec in layouts.items():
            if not waves(spec[0], spec[1]):
                continue
            (m50, m90), g, mb, (w50, w90) = model(qs, spec)
            print(f"| {age} | {name} | {w50:.0f} / {w90:.0f} | {m50:.0f} / {m90:.0f} | {m50/p50:.2f} / {m90/p90:.2f} | {g:.0f} | {mb:.1f} | {OBJS[name]:,.0f} | ${OBJS[name]*PUT_USD_PER_K/1000:.4f} |")
