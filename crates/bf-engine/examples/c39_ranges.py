# /// script
# requires-python = ">=3.12"
# dependencies = ["numpy==2.5.4"]
# ///
"""Exact-range scan reads (Round C39, estimate only, no format change).

Usage: c39_ranges.py DUMP.bin [--label NAME]
From a c39_dump.py dump (leaf byte ranges, and for each matching row its leaf and ordinal), counts the bytes and GETs of a scan read path that
fetches only the byte ranges it needs, merging only gaps under G bytes and not rounding to 256 KiB cache blocks, for two read sets:
  leaf    every probed leaf's blob and whole code range (what the scan reads today, minus the cache's rounding);
  blocks  every probed leaf's blob, and only the 32-row code blocks holding a matching row, each with its four factor slices
          (128 bytes per factor array per block).
Today's modeled bytes and GETs (256 KiB blocks, 32-block GETs) come from the same ranges and are checked against the trace.
Latency is a model: first-byte latency L plus the larger of the bytes over the NIC rate and the GET rounds at the fetch concurrency.
"""

import argparse
import json

import numpy as np

import c39_sim as sim

MAX_GET = 8 << 20  # BLOB_MERGE_MAX
FIRST_BYTE_MS = 60.0  # a cold first read, p50 (C34, C35, C36)
NIC_BYTES_PER_MS = 2.7e6  # 956 MB scanned in about 356 ms (C38)
STREAM_BYTES_PER_MS = 1e5  # about 100 MB/s for one GET stream
CONCURRENCY = 512  # fetch_concurrency of the benchmark servers


def merge(obj, start, end, gap):
    """Bytes and GETs of ranges merged where the gap to the previous is at most `gap`, one GET per at most 8 MiB."""
    if len(start) == 0:
        return 0, 0
    pos = obj.astype(np.int64) << 44
    st, en = pos + start.astype(np.int64), pos + end.astype(np.int64)
    o = np.argsort(st, kind="stable")
    st, en = st[o], en[o]
    run = np.maximum.accumulate(en)
    new = np.ones(len(st), dtype=bool)
    new[1:] = st[1:] > run[:-1] + gap
    first = np.flatnonzero(new)
    length = np.maximum.reduceat(en, first) - st[first]
    return int(length.sum()), int(np.ceil(length / MAX_GET).sum())


def latency_ms(nbytes, gets):
    rounds = max(1, int(np.ceil(gets / CONCURRENCY)))
    per_get = FIRST_BYTE_MS + (nbytes / max(gets, 1)) / STREAM_BYTES_PER_MS
    return FIRST_BYTE_MS + max(nbytes / NIC_BYTES_PER_MS, rounds * per_get)


def ranges_for(rec, mode):
    lv = rec["leaves"]
    rng = lv["range"].astype(np.int64)
    n = lv["rows"].astype(np.int64)
    # Object of a range: the fragment (blobs, and codes when the codes are in it) or the index object.
    frag = lv["seg"].astype(np.int64) * 2
    code_obj = np.where(lv["cif"] == 1, frag, frag + 1)
    obj, st, en = [frag], [rng[:, 0]], [rng[:, 1]]
    if mode == "leaf":
        obj.append(code_obj)
        st.append(rng[:, 2])
        en.append(rng[:, 3])
    else:
        key = np.unique(rec["leaf"].astype(np.int64) * 65536 + rec["ordinal"].astype(np.int64) // 32)
        hl, hb = key // 65536, key % 65536
        cs = rng[hl, 2]
        # The code block, then each of the four factor arrays' slice for the block's 32 rows.
        obj.append(code_obj[hl])
        st.append(cs + 16 * n[hl] + hb * 4096)
        en.append(cs + 16 * n[hl] + hb * 4096 + 4096)
        for a in range(4):
            lo = cs + a * 4 * n[hl] + hb * 128
            obj.append(code_obj[hl])
            st.append(lo)
            en.append(np.minimum(lo + 128, cs + (a + 1) * 4 * n[hl]))
    return np.concatenate(obj), np.concatenate(st), np.concatenate(en)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump")
    ap.add_argument("--label", default="")
    ap.add_argument("--skip", type=int, default=1)
    ap.add_argument("--max-queries", type=int, default=60)
    args = ap.parse_args()
    recs = sim.read(args.dump)[args.skip : args.skip + args.max_queries]
    gaps = [0, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20]
    acc: dict[str, list[tuple[float, float]]] = {}
    base = []
    for rec in recs:
        b, g = sim.model_bytes(rec["leaves"], 256, today=True, gets=True)
        base.append((b, g, latency_ms(b, g)))
        for mode in ("leaf", "blocks"):
            obj, st, en = ranges_for(rec, mode)
            for gap in gaps:
                nb, ng = merge(obj, st, en, gap)
                acc.setdefault(f"{mode} gap {gap >> 10} KiB", []).append((nb, ng, latency_ms(nb, ng)))
    med = lambda rows, i: float(np.median([r[i] for r in rows]))  # noqa: E731
    out = {"label": args.label, "queries": len(recs),
           "today_modeled": {"MB": med(base, 0) / 1e6, "GETs": med(base, 1), "model_ms": med(base, 2)}}
    for k, rows in acc.items():
        out[k] = {"MB": round(med(rows, 0) / 1e6, 1), "GETs": med(rows, 1), "model_ms": round(med(rows, 2), 1),
                  "bytes_vs_today": round(med(rows, 0) / med(base, 0), 3), "model_ms_vs_today": round(med(rows, 2) - med(base, 2), 1)}
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
