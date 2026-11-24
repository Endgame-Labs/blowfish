"""Summarize Q2 instruction samples by named function families, not exact phase boundaries.

Read `perf script` output plus server_tax.py counter files in a .scratch directory.
Counters default to 512 queries/epoch. Allocator samples take precedence, followed
by input, output, consistency, hydration, validation, cache, vector, and runtime
families. Inlined/shared work can remain in the engine or unresolved buckets.
"""

import argparse
import collections
import json
import statistics
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("profile_dir", type=Path)
parser.add_argument("--queries", type=int, default=512)
args = parser.parse_args()
root = args.profile_dir.resolve()
if (
    not root.is_relative_to(Path(__file__).resolve().parents[3] / ".scratch")
    or args.queries <= 0
):
    parser.error("use a .scratch profile directory and a positive query count")


def classify(frames):
    text = "\n".join(frames)
    leaf = "\n".join(frames[:3])
    if any(
        x in leaf for x in ["_rjem", "__rust_alloc", "__libc_malloc", "__libc_free"]
    ):
        return "allocator"
    if any(
        x in text
        for x in [
            "serde_json::de::",
            "bf_types::ast::RankVisitor",
            "bf_types::ast::VectorArg",
        ]
    ):
        return "JSON input"
    if any(x in text for x in ["serde_json::ser::", "serde_json::value::ser::"]):
        return "JSON output"
    if any(
        x in text
        for x in [
            ">::snapshot",
            "bf_engine::snapshot::",
            "NamespaceState as core::clone::Clone",
            "bf_wal::",
        ]
    ):
        return "snapshot and consistency"
    if any(
        x in text
        for x in [
            "bf_engine::segment::projected",
            "bf_engine::segment::documents",
            "bf_engine::indexed::hydrate",
            "bf_engine::query::project",
        ]
    ):
        return "hydration and projection"
    if any(
        x in text
        for x in [
            "bf_engine::query::validate",
            "bf_engine::query::limits",
            "bf_engine::filter::validate",
        ]
    ):
        return "named validation helpers"
    if any(
        x in text for x in ["moka::", "bf_storage::cache::", "bf_engine::read_cache::"]
    ):
        return "cache and range reader"
    if any(x in text for x in ["bf_vector::", "numkong", "simsimd"]):
        return "vector search"
    if any(
        x in text
        for x in [
            "bf_engine::query_worker::",
            "rayon_core::",
            "tokio::runtime::scheduler",
            "tokio::runtime::blocking",
            "tokio::task::",
        ]
    ):
        return "CPU and runtime dispatch"
    if "bf_engine::" in text:
        return "other engine and planning"
    if any(
        x in text
        for x in [
            "hyper::",
            "axum::",
            "http_body",
            "tower::",
            "tokio::",
            "mio::",
            "futures::",
        ]
    ):
        return "HTTP and async plumbing"
    return "shared or unresolved"


for arm in ["baseline-x1", "final", "cache-final"]:
    stack = root / (arm + ".stacks")
    if not stack.exists():
        continue
    counts = collections.Counter()
    frames = []
    with stack.open() as src:
        for line in src:
            if line.startswith("\t"):
                frames.append(line)
            elif frames:
                counts[classify(frames)] += 1
                frames = []
        if frames:
            counts[classify(frames)] += 1
    if not counts:
        raise ValueError(f"no samples in {stack}")
    summary = {
        "arm": arm,
        "samples": sum(counts.values()),
        "groups": {k: 100 * v / sum(counts.values()) for k, v in counts.most_common()},
    }
    for label in ["counter", "kernel"]:
        records = []
        for p in sorted(root.glob(f"{arm}-{label}.*.perf")):
            events = {}
            for line in p.read_text().splitlines():
                a = line.split(",")
                if len(a) > 4 and a[0].isdigit():
                    events[a[2]] = int(a[0]) / args.queries
            records.append(events)
        if records:
            summary[label] = {
                k: statistics.median(r[k] for r in records if k in r)
                for k in records[0]
            }
    print(json.dumps(summary), flush=True)
    (root / (arm + ".summary.json")).write_text(json.dumps(summary, indent=2))
