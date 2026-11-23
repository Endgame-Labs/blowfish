#!/usr/bin/env bash
# Start (or resume) the 100M load on the benchmark instance as three systemd services:
# the writable server (10 GiB cgroup), the loader (4 GiB cgroup), and a 30 s RSS/docs-per-second monitor.
# Usage: start_load.sh [--resume] [ROWS=100000000]
# Needs: scale-server built (bench/scale/build.sh), bench/scale/config.local, SCALE_SCRATCH.
# Never point this at a prefix that holds other namespaces: a writable server indexes and garbage-collects all of them.
set -euo pipefail
BIG="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$BIG/../common.sh"
RESUME=""; [ "${1:-}" = "--resume" ] && { RESUME="--resume"; shift; }
ROWS="${1:-100000000}"
NS="$(cfg namespace)"
case "$(cfg storage_url)" in *scale/*) echo "refusing: prefix contains scale/"; exit 1;; esac
OUT="$SCRATCH/out"; mkdir -p "$OUT" "$SCRATCH/load" "$SCRATCH/cache"
python3 "$BENCH_DIR/mkconfig.py" "$CONFIG" "$SCRATCH/storage-rw.json" "$SCRATCH/cache" --memory-gib 1 --disk-gib 32
SERVER="$SCRATCH/target/release/blowfish-scale-server"
run() { sudo systemd-run --uid="$USER" --collect "$@"; }
run --unit=bf-server -p MemoryMax=10G -p MemoryHigh=9G -p StandardOutput=append:$OUT/server.log -p StandardError=append:$OUT/server.log \
  -E RAYON_NUM_THREADS=6 -E MALLOC_ARENA_MAX=2 -E AWS_TIMEOUT=180s -E BF_TRACE_STAGES=1 \
  -E BF_MAX_SEGMENT_ROWS=80000 -E BF_BUILD_CONCURRENCY=2 -E BF_DEFER_COMPACTION_SEGMENTS=5000 \
  "$SERVER" "$SCRATCH/storage-rw.json" 127.0.0.1:8300
for _ in $(seq 100); do curl -sf http://127.0.0.1:8300/ >/dev/null && break; sleep 0.5; done
run --unit=bf-loader -p MemoryMax=4G -p StandardOutput=append:$OUT/load.log -p StandardError=append:$OUT/load.log \
  -E OPENBLAS_NUM_THREADS=2 -E OMP_NUM_THREADS=2 -E MALLOC_ARENA_MAX=2 -E UV_CACHE_DIR="$SCRATCH/uv-cache" -E HOME="$HOME" \
  -E PATH="$PATH" -E TMPDIR="$SCRATCH/tmp" \
  "$(command -v uv)" run "$ROOT/crates/bf-engine/examples/scale_load.py" \
  --hub-endpoint "$(cfg hub_endpoint)" --scratch "$SCRATCH/load" --rows "$ROWS" --namespace "$NS" \
  --batch 2000 --vector-type f16 --min-available-gib 1.5 $RESUME
cat > "$OUT/monitor.sh" <<M
#!/usr/bin/env bash
while true; do
  p=\$(systemctl show -p MainPID --value bf-server)
  echo "\$(date +%s) rss_kb=\$(awk '/VmRSS/{print \$2}' /proc/\$p/status) hwm_kb=\$(awk '/VmHWM/{print \$2}' /proc/\$p/status) avail_kb=\$(awk '/MemAvailable/{print \$2}' /proc/meminfo) \$(tail -n1 $OUT/load.log | cut -c1-200)" >> $OUT/monitor.log
  sleep 30
done
M
chmod +x "$OUT/monitor.sh"
run --unit=bf-monitor "$OUT/monitor.sh"
