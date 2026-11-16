# Shared by the scale scripts. Source it; do not run it.
# Private settings (bucket, endpoint, profile) live in bench/scale/config.local, which git ignores.
# Copy config.example.json to config.local and fill it in.
set -euo pipefail
source ~/.cargo/env
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$BENCH_DIR/../.." && pwd)"
SCRATCH="${SCALE_SCRATCH:-$ROOT/.scratch/scale}"
CONFIG="${SCALE_CONFIG:-$BENCH_DIR/config.local}"
mkdir -p "$SCRATCH"/{out,tmp,uv-cache}
export UV_CACHE_DIR="$SCRATCH/uv-cache" TMPDIR="$SCRATCH/tmp"
cfg() { python3 -c "import json,sys;print(json.load(open('$CONFIG')).get('$1',''))"; }
