#!/usr/bin/env bash
set -euo pipefail
source ~/.cargo/env
cd "$(dirname "$0")/.."
work=$(mktemp -d)
port=${BLOWFISH_SMOKE_PORT:-18742}
server_pid=
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT
cargo build -p blowfish
# cargo's configured target directory may differ across machines.
binary=$(cargo metadata --format-version 1 --no-deps | uv run python -c 'import json,sys; print(json.load(sys.stdin)["target_directory"] + "/debug/blowfish")')
cat >"$work/blowfish.toml" <<CONFIG
storage_url = "file://$work/store"
listen = "127.0.0.1:$port"
CONFIG
BLOWFISH_ADMIN_KEY=x "$binary" serve --config "$work/blowfish.toml" >"$work/server.log" 2>&1 &
server_pid=$!
base="http://127.0.0.1:$port"
for _ in {1..100}; do
  if curl -fsS -H 'Authorization: Bearer x' "$base/" >"$work/root.json" 2>/dev/null; then break; fi
  if ! kill -0 "$server_pid" 2>/dev/null; then cat "$work/server.log"; exit 1; fi
  sleep 0.1
done
request() { curl --fail-with-body -sS -H 'Authorization: Bearer x' -H 'Content-Type: application/json' "$@"; }
request -X POST "$base/v2/namespaces/smoke" -d '{"distance_metric":"cosine_distance","schema":{"text":{"type":"string","full_text_search":true}},"upsert_rows":[{"id":1,"vector":[1.0,0.0],"text":"hello fish"},{"id":2,"vector":[0.0,1.0],"text":"hello ocean"}]}' >"$work/write.json"
request -X POST "$base/v2/namespaces/smoke/query" -d '{"rank_by":["vector","ANN",[1.0,0.0]],"top_k":1,"include_attributes":true}' >"$work/query.json"
request "$base/v2/namespaces/smoke/metadata" >"$work/metadata.json"
uv run python - "$work" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
assert json.loads((p / 'write.json').read_text())['rows_affected'] == 2
assert json.loads((p / 'query.json').read_text())['rows'][0]['id'] == 1
assert json.loads((p / 'metadata.json').read_text())['approx_row_count'] == 2
PY
if [[ "${BLOWFISH_SDK_SMOKE:-0}" == 1 ]]; then
  env -u TURBOPUFFER_REGION TURBOPUFFER_BASE_URL="$base" TURBOPUFFER_API_KEY=x uv run --with turbopuffer python scripts/sdk_smoke.py
fi
request -X DELETE "$base/v2/namespaces/smoke" >"$work/delete.json"
printf 'Local file-store smoke passed 🐡\n'
