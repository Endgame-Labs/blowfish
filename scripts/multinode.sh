#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source ~/.cargo/env
export COMPOSE_PROJECT_NAME="blowfish-multinode-${$}"
compose=(docker compose --profile multinode)
cleanup() { "${compose[@]}" down --volumes --remove-orphans; }
trap cleanup EXIT
"${compose[@]}" up --build -d --wait proxy
uv run --directory compat compat check --base-url http://127.0.0.1:8100
uv run --no-project python scripts/multinode_stress.py
