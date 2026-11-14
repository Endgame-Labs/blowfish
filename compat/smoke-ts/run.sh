#!/bin/sh
set -eu
cd "$(dirname "$0")"
if [ -x "$HOME/.local/node/bin/node" ]; then PATH="$HOME/.local/node/bin:$PATH"; export PATH; fi
npm ci --silent
exec node smoke.mjs "$@"
