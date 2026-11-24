#!/usr/bin/env bash
# Build an instrumented server, train it, merge its profiles, and build with PGO.
# Usage: scripts/pgo-build.sh .scratch/RUN -- TRAINER [ARG ...]
# TRAINER receives BF_PGO_BINARY and LLVM_PROFILE_FILE, must stop its servers
# gracefully, and must wait for them to exit before returning.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
if [[ $# -lt 3 || $2 != -- ]]; then
  echo 'usage: scripts/pgo-build.sh .scratch/RUN -- TRAINER [ARG ...]' >&2
  exit 2
fi
run=$(realpath -m "$1")
if [[ $run != "$root/.scratch/"* || -e $run || $run == *' '* ]]; then
  echo 'RUN must be a new directory beneath this checkout’s .scratch, without spaces' >&2
  exit 2
fi
shift 2
mkdir -p "$run/raw" "$run/tmp"
export TMPDIR="$run/tmp"
target=${BF_PGO_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}
profdata=${LLVM_PROFDATA:-$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin/llvm-profdata}
if [[ ! -x $profdata ]]; then
  echo 'install rustup component llvm-tools-preview, or set LLVM_PROFDATA' >&2
  exit 2
fi
# Use identical flags and an explicit target to avoid instrumenting build scripts.
flags=${RUSTFLAGS-}
export CARGO_TARGET_DIR="$run/target"
RUSTFLAGS="$flags -Cprofile-generate=$run/raw" \
  cargo build --locked --release --target "$target" -p blowfish --bin blowfish
binary="$CARGO_TARGET_DIR/$target/release/blowfish"
cp "$binary" "$run/blowfish-instrumented"
export BF_PGO_BINARY="$run/blowfish-instrumented"
export LLVM_PROFILE_FILE="$run/raw/%m-%p.profraw"
"$@"
shopt -s nullglob
raw=("$run/raw/"*.profraw)
if [[ ${#raw[@]} -eq 0 ]]; then
  echo 'trainer produced no profiles; shut down the server gracefully and wait' >&2
  exit 1
fi
"$profdata" merge -o "$run/merged.profdata" "${raw[@]}"
"$profdata" show "$run/merged.profdata" > "$run/profile.txt"
RUSTFLAGS="$flags -Cprofile-use=$run/merged.profdata -Cllvm-args=-pgo-warn-missing-function" \
  cargo build --locked --release --target "$target" -p blowfish --bin blowfish
cp "$binary" "$run/blowfish"
printf 'PGO binary: %s\nProfile summary: %s\n' "$run/blowfish" "$run/profile.txt"
