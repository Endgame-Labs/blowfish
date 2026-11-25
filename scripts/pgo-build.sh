#!/usr/bin/env bash
# Build an instrumented server, train it, merge its profiles, and build with PGO.
# Usage: scripts/pgo-build.sh .scratch/RUN [-- TRAINER [ARG ...]]
# TRAINER receives BF_PGO_BINARY and LLVM_PROFILE_FILE, must stop its servers
# gracefully, and must wait for them to exit before returning.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
if [[ $# -lt 1 || ( $# -gt 1 && ( $2 != -- || $# -lt 3 ) ) ]]; then
  echo 'usage: scripts/pgo-build.sh .scratch/RUN [-- TRAINER [ARG ...]]' >&2
  exit 2
fi
run=$(realpath -m "$1")
if [[ $run != "$root/.scratch/"* || -e $run || $run == *' '* ]]; then
  echo 'RUN must be a new directory beneath this checkout’s .scratch, without spaces' >&2
  exit 2
fi
shift
if [[ $# -gt 0 ]]; then shift; fi
mkdir -p "$run/raw" "$run/tmp"
export TMPDIR="$run/tmp"
target=${BF_PGO_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}
profdata=${LLVM_PROFDATA:-$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin/llvm-profdata}
if [[ ! -x $profdata ]]; then
  echo 'install rustup component llvm-tools-preview, or set LLVM_PROFDATA' >&2
  exit 2
fi
rustc -vV > "$run/toolchain.txt"
sha256sum Cargo.lock > "$run/lockfile.sha256"
find crates -type f \( -name '*.rs' -o -name Cargo.toml \) -print0 \
  | sort -z | xargs -0 sha256sum > "$run/sources.sha256"
# Use identical flags and an explicit target to avoid instrumenting build scripts.
# Shared counters otherwise lose updates when training on several server threads.
flags="${RUSTFLAGS-} -Cllvm-args=-instrprof-atomic-counter-update-all"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$run/target}"
features=()
if [[ -n ${BF_PGO_FEATURES:-} ]]; then features=(--features "$BF_PGO_FEATURES"); fi
printf 'target=%s\nfeatures=%s\nrustflags=%s\nCC=%s\nCXX=%s\nCFLAGS=%s\nCXXFLAGS=%s\n' \
  "$target" "${BF_PGO_FEATURES:-}" "$flags" "${CC:-cc}" "${CXX:-c++}" \
  "${CFLAGS:-}" "${CXXFLAGS:-}" > "$run/build-settings.txt"
build=(cargo build --locked --release --target "$target" -p blowfish "${features[@]}")
RUSTFLAGS="$flags -Cprofile-generate=$run/raw" \
  "${build[@]}" --bin blowfish --example pgo_train 2>&1 | tee "$run/instrument.log"
binary="$CARGO_TARGET_DIR/$target/release/blowfish"
cp "$binary" "$run/blowfish-instrumented"
export BF_PGO_BINARY="$run/blowfish-instrumented"
export LLVM_PROFILE_FILE="$run/raw/%m-%p.profraw"
training_start=$SECONDS
if [[ $# -gt 0 ]]; then
  "$@"
else
  # Discard fixture construction and client counters; merge only production servers.
  LLVM_PROFILE_FILE="$run/trainer.profraw" \
    "$CARGO_TARGET_DIR/$target/release/examples/pgo_train" "$run/training" "$LLVM_PROFILE_FILE"
fi
printf '%s\n' "$((SECONDS - training_start))" > "$run/training-seconds.txt"
shopt -s nullglob
raw=("$run/raw/"*.profraw)
if [[ ${#raw[@]} -eq 0 ]]; then
  echo 'trainer produced no profiles; shut down the server gracefully and wait' >&2
  exit 1
fi
"$profdata" merge -o "$run/merged.profdata" "${raw[@]}"
"$profdata" show "$run/merged.profdata" > "$run/profile.txt"
RUSTFLAGS="$flags -Cprofile-use=$run/merged.profdata -Cllvm-args=-pgo-warn-missing-function" \
  "${build[@]}" --bin blowfish 2>&1 | tee "$run/optimize.log"
cp "$binary" "$run/blowfish"
sha256sum "$run/merged.profdata" "$run/blowfish" > "$run/outputs.sha256"
printf 'PGO binary: %s\nProfile summary: %s\n' "$run/blowfish" "$run/profile.txt"
