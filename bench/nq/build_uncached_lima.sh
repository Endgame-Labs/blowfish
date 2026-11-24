#!/usr/bin/env bash
# Build a run-local Lima for a disposable Apple silicon memory benchmark.
# Usage: ./build_uncached_lima.sh RUN_DIR
# Outputs RUN_DIR/runtime/bin/limactl; put that directory first in PATH for Colima.
# The caller removes RUN_DIR, including build caches, when the benchmark ends.
set -euo pipefail
if [[ $(uname -s) != Darwin || $(uname -m) != arm64 ]]; then
  echo "This benchmark runtime requires an Apple silicon Mac." >&2
  exit 1
fi
run_dir=$(cd "$1" && pwd)
runtime="$run_dir/runtime"
mkdir -p "$runtime"/{bin,share,libexec/lima,src,go-cache,go-mod,go-tmp,go}
archive="$runtime/lima.tar.gz"
curl -fsSL --retry 3 https://github.com/lima-vm/lima/archive/refs/tags/v2.3.0.tar.gz -o "$archive"
printf '%s  %s\n' 436ace830cb3e8fa512e0157a220bd8d6f826aef789c3f26d2984f0ea1e3c2ea "$archive" | shasum -a 256 -c -
tar -xzf "$archive" -C "$runtime/src" --strip-components=1
driver="$runtime/src/pkg/driver/vz/vm_darwin.go"
test "$(grep -c '^const diskImageCachingMode = vz.DiskImageCachingModeCached$' "$driver")" -eq 1
# Cached VZ backing files escape a guest cgroup's page-cache accounting.
sed 's/^const diskImageCachingMode = vz.DiskImageCachingModeCached$/const diskImageCachingMode = vz.DiskImageCachingModeUncached/' "$driver" > "$driver.new"
mv "$driver.new" "$driver"
export GOCACHE="$runtime/go-cache" GOMODCACHE="$runtime/go-mod" GOTMPDIR="$runtime/go-tmp" GOPATH="$runtime/go" TMPDIR="$runtime/go-tmp"
(
  cd "$runtime/src"
  go build -p 4 -ldflags '-X github.com/lima-vm/lima/v2/pkg/version.Version=2.3.0' -o "$runtime/bin/limactl" ./cmd/limactl
  go build -p 4 -ldflags '-X github.com/lima-vm/lima/v2/pkg/version.Version=2.3.0' -o "$runtime/libexec/lima/lima-driver-vz" ./cmd/lima-driver-vz
)
codesign -f --entitlements "$runtime/src/vz.entitlements" -s - "$runtime/bin/limactl"
codesign -f --entitlements "$runtime/src/vz.entitlements" -s - "$runtime/libexec/lima/lima-driver-vz"
# Reuse the installed version's read-only guest agent and boot resources.
lima_prefix=$(brew --prefix lima)
test -d "$lima_prefix/share/lima"
ln -sfn "$lima_prefix/share/lima" "$runtime/share/lima"
for helper in "$lima_prefix/libexec/lima/"*; do
  [[ $(basename "$helper") == lima-driver-vz ]] && continue
  ln -sfn "$helper" "$runtime/libexec/lima/$(basename "$helper")"
done
"$runtime/bin/limactl" --version
