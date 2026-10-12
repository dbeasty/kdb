#!/usr/bin/env bash
# Disk-mode benchmark: --durability sync (sync-mode full) at several document counts.
#
#   scripts/bench-disk.sh [SIZES...]          default sizes: 10000 100000 1000000
#   BENCH_DIR=/path/on/the/disk/under/test    default: a fresh directory under $TMPDIR
#
# For each size it runs `kdb-diskbench load` (bulk load + hot measurements) and then
# `kdb-diskbench reopen` (a fresh process on the same directory), printing one JSON object
# per phase. Do not run anything else heavy while this runs. Results: docs/benchmarks/disk-mode.md
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
sizes=("$@")
[ ${#sizes[@]} -eq 0 ] && sizes=(10000 100000 1000000)
base="${BENCH_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/kdb-diskbench.XXXXXX")}"
bin="$base/kdb-diskbench"
mkdir -p "$base"

(cd "$root/go" && go build -o "$bin" ./cmd/kdb-diskbench)

for n in "${sizes[@]}"; do
  dir="$base/data-$n"
  rm -rf "$dir"
  echo "== $n documents in $dir" >&2
  "$bin" load -dir "$dir" -n "$n"
  "$bin" reopen -dir "$dir" -n "$n"
  rm -rf "$dir"
done
