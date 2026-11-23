#!/usr/bin/env bash
# ab.sh <out-dir> <fast|durable> <passes> <labelA>=<linux-kdb-service> <labelB>=<linux-kdb-service> ...
# Alternates the binaries over <passes> passes (A B, A B, ...), each a fresh run.sh kdb run, then
# prints report.py's table. CELLS (default read,update,mixed50,insert), CONCURRENCY (1,8,32) and
# DOCS (100000) override the cell list; RW_FLAGS adds rwcompare flags (e.g. RW_FLAGS="-dist hot:16"). Build a binary with:
#   (cd go && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/x/kdb-service ./cmd/kdb-service)
set -uo pipefail
out=$1 dur=$2 passes=$3; shift 3
here=$(cd "$(dirname "$0")" && pwd)
labels=()
for p in $(seq 1 "$passes"); do
  for pair in "$@"; do
    label=${pair%%=*} bin=${pair#*=}
    [[ $p == 1 ]] && labels+=("$label")
    echo "=== pass $p $label $dur $(date +%T) ==="
    KDB_BIN=$bin OUT=$out "$here/run.sh" kdb "$dur" "$label" -docs "${DOCS:-100000}" \
      -concurrency "${CONCURRENCY:-1,8,32}" -cells "${CELLS:-read,update,mixed50,insert}" ${RW_FLAGS:-}
  done
done
python3 "$here/report.py" "$out/results.jsonl" "$(IFS=,; echo "${labels[*]}")"
