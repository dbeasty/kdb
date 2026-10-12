#!/usr/bin/env bash
# Backup and restore drill: prove a backup taken from a LIVE kdb-service restores to an
# identical database. A backup nobody has restored is a guess.
#
#   scripts/backup-restore-drill.sh [N]        # N documents, default 50
#   KDB_BIN_DIR=/path/to/bins scripts/backup-restore-drill.sh   # skip the go build
#
# Steps: start a service on a temp data dir -> load N documents -> refuse the offline
# `kdb-inspect backup` while the service is live (it takes the exclusive directory lock, so it is
# an offline tool) -> take the LIVE backup through the control plane -> backup-verify it -> stop
# the service -> restore into a fresh directory while it is down -> verify the restored log ->
# start a second service on it -> compare every document and the commit head.
set -euo pipefail

N="${1:-50}"
NS="drill/docs"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/kdb-drill.XXXXXX")"
PIDS=()

cleanup() {
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  if [ "${KEEP:-0}" = 1 ]; then echo "kept $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT
fail() { echo "DRILL FAILED: $*" >&2; for f in "$WORK"/svc*.log; do [ -f "$f" ] && { echo "--- $f"; tail -20 "$f"; }; done; exit 1; }

BIN="${KDB_BIN_DIR:-$WORK/bin}"
if [ -z "${KDB_BIN_DIR:-}" ]; then
  mkdir -p "$BIN"
  (cd "$ROOT/go" && go build -o "$BIN/" ./cmd/kdb-service ./cmd/kdb-inspect ./cmd/kdb ./cmd/kdb-e2e-helper)
fi

# Ask the kernel for free ports rather than guessing; peer/stream listeners are off by default,
# so only the SQL and control listeners need one.
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
docid() { printf '%032x' "$1"; }

start_service() { # data-dir sql-port control-port logfile [extra args...]
  local dir=$1 sql=$2 ctl=$3 log=$4; shift 4
  "$BIN/kdb-service" --data-dir "$dir" --namespace "$NS" \
    --sql-addr "tcp://127.0.0.1:$sql?bind=true" "$@" >"$log" 2>&1 &
  PIDS+=("$!")
  LAST_PID=$!
  for _ in $(seq 1 100); do
    if "$BIN/kdb-e2e-helper" query --addr "tcp://127.0.0.1:$sql" --namespace "$NS" --sql "SELECT 1" >/dev/null 2>&1; then return 0; fi
    kill -0 "$LAST_PID" 2>/dev/null || fail "service exited early ($log)"
    sleep 0.1
  done
  fail "service did not accept connections ($log)"
}
stop_service() { kill -TERM "$1"; wait "$1" 2>/dev/null || true; }

SQL1=$(free_port); CTL1=$(free_port); SQL2=$(free_port)
DATA="$WORK/data"; BACKUPS="$WORK/backups"; RESTORED="$WORK/restored"
mkdir -p "$DATA" "$BACKUPS" "$RESTORED"

echo "== start service (sql :$SQL1, control :$CTL1)"
start_service "$DATA" "$SQL1" "$CTL1" "$WORK/svc1.log" \
  --control-addr "127.0.0.1:$CTL1" --control-backup-dir "$BACKUPS" --control-write
PID1=$LAST_PID

echo "== load $N documents"
for i in $(seq 1 "$N"); do
  "$BIN/kdb-e2e-helper" put --addr "tcp://127.0.0.1:$SQL1" --namespace "$NS" \
    --doc-id "$(docid "$i")" --json "{\"n\":$i,\"label\":\"doc-$i\"}" >/dev/null
done

echo "== offline kdb-inspect backup must refuse a live directory"
if "$BIN/kdb-inspect" backup --data-dir "$DATA" --namespace "$NS" --to "$BACKUPS" >"$WORK/offline.out" 2>&1; then
  fail "kdb-inspect backup succeeded against a live service; it must hold the exclusive lock"
fi
grep -q "in use" "$WORK/offline.out" || fail "unexpected refusal: $(cat "$WORK/offline.out")"

echo "== live backup through the control plane"
# The control plane authenticates every request; with --rbac off any credentials are accepted.
RESP=$(curl -fsS -u drill:drill -X POST "http://127.0.0.1:$CTL1/v1/ns/drill%2Fdocs/backups" -d '{}') \
  || fail "control-plane backup request failed"
BACKUP_ID=$(printf '%s' "$RESP" | sed -n 's/.*"backupId":"\([^"]*\)".*/\1/p')
[ -n "$BACKUP_ID" ] || fail "no backupId in: $RESP"
echo "   backup $BACKUP_ID"

echo "== verify the backup"
"$BIN/kdb-inspect" backup-verify --namespace "$NS" --to "$BACKUPS" --backup-id "$BACKUP_ID" | tee "$WORK/verify.out"
grep -q "clean" "$WORK/verify.out" || fail "backup-verify reported problems"

echo "== stop the service"
stop_service "$PID1"
HEAD1=$("$BIN/kdb" --data-dir "$DATA" status "$NS" | sed -n 's/^HEAD //p')
[ -n "$HEAD1" ] || fail "could not read the original head"

echo "== restore into a fresh directory (service is down)"
"$BIN/kdb-inspect" restore --namespace "$NS" --from-backup "$BACKUPS" --backup-id "$BACKUP_ID" --out "$RESTORED"
"$BIN/kdb-inspect" verify --data-dir "$RESTORED" --namespace "$NS" --level L2
HEAD2=$("$BIN/kdb" --data-dir "$RESTORED" status "$NS" | sed -n 's/^HEAD //p')
[ "$HEAD1" = "$HEAD2" ] || fail "commit head differs: original $HEAD1, restored $HEAD2"

echo "== start a second service on the restored directory (sql :$SQL2)"
start_service "$RESTORED" "$SQL2" "" "$WORK/svc2.log"
PID2=$LAST_PID

echo "== compare every document"
for i in $(seq 1 "$N"); do
  got=$("$BIN/kdb-e2e-helper" get --addr "tcp://127.0.0.1:$SQL2" --namespace "$NS" --doc-id "$(docid "$i")" | tail -1)
  want="{\"n\":$i,\"label\":\"doc-$i\"}"
  [ "$got" = "$want" ] || fail "document $i: got $got, want $want"
done
stop_service "$PID2"

echo "DRILL PASSED: $N documents and head $HEAD1 identical after backup (live), restore (offline), restart"
