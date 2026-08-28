#!/usr/bin/env bash
# Sweeps --memory-limit-mb against the Lightsail-tier container in BOTH storage modes, which is
# how README.md's 2026-08-27 table was produced. run.sh cannot do this on its own: it always
# passes --memory, and it takes no --memory-limit-mb.
#
#   mem  - `--memory`, the in-memory runtime (NewInMemoryCommitDag + InMemoryStorageAdapter).
#          This is what every table in README.md before 2026-08-27 measured. It never reaches
#          PersistingCommitDAG, the delta log, or the group committer, so it exercises the memory
#          guard against DAG growth and nothing of the write path.
#   file - `--data-dir`, the file-backed runtime, which does commit through the delta log.
#
# The container stays at the tier's 1GB / 2 vCPU; only --memory-limit-mb varies. Each run is
# scored on whether the kernel OOM-killed it (docker's OOMKilled/exitCode 137), not on throughput
# alone - see README.md for why a surviving run at 98% peak is not a safe configuration.
#
# Usage:
#   ./mem-sweep.sh                      # both modes, 80% and 90% of the container limit
#   MODES=file PERCENTS="80 90 95" ./mem-sweep.sh
#   DURATION=120s REPEATS=3 ./mem-sweep.sh
set -o pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
IMAGE_TAG="${IMAGE_TAG:-kdb-service-lightsail-sim}"
CONTAINER="kdb-memsweep"
VOLUME="kdb-memsweep-data"
LOADTEST_BIN="/tmp/kdb-loadtest-memsweep-$$"

MEMORY="${MEMORY:-1g}"
CPUS="${CPUS:-2}"
DURATION="${DURATION:-60s}"
CONCURRENCY="${CONCURRENCY:-16}"
READ_RATIO="${READ_RATIO:-0.5}"
DOC_POOL="${DOC_POOL:-2000}"
PORT="${PORT:-19390}"
MODES="${MODES:-mem file}"
PERCENTS="${PERCENTS:-80 90}"
REPEATS="${REPEATS:-1}"

cleanup() {
  local status=$?
  set +e
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  docker volume rm -f "$VOLUME" >/dev/null 2>&1
  rm -f "$LOADTEST_BIN"
  exit "$status"
}
trap cleanup EXIT

# Portable "1g"/"512m" -> MB without GNU numfmt (absent on macOS/BSD, where this also runs).
mem_limit_to_mb() {
  local spec="$1" num unit
  num="${spec%[a-zA-Z]*}"; unit="${spec: -1}"
  case "$unit" in
    [Gg]) echo $(( num * 1024 )) ;;
    [Mm]) echo "$num" ;;
    [Kk]) echo $(( num / 1024 )) ;;
    *) echo "$num" ;;
  esac
}
CONTAINER_MB="$(mem_limit_to_mb "$MEMORY")"

run_one() {
  local mode="$1" pct="$2" run="$3"
  local limit_mb=$(( CONTAINER_MB * pct / 100 ))
  local stats; stats="$(mktemp)"

  echo ""
  echo "== mode=${mode} limit=${limit_mb}MB (${pct}% of ${MEMORY}) run ${run}/${REPEATS} =="

  docker rm -f "$CONTAINER" >/dev/null 2>&1
  docker volume rm -f "$VOLUME" >/dev/null 2>&1

  local storage_args vol_args
  storage_args=(--memory)
  vol_args=()
  if [ "$mode" = "file" ]; then
    docker volume create "$VOLUME" >/dev/null
    # Fresh named volumes are root-owned; the image runs as uid 10001.
    docker run --rm -v "$VOLUME:/var/lib/kdb" alpine:3.20 \
      chown -R 10001:10001 /var/lib/kdb >/dev/null
    storage_args=(--data-dir /var/lib/kdb)
    vol_args=(-v "$VOLUME:/var/lib/kdb")
  fi

  docker run -d --name "$CONTAINER" \
    --memory="$MEMORY" --memory-swap="$MEMORY" --cpus="$CPUS" \
    -p "${PORT}:9090" ${vol_args[@]+"${vol_args[@]}"} \
    "$IMAGE_TAG" \
    ${storage_args[@]+"${storage_args[@]}"} \
    --sql-addr "tcp://0.0.0.0:9090?bind=true" --namespace demo/loadtest \
    --memory-limit-mb "$limit_mb" >/dev/null

  local up=0 i
  for i in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/127.0.0.1/${PORT}") 2>/dev/null; then exec 3>&- 3<&-; up=1; break; fi
    sleep 0.5
  done
  if [ "$up" -ne 1 ]; then
    echo "RESULT mode=$mode pct=$pct run=$run outcome=never_started"
    docker logs "$CONTAINER" 2>&1 | tail -20
    rm -f "$stats"
    return
  fi

  ( while docker inspect "$CONTAINER" >/dev/null 2>&1; do
      docker stats --no-stream --format '{{.MemPerc}}' "$CONTAINER" >> "$stats" 2>/dev/null || true
      sleep 1
    done ) &
  local stats_pid=$!

  local out
  out="$("$LOADTEST_BIN" \
    -addr "127.0.0.1:${PORT}" -namespace demo/loadtest \
    -concurrency "$CONCURRENCY" -duration "$DURATION" \
    -read-ratio "$READ_RATIO" -doc-pool "$DOC_POOL" 2>&1)"
  echo "$out"
  kill "$stats_pid" 2>/dev/null; wait "$stats_pid" 2>/dev/null

  local ops thr oom exitcode peak outcome
  ops="$(echo "$out" | grep -o 'total ops: [0-9]*' | awk '{print $3}')"
  thr="$(echo "$out" | grep -o 'throughput: [0-9.]*' | awk '{print $2}')"
  oom="$(docker inspect "$CONTAINER" --format '{{.State.OOMKilled}}' 2>/dev/null)"
  exitcode="$(docker inspect "$CONTAINER" --format '{{.State.ExitCode}}' 2>/dev/null)"
  peak="$(tr -d '%' < "$stats" | sort -n | tail -1)"
  rm -f "$stats"
  if [ "$oom" = "true" ] || [ "$exitcode" = "137" ]; then outcome="OOM-KILLED"; else outcome="survived"; fi

  echo "RESULT mode=$mode pct=$pct run=$run limit_mb=$limit_mb ops=${ops:-0} thr=${thr:-0} outcome=$outcome exit=$exitcode oom=$oom peak_mem_pct=${peak:-n/a}"

  docker rm -f "$CONTAINER" >/dev/null 2>&1
  docker volume rm -f "$VOLUME" >/dev/null 2>&1
}

echo "== Building server image =="
docker build -q -t "$IMAGE_TAG" -f "$REPO_ROOT/docs/benchmarks/lightsail-sim/Dockerfile" "$REPO_ROOT" >/dev/null

echo "== Building load generator (host, unconstrained) =="
( cd "$REPO_ROOT/go" && go build -o "$LOADTEST_BIN" ./cmd/kdb-loadtest )

for mode in $MODES; do
  for pct in $PERCENTS; do
    for run in $(seq 1 "$REPEATS"); do
      run_one "$mode" "$pct" "$run"
    done
  done
done

echo ""
echo "== Sweep complete. Grep RESULT lines above; outcome=OOM-KILLED is the failure signal. =="
echo "== A surviving run whose peak_mem_pct is in the high 90s is NOT a safe configuration -- =="
echo "== see README.md's 2026-08-27 update. =="
