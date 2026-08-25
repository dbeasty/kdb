#!/usr/bin/env bash
# Approximates AWS Lightsail's $7/mo tier (1GB RAM, 2 vCPU) locally via Docker resource limits,
# runs kdb-service (Component 38's Go-native server) inside it, and drives sustained small-message
# read/write load against it from the host using the real wire client (go/kdb/client).
#
# This is an approximation, not a replacement for component 38 spec §7 test 8's real Lightsail
# run - see README.md for what it does and doesn't prove.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
IMAGE_TAG="kdb-service-lightsail-sim"
CONTAINER_NAME="kdb-lightsail-sim"
PORT="${PORT:-19090}"
MEMORY="${MEMORY:-1g}"
CPUS="${CPUS:-2}"
DURATION="${DURATION:-30s}"
CONCURRENCY="${CONCURRENCY:-16}"
READ_RATIO="${READ_RATIO:-0.5}"
STATS_FILE="$(mktemp)"

cleanup() {
  local status=$?
  set +e
  kill "${STATS_PID:-}" 2>/dev/null
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1
  rm -f "$STATS_FILE"
  exit "$status"
}
trap cleanup EXIT

echo "== Building server image (approximated tier: ${MEMORY} RAM, ${CPUS} vCPU) =="
docker build -t "$IMAGE_TAG" -f "$REPO_ROOT/docs/benchmarks/lightsail-sim/Dockerfile" "$REPO_ROOT"

echo "== Starting container with resource limits =="
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER_NAME" \
  --memory="$MEMORY" --memory-swap="$MEMORY" --cpus="$CPUS" \
  -p "${PORT}:9090" \
  "$IMAGE_TAG" \
  --memory --sql-addr "tcp://0.0.0.0:9090?bind=true" --namespace demo/loadtest \
  >/dev/null

echo "== Waiting for server to accept connections on 127.0.0.1:${PORT} =="
for i in $(seq 1 30); do
  if (exec 3<>"/dev/tcp/127.0.0.1/${PORT}") 2>/dev/null; then
    exec 3>&- 3<&-
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "server never became reachable; container logs:" >&2
    docker logs "$CONTAINER_NAME" >&2
    exit 1
  fi
  sleep 0.5
done

echo "== Sampling container resource usage in the background =="
( while docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; do
    docker stats --no-stream --format '{{.MemUsage}}\t{{.CPUPerc}}' "$CONTAINER_NAME" >> "$STATS_FILE" 2>/dev/null || true
    sleep 1
  done ) &
STATS_PID=$!

echo "== Building load generator (host, unconstrained) =="
( cd "$REPO_ROOT/go" && go build -o /tmp/kdb-loadtest ./cmd/kdb-loadtest )

echo "== Running load test: concurrency=${CONCURRENCY} duration=${DURATION} read_ratio=${READ_RATIO} =="
set +e
/tmp/kdb-loadtest \
  -addr "127.0.0.1:${PORT}" \
  -namespace demo/loadtest \
  -concurrency "$CONCURRENCY" \
  -duration "$DURATION" \
  -read-ratio "$READ_RATIO"
LOADTEST_STATUS=$?
set -e

kill "$STATS_PID" 2>/dev/null || true
wait "$STATS_PID" 2>/dev/null || true

echo ""
echo "== Container resource usage during the run (${MEMORY} RAM / ${CPUS} vCPU limit) =="
if [ -s "$STATS_FILE" ]; then
  echo "mem_usage	cpu_percent"
  cat "$STATS_FILE"
  echo ""
  echo "peak memory: $(cut -f1 "$STATS_FILE" | cut -d/ -f1 | sort -h | tail -1)"
else
  echo "no samples captured"
fi

echo ""
echo "== Server container status =="
docker inspect "$CONTAINER_NAME" --format 'running={{.State.Running}} exitCode={{.State.ExitCode}} oomKilled={{.State.OOMKilled}} error={{.State.Error}}' 2>/dev/null || echo "container gone"

echo ""
echo "== Server process check (no JVM anywhere - Go binary only) =="
docker exec "$CONTAINER_NAME" ps aux 2>/dev/null || docker top "$CONTAINER_NAME" 2>/dev/null || echo "(container not running - see logs below)"

echo ""
echo "== Server container logs (last 200 lines) =="
docker logs --tail 200 "$CONTAINER_NAME" 2>&1 || echo "(no logs available)"

exit "$LOADTEST_STATUS"
