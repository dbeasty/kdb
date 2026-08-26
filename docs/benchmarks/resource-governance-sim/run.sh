#!/usr/bin/env bash
# Verifies kdb-spec-layer13's resource-governance behavior (memory admission, bounded write
# queues, typed client errors, orderly abort, crash-only durability) against real low-memory and
# low-CPU Docker resource limits. See README.md for what each scenario proves and doesn't.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
IMAGE_TAG="kdb-service-lightsail-sim"
PORT_BASE="${PORT_BASE:-19190}"
# Scenario 1 needs a budget genuinely tight relative to a small, bounded write workload, so the
# trip is driven by transient concurrency overhead (many simultaneous connections) rather than
# permanent commit-history growth outrunning the budget outright - see that scenario's own
# comment on why. Tuned empirically (see kdb-pressure-test's -max-successful-writes doc comment
# for why bounding permanent growth is what makes this distinction possible at all); a container
# has different baseline overhead than a bare host process, so this may need retuning per
# platform - MEMORY_TIGHT_NOZOMBIE is the knob for that.
MEMORY_TIGHT_NOZOMBIE="${MEMORY_TIGHT_NOZOMBIE:-40m}"
# Scenario 2's *container* memory ceiling stays generous (150m): what needs to be tight is the
# *guard's* threshold (MEMORY_LIMIT_MB_ABORT below), not the container's hard cap. A tight
# container cap was tried first and found to backfire: once enough commits had accumulated to
# trip the guard, the same commits' replay after the resulting restart needed more memory than
# that same tight container had, so the kernel OOM-killed the replay itself before the process
# could even finish opening - a real, worth-documenting finding (kdb-spec-layer13 §7.5's "restart
# replay time/memory grows with history" point, sharper than expected: it can make restart
# outright impossible, not just slow, if the container is sized only for steady-state and not
# for replaying what pressure already let accumulate). A generous container cap with a tight
# guard threshold leaves headroom for exactly that replay.
MEMORY_TIGHT="${MEMORY_TIGHT:-150m}"
MEMORY_LIMIT_MB_ABORT="${MEMORY_LIMIT_MB_ABORT:-15}"
CPUS_LOW="${CPUS_LOW:-0.25}"
DATA_ROOT="$(mktemp -d)"
PRESSURE_TEST_BIN="/tmp/kdb-pressure-test-$$"

FAIL=0

cleanup() {
  local status=$?
  set +e
  docker rm -f kdb-rgsim-memory kdb-rgsim-abort kdb-rgsim-cpu >/dev/null 2>&1
  rm -rf "$DATA_ROOT"
  rm -f "$PRESSURE_TEST_BIN"
  exit "$status"
}
trap cleanup EXIT

log() { echo "== $* =="; }

# Portable "150m"/"1g"/"512k" -> MB, without relying on GNU coreutils' numfmt (not present on
# macOS/BSD, where this harness is also expected to run).
mem_limit_to_mb() {
  local spec="$1" num unit
  num="${spec%[a-zA-Z]*}"
  unit="${spec: -1}"
  case "$unit" in
    [Gg]) echo $(( num * 1024 )) ;;
    [Mm]) echo "$num" ;;
    [Kk]) echo $(( num / 1024 )) ;;
    *) echo "$num" ;; # bare number: already MB
  esac
}
assert() {
  local desc="$1" cond="$2"
  if eval "$cond"; then
    echo "PASS: $desc"
  else
    echo "FAIL: $desc"
    FAIL=1
  fi
}

wait_for_port() {
  local port="$1" tries="${2:-60}"
  for i in $(seq 1 "$tries"); do
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
      exec 3>&- 3<&-
      return 0
    fi
    sleep 0.5
  done
  return 1
}

log "Building server image"
docker build -q -t "$IMAGE_TAG" -f "$REPO_ROOT/docs/benchmarks/lightsail-sim/Dockerfile" "$REPO_ROOT" >/dev/null

log "Building pressure-test client (host, unconstrained)"
( cd "$REPO_ROOT/go" && go build -o "$PRESSURE_TEST_BIN" ./cmd/kdb-pressure-test )

scenario_memory_no_zombie() {
  local port=$((PORT_BASE))
  log "Scenario 1: low-memory (${MEMORY_TIGHT_NOZOMBIE}), no-zombie"
  docker rm -f kdb-rgsim-memory >/dev/null 2>&1 || true
  docker run -d --name kdb-rgsim-memory \
    --memory="$MEMORY_TIGHT_NOZOMBIE" --memory-swap="$MEMORY_TIGHT_NOZOMBIE" --cpus=2 \
    -p "${port}:9090" \
    "$IMAGE_TAG" \
    --memory --sql-addr "tcp://0.0.0.0:9090?bind=true" --namespace demo/rgsim \
    --memory-limit-mb "$(( $(mem_limit_to_mb "$MEMORY_TIGHT_NOZOMBIE") * 65 / 100 ))" \
    >/dev/null
  wait_for_port "$port" || { echo "FAIL: server never became reachable"; docker logs kdb-rgsim-memory; FAIL=1; return; }

  # Deliberately no -max-successful-writes cap here: a small, fixed memory budget against a
  # sustained write burst *will* permanently outgrow it (every write is a permanent commit -
  # kdb-spec-layer13 §2.11/§10, still-open future work is wiring compaction to bound this). That
  # is real and expected, not a regression - this scenario tests what should hold regardless:
  # that the client sees typed BUSY responses instead of dropped connections while it happens,
  # and that the server keeps running and keeps serving what it already has (reads are never
  # gated - see MemoryGuard's own doc comment). Whether *this exact* run also recovers depends on
  # how its permanent growth compares to its budget, which varies with hardware/Docker version/
  # doc size - not something to assert a fixed pass/fail on. The property that pressure itself is
  # reversible (not the pre-Component-48 permanent Sys latch) is what
  # TestMemoryGuardPressureClearsOnItsOwn proves deterministically in go/kdb/server; this scenario
  # is the "does the typed-error and keep-serving-reads behavior hold under a real container and
  # real wire traffic" complement to that, not a re-proof of reversibility itself.
  "$PRESSURE_TEST_BIN" \
    -addr "127.0.0.1:${port}" -namespace demo/rgsim \
    -concurrency 64 -burst-duration 20s -doc-bytes 2000 -doc-pool 200 \
    -cooldown 4s -verify-writes 20 -connect-timeout 20s \
    > /tmp/rgsim-memory-result.log 2>&1 || true
  cat /tmp/rgsim-memory-result.log

  local result_line
  result_line="$(grep '^RESULT ' /tmp/rgsim-memory-result.log || true)"
  assert "server stayed reachable and printed a RESULT line" "[ -n \"\$result_line\" ]"
  assert "at least one write (burst or post-cooldown verify) was rejected as BUSY (proves typed rejection under pressure, not silent accumulation or a dropped connection)" \
    "echo \"\$result_line\" | grep -qE 'busy=[1-9]'"
  assert "no unavailable/other-error responses (server never went unreachable or returned unclassified errors)" \
    "echo \"\$result_line\" | grep -qE 'unavailable=0 .*other_errors=0'"
  assert "container is still running (no OOM-kill, no crash - shedding writes, not dying)" \
    "[ \"\$(docker inspect kdb-rgsim-memory --format '{{.State.Running}}')\" = 'true' ]"
  if echo "$result_line" | grep -q 'recovered=true'; then
    echo "INFO: this run's permanent write volume also stayed under budget, so pressure recovered on its own - not asserted, since that ratio is workload/hardware-dependent (see the comment above); the deterministic proof of reversibility itself is go/kdb/server's TestMemoryGuardPressureClearsOnItsOwn"
  else
    echo "INFO: this run's permanent write volume outgrew the budget (expected for a sustained burst against a small fixed budget - kdb-spec-layer13 §10, compaction is still future work) - not a failure of this scenario"
  fi
  docker inspect kdb-rgsim-memory --format 'container state: running={{.State.Running}} oomKilled={{.State.OOMKilled}} exitCode={{.State.ExitCode}}'
  docker rm -f kdb-rgsim-memory >/dev/null 2>&1 || true
}

scenario_memory_abort_restart() {
  local port=$((PORT_BASE + 1))
  local data_dir="$DATA_ROOT/abort-restart"
  mkdir -p "$data_dir"
  log "Scenario 2: low-memory (${MEMORY_TIGHT}) + orderly abort + Docker restart"
  docker rm -f kdb-rgsim-abort >/dev/null 2>&1 || true
  docker run -d --name kdb-rgsim-abort \
    --memory="$MEMORY_TIGHT" --memory-swap="$MEMORY_TIGHT" --cpus=2 \
    --restart=on-failure:3 \
    -p "${port}:9090" \
    -v "${data_dir}:/data" \
    "$IMAGE_TAG" \
    --data-dir /data --sql-addr "tcp://0.0.0.0:9090?bind=true" --namespace demo/rgsim \
    --memory-limit-mb "$MEMORY_LIMIT_MB_ABORT" \
    --abort-after 3s \
    >/dev/null
  wait_for_port "$port" || { echo "FAIL: server never became reachable"; docker logs kdb-rgsim-abort; FAIL=1; return; }

  # A hard cap on successful writes keeps the persisted history small enough that a subsequent
  # restart's replay reliably fits the container's memory - see MEMORY_LIMIT_MB_ABORT's comment
  # above for why an uncapped burst here backfired the first time this scenario was built.
  "$PRESSURE_TEST_BIN" \
    -addr "127.0.0.1:${port}" -namespace demo/rgsim \
    -concurrency 16 -burst-duration 30s -doc-bytes 2000 -doc-pool 300 -max-successful-writes 150 \
    -cooldown 2s -verify-writes 5 -connect-timeout 30s \
    -verify-doc-id "00000000-0000-0000-0000-0000000000ab" \
    -verify-doc-value '{"before":"abort"}' \
    > /tmp/rgsim-abort-result.log 2>&1 || true
  cat /tmp/rgsim-abort-result.log

  log "Waiting for the container to have exited at least once with code 75 (orderly abort) and be restarted by Docker"
  local saw_abort=0
  for i in $(seq 1 60); do
    local exit_code restart_count running
    exit_code="$(docker inspect kdb-rgsim-abort --format '{{.State.ExitCode}}' 2>/dev/null || echo -1)"
    restart_count="$(docker inspect kdb-rgsim-abort --format '{{.RestartCount}}' 2>/dev/null || echo 0)"
    running="$(docker inspect kdb-rgsim-abort --format '{{.State.Running}}' 2>/dev/null || echo false)"
    if [ "$restart_count" != "0" ] || { [ "$running" = "false" ] && [ "$exit_code" = "75" ]; }; then
      saw_abort=1
      break
    fi
    sleep 1
  done
  assert "container exited with code 75 (orderly abort) and/or Docker restarted it (RestartCount > 0)" \
    "[ \"$saw_abort\" = 1 ]"
  docker inspect kdb-rgsim-abort --format 'exitCode={{.State.ExitCode}} restartCount={{.RestartCount}} running={{.State.Running}} oomKilled={{.State.OOMKilled}}'
  docker logs --tail 30 kdb-rgsim-abort 2>&1 | grep -i "orderly abort" || echo "(no orderly-abort log line found - see full logs below)"

  wait_for_port "$port" 60
  # Give the (now-restarted) server a moment to finish replaying its delta log.
  sleep 1
  "$PRESSURE_TEST_BIN" \
    -addr "127.0.0.1:${port}" -namespace demo/rgsim \
    -concurrency 1 -burst-duration 1s -verify-writes 3 -cooldown 1s \
    -connect-timeout 20s \
    -verify-doc-id "00000000-0000-0000-0000-0000000000ab" \
    -verify-doc-value '{"before":"abort"}' -verify-doc-write=false \
    > /tmp/rgsim-abort-postcheck.log 2>&1 || true
  cat /tmp/rgsim-abort-postcheck.log
  assert "the document written before the abort is intact after restart - no repair step, no corruption (kdb-spec-layer13 Component 47)" \
    "grep -q 'data_intact=true' /tmp/rgsim-abort-postcheck.log"
  # Informational, not asserted: MEMORY_LIMIT_MB_ABORT is deliberately kept small enough that the
  # persisted history alone (already present the instant replay finishes) re-trips the guard
  # immediately on restart, same as it did before the abort - a real deployment recovering from
  # this would raise the memory budget (or, longer-term, rely on compaction - kdb-spec-layer13
  # §10, still future work), not expect an unmodified tight budget to suddenly start admitting
  # writes it already couldn't. The property this scenario asserts is that the *restart itself*
  # works cleanly with the data intact - not that an unmodified undersized config self-heals.
  if grep -qE 'RESULT.*successes=[1-9]' /tmp/rgsim-abort-postcheck.log; then
    echo "INFO: the restarted server also accepted new writes immediately"
  else
    echo "INFO: the restarted server's persisted history alone still exceeds MEMORY_LIMIT_MB_ABORT, so it correctly continues rejecting new writes until reconfigured or compacted - expected, not asserted"
  fi

  docker logs --tail 200 kdb-rgsim-abort 2>&1 | tail -60
  docker rm -f kdb-rgsim-abort >/dev/null 2>&1 || true
}

scenario_cpu_starved() {
  local port=$((PORT_BASE + 2))
  log "Scenario 3: low-CPU (${CPUS_LOW} vCPU), no memory constraint"
  docker rm -f kdb-rgsim-cpu >/dev/null 2>&1 || true
  docker run -d --name kdb-rgsim-cpu \
    --memory=1g --memory-swap=1g --cpus="$CPUS_LOW" \
    -p "${port}:9090" \
    "$IMAGE_TAG" \
    --memory --sql-addr "tcp://0.0.0.0:9090?bind=true" --namespace demo/rgsim \
    >/dev/null
  wait_for_port "$port" 60 || { echo "FAIL: server never became reachable under CPU starvation"; docker logs kdb-rgsim-cpu; FAIL=1; return; }

  "$PRESSURE_TEST_BIN" \
    -addr "127.0.0.1:${port}" -namespace demo/rgsim \
    -concurrency 8 -burst-duration 15s -doc-bytes 500 \
    -cooldown 2s -verify-writes 10 -connect-timeout 20s \
    > /tmp/rgsim-cpu-result.log 2>&1 || true
  cat /tmp/rgsim-cpu-result.log

  local result_line
  result_line="$(grep '^RESULT ' /tmp/rgsim-cpu-result.log || true)"
  assert "server produced a RESULT line under CPU starvation (never fully wedged)" "[ -n \"\$result_line\" ]"
  assert "at least some writes succeeded despite ${CPUS_LOW} vCPU (degraded, not dead)" \
    "echo \"\$result_line\" | grep -qE 'successes=[1-9]'"
  assert "container still running after sustained load under CPU starvation" \
    "[ \"\$(docker inspect kdb-rgsim-cpu --format '{{.State.Running}}')\" = 'true' ]"
  docker rm -f kdb-rgsim-cpu >/dev/null 2>&1 || true
}

SCENARIO="${1:-all}"
case "$SCENARIO" in
  memory-no-zombie) scenario_memory_no_zombie ;;
  memory-abort-restart) scenario_memory_abort_restart ;;
  cpu-starved) scenario_cpu_starved ;;
  all)
    scenario_memory_no_zombie
    scenario_memory_abort_restart
    scenario_cpu_starved
    ;;
  *)
    echo "usage: $0 [memory-no-zombie|memory-abort-restart|cpu-starved|all]" >&2
    exit 2
    ;;
esac

echo ""
if [ "$FAIL" -ne 0 ]; then
  echo "== One or more assertions FAILED - see PASS/FAIL lines above =="
  exit 1
fi
echo "== All assertions PASSED =="
