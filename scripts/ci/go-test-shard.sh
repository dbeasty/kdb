#!/usr/bin/env bash
# Runs one shard of the Go test suite, as CI's go-test and go-windows matrix jobs do.
#
#   scripts/ci/go-test-shard.sh <profile> <shard> [extra go test flags...]
#   scripts/ci/go-test-shard.sh linux-race embed -short
#   scripts/ci/go-test-shard.sh list linux-race          # print each shard's invocations
#
# A shard is one or more `go test` invocations, started in parallel and reported in order. The
# shards of a profile are balanced by measured duration (go test -json from CI runners): kdb/embed
# and kdb/server are too big for one job, so they are split by test name with -run, and the last
# piece of each uses -skip with the union of the others' patterns - a test that matches no
# pattern still runs. The `rest` shard is every package no other shard names, so a new package is
# tested without anyone editing this file.
#
# PR CI passes -short (the heavy, measurement and soak tests skip or scale down; they run in the
# nightly workflow), and the nightly runs the same shards without it. Locally, `go test ./...`
# from go/ still runs everything, and `go test -short ./...` is what a PR runs.
#
# Every invocation's go test -json lands in $GO_TEST_JSON_DIR (default: $TMPDIR/kdb-go-test-json),
# which CI uploads: that is the data to rebalance the shards from when one grows.
set -euo pipefail
set -f # the -run patterns contain [A-C]: never glob them

cd "$(dirname "$0")/../../go"

# Packages whose tests build or run code outside their own import graph - Go binaries (go build
# ./cmd/...) or the Kotlin CLI through gradle. Go's test cache cannot see those inputs, so these
# always run (-count=1) instead of reporting a cached pass from an older build.
uncached_pkgs="./kdb/integration ./cmd/kdb-inspect"

short=0
for a in "$@"; do
  [ "$a" = -short ] && short=1
done

# Each line: [go test flags...] <packages...>
shards_linux_race() {
  case "$1" in
  embed)
    echo "-run ^Test[A-C] ./kdb/embed"
    echo "-run ^Test[D-N] ./kdb/embed"
    echo "-run ^TestO ./kdb/embed"
    echo "-skip ^Test[A-O] ./kdb/embed"
    ;;
  server)
    echo "-run ^Test[A-M] ./kdb/server"
    echo "-skip ^Test[A-M] ./kdb/server"
    echo "./kdb/index ./kdb/document"
    ;;
  integration)
    # The Go<->Kotlin interop smoke (TestKotlinPutThenGoGet_InteropDelta) runs gradle; the job
    # sets up Java and the Gradle cache for it.
    echo "./kdb/integration ./cmd/kdb-inspect ./cmd/kdb-service"
    ;;
  storage)
    echo "./kdb/peersync ./kdb/control ./kdb/replication"
    echo "./kdb/storage/sstable ./kdb/backup ./kdb/storage/engine"
    ;;
  index)
    echo "./kdb/index/vector ./kdb/storage/delta ./kdb/index/fulltext ./kdb/dag ./kdb/client"
    ;;
  rest) rest_of linux-race ;;
  *) return 1 ;;
  esac
}

shards_windows() {
  case "$1" in
  embed)
    echo "-run ^Test[A-N] ./kdb/embed"
    echo "-skip ^Test[A-N] ./kdb/embed"
    ;;
  server)
    echo "./kdb/server"
    # Under -short the Kotlin interop smoke runs on Linux only (the go-test integration shard):
    # on Windows its gradle build would be most of the job. The nightly runs the full set here.
    if [ "$short" = 1 ]; then
      echo "-skip ^TestKotlin ./kdb/integration"
    else
      echo "./kdb/integration"
    fi
    echo "./kdb/control ./kdb/backup ./cmd/kdb-inspect ./cmd/kdb/cli"
    ;;
  rest) rest_of windows ;;
  *) return 1 ;;
  esac
}

shard_names() {
  case "$1" in
  linux-race) echo "embed server integration storage index rest" ;;
  windows) echo "embed server rest" ;;
  *) echo "unknown profile $1" >&2; return 1 ;;
  esac
}

shard_lines() { # profile shard
  case "$1" in
  linux-race) shards_linux_race "$2" ;;
  windows) shards_windows "$2" ;;
  esac
}

# Every package with tests that the profile's other shards do not name.
rest_of() {
  local named="" s line
  for s in $(shard_names "$1"); do
    [ "$s" = rest ] && continue
    while read -r line; do
      for w in $line; do
        case "$w" in ./*) named="$named $w" ;; esac
      done
    done < <(shard_lines "$1" "$s")
  done
  # ./... minus the named packages; packages without test files are left to go vet and the builds.
  go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./... |
    sed "s#^$(go list -m)#.#" |
    while read -r p; do
      case " $named " in *" $p "*) ;; *) printf '%s ' "$p" ;; esac
    done
  echo
}

if [ "${1:-}" = list ]; then
  for s in $(shard_names "$2"); do
    echo "== $s"
    shard_lines "$2" "$s" | sed 's/^/   go test /'
  done
  exit 0
fi

profile="${1:?usage: go-test-shard.sh <linux-race|windows> <shard> [go test flags...]}"
shard="${2:?usage: go-test-shard.sh <linux-race|windows> <shard> [go test flags...]}"
shift 2
extra=("$@")
case "$profile" in
linux-race) extra=(-race ${extra[@]+"${extra[@]}"}) ;;
esac

lines=()
while IFS= read -r line; do
  [ -n "$line" ] && lines+=("$line")
done < <(shard_lines "$profile" "$shard" || { echo "unknown shard $shard for $profile" >&2; exit 2; })
[ "${#lines[@]}" -gt 0 ] || { echo "shard $shard has no packages" >&2; exit 2; }

out="${GO_TEST_JSON_DIR:-${TMPDIR:-/tmp}/kdb-go-test-json}"
mkdir -p "$out"

# Build every test binary once first, so the parallel invocations below (four of them on the same
# package for kdb/embed) don't each compile it - and so the log separates compile from test time.
pkgs=()
for line in "${lines[@]}"; do
  for w in $line; do
    case "$w" in ./*) pkgs+=("$w") ;; esac
  done
done
start=$SECONDS
echo "::group::compile ${#pkgs[@]} test packages"
go test ${extra[@]+"${extra[@]}"} -count=1 -run '^$' "${pkgs[@]}" >/dev/null ||
  echo "compiling failed; the runs below report it"
echo "::endgroup::"
echo "compiled in $((SECONDS - start))s"

pids=()
i=0
for line in "${lines[@]}"; do
  read -ra words <<<"$line"
  count=()
  for w in "${words[@]}"; do
    case " $uncached_pkgs " in *" $w "*) count=(-count=1) ;; esac
  done
  echo "go test ${extra[*]-} ${count[*]-} $line"
  go test ${extra[@]+"${extra[@]}"} ${count[@]+"${count[@]}"} -json "${words[@]}" >"$out/$profile-$shard-$i.json" 2>"$out/$profile-$shard-$i.stderr" &
  pids+=($!)
  i=$((i + 1))
done

failed=0
for i in "${!pids[@]}"; do
  if ! wait "${pids[$i]}"; then
    failed=1
  fi
done

# Report: each package's result and time, the slowest tests, and in full the output of anything
# that failed (a failing test's own output, a package that failed outside a test, a build error).
for i in "${!lines[@]}"; do
  f="$out/$profile-$shard-$i.json"
  echo "::group::go test ${lines[$i]}"
  jq -r 'select(.Test == null and (.Action == "pass" or .Action == "fail" or .Action == "skip"))
         | "\(.Action | ascii_upcase)\t\(.Package)\t\(.Elapsed // 0)s"' "$f" || true
  echo "slowest tests:"
  jq -r 'select(.Test != null and (.Action == "pass" or .Action == "fail") and (.Test | contains("/") | not))
         | "\(.Elapsed)\t\(.Package | split("/") | last)\t\(.Test)"' "$f" | sort -rn | head -8 || true
  echo "::endgroup::"
  cat "$out/$profile-$shard-$i.stderr"
  failures="$(jq -c -s '[.[] | select(.Action == "fail") | [.Package, (.Test // "")]] | unique' "$f" || echo '[["?",""]]')"
  if [ "$failures" != "[]" ]; then
    jq -j --argjson failed "$failures" '
      select(.Action == "build-output"
             or (.Action == "output" and ([.Package, (.Test // "")] as $k | $failed | any(.[]; . == $k))))
      | .Output' "$f" | sed -e 's/\r$//' || true
    echo "FAILED:"
    jq -r '.[] | "  \(.[0]) \(.[1])"' <<<"$failures"
  fi
done
exit "$failed"
