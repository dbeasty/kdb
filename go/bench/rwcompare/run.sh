#!/usr/bin/env bash
# Runs one rwcompare pass: a fresh server container (kdb or mongo) with fixed CPU/memory limits,
# and the rwcompare client in a second container on the same Docker network, so neither database
# pays Docker Desktop's host port proxy (~0.8 ms per round trip on macOS).
#
# Usage: run.sh <kdb|mongo> <durability: fast|durable> <label> [rwcompare flags...]
#   fast    = acknowledged from memory, flushed in the background within 100 ms
#             (kdb --durability async --async-sync-interval-ms 100; mongo {w:1}, its default)
#   durable = acknowledged once synced (kdb --durability sync; mongo {j:true})
#
# Environment:
#   KDB_BIN       linux kdb-service binary to run (default: build from this checkout)
#   KDB_FLAGS     extra kdb-service flags
#   SERVER_CPUS   server container CPUs (default 4)   SERVER_MEM  (default 3g)
#   CLIENT_CPUS   client container CPUs (default 4)
#   OUT           results directory (default ./results/<date>)
#   MAX_LOAD      wait until the host's 1-minute load average is below this (default 8; this
#                 Mac idles at ~5 with simulators and the Docker VM running)
set -euo pipefail

target=$1 durability=$2 label=$3
shift 3
here=$(cd "$(dirname "$0")" && pwd)
go_root=$(cd "$here/../.." && pwd)
out=${OUT:-$here/results/$(date +%Y-%m-%d)}
mkdir -p "$out"
cpus=${SERVER_CPUS:-4} mem=${SERVER_MEM:-3g} ccpus=${CLIENT_CPUS:-4}
net=rwbench
docker network inspect $net >/dev/null 2>&1 || docker network create $net >/dev/null

arch=$(docker info --format '{{.Architecture}}')
case $arch in aarch64|arm64) goarch=arm64 ;; *) goarch=amd64 ;; esac
bin_dir=$out/bin
mkdir -p "$bin_dir"
(cd "$here" && CGO_ENABLED=0 GOOS=linux GOARCH=$goarch go build -o "$bin_dir/rwcompare" .)
if [[ -z ${KDB_BIN:-} && $target == kdb ]]; then
  (cd "$go_root" && CGO_ENABLED=0 GOOS=linux GOARCH=$goarch go build -o "$bin_dir/kdb-service" ./cmd/kdb-service)
  KDB_BIN=$bin_dir/kdb-service
fi

wait_quiet() {
  local max=${MAX_LOAD:-8} waited=0
  while :; do
    local load
    load=$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || uptime | awk -F'load averages?: ' '{print $2}' | cut -d' ' -f1)
    if awk -v l="$load" -v m="$max" 'BEGIN{exit !(l < m)}'; then break; fi
    if (( waited % 60 == 0 )); then echo "# waiting for load $load < $max"; ps -Ao pcpu,etime,comm -r | sed -n 2,4p || true; fi
    sleep 5; waited=$((waited + 5))
  done
  echo "# load: $(uptime | sed 's/.*load/load/')"
}

server=rw-$target
docker rm -f $server >/dev/null 2>&1 || true
docker volume rm $server-data >/dev/null 2>&1 || true
docker volume create $server-data >/dev/null

client_flags=()
case $target in
  kdb)
    dur_flags=(--durability sync)
    [[ $durability == fast ]] && dur_flags=(--durability async --async-sync-interval-ms 100)
    docker run -d --name $server --network $net --cpus "$cpus" --memory "$mem" \
      -v $server-data:/data -v "$(dirname "$KDB_BIN")":/opt/kdb:ro \
      --platform linux/amd64 gcr.io/distroless/static-debian12:latest \
      /opt/kdb/"$(basename "$KDB_BIN")" --data-dir /data --admin-addr 0.0.0.0:9093 \
      --sql-addr 'tcp://0.0.0.0:9090?bind=true' --peer-addr 'tcp://0.0.0.0:9091?bind=true' \
      --stream-addr 'tcp://0.0.0.0:9092?bind=true' "${dur_flags[@]}" ${KDB_FLAGS:-} >/dev/null
    addr=$server:9090
    ;;
  mongo)
    docker run -d --name $server --network $net --cpus "$cpus" --memory "$mem" \
      -v $server-data:/data/db mongo:7 --quiet >/dev/null
    addr=mongodb://$server:27017
    [[ $durability == durable ]] && client_flags=(-mongo-j)
    ;;
  *) echo "target must be kdb or mongo" >&2; exit 2 ;;
esac
# The client retries its connect for up to a minute while the server starts.

wait_quiet
docker stats --no-stream --format '# server {{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' $server
docker run --rm --network $net --cpus "$ccpus" -v "$bin_dir":/opt/bench:ro -v "$out":/out \
  --platform linux/amd64 gcr.io/distroless/static-debian12:latest /opt/bench/rwcompare \
  -target "$target" -addr "$addr" -label "$label-$durability" -json /out/results.jsonl \
  ${client_flags[@]+"${client_flags[@]}"} "$@" | tee -a "$out/$target-$label-$durability.txt"
docker stats --no-stream --format '# server {{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' $server | tee -a "$out/$target-$label-$durability.txt"
docker run --rm -v $server-data:/d golang:1.26 du -sh /d | sed 's/^/# disk /' | tee -a "$out/$target-$label-$durability.txt"
docker rm -f $server >/dev/null
