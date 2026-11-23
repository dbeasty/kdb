#!/usr/bin/env bash
# prof.sh <dir-holding-linux-kdb-service> <out-dir> <cells> <concurrency> [fast|durable]
# Starts kdb-service in a container with its admin port on 127.0.0.1:19093, runs rwcompare's load
# phase plus <cells>, and once the line matching WAIT_FOR (default "loaded") appears waits
# PROF_DELAY seconds (default 4) and takes a 15 s CPU profile, a heap profile and a goroutine dump.
# Profile the cell in the order the matrix runs it: a cell's cost can depend on what ran before.
#   OPS=1500000 ./prof.sh /tmp/bin out update 32
#   WAIT_FOR="mixed50 +c=8 " PROF_DELAY=1 ./prof.sh /tmp/bin out read,update,mixed50 1,8,32
#   go tool pprof -top -cum out/cpu-*.pb.gz
set -euo pipefail
bindir=$1 out=$2 cells=$3 conc=$4 dur=${5:-fast}
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$out" "$out/bin"
net=rwbench
docker network inspect $net >/dev/null 2>&1 || docker network create $net >/dev/null
(cd "$here" && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o "$out/bin/rwcompare" .)
docker rm -f rw-kdb-prof >/dev/null 2>&1 || true
docker volume rm rw-kdb-prof-data >/dev/null 2>&1 || true
dur_flags=(--durability sync)
[[ $dur == fast ]] && dur_flags=(--durability async --async-sync-interval-ms 100)
docker run -d --name rw-kdb-prof --network $net --cpus 4 --memory 3g -p 127.0.0.1:19093:9093 \
  -v rw-kdb-prof-data:/data -v "$bindir":/opt/kdb:ro --platform linux/amd64 gcr.io/distroless/static-debian12:latest \
  /opt/kdb/kdb-service --data-dir /data --admin-addr 0.0.0.0:9093 \
  --sql-addr 'tcp://0.0.0.0:9090?bind=true' "${dur_flags[@]}" ${KDB_FLAGS:-} >/dev/null
tag=$(echo "$cells-$conc" | tr ',' '_')
log=$out/client-$tag.txt
ops=()
[[ -n ${OPS:-} ]] && ops=(-ops "$OPS")
docker run --rm --network $net --cpus 4 -v "$out/bin":/opt/bench:ro --platform linux/amd64 gcr.io/distroless/static-debian12:latest \
  /opt/bench/rwcompare -target kdb -addr rw-kdb-prof:9090 -label prof -docs "${DOCS:-100000}" \
  -cells "$cells" -concurrency "$conc" ${ops[@]+"${ops[@]}"} > "$log" 2>&1 &
cpid=$!
until grep -q -E "${WAIT_FOR:-loaded}" "$log" 2>/dev/null; do
  sleep 1
  kill -0 $cpid 2>/dev/null || { cat "$log"; echo "client exited before ${WAIT_FOR:-loaded}" >&2; exit 1; }
done
sleep "${PROF_DELAY:-4}"
uptime | sed 's/.*load/# load/' | tee "$out/load-$tag.txt"
curl -s -o "$out/cpu-$tag.pb.gz" "http://127.0.0.1:19093/debug/pprof/profile?seconds=15"
curl -s -o "$out/heap-$tag.pb.gz" "http://127.0.0.1:19093/debug/pprof/heap"
curl -s -o "$out/goroutine-$tag.txt" "http://127.0.0.1:19093/debug/pprof/goroutine?debug=2"
docker stats --no-stream --format 'server cpu={{.CPUPerc}} mem={{.MemUsage}}' rw-kdb-prof | tee -a "$out/load-$tag.txt"
wait $cpid || true
cat "$log"
docker rm -f rw-kdb-prof >/dev/null
