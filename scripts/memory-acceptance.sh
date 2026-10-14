#!/usr/bin/env bash
# Memory acceptance run (docs/kdb-memory-cassandra-plan.md): build a large namespace, then run each
# phase of TestMemoryProfile in its own process and check that every phase's peak heap stays
# within a quarter of the namespace's size on disk. Prints a table and exits 1 if any phase is over.
#
#   scripts/memory-acceptance.sh                     # 10M documents, tree.file + bounded bodies on
#   DOCS=1000000 scripts/memory-acceptance.sh        # smaller
#   TREE=0 BOUNDED=0 scripts/memory-acceptance.sh    # the defaults instead
#
# Needs an otherwise quiet machine for the timings it also prints; the memory figures it judges by
# are not sensitive to load. 10M documents take ~4 minutes to build and ~3.5 GB of disk. Run it
# under the shared benchmark lock when other sessions are benchmarking.
set -euo pipefail

DOCS=${DOCS:-10000000}
COMMITS=${COMMITS:-$DOCS}
BATCH=${BATCH:-1000}
TREE=${TREE:-1}
BOUNDED=${BOUNDED:-1}
GRAPH=${GRAPH:-1}
PHASES=${PHASES:-"build open reads fullread writes checkpoint"}
# BOOTSTRAP=1 adds a peer bootstrap: an empty node installing a snapshot of a DOCS-document node
# (replication TestSnapshotScale). Both nodes run in one process, so its peak heap covers host and
# receiver together - stricter than either node alone.
BOOTSTRAP=${BOOTSTRAP:-0}
WORK=${WORK:-$(mktemp -d -t kdb-memacc)}
KEEP=${KEEP:-0}
# Phases have fixed costs (the writes phase always makes 20,000 updates), so a small namespace's
# quarter is no budget at all; the rule is meant for the large runs.
MIN_BUDGET_MB=${MIN_BUDGET_MB:-256}

repo=$(cd "$(dirname "$0")/.." && pwd)
bin="$WORK/embed.test"
data="$WORK/data"
log="$WORK/run.log"
mkdir -p "$WORK"
echo "memory acceptance: $DOCS documents, $COMMITS writes ($BATCH to a commit), tree.file=$TREE bounded=$BOUNDED graph=$GRAPH"
echo "work dir $WORK; load: $(uptime | sed 's/.*load averages*: //')"
(cd "$repo/go" && go test -c -o "$bin" ./kdb/embed)

export KDB_MEMPROFILE_DIR="$data" KDB_MEMPROFILE_GRAPH="$GRAPH" KDB_MEMPROFILE_TREE="$TREE" \
	KDB_MEMPROFILE_BOUNDED="$BOUNDED" KDB_MEMPROFILE_DOCS="$DOCS" KDB_MEMPROFILE_COMMITS="$COMMITS" \
	KDB_MEMPROFILE_BATCH="$BATCH"
: >"$log"
for p in $PHASES; do
	if [ "$p" = build ]; then rm -rf "$data"; fi
	echo "--- phase $p ($(date +%H:%M:%S))"
	KDB_MEMPROFILE_PHASE=$p "$bin" -test.run TestMemoryProfile -test.v -test.timeout 120m 2>&1 | tee -a "$log" |
		grep -E 'PHASE|built|close [0-9]' || true
done

if [ "$BOOTSTRAP" = 1 ]; then
	echo "--- phase bootstrap ($(date +%H:%M:%S))"
	(cd "$repo/go" && go test -c -o "$WORK/replication.test" ./kdb/replication)
	KDB_TREE_FILE=$TREE KDB_BOUNDED_LIVE_BODIES=$BOUNDED KDB_GRAPH_FILE=$GRAPH KDB_SNAPSHOT_SCALE=$DOCS \
		"$WORK/replication.test" -test.run '^TestSnapshotScale$' -test.v -test.timeout 120m 2>&1 | tee -a "$log" |
		grep -E 'seeded|snapshot +[0-9]' || true
fi

data_kb=$(du -sk "$data" | cut -f1)
budget_mb=$((data_kb / 1024 / 4))
if [ "$budget_mb" -lt "$MIN_BUDGET_MB" ]; then budget_mb=$MIN_BUDGET_MB; fi
echo
echo "namespace on disk: $((data_kb / 1024)) MB; budget (a quarter, at least $MIN_BUDGET_MB MB): $budget_mb MB"
printf '%-11s %12s %12s %12s  %s\n' phase "peak heap" "live heap" "peak RSS" verdict
fail=0
while read -r line; do
	phase=$(sed -E 's/.*PHASE ([a-z]+) .*/\1/' <<<"$line")
	live=$(sed -nE 's/.*liveHeap=([0-9.]+)MB.*/\1/p' <<<"$line")
	rss=$(sed -nE 's/.*peakRSS=([0-9.]+)MB.*/\1/p' <<<"$line")
	peak=$(sed -nE 's/.*peakHeap[A-Za-z]*=([0-9.]+)MB.*/\1/p' <<<"$line")
	[ -n "$peak" ] || peak=$live # fullread reports no peak of its own
	verdict=ok
	if awk -v p="$peak" -v b="$budget_mb" 'BEGIN{exit !(p > b)}'; then
		verdict=OVER
		fail=1
	fi
	printf '%-11s %10s MB %10s MB %10s MB  %s\n' "$phase" "$peak" "$live" "$rss" "$verdict"
done < <(grep -E 'PHASE [a-z]+ ' "$log")
# The bootstrap row: "snapshot <docs> <recv> <requests> <peak heap+> ..." in MiB.
while read -r line; do
	peak=$(awk '{for (i = 1; i <= NF; i++) if ($i == "snapshot") { print $(i + 5); exit }}' <<<"$line")
	verdict=ok
	if awk -v p="$peak" -v b="$budget_mb" 'BEGIN{exit !(p > b)}'; then
		verdict=OVER
		fail=1
	fi
	printf '%-11s %10s MB %10s    %10s     %s\n' bootstrap "$peak" - - "$verdict (both nodes)"
done < <(grep -E 'replication_test|snapshot_scale_test' "$log" | grep -E ' snapshot +[0-9]')
grep -E 'sstables|merged' "$log" | sed 's/^/  note: /' | sort -u | head -5 || true
if [ "$KEEP" != 1 ]; then rm -rf "$data"; fi
echo "full log: $log"
if [ $fail = 1 ]; then
	echo "FAIL: at least one phase's peak heap is over the budget"
	exit 1
fi
echo "PASS"
