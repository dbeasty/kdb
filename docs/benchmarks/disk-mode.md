# Disk-mode benchmark: `--durability sync`, sync-mode full

Every earlier benchmark in this directory ran in memory or against a small working set. This one
loads a file-backed namespace at 10k, 100k and 1M documents with the default durability
(`DurabilitySync`, `SyncModeFull`: `F_FULLFSYNC` on macOS) and measures what an operator would
ask: how fast it loads, how fast a point read and a filtered SELECT are, how long a restart
takes, and how much memory the process holds after loading and after restarting.

Date: 2026-10-11. Branch `agent/bench-vector-parity` (from `feat/qtask-cutover-batch`).

## Machine

- Apple M3 Max, 16 cores, 48 GiB RAM, macOS 26.5.2 (Darwin 25.5.0), arm64, Go 1.26.
- Data directory on the internal APFS SSD (`/private/tmp`), not a RAM disk.
- **Not an idle machine.** Other agent sessions were running; load average was 10-16 during the
  10k and 100k runs. The 1M run's absolute numbers are likewise a lower bound on what an idle
  machine would do. The trends (which are the findings) are far larger than that noise.

## Method

`go/cmd/kdb-diskbench` opens the file runtime exactly as `kdb-service` does for a data directory
(`embed.OpenFileRuntimeWithOptions`, default `MemoryBudgetBytes`, sync + full), wrapped in
`server.NewKdbServerRuntime`. Two phases, in **separate processes** so "after restart" is a fresh
heap and RSS:

1. `load`: bulk-write N documents (about 200 bytes each: `{"n":i,"bucket":i%100,"name":...,"pad":...}`)
   in transactions of 1,000 `WriteOp`s each; then time 300 single-document upserts (one commit
   each, so an fsync that nobody shares); then take the "hot" read measurements; then close.
2. `reopen`: open the same directory in a new process, then take the same read measurements.

Measurements:

- **bulk write**: documents / wall second over the whole load (1,000-document commits).
- **single commit**: p50/p99 of one-document `Upsert`s after the bulk load.
- **point read**: `GetDocument` by id, 5,000 random ids; p50/p99.
- **filtered SELECT**: on a schemaless namespace every predicate is a full scan, so these
  are "examine every document" queries. `scan_one_row` is `SELECT kdb_id FROM docs WHERE n = <last>`
  (1 row returned); `scan_bucket` is `... WHERE bucket = 7 LIMIT 100`. Reported: the first
  execution and the p50 of the rest (5 runs, 3 at 1M). `by_kdb_id` is the keyed lookup, for contrast.
- **restart time**: wall time of `OpenFileRuntimeWithOptions` in the new process.
- **RSS**: `ps -o rss` of the benchmark process after open, after load, and after the read
  measurements.

## Commands

```
scripts/bench-disk.sh 10000 100000 1000000      # builds go/cmd/kdb-diskbench, prints one JSON object per phase
BENCH_DIR=/path/on/the/disk/under/test scripts/bench-disk.sh 100000     # choose the disk
# or by hand:
cd go && go build -o /tmp/kdb-diskbench ./cmd/kdb-diskbench
/tmp/kdb-diskbench load   -dir /path/data -n 100000 [-batch 1000 -reads 5000 -single 300]
/tmp/kdb-diskbench reopen -dir /path/data -n 100000
```

## Results

| | 10k | 100k | 1M |
|---|---:|---:|---:|
| **Bulk write** (1,000-doc commits) | 51,373 docs/s (0.19 s) | 54,148 docs/s (1.85 s) | **1,046 docs/s (955 s)** |
| Single-doc commit p50 / p99 | 4.16 / 9.87 ms (211/s) | 4.02 / 5.10 ms (243/s) | 7.14 / 10.72 ms (135/s) |
| **Point read, hot** (after load) p50 / p99 | 0.46 / 6.2 us | 1.08 / 8.3 us | 4.9 / 13.3 us |
| **Point read, after restart** p50 / p99 | 534 / 1,404 us | 540 / 1,577 us | 533 / 839 us |
| **Filtered SELECT (full scan), hot** p50 | 10.7 ms | 124 ms | 1,284 ms |
| `LIMIT 100` filtered SELECT, hot p50 | 11.4 ms | 121 ms | 1,251 ms |
| **Filtered SELECT, first run after restart** | 3.97 s | 59.98 s | **1,594.8 s (26.6 min)** |
| Filtered SELECT, later runs after restart p50 | 11.6 ms | 123 ms | 1,370 ms |
| `kdb_id = ...` SELECT (keyed), p50 | <0.01 ms | <0.01 ms | <0.01 ms |
| **Restart (reopen) time** | 127 ms | 1,036 ms | **10.3 s** |
| Close after load | 161 ms | 362 ms | 16.3 s |
| **RSS after load** (before reads) | 79 MB | 210 MB | 1,285 MB |
| RSS after load + reads | 101 MB | 228 MB | 1,656 MB |
| **RSS after restart** (open only) | 37 MB | 138 MB | 1,314 MB |
| RSS after restart + reads | 51 MB | 229 MB | 2,386 MB |
| On disk | 2 MB | 20 MB | 215 MB |

Raw JSON for every phase is what `scripts/bench-disk.sh` prints.

## What the numbers say

1. **Bulk-write throughput does not scale with namespace size.** It is flat to 100k
   (about 50k docs/s) and then collapses: the 1M load did its first 100k at ~49k docs/s, the first
   500k in 236 s (2,119 docs/s cumulative) and the last 500k in 719 s (about 700 docs/s), 955 s
   overall. Per-commit cost grows with the size of the namespace
   (a whole-tree step per commit), so loading N documents is super-linear. Not investigated
   further here; it needs its own unit.
2. **A cold scan after restart is catastrophic at 1M: 26.6 minutes**, versus 1.3 s for the same
   query once warm. The cost per document of that first scan grows with size (0.4 ms at 10k,
   0.6 ms at 100k, 1.6 ms at 1M), and it matches the 0.5 ms cost of a single cold point read:
   the first scan appears to fetch documents one at a time from disk instead of streaming
   them. The restart itself (`open`, 10 s) is cheap in comparison; the cost is deferred to the
   first query that touches every document. This is the largest finding of the run.
3. **Point reads are fast hot (microseconds) and ~0.5 ms cold**, independent of namespace size,
   so a keyed read is O(1) from disk. `kdb_id = ?` is served by the keyed lookup and never scans.
4. **Filtered SELECT is a linear scan: ~1.3 us per document hot** (10 ms / 124 ms / 1.3 s).
   The keyed and (declared-field) indexed paths avoid it.
5. **Single-document durability cost is the fsync**: 4.0-4.2 ms p50 (about 240 commits/s) up to 100k,
   matching `write-durability-modes.md`; 7.1 ms at 1M because the per-commit work above also
   grows. Batched commits amortise it (50k docs/s at small sizes).
6. **Memory is not bounded by `MemoryBudgetBytes`' 64 MiB default**: resident memory is about
   1.3 KB per document after load (1.3 GB at 1M) and climbs to 2.4 GB after a restart and a
   full scan, against 215 MB on disk. After a restart the process starts at 1.3 GB at 1M
   (138 MB at 100k) before any query.

## After the fixes this run motivated

The first run above exposed two defects, both now fixed (same machine, same commands, nothing else
heavy running; the first table is kept as the "before"):

| | before | after |
|---|---:|---:|
| Bulk write, first 100k docs | 49k docs/s | 58k docs/s |
| Bulk write, 1M docs total | 1,046 docs/s (955 s) | 50.6k docs/s (19.8 s) |
| First full scan after a restart, 10k | ~4 s | 36 ms |
| First full scan after a restart, 100k | 60 s | 0.64 s |
| First full scan after a restart, 1M | 26.6 min | 21 s |
| Same scan, warm, 1M | 1.2 s | 1.2 s |

**Bulk write collapsed past ~100k documents** because `docHashShard.evictLocked` walked the whole
recency list on every `Put`, skipping the pinned entries - and the live tree is pinned, so once a
shard's live data exceeded its budget every commit scanned every live document in the shard
(CPU profile: 72% of the load in that loop). Pinned entries now stay off the list.

**The first scan after a restart** decoded the whole delta-log commit a document lives in, once
for every document in it: a namespace loaded in 1,000-document commits cost 1,000x the decoding
(142 GB of allocation and 3,100 GCs for a 100k scan - the profile attributed the time to syscalls,
which was a macOS sampling artifact; reads themselves totalled 0.65 s). `ScanDocuments` now
resolves a window of entries first and decodes each frame once per window. The remaining
superlinearity (0.64 s at 100k, 21 s at 1M) is frames being re-decoded once per 32k-entry window;
a larger window or a frame-ordered scan would flatten it at the cost of holding more bodies.

**Memory, second pass.** The 2.2 GB resident figure after a restart plus a full scan at 1M documents
was mostly garbage, not data: the SSTable index cache was smaller than two tables' parsed indexes, so
each lookup evicted the index the next one needed and re-parsed a text footer (~100 MB of garbage a
time; a 300k-document scan ran for 11+ minutes in one layout). Indexes probed in the last 5 s are now
exempt from eviction. Together with a more compact document cache (pin counts live in the entry, no
second map; entries stored by value) the same runs now read:

| 1M documents | before | after |
|---|---:|---:|
| RSS after bulk load | 1,285 MB | 723 MB |
| RSS after restart + full scan | 2,236 MB | 1,289 MB |

The compact cache entry saves about 10 MB per 300k documents (~35 MB at 1M), less than I first
estimated; the index-cache fix is what moved the number. Live heap is now ~0.7 KB per document at
300k (233 MB -> 214 MB; bodies 57 MB, SSTable indexes 41 MB, cache 49 MB, document tree 33 MB, checkpoint 26 MB).

The collector is also paced to the live heap (`kdb/memtune`, `--gc-headroom-percent`, default 25):
with the Go default a large heap may double before it is collected. Under allocation churn on a
768 MiB live heap the peak is ~0.98 GiB tuned against ~1.54 GiB default
(`TestTunedPeakHeapIsBelowTheDefaultUnderChurn`). In these benchmarks themselves it changes nothing
(throughput and RSS are within noise) because the load and the scan allocate little once the
thrashing is gone; it matters for query-heavy workloads on large namespaces.

Still not fixed: a cold point read (`get` after a restart) costs ~0.5 ms because it decodes its
whole commit frame.

## Limits of this run

- Single node, single writer, one namespace, ~200-byte documents, schemaless (no declared fields
  or indexes besides the document key).
- `sync-mode full` only, as asked. `fast` and `async` are covered in `write-durability-modes.md`.
- RSS is from `ps` at the points named, not a peak.
- Reads are issued in-process, not over the wire, so they exclude network and protocol cost.
- The machine was shared (see above); rerun on an idle host before quoting absolute numbers.
