# Consolidated benchmark pass, memory plan (2026-10-14)

Commit **`8931ffc`** (main, after #240), Apple M3 Max (16 cores), macOS, Go 1.26.3. Run under the shared
benchmark lock (each part waits for load < 6); load 3.8–5.3 through the matrix. Raw output and the per-row
summary are in [`2026-10-14-pass/`](2026-10-14-pass/).

This pass closes the memory plan (`docs/kdb-memory-cassandra-plan.md`): it decides the `tree.file` and
bounded-live-bodies defaults and runs the plan's acceptance in full, peer bootstrap included.

## Workload matrix: four configurations

```
cd go && go test ./kdb/server/ -run '^$' -bench BenchmarkWorkload -benchtime 2s -count=5 -benchmem
```

Since #229 the matrix starts from production defaults (`FileRuntimeOptionsFromEnv`), so the graph file is on
unless `KDB_GRAPH_FILE=0`. 180/180 rows passed in each run:

| Run | Settings |
|---|---|
| graph-off | `KDB_GRAPH_FILE=0`: the configuration of every earlier published run |
| defaults | production defaults (graph file on) |
| tree-file | defaults + `KDB_TREE_FILE=1` |
| bounded | defaults + `KDB_BOUNDED_LIVE_BODIES=1` |

**What is trustworthy here.** Reads don't drift and memory-only durability rows barely do, so those are the
signal (see the comparability traps in earlier write-ups).

The fsync-bound single-user write rows are bimodal on this machine. Within this one pass, `WriteInsert`
single-user read 244, 107, 242 and 232; single-user `Transaction` read 120, 230, 234 and 233. That is the
known macOS device-contention effect: any other flushing process halves an fsync-bound neighbour. Those rows
are **not evidence** either way, and neither are the heavy-multi-user Update/Mixed/Transaction rows, which
drift within a process.

### Reads (medians, ops/sec)

| Row | graph-off | defaults | tree-file | bounded | defaults vs off | tree vs defaults | bounded vs defaults |
|---|---:|---:|---:|---:|---:|---:|---:|
| single-user / overlapping | 4,731,227 | 4,726,808 | 4,989,298 | 4,825,728 | −0.1% | +5.6% | +2.1% |
| heavy-multi / overlapping | 22,906,269 | 26,016,292 | 27,236,285 | **18,469,736** | +13.6% | +4.7% | **−29.0%** |
| single-user / non-overlapping | 5,547,609 | 5,634,605 | 5,324,619 | 5,313,169 | +1.6% | −5.5% | −5.7% |
| heavy-multi / non-overlapping | 6,414,305 | 6,341,367 | 6,352,037 | 6,133,540 | −1.1% | +0.2% | −3.3% |

Every sample of the bounded heavy-multi/overlapping row is 18.2–18.6M, against 25.6–26.1M with the defaults
(21.2M first sample), at the same 160 B and 3 allocations per read. The extra cost is contention on the read
path, not extra work.

### Memory-only durability (medians, ops/sec)

| Row | graph-off | defaults | tree-file | bounded | tree vs defaults | bounded vs defaults |
|---|---:|---:|---:|---:|---:|---:|
| insert / single-user | 46,260 | 46,492 | 45,993 | 43,606 | −1.1% | −6.2% |
| insert / heavy-multi | 35,584 | 35,847 | 34,840 | 33,639 | −2.8% | −6.2% |
| update / single-user | 49,613 | 49,345 | 48,061 | 46,590 | −2.6% | −5.6% |
| update / heavy-multi | 38,699 | 37,587 | 36,380 | 35,601 | −3.2% | −5.3% |
| transaction / single-user | 48,415 | 49,593 | 49,237 | 48,578 | −0.7% | −2.0% |
| transaction / heavy-multi | 20,713 | 20,129 | 19,507 | 19,456 | −3.1% | −3.3% |

### Against the last published run (2026-09-22, `73d72af`; graph-off is the comparable configuration)

| Row | 73d72af | now (graph-off) | Δ |
|---|---:|---:|---:|
| reads, single-user / overlapping | 4,315,957 | 4,731,227 | +9.6% |
| reads, heavy-multi / overlapping | 14,965,831 | 22,906,269 | +53.1% |
| reads, single-user / non-overlapping | 5,219,009 | 5,547,609 | +6.3% |
| reads, heavy-multi / non-overlapping | 6,010,710 | 6,414,305 | +6.7% |
| memory-only insert, single / heavy | 51,928 / 39,167 | 46,260 / 35,584 | −10.9% / −9.1% |
| memory-only update, single / heavy | 51,648 / 42,893 | 49,613 / 38,699 | −3.9% / −9.8% |
| memory-only transaction, single / heavy | 49,907 / 21,158 | 48,415 / 20,713 | −3.0% / −2.1% |

Reads improved across the board. Memory-only writes are 2–11% slower than three weeks and many features ago:
attachments, B4, squash, the index registry epoch, and checkpoint v6 all landed in between. Small but
consistent, so it's worth a look of its own rather than a conclusion from this pass.

### Decisions

- **Graph file on by default: confirmed.** Neutral to +13.6% on reads, within ±3% on memory-only writes.
- **`tree.file`: recommend making it the default.** It's about neutral on throughput (reads −5.5% to +5.6%,
  memory-only writes 1–3% slower), and it's what bounds the live tree's memory at scale. The first open of an
  existing namespace with it on is slower, because it writes the segment from the checkpoint; later opens map it.
- **Bounded live bodies: not as default yet.** The 29% concurrent-read regression has to be found and fixed
  first (a mutex/block profile of the heavy-multi-user read row). Memory-only writes are also 2–6% slower.

## 10M-document acceptance

```
KDB_MEMPROFILE_RSS=1 BOOTSTRAP=1 scripts/memory-acceptance.sh   # graph.file + tree.file + bounded bodies
```

Namespace on disk 3,358 MB; budget (a quarter) 839 MB.

| Phase | Peak heap | Live heap | Peak RSS | Verdict |
|---|---:|---:|---:|---|
| open | 10.3 MB | 6.4 MB | 40 MB | ok |
| reads (200k point reads) | 316 MB | 125 MB | 1,716 MB | ok |
| fullread (all 10M) | 58 MB | 58 MB | 1,760 MB | ok |
| writes (20k updates) | 141 MB | 88 MB | 557 MB | ok |
| checkpoint (close) | 51 MB | 9.8 MB | 230 MB | ok |
| **bootstrap** (empty node installs a snapshot) | **15,716 MB** | – | – | **OVER** |

Every phase of one node's life is now well within budget. Before this round, open peaked at 1,456 MB, reads at
1,420 MB and close at 2,077 MB. Peak RSS beyond the heap is resident pages of read-only mapped files, which the
guard no longer charges (#238).

**The peer bootstrap is the remaining memory problem**, measured here at 10M for the first time.
`TestSnapshotScale` at 10M:
- seed: 3 m 40 s;
- bootstrap: **34 min**;
- **94 GB allocated**;
- peak heap +15.7 GB (HeapAlloc, garbage included, both nodes in one process);
- **3.8 GB live afterwards**;
- 2.9 GB on the wire.

At 1M (profiled): 10.4 GB allocated, 1.26 GB peak live, 448 MB live after, 2 min 21 s. The allocation is spread
over both sides:

- **Host (3.6 GB at 1M):**
  - cold-read document versions re-admitted to the body store's arena: `docHashShard.newChunk`, 1.05 GB;
  - snapshot page buffers growing: `bytes.growSlice`, 1.03 GB;
  - frame reads.
- **Receiver (4.9 GB at 1M):**
  - the per-batch tree commit (`commitTreeLocked`, 1.64 GB);
  - SSTable and memtable writes;
  - re-reading the page spool;
  - decoding each document's JSON again;
  - `MaterializeLiveBodies`.

Next for the memory plan: make the snapshot path O(page) in memory on both sides at 10M, and fix the
bounded-bodies read contention. Then rerun this acceptance.

## 10M acceptance rerun: PASS (main `780697e`, after #257, #258, #266, #276, #277)

```
KDB_MEMPROFILE_RSS=1 BOOTSTRAP=1 scripts/memory-acceptance.sh   # graph.file + tree.file + bounded bodies
```

Namespace on disk 3,417 MB; budget (a quarter) 854 MB. Load 3.5-6 (memory figures are not load-sensitive).

| Phase | Peak heap | Live heap | Peak RSS | Verdict | Earlier today |
|---|---:|---:|---:|---|---:|
| open | 10.6 MB | 6.5 MB | 40 MB | ok | 10.3 MB |
| reads (200k point reads) | 270 MB | 48.5 MB | 1,651 MB | ok | 316 MB |
| fullread (all 10M) | 58 MB | 58 MB | 1,547 MB | ok | 58 MB |
| writes (20k updates) | 135 MB | 86.5 MB | 552 MB | ok | 141 MB |
| checkpoint (close) | 43 MB | 2.6 MB | 215 MB | ok | 51 MB |
| **bootstrap** (empty node installs a snapshot) | **501 MB** | – | – | **ok** (both nodes) | 15,716 MB |

The peer bootstrap at 10M (`TestSnapshotScale`, both nodes in one process):

| | Earlier today | Now |
|---|---:|---:|
| peak heap | 15.7 GB | **501 MB** |
| time | 34 min | **2 m 58 s** |
| allocated | 94 GB | 52 GB |
| live afterwards | 3.8 GB | about none (−8.8 MiB) |
| on the wire | 2.9 GB | 2.9 GB |

What closed it:
- #266: the receiver writes snapshot bodies straight to the blob store (no pin or admit), and `DocumentTree.WithAll` adds a sorted batch in one pass. The host serves big snapshots from an id-sorted on-disk copy.
- #276: cuts the bootstrap's transient heap further (904 MB).
- #277: a tree segment's extern table stays in the mapping instead of a heap copy (141 MB at the peak).

Close leaves the SSTable merge (1.6-1.7 GB of 49 tables) to maintenance or the next open, as designed since #215.

**This closes the memory plan's acceptance:** every phase of one node's life, and a peer bootstrap, stays within a quarter of the data's size at 10M documents.
