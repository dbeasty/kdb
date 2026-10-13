# Peer sync: bounded sessions and snapshot transfer

Date: 2026-10-13. Branch `perf/sync-session-bounds`, measured against `origin/perf/sync-chunked-adoption`
106f76f (#164, since merged to `main` as 529408e with no code change). Plan items C5 and C8, bugs 4 and 6 of
`docs/kdb-memory-cassandra-plan.md`.

Harness: `TestSnapshotScale` (`go/kdb/replication/snapshot_scale_test.go`), new in this branch, a sibling of
`TestSyncScale`:

```
KDB_SNAPSHOT_SCALE=100000,1000000 go test ./kdb/replication -run TestSnapshotScale -v
KDB_SYNC_LIVE=1 ...   # adds peak live heap (a forced GC per 100 ms sample)
```

An empty node B bootstraps from A's head with `PreferSnapshot: true`. Both nodes are file-backed
(`DurabilityAsync`) and run in one process over loopback TCP, so the heap figures cover both sides. A is
seeded in commits of 1,000 documents of about 230 bytes each.

**gate held** is the longest that B's write gate was unavailable to a local writer during the bootstrap. A
probe goroutine takes the gate in a loop, through `PeerSyncNode().Exclusive` with a no-op body, and records its
longest wait.

## Method

Every run went through the shared `bench-lock.sh`. The base and new test binaries were built once and run
alternately in one locked invocation: three timing runs each, then one `KDB_SYNC_LIVE=1` run each, at both
sizes. The lock waits for a load average below 6 only at its start. Other sessions pushed the load to 8–12 during
the 1M runs, so lead with allocations and heap. Wall times are indicative only.

## Results

### 100,000 documents (28.8 MiB on the wire, 7 requests)

| | allocated | peak heap+ | peak live+ | gate held | wall |
|---|---|---|---|---|---|
| base (3 runs) | 1,167 / 1,168 / 1,197 MiB | 225 / 195 / 189 MiB | 118 MiB | 9.9 / 9.1 / 11.3 s | 9.9 / 9.1 / 11.3 s |
| new (3 runs) | 1,057 / 1,083 / 1,106 MiB | 180 / 199 / 205 MiB | 146 MiB | 8.9 / 9.3 / 9.7 s | 9.2 / 9.7 / 10.1 s |

### 1,000,000 documents (286 MiB on the wire, 54 requests)

| | allocated | peak heap+ | peak live+ | gate held | wall |
|---|---|---|---|---|---|
| base (3 runs) | **22,675 / 22,620 / 22,650 MiB** | 2,223 / 2,313 / 2,753 MiB | 1,640 MiB | = wall | 204 / 165 / 177 s |
| new (3 runs) | **10,659 / 10,508 / 10,486 MiB** | 2,603 / 2,323 / 2,150 MiB | 1,664 MiB | 90 / 101 / 173 s | 94 / 105 / 179 s |
| base, `KDB_SYNC_LIVE=1` | 36,572 MiB | | 1,640 MiB | = wall | 222 s |
| new, `KDB_SYNC_LIVE=1` | 18,382 MiB | | 1,664 MiB | 104 s | 110 s |

At 1M documents the bootstrap allocates **12 GB less (−53%)** and takes about half the wall time in the two
runs at comparable load. The third pair ran at load 10–12. Almost all of the saving is on the host. The old
pager walked and sorted the whole tree for every one of the 54 pages, which is O(D²/page). The new one seeks to
the cursor.

The unit test `TestSnapshotPageCostIsThePage` isolates the host. A 20-document page from a third of the way in
allocates:

| documents | seek (new) | walk + sort (old) |
|---|---|---|
| 4,000 | 14.5 KB | 1.1 MB |
| 40,000 | 14.5 KB | 11.4 MB |

### What the numbers do and do not show

- **Phase 1 (the transfer) is outside the gate.** "Gate held" now equals wall time minus the transfer. On
  loopback the transfer is short: about 0.3 s at 100k and 4–6 s at 1M. Over a real network it is most of a
  bootstrap, and none of it holds the gate any more.
- **Phase 2 (install) still holds the gate for the local apply and the durable step:** about 90 s at 1M on this
  machine. A CPU profile at 200k puts most of it in storage, not peer sync, and it is mostly off-CPU:
  - memtable flushes with `F_FULLFSYNC` on each page-sized `CommitTree` batch;
  - `PersistSnapshot` → `MaterializeLiveBodies`, which writes every live body to the blob store before the
    checkpoint.

  Releasing the gate between batches would need storage to stage a tree beside the live one. `CommitTree`
  builds on the engine's single live tree, not on the tree it is passed. That is a storage API change, owned
  elsewhere. See the PR.
- **Peak live heap is unchanged (1.64 GB at 1M).** Peer sync's own share of it is now bounded: the install
  holds one page, never the applied set. `TestInstallSnapshotMemoryIsBounded` shows +257 KB live during transfer
  at both 2k and 8k documents, and no per-body retention once installed. What remains at the peak is the
  receiver's storage while it applies and persists (the live trie, the memtable, `MaterializeLiveBodies`), plus
  the host's resident state for the transfer. It needs A5, A7 and the storage-side staging above.

## Other bounds in this branch (unit-measured)

| | before | after |
|---|---|---|
| backup: live after classifying a 4,000-commit, 7 MB log | 2,768 B/commit (every commit held) | 357 B/commit (a hash per commit, ~70 B, plus the segment in flight) |
| FETCH haves per request, 200-commit pull in 2 KB pages | grows by one page's tips per page | ≤ 4 (start haves + last page's tips); every page after the first continues the host's cursor |
| `ReceivedTips` / `PendingTips` | every page's tips, appended on every failed sync | frontier, ≤ 64 |
| deepen | all pages in memory, then stored; history re-logged whole | stored and logged per page; only still-unlogged commits re-logged |
| pushed graft staging | pages in memory, fixed 1 GiB cap | unlinked file on disk, `MaxGraftPushBytes` (default 256 MiB) |
| peer-sync connections | unlimited | `PeerSyncMaxConnections` (default 64) |
