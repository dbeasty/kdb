# Node-to-node sync at scale: baseline, and a quadratic catch-up fixed

Date: 2026-10-12. Base: `origin/main` 4124676. Harness: `TestSyncScale` (`go/kdb/replication/sync_scale_test.go`):

```
KDB_SYNC_SCALE=2000,10000,50000 go test ./kdb/replication -run TestSyncScale -v
```

This is step 0 of the sync lane in `docs/kdb-memory-cassandra-plan.md`.

## Setup

Both nodes are file-backed and run in one process.

- Writes are acknowledged before fsync (`DurabilityAsync`), so neither the seeding nor the receiver is bound by
  the disk.
- Each commit rewrites one of n/10 documents with a body of about 200 bytes.
- **join:** B is empty and pulls A's n commits.
- **catch-up:** B already holds A's first n commits; A writes n more and B pulls them.

Columns:

- **peak heap+** is the peak `HeapAlloc` above the pre-sync baseline, sampled every 20 ms. It covers both sides:
  the host's diff work and the receiver's backlog.
- **allocated** is the `TotalAlloc` delta during the sync.

Timings were taken at load average 5–8 from other sessions, so treat them as indicative only. Bytes, requests and
allocations do not depend on load.

## Before: catch-up was quadratic

| scenario | commits | recv | requests | peak heap+ | allocated | wall |
|---|---|---|---|---|---|---|
| join | 2,000 | 1.0 MiB | 2 | 13.9 MiB | 58 MiB | 0.3 s |
| catch-up | 2,000 | 1.0 MiB | 2 | 26.5 MiB | **920 MiB** | 0.7 s |
| join | 10,000 | 4.9 MiB | 3 | 47.0 MiB | 247 MiB | 1.6 s |
| catch-up | 10,000 | 4.9 MiB | 3 | 82.7 MiB | **18,826 MiB** | **13.1 s** |

Five times the commits gave twenty times the allocation.

The cause was a profile hit at 90% of all allocations. `peersync.commitsBetween` runs when the receiver adopts a
fetched range, and it called `IsAncestor(h, base)` once for every commit in the range. Without
`AncestryPruning`, which is off by default, each of those calls builds base's **whole** ancestor closure. The cost
was therefore O(backlog × history).

A join never paid it, because its base is the empty root. Merge resolution had the same pattern once per diverged
document (`changed`), and field-merge once per diverged field.

## After: `dag.AncestorTest`

`AncestorTest(base)` returns a predicate built from one walk:

- **Pruning off:** base's closure is built on the first query, and every later answer is a lookup in it.
- **Pruning on:** each answer is the pruned `IsAncestor` walk, which stops at once for a commit newer than base.

It is used in `commitsBetween`, `resolveDivergedLocked` and `fieldMergeByOrigin`.

| scenario | commits | recv | requests | peak heap+ | allocated | live after | wall |
|---|---|---|---|---|---|---|---|
| join | 2,000 | 1.0 MiB | 2 | 23.3 MiB | 56 MiB | 11.5 MiB | 0.3 s |
| catch-up | 2,000 | 1.0 MiB | 2 | 30.1 MiB | 57 MiB | 1.7 MiB | 0.4 s |
| join | 10,000 | 4.9 MiB | 3 | 44.8 MiB | 240 MiB | 12.9 MiB | 1.0 s |
| catch-up | 10,000 | 4.9 MiB | 3 | 53.9 MiB | **246 MiB** | 12.5 MiB | **1.6 s** |
| join | 50,000 | 24.3 MiB | 8 | 180.0 MiB | 1,242 MiB | 48.8 MiB | 6.0 s |
| catch-up | 50,000 | 24.4 MiB | 8 | 285.2 MiB | 1,336 MiB | 34.8 MiB | 4.7 s |

Catch-up is now linear: about 25 KB allocated per commit, the same as a join.

## What the baseline says for the next steps

- **Receiver peak heap is O(backlog):** about 5.7 KB per commit at 50k, and only about 0.5 KB/commit of the sync
  is still live afterwards. The range is held resident until adoption, so this is plan item C2 (chunked adoption).
- **~25 KB is allocated per 200-byte commit**, about 50× the payload. The JSON envelope, hydration and re-encoding
  dominate. These are plan items C3 and C4 (binary packs, forwarding stored frames).
- **Wire bytes are about 500 B per commit**: base64 inside JSON with no compression (C3).
- **Requests stay few**, because pages are capped at 4 MiB, roughly 6k commits each. The per-page ancestor-closure
  recompute (C1) only shows at larger histories. Measure at 200k and 1M next, on an idle machine.
