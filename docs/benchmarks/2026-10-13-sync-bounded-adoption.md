# Node-to-node sync at 200k commits: two quadratics removed, adoption chunked

Date: 2026-10-13. Base: `perf/sync-baseline` (PR #162), which is `origin/main` 4124676 plus `dag.AncestorTest`.
Harness: `TestSyncScale` (`go/kdb/replication/sync_scale_test.go`); setup as in
`2026-10-12-sync-scale-baseline.md`.

The scenarios use two file-backed nodes in one process, with `DurabilityAsync`:
- **join:** an empty node pulls n commits.
- **catch-up:** a node that already holds n commits pulls n more.

Bodies are about 200 B unless stated. New knobs:

- `KDB_SYNC_ADOPT_BYTES` sets `V2ClientConfig.AdoptEveryBytes`; `-1` adopts only at the end.
- `KDB_SYNC_LIVE=1` adds **peak live+**: a GC before each 100 ms sample, so it shows the live heap the sync actually
  held. **peak heap+** includes uncollected garbage.

## What the 200k run found

At 50k commits the base looked linear. At 200k, a join on the base ran for **more than 14 minutes** before it was
killed. Profiling found three costs that grow with the backlog.

1. **Operations-budget eviction was O(n²) (`dag/ops_retention.go`).**
   - A commit received from a peer is pinned until it is logged, because its operations exist only in memory.
   - These commits were still put on the eviction LRU. Once a pull's backlog passed the ops budget (25% of the
     memory budget, 32 MiB by default), every stored commit triggered an eviction pass that walked past every
     pinned one stored before it.
   - Fix: unlogged commits stay off the list and join it on `MarkLogged`.
2. **Every FETCH page was worked out from scratch (`peersync/refs.go`, plan item C1).**
   - Each page cost the full ancestor closure of the haves, the whole missing set, and a sort of it.
   - Fix: a per-session `missingCursor` on the host (and per ref on the client's push) walks once and keeps the order,
     at 32 B per missing commit.
   - The pages served are identical to before. A property test checks this on random DAGs with merges, retries and
     random page sizes.
3. **`topoSort` re-sorted its ready list before every commit**, building two hex strings per comparison.
   - Fix: a heap ordered by hash bytes. Byte order equals hex order, so the output order is unchanged.

Separately, **chunked adoption** (plan item C2): a pull that fast-forwards main adopts and logs what it has received
every `AdoptEveryBytes`, 32 MiB estimated by default. Received operations are therefore pinned only until their
chunk is logged, not until the whole range has arrived. Two guards:
- Only a fast-forward of main is adopted early, rechecked under the node's serialization.
- A local commit that diverged main makes the early step a no-op, and the end-of-pull adoption merges once, as
  before.

## Results

Allocation and request counts do not depend on load. Wall times do: this machine's I/O swings by ±50% between
identical runs (see below).

| scenario | commits | base (#162) | after |
|---|---|---|---|
| join | 200,000 | **> 14 min, killed** | 25–73 s; 6.6 GB allocated |
| catch-up | 200,000 | not reached | 24–49 s; 7.1–7.6 GB allocated |
| join | 50,000 | 1,242 MiB / 6.0 s | 1,140 MiB / 10.7 s |
| catch-up | 50,000 | 1,336 MiB / 4.7 s | 1,158 MiB / 13.7 s |
| join | 10,000 | 240 MiB | 246 MiB |
| catch-up | 10,000 | 246 MiB | 255 MiB |

The 50k wall times were taken in different sessions, at different machine loads; see the variance note.

Intermediate step at 200k, with the eviction fix only (no cursor):

| | join | catch-up |
|---|---|---|
| allocated | 8.8 GB | 10.8 GB |
| wall | 51.5 s | 45 s |

Host-side FETCH allocation at 200k, both syncs:

| | before cursor | after |
|---|---|---|
| total | 11,061 MB | 4,872 MB |
| `AncestorSetOf` | 2,262 MB | ~0 |
| `topoSort` | 2,498 MB | 360 MB |
| operations hydration (needed) | 4,147 MB | ~4,500 MB |

### Chunked adoption vs adopting at the end

The test uses 50k commits with 2 KB bodies, `KDB_SYNC_LIVE=1`:

| | peak live+ | wall |
|---|---|---|
| join, end only | 163.5 MiB | 11.3 s |
| join, chunked (3 early adoptions) | 141.4 MiB | 11.4 s |
| catch-up, end only | 162.6 MiB | 11.0 s |
| catch-up, chunked (3 early adoptions) | 134.0 MiB | 11.7 s |

That is 14–18% lower peak live heap, at the same speed.

At 100k with 200 B bodies, alternating runs (load average 3.7–4.5) showed no consistent difference:

| run | end-only join | chunked join |
|---|---|---|
| 1 | 33.4 s | 19.3 s |
| 2 | 24.4 s | 11.6 s |
| 3 | 16.4 s | 21.0 s |

Catch-up runs were spread the same way. An earlier single pair at 200k showed chunked as slower (54 s vs 25 s). The
repeated runs show that was I/O noise.

## Where the remaining time goes: the receiver's log appends

At 200k, fetching all pages takes about 4 s. **Adoption takes the rest, and it is not CPU.** Adoption used about
3.3 s of CPU in a run whose adoption steps took about 68 s of wall time. It waits on the commit-log writer, which
calls `writer.Append` **once per commit, one write syscall each** (`embed/commit_log.go` `appendBatch`). That came
to about 20 s of syscall CPU, with wall time dominated by the device. Local writes pay the same cost: seeding 200k
commits takes about 49 s.

Coalescing a batch's frames into one write would speed up sync adoption and local writes alike. That is a storage
change outside this PR, recorded as a follow-up.
