# KDB basic reads and writes: closing the gap to MongoDB

Written 2026-11-22 against `main` 6bc4d0e9 (v0.9.3 plus docs); measured rebased onto v0.9.4
(2c2420ba), then rebased onto c7a6b41f (#344, which changes memtable flush-failure retry and
read-error handling, not the paths measured here); branch `perf/basic-rw`. Every item that changes architecture names the Cassandra mechanism it
follows, as `kdb-memory-cassandra-plan.md` and `kdb-qtask-cpu-plan.md` do. Pure local fixes do not.

**Status (2026-11-22):** W1-W4, F1 and D1 are implemented and measured in this branch. Reads beat
MongoDB 1.3-2.1x; concurrent writes went from 0.29-0.32x to 0.41-0.46x MongoDB (fast durability);
durable writes are now 0.78-1.11x MongoDB `j:true` with kdb-service's new Linux default (D1). Open:
W8 (several writes per commit), the only item that takes concurrent writes past the per-namespace
commit rate, needs a design before it can be switched on.

## Goal and constraints

- **Goal:** single-document reads and writes by id at or above MongoDB 7, on the same CPUs and
  memory, under the same durability guarantee.
- **Cassandra-shaped:** commit log + memtable + SSTables stay the storage design. Work moves off
  the serialized path the way Cassandra keeps it off its write path.
- **Still an embeddable database:** no new dependency in the core module (the MongoDB driver lives
  only in the benchmark's own module), no thread pools, memory still bounded by the cache budget,
  nothing that only pays off on a dedicated server.
- **KDB stays KDB:** one commit per write, a Merkle tree per commit, history, peer sync. Nothing
  here changes a commit, an on-disk byte or a wire message.

## How it is measured

`go/bench/rwcompare` (its own Go module) runs YCSB-shaped cells - 1 KB documents of ten 100-byte
fields; point reads (zipfian), single-field updates, 50/50 (YCSB A) and 95/5 (YCSB B) mixes, and
inserts - at 1, 8 and 32 workers, against:

| target | what | write / read |
|---|---|---|
| `kdb` | kdb-service over its wire protocol (Go client) | `Upsert` / `GetJSON` |
| `kdb-embed` | KDB in the benchmark process, no network | `KdbServerRuntime.Upsert` / `GetDocument` |
| `mongo` | mongod 7 over its wire protocol (official Go driver) | `insertOne`, `updateOne $set` / `findOne` |

`run.sh` gives the database container 4 CPUs and 3 GB, and runs the client in a second 4-CPU
container on the same Docker network, so neither database pays Docker Desktop's host port proxy.
Cells run a fixed number of operations (time-based cells on a growing store compare different
store sizes), every cell has its own random stream, and the script waits for the host to be quiet
before each run. A/B runs alternate the two binaries over two passes.

Durability is matched in two classes:

| class | KDB | MongoDB |
|---|---|---|
| fast | `--durability async --async-sync-interval-ms 100` | `{w:1}` (the default; journal synced every 100 ms) |
| durable | `--durability sync` | `{j:true}` |

```sh
cd go/bench/rwcompare
./run.sh kdb fast mylabel  -docs 100000 -concurrency 1,8,32   # builds kdb-service from the checkout
./run.sh mongo fast mongo  -docs 100000 -concurrency 1,8,32
KDB_BIN=/path/to/linux/kdb-service ./run.sh kdb durable other ...
```

## Baseline

On v0.9.4 (main 2c2420ba; the first measurements were on 6bc4d0e9 and agree), against MongoDB 7
under the same limits - full table under [Results](#results):

- **Reads were already ahead:** point reads 1.4-2.1x MongoDB's at every concurrency, 95/5 mixes
  0.8-1.7x.
- **Writes were 2-4x behind:** KDB flattened at ~13-18k writes/s from 8 workers up while MongoDB
  went on to 46-60k (fast); 0.44-0.76x durable.
- **Multi-second stalls:** whenever a memtable flush landed under the write gate every writer
  waited for it - 1.2-2.1 s worst-case write latency in the update and mixed cells.
- **Memory:** ~1.2-1.3 GB container memory at 100k 1 KB documents against MongoDB's 0.6-0.7 GB.

## Where the time went

A CPU profile of kdb-service under `update` at 32 workers (fast) showed the write gate held for
**96% of wall time**: write throughput was whatever one goroutine could do serially. Under the
gate, per Upsert:

| work under the gate | share of gate time |
|---|---:|
| `Document.Merge` - 4 JSON parses of body and patch, re-encode | 34% |
| reading the stored body - mostly **cold loads** of hot documents, evicted under bounded live bodies | 16-20% |
| Merkle trie update (`DocumentTree.With`), of which the new leaf's fold (`foldLeaf`, ~28 SHA-256 rounds) | 22% (12%) |
| DAG append (commit hash over the whole payload) | 8% |
| content hash, docsByHash, tree object, publish | ~10% |

Reads were never the problem: KDB already served point reads 1.3-2x faster than MongoDB.

## Done in this branch

Each measured alone, A/B against the commit before it (KDB only, fast durability, two passes;
full tables in the PR).

### W1 - Upsert merges onto the head it lands on (correctness)

`Upsert` resolved head before queueing for the gate and merged onto the document at that version.
A writer that queued behind another wrote back a body without the fields the writer ahead had just
set; LAST_WRITE reports no conflicts, so the loss was silent. **16 concurrent upserts to one
document kept 2 of 16 fields.** The merge now reads the live head at the gate front.

*Cassandra:* a mutation is applied to the partition as it is at that moment (`AtomicBTreePartition`).
Performance: neutral (±1-4%). Test: `TestConcurrentUpsertsToOneDocumentKeepEveryField`.

### W2 - Upsert works out its merge before the gate

Upsert reads the document, merges, validates, and at the gate front installs the result only if
the document's content hash in the live tree is still the one it merged onto
(`storage.DocumentHasher`, a trie lookup that never reads a body); otherwise the gate merges as
before. The installed op is a request-only `Replace`+`Prevalidated` write whose stored form is the
document the merge would have produced, so commits are byte-identical.

*Cassandra:* `AtomicBTreePartition.addAllWithSizeDelta` builds the updated partition off the lock
and installs it with a compare-and-swap, falling back to a lock under contention.

### F1 - commits that leave the same tree each file their own locations

Found while chasing a W2 regression that turned out to be this plus a harness artifact. Location
records were stashed one entry per *tree hash* until the log reported the commit's position; two
commits in flight with the same tree (change A, change B, change B back - or any write that changes
nothing) overwrote each other's entry. A version with no location falls back to the delta cold
loader, whose index refresh rescans every commit since its last refresh: **35% of server CPU** in a
mixed run at 20k writes/s, from 794 such reads. The stash is now a queue per tree, as `durablePins`
already was. Test: `TestColdReadsResolveByLocationAfterWritesThatRestoreATree` (fails before).

*Cassandra:* reads never touch the commit log; this restores that property for KDB's cold reads.

### W3 - an Upsert's version is hashed before the gate

The content hash and the Merkle leaf folded to every depth (`document.LeafSpine`; the depth it
lands at is only known under the gate) are handed to storage before queueing
(`storage.VersionPreparer`); `CommitTree` uses them only for exactly the body it stores.

*Cassandra:* keeps Merkle trees off the write path entirely (built at repair). KDB commits name
their tree, so the tree still changes per commit, but the version's own share of the hashing moves
off the serialized section.

### W4 - a full memtable is written on its own goroutine

`maybeFlushMemtable` wrote and synced the SSTable from `CommitTree`, under the gate. It now runs on
a goroutine while commits go on into the fresh generation; a commit waits only when a second
generation fills first.

*Cassandra:* `ColumnFamilyStore.switchMemtable` + the flush writer; writes block only past
`memtable_cleanup_threshold`.

### Measured, branch steps (KDB, fast durability, ops/s, mean of two passes)

Each column is its own two-pass A/B against the previous column's binary, so absolute numbers
differ a little between columns; the last column multiplies the step ratios. Read cells are the
control (no step touches the read path) and stay within ±2%.

| cell | c | main | +W1 W2 F1 | +W3 | +W4 | cumulative |
|---|--:|--:|--:|--:|--:|--:|
| load (inserts) | 32 | 19,904 | 23,125 (+16%) | 28,136 (+23%) | 28,552 (+3%) | 1.47x |
| read | 8 | 104,015 | 106,414 (+2%) | 102,643 (-2%) | - | 1.00x |
| read | 32 | 125,476 | 127,374 (+2%) | 124,932 (-2%) | - | 0.99x |
| update | 1 | 8,313 | 8,538 (+3%) | 8,403 (-2%) | 8,121 (+1%) | 1.02x |
| update | 8 | 14,522 | 22,044 (+52%) | 24,910 (+15%) | 25,199 (-1%) | 1.72x |
| update | 32 | 13,474 | 20,456 (+52%) | 22,250 (+9%) | 22,434 (0%) | 1.65x |
| mixed50 | 8 | 30,153 | 39,135 (+30%) | 42,686 (+10%) | 43,408 (0%) | 1.43x |
| mixed50 | 32 | 24,023 | 31,313 (+30%) | 34,088 (+9%) | 43,493 (+27%) | 1.81x |
| insert | 1 | 8,595 | 8,556 (0%) | 8,405 (-3%) | 8,462 (+1%) | 0.98x |
| insert | 8 | 18,013 | 21,346 (+19%) | 25,537 (+20%) | 25,082 (-1%) | 1.41x |
| insert | 32 | 17,801 | 20,696 (+16%) | 24,143 (+16%) | 24,268 (-1%) | 1.33x |

One worker gains nothing: a single writer never queues at the gate, so moving work off it only
moves it, and W3's few extra hashing rounds (every depth instead of one) cost ~2% there.

W4's gain is in its tails, not its means: in the mixed50@32 cell a memtable flush landed under the
gate and stalled every writer for **1.26-1.31 s** (max latency, both passes; durable likewise). With
W4 the worst write in that cell is 26-48 ms (fast) and 51-171 ms (durable).

The durable-mode configuration (D1, before it became the default), on the W3 binary, two passes:

| cell | c | default | `--sync-mode fast` | preallocated segments | both | both vs default |
|---|--:|--:|--:|--:|--:|--:|
| load (inserts) | 32 | 17,255 | 20,226 | 22,799 | 27,769 | 1.61x |
| update | 1 | 2,289 | 2,849 | 3,164 | 4,566 | 1.99x |
| update | 8 | 8,471 | 9,087 | 10,617 | 13,611 | 1.61x |
| update | 32 | 18,464 | 18,564 | 22,080 | 23,957 | 1.30x |
| insert | 1 | 2,721 | 2,784 | 4,147 | 4,518 | 1.66x |
| insert | 8 | 7,308 | 8,252 | 10,711 | 11,153 | 1.53x |
| insert | 32 | 18,455 | 19,261 | 22,922 | 26,099 | 1.41x |

`fdatasync` alone barely helps because a growing file's size is metadata it must commit anyway;
preallocation alone helps; together the sync skips the metadata commit (as
`kdb-segment-preallocation-plan.md` predicted) and the worst update stall at 32 workers drops from
137-163 ms to 29-31 ms.

### D1 - kdb-service on Linux defaults to `fdatasync` into preallocated segments

`--sync-mode` now defaults to `fast` and the new `--preallocate-segments` (also
`KDB_PREALLOCATE_SEGMENTS`, `preallocateSegments` in the config file) to on, **for kdb-service on
Linux only**. Elsewhere the service keeps `full` without preallocation (on macOS `fast` is
`F_BARRIERFSYNC`, which leaves the drive cache unflushed), and embedded runtimes keep their
zero-value defaults (full, no preallocation), so an embedded store with many small namespaces does
not pay a segment of disk each.

The guarantee does not change on Linux: `fdatasync` forces the data, and any metadata needed to read
it back, through the drive cache exactly as `fsync` does; it skips timestamps. On a growing file the
new size is such metadata, so the two cost the same; preallocated segments never change size, so
`fdatasync` becomes a data-only flush. The cost is disk: each open namespace's active segment takes
its full size (64 MiB by default, `DeltaMaxSegmentBytes`) from creation, and creating one writes and
syncs 64 MiB of zeros on the log writer once per rotation (D2 moves that off the write path).

*Cassandra:* the commit log is fixed-size segments its `CommitLogSegmentManager` creates up front.

Measured, the branch with no flags against v0.9.4 with no flags, durable, two passes:

| cell | c | v0.9.4 | branch, new default | change | worst write, ms (v0.9.4 / branch) |
|---|--:|--:|--:|--:|---|
| load (inserts) | 32 | 17,231 | 27,077 | +57% | 14-153 / 44-107 |
| update | 1 | 2,678 | 4,357 | +63% | 11 / 31-90 |
| update | 32 | 13,559 | 22,665 | +67% | 1,653-1,861 / 35-45 |
| mixed50 | 8 | 13,304 | 22,160 | +67% | 14-2,093 / 25-39 |
| mixed50 | 32 | 25,546 | 42,094 | +65% | 1,215-1,330 / 42-110 |
| insert | 1 | 2,609 | 4,030 | +54% | 5 / 29-31 |
| insert | 32 | 15,602 | 23,355 | +50% | 21-236 / 55-142 |

## Not done yet - the plan

Ordered by expected gain per effort. Gate-time shares are from the latest profile.

| # | item | Cassandra mechanism | expected | risk / cost |
|---|---|---|---|---|
| D2 | Durable tails: durable cells still show single 30-450 ms writes (fast mode: under 60 ms). One known cause: with D1 a new segment is created - 64 MiB of zeros written and synced - on the log writer at rotation, in line with commits. Create the next segment ahead of need on a goroutine, then attribute what is left (checkpoint, SSTable sync) with a block profile | `CommitLogSegmentManager`'s allocator keeps the next segment ready before the active one fills | p99.9 / max under durable writes | medium |
| W5 | Serialize each commit once: the DAG's commit hash and the delta record encode the same payload twice under the gate | a `Mutation` is serialized once and the buffer reused (commit log, messaging) | ~5% of gate | low |
| W6 | Insert validation: a new document is parsed into `map[string]any` (encoding/json) just to check it is an object; a validating scan is enough | - | inserts | low |
| W7 | Remaining Merkle work under the gate: internal node hashes along the path (~7%), child record reads (~6%) | (Merkle off the write path) - only a batch can share them, see W8 | | medium |
| W8 | **Coalesce concurrent single-document writes into one commit** (one trie update shares internal nodes, one DAG append, one log record). The gate caps ~25-30k commits/s per namespace; MongoDB's 50-60k concurrent writes need it. | commit-log group sync + mutation batching | the only path past ~1/(gate time) | **Decision needed:** one commit then carries several clients' writes - history, idempotency (tx id -> commit) and peer sync see batches, not single writes; off by default |
| R1 | Superseded versions first out of the body cache: with bounded live bodies a live version competes with the versions it replaced, so hot documents get evicted and read back from frames | the memtable holds only the latest cells of a partition | read p99, fewer cold loads | low |
| R2 | A cold read must never rescan the log: bound or remove the delta cold loader's refresh on the hot path (F1 removed the common cause) | reads never read the commit log | tail latency | medium |
| M1 | **Memory footprint:** at 100k 1 KB documents the KDB container ends at ~1.2-1.3 GB against MongoDB's 0.55-0.7 GB. Find what holds it (mapped tree pages, slab, memtables, index) before tuning | Cassandra's off-heap memtables + chunk cache with explicit budgets | lighter embedding | measure first |
| E1 | Embedded API: `embed.PutJSONDocument` is not safe for concurrent callers and goes around the gate; embedders use `server.NewKdbServerRuntime(rt)` or `database/sql`. A small concurrent document API over the same path (W1-W4 included) | - | embedded = no network, see below | low |
| E2 | Embedded cache default is a fixed 64 MiB; the service sizes from the machine (1/8). An embedded store with more hot data than that runs from cold reads | Cassandra sizes caches from the heap | embedded reads | decision (memory vs speed) |

## Results

Final matrix, 2026-11-22, two passes alternating order, every number the mean of both: v0.9.4,
this branch, this branch with the D1 durable configuration (`--sync-mode fast` +
`KDB_PREALLOCATE_SEGMENTS=true`), this branch embedded in the benchmark process (no network,
same 4 CPUs and 3 GB), and MongoDB 7. ops/s; ratios against MongoDB.

### fast

| cell | c | v0.9.4 | branch | branch embedded | MongoDB 7 | branch / Mongo | embedded / Mongo |
|---|--:|--:|--:|--:|--:|--:|--:|
| load | 32 | 19,240 | 27,106 | 30,324 | 60,154 | **0.45x** | 0.50x |
| read | 1 | 14,138 | 14,057 | 1,162,546 | 9,846 | **1.43x** | 118.07x |
| read | 8 | 108,954 | 101,527 | 4,781,333 | 61,187 | **1.66x** | 78.14x |
| read | 32 | 134,805 | 125,621 | 3,524,478 | 66,163 | **1.90x** | 53.27x |
| update | 1 | 8,320 | 8,293 | 23,811 | 10,302 | **0.80x** | 2.31x |
| update | 8 | 15,934 | 23,796 | 20,629 | 54,106 | **0.44x** | 0.38x |
| update | 32 | 13,277 | 21,558 | 19,909 | 46,406 | **0.46x** | 0.43x |
| mixed50 | 1 | 9,931 | 10,047 | 38,659 | 10,359 | **0.97x** | 3.73x |
| mixed50 | 8 | 29,688 | 42,047 | 38,143 | 57,677 | **0.73x** | 0.66x |
| mixed50 | 32 | 25,338 | 41,939 | 38,028 | 53,395 | **0.79x** | 0.71x |
| read95 | 1 | 13,477 | 13,367 | 210,712 | 10,684 | **1.25x** | 19.72x |
| read95 | 8 | 61,429 | 90,814 | 225,327 | 60,929 | **1.49x** | 3.70x |
| read95 | 32 | 104,194 | 104,689 | 236,796 | 62,012 | **1.69x** | 3.82x |
| insert | 1 | 8,520 | 8,549 | 28,765 | 10,894 | **0.78x** | 2.64x |
| insert | 8 | 17,626 | 24,873 | 26,610 | 55,627 | **0.45x** | 0.48x |
| insert | 32 | 17,551 | 23,623 | 24,901 | 57,222 | **0.41x** | 0.44x |

write p99 / max at 32 workers (fast), ms:

| cell | v0.9.4 | branch | branch embedded | MongoDB 7 |
|---|--:|--:|--:|--:|
| update | 4.1 / 2095 ; 4.2 / 1752 | 3.8 / 18 ; 3.7 / 17 | 2.7 / 22 ; 3.2 / 29 | 2.3 / 64 ; 3.5 / 65 |
| mixed50 | 4.0 / 1246 ; 4.0 / 1270 | 3.3 / 30 ; 3.9 / 61 | 2.7 / 43 ; 3.1 / 56 | 2.1 / 59 ; 2.4 / 61 |
| insert | 2.7 / 22 ; 2.8 / 18 | 2.4 / 17 ; 2.6 / 17 | 2.3 / 126 ; 2.3 / 153 | 2.1 / 63 ; 2.4 / 68 |

### durable

| cell | c | v0.9.4 | branch | branch, D1 config | branch embedded | MongoDB 7 | branch / Mongo | D1 / Mongo | embedded / Mongo |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| load | 32 | 15,618 | 20,094 | 27,754 | 18,988 | 26,135 | **0.77x** | 1.06x | 0.73x |
| read | 1 | 14,002 | 13,722 | 13,886 | 1,188,847 | 10,207 | **1.34x** | 1.36x | 116.48x |
| read | 8 | 103,911 | 103,386 | 102,114 | 4,380,298 | 60,640 | **1.70x** | 1.68x | 72.23x |
| read | 32 | 130,440 | 129,344 | 126,319 | 4,964,876 | 63,134 | **2.05x** | 2.00x | 78.64x |
| update | 1 | 2,654 | 2,829 | 4,804 | 2,921 | 5,222 | **0.54x** | 0.92x | 0.56x |
| update | 8 | 7,410 | 8,912 | 13,068 | 8,642 | 16,738 | **0.53x** | 0.78x | 0.52x |
| update | 32 | 15,565 | 18,746 | 23,477 | 20,141 | 25,606 | **0.73x** | 0.92x | 0.79x |
| mixed50 | 1 | 4,409 | 4,527 | 7,202 | 6,989 | 7,239 | **0.63x** | 0.99x | 0.97x |
| mixed50 | 8 | 12,094 | 15,038 | 22,906 | 16,875 | 26,475 | **0.57x** | 0.87x | 0.64x |
| mixed50 | 32 | 26,213 | 29,147 | 42,358 | 37,278 | 42,948 | **0.68x** | 0.99x | 0.87x |
| read95 | 1 | 13,563 | 12,995 | 17,339 | 55,554 | 17,446 | **0.74x** | 0.99x | 3.18x |
| read95 | 8 | 60,476 | 67,087 | 85,633 | 164,505 | 58,633 | **1.14x** | 1.46x | 2.81x |
| read95 | 32 | 86,343 | 93,873 | 99,206 | 338,563 | 59,873 | **1.57x** | 1.66x | 5.65x |
| insert | 1 | 2,583 | 2,431 | 4,100 | 2,857 | 4,150 | **0.59x** | 0.99x | 0.69x |
| insert | 8 | 8,521 | 8,727 | 12,420 | 8,542 | 11,186 | **0.78x** | 1.11x | 0.76x |
| insert | 32 | 16,216 | 18,634 | 23,465 | 19,110 | 26,538 | **0.70x** | 0.88x | 0.72x |

write p99 / max at 32 workers (durable), ms:

| cell | v0.9.4 | branch | branch, D1 config | branch embedded | MongoDB 7 |
|---|--:|--:|--:|--:|--:|
| update | 5.1 / 14 ; 5.5 / 139 | 4.7 / 17 ; 4.7 / 128 | 4.1 / 40 ; 4.3 / 44 | 4.5 / 170 ; 3.7 / 180 | 2.2 / 48 ; 11.6 / 64 |
| mixed50 | 5.6 / 1231 ; 4.7 / 1239 | 4.3 / 52 ; 6.2 / 444 | 4.4 / 83 ; 3.8 / 56 | 5.6 / 58 ; 4.0 / 46 | 2.8 / 37 ; 6.8 / 144 |
| insert | 5.4 / 235 ; 4.0 / 393 | 4.8 / 17 ; 3.9 / 179 | 3.1 / 131 ; 2.9 / 154 | 4.9 / 404 ; 5.3 / 118 | 2.4 / 142 ; 4.3 / 182 |

What it says:

- **Reads:** 1.3-2.1x MongoDB over the wire; embedded, a point read is a function call - 1.2M
  reads/s on one worker, 3.5-5M on 8-32. (Read cells in this matrix sit right after the load phase
  and last ~1.5 s; with 1M-operation read cells the branch and v0.9.4 are within 1%.)
- **Concurrent writes, fast durability:** +35-66% over v0.9.4 but still **0.41-0.46x** MongoDB. KDB
  is now bound by what each commit must do serially (Merkle path, DAG append, ~40 us) - about 25k
  commits/s per namespace. Only W8 (several writes per commit) gets past that.
- **Durable writes:** with D1, **parity**: 0.78-1.11x MongoDB `j:true` on writes, ahead on inserts at
  8 workers and on the load phase; 0.92x on updates at 1 and 32 workers.
- **Mixed and read-mostly:** 50/50 at 0.73-0.97x (fast) and 0.87-0.99x (durable, D1); 95/5 ahead of
  MongoDB everywhere except one worker under durable writes (0.74x, 0.99x with D1).
- **Single-worker latency:** a KDB write is still ~20% slower than MongoDB's on one connection
  (fast); embedded, 2.3-3.7x faster than MongoDB over the network.
- **The multi-second stalls are gone:** worst write in the update and mixed cells 17-61 ms (fast)
  against 1.2-2.1 s on v0.9.4. Durable runs still show occasional 100-450 ms maxima (D2).
- **Embedded concurrent writes are no faster than over the wire** - the same write gate decides
  both; the network only matters on one worker.

## Traps found on the way

- **Seeding cells by concurrency alone** gave `update@8` and `mixed50@8` the same (document, field,
  value) stream: the mixed cell rewrote values already written, thousands of no-op commits, which
  is what exposed F1. Every cell now has its own stream.
- **Editing `run.sh` while a run uses it** breaks the run: bash reads scripts as it goes.
- **Another session's job took the load average to 103 for half an hour**; `run.sh` waits for load
  under 8 before each run, but a run already started is contaminated - rerun anything that moved
  with load in its log.
- **Profile the cell in the order the matrix runs it.** The W2 "regression" only appeared after the
  preceding cells; a fresh store showed +31%.
- `docker pull` from this shell hung on Docker Desktop's credential helper; a clean
  `DOCKER_CONFIG` pulls the public image.
