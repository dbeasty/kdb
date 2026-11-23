# KDB basic reads and writes: closing the gap to MongoDB

Written 2026-11-22 against `main` 6bc4d0e9 (v0.9.3 plus docs), branch `perf/basic-rw`. Every item
that changes architecture names the Cassandra mechanism it follows, as
`kdb-memory-cassandra-plan.md` and `kdb-qtask-cpu-plan.md` do. Pure local fixes do not.

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

## Baseline (main 6bc4d0e9)

<!-- BASELINE -->

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

<!-- STEPS -->

## Not done yet - the plan

Ordered by expected gain per effort. Gate-time shares are from the latest profile.

| # | item | Cassandra mechanism | expected | risk / cost |
|---|---|---|---|---|
| D1 | **Durable writes: preallocated log segments + `fdatasync` by default for kdb-service on Linux.** Measured (below): durable update@1 2.0x, update@32 +30%, insert@32 +41%. Linux `fdatasync` is as power-loss-safe as `fsync`; with preallocation it skips the size-metadata commit. | `CommitLogSegmentManager` pre-creates fixed-size segments | KDB durable inserts already pass MongoDB `j:true`; updates reach 0.85-0.9x | **Decision needed:** 64 MiB of disk per active namespace up front - fine for a server, not for many small embedded namespaces (keep off for embed, or smaller embedded segments) |
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

<!-- RESULTS -->

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
