# KDB basic reads and writes: closing the gap to MongoDB

Written 2026-11-22 against `main` 6bc4d0e9 (v0.9.3 plus docs); measured rebased onto v0.9.4
(2c2420ba), then rebased onto c7a6b41f (#344, which changes memtable flush-failure retry and
read-error handling, not the paths measured here); branch `perf/basic-rw`. Every item that changes architecture names the Cassandra mechanism it
follows, as `kdb-memory-cassandra-plan.md` and `kdb-qtask-cpu-plan.md` do. Pure local fixes do not.

**Status (2026-11-22, follow-up stack):** W1-W4, F1 and D1 landed in this branch; D2, W6, W5, R1,
R2, W7 and E1 are implemented and measured in a stack of one PR each on top of it (see
[Done in the follow-up stack](#done-in-the-follow-up-stack)). Final matrix: reads 1.3-1.9x
MongoDB; durable writes 0.80-1.13x MongoDB `j:true` (v0.9.4: 0.43-0.69x) with worst cases 2-4x
lower than before D2 and below MongoDB's; concurrent writes under fast durability still
0.36-0.47x, bound by the per-namespace commit rate (~20-24k/s), which only W8 (several writes per
commit) gets past. M1 found what holds KDB's memory - the commit graph's
resident tier, unbounded between maintenance passes - and is now fixed (branch
`perf/rw-m1-graph-memory`, [M1 - measured](#m1---measured)): the commit graph flushes by size and
merges size-tiered, the container at this workload drops from 1.29 GB to 0.86 GB (MongoDB 7:
0.6-0.7 GB), write throughput unchanged.
**Waiting on the user:** W8 (design below, with the decisions it needs). E2 is decided (option 3,
built; below).

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

## Done in the follow-up stack

One PR per item, stacked on this branch in the order below, each measured against the one before
it with `ab.sh` (binaries alternated over two passes; read cells are the control): D2 #346 ->
W6 #347 -> W5 #348 -> R1 #349 -> R2 #350 -> W7 #351 -> E1 #352 -> this document's results.

At the matrix level only D2 moves anything measurable (durable tails). W6, W5, R1, R2 and W7 each
land within +-5% of their predecessor: they remove real work, but not work that limits these
cells - W6's validation already ran before the gate, W5 and W7 are 1-4% of gate time, R1 matters
only when the cache holds the wrong versions and cold reads are cheap here, and the benchmark never
reaches R2's path. They are kept because each removes a cost that grows somewhere else (inserts'
CPU, the gate's share at higher rates, cache pressure on larger data, cold reads after a restart).

### D2 - durable tails: the next segment ready ahead of need, background writers trickle-synced

With D1 a new segment's 64 MiB of zeros were written and synced by the commit-log writer at
rotation, and every durable commit queued behind it waited (27-35 ms worst writes in every write
cell). The next segment is now prepared on a goroutine once the active one is half written: an
unnamed `O_TMPFILE` file in the segment directory, linked in under its name at rotation. It is the
same file, size and synced zeros as before, invisible until then - every reader (Go, Kotlin,
older builds) refuses an unknown file in `delta/`, so a named spare there would have broken a
rollback - and a crash or close leaves nothing behind. Linux only; elsewhere segments are created
by name as before.

An execution trace (`/debug/pprof/trace`, update at 32 workers, durable) then attributed what was
left: the slowest commits (30-36 ms) all waited on the commit log's `fdatasync` while one
background writer's single final sync pushed tens of megabytes to the device - a flushed
memtable's SSTable (39 ms) or the spare's zeros (25-36 ms). No checkpoint, GC pause or gate hold
came near. Those writers now sync every MiB as they write.

*Cassandra:* `CommitLogSegmentManager` keeps the next segment ready before the active one fills;
`trickle_fsync` / `trickle_fsync_interval` for the background writers.

Durable, base = this branch, two passes; write p99.9 / max ms per pass:

| cell | c | base ops/s | D2 ops/s | change | base p99.9 / max | D2 p99.9 / max |
|---|--:|--:|--:|--:|---|---|
| update | 1 | 4,247 | 4,820 | +13% | 0.6/29, 1.1/33 | 0.6/9, 0.6/9 |
| update | 8 | 12,030 | 12,637 | +5% | 4.5/31, 4.0/34 | 4.0/12, 4.8/12 |
| update | 32 | 21,500 | 22,695 | +6% | 12.9/35, 8.1/45 | 7.6/13, 7.0/13 |
| mixed50 | 8 | 21,338 | 21,647 | +1% | 5.5/25, 5.7/20 | 4.6/7, 4.7/6 |
| mixed50 | 32 | 40,521 | 42,361 | +5% | 22.7/49, 12.9/49 | 9.4/24, 8.3/20 |
| insert | 1 | 4,133 | 3,875 | -6% | 1.0/31, 0.8/28 | 0.7/5, 0.8/6 |
| insert | 32 | 22,706 | 23,158 | +2% | 16.1/30, 15.9/34 | 14.5/21, 14.0/18 |
| read | 32 | 116,211 | 125,754 | (control) | | |

Worst-case durable writes fall 2-4x in every write cell; throughput moves within the run's noise.

### W6 - a document's JSON is validated with a scan, not a map

`validateObjectJSON` (every insert, `Replace` and replayed write) unmarshalled the body into a
`map[string]any` to check it was an object: 5.1 us and 39 allocations for a 1 KB document; the
scan takes ~1 us and none. Only the scan's *yes* is an answer: anything it rejects goes to
`json.Unmarshal` as before, which decides and produces its own error, so accepted inputs and
error texts are unchanged. The scan follows encoding/json's grammar plus the one way decoding
into `any` fails after the scanner accepts - a number out of float64's range (`1e400`). A
differential fuzz target holds it to Unmarshal (24M inputs, no mismatch).

Matrix effect: none outside noise (inserts +1-3%): Upsert already validated a new document in its
premerge, before the gate, so this saves CPU, not gate time.

### W5 - each commit is encoded once

The DAG encoded a new commit's payload to hash it and the persisting DAG encoded it again for the
delta record. `BuildCommitWithPayload` encodes once and hashes those bytes; the DAG keeps them for
the commit it appended last and `PersistAsync` takes them by commit hash. The log is byte-identical
(Go interop goldens unmoved). *Cassandra:* a `Mutation` is serialized once. Matrix effect: none
outside noise (it is ~1% of gate time).

### R1 - superseded versions leave the body cache first

Under bounded live bodies a live version and the versions it replaced share one budget, and a
superseded version of a hot document sat near the front of the CLOCK ring. A commit now demotes
the version it supersedes (or deletes) to the tail with its second chance cleared - or, while
its commit is not yet durable and it is still pinned, to the tail when the last pin goes.
*Cassandra:* the memtable keeps only the latest cells of a row.

Neutral even with the cache under pressure: `--cache-mb 128` (document budget ~38 MiB for ~105 MiB of
live bodies), interleaved with the W5 binary over two passes - read 0.98-0.99x, update 0.99-1.00x,
mixed50 1.00-1.01x, read95 0.99-1.02x. Cold reads here are served by location records and the
frame cache cheaply enough that which versions stay resident barely shows; R1 keeps the resident
set the right one, it does not make the benchmark faster.

### R2 - a cold read never rescans the open segment

A miss in the delta cold loader's index refreshed it by listing the log (rescanning the open
segment to describe it) and re-indexing that whole segment: up to 64 MiB read and decoded per
miss. The index now remembers how far it has read the segment this process is writing and, when
no segment came or went, reads only the frames appended since (`DeltaTailReader`); rotation,
damage and other processes' segments take the full refresh as before. *Cassandra:* reads never
touch the commit log. The benchmark does not reach this path after F1 (neutral in the A/B); a test
pins the bound - lookups while the open segment grows ten times cause no rescan.

### W7 - one trie walk per write under the gate

Of the remaining Merkle work, the profile (update at 32 workers, fast, gate held ~85% of wall
time) put the trie update at 32% of gate time, the DAG append at 15%, and a re-merge of documents
whose premerge went stale at 11%. One part was a safe win: each write walked the trie three times
(`HashFor` for the version it replaces, a `get` to count the tree's size, then the insert). The
insert now reports what it replaced; same tree, same pins, one walk (~4% of gate time). The
internal node hashes along the path cannot be shared without several writes per commit (W8).

Measured against R2 (two passes, 8 and 32 workers): writes 0.98-1.02x, the read control 0.95x in
both passes. A 1M-op read-only rerun came out equal in one pass (131.7k vs 131.6k reads/s at 32
workers) and 9% lower in the other. W7 changes nothing a read executes (the trees it builds are
identical), so this reads as run noise, but it is recorded as measured. The ~4% of gate time is
below what the matrix resolves.

### E1 - `kdb/docdb`, a concurrent document API for embedders

`docdb.Open(dir, namespace, opts)` and `Get`, `Put` (whole document), `Merge` (shallow merge at
the head the write lands on), `Delete`, `Close` - safe for concurrent use, every write through the
server runtime's write gate (W1-W4 apply), `Runtime()` for the rest. Backed by two new
`KdbServerRuntime` methods, `Replace` and `Delete` (last-write-wins at head; `Replace` validated
and hashed before the gate). No new dependency: it is `embed` + `server`, already in the
embeddable source bundle, which now lists `kdb/docdb` as an entry point.

### M1 - what holds KDB's memory (measured; the fix needs a design)

Measured on the final binary at the end of the matrix sequence (load + read/update/mixed50/insert
at 1, 8 and 32 workers: 100k documents, ~820k commits), fast durability, 3 GB container
(`mem.sh`: cgroup `memory.stat`, `/proc` status, a heap profile and the server's
`kdb_namespace_memory_bytes`):

| what | MiB | note |
|---|--:|---|
| `docker stats` | 1,249 | = cgroup anon + active file pages |
| cgroup anon (all Go heap + runtime) | 1,203 | Go heap in use 970 MiB, runtime total 1,343 |
| cgroup file pages | 1,251 | **not** what `docker stats` shows: 1,249 of it is inactive page cache (log, SSTables) |
| **commit graph, resident tier** | **333** | ~400 B per commit, every commit since the last graph flush |
| document bodies | 125 | its budget: 30% of the 384 MiB cache pool (live 105) |
| memtable | 124 | its bound: 20% of the pool, up to twice that while a generation is being flushed (W4) |
| live tree (slab) | 67 | |
| rescue reserve | 48 | deliberate: the memory guard's ballast |
| commit operations | 38 | its budget: 10% of the pool |
| delta frame cache | 26 | |
| history trees | 19 | |
| document index | 10 | |

With `--cache-mb 128` instead of 384 the container ends at 760-790 MiB; the pool's budgets account
for that difference, the commit graph does not shrink.

**What is unbounded: the commit graph's resident tier.** Since #159 the commit graph lives in
mapped segments (`graph.file`, on by default), but new commits stay in the heap until a
maintenance pass writes them out, and maintenance runs every 5 minutes by default. So resident
graph memory is bounded by time, not size: ~400 B x commit rate x 5 min - at 20k commits/s that is
~2.4 GB before the first pass. And the pass itself doubles it: `snapshotForSegment` copies every
resident record before writing (+318 MiB at 820k commits; the container peaked at 1.69 GB during
that flush, run with `--maintenance-interval 15s`).

**Why not a quick fix:** flushing by size under today's write policy (a full rewrite whenever the
resident tier is at least half the segments, or after 8 segments) would rewrite the whole graph
every ~8 flushes - at 10M commits, 2.5 GB every ~25 s at 20k commits/s. A size trigger needs a
compaction that keeps write amplification logarithmic.

**Proposed fix (needs a design pass on the graph file, #159's area):**

1. Flush the resident tier as a segment when it passes a size (e.g. 16 MiB / ~50k commits), on a
   goroutine, not only on the maintenance tick - *Cassandra:* memtables flush on
   `memtable_heap_space` / `memtable_cleanup_threshold`, not on a timer.
2. Merge graph segments size-tiered - merge the smallest similar-sized ones, never rewrite
   everything - *Cassandra:* SizeTieredCompactionStrategy. This needs a rule for crash
   consistency of a partial merge (today "a full segment supersedes lower sequences" is the only
   supersession rule).
3. Stream a flush from the resident chunks in key order instead of copying the whole tier first -
   *Cassandra:* a memtable flush iterates the memtable.

Expected: resident graph memory bounded at a few tens of MiB whatever the write rate, and no
2x peak at flush; the container at this workload would drop by ~300 MiB, to ~0.9 GB.

### M1 design - the commit graph as an LSM that flushes by size

The graph's resident tier is already a memtable and its segments already SSTables (#159): one
record format for both, a flush that writes the resident tier as a new segment, a full rewrite as
the major compaction. What is missing is everything that makes an LSM's memory bounded by size:
the trigger, a flush that does not copy, a compaction whose cost is logarithmic, and the read
path that makes many segments cheap. Nothing here changes a segment's bytes, a checkpoint's
bytes or a wire message.

| piece | Cassandra mechanism | KDB |
|---|---|---|
| when to flush | memtable flush when `memtable_heap_space` / `memtable_cleanup_threshold` is reached, not on a timer | the resident tier is flushed when it holds `graph.rebuildCommits` commits (50,000, ~20 MiB) - checked on every commit, run on a goroutine |
| how a flush reads | the memtable is switched out and flushed while a new one takes writes | the slots a flush reads are frozen; writes go to new slots (copy-on-write); no copy of the tier |
| which segments merge | SizeTieredCompactionStrategy: buckets of similar size (0.5x-1.5x the bucket mean), `min_threshold` 4, `max_threshold` 32, small files bucketed together | the same rule over record counts; the smallest full bucket merges first |
| dead records | single-SSTable tombstone compaction past `tombstone_threshold` 0.2 | a segment more than 20% hidden is rewritten alone (today: everything is rewritten when 20% of *all* records are hidden) |
| major compaction | `nodetool compact`: flush, then compact everything | `CompactGraphFile`: unchanged contract, one full segment |
| lookups across many files | a bloom filter per SSTable (`bloom_filter_fp_chance` 0.01) | a bloom filter per segment over its commit hashes and transaction ids, built when it is mapped |
| crash during a compaction | the lifecycle transaction log: new files become live, old ones go, as one step | no new file kind: a merge's inputs are deleted before its output gets its final name (below) |

**Thresholds.** The flush trigger is `graph.rebuildCommits`, the setting that was always
documented as this bound but was only read by the 5-minute maintenance pass. A commit that takes
the tier to the threshold starts the namespace's flusher if it is not already running (one
goroutine, exits when idle - no pool). Commits that may yet be taken back (provisional,
unlogged, and their descendants) stay resident, as today. The head is flushed like any commit:
only the first write after a flush finds its parent in a segment. The maintenance pass and
open still call `MaybeRebuildGraphFile`, now as a backstop. `graph.maxSegments` changes meaning:
from "rewrite everything past this many" to "past this many, merge the smallest four even if no
size tier is full"; its default rises from 8 to 16, since bloom filters make a segment that does
not hold a key nearly free to skip. Expected shape at 10M commits: tiers of ~50k, ~200k, ~800k
and ~3.2M records, at most three of each waiting, each record rewritten ~4 times in its life
(log4 of commits / 50k) instead of once per full rewrite.

**The streaming flush.** Today `snapshotForSegment` copies every resident record (256 B plus side
data) before writing - the 2x. Instead the flush, under the lock, notes `(hash, slot, flags)` for
each commit it will write (40 B each, sorted outside the lock) and freezes every slot below the
current high-water mark. While frozen, nothing writes a frozen slot: a write to one (a new
generation, a re-put with operations) copies the record to a fresh slot first and the old slot is
freed when the flush ends; deletes free nothing until then; the free list is not reused. Side data
(messages, octopus parents, attachment roots) is captured by reference - their values are never
mutated in place. The writer reads the frozen bytes with no lock held. Peak extra memory is the
40 B list, about a tenth of the tier, instead of all of it. The has-operations flag and
transaction ownership are decided at the snapshot, as now.

**Lookups while a flush or merge is in flight.** Unchanged in kind: a commit is in exactly one
place at every instant readers can see. During a flush the commits it writes are still resident
(their frozen slots are what the index points at); during a merge its inputs are still attached.
The swap is one critical section under the DAG's write lock: the new segment is attached, the
flushed commits leave the resident tier (or the inputs leave the segment list), and lookups that
start after it see the new layout. A flush and a merge can run at the same time - they touch
disjoint commits, the flush only commits no segment has a record for (as today) and the merge
only segment records - so a long merge never holds the resident tier above its bound. Attaching a
merge output costs O(hidden records + generation overrides), not O(records): the records it holds
are exactly its inputs' visible ones, and only what changed since the merge started (commits
deleted, generations recomputed) is reconciled. Attaching a flush drops exactly the flushed
commits' transaction-index entries, instead of probing every segment for every entry.

**Bloom filters.** Every write looks up three things that are almost always absent: its own
commit hash (twice) and its transaction id (the idempotent-retry check, twice). Today almost all
history is resident, so these are map misses; once it is flushed they would be misses against
every segment - a fanout plus interpolation search each, or ~20 probes of the transaction index -
on the write gate. A split-block bloom filter per segment (~10 bits per key, ~1% false positives,
pointer-free) answers those misses without touching the mapping: ~2.5 bytes per commit, built in
the same pass that checks the segment's checksum at open.

**Crash and reopen.** Two supersession rules exist today: segments never share a commit, and a
full segment supersedes every lower sequence (a crash between a full rewrite's rename and its
deletes). A partial merge cannot use the second rule, and older readers know no other, so merges
are ordered so that the files on disk never share a commit:

1. write the output under a temporary name, sync it, attach it in memory;
2. delete every input the checkpoint on disk does not name;
3. if no input remains on disk, rename the output to its final name; otherwise rename it to
   `graph-<seq>.kdbg.pending`, and finish the rename after the next checkpoint (which names the
   output) has been written and the inputs it replaced have been deleted.

A crash anywhere leaves a disjoint set: before (3) the output is not a segment (a stale temporary
or pending file, removed at open), and the inputs already deleted were not named by the
checkpoint, so either their commits are in the checkpoint's resident list or the log (the
checkpoint path) or every commit is resident (the replay path, where segments only take commits
out of the heap). The one window with a cost is between a checkpoint naming a pending output and
the rename: open then finds the `.pending` file, checks its sequence, checksum and count against
the checkpoint, and renames it - an older reader replays the log instead, which is correct. A
checkpoint pins the segments it names from the moment it captures them (today only after it is
written; a background merge could otherwise delete a segment between the two).

`CompactGraphFile` keeps its contract and its file order (rename, then delete or defer the
inputs): it holds the flush and merge locks, so its output - every segment plus the resident
tier, one segment marked full - really does supersede every lower sequence, and the existing rule
still covers a crash after its rename. The first segment a namespace writes is full, as today.

**Compatibility.** Segment layout (v5), checkpoint format and graph references are unchanged, so
older Go readers open everything a newer one leaves after a clean close, and after any crash too
(they ignore `.pending` and temporary files; at worst they replay). The Kotlin checkpoint reader
does not read graph segments at all - a checkpoint with graph references already makes it
replay - so nothing changes for it.

### M1 - implemented (branch `perf/rw-m1-graph-memory`)

As designed, plus what building it turned up:

- **The flush floor.** The threshold counts from what the last flush could not take (commits held
  back as provisional or unlogged), not from what was resident when it finished - counting the
  commits that arrived during the flush stopped the flusher after the first round, and the first
  test run showed a 500-commit threshold holding 17,678.
- **Sizing, not copying.** The first streaming flush still allocated 597 B per flushed commit: the
  entry list grown by `append` (Go grows large slices 1.25x at a time, so the allocations sum to
  ~5x the final size), the same for the transaction list, and a 1 MiB write buffer. Sized up front
  and with a 256 KiB buffer it is ~100 B per commit (`TestGraphFlushDoesNotCopyTheResidentTier`
  pins < 120; a copy of the tier is > 256).
- **`attachRoot` allocated on every call**, found or not (the variable escaped from inside the
  `if`): a pre-existing cost on every resident commit read, now only on a hit.
- **Syncs.** Segments were synced once at the end with `os.File.Sync`, which on macOS is
  `F_FULLFSYNC` - contrary to the format comment, harmless every 5 minutes, not every few seconds.
  Now a plain `fsync` on macOS, and on Linux a sync every megabyte as the segment is written
  (Cassandra's `trickle_fsync`, as D2 does for SSTables), so a flush never queues a whole segment
  ahead of the commit log's `fdatasync`.
- **Background writers stop before the close checkpoint** (`StopGraphWork`), so a merge never
  finishes after the last checkpoint and leaves its output pending for a checkpoint that never
  comes; a write in flight gives up and leaves everything where it was.

Tests (`go/kdb/dag/graph_lsm_test.go`): the bounded tier (resident never above threshold + one
batch, < threshold once idle, ~2.8 records written per commit at 20k commits), convergence when
appends outrun every flush, the flush's allocation, copy-on-write under concurrent re-puts and
generation rewrites, a random sequence of flushes/merges/major compactions/deletions checked
against the reader-visible view and the on-disk disjointness invariant after every step,
size-tiered bucketing, a merge waiting for the checkpoint, the three crash windows reopened
(old checkpoint, new checkpoint before the rename, replay), capture-time pinning, stopping at
close, and the bloom filter. `TestGraphResidentTierLongRun` (not `-short`) runs a million commits
or more (`KDB_GRAPH_LONGRUN`) with the heap sampled.

- **The flush's lock holds.** The first A/B (below) showed throughput unchanged but write p99.9 at
  32 workers doubled and max tripled: a 50,000-commit flush held the DAG's write lock ~8 ms to note
  what it takes and ~14 ms to attach the segment. Now the snapshot walks the frozen slots 1,024 at
  a time, releasing the lock between batches (a frozen record never changes, so a slot whose index
  entry no longer names it was freed or copied above the freeze), and the attach rebuilds the
  resident tier from what stays - held-back commits and those admitted since the freeze, all above
  it (the free list is not reused while frozen, so nothing lands below) - instead of deleting every
  flushed commit; the flushed commits' transaction-index entries go afterwards, a batch per lock
  hold. The longest a writer waits for the lock during a 50,000-commit flush: ~14 ms -> 1.2 ms
  (`TestGraphFlushWriterStall`). The 1.2 ms was the flusher winning the lock straight back
  between batches (`sync.Mutex` favours the running goroutine until a waiter has waited 1 ms), so
  a commit waited up to a millisecond at each of its lock acquisitions for the ~15 ms a snapshot
  took. Sleeping between batches as long as the last one held the lock: 0.25 ms. A GOGC=150 run
  ruled out the other suspect - more frequent GC (204 cycles over the matrix against 150, the
  333 MiB of pointer-free records having been ballast) - its tails were no better.
- **Merges are throttled to 64 MiB/s** - Cassandra's `compaction_throughput` default, for the same
  reason. Every A/B showed insert max latencies of 30-90 ms against base's 15-20: the insert
  cells run last, where the run's biggest size-tiered merge lands (four 200k-record segments into
  one, ~93 MB, nearly a second of a core in one burst on a 4-CPU container). 64 MiB/s is ~8x what
  merging needs on average at 25k commits/s (2.7 records of ~116 B a commit); flushes, which bound
  memory, and an operator's `CompactGraphFile` are not throttled.
- **Two latent defects, found by the server's deepen tests under `KDB_GRAPH_REBUILD_COMMITS=3`**
  (base passes them only because it never flushed in time; a maintenance flush between a
  snapshot bootstrap and a deepen hits both in production):
  - Generation recompute covered only the resident tier, on the premise that nothing arrives
    below a segment commit. A deepen does exactly that: the history under a snapshot root that
    was already flushed kept the root's stale generation, and pruned `IsAncestor` answered false.
    Segments now keep the set of parents they name outside themselves (their EXT sections);
    storing one of those, or `Unshallow` / `InvalidateGenerations`, makes the next recompute
    include the segments - O(history), on those rare paths only - writing overrides.
  - A checkpoint naming segments left unlogged commits out of its resident list only. One
    flushed between `MarkLogged` and a failed log write's `MarkUnlogged` came back on the next
    open with no operations to read. Such commits now go in the checkpoint's hidden list (the
    existing `GraphHidden`; no format change).

### M1 - measured

Base: `perf/rw-docs-results` (305dbb9a, now on main as #353). rwcompare matrix at 100k documents,
`ab.sh`, two alternating passes per binary, the frozen base worktree's harness; this Mac's load
was 8-13 throughout (another project's `llama-server` held 3-4 cores), the same for both binaries.

**Memory, after the full matrix sequence** (`mem.sh`-style: load + read/update/mixed50/insert at
1/8/32, ~820k commits, fast durability; peaks sampled every 2 s during the run):

| | base | M1 |
|---|--:|--:|
| `docker stats` at the end | 1,289 MiB | 861-864 MiB |
| cgroup anon (Go heap + runtime) | 1,264 MiB | 807 MiB |
| Go heap in use (`kdb_go_heap_alloc_bytes`) | 1,085 MiB | 698 MiB |
| VmHWM (peak RSS) | 1,365 MiB | 1,129 MiB |
| commit graph, at the end | 333 MiB | 21 MiB |
| commit graph, peak during the run | 333 MiB | 21 MiB |
| GC cycles over the run | 150 | 204 |

The container drops ~425 MiB, more than the 300 the M1 measurement predicted: the tier's index
and transaction-index entries went with it. Every other structure is the same size or smaller
(the live tree 75 -> 65 MiB).
KDB at this workload now sits at ~0.86 GB against MongoDB 7's 0.6-0.7 GB.

**Throughput, final binary** (ops/s means; per-pass values in parentheses; reads are the control):

| cell | c | fast m1/base | durable m1/base |
|---|--:|--:|--:|
| read | 1 / 8 / 32 | 1.09 / 1.03 / 0.98 | 0.96 / 1.00 / 1.01 |
| update | 1 / 8 / 32 | 1.01 / 1.02 / 1.00 | 0.96 / 0.93 / 0.98 |
| mixed50 | 1 / 8 / 32 | 1.01 / 0.99 / 0.98 | 0.94 / 0.96 / 0.99 |
| insert | 1 / 8 / 32 | 0.93 / 0.97 / 0.97 | 1.01 / 1.03 / 0.99 |
| load | 32 | 1.08 | 1.02 |

Write cells' geometric mean: 0.986 (fast) and 0.976 (durable) in this A/B, 0.982 and 0.990 in the
first (the binary before the lock-hold fix); reads' 1.032 and 0.990, then 0.960 and 0.969. No cell
moved the same way by more than 5% in both A/Bs except insert@1 fast (0.94, 0.93), whose base
passes themselves spread 8%. Write throughput: unchanged.

**Tails at 32 workers** (write p99.9 / max, ms, the two passes):

| cell | base | M1 before the lock fix | M1 |
|---|---|---|---|
| update fast | 9.1/23, 8.7/19 (first A/B: 8.0/13, 7.9/13) | 16.8/36, 14.8/41 | 12.9/31, 14.3/29 |
| mixed50 fast | 10.5/36, 9.1/44 | 16.9/51, 22.7/80 | 16.7/41, 17.7/42 |
| update durable | 8.5/15, 7.6/22 | 16.7/36, 14.7/39 | 12.3/20, 13.0/31 |

p99 is unchanged (4-5 ms either way); p99.9 is still ~1.5x base after the lock fix.
What it was not: GC. M1 runs 36% more GC cycles over the matrix (204 against 150) - the 333 MiB
of pointer-free records it removed were ballast that raised GOGC's trigger - but M1 with
`GOGC=150`, which brings the cadence back, had no better tails (c=32 only, two passes, load 5-7:
update p99.9 13.5/13.1 ms against M1's 11.1/10.2 and base's 8.0/8.9). What was left is the flush's
batched snapshot letting the flusher win the lock back between batches; see the yield above, and
the final A/B below.

**Long run** (`KDB_GRAPH_LONGRUN=2000000 go test ./kdb/dag -run TestGraphResidentTierLongRun`,
the default 50,000-commit threshold, heap sampled every 20 ms): 2,000,000 commits in 3.3 s; the
resident tier between 49,490 and 50,000 commits at every sample, the commit graph's heap 19-24
MiB at the samples and 31 MiB at its peak, heap in use 22-90 MiB with no doubling at a flush;
2.68 records written per commit; 6-8 segments, 216 MiB on disk. Before M1 the same run would have
held all 2,000,000 resident (~800 MB).

## Not done yet - the plan

| # | item | Cassandra mechanism | expected | status |
|---|---|---|---|---|
| W8 | **Several writes per commit**: the only way past the per-namespace commit rate (~22-25k/s) | commit-log group sync + applying a batch of mutations as one update | concurrent writes towards MongoDB's 46-60k/s | **design below; needs the user's decisions before any code** |
| E2 | Embedded cache default: fixed 64 MiB vs machine-sized | caches sized from the heap | embedded reads with hot data > 64 MiB | **decided: option 3, built (below)** |
| W9 | **Stale premerges** (new, from the W7 profile): a zipfian-hot document is often written again between an Upsert's premerge and its turn at the gate, so the gate re-merges it - 11% of gate time at update@32. Merge faster at the gate: one parse of the stored body instead of several - the merge is of top-level keys only | `AtomicBTreePartition` retries its CAS before taking the lock | up to ~10% of gate time on hot keys | low (faster merge) |
| M1 | Bound the commit graph's resident tier: flush by size, merge graph segments size-tiered, stream the flush | memtable flush on heap space; SizeTieredCompactionStrategy; per-SSTable bloom filters; trickle_fsync | ~300 MiB less at this workload, no 2x flush peak, bounded at any write rate | **done** on `perf/rw-m1-graph-memory`: 1,289 -> 861 MiB, graph 333 -> 21 MiB, writes 0.98x (noise); see [M1 - measured](#m1---measured) |

### W8 - several writes per commit (design; not implemented)

**Why:** every write is one commit, and a commit does serially, under the namespace's write gate,
work whose cost does not depend on how much the commit changes: one Merkle path from leaf to root
(a node hash at each of ~7 levels, slab records placed, child records read), one DAG append
(payload encode + SHA-256), one log record, one tree publish. The W7 profile puts the gate at ~40
us per commit (trie update 32%, DAG append 15%, a re-merge for documents whose premerge went stale
11%), so a namespace commits at most ~25k times a second whatever the CPU count; MongoDB's 46-60k concurrent writes/s cannot be
reached one write per commit. Committing the writes that are waiting at the gate together pays
that fixed cost once per batch, and N documents' Merkle paths share their upper levels, so the
trie work per write falls too.

**Cassandra:** the commit log's group sync (`commitlog_sync: group`/`batch`) already shares one
fsync across concurrent mutations, which KDB copies today; W8 goes one step further, like applying
an unlogged batch of mutations to a partition as one update: the memtable (here, the trie) is
updated once for all of them.

**Mechanism (flat combining at the gate):** the write gate becomes a queue of requests instead of a
semaphore. The writer that takes the gate drains the requests queued behind it - up to a cap (64
writes or 1 MiB of bodies) - and commits their operations as one transaction: one `CommitTree`
(one trie update over all the documents), one DAG append, one log record. Each waiting writer is
handed the result for its own write and waits for that commit's durability, exactly as now. Off
by default (`--coalesce-writes`, `coalesceWrites` in the config file, `StorageOptions` for
embedders); with it off nothing changes.

**What it is allowed to batch:** only last-write-wins writes at head - `Upsert`, `Replace`,
`Delete` (the paths that today never conflict). Transactions with a base version (STRICT/conflict
detection), schema migrations, cross-namespace groups and anything with attachments stay one per
commit, so no conflict report ever has to be split across clients.

**Semantics, and what changes for users:**

| area | today | with W8 | proposed rule |
|---|---|---|---|
| per-document history | every version of a document has its own commit and tree | unchanged if a batch never holds two writes to one document | a second write to a document already in the batch closes the batch; it starts the next one |
| commit history | one commit per client write | one commit per batch; a commit's operations come from several clients | documented; `kdb_namespace_commits` counts batches |
| reads "as of" a commit | every intermediate state has a tree | no tree between the writes of one batch | acceptable: each tree is still a consistent state that some order of the writes produced |
| tx-id idempotency (`txIndex`: transaction id -> commit, used by retries) | a transaction's `TransactionID` is the client's; `Upsert`, `Replace` and `Delete` mint theirs on the server, so nothing retries by them | the batch commit carries one fresh id | batch only writes whose id the server minted - all three paths today. If a client-supplied id ever reaches them: (a) per-write ids in memory only (a retry after a restart re-applies: a second, identical LWW commit), or (b) a new optional commit field - a format change, Kotlin included - is the decision |
| validation failures | the write fails alone | each write is validated alone at the gate front (premerge already does most of it); a failing one is dropped from the batch with its own error, the rest commit | |
| peer sync | commits travel one per write | batches travel as ordinary multi-op commits - no wire or format change; LWW merge resolves per document as now | |
| conflict reports (STRICT) | per commit | not affected: STRICT writes are never batched | |
| durability / acknowledgement | each write waits for its commit | each waits for the batch commit; group sync is unchanged | |
| audit / authorship | one author per commit | one author per batch commit | batch only writes from the same principal, or record the commit as the runtime's own (decision) |

**Expected effect (estimate, to be measured):** at 32 writers a batch would hold up to 31 queued
writes. If the ~40 us per commit splits into roughly 25 us that a batch pays once and 15 us per
write (the re-merge, the leaf, the body store), update throughput at 32 workers would move from
~22-24k/s towards ~50k/s, with the CPU spent before the gate (premerge, the wire) the next limit.
Single-writer latency is unchanged: a lone writer forms a batch of one.

**Open questions for the user:** whether "one commit = one client write" is a property any user or
tool relies on (history UIs, `kdb inspect`, audit exports, peers counting commits); whether a
batch may mix principals (or the batch commit is authored by the runtime); the cap; and the tx-id
rule should a client-supplied id ever reach these paths.

### E2 - embedded cache default (decided: option 3)

**Decided (2026-11-22): option 3, still configurable.** Built in dbeasty/kdb#354:

- `StorageOptions.MemoryBudgetBytes` set: unchanged. The figure is that runtime's (or that host's)
  own.
- Unset: `embed.DefaultMemoryBudgetBytes()`, a sixteenth of the memory the process may use (the
  cgroup limit, else physical RAM), at least 64 MiB (the old fixed value, so no host gets less) and
  at most 256 MiB. Android and iOS stay at 64 MiB.
- One pool per process: every default-sized host, runtime and `database/sql` connection registers
  with one shared, reference-counted `BudgetArbiter` (the one hosts already used per host), so N
  stores divide the default instead of each taking it. It is sized when the first store opens; the
  last close stops its goroutine.
- Cassandra's shape for caches nobody sized: a fraction with a cap. `key_cache_size` defaults to
  min(5% of heap, 100 MB), `file_cache_size` to min(¼ of heap, 512 MB). kdb-service takes an
  eighth (it owns its container); an embedded store takes half that.

Measured with the same `kdb-embed` setup (4 CPUs / 3 GB, so the new default is 192 MiB; 100k 1 KB
documents; `-embed-cache 67108864` for the old default, `-1` for the new one, `0` for the service's
384 MiB). Two passes in alternating order, each under the shared benchmark lock, and any run that
another benchmark container overlapped was thrown away and redone (none had to be). Ops/s, mean
(passes). Reads, 1M-operation cells:

| cell | c | old 64 MiB | new default (192 MiB) | service sizing (384 MiB) | new / old |
|---|--:|--:|--:|--:|--:|
| read | 1 | 313,798 (306,194 / 321,401) | 532,213 (485,733 / 578,692) | 1,081,952 | 1.70x |
| read | 8 | 955,393 (978,366 / 932,419) | 1,683,043 (1,591,233 / 1,774,853) | 3,634,267 | 1.76x |
| read | 32 | 1,070,101 (1,062,629 / 1,077,574) | 1,978,140 (1,882,939 / 2,073,341) | 3,980,106 | 1.85x |
| read95 | 1 | 165,118 (166,298 / 163,939) | 220,193 (214,185 / 226,201) | 284,718 | 1.33x |
| read95 | 8 | 170,281 (175,303 / 165,259) | 213,101 (222,765 / 203,438) | 284,203 | 1.25x |
| read95 | 32 | 151,378 (151,213 / 151,543) | 201,362 (197,798 / 204,925) | 253,970 | 1.33x |
| peak RSS after the cells | | 608-615 MB | 878-880 MB | 1,057-1,077 MB | +270 MB |

Writes, default op counts, old and new only:

| cell | c | old 64 MiB | new default (192 MiB) | new / old |
|---|--:|--:|--:|--:|
| update | 1 / 8 / 32 | 19,842 / 20,275 / 19,033 | 20,383 / 19,378 / 19,016 | 1.03x / 0.96x / 1.00x |
| insert | 1 / 8 / 32 | 27,130 / 26,129 / 25,608 | 30,116 / 28,333 / 25,927 | 1.11x / 1.08x / 1.01x |
| first 100k inserts (load phase, 4 runs each) | 32 | 29,027-34,140 | 28,694-33,242 | 0.95x, ranges overlap |
| peak RSS after the cells | | 1.17 GB | 1.41-1.50 GB | +240-320 MB |

Reads gain 1.7-1.85x and read95 1.25-1.33x; the service's sizing would roughly double that again,
at another ~190 MB. Writes are unchanged (0.96-1.11x). The memory cost is about twice the budget
increase (128 MiB more budget, ~270 MB more resident): Go's collector lets the heap grow to twice
what is live (`GOGC=100`), so a bigger cache carries more garbage too. That is the trade an embedder
now gets by default, and `MemoryBudgetBytes = 64 << 20` undoes it.

A first A/B overlapped with another session's unlocked kdb-service runs. It agreed on reads but
showed a 0.88x load phase that the clean rerun does not reproduce.

**Before the decision.** kdb-service sizes its document cache from the machine (an eighth of the container, via
`server.CachePoolBytes`); an embedded runtime takes a fixed 64 MiB (`MemoryBudgetBytes` zero). An
embedded store whose hot data is larger than that serves reads from cold loads. Measured with the
benchmark's `kdb-embed` target (KDB in the client process, 4 CPUs / 3 GB, bounded live bodies as
`FileRuntimeOptionsFromEnv` sets them, 100k 1 KB documents, 1M-operation cells), the embedded
default against the service's sizing (384 MiB pool), two passes alternated:

| cell | c | 64 MiB (embedded default) | 384 MiB (service sizing) | sized / default |
|---|--:|--:|--:|--:|
| read | 1 | 351,916 | 1,169,946 | 3.32x |
| read | 8 | 835,292 | 3,996,327 | 4.78x |
| read | 32 | 1,048,759 | 4,350,950 | 4.15x |
| read95 | 1 | 169,913 | 318,159 | 1.87x |
| read95 | 8 | 183,098 | 293,606 | 1.60x |
| read95 | 32 | 170,538 | 267,191 | 1.57x |

So an embedder with ~100 MB of hot documents gets a quarter of the read throughput the same
library gives when it is sized like the service (still 16-35x MongoDB over its network, but the
default decides it, and most embedders never change defaults). The cost of sizing up is the cache
itself: 320 MiB more resident in the host process at this data size.

Options:

1. **Keep 64 MiB** (today): predictable for a host process that embeds KDB next to its own data;
   embedders who want more set `StorageOptions.MemoryBudgetBytes` (or call
   `server.CachePoolBytes`). Document it prominently.
2. **Machine-sized, like the service** (1/8 of the cgroup limit or physical memory): fastest
   reads out of the box, but an embedded library that takes an eighth of its host's memory
   without being asked is a surprise, and N embedded namespaces each sized that way would
   overcommit unless the budget is shared.
3. **Middle:** min(1/16 of the limit, 256 MiB), shared across the runtimes of one process.

Recommendation: 1 or 3 - a library should not size itself like a server; the choice is the
user's.

## Results after the follow-up stack

Final matrix, 2026-11-22, on the top of the stack (#352) with kdb-service's defaults (on Linux:
`fdatasync` into preallocated segments, D1; spare segments and trickle sync, D2), two passes in
alternating order, every number the mean of both: v0.9.4, the branch, the branch embedded in the
benchmark process (no network, same 4 CPUs and 3 GB, the service's cache sizing; durable embedded
runs use the embedded defaults - full `fsync`, no preallocation - so D1/D2 do not apply to them),
and MongoDB 7. ops/s; ratios against v0.9.4 and MongoDB. Every pass was checked against the host's
load while it ran (peak 1-minute load 4.6-7.0; a pass that saw more than 10 was rerun - another
project's test suite and a Spotlight reindex shared the machine that evening).

### fast

| cell | c | v0.9.4 | branch | branch embedded | MongoDB 7 | branch / v0.9.4 | branch / Mongo | embedded / Mongo |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| load | 32 | 18,915 | 24,772 | 33,061 | 61,957 | 1.31x | **0.40x** | 0.53x |
| read | 1 | 14,871 | 14,484 | 1,018,749 | 11,303 | 0.97x | **1.28x** | 90.13x |
| read | 8 | 103,798 | 103,323 | 4,023,452 | 61,630 | 1.00x | **1.68x** | 65.28x |
| read | 32 | 125,055 | 120,148 | 4,079,095 | 67,992 | 0.96x | **1.77x** | 59.99x |
| update | 1 | 8,228 | 7,836 | 21,637 | 10,718 | 0.95x | **0.73x** | 2.02x |
| update | 8 | 14,122 | 21,907 | 21,995 | 55,706 | 1.55x | **0.39x** | 0.39x |
| update | 32 | 14,007 | 19,961 | 20,337 | 48,121 | 1.43x | **0.41x** | 0.42x |
| mixed50 | 1 | 10,203 | 9,674 | 41,715 | 11,254 | 0.95x | **0.86x** | 3.71x |
| mixed50 | 8 | 20,258 | 42,209 | 41,160 | 58,323 | 2.08x | **0.72x** | 0.71x |
| mixed50 | 32 | 24,052 | 42,120 | 36,725 | 54,836 | 1.75x | **0.77x** | 0.67x |
| read95 | 1 | 13,708 | 13,303 | 324,528 | 10,209 | 0.97x | **1.30x** | 31.79x |
| read95 | 8 | 65,827 | 95,031 | 290,765 | 59,341 | 1.44x | **1.60x** | 4.90x |
| read95 | 32 | 83,062 | 106,862 | 290,926 | 62,682 | 1.29x | **1.70x** | 4.64x |
| insert | 1 | 8,484 | 8,542 | 30,185 | 9,740 | 1.01x | **0.88x** | 3.10x |
| insert | 8 | 17,454 | 22,098 | 28,877 | 47,034 | 1.27x | **0.47x** | 0.61x |
| insert | 32 | 16,980 | 20,819 | 26,939 | 57,677 | 1.23x | **0.36x** | 0.47x |

write p99 / max at 32 workers (fast), ms, per pass:

| cell | v0.9.4 | branch | branch embedded | MongoDB 7 |
|---|--:|--:|--:|--:|
| update | 4.0 / 45 ; 4.0 / 1658 | 3.9 / 17 ; 4.0 / 16 | 2.9 / 27 ; 2.8 / 20 | 2.7 / 65 ; 2.2 / 62 |
| mixed50 | 4.5 / 1357 ; 4.1 / 1378 | 3.2 / 24 ; 3.4 / 18 | 3.5 / 33 ; 2.8 / 27 | 2.1 / 54 ; 2.4 / 71 |
| insert | 3.0 / 21 ; 2.8 / 18 | 2.7 / 20 ; 2.9 / 20 | 2.4 / 119 ; 2.3 / 142 | 2.0 / 56 ; 2.4 / 54 |

### durable

| cell | c | v0.9.4 | branch | branch embedded | MongoDB 7 | branch / v0.9.4 | branch / Mongo | embedded / Mongo |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| load | 32 | 14,874 | 26,784 | 22,965 | 28,595 | 1.80x | **0.94x** | 0.80x |
| read | 1 | 14,554 | 13,955 | 1,206,095 | 10,620 | 0.96x | **1.31x** | 113.57x |
| read | 8 | 106,924 | 102,924 | 4,982,368 | 61,608 | 0.96x | **1.67x** | 80.87x |
| read | 32 | 132,480 | 126,118 | 5,158,308 | 68,188 | 0.95x | **1.85x** | 75.65x |
| update | 1 | 2,074 | 4,488 | 3,477 | 4,816 | 2.16x | **0.93x** | 0.72x |
| update | 8 | 7,197 | 12,964 | 6,312 | 16,252 | 1.80x | **0.80x** | 0.39x |
| update | 32 | 14,785 | 23,967 | 20,005 | 28,379 | 1.62x | **0.84x** | 0.70x |
| mixed50 | 1 | 4,668 | 6,199 | 6,072 | 7,206 | 1.33x | **0.86x** | 0.84x |
| mixed50 | 8 | 15,376 | 22,292 | 17,167 | 26,614 | 1.45x | **0.84x** | 0.65x |
| mixed50 | 32 | 24,974 | 43,176 | 37,566 | 45,295 | 1.73x | **0.95x** | 0.83x |
| read95 | 1 | 16,038 | 14,331 | 65,680 | 17,965 | 0.89x | **0.80x** | 3.66x |
| read95 | 8 | 69,849 | 88,128 | 164,581 | 58,613 | 1.26x | **1.50x** | 2.81x |
| read95 | 32 | 91,402 | 106,050 | 370,937 | 62,721 | 1.16x | **1.69x** | 5.91x |
| insert | 1 | 2,867 | 4,141 | 3,736 | 4,623 | 1.44x | **0.90x** | 0.81x |
| insert | 8 | 8,601 | 13,047 | 8,614 | 11,531 | 1.52x | **1.13x** | 0.75x |
| insert | 32 | 16,976 | 24,572 | 20,304 | 24,490 | 1.45x | **1.00x** | 0.83x |

write p99 / max at 32 workers (durable), ms, per pass:

| cell | v0.9.4 | branch | branch embedded | MongoDB 7 |
|---|--:|--:|--:|--:|
| update | 5.4 / 1294 ; 4.8 / 1203 | 4.1 / 12 ; 4.1 / 13 | 4.0 / 198 ; 3.0 / 17 | 2.1 / 74 ; 2.2 / 181 |
| mixed50 | 5.3 / 1521 ; 5.0 / 1190 | 4.7 / 46 ; 4.5 / 75 | 6.6 / 50 ; 4.6 / 145 | 4.5 / 69 ; 3.8 / 33 |
| insert | 7.0 / 142 ; 5.1 / 177 | 3.7 / 115 ; 3.1 / 21 | 3.1 / 353 ; 3.2 / 114 | 3.6 / 110 ; 4.5 / 357 |

What it says:

- **Reads** stay 1.3-1.9x MongoDB over the wire (point reads) and 1.5-1.7x on 95/5 mixes from 8
  workers; embedded, 1-5M point reads/s.
- **Durable writes are at parity with MongoDB `j:true`:** 0.80-1.13x on writes, the load phase
  0.94x, inserts at 8 and 32 workers 1.13x and 1.00x - against v0.9.4's 0.43-0.69x. And their tails
  are better than MongoDB's: worst update at 32 workers 12-13 ms against 74-181 ms.
- **Fast writes are where they were (0.36-0.47x MongoDB from 8 workers):** the per-namespace commit
  rate (~20-24k/s) is the limit, and nothing in this stack changes how many commits a namespace can
  make. W8 is the item that does.
- **Single-worker writes:** 0.73-0.88x MongoDB fast, 0.90-0.93x durable.

### The whole stack against its base

Does the stack cost anything anywhere? The top of the stack against this branch (#345), fast,
three passes alternated, every pass load-checked; write p99.9 / max ms per pass:

| cell | c | base | final | final / base | write p99.9 / max ms (base, final) |
|---|--:|--:|--:|--:|---|
| load | 32 | 26,202 (27,616 / 25,642 / 25,347) | 28,442 (29,072 / 28,296 / 27,959) | 1.09x | 9.8/31, 10.0/39, 11.2/33 ; 7.8/30, 8.8/33, 9.4/31 |
| read | 1 | 13,428 (13,899 / 13,452 / 12,931) | 13,684 (13,435 / 13,859 / 13,759) | 1.02x |  |
| read | 8 | 102,131 (100,494 / 105,979 / 99,919) | 103,284 (105,569 / 102,430 / 101,853) | 1.01x |  |
| read | 32 | 126,396 (127,737 / 129,730 / 121,720) | 127,218 (127,925 / 128,548 / 125,182) | 1.01x |  |
| update | 1 | 8,205 (8,309 / 8,206 / 8,100) | 8,195 (8,316 / 7,825 / 8,445) | 1.00x | 0.5/10, 0.4/10, 0.5/11 ; 0.4/9, 0.5/9, 0.4/10 |
| update | 8 | 24,238 (24,571 / 24,855 / 23,289) | 23,855 (25,427 / 22,623 / 23,515) | 0.98x | 3.8/17, 3.9/21, 4.2/17 ; 3.7/11, 4.4/15, 3.9/13 |
| update | 32 | 21,622 (22,071 / 21,983 / 20,811) | 20,942 (22,612 / 20,110 / 20,105) | 0.97x | 7.9/25, 10.5/23, 8.3/22 ; 7.1/14, 7.3/195, 7.8/14 |
| mixed50 | 1 | 9,773 (9,940 / 9,945 / 9,433) | 9,695 (9,927 / 9,796 / 9,363) | 0.99x | 0.5/9, 0.5/9, 0.6/12 ; 0.9/9, 1.2/27, 0.7/10 |
| mixed50 | 8 | 41,780 (43,102 / 42,600 / 39,638) | 43,800 (44,763 / 44,101 / 42,535) | 1.05x | 4.7/10, 4.9/18, 5.2/16 ; 4.4/6, 4.7/6, 5.0/6 |
| mixed50 | 32 | 42,825 (43,421 / 43,555 / 41,500) | 42,430 (44,343 / 40,434 / 42,513) | 0.99x | 7.3/18, 11.5/27, 10.9/26 ; 6.9/17, 7.6/45, 8.1/18 |
| insert | 1 | 8,387 (8,450 / 8,415 / 8,297) | 8,597 (8,775 / 8,253 / 8,762) | 1.02x | 0.5/5, 0.9/5, 0.5/6 ; 0.5/5, 0.5/4, 0.7/6 |
| insert | 8 | 24,908 (24,917 / 25,139 / 24,667) | 23,926 (25,843 / 22,140 / 23,795) | 0.96x | 4.6/20, 4.8/19, 5.4/20 ; 2.8/11, 4.8/15, 3.8/13 |
| insert | 32 | 24,095 (24,322 / 24,237 / 23,725) | 23,728 (24,656 / 23,457 / 23,072) | 0.98x | 13.6/22, 15.3/21, 15.0/20 ; 14.4/17, 13.8/22, 14.1/20 |

Everything is within 0.96-1.09x (read controls 1.01-1.02x). One pass of the final binary had a
single 195 ms write in update@32 (the other two: 14 ms; base: 22-25 ms). It is not a rotation
waiting for its spare - preparing a 64 MiB spare takes 47-62 ms with trickle syncs (30 ms with one
sync) on this disk, against ~1.3 s for half a segment to fill at this rate - and it did not recur;
it is recorded here unattributed.

## Results at #345 (before the follow-up stack)

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
- **`run.sh` compiles the KDB module from the checkout it lives in** (the client embeds KDB for
  the `kdb-embed` target), so editing Go code in that checkout during an A/B can fail a run's
  client build and silently drop that run from the table. Run A/Bs from a separate, frozen
  worktree's copy of the harness.
- **A background writer's big final sync shows up as commit latency.** On this Docker VM a 64 MiB
  zero-fill or an SSTable's single closing `fsync` made the commit log's concurrent `fdatasync`
  take 20-30 ms. Find it with an execution trace (`/debug/pprof/trace`), not a CPU profile: the
  CPU profile sees nothing.
- `storage/delta TestListSegmentsRescansASegmentChangedInPlace` fails in a `golang:1.26` container
  (overlayfs mtime granularity) on any commit; it passes on macOS and in CI.
- **`docker stats` is not the page cache.** It reports cgroup usage minus inactive file pages; at
  the end of the matrix KDB's 1.25 GB of written log and SSTable pages were all inactive and
  uncounted, and the 1.2 GB shown was Go heap (cgroup `anon`). Read `memory.stat` (a sidecar
  container with `--pid container:<kdb> --cgroupns host -v /sys/fs/cgroup:/sys/fs/cgroup:ro`)
  before blaming mapped files or the page cache.
- **Read cells swing 5-9% between runs of the same code.** W7's read control came out 5% low in
  both passes of a short-cell A/B, then equal in one pass and 9% low in the other of a 1M-op rerun,
  for a change that runs nothing on the read path. Use 1M-op read cells, and more than two passes
  before reading a single-digit read delta into anything.
