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
0.36-0.47x, bound by the per-namespace commit rate (~20-24k/s). **W8 (several writes per commit)
is now built, off by default (`--coalesce-writes`):** it roughly doubles concurrent writes at 32
workers (update 1.81x, insert 2.03x, fast) at the cost of 9-14% on cold point reads, see
[W8](#w8---several-writes-per-commit-built-off-by-default). M1 found what holds KDB's memory - the
commit graph's resident tier, unbounded between maintenance passes - and is now fixed (#356,
[M1 - measured](#m1---measured)): the commit graph flushes by size and merges size-tiered, the
container at this workload drops from 1.29 GB to 0.9 GB (MongoDB 7: 0.6-0.7 GB), write throughput
unchanged. Nothing waits on the user: E2 is decided (option 3, built; below), and W8 is built, off
by default - whether to turn it on by default is open.

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
W6 #347 -> W5 #348 -> R1 #349 -> R2 #350 -> W7 #351 -> E1 #352 -> this document's results (#353)
-> W8, several writes per commit (below, under the plan).

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

Final: this branch's head (d2c26fd4, main merged in) against main at 0cd84dc5 (#345-#354, E2
included). rwcompare matrix at 100k documents, `ab.sh`, two alternating passes per binary, run
from a frozen worktree's harness under the benchmark lock; reads are the control.

**Memory, after the full matrix sequence** (load + read/update/mixed50/insert at 1/8/32, ~820k
commits, fast durability, 3 GB container; peaks sampled every 2 s during the run):

| | main | M1 |
|---|--:|--:|
| `docker stats`, end / peak | 1,292 / 1,291 MiB | 906 / 913 MiB |
| cgroup anon (Go heap + runtime) | 1,205 MiB | 851 MiB |
| Go heap (`kdb_go_heap_alloc_bytes`) | 1,006 MiB | 733 MiB |
| VmHWM (peak RSS) | 1,374 MiB | 1,184 MiB |
| commit graph, end / peak | 333 / 333 MiB | 3.5 / 19.8 MiB |
| GC cycles over the run | 155 | 183 |

The container drops ~385 MiB (and ~425 MiB in the earlier rounds against 305dbb9a: 1,289 -> 861),
more than the ~300 the M1 measurement predicted - the tier's index and transaction-index entries
went with it. Every other structure is the same size or smaller. KDB at this workload is now at
~0.9 GB against MongoDB 7's 0.6-0.7 GB.

**Throughput** (m1/main; c = 1 / 8 / 32):

| cell | fast | durable |
|---|--:|--:|
| read | 1.22 / 1.07 / 0.96 | 1.02 / 0.99 / 0.99 |
| update | 0.99 / 0.99 / 0.98 | 0.94 / 0.93 / 0.96 |
| mixed50 | 0.98 / 1.00 / 0.98 | 1.06 / 1.04 / 0.98 |
| insert | 0.96 / 1.00 / 1.00 | 1.05 / 1.06 / 0.99 |

Write cells' geometric mean: fast 0.987, durable 1.000. Write throughput: unchanged. (The first
durable run of this pair was discarded: another job's disk traffic swung the control reads 50%
between passes; the rerun above had load 5-6 and reads within 2%.) One cell to watch: durable
update@8 averaged 0.955 over the four clean durable rounds (0.98, 0.93, 0.98, 0.93) while durable
inserts averaged 1.016 - plausibly the graph's own syncs (~280 B a commit, a sync every MiB)
sharing the device with the commit log's `fdatasync`. The follow-up, if it proves real: sync a
segment only when a checkpoint is about to name it (one sync per checkpoint for everything flushed
since), which a torn unsynced segment already tolerates - it fails its checksum and is rewritten.
Tails at 32 workers (fast, write p99.9 per pass): update 7.7 / 7.8 ms against main's 7.7 / 7.2,
insert 11.6 / 13.6 against 13.4 / 16.6, mixed50 9.5 / 14.3 against 7.9 / 8.7; p99 unchanged.

**How the tails got there** (earlier rounds, each against 305dbb9a, two passes each):

| build | update@32 fast p99.9 / max (ms) | what changed |
|---|---|---|
| base | 8.0-9.1 / 13-23 | |
| first cut | 16.8, 14.8 / 36, 41 | a 50k flush held the DAG lock ~8 ms, then ~14 ms |
| batched snapshot, attach O(survivors) | 12.9, 14.3 / 31, 29 | longest writer wait 14 -> 1.2 ms |
| + sleep between batches | 9.1, 9.5 / 34, 19 | 1.2 -> 0.25 ms; fast inserts still 0.93-0.94x, insert max 30-90 ms |
| + merges throttled to 64 MiB/s (final) | 7.7, 7.8 / 17, 16 (vs main) | inserts 1.00x |

Write-cell geomeans of those rounds (fast / durable): 0.982 / 0.990, 0.986 / 0.976, 0.990 /
1.020. What it was not: GC. M1 runs 18-36% more GC cycles over the matrix - the 333 MiB of
pointer-free records were ballast under GOGC's trigger - but M1 with `GOGC=150`, which brings the
cadence back, had no better tails (update@32 p99.9 13.5 / 13.1 ms against M1's 11.1 / 10.2).

**Long run** (`KDB_GRAPH_LONGRUN=2000000 go test ./kdb/dag -run TestGraphResidentTierLongRun`,
the default 50,000-commit threshold, heap sampled every 20 ms): 2,000,000 commits; the resident
tier between 49,490 and 50,000 commits at every sample, the commit graph's heap 19-24 MiB at the
samples and 31-34 MiB at its peak, heap in use 22-98 MiB with no doubling at a flush; 2.40 records
written per commit with merges throttled (2.68 without); 4-8 segments, 216 MiB on disk. Before M1
the same run would have held all 2,000,000 resident (~800 MB).

## Not done yet - the plan

| # | item | Cassandra mechanism | expected | status |
|---|---|---|---|---|
| W8 | **Several writes per commit**: the only way past the per-namespace commit rate (~22-25k/s) | group commit, taken from one shared sync to one shared update | concurrent writes towards MongoDB's 46-60k/s | **built, off by default (below)**: 1.8-2.2x writes at 32 workers, -9 to -14% cold point reads; whether to turn it on by default is open |
| E2 | Embedded cache default: fixed 64 MiB vs machine-sized | caches sized from the heap | embedded reads with hot data > 64 MiB | **decided: option 3, built (below)** |
| W9 | **Stale premerges** (new, from the W7 profile): a zipfian-hot document is often written again between an Upsert's premerge and its turn at the gate, so the gate re-merges it - 11% of gate time at update@32. Merge faster at the gate: one parse of the stored body instead of several - the merge is of top-level keys only | `AtomicBTreePartition` retries its CAS before taking the lock | up to ~10% of gate time on hot keys | low (faster merge) |
| M1 | Bound the commit graph's resident tier: flush by size, merge graph segments size-tiered, stream the flush | memtable flush on heap space; SizeTieredCompactionStrategy; per-SSTable bloom filters; trickle_fsync | ~300 MiB less at this workload, no 2x flush peak, bounded at any write rate | **done** (#356): 1,292 -> 906 MiB, graph 333 -> 20 MiB, writes unchanged; see [M1 - measured](#m1---measured) |

### W8 - several writes per commit (built, off by default)

**Why:** every write is one commit, and a commit does serially, under the namespace's write gate,
work whose cost does not depend on how much the commit changes: one Merkle path from leaf to root
(a node hash at each of ~7 levels, slab records placed, child records read), one DAG append
(payload encode + SHA-256), one log record, one tree publish. The W7 profile puts the gate at ~40
us per commit (trie update 32%, DAG append 15%, a re-merge for documents whose premerge went stale
11%), so a namespace commits at most ~25k times a second whatever the CPU count; MongoDB's 46-60k concurrent writes/s cannot be
reached one write per commit. Committing the writes that are waiting at the gate together pays
that fixed cost once per batch, and N documents' Merkle paths share their upper levels, so the
trie work per write falls too.

Approved by the user ("Build W8") and built as designed: with `CoalesceWrites` on
(`--coalesce-writes`, `KDB_COALESCE_WRITES`, `coalesceWrites` in the config file,
`docdb.Options.CoalesceWrites`), the writer that reaches a namespace's write gate commits the
last-write-wins single-document writes queued behind it together with its own
(`go/kdb/server/write_batch.go`):

- A batchable write (`Upsert`, `Replace`, `Delete`; not under projection write-back) passes the
  same checks before the gate as any write - admission, authorization, its memory grant - then
  joins a queue and waits for the gate. If another writer takes it into a batch first, it stops
  waiting for the gate (`writeGate.acquireUnlessServed`) and waits for that batch's commit
  instead; if it gets the gate itself, it leads a batch.
- The leader takes itself, then the queued writes in arrival order, up to 64 writes / 16 KiB of
  bodies, stopping at the first write to a document already in the batch. It works out each
  write's operation against the head it lands on (an Upsert's premerge if still current, else
  the merge at the gate, exactly as alone) and commits them as one transaction through the same
  path every write takes (`commitAtGateFront`: indexes, engine, log, sync, notifications).
- Every writer in the batch is answered with the batch's commit once it is durable. A batch the
  engine rejects before committing (one write's schema or index violation) is retried write by
  write, so each gets the answer it would have had alone; an error after the commit (a failed
  sync) is every member's, as it would be under group commit anyway.

*Cassandra:* group commit - the commit log's group/batch sync already lets concurrent mutations
share one sync, and KDB copied that; W8 takes it from one shared sync to one shared update.

**The open questions, answered:**

1. *Does anything rely on one commit per client write?* No. Checked each consumer:
   - the wire `Upsert` reply carries only the commit hash, and a batch's writers all get that
     commit's hash, which names a state holding their write - as a multi-op transaction's does;
   - transaction ids: `Upsert`, `Replace` and `Delete` mint theirs on the server and return
     none, and the id index (`txIndex`, what a retry looks a write up by) is consulted only for
     `Commit`/`Replay`, which are never batched;
   - stream subscribers (Mode 1/2) are woken per commit and caught up by walking the DAG; the
     control server's SSE feed publishes per commit with its operation count;
   - peer sync ships commits as they are - multi-op commits are what transactions already make -
     and resolves conflicts per document under either policy; the home/fence check is per
     authoring node, and every batch is authored by this node;
   - the Kotlin replica uses commit hashes only to fetch blobs at a synced commit;
   - per-document history: unchanged, because a batch never holds two writes to one document -
     every version still has its own commit and tree. Commit-level history (and
     `kdb_namespace_commits`) shows fewer, larger commits.

   Nothing here changes the format of a commit or what one means (operations applied atomically
   onto one parent), so nothing peers or clients see changes meaning. The one behavioural change
   is coupling inside a batch: its writes become visible together and durable together.
2. *May a batch mix users?* Yes. A commit records no principal - its author is the node, its
   message the node's home stamp - and authorization is decided for each write, against its own
   principal, before it can join a batch. There is no per-commit audit record for a mixed batch
   to blur. Should a commit ever carry a principal, batches would have to be formed per
   principal - one more stopping rule in `writeBatcher.lead`.
3. *Batch cap?* 64 writes or 16 KiB of bodies, whichever comes first; a write larger than that
   commits alone. The first cut was 1 MiB, and it was measured to cost reads: a version is read
   back through its commit's frame, a cold read decompresses the whole frame, and batches of ~31
   1 KB documents made ~31 KB frames - zstd was 18% of kdb-service's CPU in point reads right
   after a batched load, and reads ran 13-20% below base. 16 KiB is Cassandra's default
   compressed chunk (`chunk_length_in_kb`), the bound on what one of its reads decompresses, and
   the bound `engine/large_frame_bodies.go` already holds KDB's reads to. With ~1 KB documents
   that is a batch of ~15, which keeps most of the gain: the fixed cost a batch shares is ~25 us,
   so per-write gate time is ~15 + 25/15 us at 15 writes against ~15 + 25/31 us at 31. The count
   cap only binds for documents under ~250 bytes.

**Measured** - `--coalesce-writes` against the top of the stack without it (#352/#353), 100k
documents, two passes in alternating order, every pass checked against the host's load (a pass
whose 1-minute load passed 13 was rerun; this 16-core host idled at 6-8 that night). ops/s, mean
(pass 1 / pass 2); write p99.9 / max ms per pass.

Fast durability:

| cell | c | base | w8 | w8 / base | write p99.9 / max ms (base, w8) |
|---|--:|--:|--:|--:|---|
| load | 32 | 23,951 (24,353 / 23,548) | 52,588 (51,337 / 53,840) | 2.20x | 9.0/62, 11.6/52 ; 8.6/16, 8.4/16 |
| read | 1 | 13,995 (14,383 / 13,607) | 11,980 (12,399 / 11,562) | 0.86x |  |
| read | 8 | 93,842 (97,672 / 90,012) | 85,197 (80,394 / 89,999) | 0.91x |  |
| read | 32 | 118,674 (124,589 / 112,760) | 101,754 (94,445 / 109,064) | 0.86x |  |
| update | 1 | 7,757 (7,607 / 7,907) | 7,805 (7,716 / 7,894) | 1.01x | 0.5/13, 0.4/12 ; 0.6/12, 0.4/11 |
| update | 8 | 21,681 (22,209 / 21,154) | 28,652 (28,292 / 29,012) | 1.32x | 4.1/12, 4.4/20 ; 4.1/11, 4.5/11 |
| update | 32 | 20,197 (20,275 / 20,120) | 36,562 (36,094 / 37,030) | 1.81x | 8.1/17, 8.3/15 ; 6.9/13, 6.9/13 |
| mixed50 | 1 | 9,621 (9,407 / 9,836) | 10,213 (9,540 / 10,886) | 1.06x | 0.7/6, 0.6/41 ; 1.5/8, 0.5/4 |
| mixed50 | 8 | 40,040 (40,910 / 39,170) | 48,025 (46,758 / 49,293) | 1.20x | 5.2/7, 5.1/7 ; 4.0/6, 4.6/7 |
| mixed50 | 32 | 40,603 (41,345 / 39,862) | 64,825 (63,947 / 65,703) | 1.60x | 8.9/55, 10.4/36 ; 7.2/13, 6.2/13 |
| insert | 1 | 8,553 (8,798 / 8,307) | 8,727 (8,565 / 8,889) | 1.02x | 0.8/7, 0.5/6 ; 0.5/3, 0.8/4 |
| insert | 8 | 22,636 (23,701 / 21,571) | 28,579 (28,778 / 28,379) | 1.26x | 3.4/14, 4.1/16 ; 6.5/13, 5.5/11 |
| insert | 32 | 20,605 (20,674 / 20,536) | 41,816 (41,851 / 41,781) | 2.03x | 16.5/24, 17.2/23 ; 11.3/17, 10.9/16 |

Durable (`--durability sync`, kdb-service defaults; base pass 1, W8 pass 1, W8 pass 2, base pass 2,
each under the shared bench lock - other sessions' passes ran between them):

| cell | c | base | w8 | w8 / base | write p99.9 / max ms (base, w8) |
|---|--:|--:|--:|--:|---|
| load | 32 | 24,857 (23,270 / 26,445) | 26,876 (25,418 / 28,334) | 1.08x | 8.3/75, 8.9/74 ; 12.7/92, 8.1/91 |
| read | 1 | 12,641 (12,304 / 12,978) | 13,635 (14,894 / 12,377) | 1.08x |  |
| read | 8 | 96,030 (94,656 / 97,403) | 89,576 (93,430 / 85,722) | 0.93x |  |
| read | 32 | 115,643 (112,300 / 118,987) | 107,900 (111,091 / 104,710) | 0.93x |  |
| update | 1 | 4,392 (4,188 / 4,596) | 3,563 (2,826 / 4,300) | 0.81x | 1.4/10, 0.6/8 ; 6.3/14, 1.2/17 |
| update | 8 | 11,773 (10,403 / 13,143) | 12,176 (12,725 / 11,627) | 1.03x | 4.6/12, 4.5/34 ; 4.5/14, 4.8/17 |
| update | 32 | 22,751 (21,254 / 24,248) | 19,326 (23,255 / 15,397) | 0.85x | 8.3/18, 6.9/16 ; 10.4/43, 14.9/77 |
| mixed50 | 1 | 6,223 (5,836 / 6,610) | 6,173 (5,285 / 7,061) | 0.99x | 1.4/13, 2.8/27 ; 1.2/15, 1.2/14 |
| mixed50 | 8 | 20,646 (18,943 / 22,349) | 19,849 (18,560 / 21,138) | 0.96x | 4.7/7, 5.5/11 ; 5.8/8, 6.5/11 |
| mixed50 | 32 | 39,619 (36,453 / 42,784) | 47,573 (46,163 / 48,983) | 1.20x | 9.3/23, 7.9/38 ; 8.0/28, 13.8/28 |
| insert | 1 | 3,800 (3,699 / 3,901) | 2,579 (3,935 / 1,224) | 0.68x | 1.0/4, 0.9/3 ; 2.1/31, 7.3/12 |
| insert | 8 | 11,239 (11,306 / 11,171) | 9,438 (13,635 / 5,241) | 0.84x | 5.6/16, 5.7/13 ; 6.4/12, 13.8/18 |
| insert | 32 | 21,742 (21,250 / 22,234) | 27,099 (26,335 / 27,862) | 1.25x | 16.1/21, 13.0/18 ; 13.7/17, 13.4/16 |

The durable numbers are noisy in a way the CPU-load check cannot see - W8's second pass has cells
a third of its first (insert at 1 worker 1,224 against 3,935), the shape of another session's disk
work stalling syncs, and base's own passes differ by 10-25%. Read pass by pass, the first pair
(nearest in time, both clean-looking) has W8 at +21-22% for updates and inserts at 8 workers and
+9-27% at 32, single-worker writes within noise. That W8 helps less under durable writes than
under fast ones is expected: a durable writer spends most of its time waiting for its sync, not at
the gate, so fewer writers are queued when a batch forms - group commit already shares the sync,
and W8 only adds the shared update.

- **Concurrent writes roughly double under fast durability** at 32 workers (update 1.81x, insert
  2.03x, the load phase 2.20x; 50/50 mixes 1.60x) and gain 20-32% at 8; under durable writes the
  gain is smaller (+9-27% at 32, +21% at 8 in the cleaner pair) because writers wait on the sync
  rather than queue at the gate. Fast tails shrink with them (worst write at 32
  workers 13-17 ms against 15-55 ms). One writer is unchanged - it forms batches of one.
- **Reads pay for it when the cache is short.** Point reads right after a batched load ran 9-14%
  below base. This store's live documents (~110 MB with overhead) sit right at the 115 MiB
  document budget, so some reads are cold, and a cold read decompresses its version's whole commit
  frame - up to 16 KiB of bodies for a batch against ~1 KB for a lone write. With the first cut's
  1 MiB cap (frames of ~31 KB) reads ran 13-20% below base and zstd was 18% of server CPU in the
  read cell, which is why the cap is 16 KiB. A store whose live set fits the cache does not pay
  this; one that does not, and reads more than it writes, may want W8 off - which is the default.
  Filing batched versions by value (a second copy, as large commits already do) would remove the
  read cost at the price of the copy; not done.

With the 1 MiB first cut, for reference (same method, fast; the durable half was not run):

| cell | c | base | w8 | w8 / base | write p99.9 / max ms (base, w8) |
|---|--:|--:|--:|--:|---|
| load | 32 | 23,701 (24,603 / 22,799) | 51,559 (50,872 / 52,246) | 2.18x | 10.8/74, 9.0/66 ; 10.0/17, 8.2/13 |
| read | 1 | 12,722 (13,171 / 12,273) | 11,130 (10,868 / 11,391) | 0.87x |  |
| read | 8 | 94,572 (87,949 / 101,195) | 78,552 (79,854 / 77,250) | 0.83x |  |
| read | 32 | 116,485 (112,579 / 120,392) | 93,046 (93,627 / 92,465) | 0.80x |  |
| update | 1 | 7,221 (6,931 / 7,510) | 7,286 (7,232 / 7,340) | 1.01x | 1.7/13, 0.7/16 ; 0.5/11, 0.5/10 |
| update | 8 | 21,651 (21,938 / 21,365) | 28,935 (29,046 / 28,824) | 1.34x | 3.9/13, 4.2/14 ; 4.2/16, 4.2/13 |
| update | 32 | 19,234 (18,981 / 19,487) | 37,155 (37,548 / 36,762) | 1.93x | 12.4/20, 8.1/25 ; 6.9/16, 7.2/13 |
| mixed50 | 1 | 9,889 (10,639 / 9,139) | 9,119 (9,108 / 9,131) | 0.92x | 1.2/7, 0.5/19 ; 0.5/3, 0.8/4 |
| mixed50 | 8 | 39,071 (37,633 / 40,508) | 47,747 (48,198 / 47,296) | 1.22x | 5.0/7, 5.3/7 ; 4.8/7, 3.8/5 |
| mixed50 | 32 | 40,412 (40,427 / 40,397) | 65,148 (66,737 / 63,560) | 1.61x | 9.2/28, 8.4/24 ; 6.9/19, 6.4/21 |
| insert | 1 | 8,748 (9,268 / 8,228) | 8,192 (8,178 / 8,206) | 0.94x | 0.8/6, 0.6/6 ; 0.7/3, 0.4/4 |
| insert | 8 | 21,160 (20,929 / 21,391) | 29,219 (29,742 / 28,696) | 1.38x | 7.1/17, 3.9/13 ; 6.1/40, 6.0/12 |
| insert | 32 | 21,078 (21,453 / 20,703) | 41,278 (44,557 / 37,998) | 1.96x | 16.1/20, 15.8/20 ; 10.9/15, 11.2/19 |


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
| audit / authorship | the commit's author is the node, its message the node's home stamp; no principal is recorded | unchanged | batches may mix principals (answer 2 above) |

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
  rate (~20-24k/s) is the limit, and nothing in this matrix's binary changes how many commits a
  namespace can make. W8 (built after this matrix, off by default) puts several writes in each:
  1.8-2.0x at 32 workers, which would take updates to ~0.75x and inserts to ~0.72x MongoDB
  (W8's own A/B, not this matrix).
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
