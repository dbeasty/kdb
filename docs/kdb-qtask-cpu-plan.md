# qtask on KDB: closing the CPU-per-request gap

Written 2026-10-20 against `main` d8a915d (= v0.9.2 plus docs). Each item cites the Cassandra
mechanism it follows where it changes architecture, as `kdb-memory-cassandra-plan.md` does.

## Status (2026-10-21)

Implemented, each its own PR, each measured alone and in combination. qtask bench, mixed @ 8
users, two back-to-back cells; v0.9.2 does 69.5-71.2 actions/s (six cells):

| item | PR | alone | cumulative |
|---|---|---:|---:|
| P1 hardware CRC32 | #328 | 80.5 / 79.6 (+14%) | 80 |
| P0 cache pool sized from the machine (new, see below) | #329 | 109.3 / 104.2 (+50%) | 118.7 / 110.8 (with P1) |
| P4 SELECTs off the session queue | #330 | flat (CPU-bound) | 122.5 / 116.9 (+3-5%) |
| P3 client declines zstd on loopback | #331 | client-only | 141.5 / 136.3 (+16%) |
| P2 `--document-copies` (off by default, switchable on restart) | #332 | flat (69.7 / 72.0) | **178.5 / 172.2 (+26%)**, disk 954 → 1467 MiB; switch build: off 110.6 / 106.0 at 920 MiB, on 169.2 / 160.9 at 1464 MiB |

Final matrix, MongoDB `j:true` and KDB in one run, 2026-10-21 (`qtask/bench/results/2026-10-21-final2-all`,
KDB = #328-#332 with #331's client; `-final2-nop2` is the same without #332). actions/s, gap to MongoDB in brackets:

| profile | users | v0.9.2 (2026-10-20) | all PRs | without #332 | MongoDB |
|---|---:|---:|---:|---:|---:|
| mixed | 1 / 8 / 32 | 30 / 68 / 48 (4.8x / 6.9x / 10.6x) | 56 / 156 / 174 (2.9x / 2.9x / 2.9x) | 49 / 119 / 124 | 160 / 457 / 510 |
| read-heavy | 1 / 8 / 32 | 28 / 62 / 39 (5.8x / 6.1x / 10.4x) | 58 / 154 / 159 (2.9x / 2.5x / 2.6x) | 50 / 112 / 116 | 167 / 378 / 412 |
| write-heavy | 1 / 8 / 32 | 34 / 95 / 98 (4.7x / 6.5x / 8.3x) | 54 / 176 / 202 (3.0x / 3.6x / 4.2x) | 49 / 150 / 153 | 159 / 631 / 849 |
| claim | 1 / 8 / 32 | 107 / 298 / 319 (3.7x / 7.5x / 8.4x) | 110 / 277 / 276 (3.5x / 8.7x / 9.6x) | 111 / 282 / 277 | 385 / 2413 / 2661 |

No errors in any cell. On disk after each profile: MongoDB 1.23-1.63 GB, KDB with #332 0.99-1.74 GB,
without it 0.91-1.21 GB. Claim did not move: none of P0-P4 touches the conditional-update path
(`ExecImmediateUpdate`), which is now the largest relative gap.

The cache pool's first cut (a quarter of the machine) put write-heavy @ 32 at the container's 4 GiB
limit and the memory guard shed 61k writes: secondary-index history and trees held about as much
again as the pool. #329 now uses an eighth, Cassandra's caches-to-RAM ratio.

What implementing changed in the plan:

- **P0 was not in the first draft and is the largest single item.** kdb-service gave its host
  the embedded default cache pool, a fixed 64 MiB: in a 4 GiB container that is a 4 MiB frame
  cache and a 9 MiB block cache, against WiredTiger's ~1.5 GB. Raising the frame cache's
  *fraction* did nothing (4 → 15 MiB); the pool itself had to grow. Sized now as Cassandra sizes
  its heap (a quarter of the cgroup limit or RAM, 64 MiB-8 GiB); embedded runtimes keep 64 MiB.
- **P2 only pays with P0.** On the 64 MiB pool the re-filed bodies were served from SSTable
  blocks that missed a 9 MiB cache; throughput did not move. The first version's extra rule
  (frame must also hold 4x the version) kept qtask's 12 KiB tasks on their frames - an
  instrumented run counted 315k such versions in 35 s - and was dropped. It costs a second copy
  of the re-filed bodies (+54% on disk here), so it is a switch, `--document-copies`, off by
  default. Changing it on restart re-files existing versions in the background (22 s on, 19 s off
  for 517k documents) and, towards fewer copies, merges the store to give the disk back.
- **P4's gain is small while the server is CPU-bound**, as expected; it is there for when
  requests are cheaper than the CPU, and for a single app instance.
- **Claim (8.7-9.6x) is the largest remaining gap**, then P5; **P5 and P6 are not started.** After P0-P4 the profile is led by `topKDocs`/`materializeUpTo`
  reading candidate bodies (P5) and GC (~9%).

## Where we stand

qtask's Docker benchmark (`qtask/bench`, prod scale: 517,464 documents; each database alone in a
2 CPU / 4 GiB container; MongoDB `w:1, j:true` against KDB `--durability sync`; 10 s warmup +
60 s per cell), run 2026-10-20 on a quiet machine, both backends in the same run
(`qtask/bench/results/2026-10-20-baseline-v0.9.2`):

| profile | users | KDB actions/s | MongoDB actions/s | gap |
|---|---:|---:|---:|---:|
| mixed | 1 / 8 / 32 | 30.4 / 68.0 / 48.2 | 145 / 467 / 511 | 4.8x / 6.9x / 10.6x |
| read-heavy | 1 / 8 / 32 | 28.1 / 62.4 / 39.4 | 163 / 381 / 410 | 5.8x / 6.1x / 10.4x |
| write-heavy | 1 / 8 / 32 | 33.5 / 95.0 / 98.2 | 156 / 619 / 812 | 4.7x / 6.5x / 8.3x |
| claim | 1 / 8 / 32 | 107 / 298 / 319 | 391 / 2222 / 2687 | 3.7x / 7.5x / 8.4x |

At 8 users KDB runs its 2 CPUs flat out (190%) while MongoDB serves 7x the work at 92%. That is
CPU per request, roughly 10x MongoDB's. It is not the disk: the volume's synchronous 4 KiB write
costs 0.38 ms and KDB's synced commits are group-committed.

## What the CPU is spent on

A 45 s CPU profile of `kdb-service` inside the measured window of mixed@8 (85 s of samples, 190%):

| what | share | where |
|---|---:|---|
| **Decompressing a whole commit frame to return one document** | **52%** | `documentFromObject` → `documentFromLargeFrame` → `delta.ReadPayloadAt` |
| of which: zstd decode | 33% | `compression.DecompressInto` |
| of which: **byte-at-a-time CRC32 over the frame** | 16% | `compression.CRC32` ← `delta.verifyFrameCRC` |
| zstd compression of every reply frame | 11% | `wire.compressBody` |
| SSTable lookups + block decode | 9% | `LsmBlobStore.Lookup`, `sstable.decodeBlock` |
| conditional updates (claims) | 7% | `ExecImmediateUpdate` |
| GC | 4% | |
| SHA-256 (Merkle trie + content hashes) | 1.6% | `sha256.blockSHA2` |
| commit gate | 1.2% | `commitAtGateFront` |
| SQL parse + plan | < 0.5% | below the profile's cut |

The reads that land on frames are the ones that fetch every candidate's body: `topKDocs`
(ORDER BY … LIMIT, 32% inclusive) and `materializeUpTo` (38% inclusive).

So three of the four suspects named before profiling are not where the time goes: Merkle hashing
(1.6%), SQL parsing (< 0.5%) and the write path (~8% all told). The fourth, one namespace for
every collection, matters through candidate counts, not per-request overhead.

### Why the frames are decompressed: a threshold boundary

U4 of the qtask production plan already files the bodies of a *large* commit by value, so a later
read finds one document instead of decompressing the frame. "Large" means the commit's bodies
reach `DefaultLargeCommitBodyBytes = 64 KiB` (`storage/config.go:134`). qtask's migration splits
its import into commits of **at most** `KDB_IMPORT_COMMIT_BYTES = 64 KiB` of bodies
(`qtask/src/scripts/migrate-mongo-to-kdb.ts:60`). So every import commit lands just under the
threshold, is filed by location, and since `documentFromLargeFrame` only re-files frames that are
large, it is never re-filed either. Every read of a 400-byte activity decompresses and CRCs a
~64 KiB frame: about 150x read amplification, paid again on every read.

### Why throughput falls from 8 to 32 users

Two things, both measured:

- The CPU is already saturated at 8 users; more users only queue.
- A goroutine dump in the same window showed 2 requests in flight across 4 connections. The
  server orders every `SqlExec`/`TxCommit` by its session id (`server/wire_listen.go:296`
  `sessionTicket`), and the TS client sends the namespace's one session id on every statement,
  reads included (`packages/kdb-client/src/client.ts`, `#execSql`/`#commitTransaction`). So one app
  process runs one statement at a time against the server, however many requests it has.

## Plan

Ordered by measured cost. Each item is its own branch and PR off `main`, benchmarked alone against
the v0.9.2 baseline with mixed/read-heavy/write-heavy/claim at 8 users, then all together on the
full 1/8/32 matrix. Benchmarks follow `kdb-benchmark-isolation-required`: quiet machine, one at a
time.

### P1. Hardware CRC32 (16%)

`compression.CRC32` is a byte-at-a-time table loop. `hash/crc32.ChecksumIEEE` computes the same
IEEE CRC (same polynomial, init and final XOR) with the CPU's CRC instructions on arm64 and the
slicing/CLMUL paths on amd64. On-disk and wire formats are unchanged, so Kotlin is unaffected.
A local code fix; no Cassandra analogue needed (Cassandra uses `java.util.zip.CRC32`, which is
the hardware intrinsic for the same reason).

### P2. Read one document without inflating its commit (52%)

Cassandra mechanism: **`CompressionMetadata` chunk offsets** - SSTables are compressed in fixed
chunks (16 KiB by default since 4.0) so a read inflates one chunk, never the file. KDB's
equivalent decision is per version: a read through a frame costs the frame, so a version whose
frame is many times its own size should be filed by value.

- The filing rule becomes per version and relative: file by value when the rest of the frame is
  both more than 16 KiB (Cassandra's chunk size) and more than 4x the version itself; otherwise keep
  the location. This replaces the absolute 64 KiB-of-bodies commit threshold that qtask's import
  sits just under. One ordinary small commit (a few documents, a few KiB) still keeps its single
  copy in the log.
- `documentFromLargeFrame` applies the same rule when it meets a frame, so data written before
  the change (every existing qtask volume) is re-filed on its first read, as U4 does for large
  frames today.
- Disk cost: the re-filed bodies get a second copy (log + SSTable), bounded by the live set. This is
  the footprint-for-speed trade, made explicit in the PR with measured sizes before and after.

### P3. Cheaper reply compression (11%)

Cassandra mechanism: **native-protocol v5 frame compression with LZ4**, chosen because it costs
a fraction of what zstd does; compression is the client's choice per connection. KDB compresses
every binary reply with zstd whenever that comes out smaller.

- Do not compress replies under a minimum size (where the saving is a few hundred bytes).
- Replies already use zstd's fastest level (`wire/binary.go:59`, `frameZstdLevel = 1`), so the
  remaining lever is per connection, as in Cassandra: a client on loopback or a private network
  can decline compression in the handshake (it already negotiates `wireFeatures`). The protocol
  may change freely (`kdb-memory-cassandra-plan` decision 2).
- Measure bytes on the wire alongside CPU, since qtask's base64 embeddings compress poorly anyway.

### P4. Statements run concurrently on one connection

Cassandra mechanism: **native-protocol stream IDs** - requests on one connection run concurrently
and complete out of order; ordering is the client's job where it needs it.

- The TS (and Go/Kotlin) client sends a session id only on frames that depend on session state:
  an open transaction, a read pinned to a commit, locks. A plain autocommit `SELECT` and a
  self-contained `txCommit` (transaction bytes in the frame) go sessionless, so the server runs
  them in parallel.
- The server keeps ordering frames that name a session, as `sessionIDOf` documents.
- Check that qtask's `replaceIf` path needs nothing from the session (it carries
  `EXPECT_CONTENT_HASH` in the transaction itself).

### P5. ORDER BY … LIMIT without reading every candidate's body

Cassandra mechanism: **SAI (storage-attached indexes)** answer the ordered, limited query from the
index and read rows only for the top K. `topKDocs` today reads every candidate's whole body to
find its sort key, and the index-backed `btreeTopK` path covers only some shapes.

- Serve the sort key from a declared BTREE index when one exists on the ORDER BY column, and
  fetch bodies only for the rows that survive the limit and the residual filter.
- Measure after P2: with bodies filed by value each candidate read is much cheaper, so the size
  of this win is only known then.

### P6 (qtask side). One namespace per collection; embeddings out of hot rows

Cassandra mechanism: **a table per entity** (each with its own memtable, sharing one commit log), and
the data-modelling rule of keeping large blobs out of rows that are read and rewritten often.

- kdb-service has opened namespaces on demand for a while (`cmd/kdb-service/namespace_per_collection_test.go`);
  qtask's comment that it can only open one (`src/data/kdb/store.ts:21-28`) is out of date. One
  namespace per collection bounds every scan and index to its collection and gives each its own
  write gate.
- Tasks carry an 8 KiB packed embedding that every status change rewrites and every `SELECT _doc`
  returns. Keep it in a separate collection keyed by task id.
- This is a data migration in qtask, so it comes after the KDB items and has its own plan.

### Not doing (measured as small)

- Deferring Merkle hashing (Cassandra builds its Merkle trees only for repair): 1.6% of CPU. KDB's
  commit identity needs the root hash, so the change would be large for little gain.
- A prepared-statement cache (Cassandra's `QueryProcessor` cache): parse and plan are below 0.5%.
  Parsing twice per request is still worth folding into P4 if it is on the way, but not on its
  own.

## Acceptance

Each PR states its own before/after at 8 users. The set is done when the full matrix (1/8/32
users, four profiles) has been re-run against MongoDB in one run, and the gap at 8 users is
reported per profile, with the on-disk size before and after P2.
