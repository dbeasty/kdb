# Moving KDB data out of the Go heap: an mmap plan

Status: in progress (branch `perf/mmap-offheap`). Updated 2026-10-11 with Phase 0 results and what has landed -
see "Results so far" at the end. The plan below is the original proposal; where the results contradict it, the
results win.

## Why

The Go GC's cost is proportional to the **live, pointer-bearing heap**, not to total data size. Data in an
`mmap`'d region is invisible to the GC, is paged by the OS, and can exceed RAM. The goal is to move the
structures that scale with *data or history size* out of the heap, without touching the hot path.

This is not a proposal to mmap the delta log wholesale. See "What mmap does not buy" below.

## Findings from the code (read before scoping)

1. **The I/O seam is positional reads, not streams.** `SegmentByteStore.Read(segment, offset, length) ([]byte, error)`
   (`go/kdb/storage/io/segment_store.go`). Every frame and SSTable block is read whole, with its length taken from an
   on-disk header. There is no mmap anywhere in the Go tree today.
2. **The commit graph is already planned.** `docs/kdb-commit-graph-on-disk.md` specifies a mapped graph file
   (519 B/commit resident today), including the refcounted-mapping lifetime rule and the optional-interface /
   build-tag platform pattern. `graphFile struct{}` is currently a stub; Phase 0 (measurement) is unrun and Phases
   2-5 are not started. **This plan reuses that work rather than duplicating it.**
3. **The live tree is still pointer-heavy.** `trieNode` holds `*trieLeaf` and `*[16]*trieNode`
   (`go/kdb/document/document_tree_trie.go`). Path compression has since landed, so the 4,900 B/doc figure in the
   memory note is **stale** and must be re-measured before any sizing decision.
4. **SSTable footer indexes are heap maps.** `tableIndexCache` holds `map[codec.Hash]BlockHandle` (~128 B/entry
   estimated), LRU-bounded at 16 MB by default (`go/kdb/storage/sstable/index_cache.go`). Immutable once written,
   so a good mmap candidate.
5. **Encryption at rest is not an obstacle.** Frame encryption lives in the codecs, not the byte store
   (`docs/kdb-spec-layer14-encryption-at-rest.md` §2.1). A mapping exposes ciphertext; decryption copies into a heap
   buffer exactly as `Read` does today. mmap works, but zero-copy does not.
6. **Platforms.** `PlatformIOShim` has in-memory, S3-replicated and browser implementations, and Linux/macOS differ
   on sync behaviour. mmap must be optional, behind build tags (`*_unix.go` / `*_other.go`).

## What mmap does not buy

- **Delta-log segment reads.** `pread` already goes through the OS page cache, and frames are compressed and
  CRC-checked, so they must be decoded into a heap buffer anyway. mmap saves a syscall, not GC pressure. Do not do
  this first; do it only if a profile shows syscall overhead.
- **Writes.** Durability and ordering stay on `write` + `fsync`. A mapped write path gives up control of when pages
  reach disk. Read-only mappings only.
- **The hot path.** Point reads must keep resolving `head` + `latestTree` outside any mapped structure
  (constraint #1 of the commit-graph doc). If `docs/benchmarks/workload-matrix.md` moves, the change is wrong.

## Difficulty by target

| # | Target | Difficulty | Win | Notes |
|---|---|---|---|---|
| A | Mapping primitive + lifetime rules | Medium | enabler | Everything depends on it; the sharpest edges live here |
| B | Commit graph file (existing plan) | Medium-High | 519 B/commit → ~0 | New on-disk format, rebuild-while-reading hazard |
| C | SSTable footer indexes | Low-Medium | up to 16 MB cache → page cache | Immutable, sorted keys, binary search over the mapping |
| D | Live document tree | **High** | largest, uncertain | Needs a pointer-free node layout first; mmap is not the first step |
| E | Cold document bodies | Medium | depends on measurement | Only if profiling shows `docsByHash` pinned bodies dominate |

Overall: **A+B+C is a few weeks of focused work; D is a redesign** and should be gated on measurement.

## Phase 0 - measure before building (1-2 days, gates everything)

Run on an otherwise idle machine (`uptime` first, no concurrent benchmarks; see the benchmarking notes).

- Re-measure live-tree bytes/doc at 10k / 50k / 200k docs on current `main`.
- Capture a GC profile (`GODEBUG=gctrace=1`, `go tool pprof` heap + CPU) of `BenchmarkWorkload*` at 200k docs. Record
  the **GC CPU share and p99 pause**. If the GC is not a bottleneck, the whole plan shrinks to B.
- Measure the commit-graph numbers the existing doc's Phase 0 asks for (100k / 500k / 2M commits).
- Check whether `GOGC` is tuned alongside the existing `GOMEMLIMIT` (`go/kdb/server/memory_budget.go`). Raising
  `GOGC` is a one-line experiment and may remove the need for part of this.

**Exit criterion:** a table of heap bytes by structure, ranked. Targets below are done in that rank order.

## Phase 1 - the mapping primitive (A)

New package `go/kdb/storage/mmapfile` (name to match the surrounding convention).

- `mmapfile_unix.go` (`//go:build unix`): `unix.Mmap(PROT_READ, MAP_SHARED)`, `Madvise` hints, `Munmap`.
- `mmapfile_other.go`: returns `ErrUnsupported`; callers fall back to the heap path.
- A `Mapping` type with **reference counting**: `Acquire() (*Region, error)` / `Region.Release()`. `Munmap` runs
  only when the last reference drops, never on the rebuilding goroutine.
- Never hand out a bare `[]byte` that outlives its `Region`. Reads copy out, or the caller holds the `Region`.
- `debug.SetPanicOnFault(true)` around mapped reads, converting SIGBUS (truncated file, I/O error) into an error that
  triggers fallback to the log.
- Optional interface on the store, mirroring `segmentStoreSizer`: `MapSnapshot(key) (*Mapping, error)`. A store that
  lacks it keeps today's behaviour.

**Tests:** concurrent readers vs. a rebuild-and-swap loop under `-race`; truncate-the-file-under-a-reader (must
error, not crash); a fallback test with the unsupported build tag; run on both macOS and Linux CI.

**Risk:** an intermittent crash that looks unrelated. The commit-graph doc already flags this as the sharpest edge.

## Phase 2 - commit graph (B)

Follow `docs/kdb-commit-graph-on-disk.md` Phases 2-5 unchanged; this plan only supplies Phase 1 above as its
dependency. Fixed decisions in that doc (generation number inside `commitNode`, graph is a cache never an authority,
`txIndex` relocated not bounded, positions in log order) are not reopened here.

## Phase 3 - SSTable index via mapping (C)

- Change the SSTable footer index to a **sorted, fixed-width array** (32-byte hash + 32-byte `BlockHandle`) so lookup
  is a binary search over the mapped bytes with no per-entry allocation.
- Keep the existing format readable: gate on a footer version field, and keep the heap-map path for old tables and
  unsupported platforms. `tableIndexCache` becomes a cache of `Mapping`s (bounded by count) rather than of parsed
  maps.
- Cross-port check: the Kotlin implementation reads the same files. Format changes must be additive or versioned, and
  covered by `docs/kdb-physical-layer-compat-test-plan.md`.
- Compaction must hold a `Region` reference on input tables until the swap completes.

## Phase 4 - live tree: pointer-free layout first (D), gated on Phase 0

Do **not** reach for mmap here first. The sequence is:

1. **Pointer-free slab (heap, no mmap).** Replace `*trieNode` with a `[]nodeRec` slab addressed by `uint32` index.
   A slab with no pointer fields is not scanned by the GC. The tree hash is unchanged, so the wire and Kotlin formats
   are untouched (as the path-compression change already established). This is a contained refactor inside
   `document_tree_trie.go`, but `trieWith` / `trieWithout` are persistent (structure-sharing), so the slab needs an
   append-only / copy-on-write discipline and a compaction story for dead nodes.
2. Re-measure. If the GC cost is gone, **stop.**
3. Only if still needed: back cold slab pages by an anonymous or file mmap. This adds manual lifetime management for
   a mutable structure, which is where the real bugs live. Treat as a separate proposal.

## Phase 5 - cold document bodies (E), optional

Only if Phase 0 shows `docsByHash` pinned bodies dominate heap. Candidate: an append-only body file with an mmap'd
offset index, reading bodies on demand through the existing `deltaColdLoader` seam. Note the existing constraint:
`docsByHash` pins everything the live tree names, so eviction must stay behind that pin.

## Cross-cutting rules

- Every mapped file is **derived state**: absent, corrupt, wrong version, or unsupported platform falls back to the
  log or the heap path. Nothing is reachable *only* through a mapping.
- Everything ships **flag-off by default** (the `dag.GraphSettings` pattern), then flips after benchmarks.
- S3 replication is unchanged: mapped files are local, not replicated.
- Benchmarks follow the isolation rules: check `uptime`, no concurrent heavy jobs, and run a no-protocol control
  before attributing any macOS delta to the code.
- This checkout is shared with other sessions: use a branch and `git pull --ff-only`, never `update-ref` on a
  checked-out branch.

## Suggested order and gates

1. Phase 0 measurement. **Gate:** is the GC (not RSS) actually a problem?
2. Phase 1 primitive + tests on both platforms.
3. Phase 3 (SSTable index): smallest consumer, proves the primitive cheaply.
4. Phase 2 (commit graph): largest certain win, largest format work.
5. Phase 4 step 1 (pointer-free tree): only if Phase 0 says the live tree dominates GC cost.
6. Phases 4 step 3 and 5: only on evidence.

## Decisions (answered 2026-10-11)

- **Linux and macOS are both release targets** (corrected 2026-10-11; first answered as Linux-only). Both are `unix`
  for mapping, so one implementation serves both and only Windows/wasm fall back (`ErrUnsupported`). Where they differ:
  - *Budget source:* Linux budgets come from the cgroup; macOS has none, so the budget comes from system memory
    (`go/kdb/storage/system_memory_darwin.go`). `MappedBytes()` must be charged against whichever budget is in force.
  - *Durability:* the graph file is written then renamed, so its writer must use the same `fsync` path as the rest of
    the store (`F_FULLFSYNC` on macOS via `storage/io/sync_darwin.go`), not a bare `File.Sync`.
  - *Benchmarks:* macOS numbers are not comparable to Linux ones, and extra flushing halves neighbours there, so any
    graph-file benchmark needs a no-protocol control and a Linux run before it is believed.
  - *Tests:* the SIGBUS-on-truncate test skips where the platform keeps pages resident; CI must run the mmapfile
    tests on both OSes.
- **Design point: ~1M documents / commits is the sweet spot; more than that must still work.** The pointer-free node
  store is the 1M answer; the mapped graph file is the answer beyond it (2M commits is ~600 MB resident at 298 B/commit).
- **Mapped regions count against the memory budget.** `mmapfile.MappedBytes()` reports whole-file bytes of live
  mappings (the safe over-estimate). The budget zones in `go/kdb/server/memory_budget.go` must add it to the process
  total they compare against the cgroup budget; `GOMEMLIMIT` does not see it. Wiring waits for the first consumer.

## Results so far (2026-10-11)

### Phase 0 measurements (M3 Max, Go 1.26.3, load average 3-7 from other sessions; heap sizes are unaffected by load)

| Structure | Result | Consequence |
|---|---|---|
| Live document tree | **~160 B/doc** (10k: 164, 50k: 156, 200k: 162) = 32 MB at 200k docs | The old 4,900 B/doc figure is obsolete - path compression landed. **Phase 4 (live tree) dropped.** |
| Commit graph, whole-namespace open heap | 523 B/commit at 20k, 438 at 100k | Linear, as the commit-graph doc predicted |
| Commit graph, DAG nodes alone | 325 B/commit, **~5.1 ms** full-GC mark at 300k commits | The structure the collector was actually scanning |

### Finding: the GC cost was pointers, not bytes

`map[Hash]commitNode` held a `document.Commit` per entry (slices, strings, a `*Hash`), so the collector traced
every commit every cycle. Replacing it with a pointer-free index + slab made the same 300k commits cost
**~0.24 ms** to mark (about 20x less) and 298 B/commit (down from 325). That is the result mmap was meant to buy,
without a file format, a mapping lifetime, or any platform fallback.

### Finding: the SSTable index is not worth mmapping for GC

The footer is a text format shared byte-for-byte with the Kotlin port and the golden fixtures, so a mapped sorted
index would have to be a sidecar file with a new lifecycle. And `map[codec.Hash]BlockHandle` has a pointer-free key
and value, so the collector does not scan it either. The cache is already byte-bounded at 16 MB. **Phase 3 dropped**
unless the memory budget (not the GC) becomes the reason.

### Landed on `perf/mmap-offheap`

1. **`go/kdb/storage/mmapfile`** - refcounted read-only mapping; `CopyAt`/`Guard` turn SIGBUS into an error;
   `MappedBytes()` for the budget. Tested under `-race`, cross-compiled for windows and js/wasm.
2. **Pointer-free resident tier** (`go/kdb/dag/node_store.go`) - replaced `map[Hash]commitNode`.
3. **The on-disk commit graph** (`go/kdb/dag/graph_file.go`, `graph_attach.go`, `graph_record.go`), behind
   `graph.file` / `KDB_GRAPH_FILE`, off by default.
4. **Budget wiring** - `server/memory_guard.go` adds `MappedBytes()` on the runtime/metrics path (macOS, Linux
   without a cgroup). The cgroup reading already charges page cache, so it is not added there.

## The altered plan (2026-10-12): one format, an LSM on disk

Two changes requested after the first graph file landed, both now built:

### One record format for memory and disk

The resident tier holds each commit as the **same 256-byte record** a graph segment holds
(`graph_record.go`), in 1 MB heap chunks of raw bytes. Consequences:

- writing a commit to disk is a byte copy; there is one encoder and one decoder for both tiers;
- the chunks are `[N]byte`, so the collector still never scans them (`TestResidentChunksArePointerFree`);
- the record carries **14 reserved bytes** (offset 242) so a later field does not need a new layout, and two
  memory-only fields in a region the writer zeroes (the namespace index; blob offsets are file-only).

### Segments, flushes and full rewrites (Cassandra-style)

The graph on disk is `ns/<namespace>/graph/graph-<seq>.kdbg`, a set of sorted segments:

| Operation | Writes | When |
|---|---|---|
| **Flush** (memtable -> SSTable) | only the resident tier, as a new partial segment | resident >= `graph.rebuildCommits` (default 50,000) and no rewrite trigger fires |
| **Full rewrite** (major compaction) | every segment + the resident tier, as one segment flagged *full* | first write; segments >= `graph.maxSegments` (default 8); hidden records > 20% (tombstone compaction); resident >= half of what is on disk; or `EmbeddedKdbRuntime.CompactCommitGraph()` |

Invariants: segments never share a commit (a flush skips any commit a segment has a record for, even a hidden one);
a full segment supersedes every lower sequence number, so a crash between its rename and the deletes is repaired at
the next open; any unusable segment discards the whole set (commits stay resident, next write is a full rewrite);
read-only runtimes map but never write or delete.

### Known limits

- **Open peak and checkpoint writes - fixed 2026-10-12.** Under `history=full` with segments attached, the checkpoint
  (format v3) names the segments instead of carrying their commits, so neither open nor a checkpoint write scales with
  history. `history=none` keeps carrying commits (its log is truncated, so it cannot fall back to a replay).
- **History reads are slower** from segments than from memory (binary search per step, per segment). Accepted by
  design constraint 3 of the commit-graph doc; measured below.
- **Not done:** the position-indexed cursor and anchors (commit-graph doc Phases 4-5) - latency/replay features,
  not memory ones.

### Earlier "still open" list (superseded by the sections above)

- Phase 0 at 1M and 2M commits to size the graph file's win (the existing 100k run took 11 minutes; use a direct-DAG
  loader, not the embedded runtime, for the large sizes).
- Wire `mmapfile.MappedBytes()` into the budget zones when the graph file lands.
- The mapped graph file (the commit-graph doc's Phases 2-5) now buys *residency* (298 B/commit -> ~0), not GC time.
  Re-decide it on the 2M-commit residency numbers, not on GC grounds.
- Operations and messages still live in pointer-bearing side maps (bounded by the operations budget).
- Re-run Phase 0 `GOGC` experiment alongside `GOMEMLIMIT`.

## Performance results (2026-10-12, M3 Max, Go 1.26.3, macOS)

Machine shared with other sessions (load average 7-13 throughout), so absolute timings are noisy; every comparison
below was taken alternately against a detached `main` worktree in the same window. Heap figures are not
load-sensitive.

### Commit graph at scale (`TestGraphFileScale`, linear history with a merge every 1,000 commits)

| | main | branch, all resident | branch, graph file attached |
|---|---:|---:|---:|
| Heap, 1M commits | 458 MB | 458 MB | **~0 MB** |
| Heap, 2M commits | 915 MB | 915 MB | **~0 MB** |
| Full-GC mark, 1M | 21 ms | 1.0-6.5 ms | **0.24 ms** |
| Full-GC mark, 2M | 50-65 ms | 2.3 ms | **0.30 ms** |
| `GetCommit` (random, 1M) | - | 0.56-0.80 us | 0.61 us |
| `IsAncestor` deep (1M) | - | 103-107 ms | 220 ms |
| `Walk` 10k commits (1M) | - | 4.2-4.8 ms | 7.6 ms |
| `GetCommitByTransactionID` (1M) | - | 0.10 us | 0.22 us |
| Segment on disk | - | - | 304 B/commit (304 MB at 1M) |
| Full rewrite (write + attach), 1M / 2M | - | - | 2.5 s / 3.8 s |
| Attach at open (map + checksum), 1M / 2M | - | - | 167 ms / 306 ms |

Reading the table: the pointer-free resident tier removes the GC cost (scan time) but not the bytes - with the
shared 256-byte record it costs the same 458 B/commit as `main`'s map (the struct slot it replaced was 425). The
graph file removes the bytes. History reads from the file are up to ~2x slower than from memory (one interpolated
search per step instead of a map probe; it was 3.5x before interpolation search).

### Hot path (`go/kdb/server` workload matrix, graph file off - the default)

Alternating `main` / branch, `-benchtime 2s`. No difference is statistically significant (benchstat, p > 0.05):

| Benchmark | main | branch | |
|---|---:|---:|---|
| Read single-user overlapping | 318 ns | 312 ns | ~ (n=4) |
| Read heavy-multi-user overlapping | 153 ns | 184 ns | ~ (p=0.89, n=4) |
| Read single-user non-overlapping | 212 ns | 241 ns | ~ (p=0.49, n=4) |
| Read heavy-multi-user non-overlapping | 161 ns | 202 ns | ~ (p=0.49, n=4) |
| Insert single-user (fsync) | 5.52 ms | 4.77 ms | ~ (p=0.24, n=6) |
| Insert, memory-only, single-user | 20.2 us | 20.4 us | ~ (p=0.94, n=6) |
| Insert, memory-only, heavy-multi-user | 24.8 us | 25.8 us | ~ (p=0.13, n=6) |
| Update single-user overlapping | 5.17 ms | 4.94 ms | ~ (p=0.39, n=6) |

Allocations per read are identical (4 allocs, 176 B). A first n=4 run showed single-user insert +25% (p=0.029)
during a load-average spike to 17-20; it did not reproduce at n=6 with the order alternated, and the memory-only
insert - which runs exactly the code this branch changed, without fsync - is flat. Treat that first number as the
neighbour-contention artifact the benchmarking notes warn about, not a regression. A quiet-machine rerun on Linux
would still be worth doing before calling the read rows flat to within a few percent.

### Whole-namespace memory across a normal life (`TestMemoryProfile`)

100,000 documents, 500,000 commits (each document inserted, then random updates), async durability, default memory
budgets, maintenance every 50,000 commits during the build. One process per phase. "Live" is heap after a GC;
"peak" is the largest live-object heap sampled during the phase; RSS is the process's resident set.

| Phase | main | branch, graph off | branch, graph on |
|---|---:|---:|---:|
| Startup: live heap after open | 297 MB | 297 MB | **27 MB** |
| Startup: peak heap during open | 1,031 MB | 1,025 MB | **108 MB** |
| Startup: RSS after open | 1,145 MB | 1,308 MB | **326 MB** |
| Startup: open time | 3.8 s | 3.5 s | 2.7 s |
| 200k point reads: live heap after | 373 MB | 373 MB | **103 MB** |
| 200k point reads: peak heap | 582 MB | 592 MB | **230 MB** |
| 20k updates: live heap after | 317 MB | 319 MB | **52 MB** |
| 20k updates: peak heap | 542 MB | 582 MB | **99 MB** |
| Close + checkpoint: allocated | 1,770 MB | 1,803 MB | **695 MB** |
| Close + checkpoint: peak heap | 1,454 MB | 1,526 MB | **177 MB** |
| Close + checkpoint: peak RSS | 1,688 MB | 1,744 MB | **415 MB** |
| Disk (whole data dir) | 335 MB | 351 MB | 419 MB |

Graph off, the branch matches `main` (as it should: the feature is off and the pointer-free store holds the same
bytes). Graph on, startup needs ~10x less memory and the checkpoint peak drops ~8x. The +84 MB on disk is the graph
segment (304 B/commit) less the commits the checkpoint no longer carries.

What is left, in order of size:
- **The checkpoint still allocates 695 MB** to write ~a few hundred KB of resident commits. The remainder is the live
  tree (100k entries) and the rest of the checkpoint built as generic `codec.Value` maps and encoded in one buffer -
  allocation churn, not retained memory. A streaming encoder for the live-tree section is the next target.
- **Reads grow the heap to ~100 MB** - the document cache filling, which is budgeted (`DocumentCacheBytes`), not a leak.
- Machine load was high (load average up to 24 from other sessions), so the timings are indicative only; the heap
  and RSS figures are not load-sensitive.

