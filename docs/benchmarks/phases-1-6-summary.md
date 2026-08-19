# Phases 1-6 summary — reengineering for 1M writes/sec

Companion to `phase0-baseline.md` (the starting-point measurements and
bottleneck list). This documents what actually got built for each phase,
the measured before/after, and what was deliberately cut from the
original phase plan once real numbers were in hand. All numbers below:
Apple M2 Pro, macOS, real disk-backed WAL, one run each - a reference
point, not a tuned benchmark suite. Re-run the commands in
`phase0-baseline.md` / this file to reproduce.

## What shipped, phase by phase

**Phase 1 - group-commit WAL.** Removed the engine-wide mutex from
`ServerEngine.WriteBlob` / `ServerStorageEngine.writeBlob` entirely;
WAL.Append and memTable.Put are each independently thread-safe, and a new
`GroupCommitter` (sequence-number based, race-tested) coalesces concurrent
fsync requests into as few physical syncs as possible. Also fixed two bugs
this surfaced: Go's `OSByteStore` was reopening the WAL file on every
single append (open+write+fstat+close), and both languages shared one
mutex between append and flush, which blocked new appends (and therefore
new group-commit waiters) for a flush's *entire* duration - silently
collapsing group commit back to one fsync per write. The second bug was
invisible in Go (fsync ~2µs there) and glaring in Kotlin (JVM's
`FileChannel.force()` is ~1000x more expensive on this platform, likely
real `F_FULLFSYNC` vs. Go's plain `fsync()`).

**Phase 2 - sharded document-storage lock.** Replaced the single
namespace-wide docs-map mutex with a 64-way shard keyed by document UUID,
in both `ServerEngine` (Go) and `ServerStorageEngine` (Kotlin).
`InMemoryStorageAdapter` (Go's test/lightweight target) and the commit
DAG's own mutex were intentionally left alone - see the Phase 2 commit
for why.

**Phase 3 - fixed O(n log n) sort-key recomputation.** The plan called for
a persistent map to avoid `CommitTree`'s O(n) map copy. Benchmarking
first showed the copy costs ~81µs at 2000 entries while
`BuildDocumentTree`'s own sort+encode+hash pass costs ~12.6ms - the
persistent map would have saved under 1% of the real cost, so it was
dropped. The actual cost was `entriesToArrayValue`/`DocumentTree.build`
calling `UUID.String()`/`toString()` inside the sort comparator (O(n log
n) allocating calls instead of O(n)). Fixed by precomputing each sort key
once; output and hash are unchanged. **12.6ms → 1.8ms (~7x)** at 2000
entries.

**Phase 4 - tunable per-namespace durability.** `Durability.SYNC` /
`ASYNC` / `MEMORY_ONLY`, wired into the write path with a real background
sync ticker for ASYNC and a final flush on shutdown. Tested against a fake
WAL asserting on sync-call counts rather than timing real disk I/O.

**Phase 5 - configurable hot-tier memory.** `HotTierMemoryConfig`: small
default (128MiB), configurable via an absolute byte ceiling or a
percentage of total system memory (Go: sysctl/procfs; Kotlin: JVM's
extended `OperatingSystemMXBean`). Falls back to the default wherever
detection isn't available (unsupported Go platform, JS/Native Kotlin
targets).

**Phase 6 - connection-level request pipelining.** The wire protocol
already carries a `correlationId` for matching out-of-order responses,
but the default server connection handler awaited each response before
reading the next frame - a client could never have more than one request
in flight per connection. `pipelinedPerConnection` now dispatches frames
concurrently; a new per-session mutex in `SqlWireHost` preserves
transaction ordering (a session's `pending` transaction builder is a
plain `var`, unsafe under concurrent access without it), and a send-mutex
keeps socket writes from interleaving.

## Before / after

### Go: `ServerEngine.WriteBlob` (real disk-backed WAL)

| Parallelism | Phase 0 baseline | After Phase 1 | Final (post Phase 2-6) |
|---:|---:|---:|---:|
| 1   | 35,827 ns/op (~28k/s) | 7,504 ns/op | 6,529 ns/op (~153k/s) |
| 8   | 33,423 ns/op (~30k/s) | 5,225 ns/op | 4,879 ns/op (~205k/s) |
| 64  | 33,123 ns/op (~30k/s) | 3,716 ns/op | 3,805 ns/op (~263k/s) |
| 256 | 33,875 ns/op (~30k/s, **flat**) | 4,242 ns/op | 4,101 ns/op (~244k/s, **scales**) |

The qualitative change matters as much as the magnitude: baseline
throughput was flat regardless of concurrency (one global lock); it now
scales with parallelism, which is the actual precondition for approaching
1M/sec by adding more concurrent writers/shards in a future pass.

### Kotlin: `ServerStorageEngine.writeBlob` (real disk-backed WAL)

| Parallelism | Before the shared-mutex fix | Final |
|---:|---:|---:|
| 1   | 233 ops/sec | 242 ops/sec |
| 8   | - | 635 ops/sec |
| 64  | - | 4,337 ops/sec |
| 256 | - | 7,049 ops/sec (~29x over parallel-1) |

Kotlin's absolute numbers are far below Go's because JVM's
`FileChannel.force()` on this platform costs ~4-9ms per physical fsync
(vs. Go's ~2µs) - not a regression introduced by this work, a pre-existing
platform cost that group commit now actually amortizes across many
writers instead of paying per write.

### Reads (added after the gap fixes above, requested separately)

Read paths were never a bottleneck in this work - no lock was ever taken
across them the way WriteBlob's used to be - but hadn't been measured.
`read_throughput_bench_test.go` / `ReadThroughputBenchTest.kt` mirror the
write benchmarks: `ReadBlob`/`readBlob` (memTable lookup, no disk I/O
once written) and `GetDocument`/`getDocument` (Phase 2's sharded doc
store lookup), 5000 entries pre-populated, concurrent reads over them.

**Go**, ns/op (higher parallelism only lightly loaded on a 12-core host,
these are near the noise floor):

| Parallelism | ReadBlob | GetDocument |
|---:|---:|---:|
| 1   | 222 ns/op (~4.5M/s) | 35 ns/op (~28.7M/s) |
| 8   | 173 ns/op (~5.8M/s) | 77 ns/op (~13.0M/s) |
| 64  | 177 ns/op (~5.6M/s) | 90 ns/op (~11.1M/s) |
| 256 | 260 ns/op (~3.8M/s) | 94 ns/op (~10.7M/s) |

**Kotlin**, ops/sec:

| Parallelism | readBlob | getDocument |
|---:|---:|---:|
| 1   | ~1,027,700/s | ~4,672,900/s |
| 8   | ~206,900/s   | ~4,651,200/s |
| 64  | ~248,900/s   | ~9,523,800/s |
| 256 | ~338,300/s   | ~6,329,100/s |

Reads are 3-4 orders of magnitude faster than writes in both languages,
as expected: they never touch disk (WAL/fsync) or take the group-commit
path - `GetDocument`/`getDocument` is just a sharded in-memory map
lookup. Kotlin's `readBlob` numbers below parallel-1 are coroutine/JVM
dispatch overhead on a memTable lookup, not disk cost - contrast with
`writeBlob`, where the ~4-9ms physical fsync (see above) actually
dominates.

### Go: transaction commit path (`defaultEngine.Commit` + `CommitTree`)

| Parallelism | Phase 0 tree_rebuild mean | Final tree_rebuild mean |
|---:|---:|---:|
| 1  | 167.6µs | 130.9µs (namespace grown to 3000 docs, vs. 1000 at baseline) |
| 8  | 44.1µs  | 28.7µs |
| 64 | 47.5µs  | 22.3µs |

`lock_wait` here was dominated by the commit DAG's own mutex - see the
gap-fix update below for what changed. `tree_rebuild` at parallel-1 with
namespace grown to 3000 docs is now consistently ~14-31µs (further down
from 130.9µs above), no longer meaningfully growing with document count
- the incremental Merkle trie gap fix below.

## Gap-fix follow-up (same effort, after the above landed)

Four of the five gaps originally listed here were revisited and fixed;
see commits after the Phase 1-6 series for full detail. Summary:

- **True O(delta) commit trees - fixed.** `document_tree_trie.go` /
  `DocumentTreeTrie.kt`: a persistent 16-ary Merkle trie over the UUID's
  32 hex nibbles replaces the old sort+wire-encode+SHA256 algorithm.
  Canonical (same entries -> same hash, independent of insertion order)
  and incremental (insert/delete touches O(32) nodes, sharing every other
  subtree). `DocumentTree.Entries`/`entries` and its cheap O(n) copy are
  untouched - only `TreeHash`'s computation changed. Cross-language
  parity secured via hardcoded vectors generated from Go and checked in
  both languages, since no live interop test compares hash output.
  Rewired the two production hot paths (`InMemoryStorageAdapter.CommitTree`,
  `ServerEngine.CommitTree` / `ServerStorageEngine.commitTree`) to update
  incrementally on every put/delete instead of rebuilding from a full
  snapshot at commit time. Full rebuild-from-scratch (wire decode) is
  slower per-entry than the old algorithm (many small SHA256 calls vs.
  one big one) - an accepted tradeoff since it's off the hot path.
- **Commit DAG sharding - addressed via RWMutex, not full sharding.**
  Full sharding of `InMemoryCommitDag` risked deadlocks for limited gain,
  since branch-head advancement is inherently sequential per branch and
  the critical section was already small. Converted to `sync.RWMutex` so
  concurrent reads (`GetCommit`/`Walk`/`Diff`/etc.) no longer block each
  other; writes stay serialized as they structurally must.
- **`InMemoryStorageAdapter` (Go) - fixed.** Split into a 64-way
  content-hash-sharded blob/document store and per-namespace-locked
  pending writes; `trees` stays under its own single mutex (low
  cardinality, never the contention source the blob/doc maps were).
- **Kotlin/Native memory detection - fixed for Native, JS left as-is.**
  Real detection via POSIX `sysconf(_SC_PHYS_PAGES)`/`sysconf(_SC_PAGESIZE)`
  for `linuxX64`/`macosArm64`. Compiles clean for both but could not be
  executed in this environment (missing full Xcode install blocks the
  Kotlin/Native link step even for the Linux target) - a real,
  acknowledged verification gap, not glossed over. JS/browser still
  returns `null`; there's no portable browser API for this.
- **Cross-shard atomicity, crash recovery under `ASYNC`/`MEMORY_ONLY` -
  not a gap.** This is the durability/consistency trade-off the original
  architecture proposal called out explicitly as the cost of the plan,
  not a bug: an `ASYNC` namespace can still lose up to one sync interval
  on crash, by design.

## Test coverage added

Go: `-race`-clean unit tests for `GroupCommitter`, `shardedDocStore`,
durability modes (`fakeWAL`-based), and hot-tier memory resolution.
Kotlin: coroutine-concurrency tests for the same (`GroupCommitterTest`,
`ShardedDocStoreTest`, `DurabilityTest`, `MemoryBudgetJvmTest`), plus
integration tests proving the new per-session lock in `SqlWireHost`
prevents corruption under concurrently-submitted same-session requests
while different sessions run independently
(`SqlWirePipeliningIntegrationTest`).

Gap-fix follow-up added: Go trie correctness/parity tests
(`document_tree_trie_test.go` - insertion-order independence, incremental-
vs-full-rebuild parity, update/delete/collapse-to-empty), a DAG
concurrent readers-vs-writers test (`concurrency_test.go`), sharded
`InMemoryStorageAdapter` concurrency tests (`in_memory_concurrency_test.go`),
Kotlin cross-language parity vectors (`DocumentTreeTrieParityTest`), and
a Kotlin/Native memory-detection test (compiles but unexecuted in this
environment - see above).

Full suite status at time of writing: all Go and Kotlin tests pass except
two pre-existing failures unrelated to this work - `TestKotlinPutThenGoGet_InteropDelta`
(Go) and `kdb-transport-tcp`/`kdb-jdbc` test compilation (Kotlin), both
caused by environment/classpath issues (a broken `gradlew` wrapper jar and
a missing `kdb-auth` test dependency, respectively) that predate this
branch.
