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

### Go: transaction commit path (`defaultEngine.Commit` + `CommitTree`)

| Parallelism | Phase 0 tree_rebuild mean | Final tree_rebuild mean |
|---:|---:|---:|
| 1  | 167.6µs | 130.9µs (namespace grown to 3000 docs, vs. 1000 at baseline) |
| 8  | 44.1µs  | 28.7µs |
| 64 | 47.5µs  | 22.3µs |

`lock_wait` here is dominated by the commit DAG's own mutex, which was
identified but deliberately not sharded in Phase 2 (see that phase's
scope note) - the next concurrency bottleneck to address if this work
continues.

## What's still not done (honest gaps)

- **True O(delta) commit trees.** Still O(n) per commit (with a much
  smaller constant after Phase 3). Requires an incremental Merkle-style
  hash, which changes the wire format and needs byte-identical Go/Kotlin
  parity for the existing interop test. Out of scope for a "test and
  merge" pass; deserves its own dedicated, reviewed migration.
- **Commit DAG sharding.** `InMemoryCommitDag`'s mutex (Go) is still a
  single lock across all commits in a namespace. Its critical section is
  small (map inserts, no I/O), so it's a real but secondary bottleneck
  compared to what Phases 1-3 fixed.
- **`InMemoryStorageAdapter` (Go).** Not sharded; it's the test/lightweight
  target, not the production disk-backed engine Phases 1-5 focused on.
- **Kotlin/Native and JS memory detection.** `totalSystemMemoryBytes()`
  returns `null` (falls back to the 128MiB default) on those targets;
  only the JVM server target has real detection.
- **Cross-shard atomicity, crash recovery under `ASYNC`/`MEMORY_ONLY`.**
  These are the durability/consistency trade-offs the original
  architecture proposal called out explicitly - Phase 4 makes them
  possible per-namespace but doesn't change what they cost: an `ASYNC`
  namespace can still lose up to one sync interval on crash, and that's
  the point of the mode, not a bug to fix.

## Test coverage added

Go: `-race`-clean unit tests for `GroupCommitter`, `shardedDocStore`,
durability modes (`fakeWAL`-based), and hot-tier memory resolution.
Kotlin: coroutine-concurrency tests for the same (`GroupCommitterTest`,
`ShardedDocStoreTest`, `DurabilityTest`, `MemoryBudgetJvmTest`), plus
integration tests proving the new per-session lock in `SqlWireHost`
prevents corruption under concurrently-submitted same-session requests
while different sessions run independently
(`SqlWirePipeliningIntegrationTest`).

Full suite status at time of writing: all Go and Kotlin tests pass except
two pre-existing failures unrelated to this work - `TestKotlinPutThenGoGet_InteropDelta`
(Go) and `kdb-transport-tcp`/`kdb-jdbc` test compilation (Kotlin), both
caused by environment/classpath issues (a broken `gradlew` wrapper jar and
a missing `kdb-auth` test dependency, respectively) that predate this
branch.
