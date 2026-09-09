# Base-tree pinning, and the wall behind it

Date: 2026-09-08. Commits: fix at **`5c83152`**, merged as **`bb9459f`** (PR #49), branched
from **`c16d8ab`**. Machine: Apple M3 Max (16 cores), Go 1.26.3 (`darwin/arm64`). Every
benchmark below was run with nothing else executing - concurrent load on this box fakes
regressions large enough to send you after the wrong commit.

Closes the second of the two defects
[`2026-09-07-perf-rerun-7947cce.md`](2026-09-07-perf-rerun-7947cce.md) split Finding 1 into.
That document's forward-looking sections are superseded by this one; its measurements stand.

## Summary

- **The heavy-multi-user write failure is fixed.** `BenchmarkWorkloadWriteInsert` fails
  outright on `c16d8ab` after 211s; at `bb9459f` the whole benchmark passes in 4.6s.
- **The plan in the previous write-up pointed at the wrong place**, and following it exactly
  does not work. Details below, because the wrong version is the one that looks obviously
  right.
- **Throughput is restored to 18,123 ops/sec against a `6d2b4bb` baseline of 30,051.** The
  failures are gone; the write path is not fully back, and the remaining gap is not this.
- **What it is instead: the document trie costs ~4,900 bytes of live heap per document,**
  linear and independent of document size. That is now the binding constraint, and it is what
  the previous write-up's "Done when" criteria were unknowingly asking us to beat.

## Results

`go test ./kdb/server/ -run '^$' -bench BenchmarkWorkloadWriteInsert -count=1`, default
benchtime, disk-backed under `DurabilitySync` + `SyncModeFull`.

| Row | `6d2b4bb` baseline | `c16d8ab` | `bb9459f` |
|---|---:|---:|---:|
| single-user | 242.6 | *(no number - benchmark failed)* | 201.3 |
| heavy-multi-user | 30,051 | **FAIL**, 211.2s | **18,123**, package ok in 4.6s |

`c16d8ab` produced no per-row numbers at all: every heavy-multi sample errored with
`deadline exceeded: timed out waiting for an earlier write to finish`, so the parent
benchmark failed before reporting.

## The defect

Concurrent writers each resolve a base version and then queue behind a capacity-1 write gate.
Every writer ahead of them publishes a newer tree, so by the time a writer reaches the front
its base tree has been evicted from the bounded history cache **by its own competitors** -
and `runSchemaPhase` rebuilds it from tree objects, on the commit path, once per operation.
Profiled at 40% of commit CPU, which is enough to back the queue up past
`DefaultWriteTimeout`.

A larger budget is the wrong instrument, and this is the part worth keeping: the set that has
to survive is not "recent trees". It is exactly the base versions of the writers currently in
flight - which no LRU can identify, and which the write queue already bounds. That is why
pinning, and not sizing, is the fix. Raising the budget to `1<<40` was already tried and
disproven in the previous session.

## Two corrections to the previous write-up's plan

The plan said to hang residency off the `dag.Pin` already taken for `tx.BaseVersion` at
`server_runtime.go:602`. Both halves of that turned out to be wrong, and each cost a full
benchmark cycle to find.

### The pin has to be taken where the base is resolved, not where the transaction runs

`Upsert` anchors on head and *then* queues. Between those two points every other writer
commits, which with a cache sized in whole trees is more than enough to evict the tree before
`runTransaction` is even entered. Pinning at the `dag.Pin` site alone left `treeFromObjects`
at **45% of CPU** - the fix was in, and the profile was unchanged.

The pin belongs in `Upsert`, at the instant the base is resolved and the tree is still live.
With it: **0 pinned-misses across 116,218 pins**.

This is invisible below about a thousand concurrent writers. A 32-writer test showed zero
rebuilds either way, which is exactly how the wrong version passes review.

### Pinned entries must leave the LRU, not be skipped inside it

The first implementation left pinned entries in the list and skipped them during the eviction
sweep. Under a writer burst - every writer pinning its base - that turns into an O(pinned)
walk under the store mutex on *every commit*, and it cost more than the rebuilds it saved:
the benchmark went from failing after 211s to failing after **577s**, with `treeFromObjects`
at 57%. Strictly worse than no fix.

Pinned entries now come out of the list on pin and re-enter at the front on release, so
eviction costs what it frees.

Related: pinned bytes are excluded from the budget comparison and from what the memory arbiter
reads as demand. `treeSizeBytes` charges every tree as an independent copy, while a base
version is a commit or two behind the live tree and shares nearly every node with it.

## The wall behind it: the document trie

At `-benchtime 3s` the benchmark still wedges - 1.87GB RSS, and it outran its own 15-minute
test timeout by well over an hour. **This is not a regression from the pinning work.** The
only reason the benchmark reaches it is that the fix made writes fast enough for Go to ramp
`b.N` into a namespace far larger than anything that ran before.

Measured directly, building a `DocumentTree` and reading `HeapAlloc` after two
`runtime.GC()` calls:

| documents | live heap | bytes/doc |
|---:|---:|---:|
| 10,000 | 48.5 MB | 5,086 |
| 50,000 | 237.6 MB | 4,982 |
| 200,000 | **932.7 MB** | 4,890 |

Linear, and **independent of document size** - the documents here were about 20 bytes of JSON
each. The cause is the fixed-depth-32 trie with no path compression in
`go/kdb/document/document_tree_trie.go`: every document's path is 32 internal nodes deep, and
below the first few levels those nodes are unshared.

This is the *live* tree, not history. The bounded-history work capped what a namespace retains
of its past; this is the floor underneath it - what a namespace costs merely to hold open at
size. A 200,000-document namespace does not fit a 1GB container no matter how the caches are
tuned.

## Verification

`TestConcurrentWritersDoNotRebuildTheirBaseTree` (`go/kdb/server/base_tree_residency_test.go`)
asserts the property rather than a duration, because a timing threshold would only be a guess
about the machine:

| | base-tree rebuilds, 32 writers |
|---|---:|
| without the pin | 27, 31, 29 |
| with it | 0, 1, 2, 2, 3 (limit 8) |

Store-level tests in `go/kdb/storage/engine/tree_store_pin_test.go` cover pin counting,
pinning *before* residency - the ordinary case, since a writer's base may already be gone when
it pins - and eviction at `Unpin` rather than at the next `Put`.

`go test ./...` and `go test -race ./...` green locally; all 16 CI checks green, each in both
paired runs.

## Still open

The previous write-up's "Done when" list is partly met and partly unreachable as written:

- ~~heavy-multi write-insert back near 30,051~~ - **not reachable by fixing rebuilds.** It sits
  at 18,123, and the remainder is the trie cost above, not tree resolution.
- `go/kdb/embed/tree_retention_test.go` still passes - the memory bounding survived. ✅
- `go test -race ./...` green. ✅
- Not re-measured this session: `memory-only/insert/single-user` against its 34,052 target,
  `BenchmarkFileBackedUpsertModes/async-100ms/parallel-1`, and
  `BenchmarkWorkloadMixedReadWrite`. The full matrix has not been re-run since `7947cce`.

**Next, and it is a bigger piece of work than this was:** path compression, or a shallower
fan-out, in the document trie. Until then treat documents-per-namespace as the binding memory
constraint and size deployments from ~5KB/document.

Do not use `-benchtime 3s` on `BenchmarkWorkloadWriteInsert` to evaluate write-path changes
until that is addressed; the ramp walks straight into the wall and the wedge will be
misattributed. Default benchtime, or `-benchtime 300ms`, compares like with like.
