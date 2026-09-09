# Bounding a historical tree rebuild, and the taper it was causing

Date: 2026-09-09. Machine: Apple M3 Max (16 cores), Go 1.26.3 (`darwin/arm64`), verified idle
(82.9-83.9% CPU idle) immediately before each measurement.

Closes the last item from
[`2026-09-08-workload-matrix-after-pinning.md`](2026-09-08-workload-matrix-after-pinning.md):
per-operation cost on the insert rows rose with namespace size, so `BenchmarkWorkloadWriteInsert`
at `-benchtime 3s` tapered to 1,292 ops/sec having grown the namespace to ~108,000 documents.

## It was not what the profile appeared to say

`treeFromObjects` was 75% of CPU, which reads as "the commit path rebuilds a tree every commit".
Instrumenting the miss path said otherwise: **78 rebuilds in the whole run**, against 235,634
pins. Not frequent and small - rare and enormous, about half a second each, which is precisely
the shape a per-commit average hides. Every other commit was fine.

The suspect going in was the tree-object full-object write, on the grounds that it is O(size).
It was not: `treeChainFullAt` already amortises it to about one entry per commit, and that has
been correct since `1359588`. Profiling the callers rather than trusting the shape of the code
is what separated the two.

## Two defects

**1. A pin that lands too late does nothing.** `Upsert` reads head and then pins its tree, and
on a large namespace the history cache holds barely one tree - so the writers behind it can
evict head before the pin arrives. Measured at 1,242 such pins. Pinning a hash protects an
entry; it cannot conjure one back.

`PinTree` now re-seats the tree from `latestTree`, the atomic snapshot that lives outside the
store and is never evicted. The pin is taken *first*, so the eviction that caused the problem
cannot undo the repair. Absent pins fell 1,242 -> 93.

**2. A rebuild was O(namespace) regardless of what was in memory.** `treeFromObjects` walked
back to the last full object and applied everything forward from empty. `treeChainFullAt` lets a
chain run to `size` entries - which is what keeps the *write* path constant per commit - so
grounding there means one resolution replays the whole namespace, however close a usable tree
was sitting.

The walk now stops at the first ancestor already in memory and applies forward from that. This
is the same move the replay strategy's rebuild has always made (`rebuildTreeByFolding` folds
from the nearest cached ancestor rather than from genesis); the objects strategy simply never
did it. `Peek` rather than `Get`, so probing does not promote entries and evict the tree it is
walking toward.

## Results

`BenchmarkWorkloadWriteInsert/heavy-multi-user`, disk-backed, `DurabilitySync`:

| | `-benchtime 3s` (~108,000 docs) |
|---|---:|
| before 2026-09-08 | **FAIL**, deadline exceeded |
| after trie compression | 1,292 |
| after re-seating the pin | 6,842 |
| after bounding the rebuild | **32,326** |

Baseline (`6d2b4bb`, a far smaller namespace) is 30,051. The row is now *above* baseline at
roughly 3.5x the documents.

Default benchtime, `-count=5`:

| | median | min | max |
|---|---:|---:|---:|
| before | 29,030 | 16,969 | 30,890 |
| after | **33,179** | 32,012 | 33,280 |

The collapse in spread is the clearest evidence of what was happening: a handful of half-second
rebuilds scattered through a run is exactly what produced a 16,969-to-30,890 range, and removing
them leaves a 4% band.

## Tests

`go/kdb/storage/engine/tree_rebuild_bound_test.go`, asserting objects fetched rather than a
duration - the claim is about the walk's length, not the machine's speed. `HistoryTreeChainSteps`
counts them and is kept as ordinary observability rather than test scaffolding.

- `TestRebuildStopsAtAResidentAncestor` - a tree five commits below a resident ancestor must
  fetch about five objects. Measured: **5 with the fix, 869 without**.
- `TestRebuildWithNoResidentAncestorStillResolves` - the shortcut is an optimisation, not a
  requirement; with nothing resident the walk must still reach a full object and produce the
  right tree.

The first version of the first test was vacuous and worth recording as a trap: shrinking the
cache budget to evict the target does not work, because eviction always keeps the
most-recently-used entry and that *is* the target. `TreeAt` then answered from cache, no rebuild
happened, and the assertion passed having measured nothing. It now replaces the store outright
and asserts the target is absent before starting.

## Still open

- **Blob-store compaction fails on large namespaces**: `could not compact the blob store
  (append size exceeds max ...) - it keeps its current tables`, seen at ~108,000 documents.
  Unrelated to this change and not investigated.
- The full workload matrix has not been re-run since these fixes; only the insert rows have.
