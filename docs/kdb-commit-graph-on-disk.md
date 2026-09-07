# Holding a Million-Commit History Without Holding It in the Heap

> **Status: plan.** Nothing here is implemented. Phase 0 is a measurement
> that decides whether the rest is worth doing at the sizes we actually
> care about.

## Where this picks up

Three pieces of retention work landed before this one, and each removed a
structure that grew with history:

| landed | what stopped growing |
|---|---|
| `perf/bounded-open-memory` | the memtable, and a namespace's whole replayed history |
| `perf/tree-retention-investigation` | historical document trees (byte-bounded LRU, one owner) |
| `perf/bounded-commit-graph` | retained segment reads, `hexSorted`, an always-on `treeToCommit` |

What is left is the commit graph itself, and it is now the only thing whose
resident cost is a function of commit count. Measured at 20,000 commits of
a small document: **519 bytes per commit**, resident from the moment the
namespace opens, under both history strategies.

| commits | projected graph heap at 519 B/commit |
|---:|---:|
| 20,000 | 10.4 MB (measured) |
| 500,000 | 260 MB |
| 2,000,000 | 1.04 GB |
| 10,000,000 | 5.2 GB |

`docs/kdb-commit-graph-memory.md` ended by saying the arithmetic did not
support evicting commit bodies — evicting them saves ~120 of the 519 bytes
and buys it with a faulting layer on every ancestry path. That conclusion
stands. This document is the other answer it pointed at: **stop keeping the
graph in the heap at all.**

## The constraint that shapes every decision below

The database serves the latest dataset. History exists for recovery, audit,
peer-sync and git-style inspection — not for query serving. Three
consequences, and they are load-bearing:

1. **The hot path must not touch any of this.** A point read resolves
   `d.head` (one atomic load, `headSnapshot`) and then the current tree from
   `latestTree`, both of which live outside every cache and index discussed
   here. If a benchmark in `docs/benchmarks/workload-matrix.md` moves, the
   change is wrong regardless of what it saves.
2. **Opening a namespace must not reconstruct historical state.** It restores
   the live tree from the checkpoint and maps a file. It does not fold, replay
   or materialize anything historical. That is already true and must stay true.
3. **History may be slow.** A historical read paying a page fault, a binary
   search and a log frame read is fine. Being *unbounded* is not fine; being
   *resident* is not fine.

Everything below is designed against those three, in that order.

## The shape of the fix

Git holds graphs orders of magnitude larger than ours without the heap cost,
because `commit-graph` is a compact on-disk file with fixed-width records and
parent references by position, memory-mapped rather than parsed. Ancestry
walks page in what they touch; the kernel evicts what they don't.

That is the model. A namespace gets a derived, rebuildable **graph file**
next to its checkpoint. The DAG becomes two tiers:

- **mapped base** — every commit the last graph build covered, at zero heap
  per commit
- **resident tail** — commits appended since that build, in today's maps,
  bounded by the rebuild threshold rather than by history

A read of a commit not in the tail is a fanout lookup plus a binary search
plus a struct read out of the mapping. No allocation unless the caller asks
for something the file deliberately does not carry.

### What the file carries, and what it deliberately does not

Positions are assigned in **log order**, which is already a topological
order: a commit is appended to the delta log only after its parents. So
`pos(parent) < pos(child)` always, and that one property is what makes both
traversal directions and the generation numbers below cheap.

| chunk | per commit | contents |
|---|---:|---|
| `FANO` | — | 256 × uint32, first-byte fanout over `OIDS` |
| `OIDS` | 32 B | commit hashes, sorted; position = index |
| `CDAT` | 76 B | parent 1 pos (u32), parent 2 pos / edge ptr (u32), generation (u32), timestamp (i64), author (16 B), tree hash (32 B), frame location (segment seq u32 + offset u64) |
| `EDGE` | ~0 | overflow parent lists, for commits with >2 parents |
| `TIDX` | 20 B | transaction UUID → position, sorted by UUID |
| `TIDX` | 36 B | tree hash → position, sorted by hash |

Roughly **164 bytes per commit on disk, zero on the heap**. At ten million
commits that is a 1.6 GB file of which the process resides only the pages it
touches — and an ancestry walk touches a handful.

Not carried: `Message`, `Operations`, `NamespaceID`. Messages and operations
are already durable in the log and already faultable — `commitOpsLoader`
does exactly this today, and the frame location in `CDAT` removes its need
to index the log by scanning it. A caller that wants a full `document.Commit`
gets one materialized on demand; a caller that wants ancestry never
allocates one.

**Generation numbers** are the one thing the file adds that memory never
had: `gen(c) = 1 + max(gen(parents))`. `ancestorClosureLocked` today builds a
map of the *entire* ancestor closure for every `IsAncestor` call — at two
million commits that is a two-million-entry map allocated per query, which
is a worse problem than the residency this document is about. With
generations, `IsAncestor(a, d)` stops descending as soon as
`gen(current) < gen(a)`. This is worth doing even if the file never ships,
which is why it is Phase 1 and not Phase 4.

## Traversing both directions

### Backward (newest → oldest) — already native

Every commit names its parents. In the file that is a position, so a step
backward is a pointer decrement into a mapped array — sequential, prefetched,
no map lookup and no hash comparison. This is strictly cheaper than today.

### Forward (oldest → newest) — from position order, not a child index

There is no parent→child edge in a commit, and adding one is the obvious
move. Don't, at least not first. Because position order is topological,
everything reachable *from* a commit lies at a higher position, and two
patterns cover essentially all real use:

- **Walk all of history forward** (recovery, `log --reverse`, re-indexing,
  peer catch-up): scan `CDAT` upward from the start position. Sequential over
  a mapped array, one pass, O(1) heap. This is the fastest possible shape and
  needs no index at all.
- **Step to the child of one commit on a linear chain**: on a namespace with
  no concurrent branches — which is the normal case here — the child is at
  `pos+1`. Check it first; fall back to a bounded upward scan, and only if
  that scan misses does it cost anything.

A `CHLD` chunk (first-child position + next-sibling position, 8 B/commit) is
the fix if measurement shows the fallback scan matters on branchy histories.
It is cheap to add later because it is derived data in a rebuildable file.
**Gate it on a measurement, not on the fact that it is obviously implementable.**

### The API

One cursor type, positioned by hash, by branch head, by genesis, or by
timestamp (binary search on `CDAT` timestamps is not sound across branches —
position order is topological, not chronological — so a timestamp seek scans
within a bounded window and reports the first commit at or after; say so in
the doc comment rather than implying an ordering the data does not have).
It steps in either direction, holds O(1) heap, and yields positions. Callers
that want a commit ask for one; callers doing ancestry never do.

## In-between states: what they actually buy

The question this plan was asked to answer is whether materializing states
between the ends of history makes recovery easier. It does, with one honest
limitation.

**An anchor is a materialized document tree recorded every N commits**, in
the same object store the `objects` history strategy already writes trees
to, keyed by commit position. This is not a new mechanism: it is exactly the
checkpoint's `liveTree`, taken at more than one point, and it should reuse
that encoding rather than invent one.

What changes:

| | today | with anchors every N |
|---|---|---|
| historical read, cold cache | O(distance to nearest cached ancestor); worst case O(position) | O(N), bounded, always |
| worst case at 2M commits | fold 2,000,000 commits | fold ≤ N |
| walk oldest→newest | O(1) per step (nearest-ancestor caching) | unchanged |
| recovery from a torn tail | replay from genesis or the last checkpoint | resume from the last anchor below the damage |
| cost | — | one tree per N commits on disk; nothing resident |

The existing `maxTreeFoldDepth = 100_000` ceiling is the tell. It exists
because an uncached deep read would otherwise walk the entire graph, and it
reports a *miss* when hit — a truthful answer, but a wrong one from the
caller's point of view. Anchors let that ceiling go away, because there is
always a materialized tree within N.

For recovery specifically, the second row of that table is the point.
Recovery asks two questions: what is the newest consistent state, and what
was the state just before the damage. The first is the checkpoint's job and
is already answered. The second is currently a fold from wherever the last
materialization happens to be; with anchors it is bounded work at a known
position, which also means a truncated log can be opened at the last anchor
below the truncation rather than replayed from the start.

### Why folding *backward* from an anchor above is not in the plan

The symmetric idea — find the nearest anchor in *either* direction and
unfold backward if it's closer — would halve the worst case to N/2. It does
not work on the data as recorded. `WriteOp` carries `{DocID, Patch}` and
`DeleteOp` carries a doc id; neither carries the pre-image. **A commit's
operations are not invertible**, so there is no way to walk a tree backward
through one without already knowing the state you are trying to reconstruct.

Making it possible means recording an inverse delta — `(docID, previous
content hash)` per touched document — as a durable sidecar. Not inside the
commit: the commit hash is a cross-language wire contract with golden tests
behind it, and this is local derived data.

That is a real feature with a real cost, and it buys a factor of two on a
bound that anchors have already made finite. **Forward-only from the anchor
below, first.** Revisit inverse deltas only if a measured workload shows the
N-commit fold is the thing hurting, and then size N down before adding a
format.

## Rules that keep this from being a correctness risk

**The graph file is a cache and never an authority.** Same contract as the
checkpoint, and `restoreNamespace` already models it: absent, unreadable,
wrong format version, wrong namespace, or describing segments that are no
longer there — any of those falls back to the maps built from the log, which
is always correct. Nothing in the read path may be reachable *only* through
the file.

**mmap and Go's GC do not protect each other.** Unmapping a region while a
reader holds a slice into it is a segfault, not a recoverable panic, and it
will happen during a graph rebuild. The rule: build the new file under a
temp name, fsync, rename, map the new one, and **keep the old mapping alive
until its readers drain** — a refcount on the mapping, released by the cursor.
Never unmap on the rebuild's thread. This is the sharpest edge in the whole
plan and the one most likely to produce an intermittent crash in CI that
looks like something else.

**Platform support is optional, not assumed.** `PlatformIOShim` is byte
oriented (`ReadSnapshot`/`WriteSnapshot`) and has in-memory, S3-replicated
and browser implementations. Add an *optional* interface a shim may
implement — mapping a snapshot, or reading it at an offset — the way
`storage.DeltaCommitStreamer` is optional next to `DeltaSegmentReader`. A
shim that doesn't implement it keeps today's heap maps and today's behaviour.
Follow the existing build-tag convention: `dir_lock_unix.go` /
`dir_lock_other.go`, `sync_darwin.go` / `sync_linux.go` / `sync_other.go`.

**S3 changes nothing.** The graph file is derived local state, like the
checkpoint. It is not replicated, and a replica that lacks one rebuilds it.

## Phases

Each phase is independently shippable and independently measurable. Do not
start a phase whose predecessor has not been measured.

### Phase 0 — measure at a size that justifies the work

Everything above is extrapolated from twenty thousand commits. Extend
`TestOpenCostPerCommitDoesNotGrow` and `open_cost_bench_test.go` to 100k,
500k and 2M commits and record open heap, open wall time, bytes per commit,
and the cost of one `IsAncestor` at depth. Two specific things to watch for,
both of which would change the plan:

- `ancestorClosureLocked` allocating a closure map proportional to history
  may dominate everything else long before residency does.
- The 519 B/commit figure includes map bucket overhead that does not scale
  linearly; the real number at 2M may be higher, not lower.

**Files:** `go/kdb/embed/open_graph_test.go`, `go/kdb/embed/open_cost_bench_test.go`

### Phase 1 — generation numbers and allocation-free ancestry

In memory, before any file exists. Add a generation number per commit
(4 bytes, computed at `putCommitLocked`), and rewrite `IsAncestor`,
`CommonAncestor` and `CommitsSince` to prune on it instead of materializing
closures. `AncestorSet` keeps its current signature for callers that
genuinely want the set.

Independently valuable, and it is the piece the file format depends on.

**Files:** `go/kdb/dag/ancestry.go`, `go/kdb/dag/in_memory_commit_dag.go`,
`go/kdb/dag/ancestry_test.go`

### Phase 2 — the graph file: writer, reader, and the two-tier DAG

Format, chunk writer, mapped reader, fanout lookup. `GetCommit`,
`HasCommit`, parents, timestamps and generations answered from the mapping;
the resident maps hold only commits appended since the last build. Written
at close and on a rebuild threshold, alongside the checkpoint.

A `document.Commit` materialized from the file needs its message and
operations from the log — the frame location in `CDAT` makes that a single
read and retires `commitOpsLoader.index()`'s full-log scan.

**Files:** new `go/kdb/embed/graph_file.go`, `graph_map_unix.go`,
`graph_map_other.go`; `go/kdb/embed/checkpoint_open.go` (map at open),
`go/kdb/embed/commit_ops_loader.go` (locations from the file),
`go/kdb/dag/in_memory_commit_dag.go` (two-tier lookup),
`go/kdb/storage/platform_io.go` (the optional interface)

### Phase 3 — move the two side indexes into the file

`treeIndex` (81 B/commit) and `txIndex` (79 B/commit) become the sorted
`TIDX` chunks. This is what resolves the note left in
`kdb-commit-graph-memory.md`: `txIndex` is what makes a retried transaction
return its original commit instead of writing a second one, so *bounding* it
trades correctness for memory — but *relocating* it does not. It stops being
heap without stopping being answerable.

**Files:** `go/kdb/dag/in_memory_commit_dag.go` (`CommitForTree`,
`GetCommitByTransactionID`), `go/kdb/embed/graph_file.go`

### Phase 4 — the bidirectional cursor

Forward and backward stepping over positions, `pos+1` fast path for linear
chains, timestamp seek with its ordering caveat documented. Replace `Walk`'s
internal frontier with it for the single-parent case; keep the existing
timestamp-ordered frontier for branchy walks, where it is correct and the
scan is not.

**Files:** new `go/kdb/dag/cursor.go`; `go/kdb/dag/in_memory_commit_dag.go`
(`Walk`), `go/kdb/dag/dag.go`

### Phase 5 — anchors

Write a tree object every N commits; teach `rebuildTreeByFolding` to ground
on the nearest anchor as well as the nearest cached ancestor, and delete
`maxTreeFoldDepth` once it can no longer be reached. Under the `objects`
strategy every tree is already an object and anchors are a no-op — this is a
`replay`-strategy feature, and the config should say so.

**Files:** `go/kdb/embed/tree_rebuild.go`,
`go/kdb/storage/engine/tree_objects.go`, `go/kdb/embed/checkpoint.go`

### Phase 6 — configuration and rebuild policy

```go
// GraphFileEnabled maps the commit graph from disk instead of holding it in
// the heap. Off leaves today's behaviour exactly as it is.
GraphFileEnabled bool

// GraphRebuildCommits is how many commits may accumulate in the resident
// tail before the graph file is rebuilt in the background. This, not the
// commit count, is what bounds resident graph memory.
GraphRebuildCommits int

// HistoryAnchorInterval is how many commits apart materialized trees are
// written, bounding what a cold historical read has to fold. Zero disables
// anchors. Ignored under the objects history strategy, where every tree is
// already an object.
HistoryAnchorInterval int
```

Env vars `KDB_GRAPH_FILE`, `KDB_GRAPH_REBUILD_COMMITS`,
`KDB_HISTORY_ANCHOR_INTERVAL`, matching `KDB_DOCUMENT_CACHE_BYTES` and the
rest. Note these are counts, not byte budgets, and that is deliberate: unlike
document and tree caches, a graph record is fixed-width, so a count *is* a
byte bound.

**Files:** `go/kdb/embed/storage_options.go`

## Testing

- **A differential harness is the primary correctness test.** Run the same
  ancestry, walk and lookup operations against the in-memory DAG and the
  mapped one and assert identical answers, including edge shapes:
  merges, octopus parents, stubs, disjoint histories. Every existing test in
  `ancestry_test.go` should run in both modes.
- **Residency flattens.** Open heap against commit count, in the shape of
  `TestTreeMemoryDoesNotFollowCommitCount` — the ratio should stop tracking
  commit count. Assert on measured heap, not on cache length; the two-maps
  bug that started this whole line of work is invisible to length assertions.
  Take two `runtime.GC()`s before reading, per the measurement lesson in
  `kdb-commit-graph-memory.md`.
- **Every degradation path opens correctly**: no file, truncated file, wrong
  format version, wrong namespace, a file describing segments that are gone,
  a file corrupted mid-chunk. Each falls back and each logs why.
- **A crash during rebuild leaves either the old file or none** — never a
  half-written one that passes its header check.
- **A rebuild while a cursor is open does not crash.** This is the mmap
  refcount test, and it needs to be a stress test under `-race`, not a unit
  test — the failure is a segfault under timing, not a data race a single
  pass will show.
- **The hot path is unchanged.** The workload matrix, re-run, with the graph
  file both on and off. A regression here fails the branch.
- **Forward walks stay linear**, in the shape of `TestHistoryWalkStaysLinear`
  — doubling history doubles the work. That test exists because folding from
  genesis went quadratic; the forward direction can regress the same way.
- **Anchors bound the cold read.** A cold read at a random position costs
  work proportional to the anchor interval, not to the position.

## Trade-offs

| aspect | cost |
|---|---|
| Resident memory | O(tail + pages touched) instead of O(commits) — the point |
| Read of current data | unchanged; served from `head` and `latestTree`, outside all of this |
| Open | maps a file instead of building maps — faster, and it stops scaling with history |
| Ancestry query | page faults instead of map lookups; generation pruning makes it touch far less |
| Historical read | bounded by the anchor interval instead of by position |
| Disk | ~164 B/commit for the graph file, plus one tree per anchor interval |
| Write path | unchanged, except a background rebuild every `GraphRebuildCommits` |
| Complexity | a new on-disk format, an mmap lifetime rule, and a platform fallback |

## Non-goals and explicit deferrals

- **Not compaction.** Nothing is pruned; the delta log stays authoritative.
  Squash and stub keep working exactly as they do.
- **Split graph chains** (git's incremental commit-graph) — a full background
  rewrite is fine until a measurement says it isn't.
- **Bloom filters for path-scoped history** — git carries them for
  `git log -- path`; we have no equivalent query yet.
- **Inverse deltas for backward tree folding** — argued against above.
  Reconsider only with a workload that shows the anchor-interval fold hurting.
- **A `CHLD` child-edge chunk** — deferred to a measurement of branchy
  forward traversal, per the traversal section.
