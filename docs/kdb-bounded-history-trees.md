# Bound Memory Usage for Historical Trees

## Problem

DocumentTrees are unbounded in memory: every tree produced by any commit stays resident for the lifetime of the namespace, growing linearly with commit count.

**Measured** (70-byte document, so nothing else can dominate):

| commits | live heap | per commit |
|---:|---:|---:|
| 500 | 7.59 MB | 15.9 KB |
| 2,000 | 19.03 MB | 10.0 KB |
| 8,000 | 65.79 MB | 8.6 KB |

A heap profile at 8,000 commits puts **50.5 MB of 75 MB in `document.trieInsertAt`** — the trie nodes themselves. A million commits would be several gigabytes.

The per-commit cost is that high because the trie is fixed-depth 32 with no path compression: rewriting one document rebuilds all 32 nodes on its path, and successive versions share none of them, since every node's hash changes when the leaf does.

## Root cause

Two in-memory maps hold the same trees:

- `dag.InMemoryCommitDag.trees`
- `engine.ServerEngine.treesByHash`

**Both are keyed by tree hash and hold the same `DocumentTree` values**, so they reference the same trie nodes. They are not two different indexes; they are the same index maintained twice.

This matters more than it looks. Evicting from one frees nothing while the other still holds the value — memory is reclaimed only when *both* evict the same entry, which two independently-managed caches will rarely do. Treating them as two caches to bound separately produces two budgets that each mean nothing, because effective retention is their union.

This is the same shape as the original retention bug on this branch, where two structures held the same document text and ablating either one showed no improvement. It is worth not reproducing.

## Design decision

**Only the current tree needs to be resident during normal operation.** Historical trees should be evicted under a budget and re-obtained on demand.

The hot path is already safe from this: `ServerEngine.treeAt` resolves the current tree from `latestTree`, an atomic snapshot held *outside* the map, before touching the map at all. Cache pressure can therefore never slow a normal read, and the current tree cannot be evicted out from under the write path.

What "re-obtained on demand" costs is entirely different under the two history strategies, and that difference drives everything below.

| | cache miss on a historical tree |
|---|---|
| `objects` | one lookup by tree hash in the object store. Measured: a historical read at 16,000 commits cost **+1.7 MB**, flat. |
| `replay` | fold forward from the nearest cached ancestor tree. Cheap when one is nearby, O(commits) when the cache is cold — plus a one-off pass to index commit operations on the first historical read of the process. |

Measured, one historical read on a checkpointed namespace:

| strategy | commits | after open | after one historical read |
|---|---:|---:|---:|
| `replay` | 1,000 | 1.81 MB | 7.91 MB |
| `replay` | 4,000 | 4.98 MB | 29.31 MB |
| `replay` | 16,000 | 17.41 MB | **114.74 MB** |
| `objects` | 16,000 | 17.41 MB | 19.11 MB |

## Implementation path

### Phase 1: make the `replay` rebuild *targeted*, not merely re-runnable

**Current state:** `rebuildHistoricalTrees` streams every segment, folds every commit, and registers *every* tree it passes through via `RegisterHistoricalTree`. It is guarded by a `sync.Once`, so it runs at most once per process.

**Why "make it re-runnable" is not enough.** A wholesale rebuild feeding a bounded cache is self-defeating: it produces N trees into a cache holding k, evicting its own output as it goes. If the tree being asked for is an early one, it is gone before the caller reads it — miss, rebuild, same outcome. That is a livelock, or at best a guaranteed full log scan on every historical read. Phase 2 is not safe until this is fixed.

**Required change:** rebuild the *one* requested tree, register only that tree, and — this is the part that decides the complexity — **fold forward from the nearest cached ancestor tree, not from genesis.** The DAG can walk back from the target commit until it finds one whose tree is in the cache; `applyCommitToTree` already folds a single commit onto an arbitrary tree, so the machinery exists. O(1) extra memory, and the result is identical to what the wholesale rebuild would have produced for that hash.

**Cost, stated plainly.** Starting every rebuild from genesis makes a target at position *k* cost O(*k*), and walking H commits with a cache too small to hold them then costs 1 + 2 + … + H — quadratic. That is a property of restarting from genesis, not of the strategy, and folding from the nearest cached ancestor removes it:

| access pattern | cost under `replay` |
|---|---|
| first historical read of the process | O(H) — builds the commit-operations index once |
| sequential walk, oldest → newest | O(1) per step, since the previous tree is still cached — **O(H) total** |
| random access | O(distance to the nearest cached ancestor) |
| worst case: cold cache, one deep read | O(position of the target) |

The cost that does not go away is that first O(H) pass: the commits' operations may themselves have been evicted, and finding them needs the hash-to-frame index built by scanning the log. That is once per process and bounded in memory, not once per miss. Under `objects` there is no equivalent pass at all — the tree is a lookup.

Do not describe a miss as "moderate", and do not assume a rebuild may start from genesis.

**Files:**
- `go/kdb/embed/tree_rebuild.go` — `rebuildHistoricalTrees`, `applyCommitToTree`
- `go/kdb/embed/checkpoint_open.go` — where the rebuilder is registered
- `go/kdb/storage/engine/restore.go` — `SetTreeRebuilder`, `rebuildTreesOnce`; the `sync.Once` goes away and the signature gains the wanted tree hash

### Phase 2: collapse the two maps into one owner

Do not bound both maps. Delete one.

`dag.InMemoryCommitDag.trees` exists to serve `GetDocumentTree` / `GetDocumentTreeOrThrow`, which have only two real external callers:

- `go/kdb/transaction/default_engine.go:142` (merge)
- `go/kdb/server/server_runtime.go:706`

plus the DAG's own diff helper. Route those at the engine's tree store and the DAG's map goes away, leaving one place that holds trees, one budget, and one eviction policy that actually reclaims memory when it fires.

**Files:**
- `go/kdb/dag/in_memory_commit_dag.go` — remove `trees`, reroute `GetDocumentTree*`
- `go/kdb/dag/dag.go` — the `CommitDAG` interface, if the accessors move
- `go/kdb/embed/persisting_dag.go` — delegation
- the two call sites above

### Phase 3: byte-bounded LRU on the single store

Bound by **bytes, not entries.** A tree's cost scales with the namespace's document count, and a tree rebuilt from objects is a fresh O(documents) trie sharing nothing with anything else — 100 such trees for a 10,000-document namespace is roughly 170 MB. A count-based limit does not bound memory, and every other budget in this engine is in bytes (`DocumentCacheBytes`, `CommitOpsBytes`, `MemtableFlushBytes`, the hot tier).

Structural sharing makes exact sizing impossible — two cached trees may share most of their nodes, so summing per-tree sizes overcounts. An approximation (documents × a per-node constant) is fine; what matters is that the knob is denominated in bytes so it composes with the others.

`container/list` plus a map is the DIY LRU already used by `shardedDocByHashStore`; follow that, including its rule that pinned entries are never evicted.

**Files:**
- `go/kdb/storage/engine/server_engine.go` — `treesByHash`, `treeAt`, `publishTreeLocked`
- `go/kdb/storage/engine/restore.go` — `RegisterHistoricalTree`

### Phase 4: configuration

```go
// HistoryTreeCacheBytes caps how many bytes of historical document trees
// stay resident before the least recently used are evicted and re-obtained
// on demand. Zero derives it from the hot-tier budget.
//
// The current tree is not subject to this: it is served from latestTree,
// outside the cache.
HistoryTreeCacheBytes int64
```

Default: a fraction of the hot-tier budget, in line with `DefaultDocumentCacheFraction` and `DefaultCommitOpsFraction`. Env var `KDB_HISTORY_TREE_CACHE_BYTES` for consistency with the others.

No `< 100 commits` special case and no `min(100, commits/10)` default. A byte budget handles small histories without a branch — they are under budget anyway — and sizing by commit count is both backwards (it would shrink the cache for the small histories that are cheapest to keep) and unknowable at configuration time.

## Also unbounded, and not fixed by any of the above

The commit graph itself: roughly **1.1 KB per commit**, restored at open from the checkpoint, under *both* strategies. That is the 17.41 MB at 16,000 commits in the table above — identical for `replay` and `objects`, because it has nothing to do with trees. At a million commits it is ~1.1 GB just to hold the namespace open.

Bounding trees leaves this as the next ceiling. It should be scoped into this work or explicitly deferred with a note, not discovered afterwards.

## Testing

- A historical read after eviction returns the same answer as before eviction, under both strategies.
- The targeted rebuild is idempotent: the same tree hash asked for twice yields the same tree, and the second ask does not depend on the first having run.
- Eviction actually reclaims: heap after N historical reads does not grow with N once the budget is reached. This is the test that would have caught two maps holding the same values, so assert on measured heap, not on cache length.
- Memory stays bounded as commit count grows — the table at the top of this document, re-run, should flatten instead of climbing.
- A sequential walk over history under `replay` stays linear. This is the test that pins the nearest-cached-ancestor behaviour: if a rebuild ever restarts from genesis, walking H commits goes quadratic and this is what notices.
- The current-tree read path is unaffected by cache pressure.

## Trade-offs

| aspect | cost |
|---|---|
| Memory | bounded (the point) |
| Normal read of current data | unchanged — served from `latestTree`, outside the cache |
| Historical read, cache hit | unchanged |
| Historical read, cache miss, `objects` | one object-store lookup; measured at +1.7 MB |
| Historical read, cache miss, `replay` | O(distance to the nearest cached ancestor), after a one-off O(commits) pass to index commit operations |
| Write path | unchanged |

## Non-goal

This is not about compacting commits or removing history. The delta log stays authoritative and nothing is pruned. This governs only what is cached in memory.
