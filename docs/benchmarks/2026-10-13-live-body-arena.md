# Live bodies and the memtable in pointer-free arenas; accounting for what a namespace holds

Branch `perf/live-body-arena` (plan items A5 step 1, A6, A10 in `docs/kdb-memory-cassandra-plan.md`).
M3 Max, 51 GB, macOS. Load averages during the locked runs were 3-7 (other sessions). Heap, allocation and
GC-mark-CPU figures are load-independent; wall times are indicative only (this Mac varies by +-50%).

**Full timing A/Bs are deferred** to the consolidated benchmark pass the user will run once the plan is done.
What is here: the 1M-document memory profile (locked runs), heap profiles, allocation counts, and an
early locked timing A/B that found (and bisected) one regression, since fixed.

## What changed

| Structure | Before | After |
|---|---|---|
| Live version store (`docsByHash`) | 64 x `map[Hash]docHashEntry{Document(string), *list.Element, ...}`; a Go string per body, a list node and a boxed hash per unpinned entry | per shard: append-only chunked byte arena (4 KiB doubling to 128 KiB; bodies > 32 KiB get an exact chunk released with the body), `[]bodyRec` slot array, intrusive `uint32` LRU ring, open-addressing `uint32` slot index. No pointer per version. |
| Compaction | n/a (strings freed by GC) | a shard compacts when >= 25% of its appended bytes are dead; emptied chunks are dropped at once; slot arrays and the placeholder map are rebuilt when they halve (Go maps/slices never shrink) |
| Point read | zero-copy (the resident string) | still zero-copy: the string aliases the arena. Safe because chunk bytes are written once and never reused; a dropped chunk is reclaimed by the GC only when the last reader lets go (the GC is the reader epoch). The cost is retention: a held body keeps its whole chunk (<= 128 KiB) alive. |
| Memtable (`SortedTable`) | `map[Hash]slot{[]byte}` + `order []Hash` | one value arena + `[]memRec` in first-insertion order + `map[Hash]uint32`. Flush output byte-identical (golden test against the old implementation kept in the test). `Manager.Get` hits no longer copy (main made its result read-only); `Contains` no longer copies a value to discard it. |
| `MemoryDemandBytes` | doc-store live charge, evictable trees, memtable value bytes, block cache | adds the doc store's real footprint (arena incl. dead bytes, slots, index, placeholder pins), memtable footprint, parsed SSTable indexes |
| History tree charge | 176 x (32 + size) per tree, as a private copy | trees published commit by commit are charged their changed paths (leaf + (log16 n + 1) internal nodes per changed doc); full trees 160 B/doc (measured) |
| Metrics | none per structure | `kdb_namespace_memory_bytes{namespace,structure}`: document_bodies(_live), document_index, live_tree, history_trees, pinned_trees, memtable, sstable_indexes, sstable_block_cache, commit_graph, commit_operations |

Cassandra analogues: the version-store arena is the memtable `offheap_objects` slab allocator applied to
the live row cache (bodies in slabs, small fixed records, region-level reclamation); the memtable arena is the
same allocator applied to the memtable proper.

## 1M documents, 2M commits, graph file on (`TestMemoryProfile`, phase `fullread`)

Each run: open a clone of the same dataset, read every live document once at head, measure. Two rounds,
alternating. Base = `origin/main` 529408e (before the rebase onto #170-#177; the store this replaces did not
change in those PRs).

| | base | arena | change |
|---|---:|---:|---:|
| Live heap after open | 201 MB | 201 MB | - |
| Live heap after full read | 1,353 / 1,353 MB | 1,231 / 1,231 MB | **-122 MB** |
| Heap growth per document | 1,152 B | 1,030 B | -122 B |
| Version store after full read (heap profile) | ~381 MB (Put 209 + Pin 72 + body strings 100) | **~166 MB** (chunks 86, slots 73, index 7) | **-57%** |
| Objects with pointers in the version store | ~2M (entry strings + map buckets) + LRU nodes | **0** | |
| Forced GC mark CPU after full read | 536 / 420 ms | 391 / 357 ms | **-15 to -27%** |
| Forced GC wall after full read | 80 / 59 ms | 54 / 41 ms | indicative |
| Full read pass (1M reads) | 30.1 / 30.4 s | 24.5 / 24.4 s | indicative |
| 200k random reads: live heap / peak | 1,099 / 1,858-2,127 MB | 1,091 / 2,050-2,140 MB | ~ |

The arena run's breakdown (`MemoryBreakdown`): bodies 92 MB (89 MB live), doc index 138 MB (of which 56 MB
was the emptied placeholder map; it is now rebuilt as it drains, so ~82 MB), live tree ~160 MB.

**Why the GC win is only ~20% overall:** after a full read, the heap is dominated by structures outside this
PR. Heap profile (arena build): `deltaColdLoader.index` 520 MB (48%), `sstable.parseFooter` 285 MB (26%),
live trie 113 MB, version store 166 MB. The cold-loader index was made pointer-free by #175 (merged after
these runs), so the post-rebase total should drop further; the consolidated pass will show it. The
version store itself went from the third-largest and pointer-heavy to small and unscanned.

Harness note: the existing `reads` phase passed the head *commit* hash to `GetDocument`, which takes a tree
hash, so every read returned not-found and the phase measured an empty loop (including the numbers in
`docs/kdb-mmap-plan.md`). Fixed here: reads use the head commit's tree, and a nil result fails the test.

## Version-store unit costs (`TestDocArenaAccountingMatchesHeap`, `TestDocHashPlaceholderPinsAreSmall`)

| | heap per version | `ResidentBytes` / heap |
|---|---:|---:|
| 200k pinned, 120 B bodies | 216 B (body included) | 1.00 |
| 40k pinned, 1 KiB bodies | 1,155 B | 1.00 |
| 200k placeholder pins | 53 B | 1.07 |

Before, the same store cost 62-142 B per version *on top of* the body string (a separate GC object). The
arena's overhead is ~96 B/version (72 B record + slack + 4-byte index bucket) with nothing for the collector to
scan.

## Allocations, after the rebase onto `origin/main` 2733e4c (`-benchtime 3000x`, memory guard pinned)

| Benchmark | base B/op, allocs | arena B/op, allocs |
|---|---:|---:|
| WorkloadRead single-user (both keyspaces) | 176, 4 | **176, 4** |
| WorkloadRead heavy-multi-user overlapping / non | 319 / 300, 4 | 279 / 261, 4 |
| WorkloadUpdate heavy-multi-user overlapping / non | 15,374 / 14,965, 179 / 178 | 16,529 / 14,441, 177 / 175 |
| WorkloadMixedReadWrite heavy overlapping / non | 7,515 / 7,013, 86 | 7,459 / 6,108, 85 |
| DocHashStoreGet (store only) | 0, 0 | 0, 0 |

## Early locked timing A/B (pre-rebase, n=6, alternating; base/arena/copy-out)

Run to decide the read API and to look for regressions; not a substitute for the final pass.

| | arena (aliasing) vs base | copy-out vs base |
|---|---:|---:|
| DocHashStoreGet parallel-1 / -8 | -25% / -17% ns/op | +27% / +27%, +1 alloc |
| WorkloadRead (4 rows) | -2.3 to -6.2% ns/op, 176 B / 4 allocs | +2.3 to +10.6%, 184-191 B / 5 allocs |
| WorkloadUpdate / Mixed heavy-multi-user | **-17 to -20% ops/s, +8% allocs** | same |
| single-user Update / Mixed | ~ | ~ |

- **Read API:** copy-out costs a measurable +2-11% on point reads and an allocation; aliasing is faster than
  base and its safety does not depend on any scheme of ours (immutable chunks, GC-reclaimed). Aliasing kept.
- **The heavy-write regression** was bisected (n=5): reverting only the memtable `SizeBytes` change matched
  base; reverting the tree-charge change did not. Charging `RecordOverheadBytes` per key in the flush trigger
  made it fire ~6x sooner (objects-strategy values are ~20-byte location records). Fixed: `SizeBytes` stays
  live value bytes (the Kotlin twin's definition) and the per-key cost is reported through `FootprintBytes`,
  which the accounting uses. Post-fix allocations above match base.
- `BenchmarkGetDocumentConcurrent` reads at `codec.Hash{}`, which resolves no tree, so it measures the miss
  path (0.9 ns, 0 allocs) and never touches the store; `BenchmarkDocHashStoreGet` was added for the store.

## Behaviour changes

- **History tree retention.** With delta charging, the history budget now holds many recent trees on a large
  namespace instead of exactly one (at 1M documents a tree used to be "charged" 176 MB against a 32 MB budget).
  Fewer rebuilds for reads at recent commits; real memory still bounded by the budget, with the estimate
  within ~2x for the contiguous runs LRU keeps (see `treeDeltaBytes`). Trees rebuilt for history reads are
  still charged in full.
- **Arbiter demand** now includes the doc store's dead arena bytes, slot index and placeholder pins (~53 B
  per unread live document), the memtable's per-key overhead and the parsed SSTable indexes. Shares shift
  toward namespaces holding more of those; eviction policy is otherwise unchanged.
- **Write path memory:** a written body is copied into the arena; the request's own string can stay alive
  for as long as the DAG's operations budget retains the commit's ops, so recent bodies can be held twice
  within that budget (before, both shared one string).

## Not done, and why

- **Cold-loader index, block-decode buffers in the breakdown:** owned by the read-path PR (#175 has since
  made the index pointer-free). A size getter on `deltaColdLoader` is the hook to add.
- **Index structures (A8):** no counters exist; instrumenting them is A8's job.
- **Budget split:** the default fractions already sum past 1 (docs 0.5, trees 0.25, memtable 0.25, SSTable
  index 0.125, block cache 0.25, ops 0.25: 1.625 in all). Left as is - a policy change, not accounting.
- **`SizeBytes` with overhead:** reverted for the regression above.
