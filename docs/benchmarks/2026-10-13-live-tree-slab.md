# Live tree in a pointer-free slab (A7, stage 1)

Branch `perf/live-tree-slab`. Plan: `kdb-memory-cassandra-plan.md` A7; `kdb-mmap-plan.md` Phase 4 step 1.
M3 Max, Go 1.26.3, macOS. Machine shared with other sessions: every timing run went through the shared
bench lock, with main and branch alternating in one invocation, load averages 5-9 (stated per table).
Heap sizes and object counts are not load-sensitive; timings are indicative.

## What changed

The document trie's nodes were Go objects: `trieNode{hash; *trieLeaf; *[16]*trieNode}`, about 2.7 heap
objects per document, every one traced by the collector on every cycle. They now live in a slab per tree
family (`go/kdb/document/document_tree_slab.go`):

- **Records:** internal nodes are 96-byte records (hash + 16 `uint32` child references), leaves are
  80-byte records (folded hash, id, content hash), in chunks of plain structs that double from 8 to
  16,384 records. No record holds a pointer, so the chunks are no-scan memory.
- **References:** a `uint32`; 0 is the empty subtree; the top bit selects the leaf pool.
- **Reference counts:** in a dense `uint32` array beside each pool. A record counts its parents, plus
  one per tree handle whose root it is.
- **Tree handles:** each `DocumentTree` carries one small `*trieTree` handle per version, the only
  per-tree heap object.

Tree hashes, shapes and every public `DocumentTree` behaviour are unchanged.

### Reclamation, and why this scheme

Trees are persistent and share structure with history (tree store, pinned readers, lock-free reader
snapshots of `latestTree`). The options were:

- **Explicit roots with an epoch mark-and-compact at checkpoint.** Every holder (live tree, tree store,
  every reader snapshot, every transaction's base) would have to register and unregister roots. That is a
  cross-package protocol, and a missed registration is a use-after-free.
- **Reference counts, with the Go GC deciding root liveness.** Chosen. The GC already knows every place a
  `DocumentTree` is held. When a handle becomes unreachable, `runtime.AddCleanup` gives back its root
  reference, and records reaching zero return to a free list and release their children. The collector
  tracks one object per tree *version*, not per node.

Reader safety: a reader holding a tree holds its handle, so its root's count is at least one, and so is
every record reachable from it. A slot is written only while unpublished (fresh, or freed by every tree
that could name it). Readers take no lock. Every read method ends with `runtime.KeepAlive(handle)`, so the
handle outlives the read even after its root was copied out.

Updates hash outside the slab lock: an edit assembles its new records on the side, which is most of the
cost (up to 33 SHA-256 rounds). It then takes the lock only to place them and count references. Cleanups
queue dropped roots under a separate lock and drain only when the slab lock is free. A writer releases at
most four queued roots per update.

**The pacing problem, and its bound.** Free slab slots look live to the GC pacer, and the records an update
replaces are garbage the pacer cannot see. In a loop that allocates little besides slab records, GC cycles
(the only way to learn a version died) come further apart as the slab grows. Measured: 50k updates on a
200k-doc tree doubled the slab. The engine's write path allocates far more per update than the records it
adds, so natural cycles normally come first. As a bound for when they don't, a slab that has created more
than half its live records since the last GC cycle requests one collection in the
background (one at a time, process-wide, at least 4 MB). That holds a pure update loop to about
1.6x the live records, also across 300k updates. A pointer trie's heap reaches 2x at its GC goal.

## Results: the live tree alone (`TestTreeScale`, `go/kdb/document/document_tree_scale_test.go`)

Bulk build from a packed checkpoint image (the open path), then 1M point reads, a chain of 100k updates
(only the latest kept), 20k deletes, 2,000 keyset pages of 100, a full ordered walk. Same public-API
harness for both builds. Two alternating rounds, load average 5-9; medians shown.

| | 100k main | 100k slab | 1M main | 1M slab | 5M main | 5M slab |
|---|---:|---:|---:|---:|---:|---:|
| Retained heap / doc after open | 164.8 B | 146.5 B | 159.2 B | **123.0 B** | 156.9 B | **119.0 B** |
| Heap objects / doc | 2.78 | 0.00 | 2.72 | **0.00** | 2.69 | **0.00** |
| Forced full GC, tree live | 1.5 ms | 0.14 ms | 8.8-10.7 ms | **0.19 ms** | 47-52 ms | **0.28 ms** |
| Forced full GC after updates | 2.5 ms | 0.2 ms | 14-18 ms | **0.2 ms** | 55 ms | **0.3-0.7 ms** |
| Open (bulk build) | 62-72 ms | 59-63 ms | 562-571 ms | 558-591 ms | 2.9-3.2 s | **2.6-2.7 s** |
| `Get` | 89-104 ns | 48-64 ns | 268-400 ns | **178-194 ns** | 418-470 ns | **227-236 ns** |
| `With` (update) | 8.7-9.2 us | 9.8-10.7 us | 9.0-9.4 us | 10.5-11.0 us | 9.4-9.7 us | 10.0-10.8 us |
| Allocations / update | 13.8 | **3** | 15.4 | **3** | 16.5 | **3** |
| Bytes allocated / update | 986 | 258 | 1,127 | 688 | 1,231 | 755 |
| Keyset page of 100 | 5.7 us | 3.8 us | 5.0 us | 3.4-3.9 us | 3.8 us | 2.7-3.1 us |
| Full ordered walk | 4-5 ms | 3 ms | 38 ms | 31 ms | 133 ms | 102-110 ms |
| Retained / doc after 100k updates | 148.8 B | 339 B | 157.6 B | 189.8 B | 156.5 B | 136.1 B |

Reading it:

- **GC cost is gone**: zero heap objects per document; a full collection with a 5M-document tree live goes
  from ~50 ms to ~0.3 ms (the collector now traces a few hundred chunk headers and handles).
- **Bytes -24%** at 1M-5M: an 80-byte leaf plus ~0.37 internal nodes of 96 bytes, plus 4-byte counts,
  against 96 + 0.37 x 176 for the pointer trie.
- **"After updates" is not a leak.** It is the bound described above. Slab chunks are retained at their high
  water and reused, while the pointer trie returns garbage nodes to the heap. At 100k the 100k updates
  replace every path in the tree several times over inside one GC cycle, so the slab ends at its 1.5-2x
  bound plus chunk slack. At 5M the same churn is small relative to the tree.
- **Updates cost ~10% more** in wall time. In-process A/B against the old pointer trie
  (`BenchmarkTrieUpdateChain`, 1M documents) gives 10.7-11.2 us vs 9.0-9.3 us. A CPU profile puts the
  difference in cache misses: writing into reused slots and releasing dropped paths near the writer. The
  pointer trie leaves that work to the background collector on other cores, and the slab removes that
  collector work (the GC rows above). Hashing is unchanged at ~4.4 us of each update.

## Results: whole namespace, 1M documents / 2M commits (`TestMemoryProfile`)

Default settings (graph file on), one process per phase on an APFS clone of one data directory,
main (`ca4c603`) and branch alternating, two rounds, load average 5-6. Open and close re-run
after the two fixes below; the others are from before them, which do not affect them.

| Phase | main | slab |
|---|---:|---:|
| Open: live heap after | 201 MB | **183 MB** |
| Open: peak heap during | 402-409 MB | **238-242 MB** |
| Open: RSS after | 695-701 MB | **521-525 MB** |
| Open: time (re-run) | 680-693 ms | 619-643 ms |
| Forced GC after open: wall / mark CPU | 5.5-6.3 ms / 26 ms | **0.4-0.5 ms / 0.4-0.5 ms** |
| Full read of every document: heap after | 868 MB | 848 MB |
| Forced GC after full read: wall / mark CPU | 7.7-8.4 ms / 35-36 ms | **3.8-6.1 ms / 7.0-7.7 ms** |
| GC CPU during the full read | 588-592 ms | 221-498 ms |
| Full read: time | 16.1-16.2 s | 17.3-19.1 s (load-sensitive; not re-run) |
| 20k updates: time / peak heap | 0.78-0.89 s / 446-458 MB | 0.76-0.88 s / **387-399 MB** |
| Close + checkpoint: time / peak heap | 70-73 ms / 367-375 MB | 65-73 ms / **343 MB** |
| Close + checkpoint: peak RSS | 749-785 MB | **667-669 MB** |
| Live heap after close (re-run) | 0.9 MB | 0.8 MB |

`MemoryBreakdown().LiveTree` reads 96.8 MB for this tree (its ids are sequential, so the trie is
shallower than for random ids), against the 160 MB the old per-document estimate reported.

Two defects found by this run and the long churn test, both fixed on the branch:

- **A slab outlived its last tree** by one collection cycle: each handle's cleanup argument
  pointed at the slab, and an argument stays reachable until its cleanup has run. Live heap after
  close was 98 MB (main: 0.9 MB). The argument now reaches the slab through a `weak.Pointer`
  (`TestSlabFreedWithItsLastTree`).
- **The GC nudge bound drifted.** It fired only when the slab had to grow. Free slots left by
  the last cycle absorb the next cycle's garbage first, so each cycle ended one floor larger
  (3.8 → 6 → 7.6 → 8.9 → 10.4 MB in a trace). It now fires on garbage created since the last
  cycle. Measured over 300k updates: 1.59x, the same as at 50k.

## Other effects

- `engine.MemoryBreakdown().LiveTree` now reports the slab's actual chunk bytes
  (`DocumentTree.SlabBytes`) instead of `160 x documents`. The slab is shared with the history trees
  derived from the live tree and includes free slots, which a per-document estimate cannot see
  (`TestLiveTreeEstimateMatchesHeap`: heap 20.0 MB, slab 19.5 MB, estimate 13.0 MB). The tree store's
  per-record constants follow the new record sizes.
- The pointer trie is kept unchanged (renamed `ptr*`) in `document_tree_trie_ref_test.go` as the
  reference the slab trie is tested against.

## Tests

- `TestSlabTrieMatchesReference`: random puts, re-puts and deletes against the reference pointer trie.
  Requires the same shape, hashes, `HashFor`, `Walk` order and `WalkAfter` pages, for the current tree and
  for historical trees held across later churn, collections and slot reuse.
- `TestSlabReclaimsDroppedTrees`: after dropping trees, the slab holds exactly the records reachable from
  the trees still held (no leak through sharing, nothing freed under a held tree).
- `TestSlabConcurrentReadersDuringWrites` and `TestSlabConcurrentWritersFromOneBase`, under `-race`, with
  freed records poisoned. Readers on published and old trees check every point read with a Merkle proof
  against the tree hash they were handed, plus full rebuilds. Concurrent writers derive chains from one
  shared base. The tests assert that slots were actually reused during the run.
- `TestSlabTreeMemoryPerDocument`: at most 135 B/doc and 0.01 heap objects/doc at 200k documents
  (measured 130.6 B and 0.0003). At most 1.8x that after 300k updates (measured 1.59x).
- `TestSlabFreedWithItsLastTree`: a slab is collected once its trees are.
- `TestSlabRecordsArePointerFree`, `TestSlabLocate`, plus every existing trie, proof, node, walk and
  packed-tree test.
- `go vet`; `go test -race ./kdb/document/...`; `go test ./...`; e2e 81 passed / 11 skipped / 2 xfailed.
  Nothing persisted changed (hashes and the packed checkpoint format are identical), so the Kotlin gate
  was not required.

## Limits

- Slab chunks never shrink. A namespace that deletes most of its documents keeps its high-water slab
  until the tree family is dropped (a reopen rebuilds it compactly from the checkpoint).
- Each tree version costs one handle plus a runtime cleanup (~0.8 us of each update).
- The live trie still grows with documents (~120 B/doc). Stage 2 (paged cold subtrees in a mapped file)
  is the answer for data larger than memory.
