# Live tree in mapped segments (A7, stage 2)

Branch `perf/live-tree-pages`, on top of stage 1 (#184, the pointer-free slab). Plan:
`kdb-memory-cassandra-plan.md` A7, second step. M3 Max, Go 1.26.3, macOS. Every timing run went through the
shared bench lock, with variants alternating in one invocation; load averages were 4-7 (13 once, noted below).
Heap, RSS and mapped bytes are not load-sensitive. Timings are indicative.

Behind `tree.file` / `KDB_TREE_FILE`, **off by default**. `tree.residentBytes` / `KDB_TREE_RESIDENT_BYTES`
(default 64 MB) sets the resident budget. With the flag off, everything is stage 1 (the "tree-off" rows below
match main).

## What it does

The trie's records (96-byte internal nodes, 80-byte leaves) can now live in **tree segments**,
`ns/<namespace>/tree/tree-<seq>.kdbt`, as well as in the slab:

- **Segment files** have the slab's own record formats and are immutable once written. They are mapped
  read-only through `storage/mmapfile`, so the OS pages them, and they count against the memory budget as
  mapped bytes, the way graph segments do.
- **References** gain a file bit. A slab record's child can point into a segment, and a segment record's
  child is local to it.
- **Externs** let a record refer into an older segment: a table of (segment, record) pairs, resolved and
  reference-counted at attach.

Writes follow the graph segments' LSM shape:

| Operation | Writes | When |
|---|---|---|
| Flush | only the tree's records still in the heap; everything else is an extern into the segments under it | resident records (excluding the current GC cycle's garbage) pass `residentBytes`, and half of it was created since the last write |
| Compaction (full rewrite) | every record, as one segment with no externs | a flush would make the tree span more than `MaxSegments` (8) segments |
| Checkpoint | a flush, unless the tree's root already is a segment's root, plus a `.src` sidecar with the SHA-256 of the checkpoint's packed tree image | every checkpoint of a paged tree |

**Rebase.** After a write, a tree made earlier still refers to heap records. The next update to it first
rebases it onto the new segment. Wherever its hash at a position equals the segment's, the subtree is the same
(Merkle), so it becomes a reference into the segment. The walk descends only where they differ, so a rebase
costs what changed since the write. It needs no snapshot kept alive and works for any tree, not just the live
line. The tree's old heap records go when its old versions are collected.

**Lifetime.** A segment lives while anything outside it refers into it: a heap record's child, a tree
handle's root, or another segment's extern. Its count drops to zero when its last referrer goes, and it is
then unmapped and deleted, unless it belongs to the newest checkpoint's chain. Reader safety is stage 1's
argument unchanged: a held tree keeps every record it reaches counted, so neither a slot nor a mapping is
released under a reader. A slab collected with segments still mapped unmaps them (a cleanup on the slab
reaching only its segment list).

**Open.** The checkpoint still carries the packed tree, and stays the authority.

- If a segment's sidecar matches the packed image's digest, the open maps it and its chain, after verifying
  every file's CRC32C, sizes and root hash, and builds nothing.
- Otherwise it builds a segment from the packed image straight to the file. That is the stage 1 bulk build,
  with records going to disk instead of the slab: never the whole tree in the heap.
- Any failure falls back to the heap build: a damaged, truncated or mistagged file, a missing dependency, no
  mmap, or a big-endian host. Version 2 checkpoints (namespaces of at most 20,000 entries) list entries
  instead of packing them; they are packed in id order, which reproduces the same bytes and digest.

Read-only runtimes map but never write or delete. Nothing persisted that any other reader uses has changed:
the checkpoint format is identical, and the segments are a Go-only cache in a new directory. The Kotlin gate
does not apply.

## Results: whole namespace, 1M documents / 2M commits (`TestMemoryProfile`)

Same data directory as stage 1 (graph file on), APFS clones per phase. Two rounds of four variants:

- **main**: `de5d7f7`, which includes stage 1.
- **tree-off**: this branch with the flag off.
- **tree, first open**: the flag on, with no segment yet, so the open builds one from the checkpoint.
- **tree, reopen**: the flag on, opening a checkpoint whose segment is tagged.

| Phase | main | tree-off | tree, first open | tree, reopen |
|---|---:|---:|---:|---:|
| Open: live heap after | 182.5-182.7 MB | 182.4-182.6 MB | **85.7-85.8 MB** | **85.7-85.8 MB** |
| Open: peak heap during | 236-248 MB | 237-248 MB | **191 MB** | **191-192 MB** |
| Open: RSS after | 519-530 MB | 520-529 MB | 566 MB | 565 MB |
| Mapped (graph + tree) | (not printed) | 264 MB | 355 MB | 355 MB |
| Open: time (first open of a cloned directory) | 0.95-1.03 s | 0.73-1.00 s | 2.16-2.54 s | 1.12-1.16 s |
| Full read of 1M documents: heap after | 850 MB | 849 MB | **752 MB** | **753 MB** |
| Forced GC after a full read: mark CPU | 6.3 ms | 6.6-7.0 ms | 6.8-8.3 ms | 6.6-9.6 ms |
| 20k updates: live heap after / peak | 279-283 / 399-400 MB | 280-284 / 400-403 MB | **168 / 237-240 MB** | **169-172 / 238-247 MB** |
| 20k updates: RSS | 694-697 MB | 695-698 MB | **622-624 MB** | **620-630 MB** |
| 20k updates: time | 0.81-0.82 s | 0.88-0.92 s | 0.91-1.05 s | 0.93-1.04 s |
| Close + checkpoint: peak heap | 343 MB | 343 MB | **246 MB** | **230-246 MB** |
| Close + checkpoint: RSS after | 402-406 MB | 396-404 MB | **301-308 MB** | **303-307 MB** |
| Close + checkpoint: time | 79-87 ms | 89-115 ms | 142-182 ms | 105-143 ms |

The tree's segment is **90.7 MB on disk** for these 1M documents (sequential ids, so ~91 B/doc).

Reading it:

- **The tree has left the heap.** About 97 MB at 1M documents moves from the Go heap into mapped,
  reclaimable page cache. Live heap after open halves (183 → 86 MB). Peak heap falls ~45 MB at open, ~160 MB
  under writes and ~100 MB at close.
- **RSS after open is ~40 MB higher.** The open touches every leaf (the version-pin walk) and the CRC pass
  reads the whole file, and touched mapped pages count in RSS. Unlike heap, the kernel can drop them under
  pressure. Under writes and after close, RSS is 70-100 MB lower.
- **Open costs.** Building the segment from the packed checkpoint (first open) hashes the whole tree, as the
  heap build does, and writes it: about +1.2 s at 1M. A reopen of a tagged checkpoint is 1.1 s on a cold
  clone. The extra time is reading the 91 MB segment for its checksum on top of the 48 MB packed image.
  Warm, the same reopen profiled at 0.40 s. The checksum is not skipped: a corrupt page would otherwise
  serve wrong hashes silently.
- **Checkpoints write.** A checkpoint of a paged tree flushes its heap records (+30-60 ms here), so the next
  open maps instead of builds.

## Results: the tree alone (`TestTreeScale`, `KDB_TREE_SCALE_FILE=1` for file mode)

Random ids, so deeper tries than above. Two rounds; the second 5M file round ran as the load average rose to 13.

| | 1M heap slab | 1M file | 5M heap slab | 5M file |
|---|---:|---:|---:|---:|
| Heap after open, per doc | 123 B | **0.1 B** | 119 B | **0.0 B** |
| Heap after 100k updates, per doc | 190 B | **30 B** | 136 B | **8 B** |
| Open (bulk build) | 0.59 s | 0.68-0.83 s | 2.8-3.4 s | 3.3-6.2 s |
| `Get` | 306-324 ns | 221-289 ns | 400-424 ns | 305-344 ns |
| Update (`With`) | 9.6-14.9 us | 10.9-11.0 us | 10.2-10.6 us | 11.2-11.7 us |
| Delete (`Without`) | 8.8-10.5 us | 6.9-7.0 us | 5.4-5.7 us | 6.1-6.6 us |
| Keyset page of 100 | 3.5-4.1 us | 4.1-4.2 us | 2.9-3.0 us | 3.8-3.9 us |
| Full ordered walk | 29-34 ms | 33-36 ms | 109-111 ms | 147-151 ms |

- "Heap after 100k updates" is the resident overlay. It is ~30 MB and ~40 MB in absolute terms, held
  under the 64 MB budget whatever the document count. The heap slab instead holds the whole tree plus its
  garbage.
- Point reads through mapped records are no slower than through the heap slab: post-order segments keep a
  subtree's records together. Walks over the whole tree are ~10-35% slower (more pages touched).

## Tests

- **`document_tree_file_test.go`:**
  - opening from a packed image equals the heap build and the reference pointer trie, and reopening the same
    image maps the segment without writing;
  - updates with a 32 KB budget (60+ flushes and compactions) match the reference trie, for the live tree and
    for historical trees held across writes, rebases and retired segments; superseded segments are unmapped
    and deleted;
  - `-race` readers check every read by Merkle proof while segments are flushed, compacted, rebased onto,
    unmapped and deleted under them;
  - damage (flipped byte, truncation, wrong sidecar) falls back to a rebuild and the same tree;
  - a checkpoint's segment is mapped by the next open;
  - binding a large heap tree moves it into a segment;
  - resident heap after open stays under 1 MB, and after updates within 4x a 2 MB budget, at both 50k and
    400k documents.
- **`kdb/embed/tree_file_test.go`, through the runtime:**
  - write, close, reopen mapping the checkpoint's chain (no new segment), every document read back;
  - reopen with the flag off reads the same tree;
  - damaged segments rebuild;
  - a read-only runtime maps the segment and changes nothing on disk.
- **Suites:** `go vet`; `go test -race ./kdb/document/...`; `go test ./... -count=1`; storage, embed, server,
  peersync and service suites again with `KDB_TREE_FILE=1 KDB_TREE_RESIDENT_BYTES=256KB`; e2e with the flag
  off and with `KDB_TREE_FILE=1 KDB_TREE_RESIDENT_BYTES=1MB` (77 passed, 15 skipped, 2 xfailed each).

## Limits and next steps

- **History trees pin what they reference.** Trees held in the engine's tree store keep their heap records
  and segments until evicted. After a compaction, the old chain stays mapped and on disk until the history
  trees using it are gone. Flushes are small, so this is mostly bounded by the history budget, but a
  compaction can briefly double the tree's disk and mapped bytes.
- **Slab chunks never shrink**, so the heap stays at the overlay's high-water mark (about 1.5x the budget plus
  a chunk).
- **The plan's acceptance test is not run.** That is a 10M-document namespace with bodies and trie at least
  4x the memory budget, in `TestMemoryProfile`. This PR removes the trie's share; pinned bodies (A5/A3) and
  the placeholder pins (A9) are the rest.
- **Default off.** Flip once the workload matrix runs with it on (the plan's rule, as with `graph.file`).
