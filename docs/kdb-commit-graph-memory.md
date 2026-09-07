# What the commit graph costs to hold open

## The claim being checked

Bounding documents, operations and trees left one structure growing with
history: the commit graph. It was recorded as roughly 1.1 KB per commit,
resident at open under both history strategies.

Measured properly, at 20,000 commits of a small document, that was 21.11 MB
of heap — 1,107 bytes per commit. It is now **9.91 MB, 519 bytes per
commit**, without any eviction machinery, because most of what was there
was not the commit graph at all.

## What it actually was

| | bytes/commit | what |
|---|---:|---|
| retained segment reads | ~458 | reading whole delta segments to extract two hashes |
| `d.commits` | 276 | the commit metadata itself |
| `d.treeToCommit` | 81 | tree-hash to commit index |
| `d.txIndex` | 79 | transaction id to commit, for idempotent retry |
| `d.hexSorted` | ~96 | sorted hex strings for prefix lookup |
| parent slices, strings | ~120 | one allocation each per commit |

Three of those are now gone.

**Segment reads, ~458 bytes/commit.** `ListSegments` calls `scanSegmentRef`
per segment, which read the entire segment — clamped only at 256 MB — to
recover its first and last commit hash. Opening calls `ListSegments`
several times, and the buffers were retained: 9.17 MB of a 21.11 MB heap,
the single largest item, and proportional to the size of the log rather
than to anything live. It now walks frame headers a window at a time and
decodes only the two frames that matter.

This one is worth remembering as a measurement lesson: it survived a single
`runtime.GC()` and looked like a phantom until a second collection
confirmed it. One collection can leave freed-but-unswept spans counted as
live.

**`hexSorted`, ~96 bytes/commit.** A sorted `[]string` of every commit's
64-character hex, maintained on every insert with a `copy`, making graph
construction O(n²). It served one prefix-lookup method, which scanned it
linearly and so gained nothing from the sort. Deleted; the lookup scans the
commits.

**`treeToCommit`, ~81 bytes/commit.** Added for the tree-fold rebuild, then
maintained for every commit in every namespace — including the ones on the
objects strategy, which is the default and never asks it anything. Now
built on first use and maintained from then on. Built rather than scanned
per call, because its caller is a history walk and scanning would make that
walk quadratic, which is the shape that path was rewritten to avoid.

## Why the rest is not worth evicting

The obvious next step is to stop holding commit bodies in memory: keep a
skeleton of parents and timestamps, evict the rest, fault it back from the
delta log where every commit is already durable.

The arithmetic does not support it. A commit body is 276 bytes/commit
including its map entry, and the skeleton that ancestry, `Walk` and
`HasCommit` need — parent hashes and a timestamp — is about 154 of those.
Evicting bodies saves ~120 bytes per commit, under a quarter of what is
left, and buys it with a faulting layer on every ancestry path, a lock
restructuring so that traversal never does I/O while holding the graph
mutex, and a lazily-restoring checkpoint.

Once operations are already evicted, a commit *is* its metadata. There is
no fat left on it.

`txIndex` (79 bytes/commit) could go the same way, but it is what makes a
retried transaction return its original commit instead of writing a second
one. Bounding it trades a correctness property for memory, so it should be
a decision someone makes deliberately rather than a side effect of a memory
change.

## What true boundedness would take

Git holds a commit graph far larger than this without the heap cost,
because it does not hold it in the heap: `commit-graph` is a compact
on-disk file, memory-mapped, with fixed-width records and parent references
by position. Ancestry walks page in what they touch and the kernel evicts
what they do not.

That is the shape of a real fix here too — a compact on-disk graph, mapped
rather than parsed into per-commit Go values and map buckets. It replaces
519 bytes of heap per commit with an mmap the OS manages, and it removes
the per-commit allocations that dominate what is left.

It is also a substantially larger piece of work than anything above, and it
should be measured against a workload that actually has millions of
commits before being taken on. At 519 bytes per commit, a namespace needs
about two million commits to reach a gigabyte; the numbers in this document
come from twenty thousand.
