# The cold-loader index, bounded

Date: 2026-10-13. Base: `main` de5d7f7 (#185). Measured with `TestMemoryProfile` (`KDB_MEMPROFILE_GRAPH=1`,
`KDB_MEMPROFILE_BOUNDED=1`, 1M documents, 2M commits, phase `fullread`), one phase per process, under the shared
benchmark lock. Heap numbers are live heap after a forced GC. Times are single runs on a shared machine and are only
indicative; the consolidated timing pass comes later.

## Why the index was built at all

A cold read resolves a version in this order: the version store, then the objects strategy's location record in the
blob store, then the delta cold loader. Under the default objects strategy the location record should answer. In the
1M dataset almost none did. In the instrumented full read, more than 800,000 of the 1,000,000 reads found **no location
record** for the version, so they fell through to the cold loader. Its first miss indexed the whole delta log.

The cause is that location records were lost at write time. `RecordCommitLocation` files a commit's records when the
commit log reports where the commit landed. Until then, the commit's changed versions wait in a stash, and that stash
held at most **256** commits. The async commit log can run about 512 commits behind: a 256-deep queue plus a batch of up
to 256 being written. So a fast writer dropped the oldest stash, which is the very next one the log reports. In a
20k-document / 40k-commit reproduction, **23,283 of 40,000** commits lost their stash. After the fix, none did, and a
full read had 0 misses.

A second, smaller hole: commits replayed at open never got records, because replay files no positions. That covers the
tail after a checkpoint, or a whole log when there is no checkpoint.

## What changed

1. **Every logged version gets a location record.**
   - The stash bound is now 4096, well above the log's lag. It is still a bound for writes that are never logged.
   - Replay files records for every commit it reads back (`RecordFrameLocations`).
   - On a namespace written after this change, the cold loader is never consulted under the objects strategy.
2. **What is left is bounded.** The cold loader is still needed for:
   - the replay strategy, which writes no location records;
   - history=none, as a fallback behind the blob store;
   - data written before this fix, such as the 1M dataset below.

   Its index is now per delta segment (package `storage/deltaindex`). Each segment has a sorted array of 40-byte
   `(hash, frame offset)` records:
   - **Sealed segments:** the array is written once as a sidecar under `ns/<ns>/deltaidx/` and memory-mapped. It is
     not on the heap, the collector never scans it, and `mmapfile.MappedBytes` reports it to the memory guard.
   - **The newest segment** keeps its array on the heap. That is bounded by the segment rotation size (64 MiB of log),
     not by history.

   embed's commit-ops loader uses the same index, keyed by commit hash.

## Numbers

### The 1M dataset as built before the fix (location records mostly missing)

The same dataset copied fresh for each run. "after" first runs build the sidecars; the second run reuses them.

| | main (de5d7f7) | this branch, 1st open | this branch, 2nd open |
|---|---|---|---|
| live heap after open | 98.9 MB | 98.9 MB | 98.7 MB |
| live heap after reading all 1M documents | **682.8 MB** | **457.2 MB** (−225 MB) | 457.2 MB |
| cold-loader index on the heap | 237.5 MB (`deltaColdLoader.index`) | 9.4 MB (newest segment) | 9.4 MB |
| cold-loader index mapped | — | 70.6 MB (7 sidecars) | 70.6 MB |
| peak RSS | 1,643 MB | 1,594 MB | 1,337 MB |
| 1M reads (single run, indicative) | 22.6 s | 22.3 s | 8.8 s |

Heap profile after the full read (`inuse_space`):

| retained by | main | this branch |
|---|---|---|
| `sstable.parseFooter` (SSTable footer indexes, A4, separate PR) | 297 MB | 299 MB |
| `engine.(*deltaColdLoader).index` | 238 MB | — |
| `deltaindex.(*Index).scan` (newest segment's records) | — | 9 MB |
| live trie slabs | 83 MB | 97 MB |
| version store, block and frame caches | ~40 MB | ~40 MB |

The retained index is now O(newest segment + segments), not O(versions):
- At 2M versions, it is 9.4 MB of heap, against 238 MB before.
- `TestRetainedHeapDoesNotGrowWithHistory` measures 2 segments vs 8 segments of 4,000 keys each: retained heap
  165,088 B vs 166,736 B. The record bytes on the heap are identical, 160,000 B (the newest segment).

### A 1M dataset built with the fix

Pending: a fresh 1M-document build with this branch is running; its full-read numbers will be added here.

## Notes

- **Sidecars are caches.** Each one names the segment it describes (sequence, size, first and last commit, tail kind)
  and carries a CRC32C. A torn, damaged, stale or foreign sidecar is rebuilt from the segment. Callers verify each frame
  against the hash they asked for.
- **A read-only runtime** uses the sidecars it finds and writes none.
- **Truncation** prunes the sidecars of the segments it deleted.
- **Location records add footer entries.** Writing a record for every version adds entries to the SSTable footer index
  (`parseFooter` above). That index is plan item A4, being moved to an mmapped sidecar in a separate PR.
