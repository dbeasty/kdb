# SSTable index sidecar at 1M documents

Date: 2026-10-13. Base: `main` de5d7f7 (#185). Plan item A4 in `docs/kdb-memory-cassandra-plan.md`.

The dataset is 1M documents and 2M commits, with the graph mapped and bounded live bodies on. It is the dataset from
#185's runs, copied fresh for each side. Measured with `TestMemoryProfile` (`KDB_MEMPROFILE_GRAPH=1`,
`KDB_MEMPROFILE_BOUNDED=1`, phases `open` then `fullread`, one phase per process) under the shared benchmark lock. Each
side uses its own test binary: main for "before", this branch for "after".

The namespace's blob store holds **one SSTable**: 484 MB, 2,369,767 entries, with one block per key. That is one entry
per stored value, so the footer index has one entry per version.

## Headline

| | before (main) | after (sidecar) |
|---|---|---|
| live heap after reading all 1M documents | 682.8 MB (+584 B/doc) | **380.7 MB** (+282 B/doc) |
| `sstable.parseFooter` in the heap profile | **274.4 MB** (116 B/entry) | **gone** |
| SSTable index on the heap (`MemoryBreakdown.SSTableIndexes`) | ~280 MB | **0** |
| SSTable index mapped (`SSTableIndexesMapped`) | 0 | 117.0 MB (49 B/entry; page cache, not heap) |
| RSS after the full read | 1,932 MB | 1,163 MB (second run), 1,336 MB (first run, includes the rebuild) |
| live heap after open | 98.7 MB | 98.7 MB |

The heap no longer grows with the SSTable's size. The index now costs 49 B per entry, on disk, in a mapped file that the
OS pages. The memory guard still charges the whole mapping to the budget, through `mmapfile.MappedBytes`, the same way
it charges the commit graph.

What the heap still holds after the full read:

| retained by | MB |
|---|---|
| `engine.(*deltaColdLoader).index` | 210 |
| live trie slab | 78 + 13 |
| version store | 24 |
| block and frame caches | 10 |

The cold-loader index is now the largest per-version structure on the heap. It is the next item, and another session
owns it.

## The one-time rebuild

The dataset was written before sidecars existed. The first lookup of the table builds its sidecar from the footer and
persists it, so later opens map it directly. The cost was measured in-process against the 2.37M-entry table:

| | peak heap | allocated | time |
|---|---|---|---|
| `parseFooter` (today's cache-miss path, and every miss after an eviction) | +614 MB | 1,147 MB | — |
| sidecar rebuild from the footer (once per table, ever) | +169 MB | 169 MB | 0.78 s |
| sidecar open + checksum verify | +0 MB | 2 KB | 21 ms |

The rebuild reads the footer one 1 MB chunk at a time into a presized slice. It never holds the footer text and a map
of it at once. Tables written by this build get their sidecar when they are written: memtable flushes and compaction
outputs both go through `DefaultWriter.Finish`.

## Lookups (micro-benchmark)

`BenchmarkSidecarLookup` (`go/kdb/storage/sstable`) uses 4 tables of 50,000 keys each. That is what a namespace holds
between compactions at the default trigger. The footer side runs at its best case: the index-cache budget is unlimited,
so every index stays resident. These are single quick runs under the bench lock at load average ~5.5.

| | footer (heap index) | sidecar (mapped) |
|---|---|---|
| index heap | 19.1 MB (95 B/key) | 0.2 MB (1 B/key: slots and mappings) |
| index mapped | 0 | 9.9 MB (49 B/key) |
| tables a miss searches | 4 of 4 (a map probe each) | **0.039 of 4** (the Bloom filter rejects ~99%) |
| hit: ns/op, allocs/op | 363 ns, 0 | 343 ns, 0 |
| miss (`Contains` of an absent key): ns/op, allocs/op | 457 ns, 0 | 340 ns, 0 |
| `Contains` of a present key: ns/op, allocs/op | 630 ns, 0 | 478 ns, 0 |

Lookups allocate nothing on either path. A hit's cost is mostly the block cache and per-table locking, so it barely
changes. A miss no longer scales with the number of tables. The measured Bloom false-positive rate is 0.98% at 10
bits/key (`TestBloomFilterFalsePositiveRateAndNoFalseNegatives`).

## Method notes

- Timing is deferred to the consolidated pass. The fullread wall times (22.7 s before; 25.1 s and then 18.7 s after) are
  single runs on a loaded machine and are not comparisons.
- `KDB_SSTABLE_SIDECAR=off` restores the footer path in an otherwise identical build, for A/B runs.
- Reproduce:
  `KDB_MEMPROFILE_DIR=<copy of ds1m> KDB_MEMPROFILE_GRAPH=1 KDB_MEMPROFILE_DOCS=1000000 KDB_MEMPROFILE_COMMITS=2000000
  KDB_MEMPROFILE_BOUNDED=1 KDB_MEMPROFILE_PPROF=<dir> KDB_MEMPROFILE_PHASE=fullread go test ./kdb/embed -run
  'TestMemoryProfile$' -v`
