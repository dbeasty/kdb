# Bounded live bodies at 1M documents

Date: 2026-10-13. Base: `main` ca4c603 (#181). Dataset: 1M documents and 2M commits, with the graph mapped. It is the
dataset from #179's runs, copied fresh for every phase. Measured with `TestMemoryProfile` (`KDB_MEMPROFILE_BOUNDED=0|1`),
one phase per process, under the shared benchmark lock, at load average 4.6–5.6. The memory budget is the default
(128 MiB), so the document cache gets 30% of it.

| | bounded off (today) | bounded on |
|---|---|---|
| live heap after open | 201 MB | **118 MB** (−41%) |
| live heap after reading all 1M documents | 850 MB (+649 B/doc) | **702 MB** (+584 B/doc) |
| peak RSS after the full read | 1,911 MB | **1,696 MB** |
| 1M reads | 12.9 s | 12.6 s |
| forced-GC mark CPU after the full read | 32.8 ms | 34.0 ms |

The open win is the per-document pins: a restored live tree no longer pins every version it names.

## What the heap still holds

Heap profile after the full read, bounded on (`heap-after-fullread`):

| retained by | MB | per version |
|---|---|---|
| `sstable.parseFooter` (SSTable footer indexes) | 280 | ~140 B |
| `engine.(*deltaColdLoader).index` | 225 | ~112 B |
| live trie (`document.newLeafNode`, `trieBuildRange`) | 109 | ~109 B per live doc |
| version store (arena chunks and slots) | 24 | bounded by the cache |
| block and frame caches | 16 | bounded |

With bounded live bodies, **document bodies are no longer what grows with the data**. What grows is three per-version
indexes, and they are the next items:

- **SSTable footer indexes:** plan item A4, a sorted, mmapped sidecar.
- **The engine's cold-loader index:** move it to disk, or stop building it at all when location records already
  answer, which is the objects strategy's default.
- **The live trie:** plan item A7, in a separate session.

## Default

The flag ships **off**. Cold live reads change the latency contract: a live document not in the cache costs a cold
read. Turning it on by default belongs with the consolidated benchmark pass.
