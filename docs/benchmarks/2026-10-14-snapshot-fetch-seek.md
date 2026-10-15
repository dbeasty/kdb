# Kotlin SNAPSHOT_FETCH: seek the trie instead of sorting every id per page

`SyncV2Host.snapshotPage` built each page by taking every id in the head tree, turning each into
a string, sorting them, and dropping the ones at or before the cursor:

```kotlin
tree.entries.keys.map { it to it.toString() }.sortedBy { it.second }.filter { it.second > m.after }
```

That cost O(n log n) work and n string allocations per page, so a full walk of n documents in
n/page pages cost O(n²/page · log n).

The trie under `DocumentTree` is keyed on the UUID's nibbles, high to low. That is UUID byte
order, which is also the order of the canonical lowercase strings, so walking the trie in order
already gives the order pages use. `DocumentTree.idsAfter(cursor)` follows the cursor's path
once and then iterates in order. A page now costs O(depth + page).

## Wire behaviour

Pages are unchanged: same documents, same order, same `done` flag and cursor. A cursor that is a
canonical id, which is the only kind Go or Kotlin peers write, seeks. Any other string (uppercase,
no dashes, a prefix, junk) goes through the old comparison against every id, now in trie order
with no sort. `SnapshotPagingTest` keeps the old page builder verbatim as a reference and checks
every page of full walks at several budgets, plus a set of odd cursors.

## Numbers

`SnapshotPagingBench` (jvmTest, gated by `KDB_SNAPSHOT_BENCH`). Documents have ~100-byte
bodies; random UUIDs; macOS, 16 cores, load ~6. The old path's full-walk time is extrapolated
from 5 timed pages at cursors spread across the id range.

| docs | page budget | pages | new walk | new ms/page | old ms/page | old walk (est.) | speedup |
|---|---|---|---|---|---|---|---|
| 100k | 4 MiB (default) | 4 | 20 ms | 5.0 | 41.6 | 0.2 s | ~8x |
| 100k | 256 KiB | 62 | 19 ms | 0.31 | 36.5 | 2.3 s | ~117x |
| 1M | 4 MiB (default) | 39 | 172 ms | 4.4 | 585 | 22.8 s | ~133x |
| 1M | 256 KiB | 619 | 171 ms | 0.28 | 543 | 336 s | ~1970x |

The new walk's cost no longer depends on the page size. What remains is building
`SnapshotDoc`s and fetching bodies. The bench's fetch is an in-memory stub, so real storage
reads will dominate both paths' per-document cost.

`DocumentTree.build` of the 1M tree took 88 s in this run (9 s at 100k). That cost is outside
this change: `headTree` builds the tree once per head and caches it in the DAG.

```
KDB_SNAPSHOT_BENCH=1 ./gradlew :kdb-peer-sync:jvmTest --tests '*SnapshotPagingBench*' --rerun
```
