# Index history bounds (A8): retained heap and latency, before and after

Branch `perf/index-history-bounds` against `origin/main` at 529408e, Apple M3 Max, Go 1.26.3. Every
measurement ran through the shared `bench-lock.sh`, with baseline and branch in one locked invocation and
interleaved run by run. Load averages at the start and end of the main run: 5.70 then 8.13 (other sessions'
jobs were queued on the same lock but not running). Wall time on this machine varies by up to ±50%, so
read the retained-heap and allocation columns first.

## What was measured

`TestIndexMemoryProfile` (`go/kdb/index/stores/memprofile_test.go`, `KDB_INDEX_MEMPROFILE=1`) builds a
commit DAG twice. The first build is the control and has no index. The second feeds one index. The
difference between the two is what the index retains. Each commit carries 10 document writes, rewrites
pick a random document, and a read runs every 10 commits (keyed kinds) or every 100 commits (scored
kinds). The report includes:

- heap after two GCs
- heap objects, a proxy for the pointers the GC must trace
- `/gc/scan/heap:bytes`, the scannable heap
- the fastest of three forced full GCs

Vector search is forced to exact mode in both trees. On main the HNSW graph panics on the second put
after a search (see "Pre-existing bug" below), so this workload cannot run on main with the graph built.

Workload shapes:

- HASH: field `k` = `"v<n>"`, 10 documents per key.
- BTREE: `k` = a random int64 in [0, docs).
- FULLTEXT: 20 words from a 2,000-word vocabulary.
- VECTOR: 32 dimensions.

## Retained heap

| index | docs | writes | main MB | branch MB | main objects | branch objects | main scannable MB | branch scannable MB | main full GC ms | branch full GC ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| HASH | 100k | 0 | 34.0 | 10.4 | 393,750 | 20,399 | 17.5 | 0.7 | 1.78 | 0.30 |
| HASH | 100k | 250k | 81.4 | 11.4 | 929,686 | 20,346 | 37.0 | 0.7 | 2.77 | 0.41 |
| HASH | 100k | 1M | **229.3** | **12.2** | 2,426,496 | 20,355 | 115.7 | 0.7 | 5.77 | 0.39 |
| BTREE | 100k | 0 | 50.3 | 15.1 | 489,604 | 53,495 | 22.5 | 1.7 | 2.38 | 0.55 |
| BTREE | 100k | 250k | 90.4 | 18.0 | 764,105 | 53,337 | 39.9 | 2.2 | 2.77 | 0.50 |
| BTREE | 100k | 1M | **224.4** | **18.1** | 1,514,051 | 53,589 | 112.6 | 2.3 | 4.06 | 0.66 |
| FULLTEXT | 5k | 0 | 16.8 | 16.8 | 390,811 | 380,813 | 11.7 | 11.7 | 1.77 | 1.73 |
| FULLTEXT | 5k | 25k | 99.9 | 36.4 | 2,285,059 | 840,149 | 69.2 | 25.1 | 8.35 | 3.96 |
| FULLTEXT | 5k | 100k | **341.7** | **51.9** | 7,967,521 | 1,219,052 | 234.4 | 35.5 | 24.43 | 5.19 |
| VECTOR | 2k | 0 | 0.8 | 0.8 | 8,030 | 4,026 | 0.5 | 0.5 | 0.24 | 0.18 |
| VECTOR | 2k | 10k | 3.9 | 2.1 | 28,034 | 12,151 | 2.0 | 1.1 | 0.47 | 0.27 |
| VECTOR | 2k | 40k | **13.1** | **2.1** | 88,035 | 12,155 | 6.7 | 1.1 | 0.93 | 0.51 |

Bytes retained per write on main:

| index | bytes per write |
|---|---:|
| HASH | ~208 |
| BTREE | ~204 |
| FULLTEXT | ~3,250 |
| VECTOR | ~313 |

These figures stay flat as writes grow. On main, retained memory is linear in write history.

On the branch:

- **HASH, BTREE and VECTOR stop growing** once the history window is full.
- **Keyed indexes are pointer-free:** about 20k heap objects for 100k documents, whatever the history.
  Main needs 2.4M objects for the same data, and its full GC takes 15× longer.
- **FULLTEXT is bounded by the live corpus plus its 4,096-event window**, but varies with timing.
  Dead postings are compacted only once they make up half of all postings, so a measurement can land
  anywhere in that sawtooth (36 to 52 MB here).
- **Unchanged:** the full-text head state (one `map[string]*termInfo` per version field) is as
  pointer-heavy as before.

## Where historical (non-head) reads come from

`ctx.AtCommit` is set by:

- every SQL read on the wire, which passes the head;
- a session's read head (`execDML`, a transaction's snapshot), which lags head by the commits that
  landed since;
- paged queries, which pin `AtCommit` across pages;
- `control/data.go` revision queries (`At`), which are time travel;
- peer conflict procedures (`conflict_procedure.go`), which pass the head.

So in practice, historical reads are within a few commits of head (session and paging), and occasionally
much older (revision queries).

## Latency and allocations (benchstat, n=8, interleaved)

Benchmarks are in `go/kdb/index/stores/index_bench_test.go`. The index sizes are:

| index | docs | rewrites |
|---|---:|---:|
| HASH, BTREE | 50k | 100k |
| FULLTEXT | 2k | 5k |
| VECTOR | 1k | 3k |

The benchmarks are:

- **HeadRead:** a read at head.
- **RecentRead:** a read pinned at the commit before head.
- **WriteThenRead:** one 10-write commit followed by a head read. This measures the write throughput of
  an indexed namespace that is also being queried.

```
                               │   main    │   branch    vs main
IndexHeadRead/HASH-16            215.7n      218.7n      +1.37% (p=0.041)
IndexHeadRead/BTREE-16           822.9µ      3.155µ     -99.62%
IndexHeadRead/FULLTEXT-16        114.8µ      114.9µ          ~
IndexHeadRead/VECTOR-16          271.8µ      274.2µ      +0.87%
IndexRecentRead/HASH-16          231.5n      3263.5n   +1309.72%   (before the plan memo; see below)
IndexRecentRead/BTREE-16         933.2µ      15.85µ     -98.30%
IndexRecentRead/FULLTEXT-16      114.6µ      119.6µ      +4.30%
IndexRecentRead/VECTOR-16        271.9µ      284.7µ      +4.71%
IndexWriteThenRead/HASH-16       17.38µ      11.38µ     -34.54%
IndexWriteThenRead/BTREE-16      816.3µ      13.90µ     -98.30%
IndexWriteThenRead/FULLTEXT-16   848.0µ      189.2µ     -77.68%
IndexWriteThenRead/VECTOR-16     766.0µ      308.6µ     -59.71%

B/op:      HeadRead/BTREE 263.9Ki -> 4.05Ki   WriteThenRead/FULLTEXT 676Ki -> 117Ki   WriteThenRead/VECTOR 475Ki -> 51Ki
allocs/op: HeadRead/BTREE 203 -> 13            WriteThenRead/FULLTEXT 7784 -> 4106     WriteThenRead/VECTOR 4025 -> 657
```

Reading the results:

- **BTREE ranges.** Main sorted every key on every range query. The branch keeps the keys in byte order
  (order-preserving encoding, merged incrementally), so it binary-searches.
- **WriteThenRead.** On main, the full-text and vector stores rebuild their whole visibility view after
  every write, using a full ancestor closure and a pass over every document's event list. The branch
  advances a head view in place.
- **RecentRead/HASH.** Main memoizes a replayed bucket map per historical cutoff, so a reader pinned one
  commit back paid nothing after the first read. The branch first re-placed the cutoff on every read
  (~3 µs). A memo of the last plan now covers this; the re-run is below.
- **RecentRead, FULLTEXT and VECTOR (+4%).** The same per-read placement cost, which is small next to
  scoring. These stores have no memo.

Re-run with the plan memo (commit "memoize the last historical plan"), n=8, interleaved, under the lock (load
5.92 to 7.75):

```
                         │   main    │   branch    vs main
IndexHeadRead/HASH-16       214.1n      216.7n      +1.24%
IndexHeadRead/BTREE-16      797.0µ      3.079µ     -99.61%
IndexRecentRead/HASH-16     228.7n      288.1n     +25.95%   (allocs 5 -> 4, B/op 204 -> 241)
IndexRecentRead/BTREE-16    887.5µ      11.88µ     -98.66%
```

A pinned read one commit behind head still costs about 60 ns more than on main. Main returns a memoized
whole-namespace bucket map, one per historical cutoff, eight of them. The branch reverses that commit's
effects for the requested key, and keeps no copies.

## Pre-existing bug found

In `vector.Store`, once a search had built the HNSW graph, a new put was inserted into a graph whose
`nodes` slice had not been refreshed since that search. The second put after a search followed a link to
the first put's node and indexed past the end of the slice. That is a panic on the commit path of any
vector index with more than `ExactThreshold` (1,000) live vectors.

- It reproduces on main: two puts after one search on an index forced to HNSW.
- It is fixed on the branch.
- The regression test is `TestVectorInsertsAfterGraphBuilt`.
