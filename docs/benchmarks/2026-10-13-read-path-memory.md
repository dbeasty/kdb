# Read-path memory: block cache, cold-frame index, pooled read buffers (2026-10-13)

Branch `perf/read-path-memory` against `origin/main` 0eee4e0. This is step 1 of the larger-than-memory lane
in `docs/kdb-memory-cassandra-plan.md`: bug 5, A1 and A2. On-disk formats do not change.

## Method

- Machine: Apple M3 Max (16 cores), macOS. Both trees were built into test binaries (`go test -c`), and
  baseline and branch were run **interleaved**: base, then new, for each spec in each round.
- Every timed run was taken while holding the shared `bench-lock.sh` lock, so no other session's benchmark
  overlapped it. The 1-minute load average was recorded before each run. Other agents' unit tests and
  e2e suites were still running, so the machine was never fully idle. Interleaving puts both sides under
  the same conditions.
- Three locked windows were used: 00:53–01:24 (1-min load 3.9–5.7), 03:38–03:46 (2.3–4.5) and 06:02–06:07
  (2.5–4.8). Samples are pooled across windows. `n` is shown per row (3–5). An earlier set of runs taken
  before the lock existed was discarded.
- Allocation counts and bytes do not depend on load. Treat ns/op as indicative where `p` is above 0.05.
- Benchmarks were added in standalone `*_bench_test.go` files and copied unchanged into the baseline tree.

## SSTable lookups (bug 5: the block cache was never read)

`BenchmarkLsmLookupBlockCache` builds the store the way the engine does, with a block cache. In the
baseline the cache was built but never read. `BenchmarkLsmLookup1MB/file` uses a store with no cache, so
its gain comes only from the A2 read path: a pooled raw-block read with no fstat.

| benchmark | base | new | Δ | allocs base → new | B/op base → new |
|---|---|---|---|---|---|
| LsmLookupBlockCache/hit | 2130 ns | 126 ns | **−94%** (p=0.008, n=5) | 3 → 0 | 1360 → 0 |
| LsmLookupBlockCache/hit-parallel | 2287 ns | 550 ns | **−76%** (p=0.008) | 3 → 0 | 1360 → 0 |
| LsmLookup1MB/file/hit (no cache) | 2.15 µs | 1.76 µs | −18% (p=0.008) | 3 → 1 | 1360 → 512 |
| LsmLookup1MB/file/miss | 79 ns | 79 ns | ~ | 0 → 0 | 0 → 0 |

## Cold loader (A1 + A2)

`BenchmarkColdIndexBuild` times the first-miss pass that indexes a 5,000-commit log.
`BenchmarkColdLoad` reads random historical versions back from an on-disk log through `deltaColdLoader`.
Each read does one frame read, a CRC, a decode and a hash check. In the names, codec-0 is uncompressed
and codec-1 is zstd.

| benchmark | base | new | Δ (p=0.016, n=5+4) | allocs base → new | B/op base → new |
|---|---|---|---|---|---|
| ColdIndexBuild | 21.98 ms | 17.29 ms | −21% | 432.9k → 277.5k | 25.95 MiB → 18.43 MiB |
| ColdLoad/codec-0/serial | 5.32 µs | 4.06 µs | −24% | 80 → 62 | 4.80 KiB → 3.30 KiB |
| ColdLoad/codec-0/parallel | 4.75 µs | 2.90 µs | −39% | 80 → 62 | 4.80 KiB → 3.30 KiB |
| ColdLoad/codec-1/serial | 5.53 µs | 4.26 µs | −23% | 80 → 62 | 4.74 KiB → 3.30 KiB |
| ColdLoad/codec-1/parallel | 4.66 µs | 3.93 µs | −16% | 80 → 62 | 4.75 KiB → 3.32 KiB |

The parallel rows also include the shared per-segment read lock: concurrent reads of one segment no longer
queue behind each other.

### Cold-frame index heap per version (A1)

`TestColdIndexHeapPerVersion` builds the index over a 60,000-commit log (68,229 indexed versions). It
reports live heap after two GCs, with the loader kept alive. Each figure was measured 3 times per tree and
was identical every run.

| | base | new |
|---|---|---|
| live heap bytes per indexed version | 269.0 | **114.5** (−57%) |
| heap objects per indexed version | 1.004 | **0.004** |

The baseline's map value embedded a `storage.DeltaSegmentRef`, whose namespace string and fields put
pointers in every entry. The new map is `map[Hash]{uint32, int64}`, so it is pointer-free and the GC does
not scan it. The remaining 114 B is Go map overhead around a 48-byte entry. The plan's next step (a
sorted, mmapped sidecar) would remove that too.

## End to end: historical reads through the engine

`BenchmarkHistoricalReadCold` (embed) writes 400 docs × 25 versions of ~1.6 KB each. The version cache is
far below that size (the store's 4 MiB floor), and trees are kept resident. It then reads random
historical versions through `ServerEngine.GetDocument`. 96% of the reads are cold. Each read goes through:

1. the location record, read from the SSTables (now through the block cache)
2. the frame read (now pooled, and usually a single read)
3. the decode

| budget | base | new | Δ | allocs base → new | B/op base → new |
|---|---|---|---|---|---|
| 4 MiB (block cache 1 MiB) | 39.0 µs | 32.0 µs | −18% (n=3+2) | 137 → 114 | 23.2 KiB → 16.4 KiB |
| 32 MiB (block cache 8 MiB) | 37.1 µs | 29.6 µs | −20% (n=3) | 134 → 113 | 22.6 KiB → 16.1 KiB |

The direction agreed in every sample, but with n=3 the time deltas do not reach p<0.05. The allocation
deltas are exact.

## Hot path must not move

`BenchmarkWorkloadRead` (server, full stack point reads of resident data), n=3:

| benchmark | base | new | Δ |
|---|---|---|---|
| single-user/overlapping | 193.2 ns | 195.8 ns | ~ (p=1.0) |
| heavy-multi-user/overlapping | 41.96 ns | 41.87 ns | ~ (p=1.0) |
| single-user/non-overlapping | 160.7 ns | 160.1 ns | ~ (p=1.0) |
| heavy-multi-user/non-overlapping | 157.5 ns | 157.0 ns | ~ (p=0.7) |

Allocations are identical (176 B, 4 allocs). The hot path is within noise.

Engine-only `BenchmarkGetDocumentConcurrent` and `BenchmarkReadBlobConcurrent` (memtable, no SSTable)
have identical allocations. Their timings moved in both directions with n=2–3, for example
`ReadBlobConcurrent/parallel-64` went 203 → 261 ns with p=0.4. Neither touches changed code: they are
served from `docsByHash` and the active memtable.

## zstd (A2)

`BenchmarkDecompress` (28 KiB payload): 10.8 µs → 10.5 µs, 1 → 1 alloc, 28.05 → 28.04 KiB (n=2). There is
no throughput change, as expected. For a frame that declares its size, the baseline decoder already
allocated that size up front. The change is a safety one: a frame declaring more than the limit is now
refused **before** anything is allocated for it. `TestDecompressRefusesADeclaredSizeOverTheLimitWithoutAllocatingIt`
covers a frame claiming 1 GiB against a 64 KiB limit. The refusal allocates less than 1 MiB. The baseline
allocated the declared gigabyte and only then failed.

## Things measured and not shipped

- **A second, cap-limited zstd decoder** (`WithDecodeAllCapLimit`), which would bound lying frames at
  `cap(dst)`. Every `zstd.Decoder` carries several MB of per-concurrency state, and the second one showed
  up as a constant +4.8 MB in `TestOpenCostPerCommitDoesNotGrow`: 1493 B/commit at 6k commits, against a
  900 B ceiling and 690 B on main. With one shared decoder the figure is 693 B. The shared decoder's own
  check stops a frame that decodes past its declared size within one block.
