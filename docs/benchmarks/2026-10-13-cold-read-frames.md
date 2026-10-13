# Cold point reads: delta-frame cache and single-document frame reads (2026-10-13)

Branch `perf/cold-read-frames` against `origin/main` 2733e4c. Items A3 (delta-frame cache) and A3b (read one
document without decoding its commit) of `docs/kdb-memory-cassandra-plan.md`. On-disk formats do not change.

## Method

- Machine: Apple M3 Max (16 cores), macOS. 1-minute load average 6.7–7.1 throughout (other sessions' work).
- **Timing A/Bs are deferred to the consolidated benchmark pass** the user will run once the plan is done. The
  numbers below are single quick runs (`-benchtime 2000x`/`3000x`), taken outside the bench lock as instructed.
  Lead with allocations, bytes allocated and segment bytes read, which do not depend on load; treat ns/op as
  indicative only.
- `BenchmarkColdPointRead` (`storage/engine/cold_point_read_bench_test.go`) names nothing new, so the same file ran
  unchanged in a detached baseline worktree.

## Measure first: what a cold point read cost

`BenchmarkColdPointRead` writes a zstd delta log through the real writer: either 4,000 commits of one document or
200 commits of 100 documents. Each body is ~500 bytes of JSON. It then reads random versions back through
`deltaColdLoader`, the path a version takes after `docsByHash` has evicted it. There is no frame cache in this
benchmark. `read-B/op` is segment bytes read per cold read, counted by a wrapping shim.

| shape | ns/op | allocs/op | B/op | read-B/op |
|---|---|---|---|---|
| docs-1 (base) | 10.5 µs | 50 | 5,043 | 4,092 |
| docs-100 (base) | 235 µs | **1,140** | **277,752** | 20,503 |

For a 100-document commit, the baseline built the whole commit to return one version: a boxed codec value per
field of every operation, 100 patch strings, and a SHA-256 of the 52 KB payload for the commit hash. A CPU
profile of the same read after A3b shows that the rest of the time is the zstd inflate of the whole ~52 KB frame,
about 90–120 µs, measured on its own. The pread of the ~20 KB frame takes 1–2 µs.

### Choosing between A3b (a) and (b)

- (a) is a decode path with no format change. It walks the encoded payload, steps over operations by their length
  prefix, and hands back only the wanted document's patch. It removes **all** of the decode cost: 1,140 → 9
  allocs and 277 KB → 0.8 KB per read. The scan itself allocates nothing
  (`TestScanCommitWritesAllocatesNothing`).
- (b) would add a per-frame index of doc id → (offset, len) in the *uncompressed* payload. It could not save more
  than (a): with (a) the decode is already ~0, and the remaining cost is inflating the frame. A zstd frame is not
  seekable, so an offset into its uncompressed bytes does not avoid inflating them. Only smaller compression units
  would avoid that, and that is a different format change (per-document or chunked compression). The frame cache
  (A3) already removes it for repeated reads.

**Decision: (a). (b) is not built.** No format change means no Kotlin or compat-test impact.

## After: A3b (single-document read) and A3 (frame cache)

| benchmark | ns/op | allocs/op | B/op | read-B/op |
|---|---|---|---|---|
| ColdPointRead/docs-1 (A3b, no cache) | 7.7 µs | 9 | 781 | 4,092 |
| ColdPointRead/docs-100 (A3b, no cache) | 165 µs | 9 | 853 | 20,503 |
| ColdPointReadFrameCache/docs-1/hit | **0.85 µs** | 9 | 781 | **0** |
| ColdPointReadFrameCache/docs-100/hit | **3.7 µs** | 9 | 778 | **0** |
| ColdPointReadFrameCache/docs-1/churn | 9.1 µs | 9 | 3,386 | 4,090 |
| ColdPointReadFrameCache/docs-100/churn | 164 µs | 9 | 778 | 20,503 |

- `hit`: the cache holds every frame of the log. The read is a map probe, a copy of the payload out of the arena,
  the scan, and the content-hash check. On docs-100, 235 µs → 3.7 µs (~60×), with no I/O.
- `churn`: a 256 KiB cache over a log many times its size, so nearly every read misses. This is the cache's cost
  when it cannot help. On docs-1 it is the insert copy, a few hundred ns, with no extra allocations once pooled.
  On docs-100 the 52 KB frames exceed a quarter of a 32 KiB shard, so they are not cached at all.
- The remaining 9 allocations per read are the returned document's string, the closures, and the loader's
  bookkeeping.

### End to end through the engine (`BenchmarkHistoricalReadCold`, embed)

This benchmark writes 400 docs × 25 versions of ~1.6 KB, one document per commit. 95% of the reads are cold. Each
one goes through the location record (SSTables) and then the frame.

| budget | base | new | allocs base → new | B/op base → new |
|---|---|---|---|---|
| 4 MiB | 35.5 µs | 28.3 µs | 106 → **63** | 16,315 → **6,063** |
| 32 MiB | 35.8 µs | 28.9 µs | 104 → **62** | 15,714 → **5,876** |

The frame cache barely hits here. Its share (1/8 of 512 KiB–4 MiB) is far smaller than the 17 MB log of
one-document frames, so what this shows is A3b. The location-record lookup still dominates the time.

### Hot path

`BenchmarkGetDocumentConcurrent` (storage/engine) does not touch any changed code, and its allocations are the
same (0 allocs/op on both sides). Its timing is part of the deferred pass, as is `BenchmarkWorkloadRead` (server).

## Deferred to the consolidated pass

- `-count=6` timing A/Bs with benchstat for `BenchmarkColdPointRead`, `BenchmarkColdPointReadFrameCache`,
  `BenchmarkHistoricalReadCold`, `BenchmarkColdLoad`, `BenchmarkGetDocumentConcurrent` and
  `BenchmarkWorkloadRead`. If `BenchmarkWorkloadRead` sets a memory budget, pin the memory guard as the server
  tests do.
