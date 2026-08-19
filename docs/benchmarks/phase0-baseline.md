# Phase 0 baseline — write path

Baseline throughput/latency numbers for the write path, ahead of the 1M
writes/sec reengineering plan. This is Phase 0 of that plan: establish a
measured reference point and per-stage instrumentation before any of the
sharding/durability/tree-rebuild work in later phases, so those phases can
be judged against real before/after numbers instead of vibes.

Machine: Apple M2 Pro (arm64), macOS 15 (Darwin 25.5.0), JDK 21.0.11
(Temurin), Go toolchain per `go.mod`. All numbers below are single-machine,
local-disk (APFS on internal SSD) measurements from one run each — they are
a reference point, not a tuned/statistically-robust benchmark suite. Re-run
before/after each later phase using the same commands below.

## Per-stage instrumentation

Both implementations now record three write-path stages under the same
names, so numbers are comparable across languages:

| Stage | Go | Kotlin |
|---|---|---|
| `lock_wait` | time blocked acquiring the engine/adapter mutex | time blocked acquiring `ServerStorageEngine`'s coroutine `Mutex` |
| `fsync_wait` | time inside `wal.Sync()` | time inside `wal?.sync()` |
| `tree_rebuild` | time inside `InMemoryStorageAdapter.CommitTree`'s tree build | time inside `ServerStorageEngine.commitTree`'s `DocumentTree.build` |

Go: `go/kdb/metrics` (`metrics.Default`, `Track`/`Record`/`Snapshot`).
Kotlin: `kdb-storage-engine` `StageRecorder` (`StageRecorder.Default`).

## Go: `ServerEngine.WriteBlob` (real disk-backed WAL)

```
go test ./storage/engine/... -run '^$' -bench BenchmarkWriteBlobConcurrent -benchtime 2000x -v
```

| Parallelism | ns/op | writes/sec | fsync mean | lock_wait mean | lock_wait p99 |
|---:|---:|---:|---:|---:|---:|
| 1   | 35,827 | ~27,900 | 137ns  | 390.8µs (dominated by 1st-call outlier; steady p50=42ns) | 1.12ms |
| 8   | 33,423 | ~29,900 | 125ns  | 3.09ms  | 3.64ms |
| 64  | 33,123 | ~30,200 | 119ns  | 20.5ms  | 26.6ms |
| 256 | 33,875 | ~29,500 | 80ns   | 33.4ms  | 65.0ms |

**Reading this**: throughput is flat (~30k ops/sec) regardless of how many
goroutines are contending, while `lock_wait` grows linearly with
parallelism (390µs → 33ms mean as parallelism goes 1 → 256). `fsync_wait`
itself stays sub-microsecond throughout. This is the single global
`sync.Mutex` in `ServerEngine.WriteBlob` (server_engine.go) doing exactly
what it looks like: serializing every write in the namespace onto one
lock, with disk I/O contributing negligible cost by comparison. Adding
concurrency currently buys nothing — all it does is grow the queue behind
the mutex.

## Go: transaction commit path (`defaultEngine.Commit` + `InMemoryStorageAdapter.CommitTree`)

```
go test ./transaction/... -run '^$' -bench BenchmarkCommitConcurrent -benchtime 1000x -v
```

Each goroutine commits to a disjoint document ID (no conflicts), isolating
lock + tree-rebuild cost from conflict-detection cost. Namespace grows to
1000 documents over the course of each run.

| Parallelism | ns/op | commits/sec | tree_rebuild mean | lock_wait mean (at n≈1000 docs) |
|---:|---:|---:|---:|---:|
| 1  | 182,730 | ~5,470  | 167.6µs | 877.6µs |
| 8  | 52,142  | ~19,180 | 44.1µs  | 1.08ms |
| 64 | 54,278  | ~18,420 | 47.5µs  | 6.64ms |

**Reading this**: two separate mutexes are in play here —
`InMemoryCommitDag`'s own internal mutex (guards `Head()`/`AppendCommit`)
and `InMemoryStorageAdapter`'s mutex (guards `CommitTree`). Both are single,
namespace-wide locks; the dag mutex was not part of the original bottleneck
list and should be added to Phase 2's sharding scope. `tree_rebuild` cost
per commit (39-150µs at this data size) is consistent with the known
O(namespace size) rebuild — `commitTree` copies every existing document
entry (in_memory.go:164-167) even when only one document changed. At larger
namespace sizes this cost grows further; see below.

## Kotlin: full-stack CLI write path (existing JMH suite, `kdb-benchmark`)

```
./gradlew :kdb-benchmark:jmh
```
(with `docCount` capped to `100, 1000` — see note below)

| Benchmark | docCount | oneShotCount | avgt (ms/op) |
|---|---:|---:|---:|
| `cliPut_batch` (one session, `docCount` sequential puts) | 1000 | — | 11,930.5 ms → **~84 writes/sec** end-to-end |
| `cliPut_oneShot` (runtime reopened per put) | 100 | 10 | 5.5 ms/op |
| `cliPut_oneShot` | 100 | 100 | 55.2 ms/op |
| `cliPut_oneShot` | 1000 | 10 | 5.5 ms/op |
| `cliPut_oneShot` | 1000 | 100 | 55.1 ms/op |

`cliPut_batch`'s ~84 writes/sec is *end-to-end through the full stack*
(CLI session → schema validation → index update → transaction commit →
`ServerStorageEngine`), so it is not directly comparable to the Go
storage-engine-only numbers above — the gap between ~84/sec here and
~30k/sec in Go's raw `WriteBlob` benchmark is mostly schema/index overhead
plus JVM/coroutine dispatch, not a Kotlin-specific storage regression.
Getting an apples-to-apples Kotlin storage-engine-only number is follow-up
work for the next phase (write a `ServerStorageEngine`-level micro-bench
analogous to the Go one, bypassing CLI/schema/index).

### Two things this run surfaced

1. **The existing `docCount=10000` benchmark parameter never completed.**
   Before this session, `CliWriteBenchmark.trialSetup` always called
   `BenchmarkFixture.seedFileDataRoot(docCount = 0)` (hardcoded), which
   NPE'd immediately (`firstId!!` on a `null` id — the seed loop never
   ran). That meant the benchmark had never actually produced a result;
   it always crashed before running. Fixed the NPE
   (`BenchmarkFixture.kt`: `firstId?.toString() ?: ""`), which unblocked
   real execution — and running for real at `docCount=10000` then took
   over an hour of CPU and did not finish, because `cliPut_batch`'s total
   cost is `O(docCount²)` (each of `docCount` commits rebuilds the entire
   tree so far). `docCount` is capped to `100, 1000` in
   `CliWriteBenchmark.kt` for now; re-enable `10000` once Phase 3
   (incremental commit trees) lands. This quadratic blowup is itself
   Phase 0 evidence for the `commitTree` bottleneck — the benchmark
   hanging *is* the finding.
2. Repeated `Error: missing parent <hash>` lines appeared on stdout during
   `docCount=1000` teardown/setup across trials. Did not investigate
   further — flagging as a known rough edge in trial cleanup (likely
   delta-segment or WAL cleanup racing directory teardown between JMH
   trials) rather than a write-path correctness bug, since results were
   still produced. Worth a look before trusting Kotlin numbers at larger
   scale in later phases.

## Bottlenecks confirmed, ranked (supersedes the informal list in the
architecture proposal now that there are numbers behind it)

1. **`ServerEngine.WriteBlob` / `ServerStorageEngine.writeBlob`**: one
   mutex per namespace across the entire write, fsync included. Go data
   above shows this flatlines throughput at ~30k ops/sec independent of
   concurrency — the single most direct blocker to 1M/sec.
2. **`commitTree` full-tree rebuild**: O(namespace size) per commit in
   both languages. Confirmed via the `O(docCount²)` blowup that stalled
   the Kotlin JMH run, and via Go's `tree_rebuild` stage growing with
   namespace size.
3. **`InMemoryCommitDag`'s internal mutex**: namespace-wide, not
   previously called out explicitly — every `Head()`/`AppendCommit`
   serializes here too, independent of the storage adapter's own mutex.
   Add to Phase 2 scope.
4. **`InMemoryStorageAdapter`'s mutex** (`PutDocument`/`CommitTree`):
   same shape as #1, different layer.

## Reproducing

- Go: `cd go/kdb && go test ./storage/engine/... -bench BenchmarkWriteBlobConcurrent -benchtime 2000x -v && go test ./transaction/... -bench BenchmarkCommitConcurrent -benchtime 1000x -v`
- Kotlin: `./gradlew :kdb-benchmark:jmh` (full suite, default iteration counts — takes several minutes; `docCount` intentionally excludes `10000` until Phase 3)
