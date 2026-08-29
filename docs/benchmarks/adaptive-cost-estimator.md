# Adaptive cost estimator: measurements

Branch `feat/adaptive-cost-estimator` vs. main at `5a2b408` (post-PR-#12 admission control).
What changed: scans stream instead of materializing the namespace; scans and point reads take
grants sized structurally (namespace cardinality x observed doc size x plan shape) refined by a
learned per-shape table fed with exactly-measured actuals; a per-class accuracy loop tracks
estimate-vs-actual (spec §12 test 10); learned state persists under `--data-dir`. Writes keep the
statically calibrated model; their corrupted feedback path (process-wide cumulative-alloc delta,
sole-in-flight gated) was deleted, along with its two `runtime/metrics.Read` calls per operation.

## Interleaved A/B, wire round trips (Apple M3 Max, 3 alternating rounds per build)

`go test ./kdb/server/ -bench 'BenchmarkWireSelect|BenchmarkWireDocumentGet|BenchmarkFileBackedUpsert$' -benchtime=1s`
with `scan_path_bench_test.go` (API-portable) copied into a worktree of main. Ranges are
min-max over the 3 rounds.

| benchmark | main ns/op | branch ns/op | main B/op | branch B/op |
|---|---:|---:|---:|---:|
| WireSelect limit5-of-1000 | 1.556-1.609M | 1.547-1.630M | 323.8K | **99.3K (-69%)** |
| WireSelect star-200 | 1.173-1.207M | 1.188-1.229M | 1.357M | 1.333M (-2%) |
| WireSelect count-1000 | 1.920-2.077M | 1.897-2.012M | 438.6K | **214.0K (-51%)** |
| WireDocumentGet | 55.6-59.1K | 57.1-60.6K | 19.3K | 20.7K (+1.3K) |
| FileBackedUpsert parallel-1 | 503-524K | 505-531K | ~14.6-17.6K | ~15.3-17.7K |
| FileBackedUpsert parallel-8 | 74.0-78.0K | 73.5-77.7K | 14.9K | 14.8-14.9K |
| FileBackedUpsert parallel-64 | 27.6-28.9K | 27.7-28.6K | 14.6-14.8K | 14.7-14.8K |

Reading:

- **Latency: parity everywhere.** Every ns/op range overlaps between builds, in both directions.
  The estimator's cost does not show above loopback-TCP/JSON noise even on queries as small as
  a point read.
- **Per-query allocation: the streaming fix dominates.** A bounded SELECT over 1000 documents no
  longer materializes the namespace's entry map (`-69%` B/op, stable to ~0.02% across rounds);
  `COUNT(*)` halves. `star-200` returns every document, so the result set dominates and the
  saving is small - as expected.
- **The downside, measured:** a point read pays **+1.3KB and 7 more allocs** per request for its
  grant + estimate + feedback (2-6% ns/op in the noise). A governed SELECT's added admission
  work, isolated by `BenchmarkScanAdmissionOverhead`, is **~598ns / 1,352B / 11 allocs** -
  ~0.04% of even the cheapest measured scan round trip. The write-path grant cycle
  (`BenchmarkWriteGrantCycle`) is 28ns/op / 1 alloc, down from a path that made two
  process-wide `runtime/metrics.Read` calls per operation.

## Behavior not visible in latency numbers

- A scan whose structural cost exceeds the node's entire grant capacity is refused *before*
  running with typed `RESOURCE_EXHAUSTED` ("resubmit smaller"), while bounded queries over the
  same namespace keep working (`TestOversizedScanRefusedResourceExhausted`). Main reserves a
  flat 1MiB for any scan and discovers the truth as memory pressure - or, between two 200ms
  guard samples, as an OOM.
- Estimate accuracy is now a measured, exported quantity: p95(actual/estimate) per class stays
  <= 1.0 over the wire (`TestScanEstimateAccuracyOverWire`), repeated shapes learn tighter
  reservations (`TestRepeatedScanShapeLearnsTighterEstimate`), and `/metrics` exposes
  `kdb_cost_estimate_accuracy_p95`, `kdb_cost_safety_multiplier`, per-class grant/deny
  counters, and the pressure zone - previously none of these left the process.

## Known limits (deliberate)

- First contact with a namespace estimates documents at a 2KiB default until one read has been
  observed; a single oversized scan can be admitted in that window. The accuracy loop then
  raises the safety multiplier. (The alternative - blocking first reads on a calibration scan -
  costs more than it protects.)
- The A/B above is loopback and in-memory-runtime; the governance sim
  (docs/benchmarks/resource-governance-sim) is the behavioral gate under real cgroup limits.
