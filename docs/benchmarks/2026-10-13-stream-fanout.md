# Stream fan-out: encode once, share across subscribers (2026-10-13)

A/B for `perf/stream-fanout` (memory/Cassandra plan, item B8). Base is `origin/main` at 2733e4c. New is the
branch head.

## What changed

Each stream subscriber has its own `sendLoop`. Before this change, every subscriber did the whole job for each
commit on its own: it loaded the commit's operations (`CommitOperations`), evaluated its filter on each document,
and JSON-encoded the `deltaCommit` frame. Now:

- a byte-bounded LRU on the hub (16 MiB, at most 128 commits) holds recent commits' operations;
- for each distinct filter text, the filtered view and its frames are encoded once, with correlation id 0;
- each subscriber copies the shared frame and patches its own correlation id (`wire.SetCorrelationID`; the id is
  only in header bytes 8..12);
- each subscriber still runs its own read check. If that check denies any write, the subscriber's frames are
  encoded for it alone, as before.

## Method

- **Machine:** Apple M3 Max, darwin/arm64, Go 1.26.3.
- **Isolation:** one `bench-lock.sh` invocation ran both sides. Inside it, each round waited for a 1-minute load
  average below 6.
- **Interleaving:** base and new alternated in 10 rounds of `-count=1 -benchmem`, from test binaries built before
  the run. benchstat ran over the 10 samples per side.
- **Benchmark:** `BenchmarkStreamFanout` in `server/stream_fanout_bench_test.go` is new in this PR. It uses only
  APIs that main already has, so the identical file ran on the base side. It has one writer (an in-memory
  runtime). Each op is one `Upsert` of a ~330-byte document, and the op ends when every subscriber has been sent
  that commit's frame. The commit's own cost is included and is identical on both sides. Subscribers are
  authenticated, so the read check is on, under `auth.AllowAll`. Their connections discard frames and report
  themselves as `core.SynchronousSender`, as TCP and WebSocket connections do. `filtered/` gives every subscriber
  the same filter, and every document matches it.
- **Load:** the 1-minute load average was 5.1–5.9 at the start of every round. The 5- and 15-minute averages were
  7–11, because another session was running e2e tests. ns/op is noisier than usual for that reason. B/op and
  allocs/op do not depend on load.

Round loads (1, 5 and 15 minutes):

```
round 1 load: { 5.74 7.91 7.23 }
round 2 load: { 5.68 7.75 7.19 }
round 3 load: { 5.91 7.60 7.16 }
round 4 load: { 5.68 7.25 7.08 }
round 5 load: { 5.82 7.14 7.05 }
round 6 load: { 5.79 7.05 7.02 }
round 7 load: { 5.33 11.71 10.90 }
round 8 load: { 5.73 10.91 10.66 }
round 9 load: { 5.32 8.95 9.91 }
round 10 load: { 5.13 8.68 9.79 }
```

## Results

```
goos: darwin
goarch: arm64
pkg: github.com/limidus/kdb/go/kdb/server
cpu: Apple M3 Max
                                  │   base.txt   │               new.txt               │
                                  │    sec/op    │   sec/op     vs base                │
StreamFanout/subs=1-16              27.85µ ±  6%   30.13µ ± 5%   +8.19% (p=0.009 n=10)
StreamFanout/subs=16-16             55.85µ ± 12%   39.80µ ± 7%  -28.74% (p=0.000 n=10)
StreamFanout/subs=128-16            214.4µ ± 10%   109.2µ ± 1%  -49.05% (p=0.000 n=10)
StreamFanout/filtered/subs=1-16     30.40µ ±  8%   31.29µ ± 3%        ~ (p=0.436 n=10)
StreamFanout/filtered/subs=16-16    66.26µ ± 11%   42.58µ ± 8%  -35.74% (p=0.000 n=10)
StreamFanout/filtered/subs=128-16   299.9µ ± 12%   111.6µ ± 3%  -62.80% (p=0.000 n=10)
geomean                             76.56µ         51.87µ       -32.26%

                                  │   base.txt    │               new.txt                │
                                  │     B/op      │     B/op      vs base                │
StreamFanout/subs=1-16               20.74Ki ± 0%   21.18Ki ± 0%   +2.12% (p=0.000 n=10)
StreamFanout/subs=16-16              59.49Ki ± 0%   23.45Ki ± 1%  -60.58% (p=0.000 n=10)
StreamFanout/subs=128-16            350.61Ki ± 0%   39.75Ki ± 0%  -88.66% (p=0.000 n=10)
StreamFanout/filtered/subs=1-16      22.64Ki ± 0%   23.10Ki ± 0%   +2.05% (p=0.000 n=10)
StreamFanout/filtered/subs=16-16     89.12Ki ± 0%   25.32Ki ± 1%  -71.59% (p=0.000 n=10)
StreamFanout/filtered/subs=128-16   588.03Ki ± 0%   41.68Ki ± 0%  -92.91% (p=0.000 n=10)
geomean                              89.48Ki        27.99Ki       -68.71%

                                  │  base.txt   │               new.txt               │
                                  │  allocs/op  │  allocs/op   vs base                │
StreamFanout/subs=1-16               206.0 ± 0%    209.5 ± 0%   +1.70% (p=0.000 n=10)
StreamFanout/subs=16-16              686.0 ± 0%    345.0 ± 0%  -49.71% (p=0.000 n=10)
StreamFanout/subs=128-16            4.278k ± 0%   1.352k ± 0%  -68.40% (p=0.000 n=10)
StreamFanout/filtered/subs=1-16      227.0 ± 0%    233.0 ± 0%   +2.64% (p=0.000 n=10)
StreamFanout/filtered/subs=16-16    1022.0 ± 0%    368.0 ± 0%  -63.99% (p=0.000 n=10)
StreamFanout/filtered/subs=128-16   6.972k ± 0%   1.375k ± 0%  -80.28% (p=0.000 n=10)
geomean                              996.3         475.2       -52.30%
```

## Reading

- **16 subscribers:** −29% time and −61% bytes unfiltered. Filtered, −36% time and −72% bytes.
- **128 subscribers:** −49% time, −89% bytes and −68% allocations unfiltered. Filtered, −63% time and −93% bytes.
  On base, per-commit cost grows by roughly 1.5–2.1 µs per subscriber. On the new side it grows by about 0.6 µs,
  which covers the wake-up, the read check, and a pooled copy of the frame.
- **1 subscriber:** +8% time (p=0.009) and +2% bytes unfiltered. The filtered case shows no significant time
  difference. With nothing to share, the cache entry, the view map and the extra frame copy are pure overhead:
  about 3–6 allocations per commit. The base side's ±6–8% spread is the same order as this delta.
- **What is left per subscriber:** about 10 allocations per subscriber per commit. They come from waking the
  `sendLoop`, `Head()`, the read check's `DocID.String()`, and the chain slice. None of it is encoding.
