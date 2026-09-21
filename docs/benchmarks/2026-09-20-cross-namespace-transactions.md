# Cross-namespace transactions — measurements (2026-09-20)

What the performance protocol in
[kdb-cross-namespace-transactions-plan.md](../kdb-cross-namespace-transactions-plan.md) costs and
buys. The benchmarks are in `go/kdb/server/cross_namespace_bench_test.go`:

```bash
cd go && go test ./kdb/server/ -run '^$' -bench 'BenchmarkCross' -benchtime 2s -count 3
```

## Setup

- Apple M3 Max, 16 cores, internal SSD, macOS. File-backed hosts in a temp dir.
- Default durability: `sync`, `syncMode=full` — every flush is `F_FULLFSYNC` (~4 ms here).
- Every operation writes fresh documents, so nothing conflicts. These numbers measure the commit
  path, not retries.
- `uptime` was checked before each run: load average was 7–10. One unrelated process
  (`zolik-server`, up 3 days) held one core at 100% throughout. Every variant ran under the same
  conditions, so the comparisons below hold; the absolute numbers would be a little higher on an
  idle machine.
- The tables show the median of 3 runs. **ops/s: higher is better. Latency (ms): lower is
  better.** One op is one transaction, whether it touches one namespace or several.

## 1. The headline: atomic, and it scales with concurrency

Two namespaces, one document written in each, per transaction.

| Writers | Simple protocol (one lock, held through both fsyncs) | **Performance protocol** | Speed-up |
|---:|---:|---:|---:|
| 1 | 94.6 ops/s · p50 11.9 ms | **93.1 ops/s** · p50 11.9 ms | 1.0× |
| 16 | 92.2 ops/s · p50 172 ms | **429.7 ops/s** · p50 36.0 ms | **4.7×** |
| 64 | 93.6 ops/s · p50 668 ms | **1,580 ops/s** · p50 39.9 ms | **16.9×** |

The simple protocol does not scale at all: throughput is capped at one transaction per two
flushes, whatever the concurrency, and latency grows linearly with the queue. The performance
protocol's decision records and part flushes are group-committed, so concurrent transactions share
fsyncs.

## 2. The price of atomicity

The same two documents written three ways.

| Writers | One namespace, one commit | Two namespaces, two commits (**not atomic**, what applications did before) | Two namespaces, `CommitAcross` (atomic) | Atomic ÷ not atomic |
|---:|---:|---:|---:|---:|
| 1 | 241 ops/s · p50 4.0 ms | 120.7 ops/s · p50 8.0 ms | 93.1 ops/s · p50 11.9 ms | 77% |
| 16 | 1,909 ops/s · p50 8.0 ms | 569.5 ops/s · p50 28.2 ms | 429.7 ops/s · p50 36.0 ms | 75% |
| 64 | 7,155 ops/s · p50 9.0 ms | 1,882 ops/s* · p50 34.1 ms | 1,580 ops/s · p50 39.9 ms | 84% |

\* The non-atomic 64-writer runs spread widely (2,303 / 1,882 / 997).

- **Atomicity costs one extra flush, the decision record.** A single writer pays 11.9 ms against
  8.0 ms for the two non-atomic commits. The two parts' flushes do not overlap on this machine
  (see §4), so the protocol's floor is about three flushes, not two.
- Under concurrency the gap stays at 75–84% of the non-atomic rate, because the decision flush is
  shared across every group in flight.
- Splitting data across namespaces is what costs most (7,155 → 1,882 at 64 writers), with or
  without atomicity. That cost is the disk's, as §4 shows.

## 3. Wider groups and disjoint groups

| Writers | 2 namespaces | 4 namespaces, all in every group | 4 namespaces, half the writers on a+b and half on c+d |
|---:|---:|---:|---:|
| 1 | 93.1 ops/s | 63.4 ops/s · p50 16.0 ms | — |
| 16 | 429.7 ops/s | 301.8 ops/s · p50 52.1 ms | 313.4 ops/s · p50 48.3 ms |
| 64 | 1,580 ops/s | 1,068 ops/s · p50 57.1 ms | 1,050 ops/s · p50 58.0 ms |

Disjoint pairs share no gate, so on a disk whose flushes ran in parallel they would approach twice
the two-namespace rate. Here they run *slower*: four logs plus the decision log are five flush
streams against one device (§4). The protocol does not serialize them; the device does.

## 4. What cross-namespace traffic costs its neighbours, and why

16 writers commit to namespace `a` alone, while 16 background writers do something else.

| Background load | `a`'s throughput | `a`'s p50 |
|---|---:|---:|
| none | 1,828 ops/s | 8.3 ms |
| **control:** ordinary single-namespace commits to `b` (no groups) | 932 ops/s | 16.3 ms |
| cross-namespace groups over `b`+`c` (no shared gate) | 448 ops/s | 35.9 ms |
| cross-namespace groups over `a`+`b` (shared gate, ack chaining) | 373 ops/s | 41.8 ms |

The control row is the key one. Adding **one** ordinary writer on another namespace halves `a`'s
throughput, and no transaction is involved. On macOS `F_FULLFSYNC` drains the whole device, so
every independent log that flushes is another stream contending for the same drain. Groups over
`b`+`c` add three streams (two part logs and the decision log), and `a` drops to about a quarter,
which is what that model predicts. When the groups share `a`, it costs a further ~17%: the ack
chaining, where `a`'s commits queued behind an undecided group wait for its decision.

So on this machine:

- The protocol's own cost to unrelated namespaces is **zero**: they share no lock with it.
- The disk's cost is proportional to the number of distinct logs flushing. That is a property of
  one-log-per-namespace under `F_FULLFSYNC`, and it predates this change.
- Sharing a namespace with groups costs about 17% on top.

`syncMode=fast` (`F_BARRIERFSYNC`) was also measured, via `KDB_BENCH_SYNC_FAST=1`. Its run-to-run
spread on this machine was 2–3× (for example 1,069 vs 3,909 ops/s for the same 64-writer
cross-namespace case), too wide to tabulate. The shape matched: the same contention pattern, at
roughly 4–10× the throughput. Linux `fdatasync` is per file, and Linux is where this should be
re-measured before drawing conclusions about the deploy target (see
[2026-09-10-durability-and-lock-contention.md](2026-09-10-durability-and-lock-contention.md)).

## 5. Correctness under the same load

Not a benchmark, but measured alongside:
`TestConcurrentTransfersPreserveTheTotal` (`-race`) moved money between 9 accounts in 3
namespaces with 8 writers, retrying conflicts. It committed 320 transfers with 163–503 conflicts
retried, and took 558–36,662 consistent snapshots. No snapshot, and not the final state, ever saw
the total change. With the snapshot's publication check removed (a mutation test), the same test
fails on every run.
