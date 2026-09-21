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
roughly 4–10× the throughput. Linux `fdatasync` is per file; §5 has the Linux run.

## 5. Linux (the deploy target)

The same suite, same code, run in Linux: a `golang:1.26` container (linux/arm64, kernel
6.12 linuxkit, 16 vCPUs) on this Mac's Docker Desktop, with the data directory on an ext4 volume
on the VM's virtio disk, not a macOS bind mount. `fsync` there is a per-file `fdatasync`-class
flush (~0.3–0.5 ms here) rather than a whole-device drain. Load inside the VM was 1.4 at start.
Median of 3. **The VM is noisier than bare metal**: some runs spread 2× (e.g. 3,972 / 7,411 /
7,941 ops/s for one cell). Read single cells loosely and the shape firmly.

```bash
docker run --rm -v "$PWD/go":/src:ro -v kdb-bench-data:/bench -w /src golang:1.26 \
  sh -c 'go test -c -o /bench/server.test ./kdb/server && TMPDIR=/bench/tmp /bench/server.test \
         -test.run "^$" -test.bench BenchmarkCross -test.benchtime 2s -test.count 3'
```

| Writers | Simple protocol | **Performance protocol** | Two commits, not atomic | One namespace, one commit |
|---:|---:|---:|---:|---:|
| 1 | 1,305 ops/s · p50 0.73 ms | **1,480** · p50 0.66 ms | 1,964 · p50 0.49 ms | 2,044 · p50 0.33 ms |
| 16 | 1,157 · p50 12.6 ms | **7,411** · p50 1.97 ms | 8,313 · p50 1.98 ms | 10,133 · p50 1.06 ms |
| 64 | 1,232 · p50 47.8 ms | **15,791** · p50 3.78 ms | 11,894 · p50 3.00 ms | 4,287* · p50 3.04 ms |

\* Single-namespace 64-writer runs had a p99 of 580–820 ms. That is the one-namespace write gate
saturating once fsync is cheap. It is not a cross-namespace effect, and it is why that cell
reads lower than 16 writers.

| Writers | 4 namespaces, all in every group | disjoint pairs (a+b, c+d) |
|---:|---:|---:|
| 16 | 5,943 ops/s | 7,636 ops/s |
| 64 | 8,739 ops/s | **19,458 ops/s** |

| 16 writers on `a`, beside 16 background writers doing… | `a`'s throughput | `a`'s p50 |
|---|---:|---:|
| nothing | 7,934 ops/s | 0.99 ms |
| ordinary commits to `b` | 13,162 | 1.02 ms |
| groups over `b`+`c` | 11,987 | 1.19 ms |
| groups over `a`+`b` | **5,051** | 2.25 ms |

What changes from macOS:

- **12.8× over the simple protocol** at 64 writers, and the protocol now scales past what the
  device allows on macOS. At 64 writers an atomic cross-namespace commit (15,791 ops/s) outruns
  two *non-atomic* commits (11,894 ops/s), because it is one round trip where they are two.
- **Disjoint groups scale** (19,458 vs 15,791 ops/s for one pair): with per-file flushes, groups
  that share no namespace no longer contend on a device-wide drain.
- **Groups on other namespaces cost their neighbours nothing.** The `groups over b+c` row sits
  with the control row, not below it. (The `nothing` row reads *lower* than the control; that is
  VM noise, and it is the first sub-benchmark to run.)
- **Sharing a namespace with groups does cost**, and more than on macOS: 5,051 against ~12,000
  ops/s. That is ack chaining, and it is inherent. A commit queued behind an undecided group in
  its namespace waits for that group's decision, a second fsync round, so its latency roughly
  doubles (2.25 ms against ~1.1 ms) and 16 latency-bound writers halve their throughput. It
  applies only to namespaces that cross-namespace transactions actually touch.
- Single-writer latency: 0.66 ms for an atomic two-namespace commit, against 0.49 ms for two
  non-atomic ones: one extra flush, the decision record.

## 6. Single-namespace commits: no change

The requirement: a transaction that touches one namespace must cost exactly what it did before.
The comparison is `main` (`a2eb827`) against the full stack (#67–#73, including the allocation
fix below). Both test binaries were built from their own tree. Five rounds interleave `main` and
the stack, so machine drift lands on both, and load was recorded before every run
(`benchstat`, n=5).

```bash
# per tree: go test -c -o <side>-server.test ./kdb/server; go test -c -o <side>-transaction.test ./kdb/transaction
<side>-server.test -test.bench '^(BenchmarkWorkload(WriteInsert|Update|Transaction|Read)|BenchmarkFileBackedUpsert|BenchmarkWireDocumentGet|BenchmarkWriteGateOverheadOnly)$' -test.benchtime 1s -test.benchmem
<side>-transaction.test -test.bench '^(BenchmarkCommitConcurrent_DisjointDocs|BenchmarkCommitBytesPerOp)$' -test.benchtime 1s -test.benchmem
```

**macOS** (load 3.9–6.0, the `zolik-server` core as above):

| | `main` → stack |
|---|---|
| Time per op, geomean over 19 server benchmarks | **+0.01%** |
| Any write, update, transaction or upsert benchmark (single-user and 16/64-way) | no significant change (p ≥ 0.095 on every one) |
| Reads | ±2–4% in both directions (−1.9% on one, +4.0% on another): noise on a path this change does not touch |
| Engine commit latency (`CommitBytesPerOp`, `CommitConcurrent_DisjointDocs`) | no significant change; geomean +0.4% |
| Allocations per commit | **identical** after the fix below (92 in the engine; 157 / 159 / 191 / 163 in the server workloads) |

**Linux** (the container from §5; load 1.3 → 6.5 over the run, identical for both sides):

| | `main` → stack |
|---|---|
| Time per op, geomean over 19 server benchmarks | **−0.69%** |
| Any write, update, transaction or upsert benchmark | no significant change (p ≥ 0.056 on every one) |
| Allocations and bytes per op, server and engine | **identical** (geomean +0.00% allocs, +0.01% bytes) |
| Engine commit latency | no significant change; geomean +0.7% |
| Significant differences | two read benchmarks got *faster* (−4.4%, −8.2%); noise on an untouched path |

**The first run found one real cost.** The Prepare/Apply split made the engine return
`*PreparedCommit`, so every ordinary commit heap-allocated the whole plan: +1 allocation and about
450 B, which is +5–7% bytes per op on small commits. Time did not move, but it was a
single-namespace cost the requirement rules out. `plan` now returns the prepared commit by value.
The ordinary commit applies it on the stack, and only the cross-namespace entry point takes its
address. After the fix, allocations match `main` exactly and bytes per op are within noise
(13,015 vs 13,009 B/op at 512-byte payloads).

A note on one benchmark: `BenchmarkCommitConcurrent_DisjointDocs` drives the engine from 16
goroutines with no write gate, and fails intermittently on `main` too (3 of 12 runs; the stack
failed 1 of 12). It is not a regression. It is that benchmark sharing an in-memory store
unsynchronized. On Linux it failed in 3 sub-benchmarks on `main` and 1 on the stack.

## 7. Correctness under the same load

Not a benchmark, but measured alongside:
`TestConcurrentTransfersPreserveTheTotal` (`-race`) moved money between 9 accounts in 3
namespaces with 8 writers, retrying conflicts. It committed 320 transfers with 163–503 conflicts
retried, and took 558–36,662 consistent snapshots. No snapshot, and not the final state, ever saw
the total change. With the snapshot's publication check removed (a mutation test), the same test
fails on every run.
