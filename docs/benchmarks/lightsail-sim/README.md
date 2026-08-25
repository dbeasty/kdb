# Lightsail-tier simulation (local, Docker)

Approximates AWS Lightsail's **$7/mo tier (1GB RAM, 2 vCPU)** — the target tier component 38's
gap analysis identified (`docs/kdb-spec-layer12-component38-go-native-server.md` §9) — locally via
Docker resource limits, and drives sustained small-message read/write load against Component 38's
Go-native `kdb-service` over its real wire protocol.

**Status: the OOM this harness first surfaced is fixed and hardened against** (2026-08-25). See
"The fix" below for the root causes and the resolution; "Original findings" is kept for the
record of what was found and how. **This is still an approximation, not the real thing** —
Component 38 spec §7 test 8 explicitly calls for running "on hardware/VM specs matching the
proposed $7/mo tier, not a developer laptop":
- This machine is Apple Silicon (arm64); real Lightsail instances are x86_64. Absolute throughput
  numbers are not directly comparable.
- Local SSD/APFS I/O characteristics differ from Lightsail's underlying storage.
- Network is loopback (0ms RTT); a real client would see real internet latency on top of every
  number below.
- Docker Desktop's own VM overhead on macOS adds some unknown tax vs. bare Linux.

Docker's `--memory`/`--cpus` flags do give a real cgroup-enforced ceiling (every OOM kill below is
a genuine kernel action, not simulated), so the *shape* of what was found here is trustworthy even
though the absolute numbers aren't a capacity-planning figure. The real test 8 still needs to run
on actual Lightsail hardware before the "$7/mo tier" claim is something to bill against.

## What's here

- `Dockerfile` — builds `go/cmd/kdb-service` into a minimal Alpine image (no JVM anywhere - the
  literal claim Component 38 exists to make true; verified by `docker exec ... ps aux` inside the
  container).
- `run.sh` — builds the image, starts it with `--memory=1g --memory-swap=1g --cpus=2`, waits for
  it to accept connections, runs the load generator from the host (unconstrained, so it isn't
  competing with the server for the same limited CPU/memory budget), samples `docker stats` every
  second during the run, and reports the container's exit status (including OOM-kill detection)
  and last-N log lines regardless of whether the server survived.
- `go/cmd/kdb-loadtest` (lives in the Go module proper, not here, since it needs to import
  `go/kdb/client`) — pre-populates a fixed pool of documents, then has every worker repeatedly
  `GetJSON`/`Upsert` a random document from that pool over real TCP connections
  (`go/kdb/client`, the same path a real Zolik client uses), reporting throughput and p50/p95/p99
  latency for a measured window (a warmup period is discarded first).

Bounded document pool, not an ever-growing insert stream, deliberately: `go/kdb/client`'s own
package doc comment describes Zolik's actual usage as a repository pattern ("read or write one
document by id"), not unbounded inserts.

## Running it

```bash
./docs/benchmarks/lightsail-sim/run.sh
```

Tunables via environment variables (defaults shown):

```bash
PORT=19090 MEMORY=1g CPUS=2 DURATION=30s CONCURRENCY=16 READ_RATIO=0.5 \
  ./docs/benchmarks/lightsail-sim/run.sh
```

`go/cmd/kdb-loadtest` also has its own flags (`-doc-pool`, `-doc-bytes`, `-warmup`) not exposed
through `run.sh`'s env vars — run it directly against an already-running container for finer
control, e.g.:

```bash
go run ./go/cmd/kdb-loadtest -addr 127.0.0.1:19090 -doc-pool 300 -concurrency 4 -duration 15s
```

To exercise the memory-pressure hardening (see below), pass `--memory-limit-mb` to `kdb-service`
when starting the container, e.g. `--memory-limit-mb 600` for a 1GB container.

## The fix (2026-08-25)

The original run (see "Original findings" below) found `kdb-service --memory` getting
OOM-killed under moderate write load once the namespace passed roughly a few hundred documents,
regardless of total operation volume or concurrency. Root-caused with local `pprof` profiling
(no Docker needed for this part - see `go tool pprof -alloc_space`) against a small
in-process repro, then verified end to end back in this harness. Two real, independent
allocation bugs, plus hardening once both were fixed still couldn't make the OOM impossible in
principle:

**1. `findExistingCommit`'s idempotent-retry check walked history on every single commit (~88%
of all allocation in the profiled run).** Both the Go (`go/kdb/transaction/default_engine.go`)
and Kotlin (`kdb-transaction/DefaultTransactionEngine.kt`) transaction engines call
`findExistingCommit` before every `Commit`/`Replay`, to detect "this exact transaction was
already committed, return that result instead of creating a duplicate" (needed for safe retries
after e.g. a dropped response). It did this by walking up to 8192 commits of DAG history and
copying each one's full record (including patch JSON) into a fresh slice, on *every* call,
whether or not a retry was actually happening. Cost scaled with total commit history, and it also
had a latent correctness bug: a retry of a transaction more than 8192 commits old would silently
stop being recognized, creating a duplicate commit instead of the idempotent no-op it should be.

  Fixed by adding a transaction-id index to the commit DAG itself
  (`InMemoryCommitDag.GetCommitByTransactionID` / `getCommitByTransactionId`, populated
  incrementally as commits land) so the lookup is O(1) and correct regardless of history length.
  Ported to both Go and Kotlin, since both shared the identical pattern.

**2. `DocumentTree.With`/`Without` eagerly copied the full flat entries map on every write
(~11% of allocation, but the *only* remaining scaling factor once (1) was fixed).** This was a
known, previously-accepted tradeoff (see `document_tree_trie.go`'s own comment and
`docs/benchmarks/phases-1-6-summary.md`'s Phase 3 notes) - the tree hash itself was already
O(delta) via a persistent trie, but the flat `map[UUID]Hash` used for fast lookups was still
copied in full and permanently retained (never evicted) on every single commit, so cost and
retained memory both grew with total document count.

  Fixed by making `Entries` lazy: `With`/`Without` now return a trie-backed tree with `Entries ==
  nil`; `Contains`/`HashFor`/`Size` all work directly against the trie (a new O(1) `trieGet`,
  plus incremental size tracking) without ever materializing the map. A new
  `MaterializedEntries()` method builds the full map on demand for the few genuinely-full-scan
  callers that need it (DAG diff, namespace scans) - never on the per-write hot path.

  `BenchmarkCommitScalingWithHistorySize` (`go/kdb/transaction`) guards this directly: commit
  latency at a 10-commit history vs. an 8,000-commit history, same document pool.

  | | before either fix | after fix 1 only | after both fixes |
  |---|---:|---:|---:|
  | history=10 | 23.8µs/op | 23.8µs/op | 30.4µs/op |
  | history=1000 | (not measured) | 51.3µs/op | 22.1µs/op |
  | history=8000 | 234.8µs/op | 234.8µs/op | 21.2µs/op |

  (history=10 and 8000 pre-fix numbers from the same benchmark; the 234.8µs value is what an
  unfixed 8000-deep history costs per commit - a ~10x scaling factor that both fixes together
  eliminate entirely, leaving commit cost flat regardless of history size.)

**3. Hardening: memory-pressure backpressure, because (1) and (2) narrow the problem but cannot
eliminate it in principle.** An in-memory, uncompacted commit DAG grows without bound *by
design* - every write is a permanent new commit, nothing evicts history. Confirmed empirically:
even with both allocation fixes, an unthrottled 1GB container under sustained 16-way write/read
load for 60s still eventually got OOM-killed (212,535 ops survived first, a ~18x improvement
over the original ~1,800–3,700, but still eventually killed).

  Added `go/kdb/server.MemoryGuard`: a background sampler (every 200ms) that trips a cheap,
  lock-free flag once process memory crosses a configured fraction of a configured budget: new
  `Commit`/`Upsert` calls are then rejected immediately with a clear `*MemoryPressureError`
  ("server is near its configured memory budget, retry later") instead of being accepted and
  risking the process getting SIGKILLed with no warning to the client. Reads are **not** gated -
  the server keeps serving existing data even while shedding write load. Wired into
  `KdbServerRuntime.SetMemoryLimit(limitBytes, rejectFraction)`, exposed via `kdb-service`'s new
  `--memory-limit-mb` flag.

  **Important tuning note, found empirically, not assumed**: the guard samples `runtime.Sys`, not
  `HeapAlloc` - `HeapAlloc` (live heap) can drop back to near zero within one GC cycle, but Go
  does not eagerly return freed pages to the OS, so the process's actual OS/cgroup-visible
  footprint tracks `Sys`, which stays elevated far longer. Gating on `HeapAlloc` was tried first
  and measured to let the process keep accepting writes for a long stretch after `Sys` had
  already climbed past the real container limit - it still got OOM-killed with the "fix" in
  place. Even gating on `Sys`, sustained heavy *read* traffic alone (never blocked by this guard)
  can push usage a meaningful amount past the configured reject threshold before it plateaus - an
  80-90%-of-container-limit configuration left the process climbing to 93%+ over several minutes
  under an extreme, unrealistic stress load (millions of ops in 3.5 minutes); a 60%-of-limit
  configuration plateaued safely at 67% under the identical load. **Recommendation: set
  `--memory-limit-mb` to roughly 60% of the container's actual `--memory` limit**, not 80-90% as
  might seem intuitive - the flag's own help text carries this guidance.

### Verification

Same 2000-document pool, same 16-way concurrency, same 1GB/2-vCPU container that reliably
OOM-killed the server within seconds before any of this:

| configuration | duration | total ops | outcome |
|---|---:|---:|---|
| pre-fix | 30s | ~1,800–3,700 (varies by concurrency) | **OOM-killed** every time |
| both algorithmic fixes, no memory limit | 60s | 212,535 | **OOM-killed** (eventually) |
| both fixes + `--memory-limit-mb 800` (80% of container) | 90s | 350,022 | **OOM-killed** (eventually, 93%+ climb) |
| both fixes + `--memory-limit-mb 600` (60% of container) | 210s | 5,741,150 | **survived**, plateaued at 67% memory |

At the safe configuration: 5.7 million operations (24,688 writes actually landed, 2,424,515
writes cleanly rejected under pressure, 5,716,462 reads served throughout) over 3.5 minutes,
process still running, `OOMKilled=false`, memory usage stable at 690.7MiB/1GiB.

### Zero JVM processes, still true

```
PID   USER     TIME  COMMAND
    1 kdb       0:00 /usr/local/bin/kdb-service --memory --sql-addr tcp://0.0.0.0:9090?bind=true ...
```

One process, the Go binary itself - the literal claim Component 38 exists to make true (test 10
in the component spec).

## Remaining follow-ups

1. Re-run against **file-backed** (`--data-dir`) mode - the fixes above apply equally (same
   `InMemoryCommitDag`/`DocumentTree` underneath), but not yet explicitly re-verified there.
2. `insertHex`'s O(n) slice-shift per commit (`go/kdb/dag/in_memory_commit_dag.go`, backing hash
   prefix lookup) is a real, still-unfixed O(n) *CPU* cost per commit - it did not show up as a
   significant *allocation* contributor in profiling (unlike the two fixes above), so it wasn't
   prioritized, but it will make commit latency creep up with history size on long-lived
   deployments. Worth a follow-up if that becomes measurable.
3. An insert-heavy, ever-growing-document-count workload (as opposed to this harness's bounded
   repository-pattern pool) is a different question from what got fixed here - DAG compaction
   (Component 19) exists but isn't wired into `kdb-service` automatically. Not exercised by this
   harness; worth its own pass if Zolik's real workload shape turns out to be insert-heavy.
4. The real Lightsail run (component 38 spec §7 test 8) still needs to happen on real x86_64
   Lightsail hardware before the cost claim is billable - this harness narrows what to expect and
   confirms the server can now run unattended under sustained load without a proper memory-limit
   configuration, but it doesn't replace that run.

## Original findings (2026-08-25, before the fix above)

Kept for the record. Every run below used `--memory` mode (in-memory runtime, no disk I/O
involved at all), on Apple M-series arm64 / Docker Desktop 4.75 / macOS.

| doc pool | concurrency | writes completed before death | measured throughput before death | outcome |
|---:|---:|---:|---:|---|
| 2000 | 16 | ~3,700 | n/a (crashed) | **OOM-killed** (`exitCode=137`, `OOMKilled=true`) |
| 2000 | 8  | ~1,900 | n/a (crashed) | **OOM-killed** |
| 2000 | 1  | ~1,800 | n/a (crashed) | **OOM-killed** |
| 500  | 4  | 5,748 | 1,938 ops/sec (mixed) | **OOM-killed** (right at the end of a 10s window) |
| 100  | 2  | 5,787 | 2,376 ops/sec (mixed) | survived cleanly, 27% memory used |

It was not simply "total write volume" that predicted the crash — the 100-document run completed
*more* total writes than any of the crashed runs without dying. It correlated with how many
distinct documents existed in the namespace, not how many times they were updated or how many
workers were hammering it concurrently — matching root cause #2 above (`DocumentTree`'s per-write
cost scaled with document count) compounding with root cause #1 (cost scaled with total commit
history, which also grows with document count in a bounded-pool workload once every document has
been touched at least once).
