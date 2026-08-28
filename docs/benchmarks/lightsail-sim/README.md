# Lightsail-tier simulation (local, Docker)

Approximates AWS Lightsail's **$7/mo tier (1GB RAM, 2 vCPU)** — the target tier component 38's
gap analysis identified (`docs/kdb-spec-layer12-component38-go-native-server.md` §9) — locally via
Docker resource limits, and drives sustained small-message read/write load against Component 38's
Go-native `kdb-service` over its real wire protocol.

**Status: the OOM this harness first surfaced is fixed and hardened against** (2026-08-25;
re-verified 2026-08-27 after group commit, and extended to file-backed mode). See "The fix" below
for the root causes and the resolution, the 2026-08-27 update for the current sweep in both
storage modes, and "Original findings" for the record of what was found and how.
**This is still an approximation, not the real thing** — Component 38 spec §7 test 8 explicitly
calls for running "on hardware/VM specs matching the proposed $7/mo tier, not a developer
laptop":
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
- `mem-sweep.sh` — sweeps `--memory-limit-mb` across a list of percentages in both storage
  modes (`--memory` and `--data-dir`), scoring each run on whether the kernel OOM-killed it.
  `run.sh` cannot do this: it always passes `--memory` and takes no `--memory-limit-mb`. This is
  what produced the 2026-08-27 table below.
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

To sweep that setting rather than pick one, use `mem-sweep.sh`, which reproduces the
2026-08-27 table below:

```bash
./docs/benchmarks/lightsail-sim/mem-sweep.sh
```

```bash
MODES=file PERCENTS="60 80 90 95" DURATION=120s REPEATS=3 \
  ./docs/benchmarks/lightsail-sim/mem-sweep.sh
```

Note that `MODES=mem` (the default `run.sh` behavior) exercises the memory guard against commit
DAG growth but never touches the delta log or group commit; `MODES=file` is the one that commits
through the real write path. The 2026-08-27 update explains why that distinction matters when
reading any number in this file.

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

  **Important tuning note, found empirically, not assumed**: the guard originally sampled
  `runtime.Sys`, not `HeapAlloc` - `HeapAlloc` (live heap) can drop back to near zero within one
  GC cycle, but Go does not eagerly return freed pages to the OS, so the process's actual
  OS/cgroup-visible footprint tracked `Sys`, which stayed elevated far longer. Gating on
  `HeapAlloc` was tried first and measured to let the process keep accepting writes for a long
  stretch after `Sys` had already climbed past the real container limit - it still got OOM-killed
  with the "fix" in place. Even gating on `Sys`, sustained heavy *read* traffic alone (never
  blocked by this guard) could push usage a meaningful amount past the configured reject
  threshold before it plateaued - an 80-90%-of-container-limit configuration left the process
  climbing to 93%+ over several minutes under an extreme, unrealistic stress load (millions of
  ops in 3.5 minutes); a 60%-of-limit configuration plateaued safely at 67% under the identical
  load. **This is superseded below** - `Sys` turned out to have a second, worse problem (it never
  decreases, so the guard latched permanently once tripped), fixed in kdb-spec-layer13 Component
  48; see "Update" for the current guidance.

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

## Update (2026-08-25): re-verified after kdb-spec-layer13 Component 48's accounting fix

The `MemoryGuard` used above sampled `runtime.MemStats.Sys`, which **never decreases** - Go
returns freed pages to the OS but keeps counting them in `Sys` (they move into `HeapReleased`
instead). Once a real workload tripped the guard, it stayed tripped for the rest of the process's
life even after GC freed everything that had pushed it over: a zombie that kept answering reads
while permanently refusing writes. That latch, not read traffic alone, is the real reason 60%
was the only safe number found above - see `docs/kdb-spec-layer13-resource-governance.md` §2.5.

Commit `7ad882a` fixed this: the guard now measures the Linux cgroup's own `memory.current` (the
exact figure `--memory` is enforced against) where available, falling back to
`runtime/metrics`'s `/memory/classes/total:bytes` minus `/memory/classes/heap/released:bytes` -
both actually decrease when memory is freed - plus real hysteresis (a separate, lower clear
threshold), so pressure that genuinely subsides releases the latch instead of requiring a
`--memory-limit-mb 0` reconfigure.

Re-ran the same 2000-document pool / 16-way concurrency / 1GB / 2-vCPU container against the
fixed guard, sweeping `--memory-limit-mb` from 60% up through 95% of the container's 1GB limit
(the reject threshold itself is still a fixed 85% of whatever budget is configured - see
`SetMemoryLimit(limitBytes, 0.85)` in `go/cmd/kdb-service/main.go`):

| `--memory-limit-mb` (% of 1GB) | duration | total ops | throughput | outcome | peak memory |
|---:|---:|---:|---:|---|---:|
| 614 (60%) | 60s | 1,203,989 | 20,066 ops/sec | **survived** | 67.4% |
| 819 (80%) | 60s | 1,204,022 | 20,067 ops/sec | **survived** | 92.9% |
| 922 (90%) | 60s | 144,628 | 2,410 ops/sec | **OOM-killed** | - |
| 973 (95%) | 60s | 154,857 | 2,581 ops/sec | **OOM-killed** | - |

(the loadtest client's "duration" is now a real wall-clock deadline regardless of server health -
see the `kdb-loadtest` duration-tracking fix below - so a 60s row with a low op count means the
server stopped making progress well before the window closed and stayed down, not that the test
ended early. The 90%/95% throughput, ~8x lower than the healthy configs' ~20,066 ops/sec, is
consistent with dying within the first several seconds and staying dead for the rest of the
window, though the exact death time wasn't captured directly.)

**80% is now genuinely safe** - previously this same configuration climbed to 93%+ and
eventually died (see the pre-fix table above); it now plateaus at 92.9% and runs the full
duration at throughput identical to the 60% configuration. The accounting + hysteresis fix raised
the safe ceiling from ~60% to ~80% of the container's actual limit, with no throughput cost.

**90% and 95% still OOM-kill, and now die fast** - within the first few seconds rather than near
the end of a long run. With the reject threshold pinned at 85% of the configured budget, a
90%/95% budget puts the trip point (~78-83% of the container) close enough to the real 100%
ceiling that a burst of already-admitted in-flight writes between the guard's 200ms samples - or
just the process's
baseline overhead (connections, goroutine stacks, the DAG itself) - is enough to blow past it
before the next sample can react. This is the gap kdb-spec-layer13 Component 48's admission
model (reserve memory *before* admitting work, rather than reject *after* usage crosses a
threshold) and its graduated Normal/Elevated/High/Critical zones are designed to close; a single
fixed trip fraction is not the end state.

**Updated recommendation: set `--memory-limit-mb` to 60-80% of the container's actual `--memory`
limit.** 80% is now the better default where every byte of capacity matters (identical
throughput to 60%, more write budget before backpressure kicks in); 60% remains the safer choice
if you'd rather have more margin than a fixed 85%-of-budget trip fraction can guarantee. Do not
configure above 80% until Component 48's full admission control (reserve-before-admit, graduated
zones) lands.

This re-verification also caught and fixed an unrelated bug in the load generator itself:
`go/cmd/kdb-loadtest`'s drain loop recreated a 20ms idle timer every iteration to poll for the
requested `-duration` elapsing, so under sustained high throughput (the results channel almost
always has something waiting) that timer never won the select before being replaced - a healthy
server would run for however long the outer safety-margin timeout allowed instead of the
requested duration. Fixed by using a single timer armed once at the start of the measured window
(commit `984d429`); verified a requested `-duration 15s` run against a busy server now measures
exactly 15s.

### Zero JVM processes, still true

```
PID   USER     TIME  COMMAND
    1 kdb       0:00 /usr/local/bin/kdb-service --memory --sql-addr tcp://0.0.0.0:9090?bind=true ...
```

One process, the Go binary itself - the literal claim Component 38 exists to make true (test 10
in the component spec).

## Remaining follow-ups

1. ~~Re-run against **file-backed** (`--data-dir`) mode~~ - **done 2026-08-27**, see the update
   below. Worth reading before trusting the numbers above as general: the `--memory` mode every
   table above uses never touches the delta log, so those rows measure the memory guard, not the
   write path.
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

## Update (2026-08-27): re-verified after group commit, and a file-backed column

Re-ran the sweep above against `e2bbc82` (which includes `456c673`'s encoder-allocation fix,
`8fe306d`'s delta-log group commit, and `01d0654`'s storage-correctness follow-ups) to check
whether the write-path work moved the safe ceiling.

**It did not, and the reason is worth writing down: the sweep above never exercised the write
path that changed.** `run.sh` starts the container with `--memory`, which builds the runtime from
`NewInMemoryCommitDag` + `InMemoryStorageAdapter` (`go/kdb/embed/memory.go`). That path never
reaches `PersistingCommitDAG`, the delta log, or the group committer at all. Every number in the
2026-08-25 table is measuring the memory guard against unbounded DAG growth, not the write path.
So the sweep is repeated below in both modes: `mem` for continuity with the recorded numbers, and
`file` (`--data-dir`, no `--memory`) for the configuration that actually commits through the
delta log.

Same 2000-document pool / 16-way concurrency / 0.5 read ratio / 1GB / 2-vCPU container / 60s.
Each row was run twice, on separately built images ~40 minutes apart; both values are shown where
they differ.

| mode | `--memory-limit-mb` (% of 1GB) | total ops | throughput | outcome | peak memory |
|---|---:|---:|---:|---|---:|
| mem | 819 (80%) | 1,050,595 / 1,034,494 | 17,510 / 17,242 ops/sec | **survived** | 92.6% / 90.0% |
| mem | 922 (90%) | 147,356 / 142,596 | 2,456 / 2,377 ops/sec | **OOM-killed** (`exitCode=137`) | 88.7% / 98.3% (last sample before the kill) |
| file | 819 (80%) | 949,546 / 1,044,982 | 15,826 / 17,416 ops/sec | **survived** | 86.1% / 83.6% |
| file | 922 (90%) | 1,017,602 / 939,456 | 16,960 / 15,658 ops/sec | survived | 98.7% / 98.4% |

**The `mem` rows reproduce the 2026-08-25 outcome.** 80% survives; 90% still OOM-kills, at
2,377-2,456 ops/sec against the recorded 2,410 - the same ~7x-degraded throughput signature of
dying early and staying dead. The boundary has not moved, which is the expected outcome given
none of the write-path commits touch that code path. (Healthy-run throughput is ~14% below the
recorded 20,067 ops/sec - 17,242-17,510 here - because this machine was not idle; the
methodology note at the end of this section applies. The survive/die outcome is unaffected.)

**The `file` rows are new, and 90% surviving there is not evidence of a raised ceiling.** Peak
memory lands at 98.4-98.7% of the container - margin-of-noise from the real ceiling, not
headroom. Three additional file-backed runs at 90% all survived but show how thin that margin is:

| run | total ops | throughput | peak memory |
|---:|---:|---:|---:|
| 1 | 897,719 | 14,962 ops/sec | 95.9% |
| 2 | 830,018 | 13,833 ops/sec | 91.4% |
| 3 | 330,980 | 5,516 ops/sec | 99.4% |

Run 3 lost ~63% of its throughput while sitting at 99.4%. That is the same pre-death shape the
`mem` 90% rows show, caught just short of the kill. **The 60-80% recommendation above stands
unchanged for both modes**; do not read the file-backed 90% rows as permission to configure
above 80%.

### Group commit is what makes the file-backed path able to reach pressure at all

The same file-backed 90% configuration run against `29300d5` (the commit before the encoder fix
and group commit) survives too - but for the opposite reason:

| build | total ops | throughput | peak memory | why it survived |
|---|---:|---:|---:|---|
| `29300d5` (pre-group-commit) | 36,280 / 39,535 / 2,682 | 603 / 658 / 44 ops/sec | 39.0% / 41.2% / 7.1% | too slow to accumulate |
| `e2bbc82` (current) | 897,719 / 830,018 / 330,980 | 14,962 / 13,833 / 5,516 ops/sec | 95.9% / 91.4% / 99.4% | guard held it |

At one physical fsync per commit the old build manages ~600 ops/sec and never gets within 60% of
the container limit in a 60-second window - there is no memory pressure to survive. Group commit
raises file-backed throughput by roughly 20-25x, which is precisely what puts this configuration
in contact with the memory ceiling for the first time. The write-path work did not weaken the guard; it made
the file-backed path fast enough for the guard to matter.

### Write-path benchmark re-verification

`BenchmarkFileBackedUpsert` (`go/kdb/server/write_path_bench_test.go`), interleaved A/B against
`8eaaf1d` (the write-path merge, before the storage-correctness follow-ups), 3 alternating rounds
of `-benchtime=3s` on one machine, to confirm `01d0654` cost nothing on the write path:

| parallelism | `e2bbc82` (current) | `8eaaf1d` (before follow-ups) |
|---:|---|---|
| 1 | 531 / 702 / 664 µs | 666 / 703 / 667 µs |
| 8 | 83 / 109 / 97 µs | 130 / 101 / 101 µs |
| 64 | 34.8 / 39.0 / 35.2 µs | 39.2 / 53.0 / 34.9 µs |

Equivalent within run-to-run variance, and `192 allocs/op` / ~15KB `B/op` held in every single
run on both builds. On an otherwise-idle machine the same benchmark reports 506 µs / 75 µs /
34 µs, matching the figures in `docs/benchmarks/write-path-allocation-fix.md`.

**Methodology note for whoever re-runs this.** These benchmarks are extremely sensitive to
competing load. An early pass in this session, taken while an unrelated Go benchmark was running
on the same machine (load average ~14), reported parallel-8 at 7-11 ms/op - a ~100x apparent
regression that does not exist, and that briefly looked like a real finding. The inflated
`B/op` (24-69KB against a true ~15KB) was the tell. Interleave the two builds in the same session
and check `allocs/op` for stability before believing any ns/op delta here.

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
