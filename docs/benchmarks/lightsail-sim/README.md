# Lightsail-tier simulation (local, Docker)

Approximates AWS Lightsail's **$7/mo tier (1GB RAM, 2 vCPU)** — the target tier component 38's
gap analysis identified (`docs/kdb-spec-layer12-component38-go-native-server.md` §9) — locally via
Docker resource limits, and drives sustained small-message read/write load against Component 38's
Go-native `kdb-service` over its real wire protocol.

**This is an approximation, not the real thing.** Component 38 spec §7 test 8 explicitly calls for
running "on hardware/VM specs matching the proposed $7/mo tier, not a developer laptop" — Docker's
`--memory`/`--cpus` flags give a real cgroup-enforced ceiling (the OOM kill below is a genuine
kernel action, not simulated), but:
- This machine is Apple Silicon (arm64); real Lightsail instances are x86_64. Absolute throughput
  numbers are not directly comparable.
- Local SSD/APFS I/O characteristics differ from Lightsail's underlying storage.
- Network is loopback (0ms RTT); a real client would see real internet latency on top of every
  number below.
- Docker Desktop's own VM overhead on macOS adds some unknown tax vs. bare Linux.

Treat every number here as "does this even survive the resource envelope, and roughly what does
degradation look like" — not as an authoritative capacity-planning figure. The real test 8 still
needs to run on actual Lightsail hardware before the "$7/mo tier" claim is something to bill
against.

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
document by id"), not unbounded inserts. See **Finding 1** below for why the pool being bounded
turned out to matter a great deal.

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

## Results (2026-08-25, Apple M-series arm64, Docker Desktop 4.75, macOS)

### Finding 1 — the 1GB tier OOM-kills under moderate write load once the dataset passes roughly a few hundred documents, well before hitting the traffic volumes that would make the cost claim interesting

This is the headline result, not a footnote. Every run below used `--memory` mode (in-memory
runtime, the simplest case — no disk I/O involved at all).

| doc pool | concurrency | writes completed before death | measured throughput before death | outcome |
|---:|---:|---:|---:|---|
| 2000 | 16 | ~3,700 | n/a (crashed) | **OOM-killed** (`exitCode=137`, `OOMKilled=true`) |
| 2000 | 8  | ~1,900 | n/a (crashed) | **OOM-killed** |
| 2000 | 1  | ~1,800 | n/a (crashed) | **OOM-killed** |
| 500  | 4  | 5,748 | 1,938 ops/sec (mixed) | **OOM-killed** (right at the end of a 10s window) |
| 100  | 2  | 5,787 | 2,376 ops/sec (mixed) | survived cleanly, 27% memory used |

Reading this: it is **not** simply "total write volume" that predicts the crash — the 100-document
run completed *more* total writes than any of the crashed runs without dying. It correlates with
**how many distinct documents exist in the namespace**, not how many times they're updated or how
many workers are hammering it concurrently (concurrency 1, 8, and 16 all died at a similar
document-pool size). `docker stats`' 1-second sampling never showed memory anywhere near the 1GB
ceiling before each kill (typically 140–340MiB on the last sample), meaning the actual growth is a
sharp spike between samples, not a visible slow climb — consistent with some per-commit or
per-document operation whose cost scales with total document count rather than being truly
incremental, despite `docs/benchmarks/phases-1-6-summary.md`'s existing notes about `DocumentTree`
being an O(delta) persistent trie. This needs a real heap-profiling investigation (`pprof` inside
the container) to root-cause - not attempted here, flagged as a follow-up.

**Practical read**: as shipped today, the Go-native server's `--memory` mode cannot be trusted to
run unattended on a 1GB instance for any workload that grows past a small number of distinct
documents, regardless of traffic volume. This doesn't necessarily kill the "$7/mo tier" cost claim
(file-backed mode, or a periodic-restart/compaction strategy, might behave differently - not yet
tested), but it means the claim isn't validated yet either, and the failure mode is silent
process death (`SIGKILL`, `exitCode=137`) rather than a graceful degradation - it looks in the logs
like the process simply vanished mid-request, not like it warned it was running low.

### Finding 2 — sustained latency and throughput, below the OOM threshold

The one clean (non-crashed) run, for a rough sense of shape: 100-document pool, concurrency 2,
8-second measured window, 70/30 read/write mix.

```
total ops: 19,063 (writes=5,787 reads=13,276 errors=0)
throughput: 2,376.0 ops/sec
write latency: p50=1.50ms p95=4.03ms p99=6.21ms max=23.42ms
read latency:  p50=0.18ms p95=0.58ms p99=1.49ms max=17.92ms
```

Reads are roughly 8x cheaper than writes at p50, as expected (writes commit through the full
transaction/DAG-append path; reads are a point lookup at the current head). This single data point
is not enough to characterize scaling behavior (need to sweep concurrency and confirm the number
holds under longer runs once Finding 1 is resolved) - reported here as a baseline, not a ceiling.

### Confirmed: zero JVM processes

```
PID   USER     TIME  COMMAND
    1 kdb       0:00 /usr/local/bin/kdb-service --memory --sql-addr tcp://0.0.0.0:9090?bind=true ...
```

One process, the Go binary itself - the literal claim Component 38 exists to make true (test 10 in
the component spec).

## Suggested follow-ups

1. **Root-cause Finding 1** with `pprof` heap profiling inside the container (add
   `net/http/pprof` behind a flag, or use `go tool pprof` against a core dump captured right
   before an engineered near-OOM). Prime suspects given the "scales with document count, not
   operation count" shape: something in the index layer or `DocumentTree` build path that isn't
   as incremental as intended once the tree is large enough, or an unbounded cache/registry keyed
   by document id.
2. Re-run this same harness against **file-backed** (`--data-dir`) mode once (1) is understood -
   might behave differently (or might not, if the growth is in an in-memory structure the delta
   log doesn't relieve).
3. Once (1) is fixed, re-run at realistic Zolik traffic volumes/durations (minutes, not seconds)
   to get an actual capacity number for the $7/mo tier claim.
4. The real Lightsail run (component 38 spec §7 test 8) still needs to happen on real x86_64
   Lightsail hardware before the cost claim is billable - this harness narrows what to expect,
   it doesn't replace that run.
