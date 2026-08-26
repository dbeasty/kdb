# KDB Component Spec — Layer 13
## Resource Governance, Crash-Only Durability, and Federated Backpressure
### Components 47–52

**File:** `kdb-spec-layer13-resource-governance.md`
**Layer:** 13 — Runtime Resource Governance
**Status:** Design — implementation-ready per component
**Modules:** `go/kdb/server`, `go/kdb/storage/{delta,io,engine}`, `go/kdb/embed`, `go/kdb/wire`, `go/kdb/client`, `go/kdb/peersync` (+ Kotlin ports where noted)
**Depends on:** Layer 4a (WAL, delta writer, platform IO), Layer 7 (wire), Layer 8 (peer sync), Layer 12 (Component 38 Go-native server)

-----

## 1. Purpose

Layer 12's hardening pass (`docs/benchmarks/lightsail-sim/README.md`) stopped `kdb-service` from
being OOM-killed under sustained load by adding `MemoryGuard`: a background sampler that trips a
flag and rejects writes. That was the right emergency brake, but it is not a resource-management
policy. It is reactive (it discovers pressure after the fact), binary (all writes or none), blind
to operation cost (a 10-byte upsert and a 16MB frame are treated identically), silent about CPU,
and — critically — **one-way**: it can enter a permanently-rejecting state that no amount of
recovered memory clears.

This layer replaces that brake with an admission-control system built on one policy:

> **Start only what we can finish.** An operation is admitted only if the resources needed to
> complete it — memory and CPU — are available at admission time and are reserved for its
> duration. Work that cannot be admitted within its deadline is rejected immediately, with a
> typed, actionable error, before it has allocated anything.

Three constraints shape the design, all of them operator requirements rather than derived goals:

1. **No zombie service.** A process that cannot make progress must not linger in a degraded
   half-serving state. It shuts down deliberately and is restarted clean. Admission control exists
   to make that path effectively unreachable — but when it is reached, the answer is a clean
   restart, not indefinite degradation.
2. **Restart must never require recovery.** After any termination — orderly, OOM-killed, or
   `kill -9` — the on-disk log and data directory must already be in a valid state that replays
   deterministically. No repair tool, no manual intervention, no "probably fine."
3. **Clients are always told.** Every failure — rejection, timeout, shutdown, restart — reaches
   the client as a typed error that says whether to retry, and when. Never a dropped connection.

**Constraint 2 is currently violated, and it blocks everything else** (see §2.1). A crash-only
overload policy is only safe on top of a restart path that works. Component 47 must land first.

-----

## 2. Findings

Each finding below was verified against the code at `24dee26`, not inferred. Findings marked
**BUG** are defects in shipped behavior; findings marked **GAP** are missing capability.

### 2.1 BUG (critical): a file-backed runtime cannot reliably restart

`OpenFileRuntime` fails permanently after a handful of restarts. Verified with a probe that opened
a data directory, wrote one document, and closed it, in a loop:

```
cycle 0: ok, head=789c0855, delta segments on disk=1
cycle 1: ok, head=b430655b, delta segments on disk=2
cycle 2: ok, head=f496cced, delta segments on disk=3
cycle 3: OPEN FAILED: missing parent b430655b54d326520ce6f9a8b8013841232c1490a58d38fa39526e46eab95199
```

Repeated runs failed at cycle 2, 2, 2, 3, and 4 — nondeterministic, which identifies the cause.
Every call to `engine.DefaultFactory.Open` creates a **new delta segment with a random UUID**
(`delta.Factory.OpenWriter` → `codec.RandomUUID()`, `go/kdb/storage/engine/server_engine.go:374`),
so each process start writes its commits to a fresh segment. `replayDeltaNamespace` then sorts
segments by **`SegmentID.String()` — the random UUID** (`go/kdb/embed/delta_replay.go:19-21`) and
replays them in that order. Once more than one segment exists, replay order is unrelated to commit
order, and `PutCommit(c, requireParents=true)` rejects a commit whose parent has not been loaded
yet.

The failure is not a corrupted read that a retry would clear: the database is **permanently
unopenable**, with no recovery path, and the data is intact but unreachable. This is the single
highest-severity item in this document.

### 2.2 BUG: durability is bypassed for federated commits

Peer sync applies inbound commits with `dag.PutCommit` (`go/kdb/peersync/host.go:150`,
`go/kdb/peersync/client.go:225`). On a file-backed node the DAG is a `PersistingCommitDAG`, whose
`PutCommit` **deliberately does not write to the delta log** — it is also the replay-from-log path,
so re-persisting would duplicate records (`go/kdb/embed/persisting_dag.go:26-29`). The result:
commits received from a peer live only in memory and vanish on restart. The node then re-fetches
them, so the data is not lost cluster-wide, but a federated node's local durability silently does
not hold. The API cannot currently distinguish "replaying my own log" from "ingesting a new remote
commit" — that distinction is missing, not just unused.

### 2.3 BUG: the delta log's CRC is written but never verified

`PageCodec.Frame` computes and stores `CRC32All(body)` at frame offset 12
(`go/kdb/storage/delta/page_codec.go:32`). Neither `PageCodec.Parse` nor `ScanSegmentBytes` ever
reads it back. Verified: corrupting a body byte while leaving the CRC field intact produces a
successful parse returning silently-wrong data —

```
Parse of CRC-mismatched frame: err=<nil> body="{\"he\x93lo\":\"world\"}"
```

Under `CompressionZSTD` (the file-runtime default) most corruption surfaces as a decompression
error instead, but that is luck, not a check, and it produces a confusing error rather than a
precise "segment X frame at offset Y is corrupt". Silent corruption is exactly the failure mode
constraint 2 exists to rule out.

Note: the scanner's handling of a **truncated tail** is already correct — `frameEnd > len(bytes)`
stops the scan (`scanner.go:29-31`), and a garbage length field decodes as a large positive number
on all supported 64-bit targets, which also stops the scan. Torn tails are safe today; corrupt
bodies are not.

### 2.4 BUG: no clean shutdown path reaches storage

`EmbeddedKdbRuntime.Close()` releases the directory lock and nothing else
(`go/kdb/embed/runtime.go:28-36`). `defaultHandle.Close()` and `ServerEngine.Close()` both exist
and both do real work — stopping the WAL's background sync ticker and performing a final
`wal.Sync()` — but `OpenFileRuntimeWithOptions` keeps only `handle.Adapter()`,
`handle.DeltaReader()` and `handle.DeltaWriter()`, dropping the handle itself
(`go/kdb/embed/file.go:55-80`). Nothing can ever call them. Consequently **even an orderly shutdown
never seals the delta segment and never performs the WAL's final sync** — an intentional `SIGTERM`
is indistinguishable on disk from a `kill -9`.

### 2.5 BUG: `MemoryGuard` latches permanently — the zombie the operator does not want

The guard compares `runtime.MemStats.Sys` against a fixed threshold. `Sys` counts every byte the
process has ever obtained from the OS and **does not decrease when Go returns pages** — released
pages move into `HeapReleased` but remain counted in `Sys`. So once the threshold is crossed, the
condition is true forever. The service then rejects every write for the rest of its life while
still accepting connections and serving reads: precisely the zombie state constraint 1 forbids.
This is also why tuning required backing off to ~60% of the container limit
(`docs/benchmarks/lightsail-sim/README.md`): the signal systematically overstates real usage by
`HeapReleased`, and the overstatement is permanent.

### 2.6 GAP: sampling the guard stops the world

`runtime.ReadMemStats` **stops all goroutines** for the duration of the read. The guard calls it
every 200ms for the lifetime of the process (`memory_guard.go:45-54`). On a 2-vCPU Lightsail
instance under load this is a recurring, self-inflicted latency source in the exact conditions the
guard is meant to improve. `runtime/metrics.Read` provides the same numbers without the pause.

### 2.7 GAP: rejection happens after the expensive part

The check sits at the top of `commitWith` (`server_runtime.go:203`), by which point the frame has
been read off the socket, JSON-decoded into a `wire.Message`, dispatched, and parsed. A server
shedding load still pays nearly the full per-request cost of everything it rejects. Each connection
additionally buffers up to 32 undecoded frames (`transport/tcp`'s `incoming` channel), an
unaccounted queue of raw payloads whose size is bounded only by `MaxFrameBytes` (16MB) × 32 ×
connections.

### 2.8 GAP: no CPU governance of any kind

Goroutine-per-connection with no connection cap (`transport/tcp.Serve`); no request deadlines
anywhere (`context.Background()` at every call site); no bound on scan work — `MaxRows: 10_000`
caps the **result set**, not the rows examined (`wire_listen.go:285`); no distinction between a
point read and a full namespace scan; `GOMAXPROCS` not reconciled with the container's CPU quota.

### 2.9 GAP: `GOMEMLIMIT` is unused

Go's soft memory limit (`runtime/debug.SetMemoryLimit`, Go 1.19+) makes the GC progressively more
aggressive as the heap approaches a target, trading CPU to avoid growth. It is the runtime's own
mechanism for "run right up to the memory ceiling without dying," and nothing in the repo sets it.

### 2.10 GAP: errors reach clients as opaque strings

Every failure is flattened into `Error *string` on `SqlResultMessage` / `UpsertResultMessage`
(`wire/payload_dto.go:160`) and re-wrapped client-side as `fmt.Errorf("kdb: %s", *r.Error)`
(`client/query.go:55,99`). Only conflicts get structured treatment, via a distinct
`ConflictReportMessage`. A client therefore **cannot distinguish "busy, retry in 50ms" from "your
transaction is invalid, never retry"** except by string matching. Retry-safety is the whole point
of a backpressure signal, and the wire protocol cannot currently express it.

### 2.11 GAP: in-memory state advances before durability

`commitWith` calls `engine.Commit` — which mutates the in-memory DAG, advances the branch head, and
writes documents into the storage adapter — and only then calls `persister.Persist`
(`server_runtime.go:214-226`). If `Persist` fails, the client correctly receives an error, but the
commit is already visible at head to every concurrent reader, and a restart will roll it back.
In-memory state is briefly ahead of disk, and a persist failure makes that divergence permanent
until restart.

### 2.12 GAP: one fsync per commit, with a group committer sitting unused

`PersistingCommitDAG.Persist` calls `Append` then `Flush` for every single commit, and
`FsyncOnFlush` defaults to `true` — so every commit costs a physical fsync. `wal.GroupCommitter`
already implements exactly the batching needed (coalescing concurrent fsync requests while
preserving the "durable through seq N" guarantee) and is wired into the WAL path but not the delta
path. Under federation, a 100-commit batch push currently costs 100 fsyncs.

-----

## 3. Design principles

**P1 — Admission, not detection.** Resources are reserved before work starts, not sampled after it
has grown. Rejection is a decision made with a number, not a reaction to a threshold crossing.

**P2 — Cost is estimated, then measured.** Every admitted operation carries an estimate. Actual
usage is measured and feeds back into the estimator, so estimates converge on reality instead of
being a static guess that rots.

**P3 — Hysteresis everywhere; no one-way doors.** Every pressure signal has separate trip and clear
thresholds and a minimum dwell time. No state that can be entered but not left. (Directly answers
§2.5.)

**P4 — Crash-only.** There is no distinct "recovery mode." The orderly shutdown path and the
`kill -9` path converge on the same on-disk state and the same startup path, so the rare path is
exercised on every single start and cannot rot. Orderly shutdown is an *optimization* (it seals and
flushes), never a *correctness requirement*.

**P5 — Durable-before-visible.** A commit becomes visible to readers only after it is durable.
Inverting §2.11 removes the divergence window entirely, and makes the log a strict prefix of
visible state at all times.

**P6 — Every failure is typed and carries retry guidance.** The client learns *what* failed, and
whether and when to retry, without parsing prose.

**P7 — Reads degrade last.** Under pressure, shed in order: replication apply → background work →
scans → writes → point reads. A node under pressure keeps serving the data it already has. (This
preserves the current guard's read-through policy, which measurement showed was right.)

-----

## 4. Component 47 — Durable Restart Contract

**Blocks every other component. Nothing else in this layer is safe until restart is trustworthy.**

Goal: after any termination, the data directory replays deterministically with no repair step.

### 4.1 Monotonic, ordered segments

Replace random-UUID segment identity with a **monotonically increasing, zero-padded sequence
number** assigned at open: `delta/000000000001.seg`. The name is the order; lexicographic sort is
commit order; `sort.Slice` on segment names becomes correct by construction. `SegmentID` remains a
UUID in `DeltaSegmentRef` for external identity, but ordering never consults it again.

Open procedure: list `delta/*.seg`, take the highest sequence, either continue appending to it (if
unsealed and under `MaxSegmentBytes`) or start `N+1`. This also stops the current behavior of
creating a fresh, often-empty segment on every start.

Migration: a directory containing UUID-named segments is not silently reordered — it is detected at
open and either (a) rewritten into sequence order by a one-shot `kdb-inspect repair-segments`
command that derives true order by walking parent links, or (b) rejected with an error naming that
command. **Never guess.** Given §2.1, existing multi-segment directories may already be unopenable;
the repair command is the migration path for them and must ship with 47.

### 4.2 Ordering that does not depend on segment boundaries

Segment order alone is necessary but not sufficient — it must be impossible for a correct log to be
misreplayed. Make replay **topological, not positional**: buffer scanned commits, then apply in
dependency order (a commit is applied once its parents are present), erroring only if a genuine
parent is missing after the whole log is read. Ordering then becomes a performance property, not a
correctness property, and §2.1's failure mode cannot recur even if segment naming is wrong again
later. Bound the buffer, and stream in segment order so the common case applies immediately with no
buffering.

### 4.3 Verified frames

`ScanSegmentBytes` verifies the stored CRC32 before parsing (fixes §2.3). On mismatch:

- **Mismatch in the final frame of the final segment** → treat as a torn tail: stop the scan, log
  it at WARN with segment and offset, truncate the segment to the last good frame, continue. This
  is correct by construction under P5: a torn tail can only be a commit that was never acked.
- **Mismatch anywhere else** → real corruption. Fail the open with a precise error naming segment,
  offset, and expected/actual CRC. Do not silently skip; do not partially serve.

### 4.4 Durable-before-visible commit ordering

Restructure `commitWith` (fixes §2.11) so the sequence is: prepare the commit → **persist and
flush** → publish to the in-memory DAG and advance head → ack the client. A persist failure means
nothing was ever visible, so the error handed back is complete and truthful, and the retry is safe.

This requires splitting `transaction.Engine.Commit`'s current single step into prepare/publish
phases, or holding `commitMu` across persist-then-publish. The lock is already held across the
whole operation today, so the ordering change costs no additional contention — only the fsync moves
inside the critical section, which §4.6 addresses.

### 4.5 A shutdown path that reaches storage

`EmbeddedKdbRuntime` retains the `engine.Handle` and `Close()` calls, in order: stop accepting →
drain in-flight → `deltaWriter.Flush()` → `handle.Close()` (WAL final sync, ticker stop) →
`Seal()` the active segment → write the manifest → release the directory lock (fixes §2.4).

Seal is an optimization, never a correctness dependency (P4): an unsealed segment is always valid
and always replayable.

### 4.6 Batched durability via the existing group committer

Route delta-log flushes through `wal.GroupCommitter` (fixes §2.12), which already provides the
needed contract: `SyncTo(seq)` returns only once everything through `seq` is durable, with one
physical fsync per batch. Concurrent commits — and especially batched replication applies (§9) —
then cost one fsync per batch rather than one per commit, which is what makes §4.4's
persist-before-publish affordable.

### 4.7 Single-writer enforcement — already correct

Verified: `embed/dir_lock.go` uses `syscall.Flock(LOCK_EX|LOCK_NB)`, a real OS-level advisory lock
that the kernel releases automatically when the process dies. An OOM-killed or `kill -9`'d process
therefore leaves no stale lock, and an automatic restart acquires the directory immediately. This
is a precondition of the crash-only policy (a lockfile whose staleness had to be inferred would
have made automatic restart unreliable) and it already holds — no change needed, but test 10 in
§4.8 guards it against regression.

### 4.8 Tests

| # | Name | Expected |
|---|---|---|
| 1 | `restartLoop100` | 100 open/write/close cycles, all succeed, all data present |
| 2 | `restartAfterSIGKILL` | `kill -9` mid-write; reopen succeeds; every acked commit present |
| 3 | `tornTailTruncated` | Append half a frame; reopen succeeds; log truncated at last good frame |
| 4 | `corruptMidLogRejected` | Flip a byte in a non-final frame; open fails naming segment+offset |
| 5 | `crcVerifiedNoCompression` | CRC mismatch under `CompressionNone` is detected (regression for §2.3) |
| 6 | `orderedSegmentNames` | Segment names sort in creation order after 10 restarts |
| 7 | `topologicalReplay` | Deliberately shuffled segment order still replays correctly |
| 8 | `ackImpliesDurable` | Every acked commit survives `kill -9`; no unacked commit is visible |
| 9 | `uuidSegmentsDetected` | A legacy UUID-named directory errors with the repair command, not corruption |
| 10 | `lockReleasedOnKill` | After `kill -9`, a new process opens the directory without manual cleanup |

-----

## 5. Component 48 — Cost Model and Memory Admission

### 5.1 Measuring the budget correctly

Replace `MemStats.Sys` (§2.5, §2.6) with, in preference order:

1. **cgroup v2** `memory.current` — on Linux, the exact number the kernel enforces `memory.max`
   against. This is ground truth; everything else is an approximation of it.
2. **`runtime/metrics`** `/memory/classes/total:bytes` minus `/memory/classes/heap/released:bytes`
   — no stop-the-world, and unlike `Sys` it *decreases* when pages are returned.
3. `MemStats.Sys - MemStats.HeapReleased` as a last resort.

Sample on a ticker as now, but into a small ring buffer, and drive decisions from a short moving
average so one spike does not trip the system.

### 5.2 The cost model

```go
// go/kdb/server/costmodel.go
type OpClass int
const (
    ClassPointRead OpClass = iota // DocumentGet
    ClassScan                     // SqlExec SELECT
    ClassWrite                    // Commit / Upsert / TxCommit
    ClassReplication              // inbound peer CommitPush
)

// Estimate returns the bytes an operation of this class and payload size is
// expected to hold live until it completes.
func (m *CostModel) Estimate(class OpClass, payloadBytes int) int64
```

Start with `base[class] + k[class] * payloadBytes`, with `base` and `k` **measured, not guessed** —
a new `BenchmarkCommitBytesPerOp` reporting `B/op` against payload size, in the same package as the
existing `BenchmarkCommitScalingWithHistorySize`, calibrates them and guards them against
regression.

Per P2, feed actuals back: each admitted operation records its real high-water usage on completion,
and `k` tracks a high percentile (p95) of observed cost per byte, clamped to a configured range so
a pathological sample cannot poison the estimator. Under-estimation is the dangerous direction, so
bias high.

`ClassScan` is the hard case, because cost depends on rows examined rather than request size. Two
mechanisms rather than a better estimate:
- Admit scans against a **conservative fixed grant** plus a **row budget** enforced during
  execution (a real limit on rows examined, not just rows returned — closing §2.8).
- A scan exceeding its row budget is aborted with a typed `ResourceExhausted`, never allowed to
  silently consume the node.

### 5.3 Grants

```go
type Admission struct {
    mem      *semaphore.Weighted // capacity = budget - reserve
    // ... per-class queues, CPU tokens, cost model
}

func (a *Admission) Acquire(ctx context.Context, class OpClass, payloadBytes int) (*Grant, error)
func (g *Grant) Release()  // idempotent; returns the reservation and records actuals
```

`golang.org/x/sync/semaphore`'s weighted semaphore provides exactly this, and
`golang.org/x/sync v0.22.0` is **already in `go.mod`** as an indirect dependency — this promotes it
to direct rather than adding anything new.

Capacity is `effectiveLimit - reserve - nonGrantedFloor`, where `nonGrantedFloor` is a measured
allowance for memory the grant system does not govern (goroutine stacks, the DAG itself, indexes).
Because the DAG grows monotonically, `nonGrantedFloor` grows over time — recomputed each sampling
tick, so grant capacity shrinks naturally as retained state grows. **This is the mechanism that
turns "the DAG grows without bound by design" from an eventual OOM into a smooth, predictable
throttle.**

### 5.4 Admit early

Move admission to the **frame boundary** (fixes §2.7): the transport reads the frame header, learns
the length, and acquires a grant sized on that length *before* reading the body into memory and
before decoding. A rejected request costs a header read and a small typed response.

Reduce the per-connection `incoming` buffer from 32 frames to a small number (2–4) and account for
it: the queue is real memory, and 32 × 16MB × N connections is not a bound worth having.

### 5.5 Graduated response, with hysteresis

| Zone | Condition | Policy |
|---|---|---|
| Normal | < 70% | Admit all classes |
| Elevated | 70–85% | Admit; shrink scan row budgets; halve replication concurrency |
| High | 85–93% | Reject `ClassWrite` and `ClassScan` with `Busy` + `retryAfterMs`; admit point reads and replication drain |
| Critical | > 93% | Reject all but point reads; cancel in-flight scans; release the rescue reserve; start the abort timer (§7) |

Every zone transition requires the condition to hold for a minimum dwell time, and downward
transitions use a lower threshold than upward (P3). Crossing into Critical is a **metric and a log
event**, never a silent state change.

### 5.6 `GOMEMLIMIT` and the rescue reserve

- `debug.SetMemoryLimit(effectiveLimit * 0.90)` at startup (fixes §2.9). The GC then spends CPU
  rather than let the heap grow — and that CPU spend is itself an early pressure signal, readable
  from `/gc/cycles/total:gc-cycles` in `runtime/metrics`.
- Allocate a **rescue reserve** (default 48MB) at startup and hold it. On entering Critical, drop
  it and call `debug.FreeOSMemory()`. That headroom is what lets in-flight commits finish, the log
  flush complete, the typed rejections be written, and the abort be logged — instead of dying
  mid-operation. Re-allocate it on returning to Normal; failure to re-allocate is itself a signal
  and triggers abort.

-----

## 6. Component 49 — CPU Governance and Bounded Queues

### 6.1 Reconcile with the container — verify, don't change

`GOMAXPROCS` must follow the cgroup CPU quota rather than host core count: on a 2-vCPU Lightsail
instance an oversubscribed scheduler gets throttled by the kernel, producing latency spikes that
look exactly like overload and would make this system shed load for no reason.

The module is on **Go 1.26**, and Go 1.25+ sets `GOMAXPROCS` from the cgroup quota automatically,
so this should already hold. Verify it under the Docker harness rather than assuming, and assert it
in a startup log line — a silent regression here would systematically mis-tune every CPU threshold
in this component.

### 6.2 Bounded queues with deadlines

Replace the bare `commitMu` (`server_runtime.go:62`) with a per-class bounded queue. Writes are
already fully serialized, so this is a change of queue discipline, not of concurrency:

```go
type Queue struct {
    slots    chan struct{} // capacity = max queued
    target   time.Duration // CoDel sojourn target
    interval time.Duration
}
```

Admission outcomes, exactly as specified by the operator requirement:

1. Queue full → immediate `Busy` with `retryAfterMs`.
2. Estimated completion (queue depth × measured p95 service time) exceeds the request deadline →
   immediate `Busy`. **Do not enqueue work that cannot finish in time** — this is P1 applied to
   time as well as memory.
3. Sojourn time stays above target across an interval (CoDel) → shed, even if the queue is short.
   Standing-queue detection catches sustained overload that depth alone misses.
4. Otherwise admit.

Under overload, switch to **adaptive LIFO**: serve the newest waiter first, because the oldest
waiter's client has most likely already timed out and its result is worthless. Under normal load
the queue is near-empty and order is irrelevant, so this costs nothing when it does not matter.

### 6.3 Deadlines end to end

Plumb `context.Context` through the commit, query, and replication paths, replacing the
`context.Background()` calls at every site (§2.8). Deadline sources, in priority order: an explicit
client-supplied deadline on the wire (new optional header field), the connection's configured
default, then a server maximum. Cancellation is what makes "unwind operations we cannot finish"
concrete — and under P5 the unwind is clean, because a cancelled commit either reached the
durable-publish step or did not.

### 6.4 CPU tokens and stall signals

Because post-fix commit cost is flat and measured (~20–30µs/op regardless of history —
`docs/benchmarks/lightsail-sim/README.md`), a token bucket for writes can be sized directly from
the CPU quota with explicit headroom reserved for reads. Under CPU pressure, shrink write tokens
first (P7).

On Linux, read **PSI** (`/proc/pressure/cpu`, `/proc/pressure/memory`): it reports actual stall
time, which is a far better overload signal than a utilization gauge — utilization saturates at
100% and stops carrying information, while PSI keeps rising. Fall back to a scheduler-latency probe
where PSI is unavailable (macOS dev, older kernels).

### 6.5 Connection limits

Cap concurrent connections; reject beyond the cap at accept time with a typed handshake rejection
(the handshake already carries `RejectionReason`). An unbounded goroutine-per-connection model is a
memory commitment the grant system cannot see.

-----

## 7. Component 50 — Fail-Fast Supervision (no zombie)

The operator requirement: *better to crash and restart clean than to linger degraded — and that
should not be needed.* Both halves are design constraints.

### 7.1 "Should not be needed"

Components 47–49 exist so abort is unreachable in normal operation. Abort is a backstop with an
alert attached, not a routine load-shedding mechanism. **If abort fires in production, that is a
bug in the admission model** — it means estimates were wrong or a cost class is unaccounted. The
abort path therefore records enough state to diagnose exactly that (§7.3).

### 7.2 The abort trigger

A watchdog fires only on **inability to make progress**, never on pressure alone:

- Critical zone (§5.5) sustained beyond `--abort-after` (default 30s) despite full shedding, **or**
- the rescue reserve cannot be re-allocated, **or**
- GC CPU fraction exceeds ~80% sustained (`/gc/cycles/...` — the runtime thrashing, the classic
  pre-OOM death spiral), **or**
- a durability invariant fails (delta append or fsync returns an error) — because continuing past a
  durability failure is exactly the zombie state, and P4 says a clean restart is strictly better.

Reaching this point means the process has already been rejecting work for 30 seconds and is not
recovering. It is not serving; it is only pretending to.

### 7.3 Orderly abort

Ordered, time-boxed (default 5s total), each step best-effort with the next running regardless:

1. Stop accepting new connections and new work; every subsequent request gets typed `Unavailable`.
2. **Fail every in-flight and queued operation with typed `Unavailable` + `retryAfterMs`, and flush
   those responses to clients.** This is the "clients are notified" requirement at its most
   important moment — no silently dropped connections.
3. Release the rescue reserve to guarantee headroom for steps 4–6.
4. Flush and fsync the delta log; sync the WAL.
5. Seal the active segment; write the manifest.
6. Log a structured abort record: zone history, grant utilization, per-class queue depths,
   estimate-vs-actual error, top allocation sites. This is the diagnostic for §7.1.
7. `os.Exit(75)` — `EX_TEMPFAIL`, distinguishing "restart me" from a config error the supervisor
   should not retry-loop on.

If the time box expires, exit immediately anyway. **A hung shutdown is itself a zombie** (P4): the
on-disk state is already valid by §4, so exiting early costs at most an unsealed segment.

### 7.4 Supervision

The service does not restart itself. It exits; a supervisor restarts it — Docker
`--restart=on-failure`, systemd `Restart=on-failure` with `RestartSec` backoff, or the platform's
own supervisor. Document required settings alongside `--memory-limit-mb`, because the crash-only
policy is only complete with a supervisor configured, and a Lightsail deployment without one turns
"restart clean" into "stay down."

Exit-code contract: `0` orderly, `75` abort (restart me), `78` config error (`EX_CONFIG`, do not
retry-loop).

### 7.5 Restart is cheap, but not free

Restart replays the full delta log. Since the DAG grows monotonically, replay time grows with
history — a node aborting under memory pressure caused by a large DAG will also be slow to restart,
exactly when it can least afford it. Two mitigations, and the honest note that they matter:

- **Snapshots.** Periodically write a materialized snapshot (the `WriteSnapshot`/`ReadSnapshot`
  shim methods already exist and are unused by this path) so replay starts from the snapshot and
  applies only the tail. Bounds restart time independent of total history.
- **Compaction.** Wiring Component 19 into `kdb-service` (see §10) is the real fix: it bounds the
  DAG itself, which bounds both memory pressure and restart time together.

Until snapshots land, `--abort-after` should be tuned with measured replay time in mind, and the
abort record logs it.

-----

## 8. Component 51 — Typed Error Model

Fixes §2.10. Without this, backpressure is unusable: a client that cannot tell "retry in 50ms" from
"never retry" must either retry everything (amplifying overload precisely when the server is
shedding) or retry nothing (turning transient pressure into user-visible failure).

### 8.1 Wire representation

Add a structured error payload alongside the existing `Error *string`, which is retained
unchanged for compatibility — old clients keep working and see the same prose.

```go
type WireError struct {
    Code         ErrorCode `json:"code"`
    Message      string    `json:"message"`
    RetryAfterMs *int      `json:"retryAfterMs,omitempty"`
    Retryable    bool      `json:"retryable"`
    Detail       any       `json:"detail,omitempty"`
}
```

| Code | Meaning | Retryable | Source |
|---|---|---|---|
| `BUSY` | Admission rejected; capacity exists later | yes, after `retryAfterMs` | §5.5, §6.2 |
| `RESOURCE_EXHAUSTED` | This operation is too large to ever admit | no — resubmit smaller | §5.2 |
| `DEADLINE_EXCEEDED` | Deadline passed while queued or running | yes, idempotent ops | §6.3 |
| `UNAVAILABLE` | Shutting down / restarting | yes, after reconnect | §7.3 |
| `CONFLICT` | Existing conflict semantics | caller-dependent | Layer 3 |
| `SCHEMA_VIOLATION` | Invalid transaction | no | Layer 2 |
| `UNAUTHORIZED` | RBAC denial | no | Layer 11/12 |
| `INTERNAL` | Unclassified | no | — |

`BUSY` and `RESOURCE_EXHAUSTED` are deliberately distinct: the first is about the server's current
state, the second about the request itself. Conflating them produces clients that retry forever on
a request that can never succeed.

### 8.2 Client behavior

`go/kdb/client` gains sentinel errors (`ErrBusy`, `ErrUnavailable`, `ErrDeadlineExceeded`,
`ErrResourceExhausted`) matching the existing `ErrConflict` pattern, usable with `errors.Is`. The
client honors `retryAfterMs` with **full jitter** — a fleet retrying in lockstep re-creates the
overload the server just shed — and applies a circuit breaker after repeated `BUSY`, so a
struggling node is given room to recover rather than hammered.

Critically: **retry is only automatic for operations that are safe to retry.** `Upsert`
(`ConflictPolicyLastWrite`, no `BaseVersion`) is idempotent by construction. `Commit` carries a
transaction id and the engine's `findExistingCommit` makes retries idempotent — which is exactly
what the O(1) transaction-id index added in `ab891af` guarantees, so this design depends on that
fix and should reference it.

### 8.3 Ports

The same codes and semantics go into the Kotlin wire layer and JDBC driver, mapping to the
appropriate `SQLTransientException` / `SQLNonTransientException` subclasses so JDBC clients get
correct retry behavior from standard tooling for free.

-----

## 9. Component 52 — Federated Backpressure and Log-Offset Replication

Federation makes every problem above worse: peers retry automatically, in parallel, and do not get
bored. A node shedding client load while peers push at full rate is not shedding load.

### 9.1 Replication is an admission class

Inbound `CommitPush` goes through `Admission.Acquire(ClassReplication, ...)` like everything else,
fixing §2.2's sibling gap: today it bypasses the guard entirely. Its priority sits below foreground
writes but above scans — local clients stay responsive, while replication apply is genuinely
cheaper per commit (no conflict detection against concurrent writers; batchable) and so earns its
own class rather than sharing the write budget.

### 9.2 Fix durability for federated commits

Split the conflated path (§2.2) into two explicit operations:

- `PutCommitFromLog(c)` — replay; must not re-persist.
- `IngestCommit(c)` — new commit from a peer; **must** persist through the same
  durable-before-visible path as a local commit (§4.4).

`peersync`'s host and client call `IngestCommit`. Ingest is batched and flushed once per batch
through the group committer (§4.6), so a 100-commit push costs one fsync.

### 9.3 Log-offset replication

The delta log, once ordered (§4.1), *is* a replication stream. Add offset-based replication
alongside the existing DAG-diff protocol:

- Peers track a `(segmentSeq, frameOffset)` cursor; "send me everything after X" is a sequential
  read, cheap to serve and resumable after interruption at exactly the right place.
- **Pull-based, so backpressure is structural**: a pressured node simply stops pulling. Nothing
  accumulates anywhere except in the sender's log, which already exists and is already durable.
  **The sender's log is the retry queue** — there is no in-memory federation queue to bound,
  because there is no in-memory federation queue.
- Push retains an ack carrying a **durable high-water mark** (the last offset durably applied), so
  a partially-applied or rejected batch resumes precisely rather than restarting.
- DAG-diff (`computeSyncPlan`) is retained for genuine fork reconciliation, where offsets are
  meaningless. Offsets handle the common case; the DAG walk handles divergence.

### 9.4 Peers must honor backpressure

A peer receiving `BUSY` applies jittered exponential backoff and reduces batch size. Peers that
ignore backpressure turn one struggling node into a cluster-wide retry storm — the single most
common way a federated system converts a recoverable local problem into an outage. Add an
integration test that asserts a pressured node's inbound replication rate actually falls (§11 test
9).

### 9.5 Restart participation

On restart, a node resumes replication from its durable high-water mark. Combined with §7, a node
that aborts under pressure rejoins cleanly: peers see a disconnect, back off, and resume from a
known offset. This is what makes the crash-only policy safe under federation rather than merely
survivable — nothing depends on the aborting node having shut down gracefully.

-----

## 10. Beyond this layer: making pressure a tiering signal

Every mechanism above manages a fundamentally fixed budget. The structural fix is to stop requiring
the whole DAG to be resident:

- **Wire Component 19 (compaction) into `kdb-service`.** It exists and is not automatically
  invoked. Memory pressure should trigger *eviction of cold history to disk* before it triggers
  rejection — converting "reject writes" into "degrade to disk-backed," which is a far better
  degradation curve and also bounds restart replay time (§7.5).
- **Use the storage tier machinery already built** (`kdb-storage-tier`, memtable/sstable, block
  cache with `HotTierMemoryConfig`) for DAG history, not just documents. `ResolveHotTierBytes`
  already implements the "small by default, configurable" budget policy this layer needs, and
  should become the single place a memory budget is expressed.
- **cgroup v2 `memory.high`** alongside `memory.max` in the Docker/Lightsail configuration:
  `memory.high` throttles and reclaims rather than killing, giving the abort path (§7.3) time to
  run instead of racing a `SIGKILL` it cannot catch.

Worth reading as precedent for the admission layer as a whole: **FoundationDB's Ratekeeper** (a
single component measures queue depth and hands clients a rate, so clients throttle themselves
before the server has to reject) and **CockroachDB's admission control** (token buckets per work
class, elastic CPU tokens, store-overload signals). Both solve this problem at this scale, and both
are well documented.

-----

## 11. Execution plan

Ordered by dependency, not by appeal. **Phase 0 is non-negotiable and ships alone.**

| Phase | Component | Scope | Gate |
|---|---|---|---|
| **0** | 47 | Ordered segments, topological replay, CRC verification, storage-reaching `Close`, durable-before-visible, group-committed flush, repair command | 100 restart cycles + `kill -9` at every step, zero data loss, zero manual recovery |
| **1** | 51 | Typed error model, wire + Go client + Kotlin/JDBC | Client distinguishes busy from fatal; existing string errors unchanged for old clients |
| **2** | 48 | Correct memory measurement, cost model + calibration benchmark, grants, early admission, `GOMEMLIMIT`, rescue reserve, hysteresis | Sustained overload plateaus; no latching; zone transitions observable |
| **3** | 49 | `GOMAXPROCS`, bounded queues with deadlines, CoDel + adaptive LIFO, context plumbing, scan row budgets, connection cap | p99 stays bounded under 10× overload; deadline-exceeding work never enqueued |
| **4** | 50 | Watchdog, orderly abort, exit codes, supervisor docs | Abort never fires under Phase 2/3 load; forced abort produces a clean, immediately-reopenable directory |
| **5** | 52 | Replication admission class, `IngestCommit` durability, offset replication, peer backoff | Pressured node's inbound replication measurably falls; federated restart resumes from high-water mark |

Phases 1 and 2 can overlap (different files, and 2 needs 1's codes only at the end). Phase 0 blocks
everything.

**Kotlin parity:** §2.2 (`PutCommit` conflation) and §2.3 (unverified CRC) should be checked in the
Kotlin implementation and ported if present — the two bugs fixed in `ab891af`/`cbb97e2` were both
shared across implementations, so the base rate for shared defects here is high.

-----

## 12. Test plan

Beyond each component's own tests (§4.8 pattern), the layer needs:

| # | Name | Expected |
|---|---|---|
| 1 | `sustainedOverloadPlateaus` | 10× capacity for 10min: memory plateaus, no OOM, no abort, throughput stable |
| 2 | `pressureIsReversible` | Trip High, remove load: server returns to Normal and accepts writes (regression for §2.5) |
| 3 | `noWorkStartedThatCannotFinish` | Every admitted op completes or is cancelled by deadline; none dies from exhaustion |
| 4 | `killDashNineAnyPoint` | `kill -9` at 20 points in the commit path; every ack'd commit present, none partial |
| 5 | `clientsAlwaysNotified` | Under overload and during abort, every request gets a typed response; zero silent drops |
| 6 | `abortNotReachedUnderLoad` | Phase 2/3 config under max sustainable load never aborts over 30min |
| 7 | `abortLeavesCleanState` | Forced abort → immediate reopen, no repair, no loss |
| 8 | `restartTimeBounded` | Restart time stays bounded as history grows (gates §7.5 snapshots) |
| 9 | `federatedBackpressure` | Pressured node's inbound replication rate falls; peers back off; no retry storm |
| 10 | `estimateAccuracy` | Estimate-vs-actual p95 error within bound; under-estimates rare and small |
| 11 | `lightsailRealHardware` | The still-outstanding Component 38 §7 test 8, now with governance enabled |

Test 11 remains the honest gate on any cost claim: everything measured so far is an arm64 Docker
approximation, per `docs/benchmarks/lightsail-sim/README.md`'s own caveats.

-----

## 13. Configuration surface

Replaces the single `--memory-limit-mb`, which is retained as a deprecated alias mapping to
`--memory-budget-mb`.

```
--memory-budget-mb N       Memory budget. Default: cgroup limit if detectable, else 0 (disabled).
                           With correct accounting (§5.1) this can be set near the real limit —
                           the ~60% guidance was a workaround for Sys over-reporting.
--memory-reserve-mb N      Rescue reserve. Default 48.
--max-connections N        Default 256.
--max-queued-writes N      Default 64.
--request-timeout DUR      Default 5s. Client may request less, never more.
--scan-row-budget N        Max rows examined per scan. Default 1_000_000.
--abort-after DUR          Sustained-Critical duration before orderly abort. Default 30s. 0 disables.
--abort-grace DUR          Time box for the abort sequence. Default 5s.
```

Every governance decision emits a metric: zone transitions, grants issued/denied by class, queue
depth and sojourn, estimate error, abort triggers. **A shedding server that cannot be observed
shedding is indistinguishable from a broken one** — and given §2.5's latching bug went unnoticed
because the service still answered reads, this is not hypothetical.

-----

## 14. Non-goals

- Distributed admission control (cluster-wide rate coordination). Per-node only; §9.4 is peer
  politeness, not a global scheduler.
- Priority/QoS per tenant or user. Classes are by operation type, not by caller.
- Automatic vertical scaling or memory-limit auto-tuning.
- Replacing the transaction engine's conflict semantics. Untouched.
- DAG compaction itself (Component 19 exists); §10 is about *wiring* it to pressure signals, and is
  explicitly future work.

-----

## 15. NBNC Estimate

| Component | Production | Tests | Total |
|---|---:|---:|---:|
| 47 Durable restart | ~700 | ~900 | ~1,600 |
| 48 Memory admission | ~800 | ~700 | ~1,500 |
| 49 CPU + queues | ~700 | ~600 | ~1,300 |
| 50 Supervision | ~400 | ~500 | ~900 |
| 51 Error model (Go + Kotlin) | ~600 | ~500 | ~1,100 |
| 52 Federation | ~900 | ~800 | ~1,700 |
| **Total** | **~4,100** | **~4,000** | **~8,100** |
