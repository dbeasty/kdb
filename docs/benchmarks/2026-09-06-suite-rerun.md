# Workload baseline at `6d2b4bb`, and three measurement traps

Date: 2026-09-06. Baseline commit: **`6d2b4bb`** (post Layer 16 + physical-layer merges).
Machine: Apple M3 Max (16 cores), macOS 26.5.2, Go 1.26.3 (`darwin/arm64`),
APFS internal SSD. Full suite (132 benchmarks) passes at `bedfc51`; the
`BenchmarkWorkload` family re-taken at `6d2b4bb` for the baseline below.

Reproduce the baseline:

```
cd go && go test ./kdb/server/ -run '^$' -bench BenchmarkWorkload -benchtime 2s -count=5 -benchmem
```

This supersedes the read/write/update/mixed/transaction numbers in
[`workload-matrix.md`](workload-matrix.md), which are not comparable to a
current run — see Trap 1.

## Baseline: `BenchmarkWorkload*` at `6d2b4bb`, `-benchtime 2s -count=5`

Rows marked ▼ drift downward across samples within one process (Trap 2), so
both the first sample and the median are recorded; compare **first-sample to
first-sample**. Unmarked rows are stable (spread ≤8%) and compare as medians.

| Workload | Concurrency / keys | ops/sec (median) | first sample | |
|---|---|---:|---:|---|
| Read | single-user / overlapping | 1,391,142 | — | |
| Read | single-user / non-overlapping | 4,019,925 | — | |
| Read | heavy-multi / overlapping | 7,259,417 | — | ⚠️ regressed, see Trap 3 |
| Read | heavy-multi / non-overlapping | 5,414,630 | — | |
| Write (insert) | single-user | 242.6 | — | |
| Write (insert) | heavy-multi | 30,051 | 31,793 | |
| Update | single-user / overlapping | 242.9 | — | |
| Update | single-user / non-overlapping | 239.9 | — | |
| Update | heavy-multi / overlapping | 19,416 | **27,238** | ▼ 45% |
| Update | heavy-multi / non-overlapping | 18,610 | **28,349** | ▼ 53% |
| Mixed 80/20 | single-user / overlapping | 1,217 | — | |
| Mixed 80/20 | single-user / non-overlapping | 1,184 | — | |
| Mixed 80/20 | heavy-multi / overlapping | 77,269 | **116,327** | ▼ 46% |
| Mixed 80/20 | heavy-multi / non-overlapping | 74,823 | **119,691** | ▼ 50% |
| Transaction | single-user / overlapping | 241.6 | — | 0 conflicts/op |
| Transaction | single-user / non-overlapping | 241.0 | — | 0 conflicts/op |
| Transaction | heavy-multi / overlapping | 13,365 | **16,631** | ▼ 29%, 2.96 conflicts/op |
| Transaction | heavy-multi / non-overlapping | 18,473 | **23,957** | ▼ 38%, 0 conflicts/op |

Durability modes (medians; all stable except async/insert/heavy at ▼28%):

| Mode | Workload | single-user | heavy-multi |
|---|---|---:|---:|
| sync-full | insert | 245.1 | 30,067 |
| sync-full | update | 239.1 | 27,067 |
| sync-full | transaction | 231.0 | 18,065 |
| async-100ms | insert | 29,039 | 27,306 |
| async-100ms | update | 28,361 | 26,225 |
| async-100ms | transaction | 28,574 | 16,192 |
| memory-only | insert | 34,052 | 29,332 |
| memory-only | update | 32,313 | 28,158 |
| memory-only | transaction | 32,185 | 16,319 |

The durability story is unchanged from `workload-matrix.md`: mode is a
single-client lever (~240 → ~30,000 ops/sec sequentially) that nearly vanishes
under concurrency, where group commit already amortizes the fsync.

## Trap 1: `keyFor` was rewritten after the old baselines were taken

`a9c8186` rewrote `keyFor`, *after* `workload-matrix.md`'s read baselines were
measured at `3670905`. The two versions measure different working sets, so
those numbers were never comparable to a later run — which is what made reads
look 71% down.

**The A/B.** Restoring the pre-`a9c8186` `keyFor` verbatim in a throwaway
worktree at one commit — server code byte-identical, only key assignment
reverted — moved heavy-multi-user/non-overlapping from 5.85M to **18.47M**,
reproducing the recorded 19.86M to within ~7%.

**Mechanism.** With `poolSize = 4096` and 1024 workers: the old scheme used 128
fixed buckets of 32 documents with every worker walking in lockstep, so ~128
documents were hot at any instant — cache-resident. The new one sets
`buckets = workerCount() = 1024`, giving each worker a disjoint 4-document
slice, so all 4096 are live and every context switch lands on a cold set. Worth
~3.2x on its own.

The new scheme is the honest one: it actually measures non-overlapping keys,
which is what the row claims. The old was measuring 128 hot documents under a
"non-overlapping" label — the same defect class as the retracted Finding 3, in
the row next door.

## Trap 2: the fsync-heavy concurrent rows are not steady-state

Successive samples within one `go test` process, in run order:

| Row | s1 | s2 | s3 | s4 | s5 |
|---|---:|---:|---:|---:|---:|
| Update heavy / non-overlapping | 28,349 | 18,719 | 18,610 | 15,191 | 13,278 |
| Update heavy / overlapping | 27,238 | 22,429 | 19,416 | 17,080 | 14,929 |
| Mixed heavy / non-overlapping | 119,691 | 90,574 | 74,823 | 63,275 | 59,334 |
| Transaction heavy / non-overlapping | 23,957 | 21,398 | 18,473 | 15,625 | 14,904 |

Drift is 29–53% on Update/Mixed/Transaction heavy-multi-user, but only ~5% on
Write-insert heavy-multi, ≤5% on every single-user row, and **~0% on every
memory-only durability row**. Reads do not drift at all.

That last point is the useful one: memory-only never syncs the WAL and is
immune, which points at accumulated WAL/disk state rather than thermal
throttling (the hypothesis in the previous revision of this file). Not yet
isolated — worth its own investigation before anyone tunes against these rows.

**Consequence:** `workload-matrix.md`'s 29.54k update baseline is a *first*
sample; a late sample of the same healthy code reads ~14k. Comparing across
different `-count` or different position in the run manufactures regressions.

## Trap 3 — and a real regression: document expiry put an `RWMutex` back on the read path

Unlike the other two, this one is a genuine code regression, introduced by the
Layer 16 merge. Reads do not drift, so a back-to-back A/B is decisive:

| Read | `bedfc51` | `6d2b4bb` | Δ |
|---|---:|---:|---:|
| heavy-multi / overlapping | **11.75M** (11.07–13.32) | **7.38M** (7.35–7.55) | **−37%** |
| heavy-multi / non-overlapping | 5.70M | 5.29M | −7.3% |
| single-user / overlapping | 1.45M | 1.36M | −6.5% |
| single-user / non-overlapping | 4.11M | 4.06M | −1.1% |

Ranges do not overlap on the first row. `go tool pprof` over
`BenchmarkWorkloadRead/heavy-multi-user/overlapping` at `6d2b4bb`:

| Symbol | Share |
|---|---|
| `sync/atomic.(*Int32).Add` (RWMutex reader count) | **10.60% flat** |
| ↳ called from `sync.RWMutex.RLock`/`RUnlock` | 8.90s / 5.02s |
| ↳ whose sole caller is `KdbServerRuntime.expirySetting` | **100%** |

`GetDocument` (`server/server_runtime.go:736`) now calls `isExpiredAtHead`,
which calls `expirySetting()` (`server/expiry.go:90`), which takes
`expiryMu.RLock()` — on **every point read**, to fetch a pointer that is `nil`
whenever no expiry policy is configured. The RCU work from Finding 2 is intact;
this is simply a *new* `RLock` added in front of it, and Finding 2's own
conclusion said why that is enough: "The cost is *per `RLock`*, so every one of
them on the path has to go, not just the first."

`clock()` (`expiry.go:104`) takes the same lock, so a deployment that actually
configures expiry pays it twice per read rather than once.

**Fix** (not applied here): publish the setting through an
`atomic.Pointer[documentExpiry]` instead of `expiryMu`+`expiry`, the same
RCU-style publication `InMemoryCommitDag.head` and `ServerEngine.latestTree`
already use. Setting expiry is a rare control-plane operation and reads are the
hot path, which is exactly the shape that pattern exists for. Behaviour is
unchanged; only the publication mechanism differs.

⚠️ Related latent hazard, same shape: `dag.Pin`/`IsPinned`
(`dag/retention.go`) take the DAG's `mu` as a **full write lock** for a
ref-count bump. Harmless where it is called today (per-transaction), but it
would serialize hard if snapshot isolation ever moves onto a per-read path —
which `workload-matrix.md`'s MVCC section anticipates.

## If you are comparing against this file later

- Read rows: comparable as medians, `-benchtime 2s -count=5`.
- Update/Mixed/Transaction heavy-multi rows: first-sample to first-sample only.
- Anything compared against a pre-`a9c8186` number: check `keyFor` first.
- Re-check the read rows after the expiry fix lands; they should recover to
  ~11.7M on heavy-multi/overlapping.
