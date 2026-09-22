# Workload matrix, full run at `73d72af`

Date: 2026-09-22 (11:44–12:08). Commit: **`73d72af`** — 153 commits ahead of the last
published full-matrix run (`73971fd`, 2026-09-09), including cross-namespace transactions
(#67-#74), distributed sync phases 10.5-16 (#80-#95), the damaged-log rollback fix (#81),
Go↔Kotlin peer sync interop, and stored-procedure conflict resolution (#97/#98).

Machine: Apple M3 Max (16 cores), macOS, Go 1.26.3 (`darwin/arm64`). Load average 6.5→4.3
across the run; two unrelated foreign test processes (`auth.test`, `match.test`, from
another project on this shared machine) were cleared before starting.

```
cd go && go test ./kdb/server/ -run '^$' -bench BenchmarkWorkload -benchtime 2s -count=5 -benchmem
```

**180/180 rows, PASS, zero `deadline exceeded`, 24m29s (1469.082s).**

Compared against:
- **Base**: [`2026-09-06-suite-rerun.md`](2026-09-06-suite-rerun.md), commit `6d2b4bb`
- **Previous run**: [`2026-09-09-workload-matrix-complete.md`](2026-09-09-workload-matrix-complete.md), commit `73971fd`

Per the established convention: reads and single-user rows are stable and compared as
medians; Update/Mixed/Transaction heavy-multi-user rows drift within a `go test` process
(Trap 2), so they're compared first-sample to first-sample. Durability-mode rows are stable
and compared as medians.

## Reads — the control

| Row | base (6d2b4bb) | prev (73971fd) | now (73d72af) | Δ vs base | Δ vs prev |
|---|---:|---:|---:|---:|---:|
| single-user / overlapping | 1,391,142 | 3,617,025 | 4,315,957 | +210% | +19.3% |
| single-user / non-overlapping | 4,019,925 | 5,401,010 | 5,219,009 | +29.8% | −3.4% |
| heavy-multi / overlapping | 7,259,417 | 6,972,877 | 14,965,831 | +106% | **+114.6%** |
| heavy-multi / non-overlapping | 5,414,630 | 5,371,865 | 6,010,710 | +11.0% | +11.9% |

Heavy-multi/overlapping roughly doubled again since the last run — consistent with the
expiry-`RWMutex` fix (PR #63) plus whatever landed in the distributed/RCU work since.

## Writes (first-sample for heavy-multi, median for single-user)

| Row | base | prev | now | Δ vs base | Δ vs prev |
|---|---:|---:|---:|---:|---:|
| WriteInsert / single-user | 242.6 | 236.9 (isolated) | **210.0** | **−13.4%** | **−11.4%** |
| WriteInsert / heavy-multi | 30,051 | 33,027 (isolated) | 38,071 | +26.7% | +15.3% |
| Update, single/overlap | 242.9 | 243.4 | 246.4 | +1.4% | +1.2% |
| Update, single/non-overlap | 239.9 | 246.0 | 234.6 | −2.2% | −4.6% |
| Update, heavy/overlap | 27,238 | 26,264 | 30,964 | +13.7% | +17.9% |
| Update, heavy/non-overlap | 28,349 | 26,983 | 32,225 | +13.7% | +19.4% |
| Mixed 80/20, single/overlap | 1,217 | — | 1,113 | −8.5% | — |
| Mixed 80/20, single/non-overlap | 1,184 | — | 1,125 | −5.0% | — |
| Mixed 80/20, heavy/overlap | 116,327 | 128,072 | 180,369 | +55.1% | +40.8% |
| Mixed 80/20, heavy/non-overlap | 119,691 | 133,400 | 182,157 | +52.2% | +36.5% |
| Transaction, single/overlap | 241.6 | — | 223.6 | −7.5% | — |
| Transaction, single/non-overlap | 241.0 | — | 226.2 | −6.1% | — |
| Transaction, heavy/overlap | 16,631 | 19,083 | 21,292 | +28.0% | +11.6% |
| Transaction, heavy/non-overlap | 23,957 | 30,543 | 36,320 | +51.6% | +18.9% |

`—` = not published in that run's write-up.

## Durability modes (medians)

| Mode | Workload | base single/heavy | prev single/heavy | now single/heavy |
|---|---|---:|---:|---:|
| sync-full | insert | 245.1 / 30,067 | 242.2 / 38,569 | **226.8** / 40,986 |
| sync-full | update | 239.1 / 27,067 | 241.6 / 38,992 | **222.2** / 42,684 |
| sync-full | transaction | 231.0 / 18,065 | 240.8 / 19,466 | **217.5** / 20,854 |
| async-100ms | insert | 29,039 / 27,306 | 39,323 / 34,633 | 39,374 / 36,153 |
| async-100ms | update | 28,361 / 26,225 | 34,576 / 37,305 | 35,879 / 38,405 |
| async-100ms | transaction | 28,574 / 16,192 | 42,253 / 18,852 | 41,946 / 19,527 |
| memory-only | insert | 34,052 / 29,332 | 48,095 / 38,149 | 51,928 / 39,167 |
| memory-only | update | 32,313 / 28,158 | 47,145 / 41,550 | 51,648 / 42,893 |
| memory-only | transaction | 32,185 / 16,319 | 46,759 / 19,931 | 49,907 / 21,158 |

## Findings

**Heavy-multi-user throughput keeps improving across the board** — every heavy-multi row is
at or well above both base and the previous run, most by double digits. Nothing here
contradicts the story so far.

**A consistent single-user regression on the sync-full / default write path.** Four
independent single-user rows that all exercise `fdatasync`-backed durability (the default)
moved down together, all with tight, non-overlapping sample spreads:

| Row | prev | now | samples (now) |
|---|---:|---:|---|
| WriteInsert / single-user | 236.9 | 210.0 | 210.6, 210.0, 213.3, 204.5, 198.1 |
| Durability/sync-full/insert/single | 242.2 | 226.8 | 226.8, 225.5, 228.8, 228.7, 223.7 |
| Durability/sync-full/update/single | 241.6 | 222.2 | 222.2, 236.4, 216.9, 227.3, 219.4 |
| Durability/sync-full/transaction/single | 240.8 | 217.5 | 223.0, 217.5, 225.1, 208.2, 212.4 |

Single-user rows are the control (one caller, no contention, ≤8% spread historically) — these
four are 6-13% down with no overlap against the previous run's numbers, and it's specific to
sync-full: the **same single-user rows under async-100ms and memory-only durability are flat**
(±4%) against the previous run. That isolates it to something on the per-commit fsync path,
not a general single-threaded slowdown. Not bisected — worth a look before it's assumed to be
noise. Candidates given what landed in the 153 commits since `73971fd`: the delta-segment
rotation/preallocation path, or added per-commit bookkeeping from cross-namespace transactions
or the distributed sync layer that runs even for a single local writer.

**`Mixed 80/20` and `Transaction` single-user rows are down 5-8% vs base** (not compared to
previous — not published there), consistent with the same sync-full path since both mixes
include commits.

## Correction (2026-09-22, same day): the single-user "regression" doesn't isolate — retracted

Attempted to bisect the sync-full single-user finding above. Before bisecting, sanity-checked
the two endpoints (`73971fd`, `73d72af`) by running the affected benchmarks **alone**
(`go test -bench 'BenchmarkWorkloadDurability/sync-full/insert/single-user'`, no other rows in
the same process), `-count=10` each, back to back:

| Row | commit | isolated median (n=10) | isolated range |
|---|---|---:|---:|
| Durability/sync-full/insert/single-user | `73971fd` (was "good") | 228.7 | 216.5–236.7 |
| Durability/sync-full/insert/single-user | `73d72af` (was "bad") | 234.2 | 219.8–242.0 |
| WriteInsert/single-user | `73d72af` (was "bad") | 233.6 | 213.9–246.3 |

**`73d72af` is not lower than `73971fd`** on the isolated durability row — if anything it's
slightly higher — and `73d72af`'s isolated `WriteInsert/single-user` (233.6) matches the
previous run's own isolated re-measurement (236.9) closely, not the 210.0 the full-matrix run
reported for the same commit. The "regression" only appeared when comparing two different
**full 180-row, 24-minute matrix runs** against each other; it does not reproduce commit-to-
commit in isolation.

**Conclusion: retracted. No code regression found, no bisect needed.** The likely cause is
run-position/accumulated-state noise on single-user rows during a long full-matrix run — the
same class of effect this project's docs have previously documented only for heavy-multi-user
Update/Mixed/Transaction rows (Trap 2 in
[2026-09-06-suite-rerun.md](2026-09-06-suite-rerun.md)) — apparently wide enough to also move
single-user rows by more than the assumed ≤8% band, at least occasionally. **Lesson:** a
delta between two full-matrix runs, even on rows previously assumed stable, should be spot-
checked with an isolated back-to-back A/B before being reported as a regression.
