# Workload matrix, complete for the first time since the regression

Date: 2026-09-09 (09:10–09:37). Commit: **`73971fd`**. Machine: Apple M3 Max (16 cores),
Go 1.26.3 (`darwin/arm64`). CPU idle **82.6% before, 82.0% after** — recorded at both ends, so
the conditions are part of the record rather than an assumption.

```
go test ./kdb/server/ -run '^$' -bench BenchmarkWorkload -benchtime 2s -count=5 -benchmem
```

Same recipe as [`2026-09-08-workload-matrix-after-pinning.md`](2026-09-08-workload-matrix-after-pinning.md),
so every row compares directly.

## The headline

| | 2026-09-08 run | this run |
|---|---|---|
| Rows measured | 119 (incomplete) | **180 (all of them)** |
| `deadline exceeded` errors | 4,823 | **0** |
| Wall clock | 5h+, then wedged and killed | **26m51s, `ok`** |
| Rows with missing samples | 2 | **0** |

Every row reports all five samples. The package passes. The previous run never finished: it
wedged on `async-100ms/insert/heavy-multi-user` at 2.23GB, taking the last ten durability rows
with it. That row now runs at 34,633 ops/sec.

## Results against the `6d2b4bb` baseline

Medians. Baselines for heavy-multi rows are first-sample figures (those rows drift within a
process — Trap 2), so small negatives there are not necessarily real.

### Reads — the control

| Row | baseline | now | Δ |
|---|---:|---:|---:|
| single-user / overlapping | 1,391,142 | 3,617,025 | **+160%** |
| single-user / non-overlapping | 4,019,925 | 5,401,010 | +34% |
| heavy-multi / overlapping | 7,259,417 | 6,972,877 | −3.9% |
| heavy-multi / non-overlapping | 5,414,630 | 5,371,865 | −0.8% |

Untouched by any of this work, and inside the noise band — which is what makes the rest of the
table worth reading.

### Writes

| Row | baseline | `7947cce` | now | Δ vs baseline |
|---|---:|---:|---:|---:|
| Update, single-user / overlapping | 242.9 | 223.6 | 243.4 | +0.2% |
| Update, single-user / non-overlapping | 239.9 | 220.4 | 246.0 | +2.5% |
| Update, heavy-multi / overlapping | 27,238 | **FAIL 5/5** | 26,264 | −3.6% |
| Update, heavy-multi / non-overlapping | 28,349 | 2,721 | 26,983 | −4.8% |
| Mixed 80/20, heavy-multi / overlapping | 116,327 | 17.6 | 128,072 | **+10%** |
| Mixed 80/20, heavy-multi / non-overlapping | 119,691 | 1,288 | 133,400 | **+11%** |
| Transaction, heavy-multi / overlapping | 16,631 | 627.2 | 19,083 | **+15%** |
| Transaction, heavy-multi / non-overlapping | 23,957 | 1,326 | 30,543 | **+27%** |

`Mixed 80/20` is a **request** rate, roughly a fifth of it commits; `Transaction` counts
successful commits and excludes conflict retries. See the units note in the previous write-up.

### Durability modes

| Mode | Workload | baseline single / heavy | now single / heavy |
|---|---|---:|---:|
| sync-full | insert | 245.1 / 30,067 | 242.2 / **38,569** |
| sync-full | update | 239.1 / 27,067 | 241.6 / **38,992** |
| sync-full | transaction | 231.0 / 18,065 | 240.8 / 19,466 |
| async-100ms | insert | 29,039 / 27,306 | **39,323** / **34,633** |
| async-100ms | update | 28,361 / 26,225 | **34,576** / **37,305** |
| async-100ms | transaction | 28,574 / 16,192 | **42,253** / 18,852 |
| memory-only | insert | 34,052 / 29,332 | **48,095** / **38,149** |
| memory-only | update | 32,313 / 28,158 | **47,145** / **41,550** |
| memory-only | transaction | 32,185 / 16,319 | **46,759** / 19,931 |

Every durability row is at or above baseline, most of them well above. `sync-full/insert/heavy`
produced **no samples at all** in the previous run and is now 28% above baseline.

## The two rows this table got wrong — root-caused and fixed

As first published, `BenchmarkWorkloadWriteInsert` reported 124.1 (single-user) and 5,307
(heavy-multi) here, against baselines of 242.6 and 30,051. Re-measured alone in the process at
the same commit and flags, the same rows gave 236.9 and 33,027. The corroboration was in this
table already: `Durability/sync-full/insert/single-user` is the *identical* workload, differing
only in passing `{DurabilitySync, SyncModeFull}` explicitly where `WriteInsert` takes the
defaults, and it reported 242.2.

**The cause was the harness, in `newWorkloadServerWithOptions`:**

```go
b.Cleanup(func() { rt.Close() })   // inside a function called once per ramp iteration
```

The framework calls a `b.Run` closure repeatedly as it ramps `b.N` (1, then 100, then ...), but
cleanups registered on a benchmark do not run until it and all its subtests finish. So every
earlier iteration's runtime stayed open alongside the one being measured - WAL, background
flushers, group-commit timers - with its temp directory still on disk.

Only `BenchmarkWorkloadWriteInsert` and `BenchmarkWorkloadDurability` open servers inside a
`b.Run` closure; every other benchmark here opens one in parent scope, where `b.Cleanup` is
exactly right. That is why these two rows were affected and the rest of the table was not.

The helper now returns an idempotent closer that the in-closure sites defer. Re-running the
whole matrix with that change, on the same machine:

| Row | before | after | isolated | baseline |
|---|---:|---:|---:|---:|
| `WriteInsert/single-user` | 124.1 | **231.4** | 236.9 | 242.6 |
| `WriteInsert/heavy-multi` | 5,307 | **32,235** | 33,027 | 30,051 |

Both now agree with their isolated measurements. Every other row moved by a few percent or not
at all - `Durability/sync-full/insert` 242.2 -> 241.9 and 38,569 -> 38,199, `Update/heavy-multi`
26,264 -> 26,893 - which is the check that this fixed a measurement defect rather than changing
what was measured.

Two things worth carrying forward. First, three separate attempts to reproduce the depression
outside a full matrix run all came back normal, including one at the exact commit; the defect
only shows with the whole set running, so "I could not reproduce it" was not evidence of
absence. Second, one of those attempts was invalid: `go test -bench` splits its regex **per path
segment**, so `-bench 'BenchmarkWorkloadRead|BenchmarkWorkloadWriteInsert/single-user'` silently
restricts *Read* to its single-user rows too. It is easy to get wrong in precisely the direction
that makes an experiment prove nothing.

## Still open

- ~~**Blob-store compaction fails on large namespaces**~~ — **fixed** by PR #54. The SSTable
  footer carries a text line per key and was written in a single append, so it outgrew the 16MB
  per-append ceiling at about 200,000 keys and every compaction failed silently. Worth noting
  the run below is what surfaced it.
- ~~The in-process interference above deserves its own look.~~ **Fixed** — see the section
  above. It was a benchmark-harness problem rather than a product one, but it silently halved
  one row and cut another to a sixth for a whole run.
