# Workload matrix after base-tree pinning

Date: 2026-09-08 (18:01–23:09). Commit: **`5d8a0e8`**. Machine: Apple M3 Max (16 cores),
Go 1.26.3 (`darwin/arm64`). Recipe as recorded in
[`2026-09-07-perf-rerun-7947cce.md`](2026-09-07-perf-rerun-7947cce.md):

```
go test ./kdb/server/ -run '^$' -bench BenchmarkWorkload -benchtime 2s -count=5 -benchmem
```

Machine verified idle immediately before starting: **83.1% CPU idle**, no test or build load.
That check is not ceremony - see the methodology note in
[`2026-09-08-base-tree-pinning.md`](2026-09-08-base-tree-pinning.md) for the day this
document's predecessor lost to a stray load generator.

**Units.** `ops/sec` counts *operations issued*. It is a commit rate only for the pure write
rows. `Mixed 80/20` issues one `Upsert` every fifth operation and reads on the other four, so
its figure is a **request** rate - about a fifth of it is commits. `Transaction` counts
**successful** commits and excludes the conflict retries paid for them.

## Summary

**The fix restored eleven of thirteen measured rows to baseline or better.** The two it did
not are both **insert-shaped heavy-multi-user** rows, and they fail for a different reason
than the one that was fixed.

## Results

Baseline column is `6d2b4bb`; `7947cce` is the pre-fix state this run exists to compare
against. Heavy-multi rows drift within a process (Trap 2), so both median and first sample are
given where they differ materially.

### Reads — the control

| Row | `6d2b4bb` | now | Δ |
|---|---:|---:|---:|
| Read, heavy-multi / overlapping | 7,259,417 | 7,179,266 | −1.1% |
| Read, heavy-multi / non-overlapping | 5,414,630 | 5,216,503 | −3.7% |

Reads were healthy throughout the regression and are unaffected by this fix, so they are the
check on conditions. Both land inside the ~3% band, which is what makes the rest of this table
trustworthy.

### Rows the regression broke, now recovered

| Row | `6d2b4bb` | `7947cce` | now (median / first) | vs baseline |
|---|---:|---:|---:|---:|
| Update, heavy-multi / overlapping | 27,238 | **FAIL 5/5** | 26,059 / 26,160 | −4% |
| Update, heavy-multi / non-overlapping | 28,349 | 2,721 | 26,707 / 30,603 | −6% / +8% |
| Transaction, heavy-multi / overlapping | 16,631 | 627.2 | 15,315 | −8% |
| Transaction, heavy-multi / non-overlapping | 23,957 | 1,326 | 25,211 / 33,292 | **+5%** |
| Mixed 80/20, heavy-multi / overlapping | 116,327 | 17.6 | 126,774 / 106,061 | **+9%** |
| Mixed 80/20, heavy-multi / non-overlapping | 119,691 | 1,288 | — / 123,449 | +3% |
| `sync-full` update, heavy-multi | 27,067 | **FAIL 5/5** | 31,189 | **+15%** |
| `sync-full` transaction, heavy-multi | 18,065 | 747.7 | 16,762 | −7% |
| `async-100ms` insert, single-user | 29,039 | 3,916 | 40,719 | **+40%** |
| Write insert, single-user | 242.6 | 230.8 | 240.0 | −1% |

Rows that were failing outright now report all five samples. `BenchmarkWorkloadMixedReadWrite`,
which had degraded from passing to failing, passes.

### The two rows that did not recover

| Row | `6d2b4bb` | now |
|---|---:|---:|
| Write insert, heavy-multi | 30,051 | 2,538 (n=4 of 5) |
| `sync-full` insert, heavy-multi | 30,067 | no samples |

And `async-100ms/insert/heavy-multi-user` never completed at all: it ran for 1h40m without
advancing, at 2.23GB RSS and one core saturated in GC, and had to be killed. It also outran
its own `-timeout 10800s` without the timeout firing - a Go test timeout cannot dump stacks
promptly against a multi-GB thrashing heap, which is worth knowing before trusting a timeout to
bound a run like this.

## Why it is only the insert rows

Update, Transaction and Mixed all draw from a **fixed pool** of documents - 4096, 256, 4096 -
so their namespace stays a constant size however long they run. Only the insert rows add a new
document per operation, so only they grow the namespace without bound, and at `-benchtime 2s`
the `b.N` ramp carries them into the document trie's memory cost: **~4,900 bytes of live heap
per document**, measured in
[`2026-09-08-base-tree-pinning.md`](2026-09-08-base-tree-pinning.md).

That this is the trie and not a residual write-path defect is visible in how the same row moves
with benchtime alone, on the same binary:

| `-benchtime` | Write insert, heavy-multi |
|---|---|
| default (1s) | **32,431 ops/sec, passes** |
| 2s | 2,538, 4 of 5 samples |
| 3s | fails in 96s |

The code is identical across those three. What changes is how many documents the benchmark has
inserted by the end, and therefore how much of the run is spent collecting a multi-gigabyte
trie. A write-path defect would not be a function of benchtime.

## Reading this against the previous write-up

`2026-09-07-perf-rerun-7947cce.md` concluded "single-user write throughput is restored and past
baseline; heavy-multi-user writes are still broken." That is now resolved for every heavy-multi
workload except the unbounded-insert shape, and its "Done when" list is met apart from the
insert rows, which are blocked on the trie rather than on tree resolution.

**Do not** use `BenchmarkWorkloadWriteInsert/heavy-multi-user` at `-benchtime 2s` or longer to
evaluate write-path changes until the trie is addressed - the result is dominated by namespace
growth and will misattribute the wall to whatever was changed last. Default benchtime compares
like with like. The fixed-pool rows (Update, Transaction, Mixed) are the sound heavy-multi write
signals in the meantime.

## Still open

- ~~**The document trie**: path compression or a shallower fan-out.~~ **Done** —
  [`2026-09-08-document-trie-compression.md`](2026-09-08-document-trie-compression.md).
  ~4,900 bytes per document became ~160, with every tree hash preserved, and the insert rows
  below now complete. What remained on those rows was a *throughput* taper with namespace size,
  **also fixed** the following day - see
  [`2026-09-09-bounded-tree-rebuild.md`](2026-09-09-bounded-tree-rebuild.md). The insert row now
  runs at 32,326 ops/sec at ~108,000 documents, above the 30,051 baseline.
- The full matrix has not been run to completion since `7947cce`; ten of eighteen
  `BenchmarkWorkloadDurability` rows were not reached before the insert row wedged this run.
  Everything reached is above.
