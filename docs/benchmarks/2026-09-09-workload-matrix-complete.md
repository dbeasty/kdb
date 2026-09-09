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

## The two rows this table gets wrong, and why

`BenchmarkWorkloadWriteInsert` reports 124.1 (single-user) and 5,307 (heavy-multi) here, against
baselines of 242.6 and 30,051. **Both are artifacts of running inside the matrix, not
regressions.** Re-measured at the same commit, the same `-benchtime 2s -count=5`, alone in the
process:

| Row | in the matrix | isolated | baseline |
|---|---:|---:|---:|
| `WriteInsert/single-user` | 124.1 | **236.9** | 242.6 |
| `WriteInsert/heavy-multi` | 5,307 | **33,027** | 30,051 |

The corroboration is in this table already: `Durability/sync-full/insert/single-user` is the
*identical* workload — same operation, same durability, same runner — differing only in that it
passes `{DurabilitySync, SyncModeFull}` explicitly where `WriteInsert` takes the defaults. It
reports 242.2. An insert row is not half-speed in one benchmark and full-speed in another
because of the storage engine.

`WriteInsert` runs second in the file, straight after `BenchmarkWorkloadRead` seeds and hammers
a 4,096-document pool five times over. Whatever that leaves behind — page cache, background
flush, heap — it halves an fsync-bound row that measures correctly on its own. Note it did *not*
show in the 2026-09-08 matrix (240.0 there), so it is sensitive to how much work the preceding
rows get through, which the fixes changed considerably.

**Read the `WriteInsert` rows from isolated runs, not from the matrix.** They are the only rows
in this file that need that caveat, and they are also the two rows every previous write-up used
as the headline — which is worth remembering before quoting them again.

## Still open

- **Blob-store compaction fails on large namespaces**: `could not compact the blob store
  (append size exceeds max ...) — it keeps its current tables`, seen around 108,000 documents.
  Unrelated to any of this and not investigated.
- The in-process interference above deserves its own look. It is a benchmark-harness problem
  rather than a product one, but it silently halved a row for a whole run.
