# Commit log: one write per drained batch, not per commit

Date: 2026-10-13. Base: `origin/main` 529408e (after #164). Machine: the development Mac, load average 4.7–9 during
the runs, from other jobs. Every run went through the shared benchmark lock, so no two measurements overlapped.

## Change

`commitLogWriter.appendBatch` (`go/kdb/embed/commit_log.go`) drains up to 256 queued records, then called
`DefaultWriter.Append` once per record: one positional write syscall per commit.

`DefaultWriter.AppendBatch` (`go/kdb/storage/delta/writer.go`) frames each record exactly as `Append` does and hands
the frames to the platform in as few writes as its append cap allows, usually one. The bytes on disk and every
record's offset are unchanged:
- `TestAppendBatchWritesWhatAppendWrites` compares whole segments, with zstd and without compression.
- `TestAppendBatchSplitsAtTheAppendCap` covers batches larger than the append cap.
- `TestAppendBatchFailureReportsTheWrittenPrefix` checks that a failed write reports exactly what reached the
  segment.

In `TestCommitLogBatchesWritesAndReportsEveryOffset`, 2,000 queued records went out in 10 writes, and every reported
offset reads back as its commit.

## Node-to-node sync, 200k commits (`TestSyncScale`, `KDB_SYNC_SCALE=200000`)

The receiver's adoption logs every fetched commit at once, which is exactly the queue-then-wait pattern a batch
serves. Alternating runs:

| run | base join | batched join | base catch-up | batched catch-up |
|---|---|---|---|---|
| 1 | 73.2 s | **12.9 s** | 63.4 s | **14.9 s** |
| 2 | 46.6 s | **10.0 s** | 44.0 s | **14.0 s** |
| 3 | 58.5 s | **9.8 s** | 60.8 s | **11.1 s** |

That is about 5× faster. Allocation is unchanged: 6.6–6.7 GB for a join and 7.6–7.7 GB for a catch-up, on both
sides.

## Server workload (`BenchmarkWorkload*`, `-count 6`, benchstat)

| benchmark | change |
|---|---|
| `WorkloadRead`, all four variants | ~ (one +0.9%, p=0.04); still 176 B/op and 4 allocs/op |
| `WorkloadWriteInsert/heavy-multi-user` | **-3.3% time** (p=0.002) |
| `WorkloadWriteInsert/single-user` | ~ |
| `WorkloadMixedReadWrite`, all variants | ~ |

A single writer gains nothing, because each commit is its own batch. Concurrent writers gain a little.
