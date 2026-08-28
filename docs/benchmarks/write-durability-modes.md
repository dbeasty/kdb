# Closing the write gap to MongoDB: sync modes and interval journal flushing

Follow-up to `write-path-allocation-fix.md`. That work took concurrent write
throughput from ~250/s to ~20k/s by batching the commit-log fsync across
writers - but a **single sustained writer** still paid one full physical fsync
per commit, ~4.2ms on Apple hardware, because group commit has nobody to share
with when only one writer exists. The zolik port
(github.com/dbeasty/zolik PR #45) benchmarked exactly that shape and measured
KDB inserts at 4.0-4.3ms against Mongo's 190-235µs - "Mongo ~20x faster".

That gap is not I/O speed; it is a durability-guarantee mismatch:

- KDB (`durability=sync`, the default) acknowledges a write only after the
  commit is physically forced to media. On macOS, Go's `os.File.Sync` issues
  `F_FULLFSYNC` - a full drive-cache flush, ~4ms on Apple SSDs.
- MongoDB's default write concern (`w:1, j:false`) acknowledges from memory
  and fsyncs its journal every ~100ms. Its ~200µs is a network round trip plus
  in-memory apply, and a crash can lose up to 100ms of acknowledged writes.

This change makes the tradeoff explicit and configurable on two independent
axes, instead of hardwiring the most expensive point in the space.

## Axis 1: `syncMode` - what a physical sync means

`--sync-mode` / `KDB_SYNC_MODE` / `syncMode` (config file), or
`embed.StorageOptions.SyncMode`:

| Mode | Primitive (darwin / linux) | Survives | Cost on Apple SSD |
|---|---|---|---:|
| `full` (default) | `F_FULLFSYNC` / `fsync` | power loss | ~4ms |
| `fast` | `F_BARRIERFSYNC` / `fdatasync` | process + OS crash; power loss can lose what the drive cache held | ~140-300µs |

`fast` is the guarantee most databases actually run with on macOS - SQLite
(`fullfsync` off is its default) and PostgreSQL both issue plain
`fsync`/barrier there, not `F_FULLFSYNC`. The barrier variant preserves
cross-file write ordering, which is what log-structured recovery needs from a
sync; on unsupported filesystems it falls back to the full sync rather than
not syncing. Implemented in `storage/io/sync_{darwin,linux,other}.go`,
selected by `storage/io.SyncMode`, honored by every segment flush (commit log,
blob WAL, SSTable).

## Axis 2: interval flushing under `durability=async`

`durability=async` already acked writes without waiting for the flush - but
`embed.commitLogWriter` still issued a physical flush after **every drained
batch**, so a single sustained writer drove one background fsync per commit:
the ack was async, the disk traffic wasn't. The drain loop now appends records
as they arrive (into the OS page cache - an acknowledged write survives a
process crash immediately) and flushes at most once per
`asyncSyncIntervalMs` (`embed.StorageOptions.AsyncSyncIntervalMillis`,
default 5ms), with a timer catching the tail when writes stop and Close
still draining + flushing everything. Crash-loss window: unchanged in kind
("whatever had not been flushed"), now bounded by the interval instead of by
batch timing. Set it to 100 for exact Mongo-default journaling semantics.

Recovery is the same delta-log replay either way - `OpenFileRuntime` replays
whatever was flushed; there is no partial-record exposure because framing is
length-prefixed and a torn tail frame is skipped.

## Measured: `BenchmarkFileBackedUpsertModes`

End-to-end `KdbServerRuntime.Upsert` against a real disk-backed runtime,
Apple M3 Max, macOS (Darwin 25.5.0), APFS on internal SSD, 2000 iterations
per row (`go test ./kdb/server -run '^$' -bench
BenchmarkFileBackedUpsertModes -benchtime 2000x -cpu 1,16 -benchmem`).

Single-threaded (`-cpu 1`, the zolik-benchmark shape - group commit cannot
help here):

| Mode | ns/op | writes/s | vs sync-full | fsync p50 |
|---|---:|---:|---:|---:|
| sync-full (old default behavior) | 4,097,994 | 244 | 1x | 3.94ms |
| sync-fast | 366,586 | 2,728 | **11x** | 142µs |
| async-100ms | 38,594 | 25,911 | **106x** | (background) |

16 logical CPUs, parallel-8:

| Mode | ns/op | writes/s |
|---|---:|---:|
| sync-full | 86,738 | 11,529 |
| sync-fast | 27,228 | 36,727 |
| async-100ms | 24,998 | 40,003 |

Against the zolik PR's Mongo baseline (190-235µs per insert, localhost):

- `sync-fast` at ~367µs single-threaded is in Mongo's band while giving a
  **stronger** guarantee than Mongo's default (KDB: acked ⇒ synced to the
  device; Mongo: acked ⇒ in memory, journaled within 100ms).
- `async` at ~39µs is ~5x faster than Mongo's insert with the **same**
  guarantee class as Mongo's default write concern - and no network hop.

## What was deliberately not done

- **Defaults unchanged**: `durability=sync` + `syncMode=full`. Choosing to
  trade power-loss protection for speed is an operator decision; nothing
  changes for existing deployments.
- **Segment preallocation** (etcd-style: preallocate WAL/delta segments so
  appends don't grow the file): would let `fdatasync` on Linux skip the
  file-size metadata commit entirely. Worth doing when a Linux deployment
  shows fdatasync-bound writes; irrelevant to the macOS numbers above.
- **Write coalescing for modify-heavy workloads** (the same document updated
  repeatedly within one flush interval writing only its final version):
  a genuine physical-write reducer under async mode, but it changes delta-log
  contents (replay would no longer see intermediate commits) and interacts
  with the commit DAG; needs its own design pass.
- **The auth registry's own store** still opens with `full` sync - auth
  writes are rare and small; not worth a knob until measured otherwise.
