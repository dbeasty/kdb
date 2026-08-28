# Write-path allocation fix and group commit

Follow-up to `phase0-baseline.md` and `phases-1-6-summary.md`. Those covered the
blob path (`WriteBlob`) and the transaction engine against the in-memory
adapter. This one covers the path a real `kdb-service` write actually takes -
`KdbServerRuntime.Upsert` against a disk-backed `embed.OpenFileRuntime` - which
had never been benchmarked, and where both problems below were hiding.

Machine: Apple M3 Max (arm64, 16 logical CPUs), macOS (Darwin 25.5.0), APFS on
internal SSD. One run each; a reference point, not a tuned suite. Reproduce with
`make bench-write`.

## What was wrong

**1. ~21MB allocated per commit.** `compression.Compress`
(`go/kdb/compression/zstd.go`) built a brand-new `zstd.Encoder` on every call and
threw it away. `zstd.NewWriter` defaults to `WithEncoderConcurrency(GOMAXPROCS)`
and eagerly allocates one encoder state per slot; at `SpeedDefault` each holds a
`1<<15` table plus a `1<<17` longTable of 8-byte entries = 1.25MiB. On a 16-core
host that is 20.0MiB per call, and `pprof -alloc_space` over a single call
measured 21.7MB.

`Compress` runs once per commit appended to the delta log
(`storage/delta.PageCodec.Frame`) and once **per entry** in an SSTable flush
(`storage/sstable.encodeBlock`), so a 10k-entry flush churned ~200GB. The decoder
in the same file was already shared via `sync.Once`; the encoder never got the
same treatment, and there was no `sync.Pool` anywhere in the Go tree.

Fixed by pooling encoders per level with `WithEncoderConcurrency(1)`.
`BenchmarkCompress` guards it: **21.7MB/op → 419 B/op, 1 alloc/op.**

**2. Every commit serialized behind an unbatched fsync.** `KdbServerRuntime`'s
`writeGate` admits one commit at a time, and `PersistingCommitDAG.Persist` ran
*inside* that exclusive section - framing, compressing, appending, then a full
physical `fsync`, per commit. The `GroupCommitter` built in Phase 1 was wired
only to `ServerEngine.WriteBlob` (blobs); document durability rides on the delta
log, which bypassed it entirely. Server write throughput was therefore
`1/(work + fsync)` no matter how many clients were writing.

Fixed with `embed.commitLogWriter`: commits are queued under the write gate
(queue order is delta-log order, which replay depends on), the gate is released
as soon as that order is fixed, and a single background goroutine appends and
issues **one fsync per drained batch**. Concurrent commits now share a sync.

## Before / after

`BenchmarkFileBackedUpsert` - real disk-backed runtime, end to end.

| Parallelism | ns/op before | ns/op after | writes/sec before | writes/sec after |
|---:|---:|---:|---:|---:|
| 1  | 4,064,415 | 562,978 | 246 | 1,776 |
| 8  | 4,027,363 | 83,311  | 248 | 12,003 |
| 64 | 4,100,002 | 49,216  | 244 | 20,319 |

Throughput was **flat** across parallelism before - adding writers bought
nothing, which is the signature of a single lock held across disk I/O. It now
scales with concurrency: **80x at 64-way**.

The mechanism, measured directly: the batch flush is recorded under
`metrics.StageFsyncWait` (so it also shows up on the admin `/metrics` endpoint -
until now only blob writes did), and at parallel-8 the benchmark reports

```
stage=fsync_wait  count=19  mean=4.23ms  p50=4.24ms  p99=4.65ms
```

**19 physical fsyncs for 1000 commits.** A single fsync costs ~4.2ms on this
machine's APFS, which is exactly why one-fsync-per-commit capped the server at
~246 writes/sec no matter what else was optimized.

| | before | after |
|---|---:|---:|
| B/op (parallel-1) | 21,457,761 | 21,206 |
| allocs/op | 487 | 192 |

**1,011x less memory per write.** For scale, MongoDB does an equivalent write in
roughly 7-25KB; this path is now in that band rather than three orders of
magnitude outside it.

Secondary allocation fixes contributing to the B/op figure, in rough order of
size, all confirmed by `pprof -alloc_space` on this benchmark:

- `document_tree_trie.go`'s `leafHash`/`internalHash` built their SHA-256
  preimage in a `make()`d slice per trie level; 32 levels deep, that was ~16KB of
  garbage per single-document put. Now a stack array (hash output unchanged -
  the cross-language parity vectors still pass).
- `codec.encodeRecord` copied and sorted the schema field list on every record
  encode, though `Registry.Freeze` already froze it and it is declared in id
  order. Now checks `sort.SliceIsSorted` first.
- `encodeLeb128U32`/`lebPrefix` allocated a throwaway slice per varint and per
  string - every record field emits at least one. Replaced with
  `appendLeb128U32`/`appendLebBytes`/`appendLebString` writing straight into the
  destination. `appendLebString` also drops a `[]byte(string)` conversion that
  copied the entire document body on every encode.
- `InMemoryCommitDag.putCommitLocked` re-encoded and re-hashed the commit payload
  to verify a hash `BuildCommit` had derived from those exact fields microseconds
  earlier. Skipped on the local-append path; still verified for commits arriving
  from delta replay or a peer.
- `shardedPendingStore.TakeAllAndClear` reallocated both maps in all 64 shards
  per commit (128 map headers to move one document). Now skips empty shards.
- `storage/io.OSByteStore.Read` allocated exactly the requested length, and
  `delta.scanSegmentRef` requested `1<<28` - a literal 256MiB allocation per
  segment scanned, whose backing array the returned `buf[:n]` then retained.
  Reads are now clamped to the file's real size.

`BenchmarkCommitScalingWithHistorySize` (in-memory adapter, unchanged code path)
also improved as a side effect: 16,118 B/op / 247 allocs/op → ~11,000 B/op /
144 allocs/op.

## Durability is now configurable

`storage.StorageEngineConfig` had `Durability`, `AsyncSyncIntervalMillis` and
`CompressionCodec` since Phase 4/5, and the engine honored them - but
`embed/file.go` hardcoded them at construction and `cmd/kdb-service` called the
non-options `OpenFileRuntime`, so no config file, environment variable, or flag
could reach them. They now thread through the existing
defaults < file < env < flags chain:

| Setting | Flag | Env | Default |
|---|---|---|---|
| durability | `--durability` | `KDB_DURABILITY` | `sync` |
| async sync period | `--async-sync-interval-ms` | `KDB_ASYNC_SYNC_INTERVAL_MS` | `5` |
| codec | `--compression` | `KDB_COMPRESSION` | `zstd` |

| Mode | Caller returns when | Loss window on crash |
|---|---|---|
| `sync` (default) | its record's fsync completes, batched with concurrent commits | none for acknowledged writes |
| `async` | its record is applied in memory and queued | whatever had not been flushed |
| `memory` | in-memory only; the delta log is skipped | everything since start |

An unknown name fails at startup rather than silently falling back.

## On-disk format: v2 pages

Both the KDBP delta page header (16 → 20 bytes) and the SSTable block header
(12 → 16 bytes) now carry a format version and a codec id.

Previously neither recorded its codec: readers applied whatever codec they were
*configured* with, so changing `compression` made every existing segment
unreadable, and `integrity.Verify` could not distinguish a codec mismatch from
real corruption - it said exactly that in its own doc comment. The SSTable
decoder was worse, inferring "was this compressed?" from `compSize == uncompSize`,
which is wrong for any payload whose compressed form happens to match its
original size.

Consequences, all verified by tests:

- A namespace written with `compression=none` reopens fine under `compression=zstd`
  and vice versa (`TestCompressionCodecIsConfigurableAndReadable`).
- One segment may mix codecs (`TestPageCodecMixedCodecsInOneSegment`).
- `kdb-inspect verify`, `repair-segments` and `backup` no longer take `--codec`;
  the flag existed only to tell the reader something the bytes now state. It
  remains on `restore`, which chooses the codec to *write*.
- An unknown codec id or a future version byte is refused loudly instead of
  guessed at.

This is a breaking on-disk change with no migration path - deliberate, since the
product is pre-release. Go and Kotlin were changed together
(`storage/delta`, `storage/sstable` on both sides); `TestKotlinPutThenGoGet_InteropDelta`
covers Kotlin-writes/Go-reads, and the `go/testdata/golden` wire-codec corpus is
unaffected (it is codec-level, not page-level).

## Reproducing

```
make bench-write                  # write path, all layers, with -benchmem
make bench                        # every Go benchmark with -benchmem
cd go && go test ./kdb/compression/ -run '^$' -bench BenchmarkCompress -benchmem
```

To re-derive the original 21MB figure, revert `Compress` to constructing a
`zstd.NewWriter` per call and run any test that compresses under
`-memprofile`, then `go tool pprof -alloc_space -top`.

## Still open

Deliberately not addressed by this work, all pre-existing.

The first four items below were **closed by `01d0654`** ("Fix the storage-layer
correctness issues left open by the write-path work"), which also fixed an
`LsmBlobStore` data race and a `Manager.Flush` ordering bug found while doing
them. They are kept here, struck through, so this document still reads as the
record of what the write-path PR knowingly deferred:

- ~~`storage/memtable`'s `Delete` writes a `nil` tombstone but `Get` treats `nil`
  as absent and falls through to the SSTable, so a delete does not shadow an
  older entry.~~ **Correctness bug.** Fixed: the table stores an explicit
  `{value, deleted}` slot and exposes `Lookup`. The tombstone still does not
  survive a flush - that needs an SSTable format change originating on the
  Kotlin side.
- ~~`SortedTable.SizeBytes` only ever increases; overwrites and deletes never
  subtract, so the flush trigger's accounting drifts upward permanently.~~
  Fixed: both `Put` and `Delete` net out against the previous slot.
- ~~WAL segment rotation never fires - `walMaxSegmentBytes` is stored but never
  compared against `segmentSize`.~~ Fixed: the WAL is now a chain of segments
  that seals and rotates at the cap.
- ~~`index`'s `replayBuckets` rebuilds the whole index from the event log on
  every lookup, calling `dag.IsAncestor` (itself a full ancestor-closure build)
  per event. Read path, and byte-for-byte duplicated in two files.~~ Fixed: both
  copies replaced by a shared memoizing `eventLog`, walking the closure once per
  replay via `dag.AncestorSet`.

Confirmed not to have cost anything on the write path: an interleaved A/B of
`BenchmarkFileBackedUpsert` across `01d0654` measures equivalent within
run-to-run variance at every parallelism, with `192 allocs/op` held throughout.
See `docs/benchmarks/lightsail-sim/README.md`'s 2026-08-27 update.

Still genuinely open:

- The write gate is still capacity-1. Its exclusive section no longer covers disk
  I/O, but making it genuinely concurrent needs per-transaction staging in
  `storage.Adapter` rather than `ServerEngine`'s single shared `pending` store -
  a much larger change, and worth re-measuring for first now that the fsync is
  out of the way.
- Unbounded in-memory DAG retention (kdb-spec-layer13 §10) still needs compaction.
