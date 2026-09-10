# Delta segment preallocation

How to stop every durable KDB write paying for a filesystem metadata commit, and what it costs
to get there.

Opened from the "Still open" list in [`benchmarks/write-path-allocation-fix.md`](benchmarks/write-path-allocation-fix.md),
which deferred this with a trigger condition: *"worth doing when a Linux deployment shows
fdatasync-bound writes."* That condition is now met — see §1.

---

## 1. Why: the measurement that opened this

Measured 2026-09-10, Linux container (2 vCPU / 1 GiB, the small-instance target),
`BenchmarkAsyncFlushInterval` and `BenchmarkFileBackedUpsertModes`, `-benchtime 2000x -count=5`:

| Mode | median ns/op | writes/s |
|---|---:|---:|
| `sync-full` | 564,659 | 1,771 |
| `sync-fast` | 570,677 | 1,752 |
| `async-1ms` | 40,993 | 24,394 |

**`syncMode=fast` is a no-op on Linux.** It measured *slower* than `full`, across 5 samples at
both 1 and 2 vCPU (564,659 vs 570,677 at 1; 567,571 vs 568,627 at 2). The knob is an 11x win on
macOS and worth nothing on the platform we actually ship to.

The cause is not the sync primitive. It is that **the delta segment grows on every append**:

- `fdatasync` skips only metadata *not needed to read the data back*.
- File **size** is exactly that metadata — bytes past EOF are invisible after a crash.
- So on a growing file `fdatasync` must commit the size anyway, and collapses into `fsync`.

For comparison, MongoDB preallocates its journal. Inspected in a live `mongo:7` container:

```
-rw------- 1 mongodb mongodb 104857600 WiredTigerLog.0000000001
-rw------- 1 mongodb mongodb 104857600 WiredTigerPreplog.0000000001
-rw------- 1 mongodb mongodb 104857600 WiredTigerPreplog.0000000002
```

One 100 MB journal file that never changes size, plus **two pre-created spares** so the zeroing
never lands on the critical path. Its `j:true` insert costs 122-176µs *including a TCP round
trip*, against our 564µs in-process. The same mechanism explains both facts.

Our own `fsync_wait` distribution shows the signature: p50 ~270µs but p99 spiking to 3.8ms and
max 13ms — tail latency characteristic of contending on the filesystem's metadata journal, not
of the data write.

---

## 2. What is already true (and makes this cheaper than it looks)

Three properties of the current code mean this is mostly a write-path change, not a format change:

1. **Segment size is already a logical in-memory counter, not a `stat()`.**
   `OSByteStore.Append` does `atomic.AddInt64(&seg.size, int64(len(bytes)))`
   (`storage/io/os_store.go:95`). Only `openFor` seeds it, from `f.Stat()`
   (`os_store.go:81`). So the writer's notion of "how full is this segment" survives the
   physical file being larger than the data in it — **provided `openFor` seeds it from the
   logical end** rather than the file size. That is the single load-bearing change.

2. **The scanner is already torn-tail tolerant.** A frame whose magic is absent, whose declared
   length is garbled, or which runs past the end is treated as a torn tail and stops the scan
   cleanly (`storage/delta/scanner.go`, `CorruptFrameError`). **A preallocated zero region has
   no magic, so it reads as end-of-data by construction.** Recovery needs no new concept.

3. **Segment rotation already exists.** `DefaultWriter.RotateIfNeeded` seals at
   `DeltaMaxSegmentBytes` (64 MiB default) and starts the next segment, landed in v0.5.0. There
   is already a place where "a new segment is created" is a distinct, single event to hook.

---

## 2a. Feature flag and format compatibility

Preallocation is a switch — `PlatformIOConfig.PreallocateBytes`, zero (the default) meaning off
and restoring exactly the previous grow-as-you-go behavior. The requirement is stronger than
"it can be turned off": **a process with it off must read an image written with it on, and vice
versa.** That holds, and for a reason worth stating rather than assuming:

- **It is not a format change.** A preallocated segment is a normal frame sequence followed by
  zeros. Frame bytes, headers, and checksums are untouched.
- **Every reader already stops at the first frame that does not parse** (§2.2). Zeros have no
  frame magic, so they terminate a scan exactly like a torn tail after a crash.
- **The segment ref reports the logical size, not the file size.** `scanSegmentRef` sets
  `SizeBytes` from `walk.ConsumedEnd` — the end of the frames — so downstream reads size
  themselves from real data and do not allocate the padding.
- **Segments are never reopened for appending**, by either setting. `delta.Factory.OpenWriter`
  always starts a new segment (`writer.go:520-526`). This is what makes the above safe rather
  than merely usually-safe: the dangerous case would be a non-preallocating process appending to
  a preallocated file, whose `openFor` would seed its logical size from `stat()` — the padded
  size — and write past the zero gap, leaving records no scan would ever reach. That case cannot
  arise. `openFor` carries a comment saying so, and what would have to change if it ever does.

The direction that needs no argument is the other one: a non-preallocated segment is just a
smaller file, and a process with the flag on treats it normally, since the setting only affects
segments it *creates*.

Covered by `delta/preallocation_compat_test.go`: both single directions, a namespace holding
segments written under *both* settings read back by readers under both settings, and a check
that the physical file really is full-size (a test that would otherwise pass with the feature
silently doing nothing).

**Scope:** delta segments only, keyed on the sequenced-segment file name. SSTables are written
once and legitimately read by file length, so padding one would change what a reader sees; the
WAL is not on the commit-ack path.

## 3. The changes

### 3.1 Writes must become positional (`WriteAt`, not `O_APPEND`)

This is the change that cannot be skipped or deferred.

Segments are opened `os.O_CREATE|os.O_WRONLY|os.O_APPEND` (`os_store.go:65`) and written with
`seg.file.Write(bytes)`. `O_APPEND` writes at the **physical end of file** — which, after
preallocation, is 64 MiB. Every append would land past the intended region and the file would
grow anyway, defeating the entire exercise.

Replace with `seg.file.WriteAt(bytes, logicalOffset)`, where `logicalOffset` is the counter the
store already maintains. Safe today: `FileBackedPlatformIO` serializes `Append` per segment
(`file_backed.go:107-109`, and the comment at `os_store.go:20-24` states the store relies on
that), so we are not depending on `O_APPEND`'s atomicity to order concurrent writers.

### 3.2 Preallocate on segment creation

New `PlatformIOConfig` field, `PreallocateBytes int64` (0 = off, preserving today's behavior).
When `openFor` creates a segment that does not yet exist, size it to `PreallocateBytes` before
returning.

**It must be written zeros, not just sized.** Three ways to make a file 64 MiB, only one of
which works:

| Method | Result |
|---|---|
| `ftruncate` | Sparse file. Writes land in holes, allocate extents, update metadata. **No benefit.** |
| `fallocate` (default flags) | Extents reserved but marked *unwritten*. First write to one flips its state — still a metadata update. **Partial benefit at best.** |
| `fallocate` + write real zeros | Extents allocated **and initialized**. Subsequent writes are pure in-place overwrites. **This is the one.** |

This is why etcd's WAL writes 64 MiB of zeros rather than truncating. Platform primitives:
`fallocate(2)` on Linux, `F_PREALLOCATE` on darwin, and a plain zero-fill fallback where
neither is available. This mirrors the existing `sync_{linux,darwin,other}.go` split, so the
build-tag structure to hang it on is already there.

### 3.3 Seeding the logical end when reopening a segment

With preallocation, `f.Stat().Size()` is always `PreallocateBytes` and tells us nothing. A
reopened segment must find its logical end by **scanning frames until the first invalid one** —
exactly what recovery already does (§2.2), so this reuses `ScanSegmentBytes` rather than
inventing a second notion of "where does the data end".

Deliberately **not** storing the logical end in a header: it would need its own durable write
per append to be trustworthy, which reintroduces the cost we are removing. The scan is O(segment)
once at open, not per write.

### 3.4 Rotation: pre-create the next segment off the critical path

Writing 64 MiB of zeros takes real time (~64ms at 1 GB/s). Doing it inside `RotateIfNeeded`
converts a rare rotation into a visible latency spike, which is a bad trade for a p99.

Keep one spare segment prepared in the background and swap it in on rotation — which is
precisely what MongoDB's two `WiredTigerPreplog` files are for. `RotateIfNeeded` already runs
only between batches (never inside one), so there is a clean, already-safe point to both consume
the spare and kick off preparation of the next.

---

## 4. Correctness concerns

- **`Read` past the logical end now returns zeros, not an error.** Today `Read` clamps to the
  file's real size (`os_store.go:116-117`), so an over-long read is naturally bounded. With a
  preallocated file that clamp stops being a bound on *data*. Every caller that reads "the whole
  segment" and scans must rely on frame validity (§2.2), not on the read length. Audit
  `integrity.Verify`, `kdb-inspect`, and the recovery path specifically.
- **`AvailableBytes` / disk accounting.** A preallocated segment consumes its full size
  immediately. Free-space checks and any pressure-abort thresholds (the exit-75 contract) must
  account for a segment costing 64 MiB the moment it is created, not as it fills.
- **Retention and truncation** plan over *sealed* segments. A sealed segment should be truncated
  down to its logical end so reclaim accounting stays honest and we do not keep 64 MiB files
  holding 3 MiB of commits.
- **Crash mid-preallocation** leaves a partially zeroed file. It has no valid frames, so a scan
  yields an empty segment — safe, but the create path should still be idempotent on reopen.
- **The Kotlin engine** writes the same physical format. Preallocation changes only file sizing,
  not frame bytes, so a Kotlin reader sees a valid frame sequence followed by zeros and stops —
  same as Go. `test-physical-golden` should confirm no golden fixture moves.

---

## 5. Tradeoffs, stated plainly

- **Disk cost.** Each *active* segment occupies its full size immediately. On a small instance
  running many namespaces this is real; `DeltaMaxSegmentBytes` is already configurable and should
  be tuned down for that case rather than preallocating 64 MiB per namespace by reflex.
- **Not free on every filesystem.** On filesystems without `fallocate`, the zero-fill fallback
  makes segment creation genuinely slower with no sync benefit. Preallocation should be
  defaultable per platform, not forced.
- **Benefit is unproven until measured.** The hypothesis is that removing the metadata commit
  brings `sync` writes toward Mongo's 122-176µs band and tightens the p99 tail. §7 is how we find
  out; if it does not move, this should be reverted rather than kept on faith.

---

## 6. Phases

- **P1 — Positional writes. DONE.** `WriteAt` at the tracked offset, `O_APPEND` dropped
  (`os_store.go`). No behavior change on its own; covered by the existing storage suite.
- **P2 — Preallocation primitive. DONE.** `preallocate_{linux,darwin,other}.go` +
  `preallocate.go`'s `zeroFill`, `PreallocateBytes` config, off by default. Scoped to delta
  segments via `preallocateFor`.
- **P3 — Create/reopen split. DONE.** `openFor` distinguishes create from reopen with `O_EXCL`
  and seeds a created segment's logical size to 0 rather than from `stat()`. The scan-based
  logical-end discovery originally planned here turned out to be unnecessary, because segments
  are never reopened for appending (§2a) — noted in the code so the assumption is visible if it
  ever stops holding.
- **P4 — Measure. DONE — both success criteria met.** Linux, 2 vCPU / 1 GiB container,
  `-benchtime 2000x -count=5`, 64 MiB segments, medians:

  | Config | write ns/op | fsync p50 | fsync p99 |
  |---|---:|---:|---:|
  | `sync-full`, prealloc off *(today's default)* | 450,313 | 287µs | 2.05ms |
  | `sync-fast`, prealloc off | 425,925 | 292µs | 1.57-6.25ms |
  | `sync-full`, prealloc on | 235,423 | 77µs | 1.23ms |
  | **`sync-fast`, prealloc on** | **145,190** | **75µs** | **136-168µs** |

  `sync-full` nearly halved (1.9x), and — the criterion that cannot be faked — **`sync-fast`
  separated from `sync-full`**: 5.7% apart without preallocation (the no-op), 1.6x apart with it.
  `fdatasync` can finally skip the metadata commit. Best config is **3.1x** the current default,
  with a sync p99 **12-15x** tighter, which is the metadata-journal contention disappearing.

  `sync-full` keeps a ~1.2ms p99 even preallocated, because `fsync` still commits inode metadata
  on a file whose size never changes. Only `fdatasync` skips it — which is precisely why the two
  modes now differ, and why `fast` is the mode that benefits.
- **P5 — Background spare segments.** Only if P4 shows rotation latency is a real p99 problem.
- **P6 — Truncate sealed segments to logical end**, so retention accounting stays honest.

P1-P4 are the substance; P5 and P6 are conditional on what P4 shows.

---

## 7. Verification

Run in constrained containers (2 vCPU / 1 GiB), matching the measurement that opened this:

1. `BenchmarkFileBackedUpsertModes` — the headline. Success is `sync-full` dropping materially
   from 564µs, and **`sync-fast` finally separating from `sync-full`**, since `fdatasync` can at
   last skip something. If the two still measure identically, preallocation is not working.
2. `fsync_wait` p50 **and p99** from `reportStagesTo`. The p99/max (3.8ms/13ms today) is where a
   metadata-journal fix should show up most.
3. `BenchmarkAsyncFlushInterval` — must not regress; async does not wait on the sync and should
   be unaffected.
4. `make test-physical-golden` — proves no on-disk frame bytes moved.
5. `go test -race ./...` and the Kotlin `test allTests` gate.

Re-run the KDB-vs-MongoDB matrix (in-process / socket / gRPC vs mongod, reads and writes, all in
2 vCPU / 1 GiB containers) before and after, so the gain is attributed to this change rather than
to the environment.
