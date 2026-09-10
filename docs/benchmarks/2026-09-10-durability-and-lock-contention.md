# Durability, preallocation, and two lock-contention fixes

Date: 2026-09-10. Machine: Apple M3 Max (16 cores), macOS 25.5.0, Docker Desktop for the Linux
rows. Go 1.26.

One session's measurements, kept together because they turned out to be one story: a sync-mode
knob that does nothing on the platform we ship to, why it does nothing, what fixing that bought,
and where KDB actually stands against MongoDB once the comparison is made fairly.

Also records one **negative** result and the reasoning error that produced it, because the
mistake is more reusable than the fix would have been.

> **Reading the tables.** Two units appear throughout and they point in opposite directions.
> Every table states which it uses:
>
> | Unit | Meaning | Direction |
> |---|---|---|
> | **ns/op**, **µs**, **ms** | time for one operation (latency) | **lower is better** ▼ |
> | **ops/sec**, **writes/s** | operations completed per second (throughput) | **higher is better** ▲ |
>
> Percentages are always stated as the change in the *quantity in that table*, so a "+97%" in an
> ops/sec table is 97% more throughput (good), while a "-49%" in an ns/op table is 49% less time
> per operation (also good). Where a row could be misread, the direction is spelled out.

---

## 0. Findings, shortest form

| | |
|---|---|
| `syncMode=fast` was a **no-op on Linux** | `fdatasync` cannot skip a metadata commit while the delta log still grows |
| Segment preallocation fixed that | **3.1x faster** durable writes, sync p99 **12-15x lower** |
| KDB now **beats MongoDB** on durable writes and reads | **1.25-1.6x faster**, both over TCP, both in 2 vCPU / 1 GiB containers |
| Expiry took an `RWMutex` on every point read | removing it: **+97/+100% throughput** on contended reads |
| `dag.Pin` held 93.9% of all mutex delay | removing it: **no measurable change** - see §7.2 |

---

## 1. Environment and method

Everything below follows three rules learned the hard way in this repo:

- **Never run two benchmarks at once**, and check machine load immediately before each. Contention
  alone has manufactured fake 75% regressions here.
- **Match the recipe, not just the benchmark name.** `-benchtime 1000x` and `-benchtime 2s` are not
  comparable; an early comparison in this session looked like a 77% regression purely from that.
- **Run A/B in both orders.** Every before/after pair below was measured twice with the order
  reversed, and both passes are reported.

Linux rows run the compiled test binary inside `golang:1.26-alpine` with `--cpus=2 --memory=1g`,
data on a **named volume** rather than a bind mount - Docker Desktop's file-sharing layer has
entirely different fsync behaviour and would invalidate the measurement. Go 1.26 reads the cgroup
quota correctly (`GOMAXPROCS=2` against `NumCPU()=16`), so there is no oversubscription.

---

## 2. The sync mode that did nothing

**Unit: µs per write (latency) - lower is better ▼.** Single writer, `-benchtime 2000x -count=5`,
medians.

| Mode | macOS (µs/write ▼) | Linux container (µs/write ▼) |
|---|---:|---:|
| `sync-full` | 4,098 | 464 |
| `sync-fast` | 367 | **579** ← *worse than `full`* |
| `async-100ms` | 38.6 | 35.9 |

`sync-fast` is an 11x win on macOS and measured **slower** (higher latency) than `sync-full` on
Linux - consistently, 5/5 samples at both 1 and 2 vCPU (564,659 vs 570,677 ns/op at 1 vCPU;
567,571 vs 568,627 at 2).

**Why.** `fdatasync` skips only metadata *not needed to read the data back*. File **size** is
exactly that metadata, and the delta log grew on every append - so `fdatasync` had to commit the
size anyway and collapsed into `fsync`. The macOS difference is unrelated: there `full` is
`F_FULLFSYNC` (a full drive-cache flush, ~4 ms on Apple SSDs) and `fast` is `F_BARRIERFSYNC`.

**MongoDB avoids this by preallocating its journal.** Verified by inspecting a live `mongo:7`
container: a 100 MB `WiredTigerLog` plus two pre-created `WiredTigerPreplog` spares, so the zeroing
never lands on the critical path. One mechanism explained both why our `fdatasync` bought nothing
*and* why Mongo's durable write was several times cheaper than ours.

---

## 3. Async flush interval

2 vCPU / 1 GiB, `GOMAXPROCS=2`, medians of 5. **Two units side by side:** ns/op is latency (lower
is better ▼), writes/s is throughput (higher is better ▲). They are the same measurement inverted.

| Mode | ns/op ▼ | writes/s ▲ | OS-crash window |
|---|---:|---:|---|
| `sync-full` | 554,322 | 1,804 | none |
| `async-1ms` | 29,077 | 34,391 | 1 ms |
| `async-5ms` | 25,766 | 38,811 | 5 ms |
| `async-100ms` | 27,298 | 36,633 | 100 ms |

**Tightening the interval is free.** 1 ms is no slower than 100 ms, because a sustained writer's
flushes are already coalesced by the drain loop rather than issued per commit. A 1 ms window is
~100x tighter than MongoDB's default at comparable throughput, so there is no reason to ship the
loose one.

Worth being precise about what `async` gives up, since it is easy to overstate: a **process** crash
is already survivable (the drain loop appends into the page cache before acking, so the kernel
still writes those bytes out). The interval bounds the **OS-crash and power-loss** window only.

---

## 4. Segment preallocation

Creating each delta segment at full size with extents allocated *and zero-filled*, so appends are
in-place overwrites with no metadata to commit. Linux, 2 vCPU / 1 GiB, 64 MiB segments.

**All three columns are latency - lower is better ▼.**

| Config | write ns/op ▼ | fsync p50 ▼ | fsync p99 ▼ |
|---|---:|---:|---:|
| `sync-full`, off *(previous default)* | 450,313 | 287 µs | 2.05 ms |
| `sync-fast`, off | 425,925 | 292 µs | 1.57-6.25 ms |
| `sync-full`, on | 235,423 | 77 µs | 1.23 ms |
| **`sync-fast`, on** | **145,190** | **75 µs** | **136-168 µs** |

Best configuration is **3.1x faster** than the previous default (450,313 → 145,190 ns/op), with the
sync p99 **12-15x lower** - the millisecond tail was metadata-journal contention, and it is gone.

The criterion that matters is not the throughput, which could be noise or environment: it is that
**`sync-fast` separates from `sync-full` only with preallocation on** (5.7% apart without, 1.6x
with). `fdatasync` can only skip a metadata commit once the file has stopped growing, so that
separation is the proof the mechanism engaged.

`sync-full` keeps a ~1.2 ms p99 even preallocated, because `fsync` still commits inode metadata on
a file whose size never changes. Only `fdatasync` skips it. **The two settings are a pair** -
neither `syncMode=fast` nor preallocation does much alone.

Three traps a naive implementation hits, all avoided: `O_APPEND` targets the end of the *file*
(64 MiB out once preallocated); `ftruncate` leaves a sparse file whose holes allocate on first
write; and `fallocate` alone can leave extents "unwritten", which still costs a metadata update to
flip. Only a real zero-fill gives initialized extents, which is why etcd's WAL zero-fills.

---

## 5. Access paths

2 CPU / 1 GiB containers, 2000 ops, one harness driving all four targets so the columns are
comparable. Earlier numbers in this repo compared an **in-process** KDB against a **networked**
Mongo, which flattered KDB by an amount nobody had measured - this is that correction.

### 5.1 Reads

**Unit: ns per read (latency) - lower is better ▼.** `par` is the concurrent worker count.

| Path | par=2 (ns/op ▼) | par=16 (ns/op ▼) |
|---|---:|---:|
| **KDB in-process** | **268-520** | **334-365** |
| KDB over TCP | 34,177 | 11,435 |
| KDB over gRPC | 48,602 | 14,494 |
| MongoDB over TCP | 52,728 | 18,643 |

### 5.2 Writes, durable tier

KDB `durability=sync` against Mongo `j:true`. **Unit: ns per write (latency) - lower is better ▼.**
`before`/`after` are preallocation off/on; Mongo is unchanged because it already preallocates.

| Path | par=2 before ▼ | par=2 after ▼ | par=16 before ▼ | par=16 after ▼ |
|---|---:|---:|---:|---:|
| KDB in-process | 615,564 | **115,842** | 104,708 | **42,066** |
| KDB over TCP | 635,977 | **138,673** | 104,201 | **56,870** |
| KDB over gRPC | 613,832 | **145,213** | 92,441 | **61,504** |
| MongoDB `j:true` | 172,861 | - | 86,682 | - |
| MongoDB `j:false` *(weaker guarantee)* | 48,835 | - | 19,751 | - |

### 5.3 What each layer costs

- **Reads: the transport is essentially the entire cost.** ~300 ns in-process against ~34,000 ns
  over TCP - about **100x more time per read**. The storage engine is sub-microsecond; the wire is
  everything. If a consumer can embed, that is not a tuning decision.
- **Writes: the transport is nearly free** - ~20% more time - because the fsync dominates it
  completely.
- **gRPC costs ~1.4x more time than raw TCP on reads and ~nothing on writes.** HTTP/2 framing shows
  up exactly where messages are small and latency-bound, and disappears where the disk dominates.

---

## 6. KDB vs MongoDB, matched

Same guarantee class, both over TCP, both in 2 CPU / 1 GiB containers.
**Unit: ns per operation (latency) - lower is better ▼.**

| | KDB (ns/op ▼) | MongoDB (ns/op ▼) | Winner |
|---|---:|---:|---|
| Durable writes, par=2 | **138,673** | 172,861 | **KDB, 1.25x faster** |
| Durable writes, par=16 | **56,870** | 86,682 | **KDB, 1.5x faster** |
| Reads, par=2 | **34,177** | 52,728 | **KDB, 1.5x faster** |
| Reads, par=16 | **11,435** | 18,643 | **KDB, 1.6x faster** |

Before preallocation Mongo won durable writes by 3.7x. The gap was the metadata commit, not the
architecture.

**Two asymmetries to keep in mind rather than argue away:**

1. KDB embedded runs *inside* the 2 CPU / 1 GiB budget with the application; Mongo gets that budget
   for the server alone plus a separate client container. That is inherent to the deployment
   models, not something a harness can normalize.
2. KDB's *default* is a stronger guarantee than Mongo's. Mongo's `j:false` (48,835 ns/op) is still
   ~2.8x faster than KDB's `sync` (138,673 ns/op) - a different promise, not a better engine. The
   like-for-like row there (KDB `async` over the wire vs Mongo `j:false`) was **not measured** and
   is the obvious gap in this table.

---

## 7. Two locks on hot paths: one win, one nothing

### 7.1 Expiry: `RWMutex` -> `atomic.Pointer` (win)

`expirySetting` took a read lock on every `GetDocument`, including with expiry disabled, because
the nil check itself needed the lock. Reads, 16 cores, both orders.

**Unit: ops/sec (throughput) - higher is better ▲.** Percentages are throughput gains.

| Row | before (ops/sec ▲) | after (ops/sec ▲) | pass 1 | pass 2 |
|---|---:|---:|---:|---:|
| **heavy-multi / overlapping** | 7,162,260 | **14,289,275** | **+96.9%** | **+99.7%** |
| heavy-multi / non-overlapping | 5,351,586 | 5,886,253 | +9.8% | +10.9% |
| single-user / overlapping | 3,759,258 | 3,864,969 | +3.6% | +1.8% |
| single-user / non-overlapping | 5,378,624 | 5,431,819 | -0.6% | +2.6% |

**`RLock` is not a read.** It increments a reader count - a *write* to a shared cache line - so N
cores reading concurrently invalidate that line on each other and serialize on coherence traffic
while never logically blocking. An atomic load only reads, so the line stays shared.

The **shape** is what makes this credible rather than lucky. Single-user rows are the control and
stay flat: one core, no contention, nothing to win - and an environmental artifact would have moved
them too. Overlapping gains most because those documents are cache-resident, so the read itself is
cheap and the lock's cache line is what throughput was bounded by. Non-overlapping gains only ~10%
because each worker already pays real data-cache misses. All three follow from one mechanism.

### 7.2 `dag.Pin`: correct fix, no measurable benefit

`Pin` took the DAG's *write* lock to increment a refcount, so every commit took that lock twice
(pin its base version, then append) and each acquisition queued behind unrelated graph work. Moving
`pins` to a dedicated `pinMu`:

**Unit: milliseconds of accumulated blocked time (lock delay) - lower is better ▼.**

| Mutex delay, memory-only writes | before (ms ▼) | after (ms ▼) |
|---|---:|---:|
| Total, all locks | 3,370 | **605** (-82%) |
| `dag.Pin` | 3,166 (93.9% of total) | **302** (10.5x less) |

**Unit: ops/sec (throughput) - higher is better ▲.**

| Row | before (ops/sec ▲) | after (ops/sec ▲) | pass 1 | pass 2 |
|---|---:|---:|---:|---:|
| insert / heavy-multi | 39,279 | 38,810 | -1.7% | -0.1% |
| update / heavy-multi | 42,506 | 41,980 | -1.6% | -0.7% |
| transaction / heavy-multi | 20,598 | 20,282 | -2.6% | -0.2% |
| insert / single-user | 49,934 | 49,078 | -1.6% | -2.0% |

**No measurable gain, and a slight consistent loss.** The reasoning error is worth recording:

> 3,370 ms of aggregate delay across ~1000 goroutines over a ~20 s run on 16 cores is ~320
> core-seconds of capacity, so **total lock delay was ~1% of it**. Removing every mutex in the
> program could not have bought more than ~1%.

A lock can be **94% of all lock delay while lock delay is 1% of the work**. Profile percentages are
shares of the thing being profiled, not of runtime. Read as "this is the bottleneck", 93.9% is
simply wrong; read as "of the little time spent blocked, nearly all of it is here", it is correct
and uninteresting.

This is the mirror of §7.1. There, reads are ~300 ns, so a contended cache line genuinely dominated
and removing it doubled throughput. Here, writes are tens of microseconds of real work and the lock
was never the limiter. **Same technique, opposite outcome; only measurement distinguishes them.**

**Recommendation: drop it.** Not merely "no benefit" - the controlled A/B is *consistently slightly
negative*, most rows in both passes, and there is a plausible mechanism: `opsPinnedLocked` now takes
an extra uncontended mutex per LRU candidate, and ops eviction runs on the commit path whenever the
budget is exceeded, which under memory-only writes is often.

The change is correct, and it does remove a real scalability ceiling that would matter at higher
core counts than this box has. That is not enough. Carrying extra locking complexity for an
unmeasurable benefit and a measurable-if-small cost is a bad trade, and the honest thing to do with
a negative result is to act on it rather than land the work because it exists.

One design note worth keeping: the obvious lock-free version (`sync.Map` + atomic counters) would
have been a **correctness bug**. Reclamation holds the DAG lock across *check-pins-then-reclaim*,
which is the only reason "`Pin` returned, therefore this commit is safe" holds. Lock-free pinning
lets a pin land between the check and the reclaim. Hence the strict `mu` (outer) -> `pinMu` (inner)
ordering, with reclaimers taking both.

### 7.3 Full matrix, for the record

`BenchmarkWorkload*`, all 180 rows, `-benchtime 2s -count=5`, run against main plus the §7.2
change: **180/180 rows, PASS, zero `deadline exceeded`**, ~25 minutes. Nothing wedged and no row
failed to produce samples.

A row-by-row delta against the previous full matrix is *not* reported, and deliberately so. That
baseline was measured earlier the same day, at a different commit, with several PRs merged in
between and a different ambient machine state - so a 1-3% difference could be attributed to
anything, and the drift-prone rows (Update / Mixed / Transaction heavy-multi-user) move more than
that within a single process anyway. Comparing across it would be exactly the trap §1 exists to
avoid. Back-to-back A/B of one change at a time, in both orders, is the only comparison in this
document that carries weight.

---

## 8. Caveats

- **Docker Desktop's VM disk, not bare-metal NVMe.** Relative rankings hold; absolute numbers need
  validating on the real Linux target before being quoted anywhere.
- **§7.1 depends on a PR that is not merged at the time of writing.** Every other row is from
  merged code.
- **Single runs for the access-path table** (§5), not `-count=5` medians. Directional conclusions
  are safe there; small deltas are not.
- Embedded reads are cache hits on recently-written documents, not cold disk reads.

## 9. Open

- **KDB `async` over the wire vs Mongo `j:false`** - the missing like-for-like row in §6.
- **The capacity-1 write gate.** Measured: KDB `async` throughput is *flat* from 2 to 16 concurrent
  writers (27,298 -> 27,600 ns/op, i.e. no gain from 8x the writers) while Mongo scales 2.7x. This
  has evidence behind it, unlike §7.2, and is the standing suspect for what actually bounds
  memory-only writes.
- **Platform-dependent `syncMode` default.** On Linux with preallocation, `fast` is *not* a
  durability trade - `fdatasync` still reaches the device, and the only metadata it skips is
  timestamps. The doc comment still says it trades power-loss protection for speed, which is true
  on macOS and misleading on Linux, where it would talk an operator out of a free 1.6x.
