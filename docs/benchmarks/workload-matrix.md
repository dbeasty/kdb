# Workload matrix — read / write / update / mixed / transaction, single- vs. heavy-multi-user, overlapping vs. non-overlapping keys

Requested report shape: ops/sec broken down by operation mix (read, write,
mixed read/write, update, transaction), concurrency (single user vs. heavy
multi user), and key distribution (overlapping vs. non-overlapping). No
existing benchmark had this shape — `storage/engine/{read,write}_throughput_bench_test.go`
and `server/write_path_bench_test.go` cover insert-only write and disjoint-ID
read/write in isolation, and `transaction/commit_throughput_bench_test.go`
deliberately isolates lock/rebuild cost by always using disjoint document
IDs. `go/kdb/server/workload_matrix_bench_test.go` (new) fills the gap:
full-stack (`KdbServerRuntime`), real disk-backed WAL with fsync, against
the same five workload shapes crossed with concurrency and key overlap. It
also adds a fourth dimension, durability mode (sync/async/memory-only, see
below), since that's the biggest lever on write throughput available today.

Machine: Apple M3 Max (16 cores), macOS 26.5.2, Go 1.26.3 (`darwin/arm64`).
Single-machine, local-disk (APFS internal SSD), one run, `-benchtime 2s`, no
tuning — a reference point, not a statistically-robust suite. "single-user"
is a plain sequential loop (one caller, no concurrency at all); "heavy
multi-user" is `testing.B.RunParallel` at parallelism 64 (i.e.
64×GOMAXPROCS = 1024 goroutines on this machine). "overlapping" means every
worker draws from the same shared document pool; "non-overlapping"
partitions the pool so workers rarely touch the same document.

Reproduce: `cd go && go test ./kdb/server/ -run '^$' -bench BenchmarkWorkload -benchtime 2s -v`

## Results

| Workload | Concurrency | Keyspace | ops/sec | ns/op |
|---|---|---|---:|---:|
| Read | single-user | overlapping | 7,142,115 | 140.0 |
| Read | single-user | non-overlapping | 7,666,053 | 130.4 |
| Read | heavy-multi-user | overlapping | 3,318,784 | 301.3 |
| Read | heavy-multi-user | non-overlapping | 3,222,609 | 310.3 |
| Write (insert) | single-user | n/a (always-new keys) | 217.2 | 4,605,023 |
| Write (insert) | heavy-multi-user | n/a (always-new keys) | 33,118 | 30,195 |
| Update (existing doc) | single-user | overlapping | 234.4 | 4,265,668 |
| Update (existing doc) | single-user | non-overlapping | 70.81 | 14,121,710 |
| Update (existing doc) | heavy-multi-user | overlapping | 32,218 | 31,039 |
| Update (existing doc) | heavy-multi-user | non-overlapping | 17,141 | 58,338 |
| Mixed read/write (80/20) | single-user | overlapping | 1,120 | 893,052 |
| Mixed read/write (80/20) | single-user | non-overlapping | 1,119 | 893,781 |
| Mixed read/write (80/20) | heavy-multi-user | overlapping | 133,453 | 7,493 |
| Mixed read/write (80/20) | heavy-multi-user | non-overlapping | 123,811 | 8,077 |
| Transaction (explicit Commit) | single-user | overlapping | 240.6 | 4,155,836 |
| Transaction (explicit Commit) | single-user | non-overlapping | 223.8 | 4,467,975 |
| Transaction (explicit Commit) | heavy-multi-user | overlapping | 31,933 | 31,316 |
| Transaction (explicit Commit) | heavy-multi-user | non-overlapping | 30,576 | 32,706 |

All "conflicts/op" columns read 0 across every transaction variant,
including heavy-multi-user/overlapping (~1024 goroutines cycling through a
256-document pool). See **Finding 1** below — this is not evidence of
correct, contention-free optimistic concurrency; it's an unrelated bug
that means conflicts can never be detected at all on this code path.

## Reading this

- **Read throughput regresses under concurrency.** Single-threaded
  sequential reads (7.1-7.7M ops/sec) *beat* the 1024-goroutine
  heavy-multi-user case (3.2-3.3M ops/sec) — concurrent reads are ~2.3x
  *slower in aggregate* than one thread with no concurrency overhead at
  all. See **Finding 2**. ⚠️ **Both halves of this have since changed:**
  the Finding 1 fix cut single-user reads to ~2.0-2.3M (correct resolution
  of `atCommit` costs real work), and the Finding 2 fix raised
  heavy-multi-user to 14.3-19.9M. Current figures are in Finding 2; the
  four Read rows in the table above are preserved as the historical
  `e8f07bd` measurement.
- **Write/update/transaction single-user numbers are all ~220-260 ops/sec**
  (~4ms/op) regardless of workload shape or key overlap. This matches
  `docs/benchmarks/phase0-baseline.md`'s finding that the write path is
  bottlenecked on WAL fsync + full-namespace `commitTree` rebuild rather
  than anything workload-shape-specific — one write, one flush, one
  rebuild, every time.
- **Heavy-multi-user write/update/transaction throughput jumps to
  ~17k-33k ops/sec** — concurrency does help writes (unlike reads), because
  concurrent commits share fsyncs via WAL group commit
  (`docs/benchmarks/phase0-baseline.md`). Non-overlapping *update* is the
  outlier at only 17,141 ops/sec vs. 32,218 for overlapping update — with a
  4096-document pool spread thin across ~1024 goroutines under
  `nonOverlappingBuckets = 128` partitioning, more distinct documents are
  touched per unit time, which likely pushes more of `commitTree`'s
  O(namespace-size) rebuild cost into the measured window; not fully
  isolated from partitioning-bucket effects here.
- **Mixed read/write is far slower per-op than pure read or pure
  write/update at single-user** (893µs/op vs. 130-140ns read / ~4.2ms
  write) because it's dominated by whichever the write component costs —
  1-in-5 ops here is a full commit, so 893µs/op ≈ mostly the 4.2ms write
  cost amortized over 5 ops (~840µs), consistent.
- **Overlapping vs. non-overlapping made no measurable difference** for
  read, mixed, or transaction workloads (well within run-to-run noise) —
  expected for read/mixed since nothing here creates lock contention keyed
  by document identity, and *expected but for the wrong reason* for
  transaction, per Finding 1.

## Durability mode: does async/memory-only actually make writes faster?

The results above all used the default `DurabilitySync` + `SyncModeFull`
(real `F_FULLFSYNC` before every write ack, ~4ms on Apple SSDs) — the
safest and slowest of the three `storage.Durability` modes
(`storage/config.go`). `BenchmarkWorkloadDurability` (new, alongside the
matrix above) crosses insert/update/transaction against all three modes at
both concurrency levels, non-overlapping keys throughout:

- **`sync-full`**: fsyncs before every ack (what the matrix above used).
- **`async-100ms`**: acks once the write is appended to the in-memory WAL;
  a background ticker flushes to disk every 100ms. A crash can lose up to
  one interval of acknowledged writes. This is the "memory mode that
  eventually flushes to persistent storage."
- **`memory-only`**: never syncs the WAL, and per `embed/persisting_dag.go`
  doesn't even queue commits for eventual persistence — genuinely
  ephemeral, not "eventually flushes," unless the caller layers its own
  checkpointing on top. Included as a ceiling, not a mode most deployments
  should actually run in.

| Mode | Workload | single-user ops/sec | heavy-multi-user ops/sec |
|---|---|---:|---:|
| sync-full | insert | 236.8 | 34,489 |
| async-100ms | insert | 29,682 | 30,777 |
| memory-only | insert | 33,978 | 31,822 |
| sync-full | update | 227.3 | 28,936 |
| async-100ms | update | 32,317 | 28,905 |
| memory-only | update | 33,380 | 30,427 |
| sync-full | transaction | 239.3 | 28,500 |
| async-100ms | transaction | 30,377 | 28,462 |
| memory-only | transaction | 33,571 | 30,761 |

**Your instinct was right, but only for single-user traffic.** At
single-user concurrency, async/memory-only are ~125-140x faster than
sync-full (236-239 ops/sec → ~30,000-34,000 ops/sec) — that's the ~4ms
fsync latency disappearing entirely from the critical path of a single
serialized caller. At heavy-multi-user concurrency, the gap nearly
vanishes: sync-full alone already reaches 28,500-34,489 ops/sec, because
concurrent commits share one physical fsync via WAL group commit
(`docs/benchmarks/phase0-baseline.md`) — there's little fsync-latency
headroom left for async/memory-only to remove. All three modes converge to
roughly the same ~28k-34k ops/sec band once enough concurrent writers are
sharing the WAL.

**Practical takeaway**: durability mode is a single-client/low-concurrency
lever, not a heavy-concurrency one. A single sequential writer (a batch
job, a migration script, an interactive session) benefits enormously from
async or memory-only durability; a busy multi-connection server is already
getting most of that benefit "for free" via group commit under the default
safe mode, so the durability/throughput tradeoff there is much smaller
than it looks from single-threaded numbers alone.

## Finding 1 (correctness bug): `ServerEngine.GetDocument` ignores its `atCommit` parameter, silently disabling `ConflictPolicyStrict`'s conflict detection

`storage/engine/server_engine.go:206`:

```go
func (e *ServerEngine) GetDocument(namespaceID string, docID codec.UUID, atCommit codec.Hash) (*document.Document, error) {
	d, ok := e.docs.Get(docID)
	if !ok {
		return nil, nil
	}
	cp := d
	return &cp, nil
}
```

`atCommit` is accepted but never consulted — every call returns the
*current live* document regardless of which commit's tree was asked for.
Contrast `storage/mem/in_memory.go`'s `InMemoryStorageAdapter.GetDocument`,
which correctly resolves `atCommit` through `a.trees[atCommit]` before
looking up the document — that adapter is not affected.

`transaction/default_engine.go`'s `detectConflicts` (the function backing
`ConflictPolicyStrict`, used by `KdbServerRuntime.Commit`/`TransactionEngine`)
depends on comparing "document at the transaction's `BaseVersion`" against
"document at the current head" to decide whether an intervening write
touched the same document. With `ServerEngine`, both lookups return the
same (current) value no matter what tree hash is passed, so
`contentHashEqual(baseDoc, existingDoc)` is always `true` and a conflict is
never raised — **`ConflictPolicyStrict` behaves exactly like
`ConflictPolicyLastWrite` for every deployment using the file-backed
`ServerEngine`, i.e. `embed.OpenFileRuntime` / `kdb-service`, the
production deploy target.**

Verified directly (not just inferred from the 0 conflicts/op above): a
throwaway test committed two transactions with the *same* stale
`BaseVersion` against the *same* document with different content — the
second should be rejected as a conflict and instead succeeded silently,
overwriting the first with no error. Root cause confirmed by comparing
`store.GetDocument` results at the base vs. target tree hashes directly:
both returned the post-first-commit content instead of the base commit's
original content.

**Impact**: any application relying on optimistic concurrency control
(read-modify-write with conflict rejection) against the Go server gets
silent last-write-wins instead, with no signal that a concurrent
conflicting write was lost. This is a correctness issue independent of
performance, uncovered only because the workload matrix specifically
tested overlapping-key transactions, which no prior benchmark did.

**Fixed**: `ServerEngine` now tracks every `DocumentTree` `CommitTree` has
produced, keyed by `TreeHash` (`treesByHash`), and keeps every committed
document version keyed by content hash (`doc_hash_shard.go`'s
`shardedDocByHashStore`) rather than only the current one. `GetDocument`
and `ScanDocuments` resolve `atCommit` through that map before looking up
content, matching `InMemoryStorageAdapter`'s existing behavior. Regression
test:
`TestKdbServerRuntimeStrictConflictDetectionAgainstFileBackedRuntime` in
`go/kdb/server/file_backed_runtime_test.go` (verified to fail against the
pre-fix code with the same "committed two transactions with the same
stale `BaseVersion`" repro described above).

## Finding 2 (scalability): the DAG's RWMutex was the read-path bottleneck — confirmed by profiling, and fixed

**Status: confirmed, fixed, and re-measured.** The hypothesis below was
right about the mechanism. Two things had to be corrected along the way:
the read numbers in the table above were already stale when they were
written, and the fix suggested here ("cache the head hash") turns out to
recover only about a third of the available win.

### The hypothesis, and the profile that confirmed it

`KdbServerRuntime.GetDocument` called `DAG.Head()` and
`DAG.GetCommit(head)` on every read, each taking `InMemoryCommitDag`'s
shared `sync.RWMutex` as `RLock` — four atomic read-modify-writes on one
shared word per read, for a read that mutates nothing.

`go tool pprof` over `BenchmarkWorkloadRead/heavy-multi-user` (1024
goroutines, 16 cores, 65.3s of samples) put this beyond doubt:

| Symbol | Share |
|---|---|
| `sync/atomic.(*Int32).Add` (RWMutex reader-count) | **40.04% flat** |
| `sync.RWMutex.RLock` + `RUnlock` | ~42% cum |
| `InMemoryCommitDag.Head` | 25.03% cum |
| `InMemoryCommitDag.GetCommit` | 19.15% cum |

`pprof -list` on both DAG methods showed the `RLock`/deferred `RUnlock`
pair absorbing nearly all of each function's cost; the actual map lookups
(`d.branches[mainBranch]`, `d.commits[hash]`) were noise by comparison.
This is the textbook `sync.RWMutex` read-side scaling failure: the whole
lock is 24 bytes on one cache line, so every `RLock` from a different core
needs exclusive ownership of that line, and throughput falls as cores are
added.

### Correction: the single-user numbers in the table above are stale

The table's read rows were measured at `e8f07bd`, which is *before*
`6e9b32f` fixed Finding 1. That fix made `ServerEngine.GetDocument`
actually resolve `atCommit` (through `treesByHash` and the by-content-hash
document store) instead of returning current state, which is correct but
roughly tripled the cost of a read. Re-measuring both commits on one
machine:

| | `e8f07bd` (pre-Finding-1-fix) | `7be4735` (current `main`) |
|---|---:|---:|
| single-user / overlapping | 6.72–6.84M | 2.01M |
| single-user / non-overlapping | 7.59–7.68M | 2.26M |
| heavy-multi-user / overlapping | 3.39–3.42M | 3.37M |
| heavy-multi-user / non-overlapping | 3.26–3.29M | 3.32M |

So **"concurrent reads are 2.3x slower than single-threaded" was true when
written and is no longer true on `main`** — the correctness fix slowed
single-threaded reads until they fell *below* the concurrent case. The
underlying defect was the same either way: 16 cores were buying 1.6x, not
the ~2.3x regression the original framing described. Concurrency was
never actually making reads slower on current `main`; it was failing to
make them meaningfully faster.

### What was measured before choosing a fix

Rather than assume the obvious fix, all the standard mechanisms for
read-mostly access to a version pointer were prototyped against this exact
access pattern (`Head()` then `GetCommit(head)`, real `codec.Hash` /
`document.Commit` types) and measured. The harness is kept runnable at
`go/kdb/dag/head_read_strategy_bench_test.go`:

| Option | 1 goroutine | 1024 goroutines | 1024 + hot writer |
|---|---:|---:|---:|
| **A** baseline: one shared `RWMutex` | 82.3M | 3.91M | 2.78M |
| **B** atomic head hash only, `GetCommit` still locked | 90.3M | 6.10M | 3.95M |
| **C** RCU: immutable snapshot behind `atomic.Pointer` | **461M** | **5.59B** | **5.63B** |
| **D** seqlock | *ruled out — unsound* | 7.65M | 5.22M |
| **E** sharded/padded per-core `RWMutex` | 91.2M | 442M | 129M |
| **F** `sync.Map` + atomic head | 67.6M | 827M | 638M |
| **G** MVCC pinned snapshot handle | 320M | 3.79B | 3.83B |

Four things this settles that an argument from first principles would not
have:

- **The fix proposed in the original Finding 2 is not sufficient.** Option
  B — cache the head hash behind an atomic pointer — takes 3.91M to 6.10M
  and stops there, because `GetCommit`'s `RLock` costs exactly as much as
  `Head`'s did. The cost is *per `RLock`*, so every one of them on the
  path has to go, not just the first.
- **Sharding (E) and `sync.Map` (F) scale but don't win**, and both
  degrade sharply once a writer is active (442M→129M, 827M→638M). C is
  unaffected by writers (5.59B→5.63B) because its readers never touch a
  line a writer dirties.
- **Seqlocks (D) are not soundly implementable in Go.** A seqlock reads
  data while a writer may be mutating it, which is a data race regardless
  of the retry loop; `go test -race` duly reported `WARNING: DATA RACE`
  between the reader's `h = v.head` and the writer's `v.head = c.Hash`.
  For a payload with pointers (`Commit` has two slices and two strings) a
  torn read is memory-unsafe, not merely stale. Ruled out on correctness,
  and it was slower than E and F anyway.
- **C is also the fastest single-threaded option** (461M vs 82.3M), so
  there is no concurrency-vs-latency trade to make here.

### MVCC vs RCU — related, but answering different questions

These are often conflated; they differ in *when* the version is resolved,
not in how it is published. RCU (C) resolves the latest version on every
operation: always fresh, but nothing ties two reads together. MVCC (G)
hands the caller a version it pins *across* operations, so N reads cost
one acquisition and all N see the same instant. They compose rather than
compete — LMDB is exactly this shape, with a lock-free atomically
published meta page (RCU) being what a read transaction pins (MVCC).

Measuring the one axis that separates them, reads/sec by group size:

| reads per snapshot | RCU (load per op) | MVCC (pinned) |
|---:|---:|---:|
| 1 | 8.84B | 11.57B |
| 8 | 18.18B | 26.37B |
| 64 | 22.20B | 30.41B |

MVCC's amortization is real (+31–45%) and **worth nothing here**: after
the fix a full `GetDocument` costs ~51–71ns, of which head resolution is
now well under a nanosecond. Amortizing that away would save a fraction of
a percent while giving up read-your-writes — a pinned snapshot cannot see
a write committed after it was acquired, so a caller that reads back its
own preceding `Commit` would miss it. RCU is the right choice for the
per-operation `GetDocument` path.

MVCC is still the right answer to a *different* question this codebase has
not yet asked: a multi-statement transaction or a long scan that needs one
consistent view across many reads. The storage layer is already
structurally ready for it — after the Finding 1 fix it retains every tree
by hash and every document version by content hash — so the missing piece
is an explicit snapshot handle, not a new storage design. Worth doing when
snapshot isolation is actually specified; not worth doing to speed up a
point read.

### What shipped

RCU-style immutable snapshot publication (option C) in two places, since
removing only the first mutex just exposed the second:

1. `InMemoryCommitDag.head` (`go/kdb/dag/in_memory_commit_dag.go`) — an
   `atomic.Pointer[headSnapshot]` holding the default branch's head hash
   *and* the commit it names, rebuilt and republished by
   `publishHeadLocked` on every mutation of `branches`/`commits`. The new
   `HeadCommit()` serves both from one atomic load, so `GetDocument` went
   from four atomic RMWs plus two map lookups to a single pointer load —
   and, as a side benefit, the hash and commit it returns are now
   guaranteed to describe the same instant, which `Head()` followed by
   `GetCommit()` never promised.
2. `ServerEngine.latestTree` (`go/kdb/storage/engine/server_engine.go`) —
   the same treatment for `treesByHash`, which after step 1 became the
   single largest remaining cost (a re-profile attributed 100% of the
   remaining `RWMutex` traffic to `ServerEngine.GetDocument`). Reads
   almost always want the newest tree, so `treeAt` answers that case from
   an atomic pointer and falls back to the map for genuine historical
   lookups.

Both follow the one rule that makes this correct: published state is
immutable, every writer builds a fresh snapshot, and publication happens
under the existing write lock — so readers stay linearizable (each read
linearizes at its atomic load) while writers serialize exactly as before.
Go's GC provides the reclamation that an RCU implementation in a
non-managed language would need epochs or hazard pointers for.

Result on `BenchmarkWorkloadRead`, `-benchtime 2s -count=5`, benchstat vs.
current `main`:

| | before | after | |
|---|---:|---:|---|
| single-user / overlapping | 2.005M | 2.076M | ~ (p=0.421) |
| single-user / non-overlapping | 2.263M | 2.394M | **+5.79%** |
| heavy-multi-user / overlapping | 3.365M | 14.311M | **+325%** |
| heavy-multi-user / non-overlapping | 3.323M | 19.862M | **+498%** |

Reads now scale ~7-8x from one core to sixteen instead of ~1.5x, with no
single-threaded regression. The re-profile shows
`sync/atomic.(*Int32).Add` gone from the top of the profile entirely
(from 40.04%); what remains is goroutine scheduling from oversubscribing
16 cores with 1024 goroutines (`runtime.usleep`, 44%) and real work
(`document.trieGet`, 8.5%). The next real target is the read path's 4
allocations / 176 B per op, not locking.
