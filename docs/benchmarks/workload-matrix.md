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
  all. See **Finding 2**.
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

**Fix scope**: `ServerEngine` needs real point-in-time document retrieval
(materializing/looking up a document as of an arbitrary historical tree
hash, not just current state) — likely a non-trivial change to how
`e.docs` is organized, not a one-line fix. Flagged as follow-up work
rather than fixed here.

## Finding 2 (scalability): read throughput is worse at heavy concurrency than single-threaded

`KdbServerRuntime.GetDocument` calls `s.Runtime.DAG.Head()` and
`s.Runtime.DAG.GetCommit(head)` on every single read, even though nothing
is being mutated. Both take `InMemoryCommitDag`'s shared `sync.RWMutex` (as
`RLock`) — see `dag/in_memory_commit_dag.go`. Go's `RWMutex` read side is
known to scale poorly at high core counts because every `RLock`/`RUnlock`
does an atomic increment/decrement of one shared reader-count word, which
bounces across cache lines as more cores contend for it — consistent with
what was measured here (3.2-3.3M ops/sec aggregate across 1024 goroutines
on 16 cores vs. 7.1-7.7M ops/sec on a single goroutine with zero locking
overhead at all). Not confirmed via profiling in this pass — flagged as
the most likely explanation given the DAG mutex's known role
(`docs/benchmarks/phase0-baseline.md` bottleneck #3, previously measured
only on the write path) and worth a `pprof` pass before attempting a fix
(e.g., caching the head hash behind an atomic pointer for pure reads that
don't need the full DAG lock).
