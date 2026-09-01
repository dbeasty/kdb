# Multi-Writer / Multi-Reader Plan

**Status:** planning. Nothing in this document is implemented.
**Scope:** the Go engine (`go/kdb/...`), which is the deploy target. Kotlin parity is called out per phase but is not the gating track.
**Question this answers:** can N concurrent application instances safely write through one `kdb-service`, and can N concurrent readers scale?

---

## 1. Audit — what exists today

### 1.1 Multi-reader: works in-process, impossible cross-process

| Capability | State |
|---|---|
| Concurrent reader connections on one service | **Works.** One `sqlWireConnHandler` per connection, sharded storage engine (`storage/engine/doc_shard.go`, `pending_shard.go`), reads take no global lock. |
| Read consistency modes | **Works.** `ReadCommitted` / `ReadYourWrites` / `Snapshot` in `server/session_manager.go`; `sess.ReadPin` is honoured by `execRead` (finish-up item 1-G8). |
| Session lifecycle | **Leaks.** `SessionManager.End` exists (`session_manager.go:106`) but **nothing calls it**. `sqlWireConnHandler.run` ([wire_listen.go:112](go/kdb/server/wire_listen.go:112)) exits its loop on disconnect without touching sessions or locks. The `sessions` map grows for the process lifetime. |
| A second **process** reading the same data dir | **Impossible.** `acquireDirLock` takes `LOCK_EX` unconditionally ([dir_lock_unix.go:24](go/kdb/embed/dir_lock_unix.go:24)). There is no read-only open mode in `FileRuntimeOptions` ([storage_options.go](go/kdb/embed/storage_options.go)). |
| Index-accelerated reads | **Do not exist.** `sql/planner.go:22` returns `PlanFullScan` unconditionally. The whole `go/kdb/index` package is imported by **nothing but its own tests** — verified by grep. Every query is a full scan bounded by `Admission.ScanRowBudget`. |

### 1.2 Multi-writer: not implemented

The three things the design calls for — real unique indexes, a conditional replace, or a lease-based lock manager — are each absent or non-functional.

**Unique indexes: a flag with no enforcement anywhere.**
- `schema.Field.Unique` exists, `SetUniqueStep` migration exists, `schema/field.go:24` validates only that `unique => indexed`.
- `index.Descriptor.Unique` exists (`index/models.go:32`).
- `index.UniqueViolationError` is **defined and never constructed** (`index/exceptions.go:42`; zero call sites).
- `index.Store.Put` has no uniqueness check, and nothing in the commit path calls it.

Net: `unique=true` is metadata that no write path reads. Two clients can insert two documents with the same email and both succeed.

**Conditional replace / CAS: does not exist.**
- Conflict detection (`transaction/default_engine.go:285` `detectConflicts`) is *content-addressed transaction-level OCC*: it compares the doc at `baseTreeHash` against the doc at `targetTreeHash` by content hash. That is a whole-transaction base-version compare, not a per-operation precondition.
- Critically, it treats *"incoming content equals current content"* as a no-op that passes — documented in `kdb-finish-up-plan.md` §345 as intended semantics ("the document is always the truth"). A CAS primitive cannot be built on top of that behaviour; it needs its own literal check.
- The available write verbs are `Commit` (OCC against the session's base version) and `Upsert` (unconditional, [server_runtime.go:314](go/kdb/server/server_runtime.go:314)). There is no `PutIfAbsent` and no `ReplaceIfVersion`.

**Lock manager: no leases, and barely wired.**
- `transaction.LockManager` ([lock_manager.go](go/kdb/transaction/lock_manager.go)) has `TryAcquire` / `Release` / `ReleaseAll` / `AssertHeld` / `AcquireAllForTransaction`. It has **no TTL, no renewal, no owner-liveness, no fencing token**. A lock is held until the owner explicitly releases.
- In the Go server it is used at exactly three call sites — `wire_listen.go:475`, `:479`, `:520`. `AcquireAllForTransaction` runs at `TxCommit` and `ReleaseAll` runs **four lines later**, on the same goroutine. Locks are never held across statements while a transaction is being built.
- Because `writeGate` ([server_runtime.go:70-80](go/kdb/server/server_runtime.go:70)) already serialises every commit into `Engine.Commit`, that acquire/release pair adds nothing the gate does not already provide. The Go lock manager is, in practice, decorative.
- Kotlin is ahead here: `LockingTransactionBuilder` acquires per-statement, and `SqlWireDisconnectCleanupTest` covers release on disconnect. Go has neither.

**Write throughput is single-threaded by construction.** `writeGate` serialises all commits per runtime. That is correct — `InMemoryCommitDag.AppendCommit` advances the head with no compare-and-swap — but it is a hard ceiling regardless of writer count.

### 1.3 Verdict

- **Multi-reader against one service: already works**, modulo the session leak and the fact that every read is a full scan.
- **Multi-reader as separate processes: blocked** by the exclusive dir lock.
- **Multi-writer: not implemented.** Concurrent app instances can corrupt application-level invariants today (duplicate natural keys, lost updates), and they will do so silently.

---

## 2. Design decisions taken

1. **Optimistic first, pessimistic second.** Unique constraints + CAS cover the majority of real multi-writer needs and require no client liveness tracking. Leases are only needed for hold-across-round-trips editing (a user with a form open). Phases are ordered accordingly.
2. **Preconditions live in the transaction envelope, not in `document.Op`.** A precondition is a request-time assertion, not a committed fact. Putting it inside `WriteOp` would change the op's `codec.RecordValue` encoding and therefore commit content hashes. Carry it in a parallel list on the wire and on `document.Transaction`, excluded from the commit hash.
3. **Unique enforcement does not wait on the index layer.** Wiring the dead `go/kdb/index` package into the commit path *and* the query planner is a large, separate piece of work (it is the query-performance track). Phase 1 instead builds a narrow unique-key registry inside the commit path. When the index layer is eventually wired, the registry becomes its backing store rather than being thrown away.
4. **Every new check runs inside `writeGate`.** The gate is the only place where "read current state, decide, append" is atomic. Any precondition evaluated outside it is a TOCTOU bug.
5. **Leases require fencing tokens.** A lease without a fence is unsound: an owner paused past its TTL can still land a write. Every lease hands back a monotonically increasing token that the commit path validates.

---

## 3. Phases

### Phase 1 — Unique key enforcement (gating)

The load-bearing primitive. Without it there is no safe "create exactly once on a natural key".

**1.1 `UniqueKeyRegistry`** — new file `go/kdb/transaction/unique_registry.go`.
- Map `(namespaceID, fieldName, normalizedValue) -> docID`, guarded by its own mutex, mutated only from inside the write gate.
- Value normalisation is explicit and documented: strings compared byte-wise (no case folding — a case-insensitive email index is a schema decision, not a default); numbers by canonical codec encoding; **absent and null values are not indexed** (SQL semantics — many rows may omit the field).
- Composite uniqueness (`Descriptor.Fields`) is out of scope for Phase 1; single-field only, matching `schema.Field.Unique`.

**1.2 Enforcement in the commit path** — `transaction/default_engine.go`, inside `finalizeTransaction`, after `runSchemaPhase` produces `writesByOpIndex` and before the conflict-policy switch (~line 197).
- For each projected write, compute its unique-field keys from the *rolling* schema (`schemaFrame.rollingSchema`), so a migration that adds `unique=true` in the same transaction is respected.
- Check against the registry **and** against the other ops in the same transaction (two ops in one tx claiming the same email must fail, not race).
- A value change or a `DeleteOp` retracts the old key. Retraction and claim are applied together, after the commit is durable — never before, or an aborted commit leaves a phantom claim.
- On violation return `ResultSchemaError` with a new `kdberr.ViolationType` — `UniqueConstraint`, appended to the `ViolationType` block at `error/payloads.go:9` (append only; the enum is `iota`-based and wire-visible). Retire the orphaned `index.UniqueViolationError` or repoint it at the new type.

**1.3 Backfill on open.**
- Adding `unique=true` to a field with existing data must scan the namespace at open/migration time and reject the migration if the existing data already violates it. Reuse `sql.Executor.fullScan`'s traversal, bounded by the scan-row budget.
- Registry state is rebuilt from the doc tree on runtime open. Do **not** persist it separately in Phase 1 — a derived structure with an independent persistence path is a second source of truth and a second recovery bug. Measure rebuild cost on open; if it is unacceptable, add a snapshot in a follow-up with `IsValid`-style validation against the head commit.

**1.4 Wire/error surface.** `ConflictReportMessage` and `sqlResultErrorClassified` must carry the violation distinguishably so a client can tell "someone else took this email" from "your base version was stale" and retry differently.

**Exit criteria:** N concurrent socket clients race to insert the same natural key; exactly one succeeds, the rest get a `UniqueConstraint` violation. Restart the service; the constraint still holds.

### Phase 2 — Conditional operations (CAS)

**2.1 Precondition model** — new type in `go/kdb/document`:
- `ExpectAny` (default, today's behaviour), `ExpectAbsent` (insert-if-not-exists), `ExpectContentHash(h)` (compare-and-set), `ExpectPresent`.
- Carried as `Preconditions []Precondition` on `document.Transaction` (envelope, per decision 2), each naming an op index. Excluded from the commit content hash.

**2.2 Evaluation** — in `finalizeTransaction`, against `targetDocTreeHash` (current head), inside the gate.
- `ExpectContentHash` is checked **literally**: unlike `detectConflicts`, a write whose incoming content equals current content still fails if the expected hash does not match. This is the explicit divergence from content-addressed no-op semantics noted in §1.2.
- Failure returns `ResultConflict` with a new `kdberr.ConflictOperationType` — `PreconditionFailed`, appended at `error/payloads.go:46`.

**2.3 Wire + codec** — `wire/transaction_codec.go` (`EncodeTransaction`/`DecodeTransaction`) gains an optional preconditions field. Field-ID-keyed `RecordValue` decoding (`document/kdb_op.go:80` `OpFromValue`) means absent = old client, which must keep decoding identically. Add a golden interop fixture for both directions.

**2.4 Client API** — `go/kdb/client/client.go` gains `PutIfAbsent`, `ReplaceIf(expectedHash)`, and a `CompareAndSwap` retry helper. Document the retry contract: on `PreconditionFailed`, re-read and re-derive; do **not** blind-retry.

**Exit criteria:** N clients increment one counter document through `ReplaceIf` in a retry loop; the final value is exactly N with zero lost updates, under `-race`, over real sockets.

### Phase 3 — Lease-based document locks (pessimistic path)

Only needed for hold-across-round-trips editing. Phases 1–2 do not depend on it.

**3.1 Fix the Go gaps first** — these are bugs today regardless of leases:
- `sqlWireConnHandler.run` ([wire_listen.go:112](go/kdb/server/wire_listen.go:112)) must, on loop exit, call `DocumentLocks.ReleaseAll` and `SessionManager.End` for every session on that connection. Port Kotlin's `SqlWireDisconnectCleanupTest` to Go.
- Decide explicitly whether Go acquires locks per-statement (porting `LockingTransactionBuilder`) or stays optimistic-only. If optimistic-only, say so in the spec and delete the commit-time acquire/release pair at `wire_listen.go:475-479` — it is misleading dead weight under `writeGate`.

**3.2 Leases** — extend `transaction.LockManager`:
- Lock record gains `owner`, `acquiredAt`, `expiresAt`, `fenceToken`.
- `TryAcquireWithLease(ns, docID, sessionID, ttl) (fenceToken, error)`; `Renew(…, ttl)`; expiry evaluated **lazily on acquire** so correctness never depends on a sweeper. Add a sweeper for map hygiene only.
- Fence tokens are monotonic per `(namespace, docID)`. `AcquireAllForTransaction` returns the token set; the commit path validates each token is still current and fails the commit if not. This is what makes a lease safe against a paused owner.
- Clock: use a single injected monotonic source so tests are deterministic and no wall-clock jump can expire a live lease.

**3.3 Wire ops** — `LockAcquire` / `LockRenew` / `LockRelease` messages plus client methods. Neither Go nor Kotlin has these today; locking is server-side-implicit in Kotlin.

**Exit criteria:** client A takes a lease and is killed; after TTL, B acquires; A's in-flight commit is rejected on a stale fence token rather than landing.

### Phase 4 — Multi-reader scale-out

**4a. In-process (small, do with Phase 3.1):** the session-leak fix above; a benchmark confirming reads do not serialise on `ServerEngine.treeMu` (`storage/engine/server_engine.go:51`) under concurrent load.

**4b. Cross-process read-only replicas (a real feature — confirm you want it):**
- `FileRuntimeOptions.ReadOnly`; `acquireDirLock` takes `LOCK_SH` in that mode and the writer's `LOCK_EX` becomes the mutual exclusion between one writer and N readers. Note the non-unix `O_EXCL` fallback ([dir_lock_other.go](go/kdb/embed/dir_lock_other.go)) has no shared mode — read-only opens would be unix-only, consistent with the existing "unix is the deployment target" stance.
- A read-only runtime must reject every write path explicitly, not merely fail to have one.
- Freshness: readers need to observe the writer's new commits. Cheapest correct option is tailing the delta-log segment sequence and the commit log on an interval, exposed as an explicit staleness bound in the read API. This is a genuine design item, not a flag.

### Phase 5 — Write throughput (follow-up, not gating)

`writeGate`'s serialisation is a per-runtime ceiling. The unlock is group commit: batch queued transactions, run conflict detection and precondition evaluation for the batch, append once. Only worth doing after Phases 1–2 make correctness real, and only if measurement shows the gate is the bottleneck.

---

## 4. Test plan

Real sockets, not in-process `handleFrame` — the existing Kotlin tests' weakness (`kdb-finish-up-plan.md` §46). Add to `go/kdb/server` and `kdb-integration/e2e`:

1. `TestConcurrentUniqueKeyRace` — N clients, same natural key, exactly one winner.
2. `TestUniqueConstraintSurvivesRestart` — claim, restart, re-claim rejected.
3. `TestMigrationToUniqueRejectsDirtyData` — backfill validation.
4. `TestCompareAndSwapNoLostUpdates` — N incrementers, final value == N.
5. `TestCASFailsOnIdenticalContent` — pins the §1.2 divergence from no-op semantics.
6. `TestDisconnectMidTransactionReleasesLocks` — Go parity with Kotlin's existing test.
7. `TestLeaseExpiryFencesStaleWriter` — the Phase 3 exit criterion.
8. `TestSessionsReclaimedOnDisconnect` — the leak fix, asserted on map size.

Standing loop unchanged: `cd go && go test -race ./... && go vet ./...`.

---

## 5. Sequencing and dependencies

```
Phase 1 (unique keys) ─┐
                       ├─→ multi-writer is safe for the common cases
Phase 2 (CAS)         ─┘
Phase 3.1 (leak/disconnect fixes) — independent, do early, it is a bug fix
Phase 3.2-3.3 (leases + fencing) — only if hold-across-round-trips is required
Phase 4a — small, bundle with 3.1
Phase 4b — separate feature, needs a product decision first
Phase 5 — after measurement
```

**Open questions for the user:**
- Is "multi-reader" concurrent client readers against one service (already works, needs 4a hygiene) or separate reader processes on one data dir (4b, a real feature)?
- Is pessimistic locking (Phase 3) actually needed, or do unique keys + CAS cover the application's write patterns?
- Should Kotlin reach parity on Phases 1–2, or does Go lead and Kotlin follow later?

---

# Implementation status

All five phases are implemented in the Go engine. `cd go && go build ./... && go vet ./... && go test -race ./kdb/...` is clean.

## Phase 1 — unique key enforcement · DONE

- `go/kdb/transaction/unique_registry.go` — `UniqueKeyRegistry`, the authoritative
  `(namespace, field, value) -> docID` owner map, checked and mutated only inside the write gate.
  Values are canonicalised through `encoding/json` so `1` and `1.0` collide; strings stay
  byte-wise (case-insensitive uniqueness is a schema decision, not a default). Absent and null
  values claim nothing, matching SQL NULL semantics.
- `planUniqueKeys` resolves a transaction's whole effect before applying any of it, and accepts a
  claim in exactly three cases: nobody holds the key, this document already holds it, or the
  current holder releases it in the same transaction. Two ops in one transaction claiming the same
  key is a violation — atomicity is not a laundering mechanism.
- Enforcement sits in `finalizeTransaction` after conflict resolution (so a custom resolver's
  rewritten document is what gets checked) and before any write is staged. The registry moves only
  once `AppendCommit` succeeds.
- `schema.KdbSchema.UniqueFields`/`HasUniqueFields` and `schema.FieldValue` are the new schema-side
  helpers.
- Rebuild-on-open and rebuild-on-schema-change: `KdbServerRuntime.RebuildUniqueKeys`,
  `SetSchemaChecked` (rejects a migration whose constraint the stored data already violates, and
  rolls the schema back), `UniqueKeyRebuildError`.
- Error surface: `kdberr.UniqueConstraint` is now produced, `wire.ErrorCodeUniqueViolation` is a
  distinct code, and `SchemaError.Error()` names the field and violation instead of counting
  operations.

**Not done:** composite (multi-field) uniqueness — `index.Descriptor.Fields` is still unused.
Single-field only, matching `schema.Field.Unique`. The dead `go/kdb/index` package is still dead;
wiring it into the query planner remains the separate performance track.

## Phase 2 — conditional operations · DONE

- `go/kdb/document/precondition.go` — `ExpectAny` / `ExpectAbsent` / `ExpectPresent` /
  `ExpectContentHash`, carried on `Transaction.Preconditions`, deliberately outside `Op` so commit
  content hashes are untouched.
- `go/kdb/transaction/preconditions.go` evaluates them against the target tree inside the gate.
  `ExpectContentHash` is compared **literally**, diverging from content-addressed conflict
  detection: a write whose bytes match what is stored still fails on a stale hash.
- Preconditions are evaluated before, and independently of, the conflict policy — so
  insert-if-absent works on the `LastWrite` Upsert engine too.
- **An operation carrying a precondition is exempt from base-version conflict detection**
  (`guardedOpIndexes`). This was found by test, not by design: without it, a client that lost one
  CAS round had a stale cached base version and was then refused for a conflict its precondition
  had already ruled out — permanently, since losing a round is what makes the base stale. CAS was
  unusable under contention until this landed.
- Wire: additive `preconditions` field on `transactionDto`, `omitempty`, so a transaction without
  preconditions encodes exactly as before. An unrecognised kind is a decode **error**, never a
  silent downgrade to `ExpectAny`.
- Client: `PutIfAbsent`, `ReplaceIf`, `ReplaceIfPresent`, `GetJSONWithHash`, `CompareAndSwap`
  (re-reads every attempt; retries only on a lost race), `ErrPreconditionFailed`,
  `PreconditionError` carrying the hash that won.

## Phase 3 — leases and fencing · DONE

- `LockManager` rewritten: per-lock `expiresAt` and a monotonic per-document `fence`, an injectable
  clock, `TryAcquireLease` / `Renew` / `ValidateFences` / `Sweep` / `ReleaseLeases`. Expiry is
  evaluated lazily on every lookup, so correctness never depends on the sweeper. A renewal keeps
  its fence; a change of holder mints a new one.
- **The commit path no longer takes locks of its own.** It calls `AssertUnheldByOthers` instead.
  The old take-all-then-release was not merely redundant against the write gate — it was harmful:
  while one writer sat in the gate holding locks, every other writer to the same document was
  refused outright rather than queueing. Under real contention that turned a queue into a storm of
  unclearable failures. What the locks were genuinely for — a document under a *client-held* lease
  must not be written by anyone else — is what the new check enforces.
- Leases bind every write path, not just `TxCommit`. `UpsertMessage` gained an additive
  `sessionId` so the holder's own upsert can be told from a stranger's.
- Wire ops `LOCK_ACQUIRE` / `LOCK_RENEW` / `LOCK_RELEASE` / `LOCK_RESULT` (0x19–0x1C), server
  handlers in `go/kdb/server/lock_listen.go` with a 30s default and 5min cap, client API
  `AcquireLock` / `RenewLock` / `ReleaseLock`.
- **Disconnect cleanup:** `sqlWireConnHandler.run` now releases every lock and ends every session
  on connection close. Previously nothing did, so a dropped client leaked its locks forever and its
  session for the process lifetime.

### Bug found and fixed along the way

**Session ids collided across connections.** `SessionManager.idSeq` was per-manager and each
connection gets its own manager, so every connection's first session called itself `sess-1`. Harmless
while session ids were only looked up within their own connection — but `DocumentLocks` is
runtime-global and keys ownership by session id, so two connections were treated as one holder: each
could take locks the other held, and either could release the other's. Ids now come from a
runtime-scoped counter (`KdbServerRuntime.nextSessionOrdinal`). Caught by
`TestLeaseBlocksAnotherSessionThenReleases`.

## Phase 4 — multi-reader · DONE

**4a:** the session leak is fixed (above). Concurrent readers already worked.

**4b — live read replicas.** The directory lock is now two locks, because "who may open this
directory" and "who may write to it" are different questions:

| | `.kdb.lock` (attach) | `.kdb.write.lock` |
|---|---|---|
| writable runtime | shared | **exclusive** |
| read-only runtime | shared | — |
| `LockDataDir` (kdb-inspect) | **exclusive** | — |

That yields all four relationships that matter: many readers coexist, at most one writer exists,
readers coexist with a *live* writer, and maintenance excludes everyone. The first attempt kept one
lock and gave readers `LOCK_SH` — which meant a read replica could only attach to a directory whose
writer had stopped, i.e. it required the thing it was replicating to be down. Mixed versions stay
safe: an old binary takes the attach lock exclusively, so the worst case is refusing to open, never
two writers.

- `engine.TargetReadOnly` opens with no WAL and no delta writer, but with the delta reader — the
  reader touches nothing the writer has open for appending, and torn-tail tolerance already existed
  in `DefaultReader.ReadAll`.
- `embed.OpenReadOnlyFileRuntime`, `FileRuntimeOptions.ReadOnly`, `EmbeddedKdbRuntime.ReadOnly` /
  `AssertWritable` / `Refresh` (a reader's view is a snapshot as of its open; `Refresh` advances
  it). `runTransaction` refuses at the front rather than failing deep in a missing component.
- Unix only — the non-flock fallback has no shared mode and says so rather than degrading.

## Phase 5 — group commit · MEASURED, NOT BUILT

The plan gated this on measurement. `BenchmarkCommitGateBreakdown`, `BenchmarkWriteGateOverheadOnly`
and `BenchmarkFileBackedCommitGateBreakdown` (`go/kdb/server/write_gate_profile_test.go`), Apple M3
Max:

| | serial | parallel (16) |
|---|---|---|
| in-memory commit | 19.7 µs/op | 23.2 µs/op |
| file-backed commit | 4022 µs/op | **528 µs/op** |
| write gate primitive alone | — | 0.65 µs/op |

The premise does not hold. The gate costs 0.65 µs against a 19.7 µs commit — 3%. And durability
grouping **already exists**: `PersistAsync` releases the gate as soon as a commit's position in the
delta log is fixed, so concurrent commits already share one physical sync. That is what the
file-backed numbers show — 7.6× more throughput under concurrency than serial, on the very fsync
cost group commit was supposed to amortise.

A batching layer would therefore add cross-transaction conflict-detection and precondition
complexity, on the exact code path Phases 1–2 depend on for correctness, to recover at most a few
percent. **Deliberately not built.** The benchmarks are committed so the decision can be re-checked
on other hardware or if the engine's per-commit cost changes.

## Test coverage added

`go/kdb/transaction/unique_registry_test.go`, `lease_test.go` · `go/kdb/server/multiwriter_test.go`
· `go/kdb/client/conditional_test.go` · `go/kdb/embed/readonly_test.go` ·
`go/kdb/server/write_gate_profile_test.go`

Highlights: 8 clients racing one natural key → exactly one winner, classified `UNIQUE_VIOLATION`;
8 clients incrementing one counter through CAS → final value exactly 8; a stale-hash write whose
content matches what is stored → still refused; lease expiry fences a stalled writer; a dropped
connection frees its locks; constraint survives a restart; a dirty migration is rejected and rolled
back; readers attach to a live writer; maintenance excludes everyone.

## Known gap, unrelated but found here

`sql.DefaultPlanner.PlanSelect` (`go/kdb/sql/planner.go:18`) **panics** on an unknown column, and
nothing recovers it in the wire handler — `SELECT nosuchcolumn FROM t` from any client takes the
process down. Found when a test used `SELECT _id`. Out of scope for this work, but it is a trivial
remote crash and should be fixed on its own.
