# Cross-namespace transactions (Layer 17, Phase E)

## Status: IMPLEMENTED (Go) — performance design (§4)

Layer 17 put many namespaces under one host, one lock and one memory pool, and left one thing
out on purpose: `[ ] E. Cross-namespace transactions`. This document is the plan for it, and
records what was built.

---

## 1. The problem

A KDB "transaction" today can only touch one namespace.

- `transaction.Engine.Commit` takes one `*dag.InMemoryCommitDag` and one namespace. Inside
  that namespace a transaction is already a real transaction: many operations, validated
  together, landing as **one** commit, all or nothing.
- `server.KdbServerRuntime` wraps exactly one namespace. Its `writeGate` (a capacity-1
  semaphore) is the only lock, so "the transaction" is in effect *a lock on one namespace*.
- `client.Commit` says so outright: *"All tx.Writes must share tx.Namespace - a
  KdbServerRuntime is scoped to one namespace, so a transaction spanning namespaces can't be
  executed atomically by the current server."*
- Each namespace has its own delta log (`embed.commitLogWriter`), so there is no single write
  that could make two namespaces' commits durable together.

So an application that moves value between two namespaces — debit `accounts`, append to
`ledger` — has to issue two commits. A crash, a conflict or a validation failure between them
leaves one written and the other not. Zolik works around this with a per-namespace
`sync.Mutex` and application-level read-check-write (Layer 17 §1.1), which is correct only
because it is single-process and gives no atomicity across a crash at all.

## 2. What "a transaction across namespaces" has to mean

Namespaces keep their own commit graphs — that split is correct and is what makes
per-namespace compaction, eviction, retention and replay work (Layer 17 §1.2). So a
cross-namespace transaction is **one commit in each participating namespace**, bound together
into a *group*:

| Property | Guarantee |
|---|---|
| **Atomicity** | Every participant's commit lands, or none does — including across a crash at any byte. |
| **Validation** | Schema, unique keys, preconditions and conflict detection run for *every* participant before *any* participant is written. One failing participant rejects the whole group, with nothing staged anywhere. |
| **Isolation (writers)** | Serializable against every other writer to the participating namespaces: all participants' write gates are held from validation through publication. |
| **Isolation (readers)** | A plain read of one namespace is read-committed, as today. A reader that needs a consistent view of *several* namespaces takes `NamespaceSet.Snapshot`, which never observes half a group. |
| **Durability** | When `CommitAcross` returns success under `durability=sync`, the group survives a crash. Under `async`, a crash may lose the group — but only whole. |
| **Identity** | Every participant commit carries the same transaction id (the group id), so the halves of a group can be found from any one of them. |

Non-goals for this phase: cross-namespace SQL (`BEGIN … COMMIT` spanning namespaces),
distributed (multi-host) transactions, and idempotent client retry of a whole group.

## 3. Options

### 3.1 Option S — simple: one host-wide lock

Take a host-wide mutex, commit each participant in turn, fsync every participant log, write a
decision record, fsync it, release the mutex.

- **Correct and small.** Nothing can interleave with a group, so no chaining is needed.
- **Slow in exactly the way this codebase has already paid to fix.** Every cross-namespace
  commit holds the lock across two sequential fsyncs; the whole host (every namespace, not just
  the participants) is serialized behind it. That is the `writeGate`-held-across-fsync shape
  that `commitLogWriter`'s group commit was written to remove, reintroduced one level up.

### 3.2 Option P — performance: per-namespace gates, pipelined two-phase commit (chosen)

The same protocol, with every lock scoped to what actually conflicts and every fsync shared.

1. **Ordered per-namespace gates, not a global lock.** A group acquires exactly its
   participants' write gates, in sorted namespace order (deadlock-free by construction; a
   single-namespace commit holds one gate and so can never close a cycle). Groups over
   disjoint namespaces, and single-namespace commits to non-participants, run fully in
   parallel.
2. **Prepare everything, then publish everything.** The engine is split into `Prepare` (every
   check — schema, preconditions, conflicts, unique keys — with nothing staged) and `Apply`
   (stage, build tree, append). All participants prepare before any applies, so a rejection
   costs nothing to unwind.
3. **Gates are released as soon as order is fixed.** Participant commits are *queued* on their
   namespaces' delta logs under the gates, exactly like a single-namespace commit, and the gates
   are released before any fsync. The next writer's validation overlaps this group's disk I/O.
4. **Two durable phases, both group-committed.**
   - *Phase 1:* each participant commit goes to its own namespace log (parallel across
     namespaces, and batched with every other commit in flight on that log).
   - *Phase 2:* once every participant is durable, a 20-byte **decision record** goes to the
     host's decision log (`<dataRoot>/txn/`). That log has its own group-commit writer, so many
     concurrent groups share one fsync.
   A group is committed **iff its decision record is durable**. Parts are always durable before
   their decision, so "decision present" implies "every part present"; recovery never has to
   look across namespaces.
5. **Ack chaining instead of lock holding.** A commit queued behind an undecided group in the
   same namespace descends from it, so it must not be acknowledged until that group is decided.
   Rather than hold the gate, the namespace's persister carries a *barrier*: every later
   commit's durability wait also waits for the group's decision. A group behind another group
   waits only for the earlier decision to be *queued* (the decision log is FIFO, so it can never
   become durable first). Single-namespace commits with no group ahead of them pay nothing.
6. **Lock-free consistent reads.** Publication of a group's heads is bracketed by an in-progress
   counter and a version counter on each participating namespace's runtime; `Snapshot` reads the
   heads it was asked for and retries until no publication touching those namespaces overlapped
   the read. Only after 64 contended retries does it fall back to briefly taking the gates, so in
   practice readers never block writers.

Expected cost: a cross-namespace commit's latency is two group-committed fsyncs (a
single-namespace commit's is one), and its gate hold time is validation + apply — the same as a
single-namespace commit — so throughput scales with concurrency instead of being capped at
1/(2·fsync).

**Measured** ([2026-09-20-cross-namespace-transactions.md](benchmarks/2026-09-20-cross-namespace-transactions.md),
macOS, `F_FULLFSYNC`): **16.9×** the simple protocol at 64 writers (1,580 vs 94 ops/s), and 75–84%
of the throughput of two *non-atomic* commits. One writer pays 11.9 ms against 8.0 ms, because the
two part flushes do not overlap on a device where every full flush drains the whole drive. That
same device-wide drain means every extra log that is flushing slows its neighbours, groups or
not. The protocol itself shares no lock with namespaces outside the group.

### 3.3 Why not a single shared log, or presence-based decisions

- *All cross-namespace commits in one shared log:* one fsync and atomic by construction, but it
  gives a namespace two logs to replay, merge-order and truncate, and every tool that reads a
  namespace log (checkpoints, history=none reclamation, the commit-ops loader, backup) assumes
  there is one. Far larger blast radius for a single saved fsync.
- *One round, "committed iff every part is present":* saves the second fsync, but deciding
  requires reading every *other* participant's log while opening one namespace, which the lazy
  per-namespace open cannot do — and a redo variant hits the same wall when a redone part's own
  parent is missing.

## 4. Design (as built)

### 4.1 Recording a group in a commit — no format change

Each participant commit is an ordinary commit. Two existing fields carry the group:

- `TransactionID` = the **group id**, identical in every participant.
- `Message` = `kdb:xns/1 host=<id> epoch=<e> group=<uuid> parts=<ns1,ns2,…>`.

`host` is a random id the data root is given (`txn/HOST`) the first time it runs a group. A part
that reaches another data root — peer sync, a copied log — names a host that is not the local
one, and the local decision log never judges it; without this, a foreign epoch number that
happened to match a local dead epoch would roll back data that committed elsewhere.

Both are inside the commit hash and already persisted by both implementations, so no on-disk or
wire format changes and the Kotlin reader sees a normal commit with a descriptive message.

### 4.2 The decision log and epochs — `embed.TxnCoordinator`

`<dataRoot>/txn/` holds:

| File | Meaning |
|---|---|
| `HOST` | this data root's host id, named in every marker it writes |
| `EPOCH` | the last epoch number handed out |
| `decisions-<e>.log` | the live writer's current epoch: header + 20-byte records (16-byte group id + CRC32C) |
| `decisions-<e>.dead` | an epoch whose writer died without sealing it — kept, it is the only record of which of its groups committed |
| *(no file for `e`)* | epoch `e` was **sealed**: every group in it committed |

- Decision records are flushed with the host's `syncMode`, the same primitive its namespace
  logs use: a decision is exactly as durable as the parts it commits.
- The epoch is started lazily, on the first cross-namespace commit, so a process that never
  uses the feature never touches `txn/`.
- A writer host renames every `decisions-*.log` it finds at open to `.dead` — it holds the
  exclusive lock, so their writers are gone.
- **Sealing:** on a clean `Host.Close` with no group in flight and none failed, the current
  epoch's file is deleted, which *is* the statement "all of it committed". The log also **rolls**
  to a new epoch once it passes 1 MiB (~52k groups) and nothing is in flight, so it stays
  bounded in a long-running process.
- **Resolving a part** during replay:

  | epoch state | decision present | decision absent |
  |---|---|---|
  | sealed | committed | committed |
  | `.dead` | committed | **aborted** |
  | `.log` (writer's own) | committed | aborted, unless still in flight in this process (held) |
  | `.log` (a read-only follower) | committed | **held** — the live writer may still decide it |

### 4.3 Replay

`replayDeltaNamespaceFrom` takes a part resolver. An **aborted** part is dropped, and so is
every commit that descends from a dropped commit. That is safe because of ack chaining (§3.2.5):
anything that descends from an undecided part was never acknowledged. Dropping by *ancestry*
rather than by log position is what keeps the decision stable across restarts — commits made
after recovery descend from the pre-group head, so the next replay keeps them.

A **held** part (follower only) is skipped along with its descendants, without error, and
reconsidered on the next `Refresh`.

### 4.4 Checkpoints never capture an undecided part

The DAG tracks *provisional* commits (appended by a group, not yet decided).
`saveCheckpoint` refuses (skips, as it does for any best-effort failure) while any are
outstanding, and re-checks after capturing the live tree, so a part published mid-checkpoint
cannot slip in. A group that failed at runtime never settles, so its namespaces stop
checkpointing until reopened — which is the point: that in-memory state must not be persisted.

### 4.5 Failure after publication — fail-stop

Validation failures happen before anything is published and cost nothing. A failure *after*
publication (an fsync error in either phase) means readers may already have seen a group that
will be rolled back on restart. The participants are then **fenced**: their commit logs latch
the failure and their server runtimes refuse further writes until reopened. This is the same
fail-stop contract a single namespace already has when its commit log fails.

### 4.6 API surface

**Go, in process** (`go/kdb/server`):

```go
set := server.NewNamespaceSet(host.Transactions())    // or nil coordinator for memory runtimes
set.Add(accounts)                                     // *KdbServerRuntime per namespace
set.Add(ledger)
res, err := set.CommitAcross([]server.NamespaceTransaction{
    {Namespace: "bank/accounts", Tx: debit},
    {Namespace: "bank/ledger",   Tx: entry},
}, principal)
heads, err := set.Snapshot("bank/accounts", "bank/ledger")  // consistent across both
```

A participant whose `BaseVersion` is the zero hash is anchored on its namespace's head at
commit time (a blind write — use preconditions for compare-and-set).

**Wire** (Go-only, like `0x14`–`0x22`): `TX_COMMIT_MULTI` (`0x23`) carries one encoded
transaction per namespace; the reply is `TX_COMMIT_MULTI_RESULT` (`0x24`) with each
participant's commit hash, or a conflict/error naming the namespace that refused. When the
server runtime has a `NamespaceSet`, `DOCUMENT_GET` is also routed by namespace, so a client can
read the base versions it commits against.

**Go client:** `client.CommitAcross(ctx, []client.Transaction)`; `client.Transaction` gains
`Deletes`.

**kdb-service** builds a `NamespaceSet` over its host. The primary namespace, the control
plane's namespaces and every wire-reachable namespace are the *same* `KdbServerRuntime`
instances, so there is exactly one write gate and one barrier per namespace in the process.
Reads never create a namespace; a cross-namespace commit may (it is a write, authorized per
namespace).

## 5. Work breakdown

| # | Step | Where |
|---|---|---|
| 1 | Split `finalizeTransaction` into `Prepare`/`Apply`; `Commit`/`Replay` become `Prepare+Apply` (no behaviour change) | `transaction/default_engine.go`, `transaction/prepare.go` |
| 2 | Provisional commits in the DAG; `AppendCommit` option; checkpoint guard | `dag/provisional.go`, `embed/checkpoint_open.go` |
| 3 | Decision log with group commit, epochs, seal/roll, part resolver | `embed/txn_coordinator.go`, `embed/txn_decision_log.go` |
| 4 | Group persistence: barrier chaining on `PersistingCommitDAG`, forced-durable parts in async mode, latch on failure | `embed/persisting_dag.go`, `embed/commit_log.go`, `embed/txn_group.go` |
| 5 | Replay drops aborted parts by ancestry, holds undecided ones | `embed/delta_replay.go`, `embed/file.go` |
| 6 | `NamespaceSet`: ordered gates, prepare-all, publish-all, pipelined completion, snapshot | `server/namespace_set.go` |
| 7 | Single-namespace path honours fencing | `server/server_runtime.go` |
| 8 | Wire messages, handler, `DOCUMENT_GET` routing | `wire/`, `server/wire_listen.go`, `server/multi_commit.go` |
| 9 | Go client `CommitAcross`, `Deletes` | `client/` |
| 10 | kdb-service wiring | `service/service.go` |
| 11 | Tests: engine equivalence, atomicity on every rejection kind, crash at every protocol point, replay stability across restarts, follower holds, checkpoint guard, invariant-preserving concurrent transfers under `-race`, snapshot consistency, deadlock freedom, wire + client e2e | `*_test.go` |
| 12 | Benchmarks: cross vs single vs non-atomic pair, disjoint scaling, cost to single-namespace writers, simple-lock variant for comparison | `server/cross_namespace_bench_test.go`, `docs/benchmarks/2026-09-20-cross-namespace-transactions.md` |

## 6. Follow-ups

- ~~Full wire routing by namespace for session-bound frames.~~ Done, §8.
- ~~Cross-namespace SQL transactions.~~ Done, §7.
- Include `txn/` in `backup.CreateDatabase`; today a database backup is taken with no writer
  running, so every epoch it could see is sealed, but a crash-left `.dead` epoch is not copied.
- The Kotlin reference has no cross-namespace commit; it reads group parts as ordinary commits.

## 7. Cross-namespace SQL transactions

`BEGIN` / `START TRANSACTION`, `COMMIT` / `END`, and `ROLLBACK` / `ABORT` (each with optional `WORK`
or `TRANSACTION`) are now parsed by the Go SQL engine and run by the wire server. A SQL
transaction can write to several namespaces, and its `COMMIT` commits every one of them through
`NamespaceSet.CommitAcross`, with every guarantee in §2.

### 7.1 Which namespace a statement touches

A SQL session is opened on one namespace. With a `NamespaceSet` configured (kdb-service always
has one), a statement's table picks its namespace:

| Table in the statement | Namespace |
|---|---|
| qualified, `bank.ledger` | always `bank/ledger`. A write may create it; a read of a missing one is an error. |
| unqualified, `ledger`, and `<session catalog>/ledger` exists | that namespace, the Kotlin/JDBC mapping |
| unqualified, anything else | the session's own namespace, which is what every table has meant on the Go server so far |

Without a `NamespaceSet` nothing is routed and table names mean what they always did.

The fallback keeps every existing client working. It has one sharp edge, which was chosen
knowingly: creating a namespace whose name matches a table that clients use unqualified redirects
those statements to the new namespace from then on. Qualify table names in new code.

### 7.2 Transaction state

The wire session already had an implicit transaction: DML buffers until `TX_COMMIT`. Nothing about
that changes. SQL `COMMIT` is `TX_COMMIT` and SQL `ROLLBACK` is `TX_ROLLBACK`; both now act on
every namespace the transaction touched. `BEGIN` starts the next transaction at the current head.
It is refused while writes are pending: it neither commits them implicitly nor keeps them silently.

For each other namespace it touches, a session keeps what it keeps for its own:

- A **pending builder**, whose base version is fixed by the first write in that namespace.
  Conflict detection works exactly as it does for the session's own namespace.
- For a `SNAPSHOT` session, a **read point**, pinned so compaction cannot reclaim it. An explicit
  `BEGIN` on a `SNAPSHOT` session takes one `NamespaceSet.Snapshot` of every namespace, so a
  transaction that reads several of them sees all of them as of one instant. Without `BEGIN`, a
  namespace's read point is its head at first touch.

Every transaction boundary (commit, rollback, `BEGIN`, session end) drops the pending builders
and releases the pins in every namespace.

### 7.3 Go client

```go
tx, err := c.BeginTx(ctx, "bank/accounts", client.TxOptions{Snapshot: true}) // or c.Begin
defer tx.Rollback(ctx)                                                        // no-op after Commit
tx.Exec(ctx, "UPDATE accounts SET balance = balance - ? WHERE owner = ?", 30, "ada")
tx.Exec(ctx, "INSERT INTO bank.ledger (owner, debit) VALUES (?, ?)", "ada", 30)
_, err = tx.Commit(ctx) // both namespaces or neither
```

A `Tx` runs on a server session of its own. The client's cached per-namespace session is shared
with `Exec`, which auto-commits, and would otherwise commit a transaction's buffered writes out
from under it. Sessions are reused across transactions: the protocol has no way to end one short
of closing the connection. A conflict in another namespace is a `*CrossNamespaceError` naming it.

## 8. Every wire frame reaches the namespace it names

Until this section, one wire listener served one namespace. Session-bound frames (`SQL_EXEC`,
`TX_COMMIT`, `TX_ROLLBACK`, the lease frames) went to the listener's runtime whatever namespace
the session was opened on, and so did most sessionless ones. With a `NamespaceSet` configured:

| Frame | Served by |
|---|---|
| `SESSION_BEGIN` | the runtime of the namespace it names. The session is bound to it for life. |
| `SQL_EXEC`, `TX_COMMIT` (including client-built bytes), `TX_ROLLBACK`, `LOCK_ACQUIRE` / `RENEW` / `RELEASE` | the session's runtime |
| `UPSERT` | its session's runtime when the session is on the same namespace, otherwise the runtime of the namespace it names |
| `TRANSACTION_REPLAY` | the runtime of the namespace it names |
| `DOCUMENT_GET`, `SEARCH`, `HISTORY_LIST`, `REVERT` | the runtime of the namespace it names; never creates one |
| `TX_COMMIT_MULTI` | each part's namespace (§4.6) |

Without a `NamespaceSet`, every frame still goes to the listener's runtime, as before.

- **Namespace creation.** A frame that writes may open a namespace that does not exist yet:
  `SESSION_BEGIN`, `UPSERT`, `TRANSACTION_REPLAY`, `TX_COMMIT_MULTI`. That is what lets `PutJSON`
  into a new namespace work, and it is authorized per namespace like any write. A frame that only
  reads never creates one. Every client-supplied id is validated
  (`embed.ValidateNamespaceID`) before it touches the filesystem, and `_`-prefixed ids stay
  reserved.
- **Leases live in their namespace.** A lease on document X in `bank/ledger` blocks writers to X
  there, and nowhere else.
- **Session ids are unique process-wide**, not per runtime. One connection's session map now holds
  sessions on several runtimes, and two independent counters would both have issued "sess-1".
- **Behaviour change, deliberate.** In kdb-service a client that opened a session on a namespace
  other than `--namespace` used to be silently served the `--namespace` data. It now gets the
  namespace it named, created on first write.
