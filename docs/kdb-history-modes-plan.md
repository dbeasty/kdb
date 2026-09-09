# Two Retention Modes: `history = FULL` and `history = NONE`

> **Status: implemented (Go), 2026-09-08.** Phases 0-3 have landed in the
> Go tree and the full suite is green, including under `-race`. Under
> `history = none` the on-disk footprint is now a function of the dataset:
> measured flat (x1.00) across 4x the history, against x4.01 under
> `history = full`. What is *not* done is listed under "What did not land"
> at the end - read that before assuming a claim here is fully backed.
>
> The axis this document plans already existed as a declared, parsed,
> validated policy field in both trees (`policy.HistoryMode`,
> `NamespacePolicy.history`) that no engine code enforced. It now means
> something on both settings.

## What is being asked for

Two ways to run the same database, chosen by configuration:

- **FULL** — what KDB is today. Every commit retained, each with its own
  identity, every past state readable, plus a navigation API good enough
  to actually use it: list commits with their messages, jump to a commit,
  step back ten and forward again, undo.
- **NONE** — an ordinary database. One current version of the data.
  Recovery from the journal after a crash and nothing more. No commit
  history accumulating on disk or in the heap.

## The one sentence that shapes the whole design

**NONE is a retention decision, not a data-model decision.**

The commit stays. A commit hash is not decoration in this engine — it is
the concurrency control. `tx.BaseVersion` is a commit hash, conflict
detection is ancestry, idempotent retry is `txIndex[transactionID]`, group
commit orders by log position, and peer sync speaks in commits. A "no
history" mode that stopped producing commits would be a second database,
not a setting.

So NONE changes exactly one thing: **how long a commit's remains are
kept.** Writes still commit, still hash, still order, still detect
conflicts. What changes is that after a checkpoint, the log frames, tree
objects, graph nodes and old document versions behind that checkpoint stop
existing. Same code path, different retention.

That framing is what keeps this a bounded piece of work instead of a fork.

---

## Ground truth: what is actually there today

Verified against the tree at `7947cce`, because two of these are the
opposite of what the code appears to promise.

### The policy field exists and does nothing

`policy.HistoryMode` (`go/kdb/policy/types.go:18`) has `FULL` and `NONE`.
It is parsed from JSON/DSL (`policy/parser.go:46`), validated against
compaction (`policy/validator.go:55`), and preset as `cacheNoHistory()`
(`policy/presets.go:56`). Both trees have it; the spec defines it
(`docs/kdb-spec-layer6-component18-namespace-policy-engine.md` §5,
"Conflict / history coupling").

Enforcement: the Kotlin hybrid engine refuses a version clause under NONE
(`HybridQueryEngine.kt:92`). The Go hybrid engine fetches the policy and
throws the result away — `_, _ = e.cfg.PolicyRegistry.Get(...)` at
`go/kdb/query/hybrid/engine.go:71`. Nothing anywhere changes what is
written or retained. A namespace configured `history = NONE` today is
byte-for-byte a FULL namespace.

### There is already a durable per-namespace mode axis to copy

`storage.HistoryStrategy` (`replay` | `objects`) answers a different
question — *how* historical trees are recovered — but it solved every
plumbing problem this work has: recorded in `namespace.json`
(`embed/namespace_meta.go:20`), reconciled at open with a typed
`HistoryStrategyMismatchError` when a caller asks for something the data
directory is not, defaulted for pre-existing namespaces, overridable by
`KDB_HISTORY_STRATEGY`, and converted offline by
`embed.MigrateHistoryStrategy` behind `kdb-inspect migrate`.

`historyMode` should be plumbed exactly the same way, and the two axes
should be collapsed where they overlap: under NONE the `objects` strategy
has nothing to serve, so NONE implies `replay` and writes no tree objects.

### Document bodies are durable **only** in the delta log

This is the fact that decides Phase 2, and it is easy to miss.

`ServerEngine.PutDocument` stages into `pending`; `CommitTree` moves it
into `docsByHash`, which is an in-memory, byte-budgeted, evicting store.
When it evicts, the cold loader goes to — the delta log
(`storage/engine/cold_loader.go:19`: *"the only place every historical
version is durable"*). The checkpoint stores the live tree as
`(docID, contentHash)` pairs and no bodies (`embed/checkpoint.go`, field
7). The blob path (`WriteBlob` → WAL → memtable → SSTable) carries tree
objects and attachments, not document versions.

And nothing ever deletes a delta segment. Not compaction — the
`compaction` package plans squashes and is not wired to anything
(`RunCycle` has no callers). Not the checkpoint — it *requires* the whole
log to still be there, and replays in full if a fingerprint disagrees
(`embed/checkpoint_open.go:44-64`).

So the log is simultaneously the journal, the archive, and the primary
store of document text. **"Stop keeping history" is not a subtraction
today; it is a subtraction that first needs an addition.** Somewhere else
has to hold the live bytes before the log behind a checkpoint can be
deleted.

### The history API is a package, not a product

`go/kdb/query/hybrid` has `AT VERSION` / `AT COMMIT` / `AT TIME` parsing,
a checkout store, and a version resolver. It has no callers outside its
own package — the Go server's `SQL_EXEC` path does not go through it. The
wire protocol (`wire/types.go`, 0x01–0x1E) has no message for listing
history, reading at a commit, or reverting. `kdb log` exists in the CLI
and runs embedded, against `dag.Walk`.

`DefaultVersionResolver` (`version_resolver.go:31`) resolves `AtTag` and
`AtTime` **to head, silently**. Not an error — a wrong answer. That is a
bug to fix, not a feature to finish.

### History is not invertible, so "roll back one operation" needs a name

A `WriteOp` is `{DocID, Patch}` and a `DeleteOp` is a doc id. Neither
carries the previous content hash. Nothing in a commit says what the data
*was*, so no commit can be run backwards.

Two different things get called rollback, and they must not be conflated:

| | what it is | cost | mutates? |
|---|---|---|---|
| **checkout / `AT COMMIT`** | read the state as of commit *X* | resolve a tree | no |
| **revert** | write a *new* commit whose tree equals *X*'s tree | diff + one commit | yes, forward |

"Undo the last operation" is implemented as *revert to the tree of the
parent commit* — forward motion producing a new commit — never as
inverting the op. This costs nothing extra (the target tree is already
resolvable) and it is what keeps the DAG append-only, peer-sync coherent,
and the undo itself undoable. Under NONE, only checkout-within-the-tail
and revert-within-the-tail can work at all, which is one of the honest
limits of that mode.

---

## Phase 0 — make the axis real and settable

Small, self-contained, unblocks both later phases.

1. **`storage.HistoryMode`** alongside `storage.HistoryStrategy`, with
   `Unset` meaning "whatever this namespace already is" — same three-state
   shape, same reasoning.
2. **Durable marker.** `historyMode` in `namespace.json`;
   `resolveHistoryMode` mirroring `resolveHistoryStrategy`; a
   `HistoryModeMismatchError` at open. A namespace built as NONE cannot be
   opened as FULL and pretend to have a past.
3. **Coupling rules, enforced at resolve time, not scattered:** NONE ⇒
   strategy is `replay` (no tree objects), `squashAfter = NEVER` (already
   a validator rule), branches and tags refused, peer sync refused or
   restricted to the retained tail.
4. **Plumbing:** `StorageOptions.HistoryMode`, `KDB_HISTORY_MODE`, the
   policy DSL/JSON path that already parses `history`, server config,
   `kdb init --history=none`. The retention window of 2c rides the same
   plumbing — `retain { duration, commits }` in policy,
   `StorageOptions.RetainDuration` / `RetainCommits`,
   `KDB_RETAIN_DURATION` / `KDB_RETAIN_COMMITS` — and is parsed and
   validated here even though nothing enforces it until Phase 2.
5. **Stop the silent wrong answer.** Go's hybrid engine consults the
   policy it already fetches and returns a typed `HistoryDisabledError`
   for a version clause under NONE (matching Kotlin's
   `HistoryDisabledException`), and `AtTag`/`AtTime` return
   `VersionNotFound` rather than head until Phase 1 resolves them.
6. **Migration** in `kdb-inspect migrate`: `--history-mode`. FULL→NONE is
   destructive and one-way (it truncates). NONE→FULL succeeds but only
   ever has history from the conversion forward — say so in the tool
   output, not just in a doc.

**Exit:** a namespace records its mode, refuses to be opened as the other,
and refuses the operations its mode cannot serve — with everything still
physically stored exactly as today.

---

## Phase 1 — FULL: finish the history API

The user-visible half. Everything here is additive and touches no on-disk
format.

### 1a. DAG primitives

- **`ResolveRef(CommitRef)` for real** — hash, branch, tag, and time.
  Time resolves to the newest commit at or before the timestamp;
  positions are assigned in log order, so this is a walk with an early
  stop rather than a sort. `RefByTag`/`RefByTime` already exist as types
  (`dag/types.go:106`) with no resolver behind them.
- **Relative refs** — `head~10`, `<hash>~3`, `head^`. First-parent walk,
  bounded by the requested distance. This is the "give me the state ten
  back" ask, and it is a parser plus a bounded walk.
- **`ListCommits(from, limit, cursor)`** → `{hash, transactionID,
  timestamp, author, message, parentHashes, treeHash, opCount}`. Built on
  `Walk`; operations stay unloaded (they are the whole document text —
  see `dag/ops_retention.go`), so listing a thousand commits reads
  metadata only.
- **`Diff(a, b)`** already exists (`in_memory_commit_dag.go:610`) and
  needs surfacing, not writing.
- **`Revert(target)`** — resolve target's tree, diff against head's tree,
  emit the write/delete ops that reconcile them, commit with a generated
  message. One new function; no new durable state.

### 1a-bis. What "restore to a commit" means, and why it is a write

Reading the state at a commit already works: `ServerEngine.treeAt`
(`storage/engine/server_engine.go:153`) resolves any commit's tree — by
lookup under `objects`, by folding the log forward under `replay` — and
`GetDocument(ns, docID, atCommit)` and `ScanDocuments(ns, atCommit, ...)`
sit on it. FULL mode keeps that unbounded and untouched. Phase 1 makes it
*reachable*; it does not make it *possible*.

Making the database **be** at commit *X* again is a different operation,
and the obvious implementation is unsafe today. `SetHead` will happily move
a branch head backwards to any present commit, but
`ServerEngine.commitTreeLocked` **ignores its `parentTreeHash` argument**
and always mutates `e.tree`, the engine's single live tree
(`server_engine.go:580`). After a backwards `SetHead`, reads at head would
resolve the old tree through `treeAt` while the next write would still
build on the newest in-memory tree — reads and writes silently disagreeing
about what the database is.

So a hard reset would require either making the engine's live tree
re-seatable (a new invariant on the one structure the hot read path
depends on) or reverting forward. **Revert forward is the choice**: a new
commit whose tree equals *X*'s tree, written through the normal path, so
`e.tree` stays the single truth, the DAG stays append-only, peer sync stays
coherent, and the undo is itself in the history and itself undoable. Same
answer `git revert` gives, for the same reason.

If a true destructive reset is ever wanted, it is a separate, offline,
exclusive-lock operation next to `MigrateHistoryStrategy` — not a runtime
command — and it is out of scope here.

### 1b. Wire it to the server

The gap is that `hybrid.Engine` is not on the server's SQL path.

- Route `SQL_EXEC` through `hybrid.Engine` so `AT COMMIT` / `AT VERSION` /
  `AT TIME` work over the wire.
- Session checkout lives in `KdbSession` next to the existing `readPin`,
  and **takes a `dag.Pin`** for its lifetime — a checkout is exactly the
  reader-table case `dag.Pin` was built for, and an unpinned checkout is a
  commit that compaction may reclaim mid-session.
- DML under a checkout or an explicit version returns a read-only error,
  per the spec's `ReadOnlyCheckoutException`.

### 1c. Protocol, SDK, CLI

New wire messages (0x1F+): `HISTORY_LIST` / `HISTORY_LIST_RESULT`,
`COMMIT_SHOW`, `REVERT`. Reading one document at a commit extends the
existing `DOCUMENT_GET` DTO with an optional `atCommit` rather than
minting a parallel message.

Wire changes land in **both trees together** — that is the standing rule
in `docs/kdb-finish-up-plan.md` and this is a wire change.

CLI, all of which currently has no remote equivalent:

```
kdb log <ns> [--limit N] [--since T] [--oneline]
kdb show <ns> <commit|head~N>
kdb diff <ns> <a> <b>
kdb checkout <ns> <commit|head~N>      # read-only session view
kdb revert <ns> <commit|head~N>        # writes a new commit
```

**Exit:** a client can list commits with messages, read the database as of
any of them, walk N back and forward, and undo — over the network, on both
implementations.

---

## Phase 2 — NONE: make latest-only actually latest-only

The invariant to establish, and the one thing worth testing hardest:

> **After a checkpoint at sequence *S*, delta segments below *S* can be
> deleted and the namespace still opens with correct data.**

That is false today, for the reason in the ground-truth section. Four
steps, in order.

### 2a. Give live document bodies a durable home outside the log

Two options; the first is recommended.

**Option A — route live bodies through the existing blob store.** Under
NONE, `CommitTree` writes each new version's bytes via the same
`WriteBlob` path that already carries tree objects: blob WAL → memtable →
SSTable, with `RecoverBlobsFromWal` already handling a crash mid-flush.
The checkpoint's live tree is `(docID, contentHash)` pairs and
`ReadBlob(contentHash)` completes them. Cost: one extra write of bytes
already being written to the log, on the write path. Win: it reuses a
crash-tested, incremental, already-compacting tier, and the eviction and
cold-load machinery in `docsByHash` keeps working unchanged — only its
cold source changes from log frame to blob.

**Option B — dump bodies into the checkpoint file.** Simpler to state,
worse in practice: the checkpoint becomes O(dataset) and is rewritten
whole on every checkpoint, so checkpoint cost stops being proportional to
what changed. Reasonable only if checkpoints are rare and the dataset is
small.

Either way the checkpoint becomes **format version 2** and, under NONE
only, an *authority* rather than a pure cache. The Go tree owns this file
alone (Kotlin's `ServerStorageEngine` writes no checkpoint), so it is not
a cross-tree gate.

### 2b. Truncate the journal behind the checkpoint

- Checkpoint on a cadence (`KDB_CHECKPOINT_INTERVAL_COMMITS` /
  `_BYTES`), not only at open-after-full-replay as today.
- After a durable checkpoint at *S*, delete sealed segments ≤ *S* − *k*
  (*k* ≥ 1 as margin, and larger while a replica or peer is behind).
- **Rework the two open-time guards**, carefully: `checkpointMatchesLog`
  and the `ThroughSequence > highestSegmentSequence` check both currently
  treat a missing segment as damage and fall back to a full replay
  (`embed/checkpoint_open.go:44-64`). Under NONE, missing low segments are
  *expected* and the full-replay fallback is no longer available at all.
  The guard has to distinguish "truncated as designed" (recorded floor
  sequence in the checkpoint, segments above it intact) from "damaged"
  (a gap above the floor) — and the damaged case under NONE is a hard
  open failure, not a silent partial recovery. This is the single most
  correctness-sensitive edit in the plan.
- Recovery becomes: load checkpoint → live tree + blob store → replay only
  segments above *S*. That is the "journal file" recovery the ask
  describes.

### 2c. The retention window — how much of the past NONE keeps

**Decided:** NONE keeps a configurable window of recent history rather than
nothing at all. Within the window a NONE namespace behaves exactly like a
FULL one; outside it, the past does not exist. That makes NONE "FULL with a
moving floor", which is both easier to implement and easier to explain than
a genuinely separate mode.

The journal has to hold recent frames anyway for crash recovery, so the
window is very nearly free up to the point where it exceeds what
checkpointing would have truncated.

**Configuration.** A dedicated policy block, next to `history`:

```
namespace("myapp/cache") {
    history = NONE
    retain {
        duration = "24h"     // default; "80h", "7d", "0" all valid
        commits  = 10000     // optional second floor
    }
}
```

- `KDB_RETAIN_DURATION` / `KDB_RETAIN_COMMITS` as env overrides, same
  precedence rules as the other storage settings.
- When both are set, the **more conservative** wins: a commit is
  reclaimable only when it is older than `duration` *and* outside the last
  `commits`. Two floors, whichever is lower.
- `duration = "0"` with no `commits` is the strict reading of NONE — keep
  only what the checkpoint has not yet absorbed.
- The default is `24h`. `80h` is a config value, not a code change; see
  the sizing note below for how to pick.

**Do not reuse `policy.RetainRule` / `retainGranularity`.** Those drive the
compaction evaluator, which NONE switches off (`squashAfter = NEVER`, an
existing validator rule), and they express granularity *tiers* for FULL
mode, not a floor. Reusing them would wake an engine that is not wired to
anything (`compaction.RunCycle` has no callers) to express something
simpler than it models.

**Enforcement granularity is the sealed segment, not the commit.** Segments
are what can be deleted, so a segment is reclaimable when *its newest
commit* is older than the window. The practical consequence, and it should
be documented rather than discovered: **the window is a floor, not a
ceiling** — actual retention overshoots by up to one segment's time span.
A deployment that needs a hard upper bound on retention needs segment
rolling by time, which is a separate feature and out of scope here.

**Three constraints gate every deletion**, and a segment goes only when all
three agree:

| constraint | why |
|---|---|
| below the checkpoint floor (*S* − *k*, from 2b) | the state is not durable elsewhere until then |
| newest commit older than the window | the retention promise |
| below the slowest peer's / replica's position | 2b's existing margin, now explicit |

**Clock handling.** Judged from the commit timestamps already in the
frames, against wall clock at truncation time. Skew that makes a commit
look newer retains it longer, which is the safe direction; the checkpoint
floor is the backstop that keeps a badly-skewed clock from retaining
forever. Never delete on sequence number alone.

**Sizing, because this is the knob people will get wrong.** Retained bytes
≈ write rate × window, *not* dataset size. A namespace writing 50 MB/h
holds ~1.2 GB at 24 h and ~4 GB at 80 h regardless of how small the live
dataset is. So the mode's headline — disk is O(dataset), not O(history) —
holds only in the limit; the honest statement is **O(dataset + writes
within the window)**. Pick the window from the disk budget and the write
rate, and expose the number (below) so it can be checked rather than
assumed.

**Validator rule:** the checkpoint cadence must be at most the window.
Otherwise nothing is ever truncatable and a namespace configured NONE
quietly behaves like FULL — the exact failure this mode exists to prevent.
Refuse the configuration rather than warn.

**Observability:** report the retained span — oldest retained commit and
its timestamp, retained segment count and bytes, and the time of the last
successful truncation — through `kdb status` and the metrics the runtime
already publishes. "Am I actually truncating?" must be answerable without
listing a directory.

### 2d. Stop producing what NONE never reads

- No tree objects, no tree-object chain (implied by `replay` strategy).
- Checkpoint stores **one** commit, not every commit.
- The DAG retains a bounded tail — head plus whatever the 2c window still
  covers, which is the same floor rather than a second independent knob.
  It must also stay at least as long as what needs it for reasons other
  than history: `txIndex` for the idempotent-retry window, and the
  peer-sync ack window if peer sync is allowed at all. Bounded by time or
  count, never a function of total history.
- Consequence worth naming: the 519 B/commit resident cost that
  `docs/kdb-commit-graph-on-disk.md` exists to remove **does not exist
  under NONE**. The mapped graph file is FULL-mode machinery. Sequence the
  two so neither blocks the other, and expect NONE to be the cheaper
  answer for deployments that never needed history.

### 2e. Refuse, loudly, what NONE cannot do

Every one of these returns a typed error naming the mode, never a
plausible-looking wrong answer: `AT COMMIT` / `AT VERSION` / `AT TIME`
outside the tail, checkout outside the tail, revert outside the tail,
`kdb log` past the tail, branch/tag creation, `DAG_DIFF` and
`COMMIT_FETCH` for a reclaimed commit, backup-from-history.

"Outside the tail" means outside the 2c retention window. Within it,
every one of these works exactly as it does under FULL — that is the point
of having a window at all. FULL is unaffected by any of this: it retains
everything, forever, unchanged.

**Exit:** a NONE namespace's on-disk footprint is O(dataset + writes
within the retention window), not O(history); it survives `kill -9` from
checkpoint + journal; every history operation works within the window and
fails with a clear reason outside it; and the retained span is
observable.

---

## Phase 3 — proving it

- **A conformance matrix**, one test table, every history operation × both
  modes × expected outcome (value, or typed error). This is the artifact
  that keeps the two modes from drifting.
- **Crash tests under NONE**: `kill -9` at each of (mid-commit,
  post-append pre-checkpoint, mid-checkpoint, post-checkpoint
  pre-truncation, mid-truncation). Note the known-flaky crash-recovery
  test in `integration` before reading a red run as a regression.
- **A truncation-safety test** that asserts the negative: a segment the
  checkpoint still needs is never deleted, and a gap above the floor fails
  the open instead of opening short.
- **Measurements**, run in isolation (never alongside another heavy job —
  contention has faked large regressions here before): write throughput,
  open time, steady-state disk, and RSS at 100 k / 1 M commits under both
  modes. The expected headline is disk and open time going from O(history)
  to O(dataset) under NONE, with FULL unmoved. Any movement in
  `docs/benchmarks/workload-matrix.md` on the FULL path means the change
  is wrong.
- **Docs**: a mode-comparison table in `README.md` and the architecture
  doc, and the migration semantics in the `kdb-inspect` help text.

---

## Sequencing and what is independent

```
Phase 0  ──┬──> Phase 1 (FULL API)        independent of Phase 2
           └──> Phase 2 (NONE retention)  independent of Phase 1
                    └──> Phase 3 (proof)  needs both
```

Phase 0 is the only shared dependency. Phase 1 is additive, low risk, and
delivers the visible half of the ask. Phase 2 is where the correctness
risk lives, and 2a is its long pole.

## Decisions this plan assumes

Stated so they are not relitigated mid-implementation:

1. Mode granularity is **per namespace**, because that is what
   `NamespacePolicy` already is, and a store with a cache namespace beside
   an audited one is the obvious deployment.
2. NONE still **commits**. Retention changes; the model does not.
3. Per-op inverse rollback is **not** built. Undo is revert-to-a-tree,
   forward.
4. The checkpoint becomes an authority **only under NONE**; under FULL it
   stays a cache the log can always replace.
5. Wire-visible changes land in Go and Kotlin together; the checkpoint
   file is Go-local and does not.
6. NONE keeps a **configurable retention window**, default 24 h, and
   behaves exactly like FULL inside it. The window is a floor enforced at
   sealed-segment granularity, not a ceiling, and it is a separate policy
   field from `retainGranularity` rather than a reuse of it.


---

## What landed, and where

Implemented in the Go tree on 2026-09-08. The Kotlin tree is unchanged; see
"What did not land".

### Phase 0 - the axis

| what | where |
|---|---|
| `storage.HistoryMode` (`unset`/`full`/`none`), `RetentionWindow`, `RetainNothing`, `ParseRetentionDuration` (`24h`, `7d`, `0`) | `go/kdb/storage/history_mode.go` |
| `historyMode` in `meta.json`, `resolveNamespaceHistory`, `HistoryModeMismatchError`, `HistoryModeStrategyConflictError` | `go/kdb/embed/namespace_meta.go` |
| `StorageOptions.HistoryMode` / `.Retain`, `KDB_HISTORY_MODE`, `KDB_RETAIN_DURATION`, `KDB_RETAIN_COMMITS` | `go/kdb/embed/storage_options.go` |
| `NamespacePolicy.Retain`, the `retain { duration, commits }` DSL/JSON block, validation | `go/kdb/policy/` |
| `kdb-inspect migrate-history --history-mode full\|none`, `embed.MigrateHistoryMode` | `go/cmd/kdb-inspect/migrate.go`, `go/kdb/embed/migrate_history.go` |

The coupling is enforced at resolve time: `none` implies `replay`, and
asking for `none` *and* `objects` together is an error rather than a silent
downgrade.

### Phase 1 - the history API

| what | where |
|---|---|
| `ResolveRef` (hash/branch/tag/time), `ParseRevision`/`ResolveRevision` (`head~10`, `head^`, `tag:v1`, `<hash>~2`), `NthAncestor`, `CommitAtOrBefore`, `ListCommits`, tags | `go/kdb/dag/history_nav.go` |
| `dag.HistoryNavigator` capability interface, forwarded by `PersistingCommitDAG` | `go/kdb/dag/history_nav.go`, `go/kdb/embed/persisting_dag.go` |
| `RevertTo` and `DiffCommits`, plus `dag.DiffTrees` and `engine.TreeAt` | `go/kdb/embed/revert.go` |
| Real `AT VERSION`/`AT COMMIT`/`AT TIME` resolution; `HistoryDisabledError`, `HistoryNotRetainedError`, `ReadOnlyCheckoutError` | `go/kdb/query/hybrid/` |
| `HISTORY_LIST` 0x1F / `HISTORY_RESULT` 0x20 / `REVERT` 0x21 / `REVERT_RESULT` 0x22 | `go/kdb/wire/history_ops.go` |
| Server handlers, authorized as DocumentRead / DocumentWrite, revert through the write gate | `go/kdb/server/history.go` |
| `Client.History`, `Client.Revert` | `go/kdb/client/history.go` |
| `kdb log --limit/--skip/--oneline`, `show`, `diff`, `revert`, `get --at`, `tag list\|create\|delete` | `go/cmd/kdb/cli/` |

Two bugs were fixed on the way, both of the "wrong answer, no error" kind:

- `DefaultVersionResolver` resolved `AtTag` and `AtTime` **to head**, so a
  query naming a tag that never existed silently returned current data and
  reported success. Same for `hybrid.Checkout` on an unknown branch.
- `hybrid.Result.ReadOnly` was hardcoded `true`, so an ordinary read at head
  reported itself as a checkout.

And one design fact was confirmed rather than assumed:
`ServerEngine.commitTreeLocked` ignores its `parentTreeHash` argument and
always builds on `e.tree`, so a backwards `SetHead` would desync reads from
writes. That is why undo is revert-forward. It is written down on
`RevertTo`.

### Phase 2 - retention

| what | where |
|---|---|
| Document bodies as content-addressed blobs under `none`; `PrepareForTruncation` walks the live tree and makes every version it names durable, fetching any that are missing | `go/kdb/storage/engine/document_bodies.go` |
| `planTruncation` / `applyTruncation` / `segmentsToDelete`, the three-constraint gate | `go/kdb/embed/truncate.go` |
| Checkpoint format v2 with `floorSequence`; the reworked open guard | `go/kdb/embed/checkpoint.go` |
| `TruncatedLogError`, `checkpointAndTruncate`, truncation at close, `EmbeddedKdbRuntime.Maintain()` | `go/kdb/embed/checkpoint_open.go`, `runtime.go` |
| `HistoryFeatureUnavailableError` (branches, tags), `HistoryNotRetainedError` | `go/kdb/embed/history_features.go` |

The ordering that makes truncation safe is: **plan → flush bodies →
checkpoint claiming the new floor → delete**. A crash anywhere in it leaves
either the previous state or a checkpoint whose floor is ahead of the
deletions, which reads as "some extra old segments", not as damage.

The one hole worth naming, found while testing: bodies written by an
*earlier session* have no blob copy, so flushing the current memtable is not
enough. `PrepareForTruncation` therefore walks the live tree and checks
every version, fetching from the log (still present at that point) anything
missing. Cost is proportional to the dataset, not to history.

### Phase 3 - proof

- **Cross-mode conformance matrix** (`mode_conformance_test.go`): eight
  history operations run under both modes. Seven behave *identically* -
  which is the design claim, since `none` inside its window is supposed to
  be `full`. Only tagging differs, and the refusal names the mode.
- **Truncation decision boundaries** (`truncate_test.go`): nothing above the
  checkpoint, at least one segment always survives, duration and commit
  floors, undatable segments kept.
- **Guard boundaries** (`checkpoint_floor_test.go`): missing below the floor
  is expected, missing at or above it is damage, extra segments below it are
  harmless (the crash window), and a zero floor still checks everything -
  which is what every `full` namespace depends on.
- **Crash states** (`none_mode_test.go`): tail replay after a crash; a stale
  checkpoint over a truncated log refuses to open; disabling checkpoints on a
  truncated namespace refuses to open.
- **Footprint measurement** (`mode_footprint_test.go`), 25 documents over 10
  and 40 sessions:

| mode | sessions | total bytes | delta log |
|---|---:|---:|---:|
| full | 10 | 182,720 | 60,400 |
| full | 40 | 725,629 | 242,350 |
| none | 10 | 12,330 | 6,040 |
| none | 40 | 12,382 | 6,065 |

  The dataset is about 13 KB. `full` grows ×4.01 for 4× the history in both
  columns; `none` holds at ×1.00 in both, with its total at roughly the size
  of the data.

### The three structures that had to stop growing

Bounding the delta log turned out to be the easy third of it, and the other
two were only visible once it was done.

1. **The delta log**, bounded by the retention window. This is what the
   plan above is about.
2. **The SSTables.** Every memtable flush wrote another table and nothing
   ever merged them, so the store grew with the number of flushes. The Go
   tree had no compaction at all (`kdb-storage-compaction` exists only in
   Kotlin), so `sstable/compaction.go` is new.

   Merging alone was not enough, and this is the part that is easy to get
   wrong: every document version is a *distinct content hash*, so in a
   store that keeps rewriting documents nothing is a duplicate and a merged
   table is exactly as large as its inputs. The store shrinks only because
   compaction also **drops versions the live tree no longer names**
   (`engine.reachableBlobKeys`). That is safe under `none` precisely
   because the blob store there holds nothing else - tree objects and
   document locations are written only under the `objects` strategy, and
   `none` forces `replay` - and it is refused under `full`, where every one
   of those entries exists to serve a read the mode promises to keep
   serving.
3. **The checkpoint.** It described every commit the namespace had ever
   made, so the one artifact a `none` namespace must load in full at open
   kept growing after the log had stopped. `CheckpointSnapshotRetaining`
   applies the same window to the graph, keeping branch heads and tags
   whatever it says - a ref naming an omitted commit would restore a
   smaller database that is also a broken one.

Two bugs surfaced while building this, both of the silently-wrong kind:

- The first merge implementation dropped a tombstone by *skipping* it,
  which left an earlier input's value for that key standing - so a full
  compaction resurrected exactly the keys it was meant to reclaim. The
  merge is now two passes: decide every key's final owner, then write.
- `RetentionWindow.Resolve` was not idempotent. It normalized the
  `RetainNothing` sentinel to a plain zero, and a second `Resolve` read
  that zero as "unset" and applied the 24 h default - so a namespace
  configured to keep nothing quietly kept a day, but only when its window
  passed through two resolvers, which is exactly what happened between
  `OpenFileRuntime` and the engine.

## The maintenance loop

`Maintain()` on a timer, which is what makes `history=none` bounded in a
process that runs for weeks rather than one that exits cleanly. Three
things beyond a bare ticker, in `go/kdb/embed/maintenance_loop.go`:

**It runs only when there is something to do.** A pass walks the live tree,
writes a checkpoint and may rewrite every SSTable, so a tick over an
unchanged namespace is pure waste. `needsWork` decides from three probes,
none of which touches the delta log:

| probe | what it catches | cost |
|---|---|---|
| `DAG.Head()` moved since the last pass | new commits to checkpoint/truncate | a mutex |
| `BlobTableCount() >= CompactTables` | flush backlog, which builds without commits | a slice length |
| `now - lastPassAt >= Sweep` | a *duration* window expiring by the clock alone | a subtraction |

The three are not redundant: commits drive truncation, flushes drive
compaction, and the clock drives the window. A namespace can have work
waiting under any one with the other two quiet - which is why "nothing has
been written" is not the same as "nothing can be reclaimed", and why the
sweep (default 30m) exists at all.

**It prefers quiet moments.** Maintenance and commits contend for the same
engine, so `MaintenanceOptions.Busy` holds a pass off while writes are in
flight. `kdb-service` wires it to `KdbServerRuntime.WriteQueueDepth() > 0` -
the write gate, because commits are what a pass actually contends with,
rather than CPU or disk load which say nothing about this engine.

**But load can never starve it.** `MaxDefer` (default 6 ticks) forces a pass
through regardless. This is the load-bearing half of the previous point: a
server under sustained write load is precisely the one whose log is growing
fastest, and a scheduler that waited politely forever would reintroduce the
unbounded growth this mode exists to prevent. Load is a reason to wait for a
better moment, never a reason to skip the work.

Stopped first in the service's shutdown sequence, before draining: `Stop`
waits for an in-flight pass rather than cutting one short, since a pass
mid-truncation is holding the ordering (bodies flushed, checkpoint written,
*then* segments deleted) that makes truncation safe.

| setting | flag | env | default |
|---|---|---|---|
| interval | `--maintenance-interval` | `KDB_MAINTENANCE_INTERVAL` | `5m` (0 disables) |

Embedded callers get the same loop through `embed.StartMaintenance`, with
`Busy` left nil if they have no load signal to offer.

## What did not land

Stated plainly, because each of these weakens a claim made above.

1. **Kotlin is untouched.** The four new wire messages are Go-only, which
   follows the existing precedent for 0x14-0x1C, and the checkpoint file is
   Go-local so its v2 format is not a cross-tree gate. But a Kotlin client
   cannot list history or revert.
2. ~~**No periodic maintenance timer.**~~ **Landed** - see "The maintenance
   loop" below. `kdb-service` runs one `embed.MaintenanceScheduler` per open
   namespace, on `--maintenance-interval` (default 5m, 0 disables).
3. **The measurement is at 40 sessions, not 100k/1M commits.** The growth
   *shape* is clear at this scale; absolute numbers at production history
   lengths are extrapolation.
4. **`retain.commits` is approximated at segment granularity** (1000 commits
   per segment, minimum one). It only ever over-retains, which is the safe
   direction, but it is not exact.
5. **Bumping the checkpoint to v2 makes every existing namespace replay its
   log once** on first open after upgrade. That is the pre-existing
   behaviour for a format change and is safe, but it is a one-time cost.
6. **Compaction only ever merges to a single table.** There is no level
   sizing and no partial merge, so a compaction rewrites the whole store.
   That is fine at the scale measured and is the wrong shape for a large
   one; the trigger (`DefaultCompactionTrigger`, 4 tables) is the only
   control over how often it happens.
7. **A crash between a compaction's swap and its deletions leaves the
   merged-away tables on disk.** Harmless for values, since every key is
   the hash of its own value and two tables cannot disagree about one - but
   a leftover input *could* shadow a tombstone in the merged output. No
   production path writes tombstones to an SSTable today
   (`memtable.Manager.Delete` has no caller), so the window is currently
   unreachable; anything that starts writing them has to close it.
