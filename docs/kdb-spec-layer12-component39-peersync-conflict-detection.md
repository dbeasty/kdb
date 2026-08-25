# Component 39 — Peer-Sync Conflict Detection & Merge

Layer 12. Depends on `kdb-dag` (Component 6, `CommitDag`), `kdb-transaction` (Component 7,
`ConflictReport`/conflict-policy types — reused, not reinvented), `kdb-peer-sync` (Component 23,
the module this fixes).

## 1. Purpose

The master spec claims Mode 3 peer-sync is "source-control for structured documents" — peers
diverge independently and "merge when they choose to," with conflicts always surfaced to the
application, never silently resolved (`kdb-spec.md` §1.3, §8.3). **That claim does not hold for
the actual code.** `PeerSyncFrameHandler.handleCommitPush` accepts any pushed commit whose parent
already exists locally and then unconditionally moves the branch head to it
(`dag.setHead("main", msg.commits.last().hash)`) — there is no check that the incoming commit
descends from the current local head. Two peers who wrote concurrently while disconnected and then
sync do not get a `ConflictReport`; whichever one pushes last simply overwrites the other's branch
pointer, and the loser's commits become unreachable from `main` even though they remain physically
present in the DAG's object store. This component makes the spec's own claim true: real divergence
detection, and a real, application-visible `ConflictReport` (or explicit merge) instead of a silent
overwrite.

## 2. Dependencies

- `kdb-dag`'s `CommitDag` — `hasCommit`, `putCommit`, `setHead`, and (needed, may not fully exist —
  confirm during implementation) an ancestry/reachability query: "is commit X an ancestor of
  commit Y?" `computeSyncPlan`'s existing `commonAncestor` helper (`PeerSyncTypes.kt`) is close to
  what's needed and should be reused/extended rather than reimplemented.
- `kdb-transaction`'s `ConflictReport`/`ConflictPolicy` types — this component's job is to make
  peer-sync *use* the existing conflict machinery other write paths already have, not to invent a
  parallel conflict model.
- `kdb-peer-sync`'s existing `PeerSyncClient`/`PeerSyncFrameHandler`/`PeerSyncTypes` — this is a fix
  to those files, not a new module.

## 3. Public Interface

```kotlin
// kdb-peer-sync/src/commonMain/kotlin/dev/kdb/peersync/PeerSyncFrameHandler.kt — the core fix

// BEFORE (current, buggy):
//   for (commit in msg.commits) {
//       if (dag.hasCommit(commit.hash)) continue
//       dag.putCommit(commit, requireParents = true)
//       cfg.materializeCommit?.invoke(commit)
//   }
//   if (msg.commits.isNotEmpty()) dag.setHead("main", msg.commits.last().hash)

// AFTER — putCommit still accepts and stores every commit unconditionally (history must not be
// lost), but setHead only happens after a real fast-forward/merge decision:
fun handleCommitPush(dag: CommitDag, msg: CommitPushMessage, cfg: PeerHostConfig): CommitPushOutcome {
    for (commit in msg.commits) {
        if (dag.hasCommit(commit.hash)) continue
        dag.putCommit(commit, requireParents = true)
        cfg.materializeCommit?.invoke(commit)
    }
    val localHead = dag.head("main")
    val incomingHead = msg.commits.lastOrNull()?.hash ?: return CommitPushOutcome.NoOp
    return when (resolveHeadUpdate(dag, localHead, incomingHead)) {
        is HeadUpdate.FastForward -> { dag.setHead("main", incomingHead); CommitPushOutcome.FastForwarded }
        is HeadUpdate.AlreadyAncestor -> CommitPushOutcome.NoOp   // incoming is behind/equal to local, nothing to do
        is HeadUpdate.Diverged -> CommitPushOutcome.Conflict(
            buildConflictReport(dag, localHead, incomingHead)
        )
    }
}

sealed class CommitPushOutcome {
    object NoOp : CommitPushOutcome()
    object FastForwarded : CommitPushOutcome()
    data class Conflict(val report: ConflictReport) : CommitPushOutcome()
}
```

```kotlin
// New: the ancestry decision this was missing entirely.
sealed class HeadUpdate {
    object FastForward : HeadUpdate()      // incoming head is a descendant of local head — safe to move
    object AlreadyAncestor : HeadUpdate()  // local head is already at or ahead of incoming — no-op
    object Diverged : HeadUpdate()         // neither is an ancestor of the other — real conflict
}

fun resolveHeadUpdate(dag: CommitDag, localHead: String, incomingHead: String): HeadUpdate
```

```kotlin
// PeerSyncClient.kt gets the symmetric fix in pullMissing()/syncBidirectional() — same
// resolveHeadUpdate call before any local dag.setHead(...), not just on the push-receiving side.
class PeerSyncClient {
    suspend fun pullMissing(): PullResult   // now returns a Conflict variant, mirroring CommitPushOutcome
    suspend fun syncBidirectional(): SyncResult
}
```

## 4. Data Structures

```kotlin
// Reuses kdb-transaction's ConflictReport shape (Component 7) — do not invent a second conflict
// report type. Populate it from the divergent commit ranges computeSyncPlan already knows how to
// find (localOnly/remoteOnly commit lists), one entry per document touched by a commit on each
// side of the divergence.
data class ConflictReport(
    val namespaceId: String,
    val localOnlyCommits: List<String>,   // commit hashes only reachable from local head
    val remoteOnlyCommits: List<String>,  // commit hashes only reachable from incoming head
    val affectedDocuments: List<DocumentConflict>,  // existing kdb-transaction type — reused
)
```

## 5. Contracts

- **`putCommit` always stores.** Every commit, from either side, is always written to the DAG's
  object store, regardless of whether it ends up reachable from `main` — history must never be
  lost, only the branch pointer decision is gated. This preserves today's one correct behavior
  (commits aren't deleted) while fixing the actually-broken one (which commit `main` points at).
- **Fast-forward is automatic and silent, same as git.** If the incoming head is a strict
  descendant of the local head (the common "I was offline, you weren't, catch me up" case), the
  head moves with no conflict surfaced — this is the case that was already accidentally correct
  before (a linear history has no ambiguity), and this fix must not regress it into an
  unnecessary conflict report.
- **True divergence always produces a `ConflictReport`, never a silent head move.** This is the
  contract the master spec already claims and this component makes actually true. The application
  (Zolik's Go client, or whatever calls into peer-sync) decides what happens next — retry with a
  merge commit, prefer one side, or surface it to a human. This component does not pick a winner.
- **`APPEND_ONLY` namespaces still need this.** It might look like an append-only, no-mutation
  namespace (Zolik's `match_results`) can't have a "real" conflict — every write is a new document,
  nothing is being overwritten in content terms. But the *branch-head-clobbering* bug affects such
  namespaces exactly the same way: peer A's and peer B's independently-created match-result commits
  both need to end up reachable from `main` after a sync, not just whichever one happened to push
  last. For `APPEND_ONLY` specifically, the correct resolution of a `Diverged` case is **always a
  merge** (both sides' commits become reachable — there is no real content conflict to ask the
  application about), whereas for `STRICT`-policy namespaces a `Diverged` case may still need
  human/application judgment if the *same document* was written differently on both sides. This
  component's `resolveHeadUpdate`/`ConflictReport` path must consult the namespace's conflict
  policy to know which of these two behaviors applies — this is the one place "conflict
  detection" and "conflict *resolution*" aren't the same thing, and `APPEND_ONLY` gets the simpler
  answer for free once ancestry detection itself works.
- **Symmetry between push and pull.** The same `resolveHeadUpdate` decision applies whether this
  node is receiving a push (`PeerSyncFrameHandler`) or has just pulled (`PeerSyncClient.pullMissing`)
  — one shared function, not two independently-maintained copies that could drift, which is exactly
  how the original bug likely went unnoticed on one side.

## 6. Error Cases

- `PeerSyncConflictException` — thrown (or returned as `CommitPushOutcome.Conflict`/equivalent on
  the pull side) when `resolveHeadUpdate` returns `Diverged` for a `STRICT`-policy namespace with a
  genuine same-document conflict; carries the `ConflictReport`.
- No new error for the `APPEND_ONLY`-auto-merge case (§5) — that path succeeds, it just does more
  work (a real merge instead of a bare head move) than the buggy version did.
- `AncestryLookupException` (or similar) if `resolveHeadUpdate` cannot determine ancestry at all —
  e.g., a commit references a parent that was never received (a gap in the DAG). This should be
  rare given `putCommit(requireParents = true)`'s existing guarantee, but must not be silently
  treated as either fast-forward or divergence if it happens.

## 7. Test Cases

1. **Simple fast-forward**: peer A is purely behind peer B (linear history), sync moves A's head
   to B's tip with no conflict — the case that already "worked" (by accident), must keep working.
2. **True concurrent divergence, `STRICT` namespace, same document touched differently on both
   sides** — sync produces a `ConflictReport` naming that document, `main` does **not** move to
   either side unilaterally. This is the test that fails against today's code and is the core
   acceptance criterion for this component.
3. **True concurrent divergence, `STRICT` namespace, different documents touched on each side (no
   actual content conflict)** — per §5, decide and test explicitly whether this auto-merges (no
   real conflict exists) or still surfaces a report for the application to acknowledge; document
   the chosen behavior precisely, since the master spec doesn't currently address document-level-vs-
   namespace-level conflict granularity in peer-sync at all.
4. **True concurrent divergence, `APPEND_ONLY` namespace (Zolik's `match_results` case
   specifically)** — both sides' commits end up reachable from `main` after sync, automatically,
   no `ConflictReport`. This is the test that directly validates Zolik's "host syncs match results
   to cloud" use case.
5. **Three-way**: A and B both diverge from a common ancestor, sync A→coordinator, then sync
   B→coordinator — the second sync must detect divergence against the *already-updated* head from
   the first, not against the original common ancestor.
6. **No history is ever lost**: after any of the above, every commit from every peer is still
   present in `dag`'s object store and reachable via `hasCommit`, even in cases where it isn't
   (yet) on `main`.
7. **Push and pull produce the same decision for the same divergence** — construct one divergent
   scenario, drive it once via `PeerSyncFrameHandler.handleCommitPush` and once via
   `PeerSyncClient.pullMissing()`, assert identical `HeadUpdate` classification from both — the
   symmetry contract from §5.
8. **RBAC interaction** (flagged in the gap analysis §6 as a related bug): confirm whether fixing
   this also needs `PeerSyncFrameHandler` to route through `AuthorizingTransactionEngine` for the
   merge/head-move decision, or whether that's genuinely a separate fix — don't silently expand
   this component's scope to include it without deciding explicitly.
9. **Regression**: the existing `WebSocketPeerSyncIntegrationTest.wsPeerSyncBidirectionalAfterPush`
   test (currently sequential — A pushes and disconnects *before* B ever writes) must still pass,
   **and** a new genuinely-concurrent variant of it (both peers write before either syncs) must be
   added, since the sequential version cannot and does not exercise this component's actual fix.
10. **Merge-commit shape for the auto-merge case (test 4)**: define and test what the resulting DAG
    structure actually looks like — a real merge commit with two parents (git-style), or two
    independent commits both left reachable via some other mechanism? This is a genuine design
    decision `kdb-dag`'s `appendMergeCommit` (referenced in the master spec's dependency rules,
    §0) may already assume an answer to — confirm against that existing function before choosing
    "invent a new merge shape" over "call the merge-commit path that's already there."

## 8. Non-Goals

- Automatic content-level merge of two conflicting edits to the *same* document (e.g. field-level
  3-way merge). Per the master spec's own design principle ("conflicts surface to the application;
  KDB never silently resolves them"), a same-document `STRICT` conflict stays a `ConflictReport`
  the caller must act on — this component makes that reporting real, it does not add automatic
  resolution beyond the `APPEND_ONLY` case where no real conflict exists to resolve.
- Fixing peer-sync's separate RBAC bypass (test 8 above) unless scoping it in during implementation
  makes clear sense — default to treating it as a related but separate fix.
- Distributed consensus/quorum among more than two peers syncing simultaneously — this component
  fixes pairwise sync correctness; N-way simultaneous multi-peer merge is out of scope.

## 9. Implementation Notes — interim mitigation for Zolik, if this lands after Zolik needs it

If Zolik's build order reaches "sync match results from a LAN host to the cloud" before this
component ships, do **not** rely on peer-sync's shared `main` branch for that namespace in the
meantime. The cheap mitigation: **each host writes to its own uniquely-named branch** (e.g.
`match-results/<hostID>`) rather than a shared `main`; the cloud-side reader lists all
`match-results/*` branches and reads each independently, rather than depending on peer-sync to
merge them onto one pointer. This sidesteps the bug entirely (no two peers ever push to the same
branch, so there's nothing to diverge) at the cost of the cloud side doing a small amount of
branch-enumeration work instead of a single-head read — a reasonable trade for an interim state,
and worth keeping even after this component ships if it turns out simpler than relying on
auto-merge for `APPEND_ONLY` namespaces in practice.

## 10. Estimated Lines

400–700 NBNC: ~150 for `resolveHeadUpdate` and the ancestry-check extension to
`computeSyncPlan`/`commonAncestor`, ~100 for wiring it into both `PeerSyncFrameHandler` and
`PeerSyncClient` (§5's symmetry requirement), ~100 for the `APPEND_ONLY` auto-merge path and its
interaction with `appendMergeCommit`, ~150–350 for tests (test 5 and test 9's genuinely-concurrent
scenario need careful construction to actually exercise concurrency rather than accidentally
staying sequential, which is exactly how the original bug went unnoticed).
