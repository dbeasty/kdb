# KDB Component Spec — Layer 6
## Component 19: Compaction Engine (DAG Squash + GC)
### `dev.kdb.compaction`

**File:** `kdb-spec-layer6-component19-compaction-engine.md`  
**Layer:** 6 — Hybrid Query + Policy  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-compaction`  
**Depends on:** Layer 2 (`CommitDag` compaction APIs), Layer 3 (`StorageAdapter`), Layer 4a (`dev.kdb.storage.compaction` — **distinct**: SSTable/delta only), Layer 4b (tier signals — optional triggers), Component 18 (`NamespacePolicy`, `CompactionPolicyEvaluator`)

-----

## 1. Purpose

Orchestrates **commit-DAG compaction**: selecting squash boundaries from namespace policy, coordinating peer acknowledgements (`CompactionIntent` / `CompactionNotice`, master §6.4 and §8.5), invoking `CommitDag.compactableBefore` + `squash` with a materialised synthetic document tree, and garbage-collecting unreachable commits and orphaned blobs.

**Storage compaction** (SSTable level merge, delta segment roll) remains Component **10f** (`dev.kdb.storage.compaction`). This module never merges LSM files; it may **enqueue** 10f jobs after a successful squash when tier signals indicate large HOT segments.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `KdbUuid` |
| `dev.kdb.error` | `CompactionBoundaryException`, `CompactionSafetyException`, `KdbException` |
| `dev.kdb.document` | `DocumentTree`, `KdbCommit` |
| `dev.kdb.dag` | `CommitDag`, `compactableBefore`, `squash`, `walk`, tags/branches |
| `dev.kdb.storage` | `StorageAdapter` — blob GC, snapshot materialisation |
| `dev.kdb.policy` (18) | `NamespacePolicy`, `CompactionPolicy`, `CompactionPolicyEvaluator` |
| `dev.kdb.storage.compaction` (10f) | `CompactionScheduler` / `runSstableCompaction` — optional post-hook |
| `dev.kdb.storage.manager.tier` (11e) | `TierSignalHooks` — subscribe for segment pressure (optional) |

-----

## 3. Public Interface

```kotlin
package dev.kdb.compaction

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.policy.NamespacePolicy
import dev.kdb.storage.StorageAdapter

interface CompactionEngine {
    /** Run one compaction cycle for a namespace (admin / scheduled). */
    suspend fun runCycle(request: CompactionRequest): CompactionResult

    /** Dry-run: report what would be squashed without mutating DAG. */
    suspend fun plan(request: CompactionRequest): CompactionPlan

    /** Register peer HEAD hashes used for safety checks. */
    fun updatePeerHeads(namespaceId: String, heads: Set<KdbHash>)
}

fun compactionEngine(
    dag: CommitDag,
    storage: StorageAdapter,
    policyProvider: suspend (String) -> NamespacePolicy,
    coordinator: CompactionCoordinator = InProcessCompactionCoordinator(),
    materializer: SnapshotMaterializer = DefaultSnapshotMaterializer(storage),
    gc: OrphanBlobGc = DefaultOrphanBlobGc(storage),
): CompactionEngine

data class CompactionRequest(
    val namespaceId: String,
    val force: Boolean = false,              // skip peer wait (single-node dev only)
    val maxSquashCommits: Int = 10_000,
)

data class CompactionResult(
    val squashedCount: Int,
    val syntheticRoot: KdbHash?,
    val gcReclaimedBytes: Long,
    val storageJobsEnqueued: Int,
)

data class CompactionPlan(
    val boundaries: List<PlannedSquash>,
    val peerSafe: Boolean,
    val blockers: List<CompactionBlocker>,
)

data class PlannedSquash(
    val boundary: KdbHash,
    val squashHashes: List<KdbHash>,
    val strategy: dev.kdb.policy.RetainStrategy,
)

sealed class CompactionBlocker {
    data class ProtectedTag(val tag: String, val hash: KdbHash) : CompactionBlocker()
    data class ProtectedBranch(val branch: String, val hash: KdbHash) : CompactionBlocker()
    data class PeerBelowBoundary(val peerId: String, val head: KdbHash) : CompactionBlocker()
    data class PolicyDisabled(val reason: String) : CompactionBlocker()
}

/** Peer coordination — in-process v1; wire adapter in Layer 7. */
interface CompactionCoordinator {
    suspend fun broadcastIntent(intent: CompactionIntent): CompactionAckSet
}

data class CompactionIntent(
    val namespaceId: String,
    val boundary: KdbHash,
    val issuedAtMillis: Long,
)

data class CompactionAckSet(
    val ackedPeers: Set<String>,
    val rejected: Map<String, KdbHash>,     // peerId → their HEAD below boundary
)

class InProcessCompactionCoordinator : CompactionCoordinator

/** Build synthetic tree at boundary from document state. */
interface SnapshotMaterializer {
    suspend fun materializeAt(commit: KdbHash): DocumentTree
}

interface OrphanBlobGc {
    suspend fun sweep(namespaceId: String, reachableHashes: Set<KdbHash>): Long
}

/** Scheduled runner (CLI `kdb compact`, cron). */
interface CompactionScheduler {
    fun start(intervalMillis: Long = 3600_000)
    fun stop()
}
```

-----

## 4. Data Structures

### Squash pipeline (internal)
1. Load policy → if `SquashMode.NEVER`, return empty plan.
2. `CompactionPolicyEvaluator.boundaryCandidates(...)`.
3. For each candidate, `dag.compactableBefore(boundary, peerHeads)`.
4. `coordinator.broadcastIntent` unless `force`.
5. `materializer.materializeAt(boundary)` → `syntheticTree`.
6. `dag.squash(squashHashes, boundary, syntheticTree, schemaHash)`.
7. `gc.sweep` + optional 10f enqueue.

### `CompactionNotice` (wire preview, Layer 7)
Logical payload for message type `0x08` — not encoded in this module v1; coordinator interface accepts callbacks for future `WireProtocol` adapter.

### Tag preservation
Per Layer 2 `squash` contract: tags on squashed commits redirect to synthetic root — engine does not delete tags.

### Synthetic commit message
Default `"compaction: squash N commits below {boundary.shortHex}"`.

-----

## 5. Contracts

### `CompactionEngine.plan`
- **Postconditions:** No DAG mutation. `peerSafe == true` iff no `PeerBelowBoundary` blockers. Every `squashHashes` entry passes `compactableBefore` dry-run checks.

### `CompactionEngine.runCycle`
- **Preconditions:** Namespace policy loaded. DAG not read-only external snapshot.
- **Postconditions:** If squashed, exactly one new synthetic root; all squashed hashes removed from active walk; tags preserved. `CompactionBoundaryException` impossible for retained history above boundary.
- **On `CompactionSafetyException`:** No partial squash (atomic per boundary).

### `SnapshotMaterializer.materializeAt`
- **Postconditions:** Tree reflects document state at `commit` (all doc ids → content hashes at that version). Same result as checkout + full scan.

### `OrphanBlobGc.sweep`
- **Postconditions:** Returns bytes reclaimed estimate; only deletes blobs unreferenced by any remaining commit tree or index manifest in namespace.

### Peer coordination
- **Preconditions:** `updatePeerHeads` called for known peers before production run.
- **Postconditions:** Squash proceeds only if all registered peers ack HEAD ≥ boundary (or `force` in dev).

### Distinction from 10f
Calling `runCycle` may incrementally trigger **at most one** SSTable merge job via injected callback — never inline LSM merge inside squash transaction.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `CompactionSafetyException` | Tag/branch/peer would be lost (from DAG) |
| `CompactionBoundaryException` | Caller uses hash below last compaction boundary |
| `PolicyDisabled` (wrapped) | `SquashMode.NEVER` |
| `PeerCompactionRejectedException` | Coordinator rejections and not `force` |
| `SnapshotMaterializationException` | Missing document tree at boundary |

```kotlin
class PeerCompactionRejectedException(
    val namespaceId: String,
    val boundary: KdbHash,
    val rejected: Map<String, KdbHash>,
) : KdbException("peer compaction rejected for $namespaceId")

class SnapshotMaterializationException(
    val commit: KdbHash,
    cause: Throwable? = null,
) : KdbException("failed to materialize snapshot at $commit", cause)
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `plan_respectsTag` | Tag on commit in squash range | Blocker or excluded from squash list |
| 2 | `squash_reducesDepth` | 20 linear commits, 1 boundary | 1 synthetic root; walk depth reduced |
| 3 | `tagSurvivesSquash` | Tag on squashed commit | `getTag` resolves to synthetic root |
| 4 | `peerHead_blocks` | Peer HEAD below boundary | `peerSafe=false`; runCycle no-op |
| 5 | `force_skipsPeerWait` | `force=true` | Squash despite peer |
| 6 | `neverSquash_policy` | `SquashMode.NEVER` | Empty plan |
| 7 | `gc_reclaimsOrphan` | Squash + orphan blob | `gcReclaimedBytes > 0` |
| 8 | `dailySnapshot_rule` | Retain DAILY_SNAPSHOTS | ≤1 commit/day kept in window |
| 9 | `branchPoint_kept` | Branch at intermediate commit | Not in `squashHashes` |
| 10 | `materializer_matchesCheckout` | Materialize vs manual tree | Identical doc hashes |
| 11 | `safetyException_atomic` | Squash with protected tag | No DAG change |
| 12 | `storageHook_enqueues` | Large HOT after squash | `storageJobsEnqueued >= 0` |

-----

## 8. Non-Goals

- **Ice archival / stub commits** — Layer 7 Storage Tier Manager (`stubCommit`, archive bundles).
- **Wire framing of CompactionIntent** — Layer 7; v1 uses `InProcessCompactionCoordinator`.
- **Cross-namespace compaction** — one namespace per request.
- **Automatic scheduled daemon on JS browser** — JVM/native only for `CompactionScheduler` v1; browser invokes manual `runCycle`.
- **Rewriting indexes during squash** — indexes remain commit-stamped; historical queries use ancestry (Component 8).
- **SSTable merge algorithms** — Component 10f.

-----

## 9. Implementation Notes

### Synthetic tree build
Walk `DocumentTree` at boundary commit; copy docId → contentHash map into new `DocumentTree` with fresh Merkle root (Layer 1 helpers).

### GC reachability
Collect hashes: all commits in DAG, document trees, index manifests referenced from storage adapter listing for namespace.

### Policy integration
Inject `CompactionPolicyEvaluator` from Component 18; unit-test with in-memory DAG + policy fixtures.

### CLI
`kdb compact myapp/users` → `CompactionEngine.runCycle` (master §11).

### Module layout
```
dev.kdb.compaction
  CompactionEngine.kt
  CompactionPlanner.kt
  CompactionCoordinator.kt
  SnapshotMaterializer.kt
  OrphanBlobGc.kt
  CompactionScheduler.kt
```

### KMP
`commonMain`. Scheduler uses coroutine `delay` on JVM/native; browser exposes `runCycle` only.

### Naming collision
Gradle module `:kdb-compaction` vs `:kdb-storage-compaction` — import packages distinguish `dev.kdb.compaction` vs `dev.kdb.storage.compaction`.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| Planner + policy integration | 600 |
| `CompactionEngine` run/plan | 500 |
| Coordinator (in-process + hooks) | 300 |
| Snapshot materializer | 400 |
| Orphan GC | 350 |
| Scheduler + CLI adapter | 200 |
| Tests | 650 |
| **Total** | **~3,000** |
