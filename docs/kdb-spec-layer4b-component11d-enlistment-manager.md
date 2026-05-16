# KDB Component Spec — Layer 4b
## Component 11d: Enlistment Manager
### `dev.kdb.storage.manager.enlistment`

**File:** `kdb-spec-layer4b-component11d-enlistment-manager.md`
**Layer:** 4b — Storage Manager
**Status:** Implementation-ready
**Depends on:** Layer 3 Component 9 (`EnlistmentHandle`, `PushResult`, `SnapshotRestoreResult`, `StorageCapabilitySet`), Components 11a–11c, Layer 4a engines

---

## 1. Purpose

The Enlistment Manager owns enlistment lifecycle: creation, engine selection, registration with the Realized Store Pool, browser push/resolve against upstream peers, and realized-store snapshot write/restore. It implements `StorageManager.requestEnlistment` and the browser-specific `EnlistmentHandle` behaviour from Component 9 — including the v3 rule that only the realized store is snapshotted to localStorage (delta log is never persisted locally) and repair always re-fetches state from a peer when snapshot load fails.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.codec` | `KdbUuid`, `KdbHash`, `KdbTimestamp` |
| `dev.kdb.error` | `KdbException`, `CompactionBoundaryException` |
| `dev.kdb.storage` | `EnlistmentHandle`, `RealizedStoreHandle`, `PushResult`, `EnlistmentPushState`, `SnapshotRestoreResult`, `SnapshotFailureReason`, `StorageCapabilitySet`, `IndexRetention`, `GpuPromotionPolicy`, `PlatformIoShim`, `StorageEngineConfig` |
| `dev.kdb.storage.engine` | `StorageEngine`, `StorageEngineRegistry` |
| `dev.kdb.storage.manager.pool` | `StorageManager`, `RealizedStorePool`, `PoolEntry` |
| `dev.kdb.storage.manager.rebuild` | `RebuildScheduler` |
| `dev.kdb.dag` | `CommitDag` — branch HEAD for anchor validation |

---

## 3. Public Interface

```kotlin
package dev.kdb.storage.manager.enlistment

import dev.kdb.codec.*
import dev.kdb.dag.CommitDag
import dev.kdb.storage.*
import dev.kdb.storage.manager.pool.StorageManager

interface EnlistmentManager {

  /**
   * Create a new enlistment on [branchRef] for [namespaceId].
   * Registers [PoolEntry], returns handle (browser: [BrowserEnlistmentHandle]).
   */
  suspend fun requestEnlistment(
    request: EnlistmentRequest,
  ): EnlistmentHandle

  suspend fun releaseEnlistment(enlistmentId: KdbUuid)

  fun activeEnlistments(): List<EnlistmentSummary>

  /** Repair path: fetch realized state from [upstreamPeer] when snapshot failed. */
  suspend fun repairFromPeer(
    enlistmentId: KdbUuid,
    upstreamPeer: PeerRef,
    anchorHint: KdbHash?,
  ): RepairResult
}

data class EnlistmentRequest(
  val namespaceId: String,
  val branchRef: String,
  val engineHint: EngineHint = EngineHint.AUTO,
  val indexRetention: IndexRetention? = null,
  val gpuPromotion: GpuPromotionPolicy? = null,
  val initialCommitHash: KdbHash? = null,
)

enum class EngineHint { AUTO, SERVER, BROWSER, IN_MEMORY, GPU }

data class EnlistmentSummary(
  val enlistmentId: KdbUuid,
  val namespaceId: String,
  val branchRef: String,
  val engineKind: EngineKind,
  val evictionState: EnlistmentEvictionState,
)

enum class EngineKind { SERVER, BROWSER, IN_MEMORY, GPU }

data class PeerRef(val nodeId: KdbUuid, val transportUri: String)

sealed class RepairResult {
  data class Restored(val anchorHash: KdbHash) : RepairResult()
  data class Failed(val reason: RepairFailureReason) : RepairResult()
}

enum class RepairFailureReason {
  PEER_UNREACHABLE, ANCHOR_COMPACTED_AWAY, SYNC_REJECTED, TIMEOUT,
}

/**
 * Browser enlistment handle — implements Layer 3 [EnlistmentHandle].
 */
class BrowserEnlistmentHandle internal constructor(
  private val pooled: dev.kdb.storage.manager.pool.PooledRealizedStoreHandle,
  private val lifecycle: BrowserEnlistmentLifecycle,
) : EnlistmentHandle, RealizedStoreHandle by pooled {

  override val branchRef: String get() = lifecycle.branchRef
  override val pushState: EnlistmentPushState get() = lifecycle.pushState

  override suspend fun push(): PushResult = lifecycle.push()
  override suspend fun fetchMissing(): Unit = lifecycle.fetchMissing()
  override suspend fun resolveAndPush(): PushResult = lifecycle.resolveAndPush()
  override val snapshotAnchorHash: KdbHash? get() = lifecycle.snapshotAnchorHash
  override suspend fun writeSnapshot(): Unit = lifecycle.writeSnapshot()
  override suspend fun restoreSnapshot(): SnapshotRestoreResult = lifecycle.restoreSnapshot()
}

interface BrowserEnlistmentLifecycle {
  val branchRef: String
  val pushState: EnlistmentPushState
  suspend fun push(): PushResult
  suspend fun fetchMissing()
  suspend fun resolveAndPush(): PushResult
  val snapshotAnchorHash: KdbHash?
  suspend fun writeSnapshot()
  suspend fun restoreSnapshot(): SnapshotRestoreResult
}

/** Engine selection: hint → namespace policy → platform default → fallback. */
interface EngineSelector {
  fun select(request: EnlistmentRequest, registry: StorageEngineRegistry): StorageEngine
}

/** Extends [StorageManager] with enlistment APIs (facade composition). */
interface StorageManagerEnlistmentExtensions {
  suspend fun requestEnlistment(request: EnlistmentRequest): EnlistmentHandle
  suspend fun releaseEnlistment(enlistmentId: KdbUuid)
}
```

`DefaultStorageManager` implements `StorageManager` + `StorageManagerEnlistmentExtensions` by delegating enlistment to `EnlistmentManager`.

---

## 4. Data Structures

### `EnlistmentRecord` (internal)
| Field | Type | Description |
|---|---|---|
| `enlistmentId` | `KdbUuid` | Generated at create |
| `namespaceId` | `String` | |
| `branchRef` | `String` | Independent branch per enlistment |
| `engine` | `StorageEngine` | Selected instance |
| `dag` | `CommitDag` | Branch-local DAG view |
| `upstreamPeer` | `PeerRef?` | Browser sync target |
| `snapshotKey` | `String` | `kdb:snapshot:{enlistmentId}` for `PlatformIoShim` |
| `localDeltaBuffer` | `List<DeltaRecord>` | Browser-only; not persisted to localStorage |

### Snapshot blob format (browser)
Layer 0 envelope: `anchorHash`, `Map<KdbUuid, KdbDocument>`, `indexState: ByteArray`, optional `schemaHash`. Outer zstd. Written via `PlatformIoShim.writeSnapshot`. **Delta log excluded.**

### `PushStateMachine`
```
IDLE → PUSHING → Success → IDLE
              → Rejected → REJECTED → fetchMissing → RESOLVING → resolveAndPush → IDLE | Rejected
```

### `EngineSelectionChain` (internal)
1. `request.engineHint` if not `AUTO`
2. Namespace policy `preferredEngine`
3. Platform default (`BROWSER` on jsMain, `SERVER` on jvmMain)
4. Fallback `IN_MEMORY` if caps allow

---

## 5. Contracts

### `requestEnlistment`
**Postconditions:**
- New `KdbUuid` enlistment id.
- `RealizedStorePool.registerEntry` with selected engine and `indexRetention` from request or `StorageEngineConfig.defaultIndexRetention`.
- Returns `EnlistmentHandle` wrapping pooled realized handle at `initialCommitHash` or branch HEAD.
- Browser: attempts `restoreSnapshot()` before serving; on `Failed` or `AnchorCompactedAway`, calls `repairFromPeer` synchronously if `upstreamPeer` configured, else returns handle with `isReady == false` until repair completes.

### `releaseEnlistment`
Decrements all handles, transitions to `RELEASED`, `unregisterEntry`, deletes snapshot key best-effort.

### `writeSnapshot`
**Best-effort:** Never throws. Serialises current realized documents + index snapshot + `anchorHash = currentCommitHash`. Skips if size > ~5 MB localStorage limit (log warning).

### `restoreSnapshot`
**Postconditions:**
- `Restored`: realized store loaded; `snapshotAnchorHash` set; background delta sync may still run.
- `Failed`: caller must `repairFromPeer`.
- `AnchorCompactedAway`: distinct from generic failure; repair must full re-sync without using stale anchor.

### `push` / `fetchMissing` / `resolveAndPush`
**Push:** Sends local commits since last acked hash to upstream. Returns `PushResult.Success` or `Rejected(missingDeltaHashes)`.

**fetchMissing:** Transitions to `RESOLVING`; pulls missing deltas by hash from peer into `localDeltaBuffer`; applies via transaction replay path.

**resolveAndPush:** Application resolves conflicts locally, then retries push.

### Delta not persisted (browser)
`localDeltaBuffer` is memory-only. Process restart loses deltas; recovery is snapshot + peer sync per v3.

### `repairFromPeer`
Fetches materialised realized state (or delta range from `anchorHint` to HEAD). On `AnchorCompactedAway`, requests full state snapshot from peer without anchor. Updates pool entry to `FULL`.

---

## 6. Error Cases

| Exception | When |
|---|---|
| `EnlistmentLimitExceededException` | `maxEnlistments` capability exceeded. |
| `NamespaceNotFoundException` | Unknown `namespaceId`. |
| `EnlistmentNotFoundException` | `release` / `repair` for unknown id. |
| `CompactionBoundaryException` | Peer cannot supply deltas for anchor (surfaced during repair). |
| `SnapshotIntegrityException` | Internal decode failure (mapped to `SnapshotRestoreResult.Failed`, not thrown from `restoreSnapshot`). |

```kotlin
class EnlistmentLimitExceededException(
  val max: Int,
) : KdbException("Enlistment limit $max exceeded")
```

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `requestEnlistment_registersPool` | Valid request. | Pool contains entry; handle non-null. |
| 2 | `engineHint_gpu_selectsGpu` | `EngineHint.GPU`, GPU registered. | `EngineKind.GPU`. |
| 3 | `snapshotRoundtrip_writeRestore` | `writeSnapshot` then `restoreSnapshot`. | `Restored(anchorHash)`. |
| 4 | `corruptSnapshot_failedNotThrown` | Corrupt localStorage bytes. | `Failed(INTEGRITY_CHECK_FAILED)`. |
| 5 | `repairFromPeer_afterFailedSnapshot` | Failed restore + mock peer. | `RepairResult.Restored`. |
| 6 | `anchorCompactedAway_distinct` | Restore returns `AnchorCompactedAway`. | Repair uses full sync path. |
| 7 | `pushRejected_fetchResolve` | Push returns `Rejected`. | `fetchMissing` then `resolveAndPush` returns `Success`. |
| 8 | `deltaNotInSnapshot` | Inspect snapshot bytes. | No delta segment payload fields. |
| 9 | `release_unregistersPool` | `releaseEnlistment`. | State `RELEASED`; `requestRealized` throws. |
| 10 | `enlistmentLimit_throws` | Create N+1 browser enlistments, cap N. | `EnlistmentLimitExceededException`. |

---

## 8. Non-Goals

- Wire protocol framing for peer messages — Layer 7.
- Transaction conflict resolution algorithms — Layer 3 Transaction Engine (caller supplies resolution before `resolveAndPush`).
- GPU promotion scheduling — signals from 11e; ingest in 11c.
- Server-side durable enlistment persistence — Layer 4a server engine.
- Auth validation of `DeltaAuthorshipEnvelope` — upper layer interceptor.

---

## 9. Implementation Notes

### Facade on `StorageManager`
`DefaultStorageManager` holds `EnlistmentManager` and forwards `requestEnlistment` / `releaseEnlistment`. Keeps 11a pool as single registry.

### Snapshot key isolation
One key per enlistment prevents cross-branch corruption. Delete key on `release`.

### Background sync after `Restored`
On successful snapshot load, serve queries immediately (`isReady` true) while async task requests deltas from `anchorHash` to peer HEAD (stream mode, Layer 7).

### Multi-enlistment browser
Each enlistment has independent `branchRef`, delta buffer, snapshot key. Global memory budget still enforced by 11a/11b.

### jsMain vs jvmMain
`BrowserEnlistmentLifecycle` only on JS; server enlistments return `PooledRealizedStoreHandle` implementing `RealizedStoreHandle` only (no push API).

### KMP
`BrowserEnlistmentHandle` in `jsMain` actual or `commonMain` with expect lifecycle — prefer `commonMain` interface + `jsMain` lifecycle impl.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `EnlistmentManager` + records | 220 |
| `EngineSelector` | 80 |
| `BrowserEnlistmentLifecycle` | 250 |
| `BrowserEnlistmentHandle` | 60 |
| Snapshot encode/decode | 120 |
| Push state machine + peer stubs | 150 |
| Tests | 450 |
| **Total** | **~1,330** |
