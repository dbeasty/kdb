# KDB Component Spec — Layer 4b
## Component 11a: Realized Store Pool
### `dev.kdb.storage.manager.pool`

**File:** `kdb-spec-layer4b-component11a-realized-store-pool.md`
**Layer:** 4b — Storage Manager
**Status:** Implementation-ready
**Depends on:** Layer 3 Component 9 (`StorageAdapter`, `RealizedStoreHandle`, `EnlistmentEvictionState`, `RebuildBlockingPolicy`), Layer 4a Storage Engine Core

---

## 1. Purpose

The Realized Store Pool is the central registry and reference-counting layer for all in-memory realized stores on a node. It implements `StorageManager.requestRealized`, maintains a global memory budget across enlistments, and hands out `RealizedStoreHandle` instances that delegate to the correct Layer 4a engine adapter. Every query and transaction path that needs a materialised document or index tree at a named commit acquires a handle here; releasing the handle decrements the ref count and makes the enlistment eligible for eviction (Component 11b).

This module owns handle identity and lifetime but does not perform LRU eviction, async rebuild, or browser enlistment lifecycle — those are Components 11b–11d.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.codec` | `KdbUuid`, `KdbHash` |
| `dev.kdb.error` | `KdbException`, `KdbErrorCode` |
| `dev.kdb.storage` (Component 9) | `StorageAdapter`, `EvictableStorageAdapter`, `RealizedStoreHandle`, `EnlistmentEvictionState`, `RebuildBlockingPolicy`, `IndexPinViolationEvent`, `StorageEngineConfig`, `EnlistmentNotFoundException` |
| `dev.kdb.storage.engine` (Layer 4a Component 10e) | `StorageEngine`, engine factory / registry per implementation variant |
| `dev.kdb.storage.manager.eviction` (Component 11b) | `EvictionManager` — invoked when ref count hits zero or memory pressure is reported |
| `dev.kdb.storage.manager.rebuild` (Component 11c) | `RebuildScheduler` — notified when a handle is issued for a non-`FULL` enlistment |

---

## 3. Public Interface

```kotlin
package dev.kdb.storage.manager.pool

import dev.kdb.codec.*
import dev.kdb.error.*
import dev.kdb.storage.*
import dev.kdb.storage.engine.StorageEngine
import dev.kdb.storage.manager.eviction.EvictionManager
import dev.kdb.storage.manager.rebuild.RebuildScheduler
import kotlinx.coroutines.flow.StateFlow

/**
 * Process-wide singleton orchestrator for realized stores.
 * Obtained via [StorageManager.get] after [StorageManager.install].
 */
interface StorageManager {

  companion object {
    /** Install the global instance. Throws if already installed. */
    fun install(instance: StorageManager)

    /** Returns the installed instance. Throws [StorageManagerNotInstalledException] if absent. */
    fun get(): StorageManager
  }

  /** Active memory budget configuration (may be updated at runtime within bounds). */
  val config: StorageEngineConfig

  /** Bytes currently attributed to realized stores (doc + index, all enlistments). */
  val realizedBytesInUse: StateFlow<Long>

  /**
   * Acquire a reference-counted handle to the realized store for [enlistmentId]
   * at [commitHash]. If the enlistment is `DOC_EVICTED` or `EVICTED`, returns
   * immediately with [RealizedStoreHandle.isReady] false and schedules rebuild
   * via [RebuildScheduler] (Component 11c).
   */
  suspend fun requestRealized(
    enlistmentId: KdbUuid,
    commitHash: KdbHash,
    blockingPolicy: RebuildBlockingPolicy = RebuildBlockingPolicy.WAIT,
  ): RealizedStoreHandle

  /**
   * Lookup an existing open handle without incrementing ref count.
   * Returns null if no registry entry or enlistment is [EnlistmentEvictionState.RELEASED].
   */
  fun peekRealized(enlistmentId: KdbUuid): RealizedStoreHandle?

  /** Current eviction state for an enlistment (delegates to pool entry). */
  fun evictionState(enlistmentId: KdbUuid): EnlistmentEvictionState

  /** Total open handle count across all enlistments (diagnostics). */
  val openHandleCount: Int

  /** Shut down: release all handles, drain pool. Idempotent. */
  suspend fun shutdown()
}

/**
 * Internal-facing pool API; [DefaultStorageManager] delegates here.
 */
interface RealizedStorePool {

  /** Register or refresh a pool entry after enlistment creation (11d). */
  suspend fun registerEntry(entry: PoolEntry)

  /** Remove registry entry after enlistment release. */
  suspend fun unregisterEntry(enlistmentId: KdbUuid)

  suspend fun acquire(
    enlistmentId: KdbUuid,
    commitHash: KdbHash,
    blockingPolicy: RebuildBlockingPolicy,
  ): PooledRealizedStoreHandle

  fun release(handleId: HandleId)

  fun entry(enlistmentId: KdbUuid): PoolEntry?

  /** Sum of docBytes + indexBytes for budget accounting. */
  fun totalRealizedBytes(): Long
}

interface HandleRegistry {
  fun register(handle: PooledRealizedStoreHandle): HandleId
  fun retain(handleId: HandleId): Int
  fun release(handleId: HandleId): Int
  fun refCount(handleId: HandleId): Int
  fun get(handleId: HandleId): PooledRealizedStoreHandle?
}

/** Reference-counted [RealizedStoreHandle] implementation owned by the pool. */
class PooledRealizedStoreHandle internal constructor(
  val handleId: HandleId,
  override val namespaceId: String,
  override val commitHash: KdbHash,
  override val enlistmentId: KdbUuid,
  internal val pool: RealizedStorePool,
  internal val entry: PoolEntry,
  private val rebuildScheduler: RebuildScheduler,
) : RealizedStoreHandle {

  override val storage: StorageAdapter get() = entry.engine.adapterFor(enlistmentId)

  override val isReady: Boolean
    get() = entry.evictionState == EnlistmentEvictionState.FULL && !entry.rebuildPending

  override suspend fun awaitReady(blockingPolicy: RebuildBlockingPolicy) {
    if (isReady) return
    rebuildScheduler.awaitReady(enlistmentId, blockingPolicy)
  }

  override fun close() = release()
  override fun release() = pool.release(handleId)

  private val pinViolationHandlers = mutableListOf<(IndexPinViolationEvent) -> Unit>()
  override fun onIndexPinViolation(handler: (IndexPinViolationEvent) -> Unit) {
    pinViolationHandlers += handler
    entry.pinViolationSink = { event -> pinViolationHandlers.forEach { it(event) } }
  }
}

@JvmInline
value class HandleId(val value: Long)

class DefaultStorageManager(
  override val config: StorageEngineConfig,
  private val pool: RealizedStorePool,
  private val registry: HandleRegistry,
  private val evictionManager: EvictionManager,
  private val rebuildScheduler: RebuildScheduler,
) : StorageManager { /* implements companion + interface */ }

class DefaultRealizedStorePool(
  private val registry: HandleRegistry,
  private val rebuildScheduler: RebuildScheduler,
  private val memoryBudgetBytes: () -> Long,
) : RealizedStorePool { /* ... */ }

class DefaultHandleRegistry : HandleRegistry { /* thread-safe atomic ref counts */ }
```

---

## 4. Data Structures

### `PoolEntry`
One row per active enlistment in the pool.

| Field | Type | Description |
|---|---|---|
| `enlistmentId` | `KdbUuid` | Stable enlistment identity |
| `namespaceId` | `String` | Namespace this enlistment serves |
| `engine` | `StorageEngine` | Layer 4a engine instance (Server / Browser / InMemory / GPU) |
| `evictionState` | `EnlistmentEvictionState` | Mirrored from eviction manager; starts at `FULL` |
| `indexRetention` | `IndexRetention` | From namespace policy |
| `docBytes` | `Long` | Last-reported document store footprint |
| `indexBytes` | `Long` | Last-reported index store footprint |
| `rebuildPending` | `Boolean` | True while Component 11c has an in-flight rebuild |
| `currentCommitHash` | `KdbHash` | HEAD commit the realized store materialises |
| `pinViolationSink` | `((IndexPinViolationEvent) -> Unit)?` | Wired from active handles |

### `HandleId`
Opaque monotonic id assigned at first `requestRealized` for a pool entry; stable for the lifetime of that handle instance in the registry.

### `MemoryBudgetSnapshot`
Diagnostic snapshot: `budgetBytes`, `usedBytes`, `reservedBytes` (handles held but not yet charged), `enlistmentCount`.

---

## 5. Contracts

### `StorageManager.install` / `get`
**Singleton:** At most one `StorageManager` per process. Second `install` throws `StorageManagerAlreadyInstalledException`. `get` before `install` throws `StorageManagerNotInstalledException`.

### `requestRealized`
**Preconditions:** `enlistmentId` must exist in the pool (`registerEntry` called by Enlistment Manager). `commitHash` must be reachable on the enlistment's branch (caller responsibility; pool does not validate DAG membership).

**Postconditions:**
- Returns a `PooledRealizedStoreHandle` with ref count incremented.
- If `evictionState` is `FULL`, `isReady` is true unless a concurrent rebuild flag was set.
- If `DOC_EVICTED` or `EVICTED`, `isReady` is false, `rebuildPending` is set, and `RebuildScheduler.scheduleRebuild` is invoked exactly once per transition to not-ready (deduplicated by 11c).
- When `blockingPolicy == WAIT`, caller may call `awaitReady()`; pool does not block inside `requestRealized` itself.

**Reference counting:** Each successful `requestRealized` increments ref count. Each `release()` decrements. Eviction (11b) may only reclaim doc/index memory when ref count is zero for all handles on that enlistment.

### `peekRealized`
Does not change ref count. Returns the canonical handle wrapper for the enlistment if any handle is registered; useful for idempotent re-entry from the same coroutine scope.

### Memory budget
**Accounting:** `docBytes + indexBytes` per `PoolEntry` summed into `realizedBytesInUse`. When sum exceeds `config.globalMemoryBudgetBytes`, pool notifies `EvictionManager.onMemoryPressure(used, budget)` — does not evict directly.

### `shutdown`
Releases all handles (ref count forced to zero), sets all entries to `RELEASED`, unregisters entries. Does not close delta log files (Layer 4a).

---

## 6. Error Cases

| Exception | When |
|---|---|
| `StorageManagerNotInstalledException` | `StorageManager.get()` before `install`. |
| `StorageManagerAlreadyInstalledException` | Second `install`. |
| `EnlistmentNotFoundException` | `requestRealized` / `evictionState` for unknown `enlistmentId`. |
| `RealizedStoreReleasedException` | `acquire` or `release` on handle after enlistment `RELEASED`. |
| `StorageAdapterException` | Engine fails to open adapter for enlistment (propagated from Layer 4a). |

```kotlin
class StorageManagerNotInstalledException(message: String = "StorageManager not installed") : KdbException(message) {
  override val code get() = KdbErrorCode.STORAGE_TIER_ERROR
}
class StorageManagerAlreadyInstalledException(message: String = "StorageManager already installed") : KdbException(message)
class RealizedStoreReleasedException(val enlistmentId: KdbUuid) : KdbException("Enlistment released: $enlistmentId")
```

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `requestRealized_incrementsRefCount` | Two `requestRealized` on same enlistment. | `registry.refCount` == 2; same `enlistmentId`, distinct or shared handle per policy (shared canonical handle id). |
| 2 | `release_decrementsToZero` | Acquire then `release` twice from two handles. | Ref count 0; entry eligible for eviction notification. |
| 3 | `requestEvicted_schedulesRebuild` | Entry in `EVICTED`. `requestRealized`. | `isReady == false`; rebuild scheduler received one schedule call. |
| 4 | `awaitReady_blocksUntilFull` | `DOC_EVICTED` + mock rebuild completes. `awaitReady(WAIT)`. | Returns after state `FULL`; `isReady == true`. |
| 5 | `partialOk_returnsBeforeReady` | `EVICTED`, `requestRealized` with `PARTIAL_OK`. | Handle returned immediately; `isReady` false until rebuild completes. |
| 6 | `memoryBudget_triggersPressure` | Sum `docBytes+indexBytes` > budget. | `EvictionManager.onMemoryPressure` invoked once (debounced). |
| 7 | `peekRealized_noRefBump` | `peekRealized` then `requestRealized`. | Ref count increases by 1 only (not 2). |
| 8 | `shutdown_releasesAll` | Three open handles. `shutdown()`. | All ref counts 0; `openHandleCount == 0`. |
| 9 | `unknownEnlistment_throws` | `requestRealized` random UUID. | `EnlistmentNotFoundException`. |
| 10 | `doubleInstall_throws` | `install` twice. | `StorageManagerAlreadyInstalledException`. |

---

## 8. Non-Goals

- LRU eviction ordering and `IndexPinViolationEvent` escalation policy — Component 11b.
- Async rebuild execution and `RebuildJob` state machine — Component 11c.
- Enlistment creation, engine selection, browser push/resolve — Component 11d.
- Delta segment tier classification — Component 11e.
- Delta log append, WAL, compaction — Layer 4a.
- GPU promotion queue — coordinated by 11d/11e hooks; ingest path is Layer 4a `GpuStorageEngine`.

---

## 9. Implementation Notes

### Singleton installation
Use a `AtomicReference<StorageManager?>` in `commonMain`. Tests call `install` with in-memory engines; production JVM/JS entry points install once at node bootstrap.

### Canonical handle per enlistment
Prefer one `PooledRealizedStoreHandle` per `(enlistmentId)` in the registry; `requestRealized` calls `retain` on the same `HandleId`. Simplifies `onIndexPinViolation` fan-out and ref-count semantics.

### `commitHash` on re-request
If `requestRealized` is called with a newer `commitHash` than `PoolEntry.currentCommitHash`, pool updates the entry and may invalidate partial materialisation — delegate to engine's `advanceToCommit` (Layer 4a) before returning handle.

### Thread safety
`HandleRegistry` operations must be thread-safe; KDB nodes serve concurrent queries. Use per-entry mutex or striped locks; ref count updates via `AtomicInteger`.

### KMP
All types in `commonMain`. No `expect/actual` in this module.

### Interaction with GPU `supportsDirectDeltaIngest`
Pool registers GPU enlistments like CPU enlistments; rebuild path for GPU uses `ingestDeltaSegment` via 11c, not document replay.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `StorageManager` + companion | 80 |
| `DefaultStorageManager` | 120 |
| `RealizedStorePool` + `DefaultRealizedStorePool` | 200 |
| `HandleRegistry` + `PooledRealizedStoreHandle` | 150 |
| `PoolEntry` + diagnostics | 60 |
| Exceptions | 40 |
| Unit + integration tests | 450 |
| **Total** | **~1,100** |
