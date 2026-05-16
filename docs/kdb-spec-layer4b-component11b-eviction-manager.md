# KDB Component Spec — Layer 4b
## Component 11b: Eviction Manager
### `dev.kdb.storage.manager.eviction`

**File:** `kdb-spec-layer4b-component11b-eviction-manager.md`
**Layer:** 4b — Storage Manager
**Status:** Implementation-ready
**Depends on:** Layer 3 Component 9, Component 11a (Realized Store Pool), Layer 4a `EvictableStorageAdapter`

---

## 1. Purpose

The Eviction Manager enforces the global memory budget for realized stores by evicting at sub-enlistment granularity: document store first, then index store (when `IndexRetention.EVICTABLE`), then full enlistment release. It drives transitions on the `EnlistmentEvictionState` machine (`FULL` → `DOC_EVICTED` → `EVICTED` → `RELEASED`) and emits `IndexPinViolationEvent` when `PINNED` indexes cannot be honoured under extreme pressure. LRU ordering uses separate weights for document and index components so query-heavy enlistments lose raw `_doc` materialisation before losing indexes.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.codec` | `KdbUuid` |
| `dev.kdb.error` | `KdbException` |
| `dev.kdb.storage` | `EnlistmentEvictionState`, `IndexRetention`, `IndexPinViolationEvent`, `EvictableStorageAdapter`, `EnlistmentNotFoundException` |
| `dev.kdb.storage.manager.pool` | `RealizedStorePool`, `PoolEntry`, `StorageManager` |
| `dev.kdb.storage.manager.rebuild` | `RebuildScheduler` — demand-driven rebuild when access hits evicted components |

---

## 3. Public Interface

```kotlin
package dev.kdb.storage.manager.eviction

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.*

interface EvictionManager {

  /**
   * Called by the pool when realized bytes exceed budget.
   * Evicts until [targetBytes] reclaimed or no evictable candidates remain.
   * Returns bytes actually reclaimed.
   */
  suspend fun onMemoryPressure(usedBytes: Long, budgetBytes: Long): Long

  /**
   * Evict document store for one enlistment (advisory).
   * No-op if ref count > 0 or state already `DOC_EVICTED`/`EVICTED`.
   */
  suspend fun evictDocuments(enlistmentId: KdbUuid): EvictionResult

  /**
   * Evict index store. Only legal when [IndexRetention.EVICTABLE]
   * and state is `FULL` or `DOC_EVICTED`.
   */
  suspend fun evictIndex(enlistmentId: KdbUuid): EvictionResult

  /** Record demand access — may promote LRU recency or trigger demand rebuild. */
  suspend fun onDemandAccess(enlistmentId: KdbUuid, component: EvictionComponent)

  /** Current state (mirrors pool entry after transition). */
  fun state(enlistmentId: KdbUuid): EnlistmentEvictionState

  /** Configure LRU weights (defaults: doc=1.0, index=0.35). */
  fun configureWeights(weights: LruWeights)
}

data class LruWeights(
  val documentWeight: Double = 1.0,
  val indexWeight: Double = 0.35,
)

enum class EvictionComponent { DOCUMENT_STORE, INDEX_STORE }

sealed class EvictionResult {
  data class Evicted(val bytesFreed: Long, val newState: EnlistmentEvictionState) : EvictionResult()
  object SkippedRefCountHeld : EvictionResult()
  object SkippedPinnedIndex : EvictionResult()
  object SkippedAlreadyEvicted : EvictionResult()
}

/**
 * LRU queue with separate tracks for doc and index eviction units.
 */
interface LruEvictionQueue {
  fun touch(enlistmentId: KdbUuid, component: EvictionComponent, sizeBytes: Long)
  fun remove(enlistmentId: KdbUuid)
  /** Returns candidates ordered by eviction priority (doc track first). */
  fun candidatesForDocEviction(limit: Int): List<LruCandidate>
  fun candidatesForIndexEviction(limit: Int): List<LruCandidate>
}

data class LruCandidate(
  val enlistmentId: KdbUuid,
  val component: EvictionComponent,
  val weightedScore: Double,
  val sizeBytes: Long,
  val indexRetention: IndexRetention,
  val refCount: Int,
)

class DefaultEvictionManager(
  private val pool: RealizedStorePool,
  private val lru: LruEvictionQueue,
  private val weights: LruWeights = LruWeights(),
  private val pinViolationPolicy: IndexPinViolationPolicy = IndexPinViolationPolicy.DEFAULT,
) : EvictionManager
```

```kotlin
/** Escalation when PINNED index cannot be kept under OOM pressure. */
data class IndexPinViolationPolicy(
  val warnAtPressureRatio: Double = 0.95,
  val emitEventAtPressureRatio: Double = 0.98,
  val allowDegradeOneEnlistmentToEvictable: Boolean = true,
)

object IndexPinViolationPolicy {
  val DEFAULT = IndexPinViolationPolicy()
}
```

---

## 4. Data Structures

### State machine (per enlistment)

```
FULL          → document + index in memory
DOC_EVICTED   → index only; documents evicted
EVICTED       → neither; enlistment metadata retained
RELEASED      → terminal; entry removed from pool
```

| Transition | Trigger |
|---|---|
| `FULL → DOC_EVICTED` | Memory pressure; doc LRU victim; ref count == 0 |
| `FULL → EVICTED` | Extreme pressure; index `EVICTABLE`; ref count == 0 |
| `DOC_EVICTED → EVICTED` | Extreme pressure; index `EVICTABLE` |
| `DOC_EVICTED → FULL` | Rebuild completes (11c) or demand rebuild |
| `EVICTED → FULL` | Full rebuild completes |
| `* → RELEASED` | All handles released; enlistment closed |

### `LruEntry` (internal)
Tracks `lastAccessNanos`, `sizeBytes`, `component`, `enlistmentId`, precomputed `weightedScore = sizeBytes * weight`.

### `PinViolationRecord`
`enlistmentId`, `emittedAt`, `degradedToEvictable: Boolean` — prevents duplicate events per pressure spike.

---

## 5. Contracts

### `onMemoryPressure`
**Goal:** Reclaim `usedBytes - budgetBytes` (minimum zero).

**Algorithm (normative):**
1. Collect doc-track LRU candidates with `refCount == 0` and state `FULL`.
2. Evict documents via `EvictableStorageAdapter.evictDocuments` until target met or candidates exhausted.
3. If still over budget, collect index-track candidates where `indexRetention == EVICTABLE`, state `FULL` or `DOC_EVICTED`, `refCount == 0`.
4. Evict index via `evictIndex` until target met.
5. If still over budget and only `PINNED` indexes remain, apply `IndexPinViolationPolicy`:
   - Log warning at `warnAtPressureRatio`.
   - At `emitEventAtPressureRatio`, emit `IndexPinViolationEvent` on all open handles for affected enlistments (via pool `pinViolationSink`).
   - Optionally degrade **one** enlistment's effective retention to `EVICTABLE` if policy allows (caller may override via handler).

**Postcondition:** Pool entry `evictionState` matches engine `evictionState(enlistmentId)`.

### `evictDocuments` / `evictIndex`
**Preconditions:** Enlistment registered; adapter implements `EvictableStorageAdapter`.

**Postconditions:** On success, LRU entry for that component removed or zeroed; `bytesFreed` returned. On `SkippedRefCountHeld`, no engine call.

### `PINNED` index
Never selected for index eviction while enlistment is open. Document store for the same enlistment **may** still be evicted (`FULL → DOC_EVICTED`).

### `onDemandAccess`
When query touches evicted doc store (`DOC_EVICTED` or `EVICTED`), schedules rebuild via `RebuildScheduler` (11c) and touches LRU doc track if component was accessed.

---

## 6. Error Cases

| Exception | When |
|---|---|
| `EnlistmentNotFoundException` | Eviction called for unknown enlistment. |
| `IllegalEvictionStateException` | e.g. `evictIndex` when state is `EVICTED` and index already gone. |
| `StorageAdapterException` | Engine `evictDocuments` / `evictIndex` fails (I/O). |

```kotlin
class IllegalEvictionStateException(
  val enlistmentId: KdbUuid,
  val current: EnlistmentEvictionState,
  val attempted: String,
) : KdbException("Illegal eviction: $attempted in $current for $enlistmentId")
```

`IndexPinViolationEvent` is **not** an exception — it is delivered to handle subscribers.

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `docEvicted_beforeIndex` | Pressure with both components resident, `EVICTABLE`. | First transition `FULL → DOC_EVICTED`; index still present. |
| 2 | `pinnedIndex_skippedOnPressure` | `PINNED`, pressure after doc evicted. | Index not evicted; warning logged. |
| 3 | `refCountBlocksEviction` | Pressure, ref count 1. | `SkippedRefCountHeld`; state stays `FULL`. |
| 4 | `evictIndex_fromDocEvicted` | `DOC_EVICTED`, `EVICTABLE`, ref 0. | `EVICTED`; bytes freed includes index. |
| 5 | `pinViolationEvent_emitted` | Pressure 98%, only PINNED indexes left. | Handlers receive `IndexPinViolationEvent`. |
| 6 | `lruOrder_favorsOldDoc` | Two enlistments; touch A recently. | B's doc evicted first. |
| 7 | `weights_indexLessPriority` | Equal bytes; evict doc track only until budget met. | Index candidate score lower than doc at same recency. |
| 8 | `onDemandAccess_schedulesRebuild` | `DOC_EVICTED`, document read. | Rebuild scheduler invoked. |
| 9 | `releasedTerminal_noEvict` | State `RELEASED`. `evictDocuments`. | `IllegalEvictionStateException` or no-op per API contract. |
| 10 | `inMemoryEngine_noRebuildPath` | InMemory enlistment at pressure. | Eviction may transition to `EVICTED`/`RELEASED` without rebuild scheduling (data loss acceptable per engine capabilities). |

---

## 8. Non-Goals

- Reference counting and `requestRealized` — Component 11a.
- Executing rebuild jobs — Component 11c.
- Browser snapshot or peer repair — Component 11d.
- Moving segments between HOT/WARM/COLD/ICE — Component 11e / Layer 6 Tier Manager.
- Forcibly killing the process on OOM — platform responsibility.

---

## 9. Implementation Notes

### Separate LRU tracks
Maintain two logical heaps keyed by `weightedScore = sizeBytes * weight`. Doc track consulted first on every pressure event. Index track entries exist only when `indexRetention == EVICTABLE` and index `sizeBytes > 0`.

### Debounce `onMemoryPressure`
Pool may fire pressure frequently; coalesce within 50ms window per node to avoid thrashing.

### Fairness across engine types
`InMemoryStorageEngine` enlistments: evict without scheduling rebuild (open question in v3 — treat as lowest priority to retain because rebuild is impossible). Server/Browser enlistments prefer evicting largest doc footprints first.

### Sync with pool
After every successful engine eviction, update `PoolEntry.evictionState` and subtract `docBytes` / `indexBytes` atomically.

### KMP
Pure `commonMain`; use `kotlin.time` for access timestamps.

### `IndexPinViolationEvent` escalation path (v3)
1. Pressure exceeds `warnAtPressureRatio` → structured log with enlistment ids and pinned bytes.
2. Pressure exceeds `emitAtPressureRatio` → emit event to all `RealizedStoreHandle.onIndexPinViolation` subscribers.
3. If `allowDegradeOneEnlistmentToEvictable`, mark the largest PINNED enlistment as effectively `EVICTABLE` for one eviction cycle only (reverts if pressure drops).
4. Caller handler may release handles, flush caches, or abort queries — Storage Manager does not auto-evict PINNED indexes without degradation flag.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `EvictionManager` + `DefaultEvictionManager` | 280 |
| `LruEvictionQueue` + weighted scoring | 180 |
| `IndexPinViolationPolicy` + event fan-out | 80 |
| Exceptions + logging | 40 |
| Tests | 400 |
| **Total** | **~980** |
