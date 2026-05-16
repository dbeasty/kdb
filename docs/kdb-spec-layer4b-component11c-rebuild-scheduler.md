# KDB Component Spec — Layer 4b
## Component 11c: Rebuild Scheduler
### `dev.kdb.storage.manager.rebuild`

**File:** `kdb-spec-layer4b-component11c-rebuild-scheduler.md`
**Layer:** 4b — Storage Manager
**Status:** Implementation-ready
**Depends on:** Layer 3 Component 9 (`DeltaSegmentReader`, `EvictableStorageAdapter`, `RebuildBlockingPolicy`), Components 11a, 11b

---

## 1. Purpose

The Rebuild Scheduler drives asynchronous restoration of evicted realized-store components. When an enlistment is `DOC_EVICTED` or `EVICTED`, it replays the delta log via `DeltaSegmentReader` to rebuild documents, then replays documents through the index layer to rebuild indexes. It integrates with `RealizedStoreHandle.awaitReady` and the `rebuildPending` flag on pool entries so callers can choose blocking wait or partial query behaviour per `RebuildBlockingPolicy`.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.codec` | `KdbUuid`, `KdbHash` |
| `dev.kdb.error` | `KdbException` |
| `dev.kdb.storage` | `DeltaSegmentReader`, `DeltaSegmentRef`, `EvictableStorageAdapter`, `EnlistmentEvictionState`, `RebuildBlockingPolicy`, `StorageAdapter` |
| `dev.kdb.storage.manager.pool` | `RealizedStorePool`, `PoolEntry` |
| `dev.kdb.storage.manager.eviction` | `EvictionManager` — notified on rebuild completion to transition toward `FULL` |
| `dev.kdb.index` | `IndexStore` rebuild hooks (replay documents into indexes) |

---

## 3. Public Interface

```kotlin
package dev.kdb.storage.manager.rebuild

import dev.kdb.codec.*
import dev.kdb.storage.*
import dev.kdb.storage.manager.pool.RealizedStorePool
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.StateFlow

interface RebuildScheduler {

  /**
   * Schedule rebuild for missing components. Idempotent: if a job
   * is already RUNNING or QUEUED for this enlistment, returns existing job id.
   */
  suspend fun scheduleRebuild(
    enlistmentId: KdbUuid,
    targetState: EnlistmentEvictionState = EnlistmentEvictionState.FULL,
  ): RebuildJobId

  /** Suspend until enlistment reaches ready per [blockingPolicy]. */
  suspend fun awaitReady(
    enlistmentId: KdbUuid,
    blockingPolicy: RebuildBlockingPolicy,
  )

  fun job(enlistmentId: KdbUuid): RebuildJob?

  fun cancel(enlistmentId: KdbUuid): Boolean

  /** Diagnostic: count of active rebuild workers. */
  val activeJobCount: StateFlow<Int>
}

@JvmInline
value class RebuildJobId(val value: Long)

enum class RebuildPhase {
  QUEUED,
  REPLAYING_DELTA,      // documents from delta log
  REPLAYING_INDEX,      // indexes from documents
  COMPLETE,
  FAILED,
  CANCELLED,
}

data class RebuildJob(
  val id: RebuildJobId,
  val enlistmentId: KdbUuid,
  val namespaceId: String,
  val phase: RebuildPhase,
  val targetState: EnlistmentEvictionState,
  val startedAtMillis: Long,
  val completedAtMillis: Long?,
  val bytesReplayed: Long,
  val error: Throwable?,
)

interface RebuildWorker {
  suspend fun execute(job: RebuildJob, context: RebuildContext): RebuildJob
}

data class RebuildContext(
  val pool: RealizedStorePool,
  val deltaReader: DeltaSegmentReader,
  val adapter: EvictableStorageAdapter,
)

class DefaultRebuildScheduler(
  private val pool: RealizedStorePool,
  private val deltaReaderFactory: (namespaceId: String) -> DeltaSegmentReader,
  private val worker: RebuildWorker = DefaultRebuildWorker(),
  private val maxConcurrentJobs: Int = 4,
) : RebuildScheduler
```

---

## 4. Data Structures

### `RebuildJob` lifecycle

```
QUEUED → REPLAYING_DELTA → REPLAYING_INDEX → COMPLETE
   │            │                  │
   └────────────┴──────────────────┴→ FAILED / CANCELLED
```

### `RebuildPlan` (internal)
Derived from current `EnlistmentEvictionState`:

| Current state | Phases executed |
|---|---|
| `DOC_EVICTED` | `REPLAYING_DELTA` only → `FULL` |
| `EVICTED` | `REPLAYING_DELTA` then `REPLAYING_INDEX` → `FULL` |
| `FULL` | No-op; job completes immediately |

### `GpuRebuildPlan` (internal)
When engine `capabilities.supportsDirectDeltaIngest == true`, skip CPU document replay; pass sealed `DeltaSegmentRef` list to `ingestDeltaSegment` instead of `rebuildDocuments`.

### `AwaiterRegistry` (internal)
Maps `enlistmentId` → list of continuations resumed on `COMPLETE` or resumed with partial error on `FAILED` when `PARTIAL_OK`.

---

## 5. Contracts

### `scheduleRebuild`
**Preconditions:** Pool entry exists; not `RELEASED`.

**Postconditions:**
- Sets `PoolEntry.rebuildPending = true` before worker starts.
- At most one active rebuild per enlistment (dedupe by `enlistmentId`).
- On `COMPLETE`, clears `rebuildPending`, sets `evictionState` to `FULL` (pool + engine), notifies `EvictionManager.onDemandAccess` touch optional.

### `DefaultRebuildWorker.execute`
**Document phase:** Calls `adapter.rebuildDocuments(enlistmentId, deltaReader)`. Engine reads all segments for namespace via `listSegments()` / `readAll` or incremental `readRange` from enlistment base hash.

**Index phase:** Calls `adapter.rebuildIndex(enlistmentId, adapter)` after documents are materialised.

**Ordering:** Delta replay must complete before index replay. Index replay must not start if document phase failed.

### `awaitReady`
- `WAIT`: suspends until `phase == COMPLETE` or throws if `FAILED`.
- `PARTIAL_OK`: returns immediately if not ready; does not suspend. Caller polls `isReady`.

### GPU direct ingest
When `supportsDirectDeltaIngest`, worker invokes `StorageAdapter.ingestDeltaSegment` for each promoted segment ref (from tier signals 11e / promotion queue) instead of `rebuildDocuments`. `REPLAYING_INDEX` may be skipped if GPU engine maintains indexes internally.

### Cancellation
`cancel` sets phase `CANCELLED`, clears `rebuildPending`. Partial materialisation may remain; next `scheduleRebuild` restarts from engine-reported state.

### Concurrency
At most `maxConcurrentJobs` workers; additional jobs stay `QUEUED` FIFO per namespace fairness optional.

---

## 6. Error Cases

| Exception | When |
|---|---|
| `EnlistmentNotFoundException` | Schedule/await for unknown enlistment. |
| `RebuildFailedException` | `awaitReady(WAIT)` and job ends `FAILED`. |
| `RebuildCancelledException` | `awaitReady(WAIT)` and job cancelled. |
| `StorageAdapterException` | Delta read or engine rebuild fails (wrapped). |

```kotlin
class RebuildFailedException(
  val enlistmentId: KdbUuid,
  val jobId: RebuildJobId,
  cause: Throwable?,
) : KdbException("Rebuild failed for $enlistmentId", cause)

class RebuildCancelledException(val enlistmentId: KdbUuid) : KdbException("Rebuild cancelled")
```

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `docEvicted_rebuildsDocumentsOnly` | `DOC_EVICTED`, schedule. | Phases: DELTA only; state `FULL`. |
| 2 | `evicted_rebuildsBothPhases` | `EVICTED`, schedule. | DELTA then INDEX; state `FULL`. |
| 3 | `awaitReady_waitBlocks` | Slow mock worker. `awaitReady(WAIT)`. | Resumes after `COMPLETE`. |
| 4 | `partialOk_doesNotBlock` | `EVICTED`, `PARTIAL_OK`. | `awaitReady` returns immediately. |
| 5 | `dedupe_singleJob` | Two `scheduleRebuild` calls. | Same `RebuildJobId`; one worker run. |
| 6 | `failedJob_throwsOnWait` | Worker throws. `awaitReady(WAIT)`. | `RebuildFailedException`. |
| 7 | `gpuDirectIngest_skipsDocReplay` | GPU adapter mock. | `ingestDeltaSegment` called; no `rebuildDocuments`. |
| 8 | `cancel_midFlight` | Cancel during DELTA. | Phase `CANCELLED`; `rebuildPending` false. |
| 9 | `concurrency_cap_queues` | 5 schedules, maxConcurrent=2. | 2 `REPLAYING_*`, 3 `QUEUED`. |
| 10 | `complete_clearsRebuildPending` | Successful rebuild. | `PoolEntry.rebuildPending == false`, `isReady` true. |

---

## 8. Non-Goals

- Opening or sealing delta segments — Layer 4a.
- LRU eviction — Component 11b.
- Browser peer fetch of missing deltas — Component 11d.
- Compaction of delta segments — Layer 4a Component 10f.
- SQL query execution during partial rebuild — Query engine interprets `isReady` / policy.

---

## 9. Implementation Notes

### Coroutine scope
Use a supervisor scope on `StorageManager` lifecycle; jobs use `async` per enlistment with structured cancellation on `shutdown`.

### Progress reporting
Optional `StateFlow<RebuildJob>` per enlistment for UI/diagnostics; not required for v1.

### Delta replay bounds
Replay from enlistment's `currentCommitHash` anchor backward to materialisation base, not entire namespace history, when engine supports bounded rebuild.

### Browser engines
Delta log is not durable locally; rebuild uses in-memory delta buffer populated by enlistment manager sync — `DeltaSegmentReader` implementation reads enlistment-local buffer, not disk.

### KMP
`commonMain` only; blocking I/O runs on `Dispatchers.Default` (or injected dispatcher).

### Integration with `requestRealized` (11a)
Pool sets `rebuildPending` before dispatching worker; `PooledRealizedStoreHandle.isReady` is false until job `COMPLETE`. Concurrent `requestRealized` calls share one job (dedupe). If eviction (11b) occurs mid-rebuild, worker checks cancellation token and aborts with `CANCELLED` unless ref count went to zero (enlistment released).

### Partial query semantics
With `RebuildBlockingPolicy.PARTIAL_OK`, engines may serve index-only results when state is `DOC_EVICTED` (documents missing) or return empty/partial document fetches — engine-specific, but scheduler does not block. Document phase completion is required before `isReady` becomes true for full consistency.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `RebuildScheduler` + queue | 200 |
| `DefaultRebuildWorker` | 180 |
| `GpuRebuildPlan` branch | 60 |
| `AwaiterRegistry` | 80 |
| Exceptions | 40 |
| Tests | 380 |
| **Total** | **~940** |
