# KDB Component Spec — Layer 4b
## Component 11e: Delta Log Tier Signals
### `dev.kdb.storage.manager.tier`

**File:** `kdb-spec-layer4b-component11e-delta-log-tier-signals.md`
**Layer:** 4b — Storage Manager
**Status:** Implementation-ready
**Depends on:** Layer 3 Component 9 (`DeltaSegmentRef`, `GpuPromotionPolicy`), Layer 4a (segment writer, compaction), Components 11a, 11c

---

## 1. Purpose

Delta Log Tier Signals classifies sealed delta segments into lifecycle tiers (`HOT`, `WARM`, `COLD`, `ICE`) and emits events that Layer 4a compaction and the future Layer 6 Storage Tier Manager consume. It does not move bytes between storage backends — it maintains segment metadata, access telemetry, and promotion/demotion signals so compaction prioritisation, GPU direct delta ingest (`supportsDirectDeltaIngest`), and archival policies have a single source of truth on the node.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.codec` | `KdbUuid`, `KdbHash`, `KdbTimestamp` |
| `dev.kdb.error` | `KdbException`, `StorageTierException` |
| `dev.kdb.storage` | `DeltaSegmentRef`, `CompressionCodec`, `GpuPromotionPolicy`, `GpuPromotionStrategy` |
| `dev.kdb.storage.manager.pool` | `StorageManager`, `PoolEntry` |
| `dev.kdb.storage.compaction` (Layer 4a Component 10f) | `CompactionScheduler` — registers as tier signal listener |
| `dev.kdb.storage.manager.rebuild` | `RebuildScheduler` — GPU ingest scheduling hook |

---

## 3. Public Interface

```kotlin
package dev.kdb.storage.manager.tier

import dev.kdb.codec.*
import dev.kdb.storage.*
import kotlinx.coroutines.flow.SharedFlow

interface DeltaLogTierRegistry {

  /** Register a newly sealed segment. Initial tier: HOT. */
  suspend fun onSegmentSealed(ref: DeltaSegmentRef, namespaceId: String)

  /** Record read access (query, rebuild, peer sync). May affect WARM retention. */
  fun onSegmentAccess(ref: DeltaSegmentRef, accessKind: SegmentAccessKind)

  /** Current tier for a segment. */
  fun tierOf(segmentId: KdbUuid): SegmentTier?

  /** All segments for namespace in tier order (HOT first). */
  fun segmentsIn(namespaceId: String): List<SegmentTierEntry>

  /** Force transition (compaction GC, admin, future Tier Manager). */
  suspend fun transition(segmentId: KdbUuid, to: SegmentTier, reason: TierTransitionReason): Boolean

  /** Stream of tier changes for subscribers (compaction, metrics, GPU queue). */
  val tierEvents: SharedFlow<TierSignalEvent>
}

enum class SegmentTier {
  /** Uncompressed or memory-mapped; active write tail + recent sealed segments. */
  HOT,
  /** Typed binary + zstd on local disk; default sealed segment home. */
  WARM,
  /** Object-store backed or secondary volume; still online. */
  COLD,
  /** Archive bundle; offline restore required. */
  ICE,
}

enum class SegmentAccessKind {
  DELTA_REPLAY, QUERY_SCAN, PEER_SYNC, GPU_PROMOTE, COMPACTION_READ,
}

enum class TierTransitionReason {
  AGE_POLICY, SIZE_POLICY, ACCESS_IDLE, COMPACTION_DEMOTE,
  ADMIN_COMMAND, ARCHIVE_JOB, GPU_PROMOTION,
}

data class SegmentTierEntry(
  val ref: DeltaSegmentRef,
  val tier: SegmentTier,
  val sealedAt: KdbTimestamp,
  val lastAccessAt: KdbTimestamp?,
  val accessCount: Long,
  val namespaceId: String,
)

sealed class TierSignalEvent {
  data class Transitioned(
    val entry: SegmentTierEntry,
    val from: SegmentTier,
    val to: SegmentTier,
    val reason: TierTransitionReason,
  ) : TierSignalEvent()

  /** Emitted when GPU promotion policy matches a WARM/COLD segment. */
  data class GpuPromotionCandidate(
    val entry: SegmentTierEntry,
    val policy: GpuPromotionPolicy,
  ) : TierSignalEvent()

  /** Compaction should prefer merging these HOT/WARM segments. */
  data class CompactionCandidate(
    val namespaceId: String,
    val segmentIds: List<KdbUuid>,
    val tier: SegmentTier,
  ) : TierSignalEvent()
}

interface TierPolicyEvaluator {
  fun evaluateHotToWarm(entry: SegmentTierEntry, policy: NamespaceTierPolicy): TierTransitionReason?
  fun evaluateWarmToCold(entry: SegmentTierEntry, policy: NamespaceTierPolicy): TierTransitionReason?
  fun evaluateGpuPromotion(entry: SegmentTierEntry, policy: GpuPromotionPolicy): Boolean
}

data class NamespaceTierPolicy(
  val hotMaxAgeMillis: Long = 24 * 60 * 60 * 1000L,
  val warmIdleMillis: Long = 7 * 24 * 60 * 60 * 1000L,
  val coldAfterMillis: Long = 30L * 24 * 60 * 60 * 1000L,
  val minWarmSegmentBytes: Long = 1024 * 1024,
)

interface TierSignalHooks {
  /** Layer 4a compaction registers here. */
  fun onCompactionCandidate(listener: (TierSignalEvent.CompactionCandidate) -> Unit)
  /** Future Layer 7 Storage Tier Manager (Component 20) registers here. */
  fun onTierTransition(listener: (TierSignalEvent.Transitioned) -> Unit)
  /** Rebuild scheduler / GPU engine registers here. */
  fun onGpuPromotionCandidate(listener: (TierSignalEvent.GpuPromotionCandidate) -> Unit)
}

class DefaultDeltaLogTierRegistry(
  private val evaluator: TierPolicyEvaluator = DefaultTierPolicyEvaluator(),
  private val clock: () -> KdbTimestamp = { KdbTimestamp.now() },
) : DeltaLogTierRegistry, TierSignalHooks
```

---

## 4. Data Structures

### Tier semantics (normative, aligned with master spec §12.1)

| Tier | Physical form | Managed by |
|---|---|---|
| `HOT` | Typed binary, fast random access; may include unsealed tail | Layer 4a engine |
| `WARM` | Typed binary + zstd on local segment files | Layer 4a + signals |
| `COLD` | zstd on object store / secondary path | Future Tier Manager (Layer 7, Component 20) |
| `ICE` | Self-contained archive bundle | Future Tier Manager (Layer 7, Component 20) |

Component 11e **implements** HOT/WARM transitions and **signals** COLD/ICE; byte movement for COLD/ICE is out of scope.

### `SegmentTierState` (internal)
`ref`, `tier`, `sealedAt`, `lastAccessAt`, `accessCount`, `writeRatePerMinute` (rolling, for GPU `maxChangeRate`).

### `GpuPromotionQueue` (internal)
Segments matching `GpuPromotionPolicy` (`minSegmentAge`, `minSegmentSize`, `maxChangeRate`, `strategy`) emit `GpuPromotionCandidate`. `PROMOTE_ON_QUERY` additionally requires `SegmentAccessKind.GPU_PROMOTE` or vector query hook. `RebuildScheduler` / GPU engine calls `ingestDeltaSegment` when `supportsDirectDeltaIngest`.

---

## 5. Contracts

### `onSegmentSealed`
**Postconditions:** Entry inserted as `HOT` with `sealedAt = now`. `tierEvents` emits no transition (initial insert). Compaction may receive `CompactionCandidate` if adjacent HOT segments exceed policy merge threshold.

### `onSegmentAccess`
Updates `lastAccessAt` and `accessCount`. Does not promote tier by itself except: `GPU_PROMOTE` access kind may emit `GpuPromotionCandidate` when strategy is `PROMOTE_ON_QUERY`.

### Automatic transitions (background tick, default 60s)
- `HOT → WARM`: segment age > `hotMaxAgeMillis` OR sealed size ≥ policy threshold and idle.
- `WARM → COLD`: idle > `warmIdleMillis` (signal only until Tier Manager exists; entry marked `COLD` logically for compaction priority).
- `COLD → ICE`: not automated in 11e v1 — admin or future archive job calls `transition(..., ICE, ARCHIVE_JOB)`.

### `transition`
**Preconditions:** Valid monotonic downgrade HOT→WARM→COLD→ICE or admin override upgrade with `ADMIN_COMMAND`.

**Postconditions:** Emits `TierSignalEvent.Transitioned`. Subscribers run asynchronously; failure in subscriber does not roll back tier state.

### Compaction hooks
When ≥2 `HOT` segments in same namespace total size > `pageTargetSizeBytes`, emit `CompactionCandidate` with segment id list. Compaction engine (10f) subscribes via `TierSignalHooks.onCompactionCandidate`.

### Future Storage Tier Manager hook
`onTierTransition` receives all transitions; Layer 7 Component 20 will perform physical COLD/ICE moves and call `transition` with `ARCHIVE_JOB` when complete.

### Browser / InMemory engines
Segments may never leave `HOT` (no durable segment files). Registry returns empty or in-memory-only entries; hooks no-op.

---

## 6. Error Cases

| Exception | When |
|---|---|
| `SegmentNotRegisteredException` | `tierOf` / `transition` for unknown `segmentId`. |
| `IllegalTierTransitionException` | Disallowed jump (e.g. `ICE → HOT` without admin). |
| `StorageTierException` | Subscriber I/O failure (propagated from compaction/tier worker, not from registry itself). |

```kotlin
class SegmentNotRegisteredException(val segmentId: KdbUuid) : KdbException("Segment not registered: $segmentId")
class IllegalTierTransitionException(
  val segmentId: KdbUuid,
  val from: SegmentTier,
  val to: SegmentTier,
) : KdbException("Illegal tier transition $from → $to for $segmentId")
```

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `sealedSegment_startsHot` | `onSegmentSealed`. | `tierOf == HOT`. |
| 2 | `agePolicy_hotToWarm` | Seal, advance clock past `hotMaxAge`. | Transition to WARM; event emitted. |
| 3 | `idlePolicy_warmToCold` | WARM segment, no access past `warmIdle`. | `COLD` + `Transitioned` reason `ACCESS_IDLE`. |
| 4 | `gpuPromote_eagerOnSeal` | `PROMOTE_EAGERLY`, large segment. | `GpuPromotionCandidate` emitted. |
| 5 | `gpuPromote_queryStrategy` | `PROMOTE_ON_QUERY`, access GPU_PROMOTE. | Candidate emitted once. |
| 6 | `maxChangeRate_blocksGpu` | High write rate segment. | No GPU candidate. |
| 7 | `compactionCandidate_adjacentHot` | Two HOT segments same namespace. | `CompactionCandidate` with both ids. |
| 8 | `illegalTransition_throws` | `ICE → HOT` without admin. | `IllegalTierTransitionException`. |
| 9 | `browserEngine_noWarmDisk` | Browser seal event. | Stays HOT; no WARM file move. |
| 10 | `subscriberFailure_retainsState` | Listener throws on transition. | Tier state still WARM; error logged. |

---

## 8. Non-Goals

- Physical upload/download to object store or ice bundles — Layer 6 Storage Tier Manager.
- Compaction merge algorithm — Layer 4a Component 10f.
- Namespace policy DSL parsing — Layer 6 Namespace Policy Engine.
- Realized store eviction — Components 11a–11b.
- Dynamic GPU policy feedback loop (hit-rate tuning) — open question in v3; static policy only in v1.

---

## 9. Implementation Notes

### Registration point
`DeltaSegmentWriter.seal()` in Layer 4a calls `DeltaLogTierRegistry.onSegmentSealed` via `StorageManager` accessor to avoid circular module deps (interface in 11e, call from 10d through manager facade).

### Thread safety
`ConcurrentHashMap` / `Mutex` per namespace for segment maps; events on `SharedFlow` with replay=0.

### GPU + tier
Only `WARM`+ segments are GPU promotion candidates (HOT may still be mutating). `supportsDirectDeltaIngest` engines receive `DeltaSegmentRef` from candidate event.

### Metrics
Optional counters: transitions per tier per namespace; not required for v1.

### KMP
`commonMain`; clock injectable for tests.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `DeltaLogTierRegistry` + entry map | 200 |
| `DefaultTierPolicyEvaluator` | 120 |
| `TierSignalHooks` + SharedFlow | 80 |
| GPU promotion queue | 100 |
| Exceptions | 40 |
| Tests | 350 |
| **Total** | **~890** |
