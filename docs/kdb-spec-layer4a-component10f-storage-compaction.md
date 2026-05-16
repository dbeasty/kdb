# KDB Component Spec — Layer 4a
## Component 10f: Storage Compaction
### `dev.kdb.storage.compaction`

**File:** `kdb-spec-layer4a-component10f-storage-compaction.md`
**Layer:** 4a — KDB Storage Engine
**Depends on:** Layer 3 Component 9; Layer 4a 10c, 10d, 10e, 10g; Layer 4b 11e (tier signals, optional)

---

## 1. Purpose

Storage Compaction maintains bounded disk use and read amplification for the LSM blob store and the append-only delta log. It plans and executes **SSTable level merges** (10c) and **delta segment seal/roll** when active segments exceed `StorageEngineConfig.pageMaxSizeBytes`. Tier hints (`HOT` / `WARM`) inform scheduling priority and compression choices; cold/ice archival remains Layer 6.

The Storage Manager (11e) may enqueue jobs; this module owns the algorithms and synchronous execution entry points `runSstableCompaction` and `runDeltaSegmentRoll`.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.storage` | `StorageEngineConfig`, `DeltaSegmentRef`, `DeltaSegmentWriter`, `CompressionCodec`, `StorageAdapterException` |
| `dev.kdb.storage.sstable` (10c) | `SsTableManifest`, `SsTableWriter`, `SsTableMerger`, `LsmBlobStore` |
| `dev.kdb.storage.delta` (10d) | `DeltaSegmentWriter`, `DeltaSegmentReader`, `DeltaSegmentWriterFactory` |
| `dev.kdb.storage.io` (10g) | `PlatformIoShim`, `SegmentNameBuilder` |
| `dev.kdb.storage.engine` (10e) | `StorageEngineHandle` (namespace context for planner) |
| `dev.kdb.codec` | `KdbUuid`, `KdbHash` |
| `dev.kdb.error` | `KdbException`, `KdbErrorCode` |

---

## 3. Public Interface

```kotlin
package dev.kdb.storage.compaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.storage.*
import dev.kdb.storage.delta.DeltaSegmentWriterFactory
import dev.kdb.storage.sstable.SsTableManifest
import dev.kdb.storage.sstable.SsTableMerger

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Tier hints                                                      ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Scheduling hint for compaction priority (aligns with Layer 4b tier signals).
 * HOT: prefer low latency, defer heavy merges. WARM: allow larger merges + zstd.
 */
enum class StorageTierHint {
    HOT,
    WARM,
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Job + result                                                    ║
// ╚══════════════════════════════════════════════════════════════════╝

enum class CompactionKind {
    SSTABLE_LEVEL_MERGE,
    DELTA_SEGMENT_ROLL,
}

/** Work unit produced by [CompactionPlanner]. */
data class CompactionJob(
    val jobId: KdbUuid,
    val namespaceId: String,
    val kind: CompactionKind,
    val tierHint: StorageTierHint,
    /** SSTable: source level. Delta: ignored. */
    val sourceLevel: Int = 0,
    /** SSTable: input file ids. Delta: active segment id to seal. */
    val inputSegmentIds: List<String> = emptyList(),
    val enqueuedAtMillis: Long,
)

/** Outcome of running one job. */
data class CompactionResult(
    val jobId: KdbUuid,
    val success: Boolean,
    val bytesRead: Long,
    val bytesWritten: Long,
    val outputSegmentIds: List<String>,
    val sealedDeltaRef: DeltaSegmentRef? = null,
    val errorMessage: String? = null,
)

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Planner                                                         ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Inspects LSM manifest + delta writer state and returns jobs safe to run now.
 * Does not execute I/O — call [runSstableCompaction] / [runDeltaSegmentRoll].
 */
interface CompactionPlanner {

  val namespaceId: String

  /**
   * Scan SSTable levels and delta segment size; return jobs ordered by priority.
   * HOT namespaces: at most one L0 merge per plan; WARM may batch deeper levels.
   */
  suspend fun plan(
    manifest: SsTableManifest,
    activeDeltaSizeBytes: Long,
    tierHint: StorageTierHint,
  ): List<CompactionJob>

  companion object {
    fun create(
      namespaceId: String,
      config: StorageEngineConfig,
    ): CompactionPlanner
  }
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Execution                                                       ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Merge SSTable files from [job.sourceLevel] to [job.sourceLevel + 1].
 * Deletes input segments via [PlatformIoShim] after successful merge.
 */
suspend fun runSstableCompaction(
  job: CompactionJob,
  merger: SsTableMerger,
  ioShim: PlatformIoShim,
  config: StorageEngineConfig,
): CompactionResult

/**
 * Seal the active delta segment when oversized, open a new writer segment,
 * and return [CompactionResult.sealedDeltaRef].
 */
suspend fun runDeltaSegmentRoll(
  job: CompactionJob,
  activeWriter: DeltaSegmentWriter,
  deltaFactory: DeltaSegmentWriterFactory,
  ioShim: PlatformIoShim,
  config: StorageEngineConfig,
): CompactionResult

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Scheduler hook (optional helper)                                ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Runs all jobs from [plan] sequentially; stops on first failure if [failFast].
 * Used by Storage Manager background worker (4b).
 */
suspend fun runCompactionBatch(
  jobs: List<CompactionJob>,
  planner: CompactionPlanner,
  merger: SsTableMerger,
  activeDeltaWriter: DeltaSegmentWriter?,
  deltaFactory: DeltaSegmentWriterFactory,
  ioShim: PlatformIoShim,
  config: StorageEngineConfig,
  failFast: Boolean = true,
): List<CompactionResult>

class CompactionFailedException(
  message: String,
  val job: CompactionJob,
  cause: Throwable? = null,
) : KdbException(message, cause) {
  override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

/** Default LSM level sizing (overridable via config extension in 10c). */
object CompactionDefaults {
  const val MAX_L0_FILES: Int = 4
  const val L1_MULTIPLIER: Int = 10
  val DELTA_ROLL_THRESHOLD_BYTES: Long = 16L * 1024 * 1024
}
```

---

## 4. Data Structures

### `StorageTierHint`
- **HOT:** Namespace or segment recently written; planner prefers delta roll over wide SSTable merges; SSTable output may use `CompressionCodec.NONE` for L0→L1.
- **WARM:** Stable data; planner may merge multiple L1+ files with `CompressionCodec.ZSTD` per `StorageEngineConfig.compressionCodec`.

### `CompactionJob`
Immutable work descriptor. `jobId` correlates logs and metrics. `inputSegmentIds` for SSTable jobs are file stems from `SegmentNameBuilder.sstable`.

### `CompactionResult`
`outputSegmentIds` lists new SSTable files created. `sealedDeltaRef` set only for `DELTA_SEGMENT_ROLL`. Failed runs set `success = false` and `errorMessage`; inputs remain on disk unless merge completed before failure.

### `CompactionKind`
Distinguishes LSM merge vs delta seal so batch runner dispatches correctly.

---

## 5. Contracts

### `CompactionPlanner.plan`
**Pre:** `manifest` reflects on-disk truth after last flush; `activeDeltaSizeBytes` from `DeltaSegmentWriter.currentSizeBytes`.
**Post:** Jobs list is deterministically ordered: (1) delta roll if size ≥ `min(pageMaxSizeBytes, DELTA_ROLL_THRESHOLD_BYTES)`, (2) L0 merges if file count ≥ `MAX_L0_FILES`, (3) deeper level merges when level size exceeds budget.
**HOT constraint:** At most one `SSTABLE_LEVEL_MERGE` job per plan; delta roll always allowed if threshold met.
**WARM:** May emit multiple SSTable jobs per level.

### `runSstableCompaction`
**Pre:** `job.kind == SSTABLE_LEVEL_MERGE`; all `inputSegmentIds` exist in manifest.
**Post:** New file(s) at `sourceLevel + 1`; manifest updated by caller (10e/10c); input segments `deleteSegment` on shim after merge fsync.
**Atomicity:** On failure, no input deleted; partial output files deleted (best-effort).
**Idempotency:** Re-running same job with same inputs after success is no-op (planner should not re-enqueue).

### `runDeltaSegmentRoll`
**Pre:** `job.kind == DELTA_SEGMENT_ROLL`; writer not sealed; size ≥ roll threshold.
**Post:** `activeWriter.seal()` returns `DeltaSegmentRef`; new writer opened via `deltaFactory.openSegment(namespaceId)`; `sealedDeltaRef` in result.
**Immutability:** Sealed segment never accepts appends (Layer 3 contract).

### `runCompactionBatch`
**Post:** One `CompactionResult` per job in order. If `failFast` and a job fails, remaining jobs are not started.

### Tier interaction (11e)
Tier signals may override `tierHint` per segment; planner treats signal as minimum priority (HOT wins over WARM for same namespace).

---

## 6. Error Cases

| Exception | When |
|---|---|
| `CompactionFailedException` | Merge I/O error, checksum mismatch, delta seal failure. |
| `StorageAdapterException` | Underlying `PlatformIoException` wrapped during segment delete/read. |
| `DeltaSegmentSealedException` | Roll job on already-sealed writer (programmer error). |
| `IllegalArgumentException` | Wrong `CompactionKind` for runner, empty `inputSegmentIds` for SSTable job. |

Failed compaction must not delete delta segments referenced by open enlistments below their `atCommit` (Storage Manager passes retention floor — v1: planner only rolls **active** writer, not historical sealed segments).

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `plan_deltaRoll_whenOversize` | activeDeltaSizeBytes = 17 MiB, max = 16 MiB. | One `DELTA_SEGMENT_ROLL` job. |
| 2 | `plan_l0Merge_fourFiles` | L0 count = 4, HOT hint. | One `SSTABLE_LEVEL_MERGE` at level 0. |
| 3 | `plan_hot_limitsMerges` | L0=8, L1=20, HOT. | ≤1 SSTable job + optional delta job. |
| 4 | `runSstableCompaction_mergesAndDeletes` | Two L0 files, 100 keys each. | One L1 file; inputs deleted; success true. |
| 5 | `runSstableCompaction_failureRetainsInputs` | Corrupt input checksum. | success false; input segments still listable. |
| 6 | `runDeltaSegmentRoll_seals` | Writer at threshold, roll job. | `isSealed` true; new writer `isSealed` false; ref in result. |
| 7 | `runDeltaSegmentRoll_idempotentGuard` | Roll twice same writer. | Second throws `DeltaSegmentSealedException`. |
| 8 | `batch_failFast_stops` | Job1 fail, Job2 pending, failFast=true. | Only one result; Job2 not run. |
| 9 | `warm_usesZstd` | WARM L1 merge. | Output manifest codec ZSTD. |
| 10 | `planner_empty_whenHealthy` | L0=1, delta small. | Empty job list. |

---

## 8. Non-Goals

- **DAG squash / peer CompactionIntent** — Layer 6 compaction engine.
- **Ice archival / cold tier** — tier manager Layer 6.
- **GPU segment recompaction** — GPU engine idle compaction is Layer 9.
- **Cross-namespace compaction** — one planner instance per namespace.
- **Automatic background scheduling** — 4b Storage Manager owns timers; this module executes jobs.
- **Browser delta durability** — roll still runs in-session but segments are not durable across reload.

---

## 9. Implementation Notes

### SSTable level merge (10c)
Use `SsTableMerger.merge(inputs, outputLevel, codec)` k-way merge by content hash key. Output single file per job in v1 (no multi-output split).

### Delta roll trigger
```text
roll when currentSizeBytes >= min(config.pageMaxSizeBytes, CompactionDefaults.DELTA_ROLL_THRESHOLD_BYTES)
```
Aligns with Layer 3 page target (8 MiB) and max (16 MiB).

### HOT vs WARM scheduling
| Hint | Delta roll | L0 merge | L1+ merge |
|---|---|---|---|
| HOT | Yes if oversized | Max 1 per plan | Deferred unless L0 > 2× MAX_L0_FILES |
| WARM | Yes | Yes | Yes when level byte size > budget |

### Segment GC
After merge, `ioShim.deleteSegment` for each `SegmentNameBuilder.sstable(namespace, level, id)`. WAL GC is separate (10a retention policy).

### Metrics
Log `jobId`, `bytesRead`, `bytesWritten`, duration_ms for operator dashboards (optional hook interface in 4b).

### Concurrency
Compaction holds a per-namespace **compaction lock** (shared with 10e flush). Reads proceed via SSTable immutability; block cache invalidated for deleted file ids.

### Testing
Use `InMemoryPlatformIoShim` + in-memory SSTable manifests; no JVM disk required for unit tests.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `CompactionJob` + `CompactionResult` + enums | 70 |
| `CompactionPlanner` implementation | 220 |
| `runSstableCompaction` | 180 |
| `runDeltaSegmentRoll` | 120 |
| `runCompactionBatch` | 60 |
| `CompactionFailedException` + defaults | 40 |
| Unit tests (planner + merge + roll) | 400 |
| **Total** | **~1,090** |

Production compaction logic (excluding tests): **~690 NBNC** (~350–450 per focused deliverable with tests in sibling module).
