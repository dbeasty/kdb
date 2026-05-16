# KDB Component Spec — Layer 7
## Component 20: Storage Tier Manager
### `dev.kdb.tier`

**File:** `kdb-spec-layer7-component20-storage-tier-manager.md`  
**Layer:** 7 — Network Foundation  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-storage-tier`  
**Depends on:** Layers 0–2, Layer 3 (`StorageAdapter`), Layer 4a (`StorageEngine`, delta segments), Layer 4b (`DeltaLogTierRegistry`, `TierSignalHooks` — 11e), Layer 6 (`NamespacePolicy` / `TierPolicy`, `CompactionEngine` hooks — read-only)

-----

## 1. Purpose

Executes **physical storage tier transitions** declared in namespace policy (master §10, §12.1): moving sealed delta segments and snapshot blobs between HOT/WARM/COLD backends, building **ice archive bundles** (master §12.3), calling `CommitDag.stubCommit`, and restoring archives into isolated namespaces.

Component 11e (`DeltaLogTierRegistry`) classifies segments and emits `TierSignalEvent.Transitioned`; this module **performs byte movement** and updates archive metadata. Component 19 (DAG squash) and 10f (LSM merge) are not invoked here except via optional post-archive GC hooks.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `encodeToBytes`, `decodeFromBytes`, `KdbTimestamp` |
| `dev.kdb.error` | `IceStorageException`, `StorageTierException`, `ArchiveRestoreException` |
| `dev.kdb.document` | `KdbCommit`, `DocumentTree`, `CommitStub`, wire types for ice bundle |
| `dev.kdb.schema` | `KdbSchema`, schema snapshot in bundle |
| `dev.kdb.dag` | `CommitDag`, `stubCommit`, `getCommit`, `walk` |
| `dev.kdb.storage` | `StorageAdapter`, blob read/write, segment paths |
| `dev.kdb.storage.manager.tier` (11e) | `DeltaLogTierRegistry`, `TierSignalHooks`, `SegmentTier`, `TierTransitionReason` |
| `dev.kdb.policy` (18) | `NamespacePolicy`, `TierPolicy`, `TierBand`, `StorageKind` |
| `dev.kdb.index` | `IndexManager` — index snapshot export for bundle |
| `dev.kdb.compression` | zstd wrap/unwrap for WARM/COLD/ICE payloads |

-----

## 3. Public Interface

```kotlin
package dev.kdb.tier

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.policy.NamespacePolicy
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.manager.tier.DeltaLogTierRegistry
import kotlinx.coroutines.flow.Flow

interface StorageTierManager {
    /** Start background worker: subscribe to tier signals, schedule moves. */
    fun start()
    fun stop()

    /** Run one evaluation pass for [namespaceId] (admin / test). */
    suspend fun runCycle(namespaceId: String): TierCycleResult

    /** Archive tagged commit to ice; stub original in DAG. */
    suspend fun archiveCommit(request: ArchiveRequest): ArchiveResult

    /** Restore ice bundle into new namespace (never overwrites live). */
    suspend fun restoreArchive(request: RestoreRequest): RestoreResult

    /** Force segment tier move (admin). */
    suspend fun moveSegment(request: SegmentMoveRequest): SegmentMoveResult
}

fun storageTierManager(
    dag: CommitDag,
    storage: StorageAdapter,
    tierRegistry: DeltaLogTierRegistry,
    tierHooks: TierSignalHooks,
    policyProvider: suspend (String) -> NamespacePolicy,
    backends: TierBackendRegistry,
    bundleWriter: IceBundleWriter = DefaultIceBundleWriter(storage),
): StorageTierManager

data class TierCycleResult(
    val segmentsMoved: Int,
    val archivesStarted: Int,
    val errors: List<TierJobError>,
)

data class ArchiveRequest(
    val namespaceId: String,
    val commitHash: KdbHash,
    val tag: String? = null,                    // must exist if policy requires tag
    val targetBackendId: String = "default-ice",
)

data class ArchiveResult(
    val bundleLocation: String,
    val stub: dev.kdb.document.CommitStub,
    val bundleHash: KdbHash,
)

data class RestoreRequest(
    val archiveLocation: String,
    val intoNamespaceId: String,
    val verifyBundle: Boolean = true,
)

data class RestoreResult(
    val namespaceId: String,
    val headCommit: KdbHash,
    val documentsImported: Int,
)

data class SegmentMoveRequest(
    val namespaceId: String,
    val segmentId: dev.kdb.codec.KdbUuid,
    val toTier: dev.kdb.storage.manager.tier.SegmentTier,
    val reason: dev.kdb.storage.manager.tier.TierTransitionReason,
)

data class SegmentMoveResult(
    val bytesMoved: Long,
    val sourcePath: String?,
    val destPath: String?,
)

/** Pluggable cold/ice/object-store backends. */
interface TierBackendRegistry {
    fun get(backendId: String): TierBackend
    fun register(backendId: String, backend: TierBackend)
}

interface TierBackend {
    val id: String
    val storageKind: dev.kdb.policy.StorageKind
    suspend fun put(key: String, bytes: ByteArray): String      // returns location URI
    suspend fun get(location: String): ByteArray
    suspend fun delete(location: String): Boolean
    suspend fun exists(location: String): Boolean
}

/** Builds master §12.3 ice bundle. */
interface IceBundleWriter {
    suspend fun writeBundle(
        dag: CommitDag,
        commit: KdbHash,
        namespaceId: String,
        indexSnapshots: ByteArray?,
    ): IceBundleArtifact
}

data class IceBundleArtifact(
    val location: String,
    val contentHash: KdbHash,
    val sizeBytes: Long,
)

/** In-memory / local-fs backends for tests and JVM embedded mode. */
fun inMemoryTierBackendRegistry(): TierBackendRegistry
fun localFsTierBackendRegistry(rootDir: String): TierBackendRegistry
```

-----

## 4. Data Structures

### `IceArchiveBundle` (logical, master §12.3)
Layer 0 record (wire schema `IceArchiveBundleWireType` in this module):

```
IceArchiveBundle {
  formatVersion:      int32          // 1
  commitMetadata:     KdbCommit      // at archived point
  schemaSnapshot:     KdbSchema?     // null if schema NONE
  documentTree:       DocumentTree
  indexSnapshots:     bytes?         // optional serialized index state
  blobManifest:       [ { hash: fixed32, size: int64, offset: int64 }, ... ]
  blobs:              bytes          // concatenated blob payload per manifest
}
```

Outer file: typed-binary body + zstd frame + footer `bundleHash` (SHA-256 of uncompressed body).

### `TierJob` (internal queue)
`namespaceId`, `segmentId` or `commitHash`, `fromTier`, `toTier`, `backendId`, `enqueuedAt`, `attempts`.

### `TierJobError`
`job`, `exception`, `retryable: Boolean`.

### COLD move semantics
Read segment bytes from WARM local path via `StorageAdapter`; `TierBackend.put` under key `namespace/segmentId`; update segment metadata pointer; call `tierRegistry.transition(..., COLD, SIZE_POLICY)`.

### ICE archival semantics
1. Resolve commit + document tree + schema at `commitHash`.
2. Export index snapshots via `IndexManager.exportAtCommit(commit)` (v1 may be empty bytes if export not implemented — bundle still valid).
3. `IceBundleWriter.writeBundle` → location URI.
4. `dag.stubCommit(commitHash, location)`.
5. Emit logical `IceArchiveNotice` payload (encoded by Component 21 when wire connected).

### Policy mapping (`TierPolicy` → evaluator)
| Policy band | `StorageKind` | Backend selection |
|---|---|---|
| `hot` | `LOCAL` | no move (11e HOT) |
| `warm` | `LOCAL_FS` | default local segment home |
| `cold` | `OBJECT_STORE` | `backends.get("cold-{ns}")` or default |
| `ice` | `ARCHIVE` | `backends.get(request.targetBackendId)` |

Age thresholds from `TierBand.maxAgeMillis` drive automatic WARM→COLD; ICE only on explicit `archiveCommit` or scheduled tag policy (v1: explicit admin/CLI only).

### `IceArchiveNotice` (wire payload preview)
```
IceArchiveNoticePayload {
  namespace:       string
  originalHash:    fixed32
  archiveLocation: string
  bundleHash:      fixed32
}
```
Message type `0x09` — encoded in `:kdb-wire`; tier manager produces payload object only.

-----

## 5. Contracts

### `StorageTierManager.start`
- **Postconditions:** Subscribes to `TierSignalHooks.onTierTransition` for WARM→COLD work items. Does not block caller.

### `runCycle`
- **Preconditions:** Namespace policy loaded.
- **Postconditions:** Every eligible WARM segment past `TierPolicy.cold.maxAgeMillis` with idle access is copied to cold backend or job queued. Returns counts; partial failures listed in `errors` without aborting other jobs.

### `archiveCommit`
- **Preconditions:** Commit exists and is not already stubbed. Tag exists if `tag` required by CLI/policy.
- **Postconditions:** Bundle written; `dag.stubCommit` replaces commit; `getCommit(hash)` returns null; `getStub(hash)` returns stub with `archiveLocation`. Tags on commit remain resolvable via stub redirect (Layer 2).
- **On failure:** No stub; bundle file deleted if write succeeded but stub failed (best-effort).

### `restoreArchive`
- **Preconditions:** `intoNamespaceId` must not exist on node OR caller passes `allowReplace=false` (default) and empty namespace.
- **Postconditions:** New namespace DAG with single imported head; documents and indexes (if snapshots present) queryable. Does not modify source namespace.
- **On corrupt bundle:** `ArchiveRestoreException`.

### `moveSegment`
- **Postconditions:** Segment bytes at destination; registry tier updated; idempotent if already at target tier.

### Interaction with 11e
Tier manager **never** transitions HOT→WARM (11e owns). It **may** call `tierRegistry.transition` after successful physical move to COLD/ICE.

### Interaction with hybrid query
Resolved stubbed commit → `IceStorageException` (Component 17) with `archiveLocation` from stub.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `StorageTierException` | Backend put/get failed, segment missing on disk |
| `IceStorageException` | Query path hits stub without restore (re-export from tier manager N/A) |
| `ArchiveRestoreException` | Bundle decode/verify/hash mismatch |
| `VersionNotFoundException` | `archiveCommit` hash not in DAG |
| `TierJobSkippedException` | Browser/in-memory engine with no durable segments (dev mode) |

```kotlin
class TierJobSkippedException(
    val namespaceId: String,
    val reason: String,
) : KdbException("tier job skipped: $reason")

class BundleIntegrityException(
    val expected: KdbHash,
    val actual: KdbHash,
) : KdbException("ice bundle hash mismatch: expected $expected, got $actual")
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `warmToCold_moveBytes` | WARM segment past cold age | Bytes on cold backend; registry COLD |
| 2 | `archiveCommit_stubsDag` | Archive HEAD commit | `getStub` populated; `getCommit` null |
| 3 | `archive_bundleRoundtrip` | Write bundle, read back | `BundleIntegrityException` absent; hash matches |
| 4 | `restore_isolatedNamespace` | Restore into `ns-recovered` | New DAG; live `ns` unchanged |
| 5 | `restore_corruptBundle` | Truncate bundle file | `ArchiveRestoreException` |
| 6 | `tagSurvivesStub` | Tag on archived commit | `getTag` resolves via stub |
| 7 | `queryStub_throwsIce` | Hybrid query at stubbed hash | `IceStorageException` with location |
| 8 | `browserEngine_skipsCold` | In-memory storage + runCycle | `segmentsMoved == 0` |
| 9 | `backendFailure_partialCycle` | Mock backend throws on 2nd segment | First moved; error in `errors` |
| 10 | `idempotentColdMove` | Move same segment twice | Second call no-op, `bytesMoved == 0` |
| 11 | `archive_missingCommit` | Unknown hash | `VersionNotFoundException` |
| 12 | `signalHook_enqueuesJob` | 11e `Transitioned` WARM→COLD | Job completes within test timeout |

-----

## 8. Non-Goals

- **Wire transport** (TCP/WebSocket) — Layer 9; this module emits notice payloads only.
- **DAG squash / `CompactionEngine`** — Layer 6 Component 19.
- **SSTable / delta segment merge** — Layer 4a Component 10f.
- **Automatic ICE by age without explicit archive job** — v2; v1 ICE is `archiveCommit` / CLI `kdb archive`.
- **Cross-namespace tier sharing** — one policy + backend map per namespace.
- **Cloud provider SDKs in commonMain** — backends are injected; S3/Azure adapters live in `jvmMain` optional modules later.
- **CompactionIntent / CompactionNotice wire** — Layer 7 Component 21 encodes; coordinator stays in `:kdb-compaction`.

-----

## 9. Implementation Notes

### Worker model
Single coroutine `SupervisorJob` + bounded channel of `TierJob`. `runCycle` drains queue synchronously for tests.

### Bundle layout on disk
`{backendRoot}/{namespaceId}/{commit.shortHex}.kdbice` — zstd compressed.

### Index export v1
If `IndexManager.exportAtCommit` unavailable, write empty `indexSnapshots` and document in bundle metadata; restore triggers index rebuild (slower but correct).

### Module layout
```
dev.kdb.tier
  StorageTierManager.kt
  TierCycleWorker.kt
  IceBundleWriter.kt
  TierBackendRegistry.kt
  IceArchiveBundleWire.kt
```

### KMP
`commonMain` manager + in-memory backends; `jvmMain` optional `LocalFsTierBackend`, `S3TierBackend` stub.

### CLI hooks (Layer 10)
`kdb archive` / `kdb restore` call `archiveCommit` / `restoreArchive` (master §11).

### Gradle
`:kdb-storage-tier` depends on `:kdb-storage-manager`, `:kdb-namespace-policy`, `:kdb-dag`, `:kdb-compression`.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `StorageTierManager` + worker | 700 |
| `IceBundleWriter` + wire schema | 550 |
| Tier backends (in-mem + local FS) | 400 |
| COLD segment mover | 450 |
| Restore path | 500 |
| Exceptions + tests | 900 |
| **Total** | **~3,500** |
