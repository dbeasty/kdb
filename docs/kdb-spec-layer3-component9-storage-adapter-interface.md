# KDB Component Spec — Layer 3
## Component 9: Storage Adapter Interface
### `dev.kdb.storage`

**File:** `kdb-spec-layer3-component9-storage-adapter-interface.md`
**Layer:** 3 — Write Path
**Depends on:** Layer 0 (BSON Codec, Error Model), Layer 1 (Document + Commit Model), Layer 2 (Schema Engine, Commit DAG)

---

## 1. Purpose

The Storage Adapter Interface is the single boundary between KDB's engine logic and all physical storage implementations. It defines the contracts that the KDB Storage Engine (Layer 4a) and the Storage Manager (Layer 4b) must satisfy so that the Transaction Engine (Component 7), Index Layer (Component 8), and all higher layers can operate without any knowledge of where or how data is physically stored. It also incorporates the design decisions from `kdb-storage-engine-design-decisions-v3.md`: the `StorageCapabilitySet` (extended with GPU fields), the `DeltaRecord` authorship envelope, the sub-enlistment eviction state machine, and the GPU direct-delta-ingest path.

This component is **interface-only**: no implementation logic lives here. Every interface defined in this spec is implemented in Layer 4a or Layer 4b.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.codec` | `KdbUuid`, `KdbHash`, `KdbTimestamp`, `BsonDocument`, `BsonValue` |
| `dev.kdb.error` | `KdbException`, `KdbErrorCode`, `KdbResult` |
| `dev.kdb.document` | `KdbDocument`, `KdbCommit`, `DocumentTree` |

---

## 3. Public Interface

```kotlin
package dev.kdb.storage

import dev.kdb.codec.*
import dev.kdb.document.*
import dev.kdb.error.*

// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECTION A — CAPABILITY DECLARATION                              ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Declares what a storage engine implementation can and cannot do.
 * Queried by the Storage Manager at enlistment creation time to
 * decide which engine to use and how to interact with it.
 */
data class StorageCapabilitySet(
    /** Engine persists delta log durably across process restart. Server: true. Browser/InMemory/GPU: false. */
    val persistsDeltaLog: Boolean,
    /** Engine persists realized store across reload (partial ok). Server: true. Browser: partial. Others: false. */
    val persistsAcrossReload: Boolean,
    /** Engine can serve GPU-accelerated bulk reads (vector ANN, columnar scan). GPU: true. Others: false. */
    val supportsGpuBulkRead: Boolean,
    /**
     * Engine can materialise its realized store directly from a [DeltaSegmentRef],
     * bypassing the CPU realized store. GPU: true. Others: false.
     * When true, the Storage Manager calls [StorageAdapter.ingestDeltaSegment]
     * instead of providing a pre-materialised document set.
     */
    val supportsDirectDeltaIngest: Boolean,
    /** Maximum concurrent enlistments. null = unlimited. */
    val maxEnlistments: Int?,
    /** Default index retention policy if namespace does not declare one. */
    val indexRetentionDefault: IndexRetention,
)

// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECTION B — DELTA STORE (append-only write log)                 ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Authorship envelope — present in every delta record.
 * The engine stores this but does NOT validate it. Validation is
 * the responsibility of an Auth Interceptor above the Transaction Engine.
 */
data class DeltaAuthorshipEnvelope(
    /** Authenticated principal identifier (user ID, node ID, etc.). */
    val principal: String,
    /** Timestamp the delta was produced. */
    val timestamp: KdbTimestamp,
    /**
     * Opaque rights token. Validated by the caller before submitting the delta;
     * stored verbatim for audit / blame queries. May be empty.
     */
    val rightsToken: String,
    /** Opaque client context (session ID, device ID, etc.). May be empty. */
    val clientContext: String,
)

/**
 * One entry in the delta log. Corresponds to one committed [KdbCommit].
 * BSON-native; stored in large append-only pages (8 MB–16 MB per segment).
 */
data class DeltaRecord(
    val commitHash: KdbHash,
    val namespaceId: String,
    val authorship: DeltaAuthorshipEnvelope,
    val commitBson: BsonDocument,       // serialised KdbCommit
    val documentPatches: List<DocumentPatch>,
)

/**
 * The before/after BSON for one document within a delta record.
 * Either [before] or [after] may be null (insert / delete respectively).
 */
data class DocumentPatch(
    val docId: KdbUuid,
    val before: BsonDocument?,
    val after: BsonDocument?,
    val contentHashAfter: KdbHash?,
)

/**
 * An opaque reference to a sealed, compressed delta segment on disk.
 * Passed to [StorageAdapter.ingestDeltaSegment] for engines that support
 * direct delta ingest (GPU engine).
 */
data class DeltaSegmentRef(
    val segmentId: KdbUuid,
    val namespaceId: String,
    val firstCommitHash: KdbHash,
    val lastCommitHash: KdbHash,
    val sizeBytes: Long,
    val compressionCodec: CompressionCodec,
)

enum class CompressionCodec { NONE, ZSTD }

/**
 * Append-only writer for the delta log. One writer per namespace segment.
 * Implementations: ServerDeltaSegmentWriter (JVM/Native), BrowserDeltaSegmentWriter (JS).
 */
interface DeltaSegmentWriter {

    val namespaceId: String
    val segmentId: KdbUuid

    /** Append one delta record. Returns the byte offset at which it was written. */
    suspend fun append(record: DeltaRecord): Long

    /** Flush buffered writes to the underlying storage. */
    suspend fun flush()

    /**
     * Seal the segment — no further appends accepted.
     * Returns the [DeltaSegmentRef] describing the sealed segment.
     */
    suspend fun seal(): DeltaSegmentRef

    /** Current uncompressed size in bytes. */
    val currentSizeBytes: Long

    /** True if this segment has been sealed. */
    val isSealed: Boolean
}

/**
 * Read interface for the delta log. Used by rebuild, peer sync, and GPU ingest.
 */
interface DeltaSegmentReader {

    val namespaceId: String

    /** Read all records from a segment in append order. */
    suspend fun readAll(segment: DeltaSegmentRef): List<DeltaRecord>

    /** Read records from [sinceCommit] to [untilCommit] (both inclusive). */
    suspend fun readRange(
        segment: DeltaSegmentRef,
        sinceCommit: KdbHash,
        untilCommit: KdbHash,
    ): List<DeltaRecord>

    /** List all sealed segments for this namespace, oldest first. */
    suspend fun listSegments(): List<DeltaSegmentRef>
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECTION C — STORAGE ADAPTER (document + tree CRUD)              ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * The core storage interface consumed by the Transaction Engine (Component 7)
 * and Index Layer (Component 8). Provides document and tree read/write
 * at a named commit. Implemented by each storage engine variant.
 */
interface StorageAdapter {

    val capabilities: StorageCapabilitySet

    // ── Document read ──────────────────────────────────────────────

    /** Read a document by ID at the given commit's document tree. */
    suspend fun getDocument(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument?

    /** Read a document or throw [DocumentNotFoundException]. */
    suspend fun getDocumentOrThrow(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument

    /**
     * Bulk read. Returns results in input order; null for missing documents.
     * Must be implementable as a single batch request to support GPU bulk-read path.
     */
    suspend fun getDocuments(
        namespaceId: String,
        docIds: List<KdbUuid>,
        atCommit: KdbHash,
    ): List<KdbDocument?>

    /** Iterate all documents in the tree at the given commit. */
    suspend fun scanDocuments(
        namespaceId: String,
        atCommit: KdbHash,
        batchSize: Int = 256,
        onBatch: suspend (List<KdbDocument>) -> Unit,
    )

    // ── Document write ─────────────────────────────────────────────

    /**
     * Write a document. Associates the content hash with docId in the
     * pending write buffer. Not durable until [flush] or [commitTree] is called.
     */
    suspend fun putDocument(
        namespaceId: String,
        document: KdbDocument,
    )

    /** Remove a document from the pending write buffer and mark for deletion. */
    suspend fun deleteDocument(
        namespaceId: String,
        docId: KdbUuid,
    )

    /**
     * Materialise all pending puts/deletes into a new [DocumentTree] anchored
     * at [parentTreeHash]. Returns the new tree. Does not modify the DAG.
     */
    suspend fun commitTree(
        namespaceId: String,
        parentTreeHash: KdbHash,
    ): DocumentTree

    /** Flush any write buffers to durable storage. */
    suspend fun flush(namespaceId: String)

    // ── Content-addressed blob store ───────────────────────────────

    /** Read raw document bytes by content hash (for peer sync / ice restore). */
    suspend fun readBlob(contentHash: KdbHash): ByteArray?

    /** Write raw document bytes. Returns the content hash. */
    suspend fun writeBlob(bytes: ByteArray): KdbHash

    // ── GPU direct delta ingest ────────────────────────────────────

    /**
     * Called by the Storage Manager when [capabilities.supportsDirectDeltaIngest] is true.
     * The engine materialises its own realized store from the segment directly,
     * bypassing the CPU realized store.
     * Only implemented by [GpuStorageEngine]; all others throw [UnsupportedOperationException].
     */
    suspend fun ingestDeltaSegment(segment: DeltaSegmentRef)
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECTION D — EVICTION + REBUILD (Storage Manager contracts)      ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Eviction state of a realized store per enlistment.
 * Tracked by the Storage Manager; transitions driven by memory pressure and demand.
 *
 *   FULL          document store + index store both in memory
 *   DOC_EVICTED   index store only; document store evicted; SQL fast, _doc slow
 *   EVICTED       neither in memory; enlistment entry retained; full rebuild on next access
 *   RELEASED      handle released; enlistment entry removed
 */
enum class EnlistmentEvictionState {
    FULL,
    DOC_EVICTED,
    EVICTED,
    RELEASED,
}

/**
 * Controls whether the index store participates in LRU eviction.
 * Declared per namespace in namespace policy.
 */
enum class IndexRetention {
    /**
     * Index stays in memory as long as the enlistment is open.
     * Only evicted when [RealizedStoreHandle] is explicitly released.
     * Under extreme OOM the Storage Manager logs a warning but will not
     * forcibly evict; see [IndexPinViolationEvent].
     */
    PINNED,
    /**
     * Index participates in LRU eviction, but with lower priority than the
     * document store (document store is always evicted first).
     */
    EVICTABLE,
}

/**
 * Emitted when a PINNED index cannot be honoured under extreme memory pressure.
 * The receiver (caller of [RealizedStoreHandle]) decides the escalation path.
 */
data class IndexPinViolationEvent(
    val namespaceId: String,
    val enlistmentId: KdbUuid,
    val currentPressureBytes: Long,
    val pinnedIndexSizeBytes: Long,
)

/**
 * Eviction interface on the storage engine — called by the Storage Manager.
 * All methods are advisory: the engine transitions its internal state and
 * returns; it does not block the caller.
 */
interface EvictableStorageAdapter : StorageAdapter {

    /** Drop the document store for [enlistmentId]. Transitions to DOC_EVICTED. */
    suspend fun evictDocuments(enlistmentId: KdbUuid)

    /**
     * Drop the index store for [enlistmentId].
     * Only called when [IndexRetention.EVICTABLE] and extreme pressure exists.
     * Transitions DOC_EVICTED → EVICTED or FULL → EVICTED.
     */
    suspend fun evictIndex(enlistmentId: KdbUuid)

    /** Async rebuild of document store from the delta log. Transitions back toward FULL. */
    suspend fun rebuildDocuments(enlistmentId: KdbUuid, fromDeltaLog: DeltaSegmentReader)

    /** Async rebuild of index from current (rebuilt) document store. */
    suspend fun rebuildIndex(enlistmentId: KdbUuid, fromDocuments: StorageAdapter)

    /** Current eviction state for an enlistment. */
    fun evictionState(enlistmentId: KdbUuid): EnlistmentEvictionState
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECTION E — REALIZED STORE HANDLE + ENLISTMENT                  ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Reference-counted handle to a namespace's realized store at a specific commit.
 * Callers hold this while they need access; call [release] when done so the
 * Storage Manager can reclaim memory.
 */
interface RealizedStoreHandle : AutoCloseable {

    val namespaceId: String
    val commitHash: KdbHash
    val enlistmentId: KdbUuid

    /** True once the realized store is fully materialised and ready to serve queries. */
    val isReady: Boolean

    /**
     * Suspend until [isReady] is true (rebuild complete).
     * [blockingPolicy] controls behaviour while rebuilding.
     */
    suspend fun awaitReady(blockingPolicy: RebuildBlockingPolicy = RebuildBlockingPolicy.WAIT)

    /** Access the underlying [StorageAdapter] for this realized store. */
    val storage: StorageAdapter

    /** Release this handle. The Storage Manager may then evict the realized store. */
    override fun close()
    fun release() = close()

    /** Subscribe to [IndexPinViolationEvent] on the indexes held by this enlistment. */
    fun onIndexPinViolation(handler: (IndexPinViolationEvent) -> Unit)
}

enum class RebuildBlockingPolicy {
    /** Suspend until rebuild completes. */
    WAIT,
    /** Return immediately; queries may return partial results until [isReady] is true. */
    PARTIAL_OK,
}

/**
 * Handle for a browser enlistment. Extends [RealizedStoreHandle] with the
 * push/resolve lifecycle specific to browser (Mode 2 / Mode 3) nodes.
 */
interface EnlistmentHandle : RealizedStoreHandle {

    val branchRef: String

    /** Current push lifecycle state. */
    val pushState: EnlistmentPushState

    /**
     * Push local commits to the upstream peer.
     * Returns [PushResult.Success] or [PushResult.Rejected] with missing delta hashes.
     */
    suspend fun push(): PushResult

    /**
     * Called after a [PushResult.Rejected] to fetch missing deltas from the peer.
     * Transitions [pushState] to [EnlistmentPushState.RESOLVING].
     */
    suspend fun fetchMissing()

    /**
     * After [fetchMissing] and local conflict resolution, attempt push again.
     * Transitions back to [EnlistmentPushState.IDLE] on success.
     */
    suspend fun resolveAndPush(): PushResult

    /**
     * The commit hash anchor recorded in the browser snapshot.
     * Used to request deltas since the snapshot on reconnect.
     * Null if no snapshot exists for this enlistment.
     */
    val snapshotAnchorHash: KdbHash?

    /**
     * Write the current realized store to localStorage/sessionStorage as a
     * BSON+zstd snapshot. Best-effort; failures are logged but not thrown.
     */
    suspend fun writeSnapshot()

    /**
     * Attempt to restore the realized store from a localStorage/sessionStorage snapshot.
     * Returns [SnapshotRestoreResult].
     */
    suspend fun restoreSnapshot(): SnapshotRestoreResult
}

enum class EnlistmentPushState {
    IDLE,       // no pending push
    PUSHING,    // push in progress
    REJECTED,   // peer rejected; awaiting fetchMissing
    RESOLVING,  // fetching and resolving deltas
}

sealed class PushResult {
    object Success : PushResult()
    data class Rejected(val missingDeltaHashes: List<KdbHash>) : PushResult()
}

sealed class SnapshotRestoreResult {
    /** Snapshot loaded successfully; realized store is ready. */
    data class Restored(val anchorHash: KdbHash) : SnapshotRestoreResult()
    /** Snapshot missing, corrupt, or integrity check failed; peer fetch required. */
    data class Failed(val reason: SnapshotFailureReason) : SnapshotRestoreResult()
    /** Snapshot anchor hash has been compacted away on the server. */
    object AnchorCompactedAway : SnapshotRestoreResult()
}

enum class SnapshotFailureReason {
    NOT_FOUND, INTEGRITY_CHECK_FAILED, DESERIALIZATION_ERROR, ANCHOR_COMPACTED_AWAY
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECTION F — PLATFORM I/O SHIM (expect/actual boundary)          ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Platform I/O shim — the only expect/actual boundary in the storage layer.
 * Implemented in jvmMain (java.nio), nativeMain (POSIX), jsMain (in-memory +
 * localStorage/sessionStorage zstd snapshot).
 *
 * All storage engine implementations in commonMain call only this interface.
 */
expect interface PlatformIoShim {

    /** Append bytes to the named segment. Returns new segment size. */
    suspend fun appendToSegment(segmentName: String, bytes: ByteArray): Long

    /** Read bytes from the named segment at [offset] with [length]. */
    suspend fun readFromSegment(segmentName: String, offset: Long, length: Int): ByteArray

    /** Flush OS write buffers for the named segment. */
    suspend fun flushSegment(segmentName: String)

    /** Seal (close for writing) the named segment. */
    suspend fun sealSegment(segmentName: String)

    /** List all segment names for a namespace. */
    suspend fun listSegments(namespaceId: String): List<String>

    /** Delete a segment (called during compaction GC). */
    suspend fun deleteSegment(segmentName: String)

    /** Total bytes available for storage on this platform. */
    suspend fun availableBytes(): Long

    // ── Browser-only: snapshot persistence ────────────────────────

    /** Read a named snapshot blob from persistent store (localStorage/sessionStorage). */
    suspend fun readSnapshot(key: String): ByteArray?

    /** Write a named snapshot blob to persistent store. Best-effort. */
    suspend fun writeSnapshot(key: String, data: ByteArray)

    /** Delete a named snapshot blob from persistent store. */
    suspend fun deleteSnapshot(key: String)
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECTION G — STORAGE ENGINE CONFIGURATION                        ║
// ╚══════════════════════════════════════════════════════════════════╝

data class StorageEngineConfig(
    /** Target page / segment size before sealing. Default: 8 MB. */
    val pageTargetSizeBytes: Long = 8L * 1024 * 1024,
    /** Maximum page / segment size. Default: 16 MB. */
    val pageMaxSizeBytes: Long = 16L * 1024 * 1024,
    /** Total memory budget for all realized stores, in bytes. */
    val globalMemoryBudgetBytes: Long,
    /** Compression codec for delta segments. Default: ZSTD. */
    val compressionCodec: CompressionCodec = CompressionCodec.ZSTD,
    /** Default index retention when namespace policy does not specify. */
    val defaultIndexRetention: IndexRetention = IndexRetention.EVICTABLE,
    /** Platform I/O shim instance (injected; differs per platform). */
    val ioShim: PlatformIoShim,
)

// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECTION H — GPU PROMOTION POLICY                                ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Controls when delta segments are promoted to GPU memory for the [GpuStorageEngine].
 * Declared per namespace in namespace policy. Ignored when no GPU engine is active.
 */
data class GpuPromotionPolicy(
    val strategy: GpuPromotionStrategy,
    /** Minimum segment age before promotion is considered. Default: 5 minutes. */
    val minSegmentAgeMillis: Long = 5 * 60 * 1000L,
    /** Minimum segment size before promotion is considered. Default: 64 MB. */
    val minSegmentSizeBytes: Long = 64L * 1024 * 1024,
    /** Maximum write rate; segments with higher churn are not promoted. Default: 100/min. */
    val maxChangeRatePerMinute: Int = 100,
)

enum class GpuPromotionStrategy {
    /** Promote the first time a GPU-accelerated query hits this segment. */
    PROMOTE_ON_QUERY,
    /** Promote as soon as the segment is sealed, regardless of query demand. */
    PROMOTE_EAGERLY,
    /** Never promote. GPU engine is not used for this namespace. */
    NEVER,
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  SECTION I — EXCEPTIONS                                          ║
// ╚══════════════════════════════════════════════════════════════════╝

class DocumentNotFoundException(
    message: String,
    val namespaceId: String,
    val docId: KdbUuid,
    val atCommit: KdbHash,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

class StorageAdapterException(
    message: String,
    val namespaceId: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

class DeltaSegmentSealedException(
    message: String,
    val segmentId: KdbUuid,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

class SnapshotIntegrityException(
    message: String,
    val key: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

class EnlistmentNotFoundException(
    message: String,
    val enlistmentId: KdbUuid,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.NAMESPACE_NOT_FOUND
}
```

---

## 4. Data Structures

### `StorageCapabilitySet`
Queried at enlistment creation. The Storage Manager uses this to select the correct engine and interaction pattern (direct delta ingest vs CPU-path materialisation, GPU bulk read, etc.). Extended from v2 with `supportsGpuBulkRead` and `supportsDirectDeltaIngest` per the v3 design decisions.

### `DeltaAuthorshipEnvelope`
Stored in every `DeltaRecord`. The `principal` and `rightsToken` fields enable blame queries. The engine stores them verbatim; it never validates them. The `clientContext` field is a free-form opaque blob for caller use (session tracking, etc.).

### `DeltaRecord`
The physical unit of the delta log. One record per committed `KdbCommit`. Contains the commit BSON and the before/after document patches for all affected documents. BSON-native; written to large append-only segments.

### `DeltaSegmentRef`
An opaque reference to a sealed, compressed segment on disk. Passed to `ingestDeltaSegment` for GPU direct ingest. Contains enough metadata for the Storage Manager to make promotion decisions without opening the segment.

### `EnlistmentEvictionState`
Four-state machine per enlistment, tracking what is and isn't in memory. Transitions are driven by the Storage Manager's eviction scheduler. `RELEASED` is terminal.

### `IndexRetention`
Two values. `PINNED` excludes the index from LRU. `EVICTABLE` participates. Stored in namespace policy; defaults to `EVICTABLE`.

### `SnapshotRestoreResult`
Three-way result: `Restored` (happy path), `Failed` (integrity or parse error), `AnchorCompactedAway` (anchor hash compacted away — distinct from generic failure so the caller can signal this cleanly rather than treating it as a generic snapshot failure, per the v3 open question resolution).

---

## 5. Contracts

### `StorageAdapter.getDocuments` (bulk read)
All implementations must accept a list of docIds and return results in input order, with null for missing documents. The batch form is mandatory (not optional) so the GPU implementation can serve it from GPU memory buffers in a single kernel dispatch. Single-document reads (`getDocument`) are implemented as `getDocuments(listOf(docId))[0]` for consistency.

### `StorageAdapter.commitTree`
**Atomicity:** All pending puts/deletes are committed atomically. If the process crashes mid-write, the next `getDocument` at the new tree hash must not return partial state. The implementation is responsible for WAL or equivalent crash recovery.

**Idempotency:** Calling `commitTree` twice with the same pending operations produces the same `DocumentTree` hash.

### `DeltaSegmentWriter.append`
**Ordering:** Records are stored in append order. Replay always reads in append order. The segment is not a random-access store.

**Immutability after seal:** Once `seal()` is called, `append` must throw `DeltaSegmentSealedException`.

### `RealizedStoreHandle.release`
**Reference counting:** The Storage Manager maintains a reference count per realized store. A store is eligible for eviction only when its reference count is zero. Calling `release` decrements the count; the store is not immediately freed.

### `EnlistmentHandle.writeSnapshot`
**Best-effort:** Failures must be logged but not thrown. A failed snapshot write degrades performance on next reload (requiring peer re-sync) but is not a correctness failure.

### `PlatformIoShim.readSnapshot` / `writeSnapshot`
**Browser constraint:** These methods write to localStorage/sessionStorage. The realized store (not the delta log) is what is snapshotted, per the v3 design decision.

---

## 6. Error Cases

| Exception | When |
|---|---|
| `DocumentNotFoundException` | `getDocumentOrThrow` finds no document at the given commit. |
| `StorageAdapterException` | Unrecoverable I/O error during read or write. Wraps the underlying platform exception. |
| `DeltaSegmentSealedException` | `DeltaSegmentWriter.append` called after `seal()`. |
| `SnapshotIntegrityException` | `restoreSnapshot` finds a snapshot whose checksum or BSON structure is invalid. |
| `EnlistmentNotFoundException` | `evictDocuments`, `evictIndex`, `rebuildDocuments`, or `rebuildIndex` called with an unknown `enlistmentId`. |

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `putAndGetDocument_roundtrip` | `putDocument` then `commitTree` then `getDocument`. | Returns the same `KdbDocument` (same JSON, same ID, same content hash). |
| 2 | `deleteDocument_notFoundAfterCommit` | `putDocument`, `commitTree`, `deleteDocument`, `commitTree` again. | `getDocument` at the second tree hash returns null. `getDocument` at the first hash still returns the doc. |
| 3 | `scanDocuments_returnsAll` | Insert 100 documents, `commitTree`. `scanDocuments` with `batchSize=10`. | All 100 docs returned across batches; no duplicates; no omissions. |
| 4 | `deltaSegmentWriter_appendAndRead` | Append 5 `DeltaRecord` instances, seal, read all via `DeltaSegmentReader`. | Records returned in append order; content identical to written records. |
| 5 | `appendAfterSeal_throws` | Seal a segment, then call `append`. | `DeltaSegmentSealedException` thrown. |
| 6 | `capabilitySet_serverEngine` | Instantiate `ServerStorageEngine`. | `persistsDeltaLog=true`, `persistsAcrossReload=true`, `supportsDirectDeltaIngest=false`. |
| 7 | `capabilitySet_browserEngine` | Instantiate `BrowserStorageEngine`. | `persistsDeltaLog=false`, `persistsAcrossReload=partial (true)`, `supportsDirectDeltaIngest=false`. |
| 8 | `snapshotRoundtrip_browser` | `writeSnapshot()` then `restoreSnapshot()`. | Returns `SnapshotRestoreResult.Restored` with the correct anchor hash. Realized store is valid. |
| 9 | `snapshotIntegrityFail_returnsFailedResult` | Corrupt the snapshot bytes in localStorage. Call `restoreSnapshot`. | Returns `SnapshotRestoreResult.Failed(INTEGRITY_CHECK_FAILED)`. Does not throw. |
| 10 | `evictDocuments_transitionsState` | FULL enlistment. Call `evictDocuments`. | `evictionState` returns `DOC_EVICTED`. `getDocument` still works (via delta replay). `IndexStore` intact. |
| 11 | `rebuildDocuments_transitionsToFull` | DOC_EVICTED enlistment. Call `rebuildDocuments`. Await completion. | `evictionState` returns `FULL`. `getDocument` served from document store again. |
| 12 | `anchorCompactedAway_distinctResult` | Snapshot's anchor hash is below the server's compaction boundary. Call `restoreSnapshot`. | Returns `SnapshotRestoreResult.AnchorCompactedAway` (not `Failed`). |

---

## 8. Non-Goals

- **WAL, MemTable, SSTable, block cache algorithms** — these are Layer 4a implementation details.
- **Storage Manager orchestration** — eviction scheduling, promotion queues, rebuild scheduler — Layer 4b.
- **GPU kernel implementations** — the GPU engine's internal columnar layout and decompaction logic — Layer 4a / Compute Adapter (Layer 9).
- **Transport framing** — delta records are written to the log and read for peer sync, but framing them for the wire is the Wire Protocol's job (Layer 7).
- **Rights validation** — the authorship envelope is stored and surfaced but not enforced here.
- **Compaction** — segment GC and squash boundaries are Compaction Engine (Layer 6) concerns.

---

## 9. Implementation Notes

### `expect interface PlatformIoShim`

Kotlin Multiplatform `expect interface` with three `actual` implementations:
- **jvmMain:** `java.nio.channels.AsynchronousFileChannel` for segment I/O; `java.nio.file.Files` for segment listing.
- **nativeMain:** POSIX `open/read/write/fsync` via `kotlinx.cinterop`.
- **jsMain:** In-memory `ArrayBuffer` map for segments; `localStorage` / `sessionStorage` for snapshot blobs.

The `PlatformIoShim` is the **only** `expect/actual` in the storage layer. Everything else is `commonMain`.

### `DeltaRecord` serialisation

BSON-encoded, then zstd-compressed, then appended to the segment with a 4-byte length prefix (big-endian) before the compressed payload. The length prefix allows forward scanning and corruption detection. The `commitHash` is recorded in the prefix metadata so the reader can skip records without full BSON decode.

### Content-addressed blob store

`writeBlob` computes SHA-256 of the input bytes and stores them keyed by hash. `readBlob` is a pure hash lookup. Deduplication is implicit: documents with identical content share storage. This is the same mechanism that allows the commit DAG to share unchanged document content between commits.

### Browser snapshot format

BSON+zstd of `{ commitHash: BinData, documentMap: { [docId]: BsonDocument, ... }, indexState: BinData }` per enlistment. The index state is the `IndexStore.snapshot()` byte array for each index in the realized store. The whole blob is written atomically to a single localStorage key per enlistment. Max size: ~5 MB per key (localStorage limit); if the realized store exceeds this, the snapshot is skipped and a warning is logged.

### `ingestDeltaSegment` for GPU

The GPU engine receives a `DeltaSegmentRef`, opens the compressed segment via the `DeltaSegmentReader` interface, decompresses into its own GPU-resident columnar format (outside commonMain — implemented in the Compute Adapter), and updates its internal segment map. The CPU realized store is never involved.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `StorageCapabilitySet` + `IndexRetention` + `EnlistmentEvictionState` | 80 |
| `DeltaAuthorshipEnvelope` + `DeltaRecord` + `DocumentPatch` + `DeltaSegmentRef` | 120 |
| `DeltaSegmentWriter` interface | 60 |
| `DeltaSegmentReader` interface | 50 |
| `StorageAdapter` interface | 120 |
| `EvictableStorageAdapter` interface | 80 |
| `RealizedStoreHandle` + `EnlistmentHandle` interfaces | 120 |
| `PushResult` + `SnapshotRestoreResult` + enums | 60 |
| `PlatformIoShim` expect interface | 80 |
| `StorageEngineConfig` | 50 |
| `GpuPromotionPolicy` + `GpuPromotionStrategy` | 60 |
| `IndexPinViolationEvent` + `RebuildBlockingPolicy` | 30 |
| Exception classes | 80 |
| Unit tests (interface contract tests, mocked implementations) | 600 |
| **Total** | **~1,590** |
