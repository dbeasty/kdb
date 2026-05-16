# KDB Component Spec — Layer 4a
## Component 10e: Storage Engine Core
### `dev.kdb.storage.engine`

**File:** `kdb-spec-layer4a-component10e-storage-engine-core.md`
**Layer:** 4a — KDB Storage Engine
**Depends on:** Layer 3 Component 9; Layer 4a Components 10a–10d, 10g

---

## 1. Purpose

The Storage Engine Core wires Layer 4a building blocks into the **concrete storage engines** that implement Layer 3's `StorageAdapter` and `EvictableStorageAdapter`. It is the integration point where WAL + MemTable + SSTable serve content-addressed blobs and realized-store blocks, and where `DeltaSegmentWriter` (10d) owns the canonical delta log for durable engines.

Four engine variants ship here: **Server** (full durability), **Browser** (partial persistence + snapshot API), **InMemory** (volatile, tests and dev), and **Gpu** (stub implementing direct delta ingest hooks). `StorageEngineFactory.open` is the single entry point for constructing a namespace-scoped engine instance from `StorageEngineConfig`.

---

## 2. Dependencies

| Module | Interface used |
|---|---|
| `dev.kdb.storage` (Layer 3) | `StorageAdapter`, `EvictableStorageAdapter`, `StorageEngineConfig`, `StorageCapabilitySet`, `DeltaSegmentWriter`, `DeltaSegmentReader`, `DeltaRecord`, `DeltaSegmentRef`, `EnlistmentEvictionState`, `IndexRetention`, exceptions |
| `dev.kdb.storage.wal` (10a) | `WriteAheadLog`, `WalReplayer`, `WalRecord` |
| `dev.kdb.storage.memtable` (10b) | `MemTable`, `MemTableSnapshot` |
| `dev.kdb.storage.sstable` (10c) | `LsmBlobStore`, `SsTableReader`, `SsTableWriter`, `BlockCache` |
| `dev.kdb.storage.delta` (10d) | `DeltaSegmentWriterFactory`, `DeltaSegmentReader` |
| `dev.kdb.storage.io` (10g) | `PlatformIoShim` via config |
| `dev.kdb.document` | `KdbDocument`, `DocumentTree` |
| `dev.kdb.codec` | `KdbUuid`, `KdbHash` |
| `dev.kdb.error` | `KdbException`, `KdbErrorCode` |

---

## 3. Public Interface

```kotlin
package dev.kdb.storage.engine

import dev.kdb.codec.*
import dev.kdb.document.*
import dev.kdb.storage.*
import dev.kdb.storage.delta.DeltaSegmentWriterFactory
import dev.kdb.storage.memtable.MemTable
import dev.kdb.storage.sstable.LsmBlobStore
import dev.kdb.storage.wal.WriteAheadLog

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Engine target + factory                                         ║
// ╚══════════════════════════════════════════════════════════════════╝

/** Which physical engine implementation to construct. */
enum class StorageEngineTarget {
    SERVER,
    BROWSER,
    IN_MEMORY,
    GPU,
}

/**
 * Opens namespace-scoped engines. One factory per process; thread-safe.
 * Injects shared [BlockCache] and WAL directory from [StorageEngineConfig].
 */
interface StorageEngineFactory {

    val target: StorageEngineTarget

    /**
     * Open or attach storage for [namespaceId].
     * Idempotent: second call returns a new [StorageEngineHandle] sharing underlying files/state.
     */
    suspend fun open(
        namespaceId: String,
        config: StorageEngineConfig,
    ): StorageEngineHandle

    companion object {
        /** Platform default: SERVER on JVM/Native, BROWSER on JS, IN_MEMORY in commonTest. */
        fun forTarget(target: StorageEngineTarget, sharedConfig: StorageEngineConfig): StorageEngineFactory
    }
}

/** Lifetime handle for one namespace attachment. Close when namespace is dropped. */
interface StorageEngineHandle : AutoCloseable {
    val namespaceId: String
    val adapter: StorageAdapter
    /** Active delta writer for appends; null for InMemory/Gpu when delta not persisted. */
    val deltaWriter: DeltaSegmentWriter?
    val deltaReader: DeltaSegmentReader?
    override fun close()
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Server engine                                                   ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Durable CPU engine: WAL → MemTable → SSTable for blobs; delta log via 10d.
 * Implements [EvictableStorageAdapter] for Storage Manager (Layer 4b).
 */
class ServerStorageEngine internal constructor(
    private val namespaceId: String,
    private val wal: WriteAheadLog,
    private val memTable: MemTable,
    private val blobStore: LsmBlobStore,
    private val deltaWriter: DeltaSegmentWriter,
    private val deltaReader: DeltaSegmentReader,
) : EvictableStorageAdapter {

    override val capabilities: StorageCapabilitySet

    // StorageAdapter — document + tree + blob (see §5)
    override suspend fun getDocument(namespaceId: String, docId: KdbUuid, atCommit: KdbHash): KdbDocument?
    override suspend fun getDocumentOrThrow(namespaceId: String, docId: KdbUuid, atCommit: KdbHash): KdbDocument
    override suspend fun getDocuments(namespaceId: String, docIds: List<KdbUuid>, atCommit: KdbHash): List<KdbDocument?>
    override suspend fun scanDocuments(namespaceId: String, atCommit: KdbHash, batchSize: Int, onBatch: suspend (List<KdbDocument>) -> Unit)
    override suspend fun putDocument(namespaceId: String, document: KdbDocument)
    override suspend fun deleteDocument(namespaceId: String, docId: KdbUuid)
    override suspend fun commitTree(namespaceId: String, parentTreeHash: KdbHash): DocumentTree
    override suspend fun flush(namespaceId: String)
    override suspend fun readBlob(contentHash: KdbHash): ByteArray?
    override suspend fun writeBlob(bytes: ByteArray): KdbHash
    override suspend fun ingestDeltaSegment(segment: DeltaSegmentRef)

    // EvictableStorageAdapter — enlistment-scoped realized store (§5)
    override suspend fun evictDocuments(enlistmentId: KdbUuid)
    override suspend fun evictIndex(enlistmentId: KdbUuid)
    override suspend fun rebuildDocuments(enlistmentId: KdbUuid, fromDeltaLog: DeltaSegmentReader)
    override suspend fun rebuildIndex(enlistmentId: KdbUuid, fromDocuments: StorageAdapter)
    override fun evictionState(enlistmentId: KdbUuid): EnlistmentEvictionState

    /** Register enlistment realized-store slot (called by 11a pool on enlist). */
    fun registerEnlistment(enlistmentId: KdbUuid, atCommit: KdbHash)
    fun releaseEnlistment(enlistmentId: KdbUuid)
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Browser engine                                                  ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Browser CPU engine: in-memory realized store + optional snapshot restore;
 * delta log not durable across reload (append in-session only).
 * [EvictableStorageAdapter] with same enlistment hooks as server.
 */
class BrowserStorageEngine internal constructor(
    private val namespaceId: String,
    private val blobStore: LsmBlobStore,
    private val sessionDeltaWriter: DeltaSegmentWriter?,
    private val deltaReader: DeltaSegmentReader?,
    private val ioShim: PlatformIoShim,
) : EvictableStorageAdapter {

    override val capabilities: StorageCapabilitySet
    // Same StorageAdapter + EvictableStorageAdapter surface as ServerStorageEngine
    // ingestDeltaSegment throws UnsupportedOperationException
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  In-memory engine                                                ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Volatile engine for tests. Delegates blob/tree semantics to [dev.kdb.storage.mem.InMemoryStorageAdapter]
 * but exposes the same factory/handle shape as durable engines.
 * Does NOT implement enlistment eviction (no rebuild path — data lost on evict).
 */
class InMemoryStorageEngine(
    namespaceId: String,
    capabilities: StorageCapabilitySet = StorageCapabilitySet.MEMORY,
) : StorageAdapter {

    override val capabilities: StorageCapabilitySet
    private val delegate: dev.kdb.storage.mem.InMemoryStorageAdapter
    // Full StorageAdapter delegation; ingestDeltaSegment throws
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  GPU engine (stub)                                               ║
// ╚══════════════════════════════════════════════════════════════════╝

/**
 * Stub GPU realized store. [ingestDeltaSegment] records segment ref for later Compute Adapter work.
 * [supportsDirectDeltaIngest] and [supportsGpuBulkRead] true; all document CRUD throw or return empty.
 */
class GpuStorageEngine internal constructor(
    private val namespaceId: String,
) : StorageAdapter {

    override val capabilities: StorageCapabilitySet

    override suspend fun getDocument(namespaceId: String, docId: KdbUuid, atCommit: KdbHash): KdbDocument? = null
    override suspend fun getDocumentOrThrow(namespaceId: String, docId: KdbUuid, atCommit: KdbHash): KdbDocument =
        throw DocumentNotFoundException("GPU engine does not serve CPU documents", namespaceId, docId, atCommit)
    override suspend fun getDocuments(namespaceId: String, docIds: List<KdbUuid>, atCommit: KdbHash): List<KdbDocument?> =
        docIds.map { null }
    override suspend fun scanDocuments(namespaceId: String, atCommit: KdbHash, batchSize: Int, onBatch: suspend (List<KdbDocument>) -> Unit) {}
    override suspend fun putDocument(namespaceId: String, document: KdbDocument) =
        throw UnsupportedOperationException("GpuStorageEngine is ingest-only in v1")
    override suspend fun deleteDocument(namespaceId: String, docId: KdbUuid) =
        throw UnsupportedOperationException("GpuStorageEngine is ingest-only in v1")
    override suspend fun commitTree(namespaceId: String, parentTreeHash: KdbHash): DocumentTree =
        throw UnsupportedOperationException("GpuStorageEngine is ingest-only in v1")
    override suspend fun flush(namespaceId: String) {}
    override suspend fun readBlob(contentHash: KdbHash): ByteArray? = null
    override suspend fun writeBlob(bytes: ByteArray): KdbHash =
        throw UnsupportedOperationException("GpuStorageEngine is ingest-only in v1")
    override suspend fun ingestDeltaSegment(segment: DeltaSegmentRef)

    /** Segments queued for GPU materialisation (in-memory until Compute Adapter exists). */
    fun pendingSegments(): List<DeltaSegmentRef>
}

// ╔══════════════════════════════════════════════════════════════════╗
// ║  Internal wiring (package-private factories)                     ║
// ╚══════════════════════════════════════════════════════════════════╝

internal object ServerStorageEngineFactory {
    suspend fun create(
        namespaceId: String,
        config: StorageEngineConfig,
        deltaFactory: DeltaSegmentWriterFactory,
    ): ServerStorageEngine
}

internal object BrowserStorageEngineFactory {
    suspend fun create(
        namespaceId: String,
        config: StorageEngineConfig,
        deltaFactory: DeltaSegmentWriterFactory,
    ): BrowserStorageEngine
}

/** Capability presets (reference values for tests). */
object StorageEngineCapabilities {
    val SERVER: StorageCapabilitySet = StorageCapabilitySet(
        persistsDeltaLog = true,
        persistsAcrossReload = true,
        supportsGpuBulkRead = false,
        supportsDirectDeltaIngest = false,
        maxEnlistments = null,
        indexRetentionDefault = IndexRetention.EVICTABLE,
    )
    val BROWSER: StorageCapabilitySet = StorageCapabilitySet(
        persistsDeltaLog = false,
        persistsAcrossReload = true,
        supportsGpuBulkRead = false,
        supportsDirectDeltaIngest = false,
        maxEnlistments = null,
        indexRetentionDefault = IndexRetention.EVICTABLE,
    )
    val GPU: StorageCapabilitySet = StorageCapabilitySet(
        persistsDeltaLog = false,
        persistsAcrossReload = false,
        supportsGpuBulkRead = true,
        supportsDirectDeltaIngest = true,
        maxEnlistments = 4,
        indexRetentionDefault = IndexRetention.EVICTABLE,
    )
}

class StorageEngineNotReadyException(
    message: String,
    val namespaceId: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}
```

---

## 4. Data Structures

### `StorageEngineTarget`
Selects which engine class `StorageEngineFactory.forTarget` constructs. JVM integration tests use `IN_MEMORY` or `SERVER` with temp `PlatformIoConfig`.

### `StorageEngineHandle`
Bundles `adapter` with active delta I/O for the Transaction Engine / Storage Manager. Closing the handle does not delete on-disk segments (namespace teardown is a separate admin operation).

### `ServerStorageEngine` internal state (owned, not all public)
| Field | Role |
|---|---|
| `wal` | Durability log for memtable + tree commits before SSTable flush |
| `memTable` | Pending blob and tree block writes |
| `blobStore` | LSM read path for `readBlob` / `writeBlob` |
| `deltaWriter` / `deltaReader` | Canonical delta log (10d) |
| `enlistments: Map<KdbUuid, EnlistmentSlot>` | Per-enlistment doc map, index handle ref, eviction state |

### `EnlistmentSlot` (internal)
```kotlin
internal data class EnlistmentSlot(
    val enlistmentId: KdbUuid,
    var atCommit: KdbHash,
    var state: EnlistmentEvictionState,
    var docMap: MutableMap<KdbUuid, KdbDocument>?,  // null when DOC_EVICTED/EVICTED
    // indexStore ref held by 11a — engine only signals evict/rebuild
)
```

### `GpuStorageEngine.pendingSegments`
In-memory list of `DeltaSegmentRef` passed to `ingestDeltaSegment` until Layer 9 Compute Adapter consumes them.

---

## 5. Contracts

### Write path (`putDocument` → `commitTree` → `flush`)
1. `putDocument` / `deleteDocument` update an in-memory **pending tree overlay** and write document bodies to `writeBlob` (content-addressed).
2. `commitTree` builds a new `DocumentTree` from parent + pending ops, appends a **WAL record** (`WalRecord.TreeCommit`), applies to enlistment `docMap` if registered, clears pending.
3. `flush` forces MemTable → SSTable flush, WAL flush, and `deltaWriter.flush()` when a delta append occurred in the same session.

**Atomicity:** After successful `commitTree`, `getDocument` at the returned tree hash sees the full tree. Crash mid-WAL replay restores via 10a `WalReplayer` on next `open`.

### Blob path
`writeBlob` → MemTable → (on flush) SSTable; `readBlob` checks MemTable then LSM levels. Hash is SHA-256 (Layer 0 `kdbSha256`).

### Delta path (Server)
Transaction Engine (via Storage Manager) appends `DeltaRecord` to `deltaWriter` **after** `commitTree` succeeds. Segment rolls when size ≥ `config.pageTargetSizeBytes` (delegates to 10f `runDeltaSegmentRoll`).

### `ingestDeltaSegment`
- **Server / Browser / InMemory:** throws `UnsupportedOperationException`.
- **Gpu:** appends ref to `pendingSegments()`; idempotent on same `segmentId`.

### Eviction (`ServerStorageEngine`, `BrowserStorageEngine`)
- `evictDocuments`: `docMap = null`, state → `DOC_EVICTED`; does not delete delta log or SSTables.
- `evictIndex`: state → `EVICTED` (or from `FULL` if index evictable); index rebuild delegated to 11c via `rebuildIndex`.
- `rebuildDocuments`: replay `fromDeltaLog` for enlistment's commit range into new `docMap`; state → `FULL` when complete.
- Unknown `enlistmentId` → `EnlistmentNotFoundException`.

### `InMemoryStorageEngine`
Implements `StorageAdapter` only. Eviction methods are not on the type; Storage Manager must not call eviction on pure in-memory enlistments (design decision v3 open question: treat as RELEASED on pressure).

### `StorageEngineFactory.open`
**Pre:** `config.ioShim` non-null for SERVER/BROWSER.
**Post:** WAL recovered, active delta segment open (server), `adapter` ready for reads at last known trees.
**Idempotent:** Multiple handles share one `ServerStorageEngine` instance per namespace (reference counted).

---

## 6. Error Cases

| Exception | When |
|---|---|
| `DocumentNotFoundException` | `getDocumentOrThrow` miss (all engines). |
| `StorageAdapterException` | WAL/SSTable/delta I/O failure during read/write. |
| `EnlistmentNotFoundException` | Eviction/rebuild on unknown enlistment (server/browser). |
| `StorageEngineNotReadyException` | Read before `open` recovery completes (optional strict mode). |
| `UnsupportedOperationException` | GPU document CRUD; `ingestDeltaSegment` on non-GPU engines. |
| `DeltaSegmentSealedException` | Propagated from delta writer after seal (caller must open new segment). |

---

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `server_putCommitTree_roundtrip` | WAL+MT+SSTable backed server; put 1 doc, commitTree, get. | Same document at new tree hash. |
| 2 | `server_walRecovery_afterCrash` | Put, commitTree without flush; simulate reopen. | Tree and blob recoverable. |
| 3 | `server_deltaAppend_afterCommit` | commitTree then deltaWriter.append record. | Segment size increases; reader lists segment. |
| 4 | `browser_noPersistDelta_reload` | Append delta, new factory open. | Empty segment list; snapshot API still works. |
| 5 | `inMemory_matchesLayer3Adapter` | InMemoryStorageEngine vs mem adapter. | Identical commitTree/get semantics. |
| 6 | `gpu_ingestDeltaSegment_queues` | Call ingest twice same segment. | `pendingSegments()` size 1 (dedupe by segmentId). |
| 7 | `gpu_putDocument_throws` | putDocument on GpuStorageEngine. | `UnsupportedOperationException`. |
| 8 | `evictDocuments_docEvicted` | Register enlistment, evictDocuments. | `evictionState` DOC_EVICTED; getDocument uses replay path or throws until rebuild. |
| 9 | `factory_open_idempotent` | open twice same namespace. | Same engine instance, refCount 2; close both safe. |
| 10 | `capabilities_serverPreset` | ServerStorageEngine.capabilities. | persistsDeltaLog=true, supportsDirectDeltaIngest=false. |

---

## 8. Non-Goals

- **Storage Manager orchestration** (pool, LRU, rebuild scheduler) — Layer 4b.
- **Compaction execution** — delegates triggers to 10f; engine only calls planner hooks.
- **Index algorithms** — Index Layer (Layer 5); engine holds enlistment slot only.
- **GPU kernels / columnar layout** — stub only; Compute Adapter Layer 9.
- **Peer sync / wire framing** — Layer 7.
- **Rights validation on delta authorship** — stored verbatim, not checked here.
- **Namespace policy parsing** — caller passes resolved `StorageEngineConfig`.

---

## 9. Implementation Notes

### Wiring diagram (Server)

```
putDocument/deleteDocument
    → pending overlay + writeBlob → MemTable
commitTree
    → WAL(TreeCommit) → update enlistment docMap + materialize DocumentTree hash
flush
    → MemTable.flush(SsTableWriter) → WAL.markFlushed → deltaWriter.flush()
readBlob
    → MemTable → LsmBlobStore (block cache)
getDocument
    → enlistment docMap OR rebuild from tree hash + blob store
```

### Browser differences
- No WAL durability across reload; MemTable/SSTable may use in-memory LSM only.
- `sessionDeltaWriter` optional; delta segments lost on tab close.
- Snapshot read/write uses `config.ioShim` + `SnapshotKeyBuilder` (10g).

### Factory placement
Implement `DefaultStorageEngineFactory` in `commonMain` with `expect` platform target detection, or explicit `forTarget` in tests.

### Concurrency
Per-namespace mutex for `commitTree` and `flush`; per-enlistment mutex for doc map. WAL append serialised by 10a.

### Gradle
Module `:kdb-storage-engine` depends on `:kdb-storage`, `:kdb-storage-wal`, `:kdb-storage-memtable`, `:kdb-storage-sstable`, `:kdb-storage-delta`, `:kdb-storage-io`.

### Gpu stub evolution
Replace `pendingSegments()` queue with async ingest job when Compute Adapter lands; keep `capabilities` unchanged.

---

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `StorageEngineFactory` + `StorageEngineHandle` | 120 |
| `ServerStorageEngine` | 650 |
| `BrowserStorageEngine` | 420 |
| `InMemoryStorageEngine` wrapper | 80 |
| `GpuStorageEngine` stub | 100 |
| `EnlistmentSlot` + recovery | 180 |
| `StorageEngineCapabilities` + exceptions | 60 |
| Integration tests (server temp dir, in-memory, gpu stub) | 700 |
| **Total** | **~2,310** |

Core production code (excluding tests): **~1,530 NBNC**; focused engine-only slice **~400–450** aligns with per-file spec budget when tests are separate module.
