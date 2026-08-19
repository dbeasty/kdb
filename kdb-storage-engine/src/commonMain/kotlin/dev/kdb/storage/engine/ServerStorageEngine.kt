package dev.kdb.storage.engine

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import dev.kdb.document.kdbSha256
import dev.kdb.compute.ComputeAdapter
import dev.kdb.compute.GpuSegmentIngestRequest
import dev.kdb.storage.*
import dev.kdb.inspect.sidecar.deltaDebugHookOrNoOp
import dev.kdb.storage.delta.DeltaSegmentFactory
import dev.kdb.storage.memtable.MemTableManager
import dev.kdb.storage.sstable.BlockCache
import dev.kdb.storage.sstable.LsmBlobStore
import dev.kdb.storage.wal.DefaultWriteAheadLogFactory
import dev.kdb.storage.wal.WalPutBlob
import dev.kdb.storage.wal.WalRecord
import dev.kdb.storage.wal.WalRecordKind
import dev.kdb.storage.wal.WriteAheadLog
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public open class ServerStorageEngine(
    override val namespaceId: String,
    private val config: StorageEngineConfig,
    private val wal: WriteAheadLog?,
) : StorageEngine {
    override val capabilities: StorageCapabilitySet =
        StorageCapabilitySet(
            persistsDeltaLog = true,
            persistsAcrossReload = wal != null,
            supportsGpuBulkRead = false,
            supportsDirectDeltaIngest = false,
            maxEnlistments = null,
            indexRetentionDefault = IndexRetention.EVICTABLE,
        )

    private val mutex = Mutex()
    private val cache = BlockCache(config.globalMemoryBudgetBytes / 4)
    private val blobStore = LsmBlobStore(config.ioShim, namespaceId, cache)
    private val memTable = MemTableManager(namespaceId, config.ioShim, blobStore)
    private val docs = mutableMapOf<KdbUuid, KdbDocument>()
    private val enlistmentStates = mutableMapOf<KdbUuid, EnlistmentEvictionState>()

    override suspend fun writeBlob(bytes: ByteArray): KdbHash {
        val hash = KdbHash.fromBytes(kdbSha256(bytes))
        // No outer engine-wide mutex here: wal.append (own internal mutex, sequences safely),
        // wal.sync (group-commit -- concurrent callers batch onto one fsync), and memTable.put
        // (own internal mutex) are each independently safe for concurrent callers. An earlier
        // attempt to remove this mutex was reverted after appearing to cause kdb-jdbc's
        // FilePersistenceTest to fail; a repeated-run comparison later showed that test fails at
        // the same ~50% rate with or without this mutex (pre-existing flake, unrelated -- see
        // DefaultIndexWriter.applyCommit's silent `?: continue` on a getDocument miss), so the
        // mutex was removed again since it never was the cause and bought no correctness.
        wal?.append(
            WalRecord(
                0,
                KdbTimestamp.now(),
                WalRecordKind.PutBlob,
                WalPutBlob(hash, bytes).encode(),
            ),
        )
        wal?.sync()
        memTable.put(hash, bytes)
        return hash
    }

    override suspend fun readBlob(contentHash: KdbHash): ByteArray? = memTable.get(contentHash)

    /** Replay [WriteAheadLog] PutBlob records into the memtable (file-mode reopen). */
    public suspend fun recoverBlobsFromWal() {
        val log = wal ?: return
        log.recover { record ->
            if (record.kind != WalRecordKind.PutBlob) return@recover
            val payload = record.payload
            if (payload.size < 32) return@recover
            val hash = KdbHash.fromBytes(payload.copyOfRange(0, 32))
            val bytes = payload.copyOfRange(32, payload.size)
            mutex.withLock {
                memTable.put(hash, bytes)
            }
        }
    }

    override suspend fun putDocument(namespaceId: String, document: KdbDocument) {
        mutex.withLock { docs[document.id] = document }
    }

    override suspend fun getDocument(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument? = mutex.withLock { docs[docId] }

    override suspend fun getDocumentOrThrow(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument =
        getDocument(namespaceId, docId, atCommit)
            ?: throw DocumentNotFoundException("not found", namespaceId, docId, atCommit)

    override suspend fun getDocuments(
        namespaceId: String,
        docIds: List<KdbUuid>,
        atCommit: KdbHash,
    ): List<KdbDocument?> = docIds.map { getDocument(namespaceId, it, atCommit) }

    override suspend fun scanDocuments(
        namespaceId: String,
        atCommit: KdbHash,
        batchSize: Int,
        onBatch: suspend (List<KdbDocument>) -> Unit,
    ) {
        mutex.withLock {
            docs.values.chunked(batchSize).forEach { onBatch(it) }
        }
    }

    override suspend fun deleteDocument(namespaceId: String, docId: KdbUuid) {
        mutex.withLock { docs.remove(docId) }
    }

    override suspend fun commitTree(namespaceId: String, parentTreeHash: KdbHash): DocumentTree {
        val entries = mutex.withLock { docs.mapValues { (_, d) -> d.contentHash } }
        return DocumentTree.build(entries)
    }

    override suspend fun flush(namespaceId: String) {
        mutex.withLock {
            memTable.flush()
            wal?.sync()
        }
    }

    override suspend fun ingestDeltaSegment(segment: DeltaSegmentRef) {}

    override suspend fun evictDocuments(enlistmentId: KdbUuid) {
        enlistmentStates[enlistmentId] = EnlistmentEvictionState.DOC_EVICTED
    }

    override suspend fun evictIndex(enlistmentId: KdbUuid) {
        enlistmentStates[enlistmentId] = EnlistmentEvictionState.EVICTED
    }

    override suspend fun rebuildDocuments(enlistmentId: KdbUuid, fromDeltaLog: DeltaSegmentReader) {}

    override suspend fun rebuildIndex(enlistmentId: KdbUuid, fromDocuments: StorageAdapter) {}

    override fun evictionState(enlistmentId: KdbUuid): EnlistmentEvictionState =
        enlistmentStates[enlistmentId] ?: EnlistmentEvictionState.FULL
}

private fun WalPutBlob.encode(): ByteArray = contentHash.bytes + bytes

public class DefaultStorageEngineFactory(
    override val target: StorageEngineTarget,
) : StorageEngineFactory {
    override suspend fun open(namespaceId: String, config: StorageEngineConfig): StorageEngineHandle {
        val wal =
            if (target == StorageEngineTarget.SERVER) {
                DefaultWriteAheadLogFactory().openOrCreate(namespaceId, config, config.ioShim)
            } else {
                null
            }
        val engine =
            when (target) {
                StorageEngineTarget.SERVER -> ServerStorageEngine(namespaceId, config, wal)
                StorageEngineTarget.BROWSER -> BrowserStorageEngine(namespaceId, config, null)
                StorageEngineTarget.IN_MEMORY -> InMemoryStorageEngine(namespaceId, config)
                StorageEngineTarget.GPU -> GpuStorageEngine(namespaceId, config)
            }
        val deltaFactory = DeltaSegmentFactory(config, deltaDebugHookOrNoOp(config.debugSidecar))
        val delta = if (target == StorageEngineTarget.SERVER) deltaFactory.openWriter(namespaceId) else null
        val reader = deltaFactory.openReader(namespaceId)
        return DefaultStorageEngineHandle(namespaceId, engine, delta, reader)
    }
}

private class DefaultStorageEngineHandle(
    override val namespaceId: String,
    override val adapter: StorageAdapter,
    override val deltaWriter: DeltaSegmentWriter?,
    override val deltaReader: DeltaSegmentReader?,
) : StorageEngineHandle {
    override fun close() {}
}

public class BrowserStorageEngine(
    namespaceId: String,
    config: StorageEngineConfig,
    wal: WriteAheadLog?,
) : ServerStorageEngine(namespaceId, config, wal)

public class InMemoryStorageEngine(
    namespaceId: String,
    config: StorageEngineConfig,
) : ServerStorageEngine(namespaceId, config, null)

public class GpuStorageEngine(
    namespaceId: String,
    config: StorageEngineConfig,
    private val computeAdapter: ComputeAdapter? = null,
) : ServerStorageEngine(namespaceId, config, null) {
    private val pendingSegments = mutableListOf<DeltaSegmentRef>()

    override val capabilities: StorageCapabilitySet =
        StorageCapabilitySet(
            persistsDeltaLog = false,
            persistsAcrossReload = false,
            supportsGpuBulkRead = computeAdapter?.capabilities?.supportsVectorSearch == true,
            supportsDirectDeltaIngest = computeAdapter?.capabilities?.supportsDirectDeltaIngest == true,
            maxEnlistments = null,
            indexRetentionDefault = IndexRetention.EVICTABLE,
        )

    override suspend fun ingestDeltaSegment(segment: DeltaSegmentRef) {
        pendingSegments.add(segment)
        val adapter = computeAdapter ?: return
        val handle =
            adapter.ingestDeltaSegment(GpuSegmentIngestRequest(segment, ByteArray(0)))
        pendingSegments.remove(segment)
        adapter.releaseSegment(handle)
    }

    public fun pendingSegments(): List<DeltaSegmentRef> = pendingSegments.toList()
}
