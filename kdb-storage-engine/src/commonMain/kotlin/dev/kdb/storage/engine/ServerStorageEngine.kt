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
import dev.kdb.storage.wal.GroupCommitter
import dev.kdb.storage.wal.WalPutBlob
import dev.kdb.storage.wal.WalRecord
import dev.kdb.storage.wal.WalRecordKind
import dev.kdb.storage.wal.WriteAheadLog
import dev.kdb.storage.Durability
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.TimeSource

/**
 * Best-effort final flush from non-suspend code (see [ServerStorageEngine.stopAsyncSync]).
 * `kotlinx.coroutines.runBlocking` has no JS implementation - a single-threaded event loop can't
 * synchronously wait for a suspend function the way JVM/Native can - so this can't be one
 * `commonMain` function. JVM and Native actually block until [wal]'s sync completes; JS starts
 * the sync and returns immediately without waiting for it (see the `jsMain` actual's doc comment
 * for why that's the honest option here, not a shortcut).
 */
internal expect fun blockingFinalSync(wal: WriteAheadLog?)

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

    // docs holds committed (visible) documents, sharded (ShardedDocStore)
    // rather than guarded by one namespace-wide mutex. putDocument/
    // deleteDocument stage into pending instead of writing here directly:
    // writes are not visible via getDocument until commitTree flushes
    // them, matching the pre-existing InMemoryStorageAdapter contract and
    // letting a failed transaction's write phase be rolled back via
    // discardPending without ever having mutated committed state. It is
    // intentionally separate from writeBlob: WriteAheadLog.append and
    // MemTableManager.put are each independently thread-safe, so blob
    // writes never take a lock at all - see Phase 1/2 of
    // docs/benchmarks/phase0-baseline.md.
    private val groupCommit = GroupCommitter()
    private val cache = BlockCache(config.resolvedGlobalMemoryBudgetBytes() / 4)
    private val blobStore = LsmBlobStore(config.ioShim, namespaceId, cache)
    private val memTable = MemTableManager(namespaceId, config.ioShim, blobStore)
    private val docs = ShardedDocStore()
    private val pending = ShardedPendingStore()
    private val enlistmentStates = mutableMapOf<KdbUuid, EnlistmentEvictionState>()

    // treeMu guards tree, a running DocumentTree updated incrementally
    // (O(delta) via its persistent trie - see DocumentTreeTrie.kt) as
    // commitTree flushes staged writes, instead of being rebuilt from a
    // full docs snapshot each time. Reintroducing staging (recovering a
    // WIP feature - see docs/benchmarks/phases-1-6-summary.md) put
    // putDocument/deleteDocument's visibility back behind commitTree; an
    // earlier pass had them update docs+tree immediately, which was
    // faster but silently dropped the "not visible until commit"
    // guarantee transactions depend on for write-phase rollback. The
    // O(delta) tree update from that pass is preserved; only the "when"
    // changed.
    private val treeMu = Mutex()
    private var tree = DocumentTree.EMPTY

    private val asyncScope: CoroutineScope? =
        if (wal != null && config.durability == Durability.ASYNC) {
            CoroutineScope(SupervisorJob() + Dispatchers.Default)
        } else {
            null
        }

    init {
        asyncScope?.launch {
            val interval = config.asyncSyncIntervalMillis ?: 5L
            while (isActive) {
                delay(interval)
                wal?.sync()
            }
        }
    }

    /**
     * Stops the background async-sync loop (if running) and does a final
     * flush, so an ASYNC-durability namespace doesn't lose writes made
     * just before shutdown. No-op for other durability modes.
     *
     * Not `suspend`: this overrides [StorageEngineHandle.close] via
     * [kotlin.AutoCloseable], whose contract is synchronous, so the final
     * flush has to be triggered from ordinary (non-suspend) code - see
     * [blockingFinalSync]'s platform-specific implementations for how each
     * target reconciles that with [WriteAheadLog.sync] being `suspend`.
     */
    public fun stopAsyncSync() {
        val scope = asyncScope ?: return
        scope.cancel()
        blockingFinalSync(wal)
    }

    override suspend fun writeBlob(bytes: ByteArray): KdbHash {
        val hash = KdbHash.fromBytes(kdbSha256(bytes))
        val w = wal
        if (w != null) {
            val result =
                w.append(
                    WalRecord(
                        0,
                        KdbTimestamp.now(),
                        WalRecordKind.PutBlob,
                        WalPutBlob(hash, bytes).encode(),
                    ),
                )
            when (config.durability) {
                Durability.SYNC -> {
                    val fsyncStart = TimeSource.Monotonic.markNow()
                    groupCommit.syncTo(result.sequence) { w.sync() }
                    StageRecorder.Default.record(StorageStage.FSYNC_WAIT, fsyncStart.elapsedNow())
                }
                Durability.ASYNC -> {
                    // Acknowledged once appended; the background loop
                    // above syncs periodically instead of per-write. A
                    // crash can lose up to one sync interval of writes.
                }
                Durability.MEMORY_ONLY -> {
                    // Never synced by this engine; caller owns any checkpointing.
                }
            }
        }
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
            memTable.put(hash, bytes)
        }
    }

    /** Stages document; it is not visible via getDocument until commitTree flushes staged writes. */
    override suspend fun putDocument(namespaceId: String, document: KdbDocument) {
        pending.put(document)
    }

    override suspend fun getDocument(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument? = docs.get(docId)

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
        docs.snapshot().chunked(batchSize).forEach { onBatch(it) }
    }

    /** Stages a deletion; it is not applied via getDocument until commitTree flushes staged writes. */
    override suspend fun deleteDocument(namespaceId: String, docId: KdbUuid) {
        pending.delete(docId)
    }

    /**
     * Drops any putDocument/deleteDocument calls made since the last
     * commitTree, restoring the last-committed visible state. Used to
     * roll back a transaction whose write phase failed partway through.
     */
    override suspend fun discardPending(namespaceId: String) {
        pending.discardAll()
    }

    /**
     * Flushes staged puts/deletes into docs (committed, sharded) and
     * applies them to the running tree incrementally (O(delta) per
     * changed doc via its persistent trie - DocumentTreeTrie.kt), then
     * returns it. parentTreeHash is ignored, matching the pre-existing
     * behavior this preserves: ServerStorageEngine has always reflected
     * current live (now: current committed) state rather than tracking
     * per-branch history.
     */
    override suspend fun commitTree(namespaceId: String, parentTreeHash: KdbHash): DocumentTree {
        val (puts, deletes) = pending.takeAllAndClear()
        return treeMu.withLock {
            for (id in deletes) {
                docs.delete(id)
                tree = tree.without(id)
            }
            for (doc in puts) {
                docs.put(doc)
                tree = tree.with(doc.id, doc.contentHash)
            }
            tree
        }
    }

    override suspend fun flush(namespaceId: String) {
        memTable.flush()
        wal?.sync()
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
    override fun close() {
        (adapter as? ServerStorageEngine)?.stopAsyncSync()
    }
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
