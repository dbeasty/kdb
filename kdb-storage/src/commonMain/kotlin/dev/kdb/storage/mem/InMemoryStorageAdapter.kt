package dev.kdb.storage.mem

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import dev.kdb.document.kdbSha256
import dev.kdb.storage.DocumentNotFoundException
import dev.kdb.storage.DeltaSegmentRef
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.StorageCapabilitySet
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Volatile adapter that keeps blobs + committed document trees keyed by hashes.
 *
 * Implements the [commitTree]/[scanDocuments]/[blob] semantics required by Layers 3–4 specs;
 * persistence is delegated to forthcoming Layer 4 engines.
 */
public class InMemoryStorageAdapter(
    override val capabilities: StorageCapabilitySet = StorageCapabilitySet.MEMORY,
) : StorageAdapter {

    private val mutex = Mutex()
    private val blobs = mutableMapOf<KdbHash, ByteArray>()
    private val docsByBlob = mutableMapOf<KdbHash, KdbDocument>()
    private val trees = mutableMapOf<KdbHash, DocumentTree>()
    private val pendingPuts = mutableMapOf<String, MutableMap<KdbUuid, KdbDocument>>()
    private val pendingDeletes = mutableMapOf<String, MutableSet<KdbUuid>>()

    init {
        trees[DocumentTree.EMPTY.treeHash] = DocumentTree.EMPTY
    }

    override suspend fun getDocument(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument? =
        mutex.withLock {
            val tree = trees[atCommit] ?: return@withLock null
            val h = tree.hashFor(docId) ?: return@withLock null
            docsByBlob[h]
        }

    override suspend fun getDocumentOrThrow(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument =
        getDocument(namespaceId, docId, atCommit)
            ?: throw DocumentNotFoundException("missing document $docId", namespaceId, docId, atCommit)

    override suspend fun getDocuments(
        namespaceId: String,
        docIds: List<KdbUuid>,
        atCommit: KdbHash,
    ): List<KdbDocument?> =
        docIds.map { getDocument(namespaceId, it, atCommit) }

    override suspend fun scanDocuments(
        namespaceId: String,
        atCommit: KdbHash,
        batchSize: Int,
        onBatch: suspend (List<KdbDocument>) -> Unit,
    ) {
        val entries =
            mutex.withLock { trees[atCommit]?.entries ?: emptyMap() }
        val buf = mutableListOf<KdbDocument>()
        for ((_, h) in entries) {
            val d =
                mutex.withLock { docsByBlob[h] }
                    ?: continue
            buf.add(d)
            if (buf.size >= batchSize) {
                onBatch(buf.toList())
                buf.clear()
            }
        }
        if (buf.isNotEmpty()) {
            onBatch(buf.toList())
        }
    }

    override suspend fun putDocument(
        namespaceId: String,
        document: KdbDocument,
    ) {
        mutex.withLock {
            val m = pendingPuts.getOrPut(namespaceId) { mutableMapOf() }
            pendingDeletes.getOrPut(namespaceId) { mutableSetOf() }.remove(document.id)
            m[document.id] = document
        }
    }

    override suspend fun deleteDocument(
        namespaceId: String,
        docId: KdbUuid,
    ) {
        mutex.withLock {
            pendingPuts.getOrPut(namespaceId) { mutableMapOf() }.remove(docId)
            pendingDeletes.getOrPut(namespaceId) { mutableSetOf() }.add(docId)
        }
    }

    override suspend fun commitTree(
        namespaceId: String,
        parentTreeHash: KdbHash,
    ): DocumentTree =
        mutex.withLock {
            val base =
                trees[parentTreeHash]
                    ?: error("missing parent tree $parentTreeHash")
            val dels = pendingDeletes.remove(namespaceId) ?: mutableSetOf()
            val puts = pendingPuts.remove(namespaceId) ?: mutableMapOf()
            val next = base.entries.toMutableMap()
            for (id in dels) {
                next.remove(id)
            }
            for ((id, doc) in puts) {
                rememberDocumentLocked(doc)
                next[id] = doc.contentHash
            }
            val built = DocumentTree.build(next)
            trees[built.treeHash] = built
            built
        }

    override suspend fun discardPending(namespaceId: String) {
        mutex.withLock {
            pendingPuts.remove(namespaceId)
            pendingDeletes.remove(namespaceId)
        }
    }

    override suspend fun flush(namespaceId: String) {
        mutex.withLock { }
    }

    override suspend fun readBlob(contentHash: KdbHash): ByteArray? =
        mutex.withLock { blobs[contentHash]?.copyOf() }

    override suspend fun writeBlob(bytes: ByteArray): KdbHash {
        val hash = KdbHash.fromBytes(kdbSha256(bytes))
        mutex.withLock {
            blobs[hash] = bytes.copyOf()
        }
        return hash
    }

    private fun rememberDocumentLocked(doc: KdbDocument) {
        val h = doc.contentHash
        blobs[h] = doc.json.encodeToByteArray()
        docsByBlob[h] = doc
    }

    override suspend fun ingestDeltaSegment(segment: DeltaSegmentRef) {
        throw UnsupportedOperationException(
            "MemoryStorageAdapter cannot ingest compressed delta segments (${segment.segmentId})",
        )
    }
}
