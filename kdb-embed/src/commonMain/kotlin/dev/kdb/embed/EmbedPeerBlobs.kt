package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.peersync.PeerBlobCorruptException
import dev.kdb.peersync.PeerBlobReader
import dev.kdb.peersync.PeerBlobStore
import dev.kdb.peersync.PeerStoredBlob

/** [namespaceId]'s attachment store as peer sync moves blobs in and out of it (BLOB_FETCH,
 * ATTACHMENT_UPLOAD). Pass it as PeerHostConfig.blobs / PeerClientConfig.blobs. */
public fun EmbeddedKdbRuntime.peerBlobStore(namespaceId: String = defaultNamespace): PeerBlobStore = EmbedPeerBlobStore(this, namespaceId)

private class EmbedPeerBlobStore(
    private val runtime: EmbeddedKdbRuntime,
    private val namespaceId: String,
) : PeerBlobStore {
    private suspend fun store() = runtime.attachmentStore(namespaceId)

    override suspend fun has(hash: KdbHash): Boolean = store().has(hash)

    override suspend fun open(hash: KdbHash): PeerBlobReader? {
        val r =
            try {
                store().open(hash)
            } catch (e: AttachmentBlobNotFoundException) {
                return null
            } catch (e: AttachmentCorruptException) {
                throw PeerBlobCorruptException(e.message ?: "corrupt blob ${hash.toHex()}", e)
            }
        return Reader(r)
    }

    override suspend fun put(source: suspend (ByteArray, Int, Int) -> Int): PeerStoredBlob {
        val stored = store().put(AttachmentSource { b, off, len -> source(b, off, len) })
        return PeerStoredBlob(stored.hash, stored.size)
    }

    override suspend fun hold(hash: KdbHash): Boolean = store().hold(hash)

    private class Reader(
        private val r: AttachmentReader,
    ) : PeerBlobReader {
        override val size: Long get() = r.size

        override suspend fun chunkLeaf(index: Long): KdbHash =
            try {
                r.chunkLeaf(index)
            } catch (e: AttachmentCorruptException) {
                throw PeerBlobCorruptException(e.message ?: "corrupt blob ${r.contentHash.toHex()}", e)
            }

        override suspend fun readAt(
            position: Long,
            buffer: ByteArray,
            offset: Int,
            length: Int,
        ): Int =
            try {
                r.readAt(position, buffer, offset, length)
            } catch (e: AttachmentCorruptException) {
                throw PeerBlobCorruptException(e.message ?: "corrupt blob ${r.contentHash.toHex()}", e)
            }

        override suspend fun close() = r.close()
    }
}

/** [namespaceId]'s snapshot bootstrap and serving for peer sync: install through [installSnapshot]
 * (durable first, in a file runtime), serve documents at the head. Pass it as
 * PeerClientConfig.snapshots / PeerHostConfig.snapshots. */
public fun EmbeddedKdbRuntime.peerSnapshots(namespaceId: String = defaultNamespace): dev.kdb.peersync.PeerSnapshots {
    val rt = this
    return object : dev.kdb.peersync.PeerSnapshots {
        override suspend fun canInstall(): Boolean = rt.canInstallSnapshot(namespaceId)

        override suspend fun install(
            root: dev.kdb.document.KdbCommit,
            documents: List<dev.kdb.document.KdbDocument>,
            attachmentTables: ByteArray?,
        ) = rt.installSnapshot(namespaceId, SnapshotState(root, documents, attachmentTables))

        override suspend fun shallowRoots(): List<KdbHash> = rt.shallowRootsOf(namespaceId)

        // A commit synced in has its documents in storage but no tree in the DAG: the tree is built
        // from storage once, checked against the commit, and kept for the pages that follow.
        override suspend fun headTree(head: dev.kdb.document.KdbCommit): dev.kdb.document.DocumentTree {
            rt.dag.getDocumentTree(head.documentTreeHash)?.let { return it }
            val entries = HashMap<dev.kdb.codec.KdbUuid, KdbHash>()
            rt.storage.scanDocuments(namespaceId, head.documentTreeHash) { batch -> for (d in batch) entries[d.id] = d.contentHash }
            val tree = dev.kdb.document.DocumentTree.build(entries)
            check(tree.treeHash == head.documentTreeHash) { "the live documents do not hash to ${head.hash.toHex()}'s tree" }
            rt.dag.putDocumentTree(tree)
            return tree
        }

        override suspend fun documentsAtHead(ids: List<dev.kdb.codec.KdbUuid>): List<dev.kdb.document.KdbDocument?> =
            // Storage reads take the tree's hash (an in-memory adapter keys its trees by it).
            rt.storage.getDocuments(namespaceId, ids, rt.dag.getCommitOrThrow(rt.dag.head()).documentTreeHash)
    }
}
