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

        override suspend fun chunkLeaf(index: Long): KdbHash = r.chunkLeaf(index)

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
