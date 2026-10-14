package dev.kdb.replica

import dev.kdb.codec.KdbHash
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.attachmentContentHash
import dev.kdb.document.attachmentIdentity
import dev.kdb.document.chunkLeafHash
import dev.kdb.document.verifyMerkleInclusion
import dev.kdb.wire.WireBlobFetchEntry
import kotlinx.serialization.Serializable

/** An attachment's bytes could not be had: not held here, refused by the host, or failing verification. */
public class BlobUnavailableException(message: String) : RuntimeException(message)

@Serializable
internal data class BlobMeta(
    val size: Long,
    /** Chunks held, by index. */
    val have: List<Long> = emptyList(),
    /** Written here and not yet confirmed by the host. */
    val pinned: Boolean = false,
)

internal fun chunkCount(size: Long): Long = maxOf(1L, (size + ATTACHMENT_CHUNK_BYTES - 1) / ATTACHMENT_CHUNK_BYTES)

/**
 * Attachment bytes on the replica (docs/kdb-client-replica-plan.md §9; packages/kdb-replica/src/blobs.ts):
 * fetched lazily, on first read, by BLOB_FETCH - every chunk checked against the blob's content hash
 * with its RFC 6962 audit path - and kept by content hash in 1 MiB chunks, so a blob shared by
 * several attachments is stored once and an interrupted read resumes. A blob the replica wrote is
 * pinned until the host holds it. The store layout is the TS replica's (`bm/<hash>`, `b/<hash>/<i>`).
 */
public class BlobStore(private val store: ReplicaStore) {
    private fun metaKey(hash: String) = "bm/$hash"

    private fun chunkKey(hash: String, i: Long) = "b/$hash/${i.toString().padStart(8, '0')}"

    private fun encodeMeta(m: BlobMeta) = metaJson.encodeToString(BlobMeta.serializer(), m).encodeToByteArray()

    internal suspend fun meta(hash: String): BlobMeta? =
        store.get(metaKey(hash))?.let { metaJson.decodeFromString(BlobMeta.serializer(), it.decodeToString()) }

    /** Whether every chunk of [hash] is held here. */
    public suspend fun complete(hash: String): Boolean = meta(hash)?.let { it.have.size.toLong() == chunkCount(it.size) } ?: false

    /** Whether [hash] was written here and the host has not confirmed it yet. */
    public suspend fun pinned(hash: String): Boolean = meta(hash)?.pinned ?: false

    /** Stores bytes written on this replica, pinned until the host has them. Returns the content hash. */
    public suspend fun putLocal(data: ByteArray): String {
        val hash = attachmentContentHash(data).toHex()
        val n = chunkCount(data.size.toLong())
        val batch = Batch()
        for (i in 0 until n) {
            val from = (i * ATTACHMENT_CHUNK_BYTES).toInt()
            batch[chunkKey(hash, i)] = data.copyOfRange(from, minOf(data.size, from + ATTACHMENT_CHUNK_BYTES))
        }
        batch[metaKey(hash)] = encodeMeta(BlobMeta(data.size.toLong(), (0 until n).toList(), pinned = true))
        store.apply(batch)
        return hash
    }

    public suspend fun unpin(hash: String) {
        val m = meta(hash) ?: return
        if (m.pinned) store.apply(Batch().apply { put(metaKey(hash), encodeMeta(m.copy(pinned = false))) })
    }

    internal suspend fun chunk(hash: String, i: Long): ByteArray? = store.get(chunkKey(hash, i))

    /** The whole blob, when every chunk is held; null otherwise. */
    public suspend fun read(hash: String): ByteArray? {
        val m = meta(hash) ?: return null
        val n = chunkCount(m.size)
        if (m.have.size.toLong() != n) return null
        if (m.size > Int.MAX_VALUE - 8) throw BlobUnavailableException("kdb replica: blob $hash is too large to read whole")
        val out = ByteArray(m.size.toInt())
        var off = 0
        for (i in 0 until n) {
            val c = chunk(hash, i) ?: return null
            c.copyInto(out, off)
            off += c.size
        }
        if (off.toLong() != m.size) throw BlobUnavailableException("kdb replica: blob $hash is held damaged")
        return out
    }

    /**
     * Fetches what is missing of the blob [hash] - attachment ([docId], [attachmentId]) as of
     * [commitHex] - from the host, verifying each chunk, and returns the whole blob.
     */
    public suspend fun fetch(
        conn: PeerConnection,
        namespace: String,
        docId: String,
        attachmentId: String,
        commitHex: String,
        hash: String,
        size: Long,
    ): ByteArray {
        val n = chunkCount(size)
        var meta = meta(hash) ?: BlobMeta(size)
        val have = meta.have.toMutableSet()
        val want = KdbHash.fromBytes(hex32(hash))
        var guard = 0L
        while (have.size.toLong() < n) {
            if (guard++ > n + 16) throw BlobUnavailableException("kdb replica: blob $hash did not arrive")
            var first = 0L
            while (first in have) first++
            val entry =
                WireBlobFetchEntry(
                    docId = parseUuid(docId),
                    attachmentId = parseUuid(attachmentId),
                    commitHex = commitHex,
                    hash = want,
                    offset = first * ATTACHMENT_CHUNK_BYTES,
                    maxBytes = 0,
                )
            val res = conn.blobFetch(namespace, listOf(entry))
            if (!res.error.isNullOrEmpty()) {
                throw BlobUnavailableException("kdb replica: blob fetch refused: ${res.errorCode}: ${res.error}")
            }
            val part = res.parts.firstOrNull()
            if (part == null) {
                if (res.more) continue
                throw BlobUnavailableException("kdb replica: the host sent no part of blob $hash")
            }
            if (!part.error.isNullOrEmpty()) throw BlobUnavailableException("kdb replica: blob $hash: ${part.errorCode}: ${part.error}")
            // The part's tree and size must be the blob's identity; every chunk must prove into the tree.
            val tree = part.tree ?: throw BlobUnavailableException("kdb replica: blob $hash: the host sent no tree")
            if (part.hash != want || part.size != size || attachmentIdentity(tree, part.size) != want) {
                throw BlobUnavailableException("kdb replica: blob $hash: the host's tree and size do not make its hash")
            }
            if (part.chunks.isEmpty()) throw BlobUnavailableException("kdb replica: blob $hash: an empty part")
            val batch = Batch()
            var off = 0
            for (proof in part.chunks) {
                val len = maxOf(0L, minOf(ATTACHMENT_CHUNK_BYTES.toLong(), size - proof.leaf * ATTACHMENT_CHUNK_BYTES)).toInt()
                if (off + len > part.data.size) throw BlobUnavailableException("kdb replica: blob $hash: a part shorter than its chunks")
                val data = part.data.copyOfRange(off, off + len)
                off += len
                if (!verifyMerkleInclusion(chunkLeafHash(data), proof.leaf, n, proof.path, tree)) {
                    throw BlobUnavailableException("kdb replica: blob $hash: chunk ${proof.leaf} fails its proof")
                }
                batch[chunkKey(hash, proof.leaf)] = data
                have.add(proof.leaf)
            }
            meta = meta.copy(size = size, have = have.sorted())
            batch[metaKey(hash)] = encodeMeta(meta)
            store.apply(batch)
        }
        return read(hash) ?: throw BlobUnavailableException("kdb replica: blob $hash is incomplete")
    }

    /**
     * Uploads a blob held here on a sync session, before the commit or write-back that references
     * it: the first chunk names the blob (`expect`), so a host that already holds it says so at once.
     */
    public suspend fun upload(conn: PeerConnection, namespace: String, hash: String) {
        val m = meta(hash) ?: throw BlobUnavailableException("kdb replica: blob $hash to upload is not held here")
        val n = chunkCount(m.size)
        val want = KdbHash.fromBytes(hex32(hash))
        var uploadId = ""
        var offset = 0L
        var i = 0L
        while (true) {
            val data = chunk(hash, i) ?: if (m.size == 0L) ByteArray(0) else throw BlobUnavailableException("kdb replica: blob $hash lacks chunk $i")
            val final = i == n - 1
            val r = conn.upload(namespace, uploadId, offset, data, final, if (uploadId.isEmpty()) want else null)
            if (!r.error.isNullOrEmpty()) throw BlobUnavailableException("kdb replica: upload of $hash refused: ${r.errorCode}: ${r.error}")
            val got = r.hash
            if (got != null) {
                if (got != want || r.size != m.size) throw BlobUnavailableException("kdb replica: the host stored ${got.toHex()}, not $hash")
                return
            }
            if (final) throw BlobUnavailableException("kdb replica: the host did not confirm upload of $hash")
            uploadId = r.uploadId
            offset = r.received
            if (offset % ATTACHMENT_CHUNK_BYTES != 0L) throw BlobUnavailableException("kdb replica: the host received part of a chunk")
            i = offset / ATTACHMENT_CHUNK_BYTES
        }
    }
}
