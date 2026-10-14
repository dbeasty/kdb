package dev.kdb.replica

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
    /** When the blob was last written or read here (ms since the epoch), for eviction. */
    val used: Long = 0,
)

/** What the blob cache holds. */
public data class BlobCacheStats(
    /** Bytes of every blob held, pinned or not. */
    val bytes: Long,
    val blobs: Int,
    /** Bytes of pinned blobs: written here, not yet uploaded - never evicted. */
    val pinnedBytes: Long,
    /** The bound on unpinned bytes; 0 for none. */
    val limit: Long,
)

/** Default bound on the bytes of fetched attachments kept. */
public const val DEFAULT_BLOB_CACHE_BYTES: Long = 256L shl 20

/** A read refreshes a blob's recency at most this often, so reading is not a write each time. */
private const val TOUCH_EVERY_MS = 60_000L

private fun nowMs(): Long = KdbTimestamp.now().epochMillis

/** The bytes a blob's held chunks take (every chunk is 1 MiB but the last). */
private fun heldBytes(m: BlobMeta): Long {
    val n = chunkCount(m.size)
    return m.have.sumOf { i -> if (i == n - 1) m.size - i * ATTACHMENT_CHUNK_BYTES else ATTACHMENT_CHUNK_BYTES.toLong() }
}

internal fun chunkCount(size: Long): Long = maxOf(1L, (size + ATTACHMENT_CHUNK_BYTES - 1) / ATTACHMENT_CHUNK_BYTES)

/**
 * Attachment bytes on the replica (docs/kdb-client-replica-plan.md §9; packages/kdb-replica/src/blobs.ts):
 * fetched lazily, on first read, by BLOB_FETCH - every chunk checked against the blob's content hash
 * with its RFC 6962 audit path - and kept by content hash in 1 MiB chunks, so a blob shared by
 * several attachments is stored once and an interrupted read resumes. A blob the replica wrote is
 * pinned until the host holds it. The store layout is the TS replica's (`bm/<hash>`, `b/<hash>/<i>`).
 *
 * Fetched bytes are a cache, not the replica's data: the host keeps every blob a commit references,
 * so what is evicted here is fetched again on the next read. The cache is bounded by bytes
 * ([cacheBytes]; 0 for no bound) and evicts the least recently used blobs first - never a pinned
 * one, whose bytes exist nowhere else yet. It is trimmed after every fetch and every unpin, and on
 * demand ([evict]).
 */
public class BlobStore(private val store: ReplicaStore, cacheBytes: Long = DEFAULT_BLOB_CACHE_BYTES) {
    private val limit = maxOf(0L, cacheBytes)
    private val evicting = Mutex()

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
        batch[metaKey(hash)] = encodeMeta(BlobMeta(data.size.toLong(), (0 until n).toList(), pinned = true, used = nowMs()))
        store.apply(batch)
        return hash
    }

    /** The host holds [hash] now: it becomes a cache entry like any fetched blob. */
    public suspend fun unpin(hash: String) {
        val m = meta(hash) ?: return
        if (m.pinned) {
            store.apply(Batch().apply { put(metaKey(hash), encodeMeta(m.copy(pinned = false, used = nowMs()))) })
            trim()
        }
    }

    private suspend fun metas(): List<Pair<String, BlobMeta>> =
        store.scan("bm/", prefixEnd("bm/")).map { (k, v) ->
            k.removePrefix("bm/") to metaJson.decodeFromString(BlobMeta.serializer(), v.decodeToString())
        }

    /** What the cache holds. */
    public suspend fun stats(): BlobCacheStats {
        var bytes = 0L
        var pinnedBytes = 0L
        val all = metas()
        for ((_, m) in all) {
            val held = heldBytes(m)
            bytes += held
            if (m.pinned) pinnedBytes += held
        }
        return BlobCacheStats(bytes, all.size, pinnedBytes, limit)
    }

    /**
     * Evicts unpinned blobs, least recently used first, until the unpinned bytes are at most [target]
     * (default: the cache's bound; with no bound, nothing). Returns the bytes freed.
     */
    public suspend fun evict(target: Long? = null): Long =
        evicting.withLock { evictLocked(target ?: if (limit > 0) limit else Long.MAX_VALUE, null) }

    /** Trims to the bound, keeping [keep] - the blob just fetched, which its reader is about to use. */
    internal suspend fun trim(keep: String? = null): Long =
        if (limit == 0L) 0L else evicting.withLock { evictLocked(limit, keep) }

    private suspend fun evictLocked(target: Long, keep: String?): Long {
        val unpinnedMetas = metas().filter { !it.second.pinned }
        var unpinned = unpinnedMetas.sumOf { heldBytes(it.second) }
        if (unpinned <= target) return 0L
        var freed = 0L
        for ((hash, m) in unpinnedMetas.filter { it.first != keep }.sortedBy { it.second.used }) {
            if (unpinned <= target) break
            val batch = Batch()
            batch[metaKey(hash)] = null
            for (i in m.have) batch[chunkKey(hash, i)] = null
            store.apply(batch)
            val n = heldBytes(m)
            unpinned -= n
            freed += n
        }
        return freed
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
            val c = chunk(hash, i) ?: return null // evicted under the read: the caller fetches it again
            c.copyInto(out, off)
            off += c.size
        }
        if (off.toLong() != m.size) throw BlobUnavailableException("kdb replica: blob $hash is held damaged")
        val now = nowMs()
        if (!m.pinned && now - m.used > TOUCH_EVERY_MS) store.apply(Batch().apply { put(metaKey(hash), encodeMeta(m.copy(used = now))) })
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
            meta = meta.copy(size = size, have = have.sorted(), used = nowMs())
            batch[metaKey(hash)] = encodeMeta(meta)
            store.apply(batch)
        }
        val out = read(hash) ?: throw BlobUnavailableException("kdb replica: blob $hash is incomplete")
        trim(hash)
        return out
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
