package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.AttachmentHasher
import dev.kdb.document.attachmentIdentity
import dev.kdb.document.chunkLeafHash
import dev.kdb.document.merkleTreeHash
import dev.kdb.storage.PlatformIoShim
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

/** How long [AttachmentBlobStore.collect] keeps a finished upload no commit references yet, unless told otherwise. */
public val DEFAULT_ATTACHMENT_UPLOAD_GRACE: Duration = 1.hours

/**
 * Attachment bytes for one namespace (docs/kdb-attachments-plan.md, "Storage"): one blob per
 * scheme-1 content hash, written and read as a stream of 1 MiB chunks through the platform I/O shim,
 * never held whole.
 *
 * Layout, under `ns/<namespace>/attachment-kt/`:
 *  - `u-<uuid>`: a blob's bytes, streamed under a random name while its hash is not yet known;
 *  - `h-<hash>`: the blob's pointer - a header naming its bytes segment, its size, and its chunk
 *    (leaf) hashes - written, flushed and sealed only once the bytes are, so a crash leaves either a
 *    readable blob or an unreferenced upload that [collect] removes;
 *  - `g-<hash>.<millis>`: an empty segment recording when the blob's upload grace started (below).
 *
 * Uploads are held. A writer uploads first and commits after, so a finished blob no commit references
 * yet is not garbage: [collect] keeps it for [uploadGrace] after its upload finished. The start is in
 * the hold segment's name, written before the pointer, so the hold survives a restart; a blob stored
 * before holds existed has none, and has no grace. A re-upload of a stored blob restarts the grace.
 * The hold ends as soon as a durable commit references the blob ([committed]), or a [collect] sees it
 * referenced: from then on references alone decide, so a version replaced before the next collect is
 * not kept for the rest of its grace. (A hold is its own segment, not a pointer field, so a pointer
 * never has to be rewritten - the shim cannot replace a segment atomically - and an older build,
 * which ignores segments it does not know, still reads every pointer.)
 *
 * Reads verify: [open] checks the stored leaves fold to the content hash, and every chunk is checked
 * against its leaf before a byte of it is handed out. An open reader pins its blob: [delete] and
 * [collect] defer a pinned blob's removal to the last reader's [AttachmentReader.close].
 *
 * The on-disk layout is Kotlin's own (the prefix keeps it apart from Go's attachstore segments).
 * Go's small-blob packing and binary deltas are not implemented: every blob is stored whole.
 */
public class AttachmentBlobStore(
    private val shim: PlatformIoShim,
    private val namespaceId: String,
    /** Caps one attachment; 0 is no limit. */
    public val maxBytes: Long = 0,
    /** How long [collect] keeps a finished upload no commit references yet: the longest an upload
     * may wait for its commit. Not positive: [DEFAULT_ATTACHMENT_UPLOAD_GRACE]. */
    uploadGrace: Duration = DEFAULT_ATTACHMENT_UPLOAD_GRACE,
) {
    public val uploadGrace: Duration = if (uploadGrace.isPositive()) uploadGrace else DEFAULT_ATTACHMENT_UPLOAD_GRACE

    private val prefix = "ns/$namespaceId/attachment-kt/"
    private val mutex = Mutex()
    private val pins = mutableMapOf<KdbHash, Int>()
    private val doomed = mutableMapOf<KdbHash, String>() // pinned blobs to remove on last release: hash -> bytes segment
    private val uploading = mutableSetOf<String>() // upload segments in flight, which collect must leave
    private val holds = mutableMapOf<KdbHash, Long>() // held blobs: hash -> when the grace started (epoch ms)
    private val holdSegments = mutableMapOf<KdbHash, MutableList<String>>() // their hold segments on disk
    private var holdsLoaded = false

    private fun pointerName(h: KdbHash) = prefix + "h-" + h.toHex()

    /**
     * Streams [source] into the store and returns its content hash and size. The bytes and their
     * pointer are flushed before this returns, so a commit referencing them may follow.
     */
    public suspend fun put(source: AttachmentSource): StoredAttachmentBlob {
        val upload = prefix + "u-" + KdbUuid.random()
        mutex.withLock { uploading += upload }
        val hasher = AttachmentHasher()
        val buf = ByteArray(ATTACHMENT_CHUNK_BYTES)
        var wroteAny = false
        try {
            while (true) {
                var fill = 0
                while (fill < buf.size) {
                    val n = source.read(buf, fill, buf.size - fill)
                    if (n < 0) break
                    fill += n
                }
                if (fill > 0) {
                    if (maxBytes > 0 && hasher.size + fill > maxBytes) {
                        throw AttachmentTooLargeException("attachment exceeds the size limit of $maxBytes bytes")
                    }
                    hasher.update(buf, 0, fill)
                    shim.appendToSegment(upload, if (fill == buf.size) buf else buf.copyOf(fill))
                    wroteAny = true
                }
                if (fill < buf.size) break
            }
            if (!wroteAny) shim.appendToSegment(upload, ByteArray(0))
            shim.flushSegment(upload)
            shim.sealSegment(upload)
        } catch (e: Throwable) {
            mutex.withLock { uploading -= upload }
            runCatching { shim.deleteSegment(upload) }
            throw e
        }
        val hash = hasher.finish()
        val size = hasher.size
        mutex.withLock {
            uploading -= upload
            loadHoldsLocked()
            if (readPointer(hash) != null) {
                // Already stored: the upload was a duplicate. A removal queued behind a reader is
                // called off - the blob is wanted again - and its grace restarts.
                doomed.remove(hash)
                shim.deleteSegment(upload)
                holdLocked(hash)
            } else {
                holdLocked(hash) // before the pointer: a pointer is never without its hold
                val ptr = pointerName(hash)
                runCatching { shim.deleteSegment(ptr) } // a torn pointer from a crash
                shim.appendToSegment(ptr, encodePointer(upload, size, hasher.leafHashes))
                shim.flushSegment(ptr)
                shim.sealSegment(ptr)
            }
        }
        return StoredAttachmentBlob(hash, size)
    }

    /** Whether the store holds a blob under [hash] (its pointer is intact). */
    public suspend fun has(hash: KdbHash): Boolean = mutex.withLock { readPointer(hash) != null && hash !in doomed }

    /** Opens [hash] for a verified, streaming read, pinning it until the reader is closed. */
    public suspend fun open(hash: KdbHash): AttachmentReader {
        val ptr =
            mutex.withLock {
                if (hash in doomed) null else readPointer(hash)?.also { pins[hash] = (pins[hash] ?: 0) + 1 }
            } ?: throw AttachmentBlobNotFoundException("attachment blob not stored: ${hash.toHex()}")
        if (attachmentIdentity(merkleTreeHash(ptr.leaves), ptr.size) != hash) {
            release(hash)
            throw AttachmentCorruptException("stored chunk hashes of ${hash.toHex()} do not fold to it")
        }
        return AttachmentReader(this, hash, ptr)
    }

    /** Every blob the store holds. */
    public suspend fun list(): List<KdbHash> =
        shim.listSegments(namespaceId).filter { it.startsWith(prefix + "h-") }.mapNotNull {
            runCatching { KdbHash.fromHex(it.removePrefix(prefix + "h-")) }.getOrNull()
        }

    /** Removes [hash] - at once, or when its last reader closes. */
    public suspend fun delete(hash: KdbHash) {
        mutex.withLock { deleteLocked(hash) }
    }

    private suspend fun deleteLocked(hash: KdbHash) {
        val ptr = readPointer(hash) ?: return
        releaseHoldLocked(hash)
        if ((pins[hash] ?: 0) > 0) {
            doomed[hash] = ptr.segment
            return
        }
        shim.deleteSegment(pointerName(hash))
        shim.deleteSegment(ptr.segment)
    }

    /**
     * Ends the hold on blobs a durable commit references: from here on references alone decide.
     * ([collect] also ends the hold on any blob it finds referenced.)
     */
    public suspend fun committed(hashes: Collection<KdbHash>) {
        mutex.withLock {
            loadHoldsLocked()
            for (h in hashes) releaseHoldLocked(h)
        }
    }

    /**
     * Deletes every blob [keep] rejects (deferred while read) unless it is within its upload grace
     * ([uploadGrace]), and every bytes segment no pointer names - uploads a crash interrupted.
     * Returns how many blobs were removed.
     */
    public suspend fun collect(keep: (KdbHash) -> Boolean): Int =
        mutex.withLock {
            loadHoldsLocked()
            val now = nowMillis()
            val names = shim.listSegments(namespaceId).filter { it.startsWith(prefix) }
            val referenced = mutableSetOf<String>()
            val stored = mutableSetOf<KdbHash>()
            var removed = 0
            for (name in names) {
                if (!name.startsWith(prefix + "h-")) continue
                val h = runCatching { KdbHash.fromHex(name.removePrefix(prefix + "h-")) }.getOrNull() ?: continue
                val ptr = readPointer(h)
                if (ptr == null) {
                    shim.deleteSegment(name) // torn
                    continue
                }
                val since = holds[h]
                when {
                    keep(h) -> {
                        releaseHoldLocked(h) // committed: references decide from here on
                        referenced += ptr.segment
                        stored += h
                    }
                    h in doomed -> referenced += ptr.segment
                    since != null && now - since < uploadGrace.inWholeMilliseconds -> {
                        referenced += ptr.segment // uploaded, its commit still to come
                        stored += h
                    }
                    else -> {
                        deleteLocked(h)
                        if (h in doomed) referenced += ptr.segment
                        removed++
                    }
                }
            }
            for (name in names) {
                if (name.startsWith(prefix + "u-") && name !in referenced && name !in uploading && doomed.values.none { it == name }) {
                    shim.deleteSegment(name)
                }
            }
            // A hold whose blob is gone, or that a crash left before its pointer was written.
            for (h in holdSegments.keys.toList()) if (h !in stored) releaseHoldLocked(h)
            removed
        }

    /** Reads the hold segments once. Must hold [mutex]. */
    private suspend fun loadHoldsLocked() {
        if (holdsLoaded) return
        for (name in shim.listSegments(namespaceId)) {
            if (!name.startsWith(prefix + "g-")) continue
            val (h, at) = parseHold(name) ?: continue
            holdSegments.getOrPut(h) { mutableListOf() } += name
            holds[h] = maxOf(holds[h] ?: at, at)
        }
        holdsLoaded = true
    }

    /**
     * Starts [h]'s grace now. The hold segment is rewritten only once the recorded start is half a
     * grace old, so the hold survives a restart without a write per upload of a blob uploaded often.
     * Must hold [mutex].
     */
    private suspend fun holdLocked(h: KdbHash) {
        val now = nowMillis()
        val old = holdSegments[h].orEmpty()
        val recorded = old.mapNotNull { parseHold(it)?.second }.maxOrNull()
        holds[h] = now
        if (recorded != null && now - recorded < uploadGrace.inWholeMilliseconds / 2) return
        val name = prefix + "g-" + h.toHex() + "." + now
        if (name !in old) {
            shim.appendToSegment(name, ByteArray(0))
            shim.flushSegment(name)
            shim.sealSegment(name)
        }
        for (o in old) if (o != name) runCatching { shim.deleteSegment(o) }
        holdSegments[h] = mutableListOf(name)
    }

    /** Ends [h]'s hold, in memory and on disk. Must hold [mutex]. */
    private suspend fun releaseHoldLocked(h: KdbHash) {
        holds.remove(h)
        holdSegments.remove(h)?.forEach { runCatching { shim.deleteSegment(it) } }
    }

    private fun parseHold(name: String): Pair<KdbHash, Long>? {
        val rest = name.removePrefix(prefix + "g-")
        val dot = rest.lastIndexOf('.')
        if (dot < 0) return null
        val h = runCatching { KdbHash.fromHex(rest.substring(0, dot)) }.getOrNull() ?: return null
        val at = rest.substring(dot + 1).toLongOrNull() ?: return null
        return h to at
    }

    private fun nowMillis(): Long = KdbTimestamp.now().epochMillis

    internal suspend fun release(hash: KdbHash) {
        mutex.withLock {
            val n = (pins[hash] ?: 0) - 1
            if (n > 0) {
                pins[hash] = n
                return
            }
            pins.remove(hash)
            val seg = doomed.remove(hash) ?: return
            runCatching { shim.deleteSegment(pointerName(hash)) }
            runCatching { shim.deleteSegment(seg) }
        }
    }

    internal suspend fun readChunk(
        ptr: BlobPointer,
        index: Int,
    ): ByteArray {
        val off = index.toLong() * ATTACHMENT_CHUNK_BYTES
        val len = minOf(ATTACHMENT_CHUNK_BYTES.toLong(), ptr.size - off).toInt()
        val bytes = readFully(ptr.segment, off, len)
        if (bytes.size != len || chunkLeafHash(bytes) != ptr.leaves[index]) {
            throw AttachmentCorruptException("chunk $index of ${ptr.segment} does not match its stored hash")
        }
        return bytes
    }

    private suspend fun readFully(
        segment: String,
        offset: Long,
        length: Int,
    ): ByteArray {
        val out = ByteArray(length)
        var got = 0
        while (got < length) {
            val part = shim.readFromSegment(segment, offset + got, length - got)
            if (part.isEmpty()) return out.copyOf(got)
            part.copyInto(out, got)
            got += part.size
        }
        return out
    }

    private suspend fun readPointer(hash: KdbHash): BlobPointer? {
        val name = pointerName(hash)
        return try {
            val head = readFully(name, 0, POINTER_HEADER)
            if (head.size < POINTER_HEADER || !head.copyOf(4).contentEquals(POINTER_MAGIC)) return null
            val size = be64(head, 4)
            val nameLen = be32(head, 12)
            val leafCount = be32(head, 16)
            if (nameLen < 0 || leafCount < 1 || size < 0) return null
            val rest = readFully(name, POINTER_HEADER.toLong(), nameLen + 32 * leafCount)
            if (rest.size != nameLen + 32 * leafCount) return null
            val segment = rest.copyOf(nameLen).decodeToString()
            val leaves = (0 until leafCount).map { KdbHash(rest.copyOfRange(nameLen + 32 * it, nameLen + 32 * it + 32)) }
            BlobPointer(segment, size, leaves)
        } catch (_: Exception) {
            null
        }
    }

    private fun encodePointer(
        segment: String,
        size: Long,
        leaves: List<KdbHash>,
    ): ByteArray {
        val n = segment.encodeToByteArray()
        val out = ByteArray(POINTER_HEADER + n.size + 32 * leaves.size)
        POINTER_MAGIC.copyInto(out)
        for (i in 0 until 8) out[4 + i] = (size ushr (56 - 8 * i)).toByte()
        for (i in 0 until 4) out[12 + i] = (n.size ushr (24 - 8 * i)).toByte()
        for (i in 0 until 4) out[16 + i] = (leaves.size ushr (24 - 8 * i)).toByte()
        n.copyInto(out, POINTER_HEADER)
        leaves.forEachIndexed { i, l -> l.bytes.copyInto(out, POINTER_HEADER + n.size + 32 * i) }
        return out
    }

    private companion object {
        val POINTER_MAGIC = "KAT1".encodeToByteArray()
        const val POINTER_HEADER = 20

        fun be64(
            b: ByteArray,
            off: Int,
        ): Long {
            var v = 0L
            for (i in 0 until 8) v = (v shl 8) or (b[off + i].toLong() and 0xff)
            return v
        }

        fun be32(
            b: ByteArray,
            off: Int,
        ): Int {
            var v = 0
            for (i in 0 until 4) v = (v shl 8) or (b[off + i].toInt() and 0xff)
            return v
        }
    }
}

internal class BlobPointer(
    val segment: String,
    val size: Long,
    val leaves: List<KdbHash>,
)

/** A stored blob: its scheme-1 content hash and its length. */
public data class StoredAttachmentBlob(
    val hash: KdbHash,
    val size: Long,
)

/** Bytes to upload, pulled in pieces: [read] fills up to `length` bytes and returns how many, or -1
 * at the end. */
public fun interface AttachmentSource {
    public suspend fun read(
        buffer: ByteArray,
        offset: Int,
        length: Int,
    ): Int

    public companion object {
        /** A source over bytes already in memory. */
        public fun of(bytes: ByteArray): AttachmentSource {
            var pos = 0
            return AttachmentSource { buf, off, len ->
                if (pos >= bytes.size) {
                    -1
                } else {
                    val n = minOf(len, bytes.size - pos)
                    bytes.copyInto(buf, off, pos, pos + n)
                    pos += n
                    n
                }
            }
        }
    }
}

/**
 * A streaming read of one blob: sequential ([read]) or random access ([readAt]), each chunk verified
 * against its stored hash before any of it is returned. Pins the blob until [close].
 */
public class AttachmentReader internal constructor(
    private val store: AttachmentBlobStore,
    public val contentHash: KdbHash,
    private val ptr: BlobPointer,
) {
    public val size: Long get() = ptr.size
    private var position = 0L
    private var cachedIndex = -1
    private var cached = ByteArray(0)
    private var closed = false

    private suspend fun chunk(index: Int): ByteArray {
        check(!closed) { "attachment reader is closed" }
        if (index != cachedIndex) {
            cached = store.readChunk(ptr, index)
            cachedIndex = index
        }
        return cached
    }

    /** Reads up to [length] bytes at [pos] into [buffer]; returns how many, or -1 at the end. */
    public suspend fun readAt(
        pos: Long,
        buffer: ByteArray,
        offset: Int = 0,
        length: Int = buffer.size - offset,
    ): Int {
        if (pos >= size) return -1
        var done = 0
        var p = pos
        while (done < length && p < size) {
            val index = (p / ATTACHMENT_CHUNK_BYTES).toInt()
            val c = chunk(index)
            val inChunk = (p - index.toLong() * ATTACHMENT_CHUNK_BYTES).toInt()
            val n = minOf(length - done, c.size - inChunk)
            c.copyInto(buffer, offset + done, inChunk, inChunk + n)
            done += n
            p += n
        }
        return done
    }

    /** Reads the next bytes; -1 at the end. */
    public suspend fun read(
        buffer: ByteArray,
        offset: Int = 0,
        length: Int = buffer.size - offset,
    ): Int {
        val n = readAt(position, buffer, offset, length)
        if (n > 0) position += n
        return n
    }

    /** Reads everything left - for small attachments and tests. */
    public suspend fun readAll(): ByteArray {
        val left = size - position
        require(left <= Int.MAX_VALUE) { "attachment too large to read whole" }
        val out = ByteArray(left.toInt())
        var off = 0
        while (off < out.size) {
            val n = read(out, off, out.size - off)
            if (n <= 0) break
            off += n
        }
        return out
    }

    /** Releases the pin. Idempotent. */
    public suspend fun close() {
        if (closed) return
        closed = true
        store.release(contentHash)
    }
}

public class AttachmentBlobNotFoundException(
    message: String,
) : NoSuchElementException(message)

public class AttachmentCorruptException(
    message: String,
) : IllegalStateException(message)

public class AttachmentTooLargeException(
    message: String,
) : IllegalArgumentException(message)
