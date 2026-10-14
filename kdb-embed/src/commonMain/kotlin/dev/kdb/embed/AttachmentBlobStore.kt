package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.AttachmentHasher
import dev.kdb.document.MerkleBuilder
import dev.kdb.document.attachmentContentHash
import dev.kdb.document.attachmentIdentity
import dev.kdb.document.chunkLeafHash
import dev.kdb.document.kdbSha256
import dev.kdb.storage.PlatformIoShim
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

/** How long [AttachmentBlobStore.collect] keeps a finished upload no commit references yet, unless told otherwise. */
public val DEFAULT_ATTACHMENT_UPLOAD_GRACE: Duration = 1.hours

/** How long an [AttachmentReader] may hold its pins, unless told otherwise. */
public val DEFAULT_ATTACHMENT_READ_LEASE: Duration = 1.hours

/** Largest blob kept inline in its pointer, unless told otherwise (never more than one chunk). */
public const val DEFAULT_ATTACHMENT_SMALL_MAX_BYTES: Long = 256L shl 10

/** Largest version [AttachmentBlobStore.storeAsDelta] encodes, unless told otherwise. */
public const val DEFAULT_ATTACHMENT_DELTA_MAX_BYTES: Long = 256L shl 20

/** Deepest delta chain a read goes through, unless told otherwise. */
public const val DEFAULT_ATTACHMENT_DELTA_MAX_DEPTH: Int = 8

/**
 * Attachment bytes for one namespace (docs/kdb-attachments-plan.md, "Storage") - Go's
 * go/kdb/storage/attachstore, segment for segment, so a data directory written by either build is
 * read and maintained by the other. One blob per scheme-1 content hash, written and read as a
 * stream of 1 MiB chunks through the platform I/O shim, never held whole.
 *
 * Layout, under `ns/<namespace>/attachment/`:
 *  - `u-<uuid>`: a blob's bytes (whole, or a delta), or its chunk leaf hashes (32 bytes each),
 *    streamed under a random name while the hash is not yet known;
 *  - `h-<hash>.<generation>`: the blob's pointer (KDBATP3; KDBATP2 is read) naming its bytes and
 *    leaves segments, its size, when its upload finished, and for a delta its base - or, for a blob
 *    up to [smallMaxBytes], holding the bytes inline. A pointer is replaced (whole -> delta, a new
 *    finish time) by writing the next generation before the old one goes, so a crash leaves a
 *    readable blob;
 *  - `m-<generation>`: the mark ([mark]), the heads delta compaction last went through;
 *  - `r-<generation>`: Go's reference index for its GC, which this build neither reads nor writes,
 *    and [collect] leaves alone.
 *
 * Readers pin. [open] pins every segment the read resolves to - bytes, leaves, a delta's whole base
 * chain - and [AttachmentReader.close] releases them; [storeAsDelta] and [collect] defer deleting a
 * pinned segment to its last release. Pins are leases ([readLease]): a reader stalled past its lease
 * is released by the next [collect] and fails with [AttachmentReadLeaseExpiredException].
 *
 * Uploads are held: a finished blob no commit references yet is kept by [collect] for [uploadGrace]
 * after its upload finished (recorded in its pointer, so the hold survives a restart). [hold]
 * restarts the grace; [committed], or a [collect] that sees the blob referenced, ends it.
 *
 * Reads verify: a leaves segment is checked to fold to the content hash once, an inline blob when
 * its pointer is read, and every chunk against its leaf before a byte of it is handed out.
 *
 * Not carried over from Go: small blobs kept in the ordinary blob store (no build that writes the
 * current format ever put them there) and parallel chunk hashing.
 */
public class AttachmentBlobStore(
    private val shim: PlatformIoShim,
    private val namespaceId: String,
    /** Caps one attachment; 0 is no limit. */
    public val maxBytes: Long = 0,
    /** How long [collect] keeps a finished upload no commit references yet: the longest an upload
     * may wait for its commit. Not positive: [DEFAULT_ATTACHMENT_UPLOAD_GRACE]. */
    uploadGrace: Duration = DEFAULT_ATTACHMENT_UPLOAD_GRACE,
    /** How long a reader may hold its pins. Not positive: [DEFAULT_ATTACHMENT_READ_LEASE]. */
    readLease: Duration = DEFAULT_ATTACHMENT_READ_LEASE,
    /** Largest blob kept inline in its pointer. Not positive: the default; capped at one chunk. */
    smallMaxBytes: Long = DEFAULT_ATTACHMENT_SMALL_MAX_BYTES,
    /** Largest version [storeAsDelta] encodes. Not positive: the default. */
    deltaMaxBytes: Long = DEFAULT_ATTACHMENT_DELTA_MAX_BYTES,
    /** Deepest delta chain. Not positive: the default. */
    deltaMaxDepth: Int = DEFAULT_ATTACHMENT_DELTA_MAX_DEPTH,
) {
    public val uploadGrace: Duration = if (uploadGrace.isPositive()) uploadGrace else DEFAULT_ATTACHMENT_UPLOAD_GRACE
    public val readLease: Duration = if (readLease.isPositive()) readLease else DEFAULT_ATTACHMENT_READ_LEASE
    public val smallMaxBytes: Long =
        (if (smallMaxBytes > 0) smallMaxBytes else DEFAULT_ATTACHMENT_SMALL_MAX_BYTES).coerceAtMost(ATTACHMENT_CHUNK_BYTES.toLong())
    public val deltaMaxBytes: Long = if (deltaMaxBytes > 0) deltaMaxBytes else DEFAULT_ATTACHMENT_DELTA_MAX_BYTES
    public val deltaMaxDepth: Int = if (deltaMaxDepth > 0) deltaMaxDepth else DEFAULT_ATTACHMENT_DELTA_MAX_DEPTH

    /** The prefix of every segment this store writes - Go's attachstore.SegmentPrefix. */
    public val prefix: String = "ns/$namespaceId/attachment/"

    private val mutex = Mutex()
    private var loaded = false
    private val pointers = HashMap<KdbHash, Pointer>()
    private val uploads = HashSet<String>() // segments being written, which collect must leave alone
    private val held = HashMap<KdbHash, Long>() // finished blobs no collect has seen referenced, since when (epoch ns)
    private val verified = HashSet<String>() // leaves segments known to fold to their root
    private val pinned = HashMap<String, Int>() // segment -> open readers using it
    private val deferred = HashSet<String>() // segments to delete once unpinned
    private val readers = HashSet<AttachmentReader>()
    private var mark: List<KdbHash> = emptyList()
    private var markSeg = ""
    private var markGen = 0

    /** What grace and lease times are read from, in epoch ns. Tests set it to step time. */
    internal var clock: () -> Long = ::nowNanos

    // ---- pointers ----

    internal class Pointer(
        val gen: Int,
        val name: String,
        val segment: String,
        val leaves: String,
        val size: Long,
        val delta: Boolean,
        val deltaV2: Boolean,
        val base: KdbHash?,
        val inline: ByteArray?,
        /** When the upload last finished (or was last held), epoch nanoseconds; 0 for none. */
        val finished: Long,
    ) {
        fun copy(
            gen: Int = this.gen,
            name: String = this.name,
            finished: Long = this.finished,
        ) = Pointer(gen, name, segment, leaves, size, delta, deltaV2, base, inline, finished)
    }

    private fun pointerName(
        h: KdbHash,
        gen: Int,
    ) = prefix + "h-" + h.toHex() + "." + gen.toString().padStart(6, '0')

    private fun markName(gen: Int) = prefix + "m-" + gen.toString().padStart(6, '0')

    /** Reads every pointer and the mark once. Must hold [mutex]. */
    private suspend fun loadLocked() {
        if (loaded) return
        var bestMark = -1
        for (name in shim.listSegments(namespaceId)) {
            if (name.startsWith(prefix + "m-")) {
                val g = name.removePrefix(prefix + "m-").toIntOrNull() ?: continue
                if (g > markGen) markGen = g
                val commits = readMark(name) ?: continue
                if (g > bestMark) {
                    bestMark = g
                    markSeg = name
                    mark = commits
                }
                continue
            }
            if (!name.startsWith(prefix + "h-")) continue
            val (h, p) = readPointer(name) ?: continue // torn by a crash: collect retires it
            val cur = pointers[h]
            if (cur == null || p.gen > cur.gen) pointers[h] = p
        }
        for ((h, p) in pointers) if (p.finished != 0L) held[h] = p.finished
        loaded = true
    }

    private suspend fun readPointer(name: String): Pair<KdbHash, Pointer>? {
        val base = name.removePrefix(prefix + "h-")
        val dot = base.lastIndexOf('.')
        if (dot < 0) return null
        val h = runCatching { KdbHash.fromHex(base.substring(0, dot)) }.getOrNull() ?: return null
        val gen = base.substring(dot + 1).toIntOrNull() ?: return null
        var raw = runCatching { shim.readFromSegment(name, 0, 4096) }.getOrNull() ?: return null
        if (raw.size < POINTER_MAGIC.length) return null
        var header = POINTER_MAGIC.length + 1 + 8
        when (raw.copyOf(POINTER_MAGIC.length).decodeToString()) {
            POINTER_MAGIC -> header += 8
            POINTER_MAGIC_V2 -> {}
            else -> return null
        }
        if (raw.size < header + 2 + 4) return null
        val flags = raw[POINTER_MAGIC.length].toInt() and 0xff
        if (flags and FLAG_INLINE != 0) {
            val n = be64(raw, POINTER_MAGIC.length + 1)
            if (n < 0 || n > 64L shl 20) return null
            raw = runCatching { shim.readFromSegment(name, 0, header + 2 + n.toInt() + 4) }.getOrNull() ?: return null
        }
        if (raw.size < 4) return null
        val body = raw.copyOf(raw.size - 4)
        if (!kdbSha256(body).copyOf(4).contentEquals(raw.copyOfRange(raw.size - 4, raw.size))) return null
        var off = POINTER_MAGIC.length + 1
        val size = be64(body, off)
        off += 8
        var finished = 0L
        if (off < header) {
            val t = be64(body, off)
            if (t > 0) finished = t
            off += 8
        }
        val names = arrayOfNulls<String>(2)
        for (k in 0..1) {
            var l = 0L
            var shift = 0
            while (true) {
                if (off >= body.size || shift > 63) return null
                val b = body[off++].toInt() and 0xff
                l = l or ((b and 0x7f).toLong() shl shift)
                if (b < 0x80) break
                shift += 7
            }
            if (l < 0 || l > body.size - off) return null
            names[k] = body.copyOfRange(off, off + l.toInt()).decodeToString()
            off += l.toInt()
        }
        val delta = flags and FLAG_DELTA != 0
        if (flags and FLAG_INLINE != 0) {
            val inline = body.copyOfRange(off, body.size)
            if (inline.size.toLong() != size || attachmentContentHash(inline) != h) return null
            return h to Pointer(gen, name, names[0]!!, names[1]!!, size, false, false, null, inline, finished)
        }
        var baseHash: KdbHash? = null
        if (delta) {
            if (body.size - off != 32) return null
            baseHash = KdbHash(body.copyOfRange(off, off + 32))
        }
        return h to Pointer(gen, name, names[0]!!, names[1]!!, size, delta, delta && flags and FLAG_DELTA_V2 != 0, baseHash, null, finished)
    }

    internal fun encodePointer(p: Pointer): ByteArray {
        val names = listOf(p.segment, p.leaves).map { it.encodeToByteArray() }
        val inline = p.inline ?: ByteArray(0)
        val out = ByteArray(POINTER_MAGIC.length + 17 + names.sumOf { it.size + 5 } + (if (p.delta) 32 else 0) + inline.size)
        var n = 0
        POINTER_MAGIC.encodeToByteArray().copyInto(out)
        n += POINTER_MAGIC.length
        var flags = 0
        if (p.delta) flags = flags or FLAG_DELTA
        if (p.deltaV2) flags = flags or FLAG_DELTA_V2
        if (p.inline != null) flags = flags or FLAG_INLINE
        out[n++] = flags.toByte()
        for (v in longArrayOf(p.size, p.finished)) for (i in 0 until 8) out[n++] = (v ushr (56 - 8 * i)).toByte()
        for (b in names) {
            var v = b.size
            while (v >= 0x80) {
                out[n++] = ((v and 0x7f) or 0x80).toByte()
                v = v ushr 7
            }
            out[n++] = v.toByte()
            b.copyInto(out, n)
            n += b.size
        }
        if (p.delta) {
            p.base!!.bytes.copyInto(out, n)
            n += 32
        }
        inline.copyInto(out, n)
        n += inline.size
        val body = out.copyOf(n)
        return body + kdbSha256(body).copyOf(4)
    }

    /** Makes [p] the durable pointer for [h], the generation after the current one (whose finish
     * time it keeps when it has none). Must hold [mutex]. */
    private suspend fun writePointerLocked(
        h: KdbHash,
        p0: Pointer,
    ): Pointer {
        var p = p0
        pointers[h]?.let { cur -> p = p.copy(gen = cur.gen + 1, finished = if (p.finished == 0L) cur.finished else p.finished) }
        p = p.copy(name = pointerName(h, p.gen))
        writeSegment(p.name, encodePointer(p))
        return p
    }

    private suspend fun writeSegment(
        name: String,
        data: ByteArray,
    ) {
        shim.appendToSegment(name, data)
        shim.flushSegment(name)
        shim.sealSegment(name)
    }

    private fun newSegmentName() = prefix + "u-" + KdbUuid.random()

    // ---- writing ----

    /**
     * Streams [source] into the store and returns its content hash and size. The bytes and their
     * pointer are durable before this returns, so a commit referencing them may follow; the blob is
     * held for [uploadGrace] until one does. A blob already stored is not stored twice.
     */
    public suspend fun put(source: AttachmentSource): StoredAttachmentBlob {
        val up = begin()
        val buf = ByteArray(64 shl 10)
        try {
            while (true) {
                val n = source.read(buf, 0, buf.size)
                if (n < 0) break
                if (n > 0) up.write(buf, 0, n)
            }
        } catch (e: Throwable) {
            up.abort()
            throw e
        }
        return up.finish()
    }

    /** Starts an upload: [AttachmentUpload.write] the bytes, then [AttachmentUpload.finish] (or abort). */
    public fun begin(): AttachmentUpload = AttachmentUpload(this)

    internal suspend fun startSegment(): String {
        val name = newSegmentName()
        mutex.withLock { uploads += name }
        return name
    }

    internal suspend fun appendTo(
        segment: String,
        bytes: ByteArray,
    ) {
        shim.appendToSegment(segment, bytes)
    }

    internal suspend fun abandon(segment: String) {
        runCatching { shim.deleteSegment(segment) }
        mutex.withLock { uploads -= segment }
    }

    /** Makes a finished upload durable: its bytes inline ([segment] null) or in [segment]. */
    internal suspend fun finishUpload(
        hash: KdbHash,
        size: Long,
        segment: String?,
        last: ByteArray,
        leaves: List<KdbHash>,
    ): StoredAttachmentBlob {
        val info = StoredAttachmentBlob(hash, size)
        if (size == 0L) {
            segment?.let { abandon(it) }
            return info // never stored: see EMPTY_HASH
        }
        if (segment == null) {
            // Small: the bytes go in the pointer.
            mutex.withLock {
                loadLocked()
                if (hash in pointers) {
                    holdLocked(hash, clock())
                } else {
                    val now = clock()
                    pointers[hash] = writePointerLocked(hash, Pointer(0, "", "", "", size, false, false, null, last, now))
                    held[hash] = now
                }
            }
            return info
        }
        val leavesSeg: String
        try {
            shim.flushSegment(segment)
            shim.sealSegment(segment)
            leavesSeg = writeLeaves(leaves)
        } catch (e: Throwable) {
            abandon(segment)
            throw e
        }
        mutex.withLock {
            uploads -= segment
            loadLocked()
            if (hash in pointers) {
                runCatching { shim.deleteSegment(segment) } // already stored: keep the one copy
                runCatching { shim.deleteSegment(leavesSeg) }
                holdLocked(hash, clock())
            } else {
                val now = clock()
                pointers[hash] = writePointerLocked(hash, Pointer(0, "", segment, leavesSeg, size, false, false, null, null, now))
                held[hash] = now
                verified += leavesSeg
            }
        }
        return info
    }

    private suspend fun writeLeaves(leaves: List<KdbHash>): String {
        val name = newSegmentName()
        mutex.withLock { uploads += name }
        try {
            val perWrite = ATTACHMENT_CHUNK_BYTES / 32
            var i = 0
            while (i < leaves.size) {
                val end = minOf(i + perWrite, leaves.size)
                val b = ByteArray((end - i) * 32)
                for (k in i until end) leaves[k].bytes.copyInto(b, (k - i) * 32)
                shim.appendToSegment(name, b)
                i = end
            }
            shim.flushSegment(name)
            shim.sealSegment(name)
            return name
        } finally {
            mutex.withLock { uploads -= name }
        }
    }

    /**
     * Restarts [hash]'s upload grace, as if it had just been uploaded: for a client about to commit
     * a reference to a blob it did not upload because the store already holds it (a wire upload's
     * Expect). Reports whether the store holds it.
     */
    public suspend fun hold(hash: KdbHash): Boolean =
        mutex.withLock {
            loadLocked()
            if (hash !in pointers) return@withLock false
            holdLocked(hash, clock())
            true
        }

    /** Starts [h]'s grace at [now]. The pointer is rewritten only once its recorded time is half a
     * grace old. Must hold [mutex]; [h] must have a pointer. */
    private suspend fun holdLocked(
        h: KdbHash,
        now: Long,
    ) {
        held[h] = now
        val cur = pointers.getValue(h)
        if (now - cur.finished < uploadGrace.inWholeNanoseconds / 2) return
        pointers[h] = writePointerLocked(h, cur.copy(finished = now)) // collect retires the old generation
    }

    /**
     * Ends the hold on blobs a durable commit references: from here on references alone decide.
     * ([collect] also ends the hold on any blob it finds referenced.)
     */
    public suspend fun committed(hashes: Collection<KdbHash>) {
        mutex.withLock {
            loadLocked()
            for (h in hashes) held.remove(h)
        }
    }

    // ---- reading ----

    /** Whether the store holds a blob under [hash]. The empty blob is always held. */
    public suspend fun has(hash: KdbHash): Boolean {
        if (hash == EMPTY_HASH) return true
        return mutex.withLock {
            loadLocked()
            hash in pointers
        }
    }

    /** Every blob the store holds, sorted by hash. */
    public suspend fun list(): List<KdbHash> =
        mutex.withLock {
            loadLocked()
            pointers.keys.sortedBy { it.toHex() }
        }

    /** [hash]'s pointer chain, [hash] first, then its delta bases; null for a blob not held. Must hold [mutex]. */
    private fun resolveLocked(hash: KdbHash): List<Pair<KdbHash, Pointer>>? {
        val chain = ArrayList<Pair<KdbHash, Pointer>>()
        var at = hash
        while (true) {
            val p = pointers[at]
            if (p == null) {
                if (chain.isEmpty()) return null
                throw AttachmentBlobNotFoundException("attachment blob not stored: delta base ${at.toHex()} of ${hash.toHex()}")
            }
            chain += at to p
            if (!p.delta) return chain
            if (chain.size > deltaMaxDepth + 1) {
                throw AttachmentCorruptException("attachment store: delta chain at ${hash.toHex()} is deeper than $deltaMaxDepth")
            }
            at = p.base!!
        }
    }

    /**
     * Opens [hash] for a verified, streaming read. The reader is pinned to the version as it is
     * now: the segments it reads from are not deleted until it is closed, or its lease runs out.
     */
    public suspend fun open(hash: KdbHash): AttachmentReader {
        val deadline = clock() + readLease.inWholeNanoseconds
        if (hash == EMPTY_HASH) return AttachmentReader(this, hash, 0, MemSource(ByteArray(0)), "", deadline)
        val reader: AttachmentReader
        val chain: List<Pair<KdbHash, Pointer>>
        mutex.withLock {
            loadLocked()
            chain = resolveLocked(hash) ?: throw AttachmentBlobNotFoundException("attachment blob not stored: ${hash.toHex()}")
            val head = chain[0].second
            reader = AttachmentReader(this, hash, head.size, MemSource(ByteArray(0)), if (head.inline != null) "" else head.leaves, deadline)
            for ((_, p) in chain) {
                pinLocked(reader, p.segment)
                pinLocked(reader, p.leaves)
            }
            readers += reader
        }
        try {
            reader.src = sourceFor(chain)
            if (chain[0].second.inline == null) verifyLeaves(hash, chain[0].second)
        } catch (e: Throwable) {
            reader.close()
            throw e
        }
        return reader
    }

    private suspend fun sourceFor(chain: List<Pair<KdbHash, Pointer>>): BlobSource {
        val last = chain.last().second
        var src: BlobSource = last.inline?.let { MemSource(it) } ?: SegSource(shim, last.segment, last.size)
        for (i in chain.size - 2 downTo 0) {
            val p = chain[i].second
            val seg = SegSource(shim, p.segment, segmentLength(shim, p.segment))
            val d = if (p.deltaV2) DeltaSourceV2.open(src, seg) else deltaSourceV1(src, seg)
            if (d.size != p.size) throw AttachmentCorruptException("attachment store: corrupt delta")
            src = d
        }
        return src
    }

    /** Checks once per leaves segment that the stored leaves fold to [hash], streaming them. */
    private suspend fun verifyLeaves(
        hash: KdbHash,
        p: Pointer,
    ) {
        if (mutex.withLock { p.leaves in verified }) return
        val count = (p.size + ATTACHMENT_CHUNK_BYTES - 1) / ATTACHMENT_CHUNK_BYTES
        val seg = SegSource(shim, p.leaves, count * 32)
        val m = MerkleBuilder()
        val buf = ByteArray(32 * 4096)
        var off = 0L
        try {
            while (off < seg.size) {
                val n = minOf(buf.size.toLong(), seg.size - off).toInt()
                seg.readAt(buf, 0, n, off)
                for (i in 0 until n step 32) m.add(KdbHash(buf.copyOfRange(i, i + 32)))
                off += n
            }
        } catch (e: AttachmentCorruptException) {
            throw AttachmentCorruptException("stored chunk hashes of ${hash.toHex()} are damaged: ${e.message}")
        }
        if (attachmentIdentity(m.root(), p.size) != hash) {
            throw AttachmentCorruptException("stored chunk hashes of ${hash.toHex()} do not fold to it")
        }
        mutex.withLock { verified += p.leaves }
    }

    internal suspend fun leafAt(
        leaves: String,
        index: Long,
    ): KdbHash {
        val b = ByteArray(32)
        SegSource(shim, leaves, (index + 1) * 32).readAt(b, 0, 32, index * 32)
        return KdbHash(b)
    }

    /** Pins [name] for [r]. Must hold [mutex]. */
    private fun pinLocked(
        r: AttachmentReader,
        name: String,
    ) {
        if (name.isEmpty()) return
        pinned[name] = (pinned[name] ?: 0) + 1
        r.pins += name
    }

    internal suspend fun release(r: AttachmentReader) {
        mutex.withLock {
            if (!readers.remove(r)) return
            for (name in r.pins) {
                val n = (pinned[name] ?: 0) - 1
                if (n > 0) {
                    pinned[name] = n
                    continue
                }
                pinned.remove(name)
                if (deferred.remove(name)) runCatching { shim.deleteSegment(name) }
            }
            r.pins.clear()
        }
    }

    /** Releases readers past their lease; their next read fails with [AttachmentReadLeaseExpiredException]. */
    private suspend fun expireReaders(now: Long) {
        val stale = mutex.withLock { readers.filter { now > it.deadline } }
        for (r in stale) {
            release(r)
            r.expired = true
        }
    }

    /** Deletes [name], or queues it while a reader has it pinned. Must hold [mutex]. */
    private suspend fun deleteOrDeferLocked(name: String) {
        if (name.isEmpty()) return
        if ((pinned[name] ?: 0) > 0) {
            deferred += name
            return
        }
        runCatching { shim.deleteSegment(name) }
    }

    /** How many segments open readers hold, and how many deletions wait on them. */
    public suspend fun pinnedSegments(): Pair<Int, Int> = mutex.withLock { pinned.size to deferred.size }

    // ---- maintenance ----

    /**
     * Re-stores [old] as a delta against [base] (the version that replaced it) when [old] is a whole
     * standalone blob within [deltaMaxBytes], the chain stays within [deltaMaxDepth], and the delta
     * is under half [old]'s size. Reports whether it did. Readers already open on [old] keep reading
     * the whole copy, which is deleted when the last of them closes.
     */
    public suspend fun storeAsDelta(
        old: KdbHash,
        base: KdbHash,
    ): Boolean {
        if (old == base || old == EMPTY_HASH || base == EMPTY_HASH) return false
        val p =
            mutex.withLock {
                loadLocked()
                val p = pointers[old] ?: return false
                val baseChain = resolveLocked(base) ?: return false
                if (p.delta || p.inline != null || p.size > deltaMaxBytes || baseChain[0].second.size > deltaMaxBytes ||
                    baseChain.size > deltaMaxDepth
                ) {
                    return false
                }
                p
            }
        val oldR = open(old)
        try {
            val baseR = open(base)
            try {
                val seg = newSegmentName()
                mutex.withLock { uploads += seg }
                try {
                    var written = 0L
                    val limit = p.size / 2
                    val tooBig =
                        try {
                            encodeDelta(baseR.src, oldR.src) { b ->
                                written += b.size
                                if (written >= limit) throw DeltaTooBig()
                                shim.appendToSegment(seg, b)
                            }
                            false
                        } catch (_: DeltaTooBig) {
                            true
                        }
                    if (tooBig) {
                        runCatching { shim.deleteSegment(seg) }
                        return false
                    }
                    shim.flushSegment(seg)
                    shim.sealSegment(seg)
                    mutex.withLock {
                        val cur = pointers[old]
                        if (cur == null || cur.gen != p.gen) {
                            runCatching { shim.deleteSegment(seg) } // changed under us; leave it be
                            return false
                        }
                        pointers[old] = writePointerLocked(old, Pointer(0, "", seg, cur.leaves, cur.size, true, false, base, null, 0))
                        deleteOrDeferLocked(cur.name)
                        deleteOrDeferLocked(cur.segment)
                    }
                    return true
                } catch (e: Throwable) {
                    runCatching { shim.deleteSegment(seg) }
                    throw e
                } finally {
                    mutex.withLock { uploads -= seg }
                }
            } finally {
                baseR.close()
            }
        } finally {
            oldR.close()
        }
    }

    private class DeltaTooBig : RuntimeException()

    /**
     * Deletes every standalone blob [keep] rejects - except the bases of kept deltas, and blobs
     * within their upload grace - and every segment no pointer names (uploads a crash interrupted,
     * pointers superseded by a higher generation). Segments an open reader has pinned go when it
     * closes. Readers past their lease are released first. Returns how many blobs were removed.
     */
    public suspend fun collect(keep: (KdbHash) -> Boolean): Int {
        val now = clock()
        expireReaders(now)
        return mutex.withLock {
            loadLocked()
            val live = HashSet<KdbHash>()
            for (h in pointers.keys) {
                val since = held[h]
                when {
                    keep(h) -> {
                        live += h
                        held.remove(h) // committed: references decide from here on
                    }
                    since != null && now - since < uploadGrace.inWholeNanoseconds -> live += h // its commit still to come
                }
            }
            for (h in live.toList()) { // a kept delta keeps its chain
                var p = pointers[h]
                while (p != null && p.delta && p.base!! !in live) {
                    live += p.base!!
                    p = pointers[p.base!!]
                }
            }
            var removed = 0
            for (h in pointers.keys.toList()) {
                if (h !in live) {
                    pointers.remove(h)
                    held.remove(h)
                    removed++
                }
            }
            val referenced = HashSet<String>()
            referenced += markSeg
            for (p in pointers.values) {
                referenced += p.name
                referenced += p.segment
                referenced += p.leaves
            }
            for (name in shim.listSegments(namespaceId)) {
                if (!name.startsWith(prefix) || name in referenced || name in uploads) continue
                // Go's reference index (r-<gen>), a note for its GC: Go retires older ones itself.
                if (name.startsWith(prefix + "r-")) continue
                deleteOrDeferLocked(name)
            }
            removed
        }
    }

    // ---- the mark ----

    private suspend fun readMark(name: String): List<KdbHash>? {
        val head = runCatching { shim.readFromSegment(name, 0, MARK_MAGIC.length + 4) }.getOrNull() ?: return null
        if (head.size < MARK_MAGIC.length + 4 || head.copyOf(MARK_MAGIC.length).decodeToString() != MARK_MAGIC) return null
        val n = be32(head, MARK_MAGIC.length)
        if (n < 0 || n > 1 shl 20) return null
        val total = MARK_MAGIC.length + 4 + n * 32 + 4
        val raw = runCatching { shim.readFromSegment(name, 0, total) }.getOrNull() ?: return null
        if (raw.size != total) return null
        val body = raw.copyOf(total - 4)
        if (!kdbSha256(body).copyOf(4).contentEquals(raw.copyOfRange(total - 4, total))) return null
        return (0 until n).map { KdbHash(body.copyOfRange(MARK_MAGIC.length + 4 + 32 * it, MARK_MAGIC.length + 4 + 32 * it + 32)) }
    }

    /**
     * The commits [setMark] last recorded - the heads delta compaction went through - or none. A
     * torn mark reads as none: compaction then looks at all of history once more, slower, never wrong.
     */
    public suspend fun mark(): List<KdbHash> =
        mutex.withLock {
            loadLocked()
            mark
        }

    /** Durably records [commits] as the mark, under the next generation before the old one goes. */
    public suspend fun setMark(commits: List<KdbHash>) {
        mutex.withLock {
            loadLocked()
            val head = MARK_MAGIC.encodeToByteArray()
            val body = ByteArray(head.size + 4 + 32 * commits.size)
            head.copyInto(body)
            for (i in 0 until 4) body[head.size + i] = (commits.size ushr (24 - 8 * i)).toByte()
            commits.forEachIndexed { i, h -> h.bytes.copyInto(body, head.size + 4 + 32 * i) }
            val gen = markGen + 1
            val name = markName(gen)
            try {
                writeSegment(name, body + kdbSha256(body).copyOf(4))
            } catch (e: Throwable) {
                runCatching { shim.deleteSegment(name) }
                throw e
            }
            if (markSeg.isNotEmpty()) runCatching { shim.deleteSegment(markSeg) } // a leftover is retired by collect
            markGen = gen
            markSeg = name
            mark = commits.toList()
        }
    }

    /**
     * Flips one stored byte of [hash] at [offset], the way a bad disk would - for tests of what a
     * damaged copy turns into. Only a blob stored whole in its own segment can be damaged this way.
     */
    public suspend fun corruptForTest(
        hash: KdbHash,
        offset: Long,
    ) {
        mutex.withLock {
            loadLocked()
            val p = pointers[hash]
            require(p != null && p.segment.isNotEmpty() && !p.delta) { "${hash.toHex()} is not stored whole in a segment" }
            require(offset in 0 until p.size) { "offset $offset outside ${hash.toHex()}'s ${p.size} bytes" }
            val raw = ByteArray(p.size.toInt())
            SegSource(shim, p.segment, p.size).readAt(raw, 0, raw.size, 0)
            raw[offset.toInt()] = (raw[offset.toInt()].toInt() xor 0xff).toByte()
            shim.deleteSegment(p.segment)
            shim.appendToSegment(p.segment, raw)
        }
    }

    /** Re-points [old] at an already written delta [segment] against [base] - for tests of delta
     * formats this build does not write. */
    internal suspend fun installDeltaForTest(
        old: KdbHash,
        base: KdbHash,
        segment: String,
        v2: Boolean,
    ) {
        mutex.withLock {
            loadLocked()
            val cur = pointers.getValue(old)
            pointers[old] = writePointerLocked(old, Pointer(0, "", segment, cur.leaves, cur.size, true, v2, base, null, 0))
            deleteOrDeferLocked(cur.name)
            deleteOrDeferLocked(cur.segment)
        }
    }

    internal companion object {
        const val POINTER_MAGIC = "KDBATP3"
        const val POINTER_MAGIC_V2 = "KDBATP2"
        const val MARK_MAGIC = "KDBATM1"
        const val FLAG_DELTA = 1
        const val FLAG_INLINE = 2
        const val FLAG_DELTA_V2 = 4

        /** The content hash of zero bytes: never stored (a blob store cannot tell zero bytes from
         * none), always present. */
        val EMPTY_HASH: KdbHash = attachmentContentHash(ByteArray(0))

        fun nowNanos(): Long = KdbTimestamp.now().toEpochMicros() * 1000

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

/**
 * One blob being uploaded ([AttachmentBlobStore.begin]): [write] its bytes in pieces of any size,
 * then [finish]. Bytes go to their segment a chunk at a time as they arrive, hashed on the way; a
 * blob that ends within [AttachmentBlobStore.smallMaxBytes] is kept inline instead. Not safe for
 * concurrent use.
 */
public class AttachmentUpload internal constructor(
    private val store: AttachmentBlobStore,
) {
    private val hasher = AttachmentHasher()
    private val cur = ByteArray(ATTACHMENT_CHUNK_BYTES)
    private var fill = 0
    private var segment: String? = null
    private var done = false

    /** Bytes written so far. */
    public val size: Long get() = hasher.size

    public suspend fun write(
        data: ByteArray,
        offset: Int = 0,
        length: Int = data.size - offset,
    ) {
        check(!done) { "attachment upload already finished" }
        if (store.maxBytes > 0 && hasher.size + length > store.maxBytes) {
            abort()
            throw AttachmentTooLargeException("attachment exceeds the size limit of ${store.maxBytes} bytes")
        }
        hasher.update(data, offset, length)
        var off = offset
        var left = length
        while (left > 0) {
            val n = minOf(left, cur.size - fill)
            data.copyInto(cur, fill, off, off + n)
            fill += n
            off += n
            left -= n
            if (fill == cur.size) {
                val seg = segment ?: store.startSegment().also { segment = it }
                store.appendTo(seg, cur.copyOf())
                fill = 0
            }
        }
    }

    /** Makes the blob durable and returns its content hash and size. */
    public suspend fun finish(): StoredAttachmentBlob {
        check(!done) { "attachment upload already finished" }
        done = true
        val hash = hasher.finish()
        val last = cur.copyOf(fill)
        var seg = segment
        try {
            if (seg == null && fill.toLong() > store.smallMaxBytes) seg = store.startSegment()
            if (seg != null && fill > 0) store.appendTo(seg, last)
        } catch (e: Throwable) {
            seg?.let { store.abandon(it) }
            throw e
        }
        return store.finishUpload(hash, hasher.size, seg, last, hasher.leafHashes)
    }

    /** Discards the upload. Idempotent. */
    public suspend fun abort() {
        if (done) return
        done = true
        segment?.let { store.abandon(it) }
    }
}

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
 * A streaming read of one blob version: sequential ([read]) or random access ([readAt]), each chunk
 * verified against its leaf hash before any of it is returned. Holds pins on everything it reads
 * from until [close] (or until its lease runs out: then reads fail with
 * [AttachmentReadLeaseExpiredException]).
 */
public class AttachmentReader internal constructor(
    private val store: AttachmentBlobStore,
    public val contentHash: KdbHash,
    public val size: Long,
    internal var src: BlobSource,
    /** The leaves segment; "" for an inline blob, checked whole when its pointer was read. */
    private val leaves: String,
    internal val deadline: Long,
) {
    internal val pins = ArrayList<String>()
    internal var expired = false
    private val mutex = Mutex()
    private var position = 0L
    private var cachedIndex = -1L
    private var cached = ByteArray(0)
    private var closed = false

    /** Chunk [index]'s leaf hash - for proving a chunk to someone else. */
    public suspend fun chunkLeaf(index: Long): KdbHash {
        if (leaves.isEmpty()) {
            val start = index * ATTACHMENT_CHUNK_BYTES
            val n = minOf(ATTACHMENT_CHUNK_BYTES.toLong(), size - start).toInt()
            val data = ByteArray(maxOf(n, 0))
            src.readAt(data, 0, data.size, start)
            return chunkLeafHash(data)
        }
        return store.leafAt(leaves, index)
    }

    private suspend fun chunk(index: Long): ByteArray {
        if (expired) throw AttachmentReadLeaseExpiredException("attachment read lease expired: ${contentHash.toHex()}")
        check(!closed) { "attachment reader is closed" }
        if (index == cachedIndex) return cached
        val start = index * ATTACHMENT_CHUNK_BYTES
        val n = minOf(ATTACHMENT_CHUNK_BYTES.toLong(), size - start).toInt()
        val buf = if (cached.size == n) cached else ByteArray(n)
        cachedIndex = -1
        src.readAt(buf, 0, n, start)
        if (leaves.isNotEmpty() && chunkLeafHash(buf) != store.leafAt(leaves, index)) {
            throw AttachmentCorruptException("chunk $index of ${contentHash.toHex()} does not match its stored hash")
        }
        cached = buf
        cachedIndex = index
        return buf
    }

    /** Reads up to [length] bytes at [pos] into [buffer]; returns how many, or -1 at the end. */
    public suspend fun readAt(
        pos: Long,
        buffer: ByteArray,
        offset: Int = 0,
        length: Int = buffer.size - offset,
    ): Int =
        mutex.withLock {
            require(pos >= 0) { "negative offset" }
            if (pos >= size) return@withLock -1
            var done = 0
            var p = pos
            while (done < length && p < size) {
                val index = p / ATTACHMENT_CHUNK_BYTES
                val c = chunk(index)
                val inChunk = (p - index * ATTACHMENT_CHUNK_BYTES).toInt()
                val n = minOf(length - done, c.size - inChunk)
                c.copyInto(buffer, offset + done, inChunk, inChunk + n)
                done += n
                p += n
            }
            done
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

    /** Releases the pins; a segment whose deletion waited on them goes now. Idempotent. */
    public suspend fun close() {
        if (closed) return
        closed = true
        if (!expired) store.release(this)
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

/** A read outlived its lease ([AttachmentBlobStore.readLease]) and was released. */
public class AttachmentReadLeaseExpiredException(
    message: String,
) : IllegalStateException(message)
