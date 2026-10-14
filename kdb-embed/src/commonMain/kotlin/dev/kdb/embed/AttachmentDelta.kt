package dev.kdb.embed

import dev.kdb.storage.PlatformIoShim

/*
 * Binary deltas, git-pack style - a byte-for-byte mirror of go/kdb/storage/attachstore/delta.go and
 * reader.go. A target is a sequence of instructions against a base: copy a run of the base, or
 * insert literal bytes. A replaced attachment version is stored as a delta against the version that
 * replaced it ([AttachmentBlobStore.storeAsDelta]), so the newest bytes are always whole.
 *
 * Version 1 (what this build writes): uvarint target length, then instructions to the segment's end:
 *
 *     0x00 uvarint n, then n literal bytes
 *     0x01 uvarint offset, uvarint n: copy base[offset : offset+n]
 *
 * Version 2 (pointer flag 0x04; written by Go for versions of any size, read here): frames, an
 * index and a footer, all integers big-endian -
 *
 *     frame  := the frame's literal bytes, then its op block (ops as in v1, a literal carrying no
 *               bytes: it takes the next n of the frame's literal area)
 *     index  := one 32-byte entry per frame: tStart, litOff, opsOff, opsLen (u64 each), ascending
 *               tStart from 0; a frame covers the target up to the next tStart (the last, to its end)
 *     footer := indexOff, frameCount, targetSize (u64 each), "KDBDLT2\n" - the segment's last 32 bytes
 *
 * Either is read by random access ([deltaSourceV1], [DeltaSourceV2]): a read touches only the base
 * ranges and literal bytes it needs, and nothing is rebuilt whole.
 */

private const val OP_LITERAL = 0x00
private const val OP_COPY = 0x01
private const val DELTA_PRIME = 0x100000001b3L
private const val MAX_INDEX = 1 shl 20
private const val OUT_PIECE = 1 shl 20
private const val MAX_LITERAL = 1 shl 20
private const val PAGE_BYTES = 1 shl 20
private const val CACHE_PAGES = 4
private const val DELTA_V2_FOOTER = 32
private const val DELTA_V2_MAX_OPS = 1 shl 20
private val DELTA_V2_MAGIC = "KDBDLT2\n".encodeToByteArray()

private fun corruptDelta(): AttachmentCorruptException = AttachmentCorruptException("attachment store: corrupt delta")

/** Random access to one blob version's bytes, wherever they live. */
internal interface BlobSource {
    val size: Long

    /** Fills dst[dstOff, dstOff+len) from [off]; a short source is an error. */
    suspend fun readAt(
        dst: ByteArray,
        dstOff: Int,
        len: Int,
        off: Long,
    )
}

internal class MemSource(
    private val bytes: ByteArray,
) : BlobSource {
    override val size: Long get() = bytes.size.toLong()

    override suspend fun readAt(
        dst: ByteArray,
        dstOff: Int,
        len: Int,
        off: Long,
    ) {
        if (off < 0 || off + len > bytes.size) throw AttachmentCorruptException("attachment store: read past the end of an inline blob")
        bytes.copyInto(dst, dstOff, off.toInt(), off.toInt() + len)
    }
}

internal class SegSource(
    private val shim: PlatformIoShim,
    val name: String,
    override val size: Long,
) : BlobSource {
    override suspend fun readAt(
        dst: ByteArray,
        dstOff: Int,
        len: Int,
        off: Long,
    ) {
        var got = 0
        while (got < len) {
            val b = shim.readFromSegment(name, off + got, len - got)
            if (b.isEmpty()) throw AttachmentCorruptException("attachment store: segment $name is shorter than its blob")
            b.copyInto(dst, dstOff + got, 0, minOf(b.size, len - got))
            got += b.size
        }
    }
}

/** A segment's length, found by probing (the shim reports none) - Go's segmentLength. */
internal suspend fun segmentLength(
    shim: PlatformIoShim,
    name: String,
): Long {
    var lo = 0L
    var hi = 1L
    while (true) {
        val b = runCatching { shim.readFromSegment(name, hi, 1) }.getOrNull()
        if (b == null || b.isEmpty()) break
        lo = hi
        hi *= 2
    }
    while (lo + 1 < hi) {
        val mid = (lo + hi) / 2
        val b = runCatching { shim.readFromSegment(name, mid, 1) }.getOrNull()
        if (b != null && b.size == 1) lo = mid else hi = mid
    }
    return if (shim.readFromSegment(name, 0, 1).isEmpty()) 0 else lo + 1
}

/** Target bytes [tOff, tOff+n) come from the base at [off] ([copy]) or from the delta segment at [off]. */
internal class DeltaInstr(
    val tOff: Long,
    val n: Long,
    val off: Long,
    val copy: Boolean,
)

/** Reads a run of instructions starting at the one covering [off]; shared by both versions. */
private suspend fun readThrough(
    ins: List<DeltaInstr>,
    base: BlobSource,
    seg: SegSource,
    dst: ByteArray,
    dstOff: Int,
    len: Int,
    off: Long,
): Int {
    var i = lowerBound(ins.size) { ins[it].tOff + ins[it].n > off }
    var o = off
    var done = 0
    while (done < len && i < ins.size) {
        val inst = ins[i]
        val within = o - inst.tOff
        val take = minOf((len - done).toLong(), inst.n - within).toInt()
        if (inst.copy) base.readAt(dst, dstOff + done, take, inst.off + within) else seg.readAt(dst, dstOff + done, take, inst.off + within)
        done += take
        o += take
        i++
    }
    return done
}

private inline fun lowerBound(
    n: Int,
    pred: (Int) -> Boolean,
): Int {
    var lo = 0
    var hi = n
    while (lo < hi) {
        val mid = (lo + hi) ushr 1
        if (pred(mid)) hi = mid else lo = mid + 1
    }
    return lo
}

/** A version-1 delta, its instructions indexed in memory (versions within DeltaMaxBytes). */
internal class DeltaSourceV1(
    private val base: BlobSource,
    private val seg: SegSource,
    override val size: Long,
    private val ins: List<DeltaInstr>,
) : BlobSource {
    override suspend fun readAt(
        dst: ByteArray,
        dstOff: Int,
        len: Int,
        off: Long,
    ) {
        if (off < 0 || off + len > size) throw corruptDelta()
        if (readThrough(ins, base, seg, dst, dstOff, len, off) != len) throw corruptDelta()
    }
}

/** Streams a segment front to back, a page at a time. */
private class SegStream(
    private val seg: SegSource,
    private var pos: Long = 0,
) {
    private var buf = ByteArray(0)
    private var bufAt = 0L

    val position: Long get() = pos

    suspend fun byte(): Int {
        if (pos >= seg.size) throw corruptDelta()
        if (pos < bufAt || pos >= bufAt + buf.size) {
            val n = minOf(64L * 1024, seg.size - pos).toInt()
            buf = ByteArray(n)
            seg.readAt(buf, 0, n, pos)
            bufAt = pos
        }
        return buf[(pos++ - bufAt).toInt()].toInt() and 0xff
    }

    suspend fun uvarint(): Long {
        var v = 0L
        var shift = 0
        while (true) {
            val b = byte()
            if (shift == 63 && b > 1) throw corruptDelta()
            v = v or ((b and 0x7f).toLong() shl shift)
            if (b < 0x80) return v
            shift += 7
            if (shift > 63) throw corruptDelta()
        }
    }

    fun skip(n: Long) {
        pos += n
    }
}

/** Reads a version-1 delta segment's instructions (not its literal bytes) - Go's parseDelta. */
internal suspend fun deltaSourceV1(
    base: BlobSource,
    seg: SegSource,
): DeltaSourceV1 {
    val r = SegStream(seg)
    val n = r.uvarint()
    val ins = ArrayList<DeltaInstr>()
    var t = 0L
    while (r.position < seg.size) {
        when (r.byte()) {
            OP_LITERAL -> {
                val l = r.uvarint()
                if (l < 0 || l > seg.size - r.position) throw corruptDelta()
                ins += DeltaInstr(t, l, r.position, false)
                r.skip(l)
                t += l
            }
            OP_COPY -> {
                val bo = r.uvarint()
                val l = r.uvarint()
                if (bo < 0 || l < 0 || bo > base.size || l > base.size - bo) throw corruptDelta()
                ins += DeltaInstr(t, l, bo, true)
                t += l
            }
            else -> throw corruptDelta()
        }
    }
    if (t != n) throw corruptDelta()
    return DeltaSourceV1(base, seg, n, ins)
}

private fun be64(
    b: ByteArray,
    off: Int,
): Long {
    var v = 0L
    for (i in 0 until 8) v = (v shl 8) or (b[off + i].toLong() and 0xff)
    return v
}

/**
 * A version-2 delta: the frame index is searched on disk, and one frame's ops are parsed at a time,
 * so a delta of any size is read in bounded memory.
 */
internal class DeltaSourceV2 private constructor(
    private val base: BlobSource,
    private val seg: SegSource,
    override val size: Long,
    private val indexOff: Long,
    private val frames: Long,
) : BlobSource {
    private var cachedFrame = -1L
    private var cachedIns: List<DeltaInstr> = emptyList()
    private var cachedEnd = 0L

    private suspend fun entry(i: Long): LongArray {
        val b = ByteArray(32)
        seg.readAt(b, 0, 32, indexOff + 32 * i)
        return longArrayOf(be64(b, 0), be64(b, 8), be64(b, 16), be64(b, 24))
    }

    /** The instructions of the frame covering target offset [off], and where the frame ends. */
    private suspend fun frameAt(off: Long): Pair<List<DeltaInstr>, Long> {
        var lo = 0L
        var hi = frames // first frame whose tStart > off
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (entry(mid)[0] > off) hi = mid else lo = mid + 1
        }
        val f = lo - 1
        if (f < 0) throw corruptDelta()
        if (f == cachedFrame) return cachedIns to cachedEnd
        val e = entry(f)
        val tStart = e[0]
        val litOff = e[1]
        val opsOff = e[2]
        val opsLen = e[3]
        val end = if (f + 1 < frames) entry(f + 1)[0] else size
        if (end <= tStart || end > size || opsLen < 0 || opsLen > DELTA_V2_MAX_OPS || opsOff < 0 || opsOff + opsLen > indexOff ||
            litOff < 0 || litOff > opsOff
        ) {
            throw corruptDelta()
        }
        val ops = ByteArray(opsLen.toInt())
        seg.readAt(ops, 0, ops.size, opsOff)
        var p = 0

        fun uv(): Long {
            var v = 0L
            var shift = 0
            while (true) {
                if (p >= ops.size || shift > 63) throw corruptDelta()
                val b = ops[p++].toInt() and 0xff
                v = v or ((b and 0x7f).toLong() shl shift)
                if (b < 0x80) return v
                shift += 7
            }
        }
        val ins = ArrayList<DeltaInstr>()
        var t = tStart
        var lit = litOff
        while (p < ops.size) {
            when (ops[p++].toInt() and 0xff) {
                OP_LITERAL -> {
                    val l = uv()
                    if (l <= 0 || l > opsOff - lit) throw corruptDelta()
                    ins += DeltaInstr(t, l, lit, false)
                    lit += l
                    t += l
                }
                OP_COPY -> {
                    val bo = uv()
                    val l = uv()
                    if (bo < 0 || l <= 0 || bo > base.size || l > base.size - bo) throw corruptDelta()
                    ins += DeltaInstr(t, l, bo, true)
                    t += l
                }
                else -> throw corruptDelta()
            }
            if (t > end) throw corruptDelta()
        }
        if (t != end) throw corruptDelta()
        cachedFrame = f
        cachedIns = ins
        cachedEnd = end
        return ins to end
    }

    override suspend fun readAt(
        dst: ByteArray,
        dstOff: Int,
        len: Int,
        off: Long,
    ) {
        if (off < 0 || off + len > size) throw corruptDelta()
        var done = 0
        var o = off
        while (done < len) {
            val (ins, end) = frameAt(o)
            val take = minOf((len - done).toLong(), end - o).toInt()
            if (readThrough(ins, base, seg, dst, dstOff + done, take, o) != take) throw corruptDelta()
            done += take
            o += take
        }
    }

    companion object {
        suspend fun open(
            base: BlobSource,
            seg: SegSource,
        ): DeltaSourceV2 {
            if (seg.size < DELTA_V2_FOOTER) throw corruptDelta()
            val f = ByteArray(DELTA_V2_FOOTER)
            seg.readAt(f, 0, f.size, seg.size - DELTA_V2_FOOTER)
            if (!f.copyOfRange(24, 32).contentEquals(DELTA_V2_MAGIC)) throw corruptDelta()
            val indexOff = be64(f, 0)
            val frames = be64(f, 8)
            val size = be64(f, 16)
            if (indexOff < 0 || frames < 0 || size < 0 || frames > (seg.size - DELTA_V2_FOOTER) / 32 ||
                indexOff + 32 * frames != seg.size - DELTA_V2_FOOTER || (frames == 0L) != (size == 0L)
            ) {
                throw corruptDelta()
            }
            val d = DeltaSourceV2(base, seg, size, indexOff, frames)
            if (frames > 0 && d.entry(0)[0] != 0L) throw corruptDelta()
            return d
        }
    }
}

/** Serves single bytes of a source a page at a time. */
private class PageCache(
    private val src: BlobSource,
) {
    private val pages = HashMap<Long, ByteArray>()
    private val order = ArrayDeque<Long>()

    suspend fun page(i: Long): ByteArray {
        pages[i]?.let { return it }
        val n = minOf(PAGE_BYTES.toLong(), src.size - i * PAGE_BYTES).toInt()
        if (order.size >= CACHE_PAGES) pages.remove(order.removeFirst())
        val buf = ByteArray(n)
        src.readAt(buf, 0, n, i * PAGE_BYTES)
        pages[i] = buf
        order.addLast(i)
        return buf
    }

    private var lastIndex = -1L
    private var lastPage = ByteArray(0)

    /** The byte at [off] if its page is the one last used, else -1 - the hot loops' fast path,
     * which avoids a suspend call per byte. */
    fun cached(off: Long): Int {
        val i = off / PAGE_BYTES
        return if (i == lastIndex) lastPage[(off - i * PAGE_BYTES).toInt()].toInt() and 0xff else -1
    }

    suspend fun byteAt(off: Long): Int {
        val c = cached(off)
        if (c >= 0) return c
        val i = off / PAGE_BYTES
        lastPage = page(i)
        lastIndex = i
        return lastPage[(off - i * PAGE_BYTES).toInt()].toInt() and 0xff
    }
}

private class ByteOut {
    var buf = ByteArray(1024)
    var len = 0

    fun ensure(extra: Int) {
        if (len + extra <= buf.size) return
        var n = buf.size * 2
        while (n < len + extra) n *= 2
        buf = buf.copyOf(n)
    }

    fun byte(b: Int) {
        ensure(1)
        buf[len++] = b.toByte()
    }

    fun uvarint(v: Long) {
        var x = v
        while (x.toULong() >= 0x80u) {
            byte((x.toInt() and 0x7f) or 0x80)
            x = x ushr 7
        }
        byte(x.toInt())
    }
}

/**
 * Writes [target] as a version-1 delta against [base] to [emit] - exactly the bytes Go's
 * encodeDelta writes. Neither version is held whole: the base is indexed at block-aligned offsets
 * by a hash of the block there, the target scanned with a rolling hash through a small page cache;
 * a hit is verified, extended both ways, and emitted as a copy.
 */
internal suspend fun encodeDelta(
    base: BlobSource,
    target: BlobSource,
    emit: suspend (ByteArray) -> Unit,
) {
    var block = 32L
    while (base.size / block > MAX_INDEX) block *= 2
    val out = ByteOut()
    out.uvarint(target.size)

    suspend fun flush(force: Boolean) {
        if (out.len == 0 || (!force && out.len < OUT_PIECE)) return
        emit(out.buf.copyOf(out.len))
        out.len = 0
    }

    suspend fun literal(
        fromIn: Long,
        to: Long,
    ) {
        var from = fromIn
        while (from < to) {
            val n = minOf(to - from, MAX_LITERAL.toLong()).toInt()
            out.byte(OP_LITERAL)
            out.uvarint(n.toLong())
            out.ensure(n)
            target.readAt(out.buf, out.len, n, from)
            out.len += n
            from += n
            flush(false)
        }
    }
    if (base.size < block || target.size < block) {
        literal(0, target.size)
        flush(true)
        return
    }

    // Index the base, a page at a time; the first offset of each block hash wins.
    val index = HashMap<Long, Long>()
    val pgCap = (PAGE_BYTES - PAGE_BYTES % block).toInt()
    val pg = ByteArray(pgCap)
    var off = 0L
    while (off + block <= base.size) {
        var n = minOf(pgCap.toLong(), base.size - off)
        n -= n % block
        base.readAt(pg, 0, n.toInt(), off)
        var b = 0
        while (b < n) {
            var h = 0L
            for (k in 0 until block.toInt()) h = h * DELTA_PRIME + (pg[b + k].toLong() and 0xff)
            if (h !in index) index[h] = off + b
            b += block.toInt()
        }
        off += pgCap
    }
    val bc = PageCache(base)
    val tc = PageCache(target)

    var pow = 1L
    for (i in 0 until block - 1) pow *= DELTA_PRIME

    suspend fun hashAt(i: Long): Long {
        var h = 0L
        for (k in 0 until block) {
            val c = tc.cached(i + k)
            h = h * DELTA_PRIME + (if (c >= 0) c else tc.byteAt(i + k))
        }
        return h
    }

    suspend fun same(
        bo: Long,
        to: Long,
    ): Boolean {
        for (k in 0 until block) {
            val x = bc.cached(bo + k).let { if (it >= 0) it else bc.byteAt(bo + k) }
            val y = tc.cached(to + k).let { if (it >= 0) it else tc.byteAt(to + k) }
            if (x != y) return false
        }
        return true
    }

    val tn = target.size
    val bn = base.size
    var litStart = 0L
    var i = 0L
    var h = hashAt(0)
    while (i + block <= tn) {
        val bo = index[h]
        if (bo != null && same(bo, i)) {
            var start = i
            var bstart = bo
            while (start > litStart && bstart > 0) {
                if (bc.byteAt(bstart - 1) != tc.byteAt(start - 1)) break
                start--
                bstart--
            }
            var end = i + block
            var bend = bo + block
            while (end < tn && bend < bn) {
                val x = bc.cached(bend).let { if (it >= 0) it else bc.byteAt(bend) }
                val y = tc.cached(end).let { if (it >= 0) it else tc.byteAt(end) }
                if (x != y) break
                end++
                bend++
            }
            literal(litStart, start)
            out.byte(OP_COPY)
            out.uvarint(bstart)
            out.uvarint(end - start)
            flush(false)
            litStart = end
            i = end
            if (i + block <= tn) h = hashAt(i)
            continue
        }
        if (i + block >= tn) break
        val first = tc.cached(i).let { if (it >= 0) it else tc.byteAt(i) }.toLong()
        val next = tc.cached(i + block).let { if (it >= 0) it else tc.byteAt(i + block) }.toLong()
        h = (h - first * pow) * DELTA_PRIME + next
        i++
    }
    literal(litStart, tn)
    flush(true)
}
