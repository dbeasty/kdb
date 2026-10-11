package dev.kdb.compression

import dev.kdb.compression.ZstdTables.highBit

/**
 * A pure-Kotlin zstd decoder (RFC 8878), the [ZstdCompression.decompress] of Kotlin/JS and
 * Kotlin/Native. The JVM keeps zstd-jni; this exists so the targets without a native libzstd can
 * read the frames Go (klauspost/compress) and the JVM (zstd-jni) write - docs/kdb-open-issues-plan.md,
 * roadmap R1.
 *
 * Scope is exactly what KDB's on-disk formats need: one-shot decoding of a whole buffer into
 * memory, concatenated and skippable frames, every block and literals type, every sequence table
 * mode, and the optional content checksum (verified when present). Dictionaries are not
 * supported - no KDB format uses one - and a frame that names one is rejected.
 *
 * Any malformed input - a truncated frame, a table that does not add up, a bitstream that is not
 * consumed exactly, an offset reaching before the start of the frame, a checksum or declared size
 * that does not match - throws [ZstdFormatException]. It never returns bytes it could not fully
 * account for.
 */
internal object ZstdDecoder {
    fun decompress(input: ByteArray, maxOutputSize: Int): ByteArray {
        val out = OutputBuffer(maxOutputSize, input.size)
        var p = 0
        while (p < input.size) {
            if (input.size - p < 4) corrupt("${input.size - p} trailing bytes after the last frame")
            val magic = readIntLe(input, p)
            p +=
                when {
                    magic == ZstdTables.MAGIC -> FrameDecoder(input, p + 4, out).decode() - p
                    magic and ZstdTables.SKIPPABLE_MAGIC_MASK == ZstdTables.SKIPPABLE_MAGIC -> {
                        if (input.size - p < 8) corrupt("truncated skippable frame header")
                        val size = readIntLe(input, p + 4).toLong() and 0xFFFFFFFFL
                        if (size > input.size - p - 8) corrupt("skippable frame runs past the input")
                        8 + size.toInt()
                    }
                    else -> corrupt("bad frame magic 0x${(magic.toLong() and 0xFFFFFFFFL).toString(16)}")
                }
        }
        return out.toByteArray()
    }
}

/** Thrown for any input the decoder cannot account for. Never caught inside the decoder. */
private fun corrupt(message: String): Nothing = throw ZstdFormatException("corrupt zstd data: $message")

private fun readIntLe(b: ByteArray, p: Int): Int =
    (b[p].toInt() and 0xFF) or ((b[p + 1].toInt() and 0xFF) shl 8) or
        ((b[p + 2].toInt() and 0xFF) shl 16) or ((b[p + 3].toInt() and 0xFF) shl 24)

private fun u8(b: ByteArray, p: Int): Int = b[p].toInt() and 0xFF

/** The whole decoded output across frames, bounded by maxOutputSize. */
private class OutputBuffer(val max: Int, inputSize: Int) {
    var buf = ByteArray(minOf(max.toLong(), maxOf(256L, inputSize * 4L)).toInt())
    var size = 0

    fun reserve(n: Int) {
        if (n > max - size) {
            throw ZstdFormatException("decompressed size exceeds max $max")
        }
        val need = size + n
        if (need > buf.size) {
            var cap = buf.size.coerceAtLeast(256)
            while (cap < need) cap = if (cap > max / 2) max else cap * 2
            buf = buf.copyOf(cap)
        }
    }

    fun toByteArray(): ByteArray = if (size == buf.size) buf else buf.copyOf(size)
}

/** FSE decoding table: per state, the symbol, the bits to read, and the base of the next state. */
private class FseTable(val log: Int, val symbol: IntArray, val nbBits: IntArray, val base: IntArray) {
    companion object {
        fun build(norm: ShortArray, maxSymbol: Int, log: Int): FseTable {
            val size = 1 shl log
            val cells = ZstdTables.spreadSymbols(norm, maxSymbol, log)
            val next = IntArray(maxSymbol + 1) { s -> if (norm[s].toInt() == -1) 1 else norm[s].toInt() }
            val nbBits = IntArray(size)
            val base = IntArray(size)
            for (u in 0 until size) {
                val s = cells[u]
                val n = next[s]++
                nbBits[u] = log - highBit(n)
                base[u] = (n shl nbBits[u]) - size
            }
            return FseTable(log, cells, nbBits, base)
        }

        fun rle(symbol: Int): FseTable = FseTable(0, intArrayOf(symbol), intArrayOf(0), intArrayOf(0))

        val LL_DEFAULT = build(ZstdTables.LL_DEFAULT_NORM, ZstdTables.LL_DEFAULT_NORM.size - 1, ZstdTables.LL_DEFAULT_LOG)
        val ML_DEFAULT = build(ZstdTables.ML_DEFAULT_NORM, ZstdTables.ML_DEFAULT_NORM.size - 1, ZstdTables.ML_DEFAULT_LOG)
        val OF_DEFAULT = build(ZstdTables.OF_DEFAULT_NORM, ZstdTables.OF_DEFAULT_NORM.size - 1, ZstdTables.OF_DEFAULT_LOG)
    }
}

/**
 * Reads a backward bitstream (RFC 8878 §4.1): the last byte holds a 1-bit end marker above the
 * final bits, and fields are read from the highest bit downwards. Bits below the start read as
 * zero; [overread] reports whether any were taken, which every caller treats as corruption once
 * it is done.
 */
private class BackwardBits(private val src: ByteArray, private val start: Int, end: Int) {
    /** Bits [0, pos) relative to [start] are still unread. */
    var pos: Int

    init {
        if (end <= start) corrupt("empty bitstream")
        val last = u8(src, end - 1)
        if (last == 0) corrupt("bitstream has no end marker")
        pos = (end - start - 1) * 8 + highBit(last)
    }

    val overread: Boolean get() = pos < 0
    val finished: Boolean get() = pos == 0

    fun read(n: Int): Int {
        if (n == 0) return 0
        if (n > 24) {
            val hi = read(n - 16)
            return (hi shl 16) or read(16)
        }
        pos -= n
        return extract(pos, n)
    }

    fun peek(n: Int): Int = extract(pos - n, n)

    fun skip(n: Int) {
        pos -= n
    }

    private fun extract(low: Int, n: Int): Int {
        if (low < 0) {
            val avail = n + low
            return if (avail <= 0) 0 else extract(0, avail) shl -low
        }
        val i = start + (low ushr 3)
        val shift = low and 7
        val bytes = (shift + n + 7) ushr 3
        var v = 0
        for (k in 0 until bytes) v = v or (u8(src, i + k) shl (8 * k))
        return (v ushr shift) and ((1 shl n) - 1)
    }
}

/**
 * Reads an FSE table description (RFC 8878 §4.1.1) from [src] at [p]. Returns the normalized
 * counts (index = symbol), with the accuracy log and the bytes consumed.
 */
private class NormalizedCounts(val norm: ShortArray, val maxSymbol: Int, val log: Int, val bytesRead: Int) {
    companion object {
        fun read(src: ByteArray, p: Int, end: Int, maxSymbol: Int, maxLog: Int): NormalizedCounts {
            val totalBits = (end - p) * 8
            var bit = 0

            fun bits(n: Int): Int {
                var v = 0
                for (k in 0 until n) {
                    val b = bit + k
                    if (b < totalBits && (u8(src, p + (b ushr 3)) ushr (b and 7)) and 1 != 0) v = v or (1 shl k)
                }
                return v
            }

            if (totalBits < 4) corrupt("truncated FSE table description")
            val log = bits(4) + 5
            bit = 4
            if (log > maxLog) corrupt("FSE accuracy log $log exceeds $maxLog")
            val norm = ShortArray(maxSymbol + 1)
            var remaining = (1 shl log) + 1
            var threshold = 1 shl log
            var nbBits = log + 1
            var symbol = 0
            var previousZero = false
            while (remaining > 1) {
                if (previousZero) {
                    var n0 = symbol
                    while (true) {
                        val r = bits(2)
                        bit += 2
                        if (bit > totalBits) corrupt("truncated FSE table description")
                        n0 += r
                        if (r != 3) break
                    }
                    if (n0 > maxSymbol) corrupt("FSE zero run past the maximum symbol")
                    symbol = n0
                    if (symbol > maxSymbol) corrupt("FSE table describes too many symbols")
                }
                if (symbol > maxSymbol) corrupt("FSE table describes too many symbols")
                val max = (2 * threshold - 1) - remaining
                var count: Int
                val low = bits(nbBits - 1)
                if (low < max) {
                    count = low
                    bit += nbBits - 1
                } else {
                    count = bits(nbBits)
                    if (count >= threshold) count -= max
                    bit += nbBits
                }
                if (bit > totalBits) corrupt("truncated FSE table description")
                count--
                remaining -= if (count < 0) -count else count
                norm[symbol++] = count.toShort()
                previousZero = count == 0
                if (remaining < threshold) {
                    if (remaining <= 1) break
                    nbBits = highBit(remaining) + 1
                    threshold = 1 shl (nbBits - 1)
                }
            }
            if (remaining != 1) corrupt("FSE table probabilities do not add up")
            return NormalizedCounts(norm, symbol - 1, log, (bit + 7) ushr 3)
        }
    }
}

private class HuffmanTable(val log: Int, val entries: IntArray) {
    fun symbol(e: Int): Int = entries[e] ushr 8

    fun nbBits(e: Int): Int = entries[e] and 0xFF
}

private class FrameDecoder(private val src: ByteArray, private var p: Int, private val out: OutputBuffer) {
    private val frameStart = out.size
    private val reps = intArrayOf(1, 4, 8)
    private var huffman: HuffmanTable? = null
    private var llTable: FseTable? = null
    private var ofTable: FseTable? = null
    private var mlTable: FseTable? = null

    private fun need(n: Int) {
        if (n > src.size - p) corrupt("frame truncated")
    }

    /** Decodes one frame whose magic has been consumed; returns the input offset after it. */
    fun decode(): Int {
        need(1)
        val fhd = u8(src, p++)
        val fcsFlag = fhd ushr 6
        val singleSegment = (fhd ushr 5) and 1 == 1
        if ((fhd ushr 3) and 1 != 0) corrupt("reserved frame header bit is set")
        val checksum = (fhd ushr 2) and 1 == 1
        val dictIdSize = intArrayOf(0, 1, 2, 4)[fhd and 3]
        if (!singleSegment) {
            need(1)
            p++ // window descriptor: everything is decoded into memory bounded by maxOutputSize
        }
        need(dictIdSize)
        var dictId = 0L
        for (k in 0 until dictIdSize) dictId = dictId or (u8(src, p + k).toLong() shl (8 * k))
        p += dictIdSize
        if (dictId != 0L) throw ZstdFormatException("zstd frame needs dictionary $dictId; dictionaries are not supported")
        val fcsSize = if (fcsFlag == 0) (if (singleSegment) 1 else 0) else 1 shl fcsFlag
        need(fcsSize)
        var contentSize = -1L
        if (fcsSize > 0) {
            contentSize = 0L
            for (k in 0 until fcsSize) contentSize = contentSize or (u8(src, p + k).toLong() shl (8 * k))
            if (fcsSize == 2) contentSize += 256
            if (fcsSize == 8 && contentSize < 0) corrupt("frame content size overflows")
            p += fcsSize
            if (contentSize > out.max - out.size) {
                throw ZstdFormatException("decompressed size $contentSize exceeds max ${out.max}")
            }
            out.reserve(contentSize.toInt())
        }

        while (true) {
            need(3)
            val header = u8(src, p) or (u8(src, p + 1) shl 8) or (u8(src, p + 2) shl 16)
            p += 3
            val last = header and 1 == 1
            val type = (header ushr 1) and 3
            val size = header ushr 3
            if (size > ZstdTables.BLOCK_SIZE_MAX) corrupt("block of $size bytes exceeds the 128KB maximum")
            when (type) {
                ZstdTables.BLOCK_RAW -> {
                    need(size)
                    out.reserve(size)
                    src.copyInto(out.buf, out.size, p, p + size)
                    out.size += size
                    p += size
                }
                ZstdTables.BLOCK_RLE -> {
                    need(1)
                    out.reserve(size)
                    out.buf.fill(src[p], out.size, out.size + size)
                    out.size += size
                    p += 1
                }
                ZstdTables.BLOCK_COMPRESSED -> {
                    need(size)
                    decodeCompressedBlock(p, p + size)
                    p += size
                }
                else -> corrupt("reserved block type")
            }
            if (last) break
        }

        val produced = out.size - frameStart
        if (contentSize >= 0 && produced.toLong() != contentSize) {
            corrupt("frame declares $contentSize bytes but decodes to $produced")
        }
        if (checksum) {
            need(4)
            val want = readIntLe(src, p)
            p += 4
            val got = XxHash64.hash(out.buf, frameStart, produced).toInt()
            if (got != want) corrupt("content checksum mismatch")
        }
        return p
    }

    private fun decodeCompressedBlock(start: Int, end: Int) {
        val (literals, litEnd) = decodeLiterals(start, end)
        decodeSequences(litEnd, end, literals)
    }

    // --- literals --------------------------------------------------------------------------------

    private fun decodeLiterals(start: Int, end: Int): Pair<ByteArray, Int> {
        if (start >= end) corrupt("compressed block has no literals section")
        val b0 = u8(src, start)
        val type = b0 and 3
        val sizeFormat = (b0 ushr 2) and 3
        fun byte(k: Int): Int {
            if (start + k >= end) corrupt("truncated literals header")
            return u8(src, start + k)
        }
        when (type) {
            0, 1 -> {
                val (size, headerSize) =
                    when (sizeFormat) {
                        0, 2 -> (b0 ushr 3) to 1
                        1 -> ((b0 ushr 4) or (byte(1) shl 4)) to 2
                        else -> ((b0 ushr 4) or (byte(1) shl 4) or (byte(2) shl 12)) to 3
                    }
                if (size > ZstdTables.BLOCK_SIZE_MAX) corrupt("literals section larger than a block")
                val body = start + headerSize
                return if (type == 0) {
                    if (size > end - body) corrupt("raw literals run past the block")
                    src.copyOfRange(body, body + size) to body + size
                } else {
                    if (body >= end) corrupt("RLE literals missing their byte")
                    ByteArray(size) { src[body] } to body + 1
                }
            }
            else -> {
                val streams4 = sizeFormat != 0
                val regen: Int
                val compressed: Int
                val headerSize: Int
                when (sizeFormat) {
                    0, 1 -> {
                        regen = (b0 ushr 4) or ((byte(1) and 0x3F) shl 4)
                        compressed = (byte(1) ushr 6) or (byte(2) shl 2)
                        headerSize = 3
                    }
                    2 -> {
                        regen = (b0 ushr 4) or (byte(1) shl 4) or ((byte(2) and 3) shl 12)
                        compressed = (byte(2) ushr 2) or (byte(3) shl 6)
                        headerSize = 4
                    }
                    else -> {
                        regen = (b0 ushr 4) or (byte(1) shl 4) or ((byte(2) and 0x3F) shl 12)
                        compressed = (byte(2) ushr 6) or (byte(3) shl 2) or (byte(4) shl 10)
                        headerSize = 5
                    }
                }
                if (regen > ZstdTables.BLOCK_SIZE_MAX) corrupt("literals section larger than a block")
                val body = start + headerSize
                if (compressed > end - body) corrupt("compressed literals run past the block")
                var streamStart = body
                if (type == 2) {
                    val (table, consumed) = readHuffmanTable(body, body + compressed)
                    huffman = table
                    streamStart += consumed
                } else if (huffman == null) {
                    corrupt("treeless literals with no previous Huffman table")
                }
                val literals = ByteArray(regen)
                decodeHuffmanStreams(huffman!!, streamStart, body + compressed, literals, streams4)
                return literals to body + compressed
            }
        }
    }

    private fun readHuffmanTable(start: Int, end: Int): Pair<HuffmanTable, Int> {
        if (start >= end) corrupt("missing Huffman tree description")
        val header = u8(src, start)
        val weights = IntArray(256)
        var count: Int
        val consumed: Int
        if (header >= 128) {
            count = header - 127
            consumed = 1 + (count + 1) / 2
            if (consumed > end - start) corrupt("truncated Huffman weights")
            for (i in 0 until count) {
                val b = u8(src, start + 1 + i / 2)
                weights[i] = if (i % 2 == 0) b ushr 4 else b and 0xF
            }
        } else {
            consumed = 1 + header
            if (header == 0 || consumed > end - start) corrupt("truncated FSE-compressed Huffman weights")
            val wEnd = start + consumed
            val counts = NormalizedCounts.read(src, start + 1, wEnd, maxSymbol = 12, maxLog = 6)
            val table = FseTable.build(counts.norm, counts.maxSymbol, counts.log)
            val bits = BackwardBits(src, start + 1 + counts.bytesRead, wEnd)
            var s1 = bits.read(table.log)
            var s2 = bits.read(table.log)
            count = 0
            // Two interleaved states; when the stream runs out, the other state's symbol is the last.
            while (true) {
                if (count > 253) corrupt("too many Huffman weights")
                weights[count++] = table.symbol[s1]
                s1 = table.base[s1] + bits.read(table.nbBits[s1])
                if (bits.overread) {
                    weights[count++] = table.symbol[s2]
                    break
                }
                weights[count++] = table.symbol[s2]
                s2 = table.base[s2] + bits.read(table.nbBits[s2])
                if (bits.overread) {
                    weights[count++] = table.symbol[s1]
                    break
                }
            }
        }
        if (count > 255) corrupt("too many Huffman weights")
        var total = 0
        for (i in 0 until count) {
            val w = weights[i]
            if (w > 11) corrupt("Huffman weight $w too large")
            if (w > 0) total += 1 shl (w - 1)
        }
        if (total == 0) corrupt("Huffman weights are all zero")
        val log = highBit(total) + 1
        if (log > 11) corrupt("Huffman table deeper than 11 bits")
        val rest = (1 shl log) - total
        if (rest and (rest - 1) != 0) corrupt("Huffman weights do not complete a prefix code")
        weights[count++] = highBit(rest) + 1
        val rank = IntArray(13)
        for (i in 0 until count) rank[weights[i]]++
        if (rank[1] < 2 || rank[1] and 1 != 0) corrupt("Huffman weights do not complete a prefix code")

        val entries = IntArray(1 shl log)
        val next = IntArray(13)
        var pos = 0
        for (w in 1..log) {
            next[w] = pos
            pos += rank[w] shl (w - 1)
        }
        for (s in 0 until count) {
            val w = weights[s]
            if (w == 0) continue
            val len = 1 shl (w - 1)
            entries.fill((s shl 8) or (log + 1 - w), next[w], next[w] + len)
            next[w] += len
        }
        return HuffmanTable(log, entries) to consumed
    }

    private fun decodeHuffmanStreams(table: HuffmanTable, start: Int, end: Int, out: ByteArray, streams4: Boolean) {
        if (!streams4) {
            decodeHuffmanStream(table, start, end, out, 0, out.size)
            return
        }
        if (end - start < 10) corrupt("4-stream literals too short for a jump table")
        val s1 = u8(src, start) or (u8(src, start + 1) shl 8)
        val s2 = u8(src, start + 2) or (u8(src, start + 3) shl 8)
        val s3 = u8(src, start + 4) or (u8(src, start + 5) shl 8)
        val b1 = start + 6
        val b2 = b1 + s1
        val b3 = b2 + s2
        val b4 = b3 + s3
        if (b4 >= end) corrupt("Huffman jump table points past the literals")
        val seg = (out.size + 3) / 4
        val last = out.size - 3 * seg
        if (last < 0) corrupt("4-stream literals too short to split")
        decodeHuffmanStream(table, b1, b2, out, 0, seg)
        decodeHuffmanStream(table, b2, b3, out, seg, seg)
        decodeHuffmanStream(table, b3, b4, out, 2 * seg, seg)
        decodeHuffmanStream(table, b4, end, out, 3 * seg, last)
    }

    private fun decodeHuffmanStream(table: HuffmanTable, start: Int, end: Int, out: ByteArray, at: Int, n: Int) {
        val bits = BackwardBits(src, start, end)
        val log = table.log
        for (i in at until at + n) {
            val e = bits.peek(log)
            out[i] = table.symbol(e).toByte()
            bits.skip(table.nbBits(e))
        }
        if (!bits.finished) corrupt("Huffman stream not consumed exactly")
    }

    // --- sequences -------------------------------------------------------------------------------

    private fun decodeSequences(start: Int, end: Int, literals: ByteArray) {
        if (start >= end) corrupt("compressed block has no sequences section")
        var q = start
        val b0 = u8(src, q++)
        val nbSeq =
            when {
                b0 < 128 -> b0
                b0 < 255 -> {
                    if (q >= end) corrupt("truncated sequence count")
                    ((b0 - 128) shl 8) + u8(src, q++)
                }
                else -> {
                    if (q + 1 >= end) corrupt("truncated sequence count")
                    val v = u8(src, q) + (u8(src, q + 1) shl 8) + 0x7F00
                    q += 2
                    v
                }
            }
        if (nbSeq == 0) {
            if (q != end) corrupt("bytes after an empty sequences section")
            out.reserve(literals.size)
            literals.copyInto(out.buf, out.size)
            out.size += literals.size
            return
        }
        if (q >= end) corrupt("missing sequence compression modes")
        val modes = u8(src, q++)
        if (modes and 3 != 0) corrupt("reserved sequence mode bits are set")
        q = readTable(q, end, modes ushr 6, Kind.LL)
        q = readTable(q, end, (modes ushr 4) and 3, Kind.OF)
        q = readTable(q, end, (modes ushr 2) and 3, Kind.ML)
        val ll = llTable!!
        val of = ofTable!!
        val ml = mlTable!!

        val bits = BackwardBits(src, q, end)
        var llState = bits.read(ll.log)
        var ofState = bits.read(of.log)
        var mlState = bits.read(ml.log)
        var litPos = 0
        for (n in 0 until nbSeq) {
            val ofCode = of.symbol[ofState]
            val mlCode = ml.symbol[mlState]
            val llCode = ll.symbol[llState]
            // Codes above 30 name offsets past 2GB, beyond any output this decoder can produce.
            if (ofCode > 30) corrupt("offset code $ofCode")
            val ofValue = (1 shl ofCode) + bits.read(ofCode)
            val matchLen = ZstdTables.ML_BASE[mlCode] + bits.read(ZstdTables.ML_BITS[mlCode])
            val litLen = ZstdTables.LL_BASE[llCode] + bits.read(ZstdTables.LL_BITS[llCode])
            if (bits.overread) corrupt("sequence bitstream overread")

            val offset: Int
            if (ofValue > 3) {
                offset = ofValue - 3
                reps[2] = reps[1]
                reps[1] = reps[0]
                reps[0] = offset
            } else {
                val idx = ofValue - 1 + (if (litLen == 0) 1 else 0)
                when (idx) {
                    0 -> offset = reps[0]
                    1 -> {
                        offset = reps[1]
                        reps[1] = reps[0]
                        reps[0] = offset
                    }
                    else -> {
                        offset = if (idx == 2) reps[2] else reps[0] - 1
                        reps[2] = reps[1]
                        reps[1] = reps[0]
                        reps[0] = offset
                    }
                }
            }

            if (litLen > literals.size - litPos) corrupt("sequence copies more literals than the block holds")
            out.reserve(litLen)
            literals.copyInto(out.buf, out.size, litPos, litPos + litLen)
            out.size += litLen
            litPos += litLen

            if (offset <= 0 || offset > out.size - frameStart) corrupt("match offset $offset reaches before the frame")
            out.reserve(matchLen)
            val buf = out.buf
            var from = out.size - offset
            var to = out.size
            if (offset >= matchLen) {
                buf.copyInto(buf, to, from, from + matchLen)
            } else {
                repeat(matchLen) { buf[to++] = buf[from++] }
            }
            out.size += matchLen

            if (n != nbSeq - 1) {
                llState = ll.base[llState] + bits.read(ll.nbBits[llState])
                mlState = ml.base[mlState] + bits.read(ml.nbBits[mlState])
                ofState = of.base[ofState] + bits.read(of.nbBits[ofState])
            }
        }
        if (!bits.finished) corrupt("sequence bitstream not consumed exactly")
        val rest = literals.size - litPos
        out.reserve(rest)
        literals.copyInto(out.buf, out.size, litPos, literals.size)
        out.size += rest
    }

    private enum class Kind(val maxSymbol: Int, val maxLog: Int) {
        LL(ZstdTables.LL_MAX_SYMBOL, ZstdTables.LL_MAX_LOG),
        OF(ZstdTables.OF_MAX_SYMBOL, ZstdTables.OF_MAX_LOG),
        ML(ZstdTables.ML_MAX_SYMBOL, ZstdTables.ML_MAX_LOG),
    }

    /** Reads (or selects) the table for [kind] in [mode]; returns the offset after its description. */
    private fun readTable(q: Int, end: Int, mode: Int, kind: Kind): Int {
        var next = q
        val table: FseTable =
            when (mode) {
                0 ->
                    when (kind) {
                        Kind.LL -> FseTable.LL_DEFAULT
                        Kind.OF -> FseTable.OF_DEFAULT
                        Kind.ML -> FseTable.ML_DEFAULT
                    }
                1 -> {
                    if (q >= end) corrupt("missing RLE symbol")
                    val s = u8(src, q)
                    if (s > kind.maxSymbol) corrupt("RLE symbol $s out of range")
                    next = q + 1
                    FseTable.rle(s)
                }
                2 -> {
                    val counts = NormalizedCounts.read(src, q, end, kind.maxSymbol, kind.maxLog)
                    next = q + counts.bytesRead
                    FseTable.build(counts.norm, counts.maxSymbol, counts.log)
                }
                else ->
                    when (kind) {
                        Kind.LL -> llTable
                        Kind.OF -> ofTable
                        Kind.ML -> mlTable
                    } ?: corrupt("repeat sequence table with no previous table")
            }
        when (kind) {
            Kind.LL -> llTable = table
            Kind.OF -> ofTable = table
            Kind.ML -> mlTable = table
        }
        return next
    }
}
