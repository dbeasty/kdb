package dev.kdb.compression

import dev.kdb.compression.ZstdTables.highBit

/**
 * A small pure-Kotlin zstd encoder (RFC 8878), the [ZstdCompression.compress] of Kotlin/JS and
 * Kotlin/Native. The JVM keeps zstd-jni.
 *
 * It writes ordinary zstd frames that any conforming decoder reads - Go's klauspost/compress,
 * zstd-jni and [ZstdDecoder] all do (ZstdEncoderInteropTest, go/kdb/interop) - but trades ratio
 * for size and simplicity:
 *  - one greedy hash-table match finder (4-byte minimum match, matches may reach back across
 *    blocks to the start of the frame) that tries the most recent offset first, and codes
 *    offsets as repeat offsets whenever the decoder's history allows;
 *  - literals Huffman-coded per block when that is smaller (ZstdLiteralsEncoder), never reusing
 *    the previous block's table;
 *  - sequence codes per block as RLE, predefined or custom FSE tables, whichever is smallest
 *    (ZstdSequenceTables) - never repeat mode;
 *  - a block that does not shrink is stored raw.
 * Every frame is single-segment, declares its content size, and carries the XXH64 content
 * checksum (as Go's frames do), so a damaged frame fails to decode rather than misdecoding. The
 * output is deterministic: the same input gives the same bytes on every target.
 */
internal object ZstdEncoder {
    private const val MIN_MATCH = 4

    fun compress(input: ByteArray): ByteArray {
        val out = ByteSink(input.size / 2 + 32)
        out.int(ZstdTables.MAGIC)
        val size = input.size.toLong()
        val fcsFlag =
            when {
                size < 256 -> 0
                size < 65536 + 256 -> 1
                else -> 2
            }
        out.byte((fcsFlag shl 6) or 0x20 or 0x04) // single segment, content checksum, no dictionary
        when (fcsFlag) {
            0 -> out.byte(input.size)
            1 -> out.le(input.size - 256, 2)
            else -> out.int(input.size)
        }

        val block = BlockEncoder(input)
        if (input.isEmpty()) {
            out.le(1, 3) // last, raw, size 0
        }
        var start = 0
        while (start < input.size) {
            val end = minOf(input.size, start + ZstdTables.BLOCK_SIZE_MAX)
            val last = end == input.size
            val compressed = block.encode(start, end)
            if (compressed == null) {
                out.le((if (last) 1 else 0) or (ZstdTables.BLOCK_RAW shl 1) or ((end - start) shl 3), 3)
                out.bytes(input, start, end)
            } else {
                out.le((if (last) 1 else 0) or (ZstdTables.BLOCK_COMPRESSED shl 1) or (compressed.size shl 3), 3)
                out.bytes(compressed.buf, 0, compressed.size)
            }
            start = end
        }
        out.int(XxHash64.hash(input).toInt())
        return out.toByteArray()
    }

    /**
     * Encodes the blocks of one frame. Scratch space is sized to the input, so compressing a small
     * commit payload does not allocate block-sized buffers; the match-finder table is shared by
     * all blocks of the frame, which is what lets a match reach back into an earlier block.
     */
    private class BlockEncoder(private val src: ByteArray) {
        private val maxBlock = minOf(src.size, ZstdTables.BLOCK_SIZE_MAX)
        private val hashLog = (highBit(maxOf(src.size, 1)) + 1).coerceIn(8, 16)
        private val table = IntArray(1 shl hashLog) { -1 }
        private val literals = ByteArray(maxBlock)
        private val litLens = IntArray(maxBlock / MIN_MATCH + 1)
        private val matchLens = IntArray(litLens.size)
        private val ofValues = IntArray(litLens.size)
        private val sink = ByteSink(maxBlock + 64)

        private val reps = RepeatOffsets()

        /** The compressed block body for [start, end), or null when storing it raw is no larger. */
        fun encode(start: Int, end: Int): ByteSink? {
            // The history persists across the frame's compressed blocks, but a block stored raw
            // does not touch the decoder's, so roll back what this block's parse did to ours.
            val saved = reps.snapshot()
            val body = encodeBody(start, end)
            if (body == null) reps.restore(saved)
            return body
        }

        private fun encodeBody(start: Int, end: Int): ByteSink? {
            var nbSeq = 0
            var litSize = 0
            var pos = start
            var litStart = start
            val limit = end - MIN_MATCH
            while (pos <= limit) {
                val v = readInt(src, pos)
                val h = hash(v)
                var cand = table[h]
                table[h] = pos
                // The most recent offset first: structured data repeats at a fixed stride, and a
                // repeat offset costs almost nothing to code.
                val rep = pos - reps.recent
                if (pos > litStart && rep >= 0 && readInt(src, rep) == v) cand = rep
                if (cand >= 0 && readInt(src, cand) == v) {
                    var len = MIN_MATCH
                    while (pos + len < end && src[cand + len] == src[pos + len]) len++
                    var s = pos
                    var c = cand
                    while (s > litStart && c > 0 && src[s - 1] == src[c - 1]) {
                        s--
                        c--
                        len++
                    }
                    src.copyInto(literals, litSize, litStart, s)
                    litSize += s - litStart
                    litLens[nbSeq] = s - litStart
                    ofValues[nbSeq] = reps.encode(s - litStart, s - c)
                    matchLens[nbSeq] = len
                    nbSeq++
                    pos = s + len
                    litStart = pos
                    if (pos - 2 <= limit) table[hash(readInt(src, pos - 2))] = pos - 2
                } else {
                    pos += 1 + ((pos - litStart) ushr 6)
                }
            }
            src.copyInto(literals, litSize, litStart, end)
            litSize += end - litStart

            sink.reset()
            ZstdLiteralsEncoder.write(sink, literals, litSize)
            when {
                nbSeq < 128 -> sink.byte(nbSeq)
                nbSeq < 0x7F00 -> {
                    sink.byte((nbSeq ushr 8) + 128)
                    sink.byte(nbSeq and 0xFF)
                }
                else -> {
                    sink.byte(255)
                    sink.le(nbSeq - 0x7F00, 2)
                }
            }
            if (nbSeq > 0) writeSequences(nbSeq)
            return if (sink.size < end - start) sink else null
        }

        private fun hash(v: Int): Int = (v * -1640531535) ushr (32 - hashLog) // 0x9E3779B1

        /**
         * The sequences bitstream (RFC 8878 §3.1.1.3.2.2): written last sequence first, so the
         * decoder, reading backwards, meets the first sequence first.
         */
        private fun writeSequences(nbSeq: Int) {
            val llCode = IntArray(nbSeq)
            val mlCode = IntArray(nbSeq)
            val ofCode = IntArray(nbSeq)
            for (i in 0 until nbSeq) {
                llCode[i] = code(ZstdTables.LL_BASE, litLens[i])
                mlCode[i] = code(ZstdTables.ML_BASE, matchLens[i])
                ofCode[i] = highBit(ofValues[i])
            }
            val llChoice = ZstdSequenceTables.choose(llCode, nbSeq, ZstdSequenceTables.Kind.LL)
            val ofChoice = ZstdSequenceTables.choose(ofCode, nbSeq, ZstdSequenceTables.Kind.OF)
            val mlChoice = ZstdSequenceTables.choose(mlCode, nbSeq, ZstdSequenceTables.Kind.ML)
            sink.byte((llChoice.mode shl 6) or (ofChoice.mode shl 4) or (mlChoice.mode shl 2))
            for (c in listOf(llChoice, ofChoice, mlChoice)) sink.bytes(c.description, 0, c.description.size)

            val bits = BitWriter(sink)
            val n = nbSeq - 1
            val ml = mlChoice.coder.also { it.start(mlCode[n]) }
            val of = ofChoice.coder.also { it.start(ofCode[n]) }
            val ll = llChoice.coder.also { it.start(llCode[n]) }
            extraBits(bits, n, llCode[n], mlCode[n], ofCode[n])
            for (i in n - 1 downTo 0) {
                of.encode(bits, ofCode[i])
                ml.encode(bits, mlCode[i])
                ll.encode(bits, llCode[i])
                extraBits(bits, i, llCode[i], mlCode[i], ofCode[i])
            }
            ml.flush(bits)
            of.flush(bits)
            ll.flush(bits)
            bits.close()
        }

        private fun extraBits(bits: BitWriter, i: Int, ll: Int, ml: Int, of: Int) {
            bits.add(litLens[i] - ZstdTables.LL_BASE[ll], ZstdTables.LL_BITS[ll])
            bits.add(matchLens[i] - ZstdTables.ML_BASE[ml], ZstdTables.ML_BITS[ml])
            bits.add(ofValues[i] - (1 shl of), of)
        }
    }

    /** The largest code whose baseline is <= [value]. */
    private fun code(base: IntArray, value: Int): Int {
        var lo = 0
        var hi = base.size - 1
        while (lo < hi) {
            val mid = (lo + hi + 1) ushr 1
            if (base[mid] <= value) lo = mid else hi = mid - 1
        }
        return lo
    }


    private fun readInt(b: ByteArray, p: Int): Int =
        (b[p].toInt() and 0xFF) or ((b[p + 1].toInt() and 0xFF) shl 8) or
            ((b[p + 2].toInt() and 0xFF) shl 16) or ((b[p + 3].toInt() and 0xFF) shl 24)
}

/**
 * The decoder's repeat-offset history (RFC 8878 §3.1.2.5), kept by the encoder so it can code an
 * offset the decoder already holds as Offset_Value 1-3 - and must keep in exact step, since a
 * repeat code means whatever the decoder's history says.
 */
internal class RepeatOffsets {
    private val reps = intArrayOf(1, 4, 8)

    /** The most recently used offset. */
    val recent: Int get() = reps[0]

    fun snapshot(): IntArray = reps.copyOf()

    fun restore(saved: IntArray) {
        saved.copyInto(reps)
    }

    /** The Offset_Value that makes the decoder use [offset] after [litLen] literals; updates the history as it will. */
    fun encode(litLen: Int, offset: Int): Int {
        val value =
            if (litLen > 0) {
                when (offset) {
                    reps[0] -> 1
                    reps[1] -> 2
                    reps[2] -> 3
                    else -> offset + 3
                }
            } else {
                when (offset) {
                    reps[1] -> 1
                    reps[2] -> 2
                    reps[0] - 1 -> 3
                    else -> offset + 3
                }
            }
        // The decoder's update (ZstdDecoder.decodeSequences): a new offset or anything but
        // "repeat 1" moves to the front; "repeat 2" swaps with the front.
        val idx = if (value > 3) 3 else value - 1 + (if (litLen == 0) 1 else 0)
        when (idx) {
            0 -> {}
            1 -> {
                reps[1] = reps[0]
                reps[0] = offset
            }
            else -> {
                reps[2] = reps[1]
                reps[1] = reps[0]
                reps[0] = offset
            }
        }
        return value
    }
}

/** FSE encoding table for one predefined distribution (the reference's FSE_buildCTable). */
internal class FseEncoder(norm: ShortArray, private val log: Int) {
    private val stateTable: IntArray
    private val deltaNbBits: IntArray
    private val deltaFindState: IntArray

    init {
        val maxSymbol = norm.size - 1
        val size = 1 shl log
        val cells = ZstdTables.spreadSymbols(norm, maxSymbol, log)
        val cumul = IntArray(maxSymbol + 2)
        for (s in 0..maxSymbol) cumul[s + 1] = cumul[s] + (if (norm[s].toInt() == -1) 1 else norm[s].toInt())
        stateTable = IntArray(size)
        for (u in 0 until size) stateTable[cumul[cells[u]]++] = size + u
        deltaNbBits = IntArray(maxSymbol + 1)
        deltaFindState = IntArray(maxSymbol + 1)
        var total = 0
        for (s in 0..maxSymbol) {
            when (val n = norm[s].toInt()) {
                0 -> deltaNbBits[s] = ((log + 1) shl 16) - size
                -1, 1 -> {
                    deltaNbBits[s] = (log shl 16) - size
                    deltaFindState[s] = total - 1
                    total++
                }
                else -> {
                    val maxBitsOut = log - highBit(n - 1)
                    deltaNbBits[s] = (maxBitsOut shl 16) - (n shl maxBitsOut)
                    deltaFindState[s] = total - n
                    total += n
                }
            }
        }
    }

    inner class State(var value: Int) {
        fun encode(bits: BitWriter, symbol: Int) {
            val nbBits = (value + deltaNbBits[symbol]) ushr 16
            bits.add(value and ((1 shl nbBits) - 1), nbBits)
            value = stateTable[(value ushr nbBits) + deltaFindState[symbol]]
        }

        fun flush(bits: BitWriter) = bits.add(value and ((1 shl log) - 1), log)
    }

    fun start(symbol: Int): State {
        val nbBits = (deltaNbBits[symbol] + (1 shl 15)) ushr 16
        val v = (nbBits shl 16) - deltaNbBits[symbol]
        return State(stateTable[(v ushr nbBits) + deltaFindState[symbol]])
    }

    companion object {
        val LL = FseEncoder(ZstdTables.LL_DEFAULT_NORM, ZstdTables.LL_DEFAULT_LOG)
        val ML = FseEncoder(ZstdTables.ML_DEFAULT_NORM, ZstdTables.ML_DEFAULT_LOG)
        val OF = FseEncoder(ZstdTables.OF_DEFAULT_NORM, ZstdTables.OF_DEFAULT_LOG)
    }
}

/** Forward little-endian bit writer; [close] adds the end marker a backward reader starts from. */
internal class BitWriter(private val sink: ByteSink) {
    private var acc = 0
    private var count = 0

    fun add(value: Int, n: Int) {
        if (n > 24) {
            add(value and 0xFFFF, 16)
            add(value ushr 16, n - 16)
            return
        }
        acc = acc or (value shl count)
        count += n
        while (count >= 8) {
            sink.byte(acc and 0xFF)
            acc = acc ushr 8
            count -= 8
        }
    }

    fun close() {
        add(1, 1)
        if (count > 0) sink.byte(acc and 0xFF)
    }
}

internal class ByteSink(initial: Int) {
    var buf = ByteArray(maxOf(16, initial))
    var size = 0

    private fun ensure(n: Int) {
        if (size + n > buf.size) buf = buf.copyOf(maxOf(size + n, buf.size * 2))
    }

    fun reset() {
        size = 0
    }

    fun byte(v: Int) {
        ensure(1)
        buf[size++] = v.toByte()
    }

    fun le(v: Int, n: Int) {
        ensure(n)
        for (k in 0 until n) buf[size++] = (v ushr (8 * k)).toByte()
    }

    fun int(v: Int) = le(v, 4)

    fun bytes(src: ByteArray, from: Int, to: Int) {
        ensure(to - from)
        src.copyInto(buf, size, from, to)
        size += to - from
    }

    fun toByteArray(): ByteArray = buf.copyOf(size)
}
