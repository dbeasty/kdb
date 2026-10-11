package dev.kdb.compression

import dev.kdb.compression.ZstdTables.highBit

/**
 * Writes a block's literals section (RFC 8878 §3.1.1.3.1) for [ZstdEncoder]: Huffman-coded when
 * that is smaller, RLE when every literal is the same byte, raw otherwise.
 *
 * The Huffman table is described with whichever of 4-bit direct weights (only possible when the
 * highest literal byte is below 129) and FSE-compressed weights is smaller. One stream for fewer
 * than 256 literals, four streams above that, as libzstd does. Every block carries its own table;
 * the encoder never emits treeless literals.
 */
internal object ZstdLiteralsEncoder {
    private const val MAX_BITS = 11
    private const val WEIGHT_TABLE_LOG = 6

    fun write(sink: ByteSink, literals: ByteArray, n: Int) {
        if (n == 0) {
            sink.byte(0) // raw, size 0
            return
        }
        val freq = IntArray(256)
        for (i in 0 until n) freq[literals[i].toInt() and 0xFF]++
        val distinct = freq.count { it > 0 }
        if (distinct == 1) {
            writeRawOrRleHeader(sink, type = 1, n)
            sink.byte(literals[0].toInt())
            return
        }
        if (n >= 64) {
            val huff = ByteSink(n + 64)
            if (writeHuffman(huff, literals, n, freq) && huff.size < rawSectionSize(n)) {
                sink.bytes(huff.buf, 0, huff.size)
                return
            }
        }
        writeRawOrRleHeader(sink, type = 0, n)
        sink.bytes(literals, 0, n)
    }

    private fun rawSectionSize(n: Int): Int = n + (if (n < 32) 1 else if (n < 4096) 2 else 3)

    private fun writeRawOrRleHeader(sink: ByteSink, type: Int, n: Int) {
        when {
            n < 32 -> sink.byte(type or (n shl 3))
            n < 4096 -> sink.le(type or (1 shl 2) or (n shl 4), 2)
            else -> sink.le(type or (3 shl 2) or (n shl 4), 3)
        }
    }

    /** Writes a compressed (type 2) literals section into [out]; false if it cannot be expressed. */
    private fun writeHuffman(out: ByteSink, literals: ByteArray, n: Int, freq: IntArray): Boolean {
        val lengths = codeLengths(freq)
        val maxBits = lengths.max()
        val lastSymbol = lengths.indexOfLast { it > 0 }
        // Weights for symbols 0 until lastSymbol; the last symbol's weight is implied.
        val weights = IntArray(lastSymbol) { s -> if (lengths[s] == 0) 0 else maxBits + 1 - lengths[s] }
        // Whichever weight description is smaller, as libzstd does: direct 4-bit weights only reach
        // symbol 128, and FSE wins whenever the weights are mostly zeros (a few ASCII letters).
        val direct =
            if (lastSymbol > 128) {
                null
            } else {
                ByteSink(65).also { d ->
                    d.byte(127 + lastSymbol)
                    for (i in 0 until lastSymbol step 2) {
                        d.byte((weights[i] shl 4) or (if (i + 1 < lastSymbol) weights[i + 1] else 0))
                    }
                }
            }
        val fse = fseWeights(weights)
        val tree =
            when {
                direct == null -> fse ?: return false
                fse == null -> direct
                else -> if (fse.size < direct.size) fse else direct
            }

        // Canonical codes, assigned exactly as the decoder builds its table (ZstdDecoder.readHuffmanTable).
        val codes = IntArray(256)
        val rank = IntArray(MAX_BITS + 2)
        for (s in 0..lastSymbol) if (lengths[s] > 0) rank[maxBits + 1 - lengths[s]]++
        val next = IntArray(MAX_BITS + 2)
        var pos = 0
        for (w in 1..maxBits) {
            next[w] = pos
            pos += rank[w] shl (w - 1)
        }
        for (s in 0..lastSymbol) {
            if (lengths[s] == 0) continue
            val w = maxBits + 1 - lengths[s]
            codes[s] = next[w] ushr (w - 1)
            next[w] += 1 shl (w - 1)
        }

        val body = ByteSink(n + 16)
        body.bytes(tree.buf, 0, tree.size)
        val fourStreams = n >= 256
        if (fourStreams) {
            val seg = (n + 3) / 4
            val jumpAt = body.size
            body.le(0, 6)
            val sizes = IntArray(3)
            for (k in 0 until 4) {
                val from = k * seg
                val to = minOf(n, from + seg)
                val before = body.size
                writeStream(body, literals, from, to, codes, lengths)
                if (k < 3) sizes[k] = body.size - before
            }
            if (sizes.any { it > 0xFFFF }) return false
            for (k in 0 until 3) {
                body.buf[jumpAt + 2 * k] = sizes[k].toByte()
                body.buf[jumpAt + 2 * k + 1] = (sizes[k] ushr 8).toByte()
            }
        } else {
            writeStream(body, literals, 0, n, codes, lengths)
        }

        val c = body.size
        when {
            !fourStreams && n <= 1023 && c <= 1023 -> out.le(2 or (0 shl 2) or (n shl 4) or (c shl 14), 3)
            fourStreams && n <= 1023 && c <= 1023 -> out.le(2 or (1 shl 2) or (n shl 4) or (c shl 14), 3)
            fourStreams && n <= 16383 && c <= 16383 -> out.le(2 or (2 shl 2) or (n shl 4) or (c shl 18), 4)
            fourStreams && n <= 262143 && c <= 262143 -> {
                out.le(2 or (3 shl 2) or (n shl 4) or ((c and 0x3FF) shl 22), 4)
                out.byte(c ushr 10)
            }
            else -> return false
        }
        out.bytes(body.buf, 0, c)
        return true
    }

    /** One Huffman stream: written last literal first, so the backward reader meets the first first. */
    private fun writeStream(sink: ByteSink, literals: ByteArray, from: Int, to: Int, codes: IntArray, lengths: IntArray) {
        val bits = BitWriter(sink)
        for (i in to - 1 downTo from) {
            val s = literals[i].toInt() and 0xFF
            bits.add(codes[s], lengths[s])
        }
        bits.close()
    }

    /**
     * Huffman code lengths for [freq], at most [MAX_BITS] long. When the optimal tree is deeper,
     * the frequencies are halved (rounding up, so no symbol drops out) and the tree rebuilt - a
     * flatter, still complete, prefix code.
     */
    private fun codeLengths(freq: IntArray): IntArray {
        val f = freq.copyOf()
        while (true) {
            val lengths = huffmanLengths(f)
            if (lengths.max() <= MAX_BITS) return lengths
            for (s in 0 until 256) if (f[s] > 0) f[s] = (f[s] + 1) / 2
        }
    }

    private fun huffmanLengths(freq: IntArray): IntArray {
        val symbols = (0 until 256).filter { freq[it] > 0 }
        val nodes = symbols.size
        val weight = LongArray(2 * nodes)
        val parent = IntArray(2 * nodes) { -1 }
        for ((i, s) in symbols.withIndex()) weight[i] = freq[s].toLong()
        val alive = BooleanArray(2 * nodes)
        for (i in 0 until nodes) alive[i] = true
        var count = nodes
        repeat(nodes - 1) {
            var a = -1
            var b = -1
            for (i in 0 until count) {
                if (!alive[i]) continue
                if (a < 0 || weight[i] < weight[a]) {
                    b = a
                    a = i
                } else if (b < 0 || weight[i] < weight[b]) {
                    b = i
                }
            }
            alive[a] = false
            alive[b] = false
            weight[count] = weight[a] + weight[b]
            parent[a] = count
            parent[b] = count
            alive[count] = true
            count++
        }
        val lengths = IntArray(256)
        for ((i, s) in symbols.withIndex()) {
            var d = 0
            var p = i
            while (parent[p] >= 0) {
                p = parent[p]
                d++
            }
            lengths[s] = d
        }
        return lengths
    }

    /**
     * FSE-compressed Huffman weights (RFC 8878 §4.2.1.2): a table description, then the weights
     * coded with two interleaved states, laid out as the reference FSE_compress_usingCTable does.
     * Null when the weights cannot be expressed this way: fewer than two of them, a single
     * distinct value (an FSE stream whose states read no bits never ends - libzstd's
     * HUF_compressWeights declines the same case), or a result that does not fit the 1-byte size
     * header (< 128 bytes).
     */
    private fun fseWeights(weights: IntArray): ByteSink? {
        if (weights.size < 2) return null
        val maxWeight = weights.max()
        val counts = IntArray(maxWeight + 1)
        for (w in weights) counts[w]++
        if (counts.count { it > 0 } < 2) return null
        val norm = normalize(counts, weights.size, WEIGHT_TABLE_LOG)
        val body = ByteSink(160)
        writeNormalizedCounts(body, norm, WEIGHT_TABLE_LOG)
        val enc = FseEncoder(norm, WEIGHT_TABLE_LOG)
        val bits = BitWriter(body)
        var i = weights.size
        val s1: FseEncoder.State
        val s2: FseEncoder.State
        if (weights.size and 1 == 1) {
            s1 = enc.start(weights[--i])
            s2 = enc.start(weights[--i])
            s1.encode(bits, weights[--i])
        } else {
            s2 = enc.start(weights[--i])
            s1 = enc.start(weights[--i])
        }
        while (i > 0) {
            s2.encode(bits, weights[--i])
            s1.encode(bits, weights[--i])
        }
        s2.flush(bits)
        s1.flush(bits)
        bits.close()
        if (body.size >= 128) return null
        return ByteSink(body.size + 1).also {
            it.byte(body.size)
            it.bytes(body.buf, 0, body.size)
        }
    }

    /** Scales [counts] (summing to [total]) to a distribution summing to 2^[log], every used symbol >= 1. */
    private fun normalize(counts: IntArray, total: Int, log: Int): ShortArray {
        val size = 1 shl log
        val norm = IntArray(counts.size) { s -> if (counts[s] == 0) 0 else maxOf(1, counts[s] * size / total) }
        var sum = norm.sum()
        while (sum != size) {
            var best = -1
            for (s in norm.indices) {
                if (norm[s] == 0 || (sum > size && norm[s] == 1)) continue
                if (best < 0 || norm[s] > norm[best]) best = s
            }
            if (sum > size) norm[best]-- else norm[best]++
            sum += if (sum > size) -1 else 1
        }
        return ShortArray(norm.size) { norm[it].toShort() }
    }

    /** The FSE table description the decoder's NormalizedCounts.read parses (RFC 8878 §4.1.1). */
    private fun writeNormalizedCounts(sink: ByteSink, norm: ShortArray, log: Int) {
        val bits = ForwardBits(sink)
        bits.add(log - 5, 4)
        var remaining = (1 shl log) + 1
        var threshold = 1 shl log
        var nbBits = log + 1
        var symbol = 0
        var previousZero = false
        while (remaining > 1) {
            if (previousZero) {
                val start = symbol
                while (norm[symbol].toInt() == 0) symbol++
                var run = symbol - start
                while (run >= 3) {
                    bits.add(3, 2)
                    run -= 3
                }
                bits.add(run, 2)
            }
            var count = norm[symbol++].toInt()
            val max = (2 * threshold - 1) - remaining
            remaining -= if (count < 0) -count else count
            count++
            if (count >= threshold) count += max
            bits.add(count, if (count < max) nbBits - 1 else nbBits)
            previousZero = count == 1
            while (remaining < threshold) {
                nbBits--
                threshold = threshold shr 1
            }
        }
        bits.flush()
    }

    /** Little-endian bit writer with no end marker - for the forward-read table description. */
    private class ForwardBits(private val sink: ByteSink) {
        private var acc = 0
        private var count = 0

        fun add(value: Int, n: Int) {
            acc = acc or (value shl count)
            count += n
            while (count >= 8) {
                sink.byte(acc and 0xFF)
                acc = acc ushr 8
                count -= 8
            }
        }

        fun flush() {
            if (count > 0) sink.byte(acc and 0xFF)
        }
    }
}
