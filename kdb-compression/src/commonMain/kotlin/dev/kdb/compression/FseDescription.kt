package dev.kdb.compression

import dev.kdb.compression.ZstdTables.highBit

/**
 * Building custom FSE tables for [ZstdEncoder]: normalizing symbol counts, writing the table
 * description, choosing an accuracy log, and estimating coded sizes. Integer-only, so every
 * Kotlin target makes the same choices and writes the same bytes.
 */
internal object FseDescription {
    /**
     * The accuracy log for coding [n] symbols whose largest value is [maxSymbol]: the reference's
     * FSE_optimalTableLog, widened if needed so every distinct symbol gets a cell. -1 when even
     * [maxLog] is too small for [distinct] symbols.
     */
    fun optimalLog(n: Int, maxSymbol: Int, distinct: Int, maxLog: Int): Int {
        val maxBitsSrc = highBit(maxOf(n - 1, 1)) - 2
        val minBits = minOf(highBit(n) + 1, highBit(maxOf(maxSymbol, 1)) + 2)
        var log = minOf(maxLog, maxBitsSrc)
        if (minBits > log) log = minBits
        log = log.coerceIn(5, maxLog)
        while ((1 shl log) < distinct) log++
        return if (log > maxLog) -1 else log
    }

    /** log2([n]) in 1/256ths of a bit, linearly interpolated; [n] >= 1. */
    private fun log2Fixed(n: Int): Int {
        val hb = highBit(n)
        return (hb shl 8) + (((n - (1 shl hb)) shl 8) ushr hb)
    }

    /**
     * Approximate bits, in 1/256ths, to code [counts] (indexed by symbol, [maxCountSymbol] the
     * last used) with distribution [norm] of accuracy [log]; Int.MAX_VALUE if a used symbol has no
     * probability there. A "less than 1" (-1) symbol costs a full [log] bits.
     */
    fun cost256(counts: IntArray, maxCountSymbol: Int, norm: ShortArray, log: Int): Long {
        var total = 0L
        for (s in 0..maxCountSymbol) {
            val c = counts[s]
            if (c == 0) continue
            val n = if (s < norm.size) norm[s].toInt() else 0
            if (n == 0) return Long.MAX_VALUE
            val p = if (n < 0) 1 else n
            total += c.toLong() * ((log shl 8) - log2Fixed(p))
        }
        return total
    }

    /** Scales [counts] (summing to [total]) to a distribution summing to 2^[log], every used symbol >= 1. */
    fun normalize(counts: IntArray, total: Int, log: Int): ShortArray {
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
    fun write(sink: ByteSink, norm: ShortArray, log: Int) {
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
