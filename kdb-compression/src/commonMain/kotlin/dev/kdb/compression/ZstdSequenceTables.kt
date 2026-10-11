package dev.kdb.compression

/**
 * Chooses, per block, how each of the three sequence code streams (literal lengths, offsets,
 * match lengths) is coded (RFC 8878 §3.1.1.3.2.1): RLE when one code is used throughout, the
 * predefined distribution, or a custom FSE table built from this block's counts - whichever
 * [FseDescription.cost256] says is smallest, description included. Repeat mode (reusing the
 * previous block's table) is never chosen.
 */
internal object ZstdSequenceTables {
    enum class Kind(
        val maxSymbol: Int,
        val maxLog: Int,
        val defaultNorm: ShortArray,
        val defaultLog: Int,
        val default: FseEncoder,
    ) {
        LL(ZstdTables.LL_MAX_SYMBOL, ZstdTables.LL_MAX_LOG, ZstdTables.LL_DEFAULT_NORM, ZstdTables.LL_DEFAULT_LOG, FseEncoder.LL),
        OF(ZstdTables.OF_MAX_SYMBOL, ZstdTables.OF_MAX_LOG, ZstdTables.OF_DEFAULT_NORM, ZstdTables.OF_DEFAULT_LOG, FseEncoder.OF),
        ML(ZstdTables.ML_MAX_SYMBOL, ZstdTables.ML_MAX_LOG, ZstdTables.ML_DEFAULT_NORM, ZstdTables.ML_DEFAULT_LOG, FseEncoder.ML),
    }

    /** The chosen mode (0 predefined, 1 RLE, 2 FSE), its description bytes, and the coder to use. */
    class Choice(val mode: Int, val description: ByteArray, val coder: SequenceCoder)

    fun choose(codes: IntArray, n: Int, kind: Kind): Choice {
        val counts = IntArray(kind.maxSymbol + 1)
        for (i in 0 until n) counts[codes[i]]++
        val maxSymbol = counts.indexOfLast { it > 0 }
        val distinct = counts.count { it > 0 }
        if (distinct == 1) return Choice(1, byteArrayOf(maxSymbol.toByte()), SequenceCoder(null))

        var best = Choice(0, ByteArray(0), SequenceCoder(kind.default))
        var bestCost = FseDescription.cost256(counts, maxSymbol, kind.defaultNorm, kind.defaultLog)
        val log = FseDescription.optimalLog(n, maxSymbol, distinct, kind.maxLog)
        if (log > 0) {
            val norm = FseDescription.normalize(counts.copyOf(maxSymbol + 1), n, log)
            val description = ByteSink(64)
            FseDescription.write(description, norm, log)
            val cost = FseDescription.cost256(counts, maxSymbol, norm, log) + description.size * 8L * 256
            if (cost < bestCost) {
                bestCost = cost
                best = Choice(2, description.toByteArray(), SequenceCoder(FseEncoder(norm, log)))
            }
        }
        return best
    }
}

/** One stream's FSE state, or nothing at all for an RLE stream (whose states read no bits). */
internal class SequenceCoder(private val table: FseEncoder?) {
    private var state: FseEncoder.State? = null

    fun start(symbol: Int) {
        state = table?.start(symbol)
    }

    fun encode(bits: BitWriter, symbol: Int) {
        state?.encode(bits, symbol)
    }

    fun flush(bits: BitWriter) {
        state?.flush(bits)
    }
}
