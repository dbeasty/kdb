package dev.kdb.compression

/** Constants and code tables shared by [ZstdDecoder] and [ZstdEncoder] - RFC 8878 §3.1.1.3.2. */
internal object ZstdTables {
    const val MAGIC: Int = -0x2d04ad8 // 0xFD2FB528, read little-endian
    const val SKIPPABLE_MAGIC_MASK: Int = -0x10 // 0xFFFFFFF0
    const val SKIPPABLE_MAGIC: Int = 0x184D2A50
    const val BLOCK_SIZE_MAX: Int = 128 * 1024

    const val BLOCK_RAW: Int = 0
    const val BLOCK_RLE: Int = 1
    const val BLOCK_COMPRESSED: Int = 2

    const val LL_MAX_SYMBOL: Int = 35
    const val ML_MAX_SYMBOL: Int = 52
    const val OF_MAX_SYMBOL: Int = 31
    const val LL_MAX_LOG: Int = 9
    const val ML_MAX_LOG: Int = 9
    const val OF_MAX_LOG: Int = 8

    val LL_BASE: IntArray =
        intArrayOf(
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 18, 20, 22, 24, 28, 32, 40, 48, 64, 128, 256, 512, 1024, 2048, 4096,
            8192, 16384, 32768, 65536,
        )
    val LL_BITS: IntArray =
        intArrayOf(
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12,
            13, 14, 15, 16,
        )
    val ML_BASE: IntArray =
        intArrayOf(
            3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
            35, 37, 39, 41, 43, 47, 51, 59, 67, 83, 99, 131, 259, 515, 1027, 2051,
            4099, 8195, 16387, 32771, 65539,
        )
    val ML_BITS: IntArray =
        intArrayOf(
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8, 9, 10, 11,
            12, 13, 14, 15, 16,
        )

    /** Predefined distributions (RFC 8878 §3.1.1.3.2.2.1), accuracy logs 6, 6 and 5. */
    val LL_DEFAULT_NORM: ShortArray =
        shortArrayOf(
            4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1,
            -1, -1, -1, -1,
        )
    const val LL_DEFAULT_LOG: Int = 6
    val ML_DEFAULT_NORM: ShortArray =
        shortArrayOf(
            1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1,
            -1, -1, -1, -1, -1,
        )
    const val ML_DEFAULT_LOG: Int = 6
    val OF_DEFAULT_NORM: ShortArray =
        shortArrayOf(
            1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1,
        )
    const val OF_DEFAULT_LOG: Int = 5

    /** Index of the highest set bit; [v] must be positive. */
    fun highBit(v: Int): Int = 31 - v.countLeadingZeroBits()

    /**
     * The FSE symbol spread shared by the decoder and encoder tables (RFC 8878 §4.1.1): "less than
     * 1" symbols take the last cells, the rest are scattered with a fixed step. Returns the cell ->
     * symbol map and how many cells the low-probability symbols took at the top.
     */
    fun spreadSymbols(norm: ShortArray, maxSymbol: Int, tableLog: Int): IntArray {
        val size = 1 shl tableLog
        val cells = IntArray(size)
        var high = size - 1
        for (s in 0..maxSymbol) if (norm[s].toInt() == -1) cells[high--] = s
        val step = (size ushr 1) + (size ushr 3) + 3
        val mask = size - 1
        var pos = 0
        for (s in 0..maxSymbol) {
            repeat(norm[s].toInt().coerceAtLeast(0)) {
                cells[pos] = s
                do {
                    pos = (pos + step) and mask
                } while (pos > high)
            }
        }
        if (pos != 0) throw ZstdFormatException("FSE table distribution does not fill the table")
        return cells
    }
}
