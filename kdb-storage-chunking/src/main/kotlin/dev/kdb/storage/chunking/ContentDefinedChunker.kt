package dev.kdb.storage.chunking

/**
 * Chunk boundary sizing. Boundaries are content-defined (a rolling hash decides where to
 * cut), so a byte-identical run anywhere in two different buffers is chunked identically
 * regardless of what was inserted/deleted around it elsewhere — this is what lets
 * near-duplicate blobs share most of their chunks without any explicit diff step.
 */
public data class ChunkerConfig(
    val minSize: Int = 64 * 1024,
    val avgSize: Int = 256 * 1024,
    val maxSize: Int = 1024 * 1024,
) {
    init {
        require(minSize in 1..avgSize) { "minSize must be in 1..avgSize" }
        require(avgSize <= maxSize) { "avgSize must be <= maxSize" }
    }
}

public data class ChunkSlice(val offset: Int, val length: Int)

/**
 * Gear-hash content-defined chunker (FastCDC-style cut rule).
 */
public object ContentDefinedChunker {

    private val GEAR: LongArray = buildGearTable()

    public fun chunk(
        data: ByteArray,
        config: ChunkerConfig = ChunkerConfig(),
    ): List<ChunkSlice> {
        if (data.isEmpty()) return emptyList()
        val mask = maskFor(config.avgSize)
        val slices = mutableListOf<ChunkSlice>()
        var start = 0
        var hash = 0L
        var i = 0
        while (i < data.size) {
            val size = i - start + 1
            hash = (hash shl 1) + GEAR[data[i].toInt() and 0xFF]
            val mustCut = size >= config.maxSize
            val mayCut = size >= config.minSize && (hash and mask) == 0L
            if (mustCut || mayCut) {
                slices += ChunkSlice(start, size)
                start = i + 1
                hash = 0L
            }
            i++
        }
        if (start < data.size) {
            slices += ChunkSlice(start, data.size - start)
        }
        return slices
    }

    /** Mask whose popcount targets an expected run length of [avgSize] bytes between cuts. */
    private fun maskFor(avgSize: Int): Long {
        val clamped = avgSize.coerceAtLeast(2)
        val bits = 32 - Integer.numberOfLeadingZeros(clamped - 1)
        return (1L shl bits) - 1
    }

    private fun buildGearTable(): LongArray {
        var seed = 0x9E3779B97F4A7C15UL
        val table = LongArray(256)
        for (i in 0 until 256) {
            seed = splitmix64(seed)
            table[i] = seed.toLong()
        }
        return table
    }

    private fun splitmix64(x0: ULong): ULong {
        var z = x0 + 0x9E3779B97F4A7C15UL
        z = (z xor (z shr 30)) * 0xBF58476D1CE4E5B9UL
        z = (z xor (z shr 27)) * 0x94D049BB133111EBUL
        return z xor (z shr 31)
    }
}
