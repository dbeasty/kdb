package dev.kdb.compression

/**
 * XXH64 with seed 0 - the hash a zstd frame's optional content checksum is the low 32 bits of
 * (RFC 8878 §3.1.1). Used by the pure-Kotlin [ZstdDecoder] and [ZstdEncoder].
 */
internal object XxHash64 {
    private const val P1: Long = -7046029288634856825L // 0x9E3779B185EBCA87
    private const val P2: Long = -4417276706812531889L // 0xC2B2AE3D27D4EB4F
    private const val P3: Long = 1609587929392839161L // 0x165667B19E3779F9
    private const val P4: Long = -8796714831421723037L // 0x85EBCA77C2B2AE63
    private const val P5: Long = 2870177450012600261L // 0x27D4EB2F165667C5

    fun hash(data: ByteArray, offset: Int = 0, length: Int = data.size - offset): Long {
        val end = offset + length
        var p = offset
        var h: Long
        if (length >= 32) {
            var v1 = P1 + P2
            var v2 = P2
            var v3 = 0L
            var v4 = -P1
            val limit = end - 32
            while (p <= limit) {
                v1 = round(v1, readLong(data, p))
                v2 = round(v2, readLong(data, p + 8))
                v3 = round(v3, readLong(data, p + 16))
                v4 = round(v4, readLong(data, p + 24))
                p += 32
            }
            h = v1.rotateLeft(1) + v2.rotateLeft(7) + v3.rotateLeft(12) + v4.rotateLeft(18)
            h = mergeRound(h, v1)
            h = mergeRound(h, v2)
            h = mergeRound(h, v3)
            h = mergeRound(h, v4)
        } else {
            h = P5
        }
        h += length.toLong()
        while (p + 8 <= end) {
            h = h xor round(0L, readLong(data, p))
            h = h.rotateLeft(27) * P1 + P4
            p += 8
        }
        if (p + 4 <= end) {
            h = h xor ((readInt(data, p).toLong() and 0xFFFFFFFFL) * P1)
            h = h.rotateLeft(23) * P2 + P3
            p += 4
        }
        while (p < end) {
            h = h xor ((data[p].toLong() and 0xFF) * P5)
            h = h.rotateLeft(11) * P1
            p++
        }
        h = h xor (h ushr 33)
        h *= P2
        h = h xor (h ushr 29)
        h *= P3
        h = h xor (h ushr 32)
        return h
    }

    private fun round(acc: Long, input: Long): Long = (acc + input * P2).rotateLeft(31) * P1

    private fun mergeRound(acc: Long, v: Long): Long = (acc xor round(0L, v)) * P1 + P4

    private fun readInt(b: ByteArray, p: Int): Int =
        (b[p].toInt() and 0xFF) or ((b[p + 1].toInt() and 0xFF) shl 8) or
            ((b[p + 2].toInt() and 0xFF) shl 16) or ((b[p + 3].toInt() and 0xFF) shl 24)

    private fun readLong(b: ByteArray, p: Int): Long =
        (readInt(b, p).toLong() and 0xFFFFFFFFL) or (readInt(b, p + 4).toLong() shl 32)
}
