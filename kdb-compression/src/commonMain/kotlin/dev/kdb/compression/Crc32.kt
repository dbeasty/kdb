package dev.kdb.compression

public object Crc32 {
    private val table =
        IntArray(256) {
            var c = it
            repeat(8) { c = if (c and 1 != 0) (0xEDB88320.toInt() xor (c ushr 1)) else c ushr 1 }
            c
        }

    public fun of(data: ByteArray, offset: Int = 0, length: Int = data.size - offset): Int {
        var crc = 0xFFFFFFFF.toInt()
        val end = (offset + length).coerceAtMost(data.size)
        for (i in offset until end) {
            crc = table[(crc xor data[i].toInt()) and 0xFF] xor (crc ushr 8)
        }
        return crc.inv()
    }
}
