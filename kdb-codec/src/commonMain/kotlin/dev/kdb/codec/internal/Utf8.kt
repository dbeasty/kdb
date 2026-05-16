package dev.kdb.codec.internal

import dev.kdb.error.KdbDecodeException

/** UTF-8 byte sequence validated and decoded as a Kotlin string (no replacement characters). */
internal fun decodeUtf8Strict(bytes: ByteArray, offset: Int, length: Int): String {
    if (length == 0) return ""
    var i = offset
    val limit = offset + length
    val sb = StringBuilder(length)
    while (i < limit) {
        val decoded = decodeOneUtf8Cp(bytes, i, limit)
        i += decoded.second
        appendCodePoint(sb, decoded.first)
    }
    return sb.toString()
}

private fun decodeOneUtf8Cp(bytes: ByteArray, i: Int, limit: Int): Pair<Int, Int> {
    val start = i
    fun truncated(): Nothing = throw KdbDecodeException("truncated UTF-8", offset = start)
    fun bad(): Nothing = throw KdbDecodeException("invalid UTF-8", offset = start)

    val b0 = bytes[i].toInt() and 0xFF
    when {
        b0 and 0x80 == 0 -> return b0 to 1
        b0 and 0xE0 == 0xC0 -> {
            if (i + 1 >= limit) truncated()
            val b1 = bytes[i + 1].toInt() and 0xFF
            if (b1 and 0xC0 != 0x80) bad()
            val cp = ((b0 and 0x1F) shl 6) or (b1 and 0x3F)
            if (cp < 0x80 || cp in SURROGATE_RANGE) bad()
            return cp to 2
        }
        b0 and 0xF0 == 0xE0 -> {
            if (i + 2 >= limit) truncated()
            val b1 = bytes[i + 1].toInt() and 0xFF
            val b2 = bytes[i + 2].toInt() and 0xFF
            if ((b1 and 0xC0 != 0x80) || (b2 and 0xC0 != 0x80)) bad()
            val cp =
                (((b0 and 0x0F) shl 12) or
                    ((b1 and 0x3F) shl 6) or
                    (b2 and 0x3F))
            if (cp < 0x0800 || cp in SURROGATE_RANGE) bad()
            return cp to 3
        }
        b0 and 0xF8 == 0xF0 -> {
            if (i + 3 >= limit) truncated()
            val b1 = bytes[i + 1].toInt() and 0xFF
            val b2 = bytes[i + 2].toInt() and 0xFF
            val b3 = bytes[i + 3].toInt() and 0xFF
            if ((b1 and 0xC0 != 0x80) || (b2 and 0xC0 != 0x80) || (b3 and 0xC0 != 0x80)) bad()
            val cp =
                (((b0 and 0x07).toLong() shl 18) or
                    ((b1 and 0x3F).toLong() shl 12) or
                    ((b2 and 0x3F).toLong() shl 6) or
                    (b3 and 0x3F).toLong())
                    .toInt()
            if (cp < 0x10000 || cp > 0x10FFFF) bad()
            return cp to 4
        }
        else -> bad()
    }
}

private val SURROGATE_RANGE = 0xD800..0xDFFF

private fun appendCodePoint(sb: StringBuilder, codePoint: Int) {
    if (codePoint in SURROGATE_RANGE) throw IllegalStateException("surrogate scala")
    if (codePoint < 0x10000) {
        sb.append(codePoint.toChar())
    } else {
        val osp = codePoint - 0x10000
        val high = 0xD800 + (osp ushr 10)
        val low = 0xDC00 + (osp and 0x03FF)
        sb.append(high.toChar()).append(low.toChar())
    }
}
