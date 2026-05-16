package dev.kdb.transaction

internal fun ByteArray.toLowerHex(): String =
    joinToString("") { b ->
        val v = b.toInt() and 0xFF
        buildString(2) {
            append(("0123456789abcdef"[v / 16]))
            append(("0123456789abcdef"[v % 16]))
        }
    }

internal fun String.decodeLowerHex(): ByteArray {
    require(length % 2 == 0) { "even hex length expected" }
    val out = ByteArray(length / 2)
    for (i in out.indices) {
        val hi = hexNibble(this[i * 2])
        val lo = hexNibble(this[i * 2 + 1])
        out[i] = ((hi shl 4) or lo).toByte()
    }
    return out
}

private fun hexNibble(c: Char): Int =
    when (c) {
        in '0'..'9' -> c - '0'
        in 'a'..'f' -> 10 + (c - 'a')
        in 'A'..'F' -> 10 + (c - 'A')
        else -> error("bad hex digit $c")
    }
