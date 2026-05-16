package dev.kdb.codec.internal

internal fun putIntLe(buf: ByteArray, p: Int, v: Int): Int {
    var i = p
    buf[i++] = v.toByte()
    buf[i++] = (v shr 8).toByte()
    buf[i++] = (v shr 16).toByte()
    buf[i++] = (v shr 24).toByte()
    return i
}

internal fun putLongLe(buf: ByteArray, p: Int, v: Long): Int {
    var x = v
    var i = p
    repeat(8) {
        buf[i++] = x.toInt().toByte()
        x = x shr 8
    }
    return i
}

internal fun readIntLe(buf: ByteArray, p: Int): Int =
    (
        buf[p].toInt() and 0xFF or
            ((buf[p + 1].toInt() and 0xFF) shl 8) or
            ((buf[p + 2].toInt() and 0xFF) shl 16) or
            ((buf[p + 3].toInt() and 0xFF) shl 24)
    )

internal fun readLongLe(buf: ByteArray, p: Int): Long {
    val lo = readIntLe(buf, p).toULong() and 0xFFFF_FFFFuL
    val hi = readIntLe(buf, p + 4).toULong() and 0xFFFF_FFFFuL
    return (lo or (hi shl 32)).toLong()
}

internal fun writeU8(buf: ByteArray, p: Int, v: Int): Int {
    buf[p] = (v and 0xFF).toByte()
    return p + 1
}

/** CString: UTF-8 bytes without interior NUL, plus trailing 0x00. */
internal fun writeCString(buf: ByteArray, p: Int, utf8FieldName: ByteArray): Int {
    utf8FieldName.copyInto(buf, p)
    val z = p + utf8FieldName.size
    buf[z] = 0
    return z + 1
}

/** BSON UTF-8 string element: int32 `len` (UTF-8 bytes + trailing 0x00), payload, trailing 0x00. */
internal fun writeBsonStringPayload(buf: ByteArray, p: Int, utf8Content: ByteArray): Int {
    var q = putIntLe(buf, p, utf8Content.size + 1)
    utf8Content.copyInto(buf, q)
    q += utf8Content.size
    buf[q] = 0
    return q + 1
}
