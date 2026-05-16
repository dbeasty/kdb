package dev.kdb.codec.internal

import dev.kdb.error.KdbDecodeException
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.io.readByteArray

internal fun encodeLeb128U32(v: UInt): ByteArray {
    val sink = Buffer()
    var cur = v
    while (true) {
        val chunk = (cur and 0x7FU).toByte()
        cur = cur shr 7
        if (cur == 0u) {
            sink.write(byteArrayOf(chunk))
            break
        }
        sink.write(byteArrayOf((chunk.toInt() or 0x80).toByte()))
    }
    return sink.readByteArray()
}

internal fun encodeLeb128U64(v: ULong): ByteArray {
    val sink = Buffer()
    var cur = v
    while (true) {
        val chunk = (cur and 0x7FUL).toByte()
        cur = cur shr 7
        if (cur == 0UL) {
            sink.write(byteArrayOf(chunk))
            break
        }
        sink.write(byteArrayOf((chunk.toInt() or 0x80).toByte()))
    }
    return sink.readByteArray()
}

internal class Pos(val base: Int) {
    var i: Int = 0

    fun mark(): Int = base + i
}

internal fun readLeb128U64(raw: ByteArray, pos: Pos): Long {
    var result = 0L
    var shift = 0
    repeat(11) {
        if (pos.i >= raw.size) throw KdbDecodeException("truncated varint", pos.mark())
        val byte = raw[pos.i++].toLong() and 0xFFL
        result = result or ((byte and 0x7FL) shl shift)
        if (byte and 0x80L == 0L) return result
        shift += 7
        if (shift > 63) throw KdbDecodeException("varint overflow", pos.mark())
    }
    throw KdbDecodeException("varint overflow", pos.base + pos.i - 1)
}

internal class SourcePull(private val src: Source) {
    var offset: Int = 0

    fun readExact(n: Int): ByteArray {
        if (n == 0) return ByteArray(0)
        val bytes = src.readByteArray(n)
        offset += bytes.size
        if (bytes.size != n) {
            throw KdbDecodeException("unexpected EOF reading $n bytes", offset.coerceAtLeast(0))
        }
        return bytes
    }

    fun readU8(): Byte {
        val b = readExact(1)
        return b[0]
    }

    fun readLeb128U64(): Long {
        var result = 0L
        var shift = 0
        repeat(11) {
            val byte = readU8().toLong() and 0xFFL
            result = result or ((byte and 0x7FL) shl shift)
            if (byte and 0x80L == 0L) return result
            shift += 7
            if (shift > 63) throw KdbDecodeException("varint overflow", offset)
        }
        throw KdbDecodeException("varint overflow", offset)
    }
}

internal fun putLe16(b: Buffer, v: Short) {
    val i = v.toInt() and 0xFFFF
    b.write(byteArrayOf((i and 0xFF).toByte()))
    b.write(byteArrayOf(((i ushr 8) and 0xFF).toByte()))
}

internal fun putLe32(b: Buffer, v: Int) {
    var x = v
    repeat(4) {
        b.write(byteArrayOf((x and 0xFF).toByte()))
        x = x ushr 8
    }
}

internal fun putLe64(b: Buffer, v: Long) {
    var x = v
    repeat(8) {
        b.write(byteArrayOf((x and 0xFF).toByte()))
        x = x ushr 8
    }
}

internal fun readLe16(raw: ByteArray, pos: Pos): Short {
    if (pos.i + 2 > raw.size) throw KdbDecodeException("truncated i16", pos.mark())
    val b0 = raw[pos.i++].toInt() and 0xFF
    val b1 = raw[pos.i++].toInt() and 0xFF
    return ((b1 shl 8) or b0).toShort()
}

internal fun readLe32(raw: ByteArray, pos: Pos): Int {
    if (pos.i + 4 > raw.size) throw KdbDecodeException("truncated i32", pos.mark())
    var v = 0
    var shift = 0
    repeat(4) {
        v = v or ((raw[pos.i++].toInt() and 0xFF) shl shift)
        shift += 8
    }
    return v
}

internal fun readLe64(raw: ByteArray, pos: Pos): Long {
    if (pos.i + 8 > raw.size) throw KdbDecodeException("truncated i64", pos.mark())
    var v = 0L
    var shift = 0
    repeat(8) {
        v = v or ((raw[pos.i++].toLong() and 0xFFL) shl shift)
        shift += 8
    }
    return v
}

internal fun putFloat32(buf: Buffer, v: Float) = putLe32(buf, v.toBits())

internal fun putFloat64(buf: Buffer, v: Double) = putLe64(buf, v.toBits())

internal fun readFloat32(raw: ByteArray, pos: Pos): Float = Float.fromBits(readLe32(raw, pos))

internal fun readFloat64(raw: ByteArray, pos: Pos): Double = Double.fromBits(readLe64(raw, pos))
