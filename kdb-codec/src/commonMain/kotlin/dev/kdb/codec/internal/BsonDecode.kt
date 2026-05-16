package dev.kdb.codec.internal

import dev.kdb.codec.BsonArray
import dev.kdb.codec.BsonBinary
import dev.kdb.codec.BsonBoolean
import dev.kdb.codec.BsonDateTime
import dev.kdb.codec.BsonDocument
import dev.kdb.codec.BsonDouble
import dev.kdb.codec.BsonInt32
import dev.kdb.codec.BsonInt64
import dev.kdb.codec.BsonNull
import dev.kdb.codec.BsonString
import dev.kdb.codec.BsonType
import dev.kdb.codec.BsonValue
import dev.kdb.codec.bsonOnDuplicateDocumentKey
import dev.kdb.error.BsonDecodeException

internal data class ParsedBson(val doc: BsonDocument, val endExclusive: Int)

internal fun bsonDecodeRoot(bytes: ByteArray): BsonDocument {
    if (bytes.size < 5) throw BsonDecodeException("BSON too short", offset = 0)
    val ln = readIntLe(bytes, 0)
    if (ln != bytes.size) {
        throw BsonDecodeException("BSON length mismatch (declared=$ln actual=${bytes.size})", offset = 0)
    }
    return bsonDecodeAt(bytes, hdr = 0, boundaryExclusive = bytes.size).doc
}

internal fun bsonDecodeAt(bytes: ByteArray, hdr: Int, boundaryExclusive: Int): ParsedBson {
    val docLen = readLenValidated(bytes, hdr, boundaryExclusive)
    val docEnd = hdr + docLen
    var cur = hdr + 4
    val out = LinkedHashMap<String, BsonValue>()
    while (cur < docEnd) {
        val tb = bytes[cur++].toInt() and 0xFF
        if (tb == 0) {
            if (cur != docEnd) throw BsonDecodeException("BSON NUL terminator misaligned", offset = cur - 1)
            return ParsedBson(BsonDocument(out), docEnd)
        }
        val ty = BsonType.fromOrNull(tb.toByte()) ?: badType(off = cur - 1, t = tb)
        val (name, nx) = readCString(bytes, cur, docEnd)
        cur = nx
        if (out.containsKey(name)) bsonOnDuplicateDocumentKey(name)
        val (valu, nz) = readVal(bytes, ty, cur, docEnd)
        cur = nz
        out[name] = valu
    }
    throw BsonDecodeException("BSON missing terminator", offset = hdr)
}

private fun readLenValidated(bytes: ByteArray, hdr: Int, boundaryExclusive: Int): Int {
    if (boundaryExclusive - hdr < 5) throw BsonDecodeException("truncated BSON length prefix", offset = hdr)
    val ln = readIntLe(bytes, hdr)
    if (ln < 5 || hdr + ln > boundaryExclusive) {
        throw BsonDecodeException("invalid BSON nested length (len=$ln)", offset = hdr)
    }
    return ln
}

private fun badType(off: Int, t: Int): Nothing {
    val b = t and 0xFF
    val h = if (b < 16) "0" + b.toString(16) else b.toString(16)
    throw BsonDecodeException("unsupported BSON type 0x${h.uppercase()}", offset = off)
}

private fun readCString(bytes: ByteArray, cursor: Int, docEnd: Int): Pair<String, Int> {
    var p = cursor
    while (p < docEnd) {
        if (bytes[p].toInt() == 0) {
            val s = decodeUtf8Strict(bytes, cursor, p - cursor)
            return Pair(s, p + 1)
        }
        p++
    }
    throw BsonDecodeException("unterminated CString", offset = cursor)
}

private fun readBsonUtf8(bytes: ByteArray, cur: Int, docEnd: Int): Pair<BsonString, Int> {
    need(cur, 4, docEnd)
    val ln = readIntLe(bytes, cur)
    if (ln < 2) throw BsonDecodeException("invalid UTF-8 BSON string length=$ln", offset = cur)
    val contentOffset = cur + 4
    val lastInclusive = contentOffset + ln - 1
    if (lastInclusive >= docEnd) throw BsonDecodeException("truncated BSON string", offset = cur)
    if (bytes[lastInclusive].toInt() != 0) {
        throw BsonDecodeException("BSON string payload missing NUL terminator", offset = lastInclusive)
    }
    val text = decodeUtf8Strict(bytes, contentOffset, ln - 1)
    return Pair(BsonString(text), lastInclusive + 1)
}

private fun readBin(bytes: ByteArray, cur: Int, docEnd: Int): Pair<BsonBinary, Int> {
    need(cur, 5, docEnd)
    val payloadLen = readIntLe(bytes, cur)
    if (payloadLen < 0) throw BsonDecodeException("invalid binary length=$payloadLen", offset = cur)
    val subtype = bytes[cur + 4]
    val dataStart = cur + 5
    val nx = dataStart + payloadLen
    if (nx > docEnd) throw BsonDecodeException("truncated BSON binary", offset = cur)
    val data = ByteArray(payloadLen)
    bytes.copyInto(data, 0, dataStart, nx)
    return Pair(BsonBinary(subtype = subtype, data = data), nx)
}

private fun readVal(bytes: ByteArray, ty: BsonType, cur: Int, docEnd: Int): Pair<BsonValue, Int> =
    when (ty) {
        BsonType.DOUBLE -> {
            need(cur, 8, docEnd)
            Pair(BsonDouble(Double.fromBits(readLongLe(bytes, cur))), cur + 8)
        }
        BsonType.STRING -> readBsonUtf8(bytes, cur, docEnd)
        BsonType.DOCUMENT -> bsonDecodeAt(bytes, cur, docEnd).let { p -> Pair(p.doc, p.endExclusive) }
        BsonType.ARRAY ->
            bsonDecodeAt(bytes, cur, docEnd).let {
                Pair(arrayDocToArray(it.doc), it.endExclusive)
            }
        BsonType.BINARY -> readBin(bytes, cur, docEnd)
        BsonType.BOOLEAN -> {
            need(cur, 1, docEnd)
            Pair(BsonBoolean(bytes[cur].toInt() != 0), cur + 1)
        }
        BsonType.DATETIME -> {
            need(cur, 8, docEnd)
            Pair(BsonDateTime(readLongLe(bytes, cur)), cur + 8)
        }
        BsonType.NULL -> Pair(BsonNull, cur)
        BsonType.INT32 -> {
            need(cur, 4, docEnd)
            Pair(BsonInt32(readIntLe(bytes, cur)), cur + 4)
        }
        BsonType.INT64 -> {
            need(cur, 8, docEnd)
            Pair(BsonInt64(readLongLe(bytes, cur)), cur + 8)
        }
    }

private fun arrayDocToArray(d: BsonDocument): BsonArray {
    val n = d.fields.size
    if (n == 0) return BsonArray()
    val els = mutableListOf<BsonValue>()
    repeat(n) { i ->
        val v =
            d.fields[i.toString()] ?: throw BsonDecodeException(
                message = "BSON array missing contiguous key \"$i\"",
                offset = -1,
            )
        els.add(v)
    }
    return BsonArray(els.toMutableList())
}

private fun need(cur: Int, n: Int, docEndEx: Int) {
    if (cur + n > docEndEx) throw BsonDecodeException("truncated BSON value", offset = cur)
}

