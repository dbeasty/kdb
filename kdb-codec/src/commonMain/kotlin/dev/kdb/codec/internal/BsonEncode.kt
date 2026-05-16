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
import dev.kdb.codec.BsonValue
import dev.kdb.error.BsonEncodeException

internal fun bsonEncodedValuePayloadSize(v: BsonValue): Int =
    when (v) {
        is BsonDocument -> bsonDocumentEncodedSize(v.fields)
        is BsonArray -> bsonDocumentEncodedSize(bsonArrayMap(v))
        is BsonString -> utfBytes(v.value).let { x -> 4 + (x.size + 1) }
        is BsonBinary -> 4 + 1 + v.data.size
        is BsonNull -> 0
        is BsonBoolean -> 1
        is BsonInt32 -> 4
        is BsonInt64 -> 8
        is BsonDouble -> 8
        is BsonDateTime -> 8
    }

internal fun bsonDocumentEncodedSize(fields: LinkedHashMap<String, BsonValue>): Int =
    4 + bsonInnerBareEncodedSize(fields)

private fun bsonInnerBareEncodedSize(fields: LinkedHashMap<String, BsonValue>): Int {
    var sum = 0
    fields.forEach { (k, v) ->
        sum += 1
        sum += bsonKeyUtf(k).size + 1
        sum += bsonEncodedValuePayloadSize(v)
    }
    return sum + 1
}

private fun bsonArrayMap(arr: BsonArray): LinkedHashMap<String, BsonValue> {
    val m = LinkedHashMap<String, BsonValue>(arr.elements.size.coerceAtLeast(16))
    arr.elements.forEachIndexed { idx, vv -> m[idx.toString()] = vv }
    return m
}

private fun utfBytes(s: String): ByteArray = s.encodeToByteArray()

internal fun bsonKeyUtf(name: String): ByteArray =
    utfBytes(name).also { u ->
        if (u.any { it == 0.toByte() }) throw BsonEncodeException("BSON field name must not contain NUL")
    }

internal fun bsonEncode(fields: LinkedHashMap<String, BsonValue>): ByteArray {
    val total = bsonDocumentEncodedSize(fields)
    val buf = ByteArray(total)
    putIntLe(buf, 0, total)
    bsonEncodeInnerBare(buf, startIncl = 4, endExclusive = total, fields = fields)
    return buf
}

internal fun bsonEncodeInnerBare(buf: ByteArray, startIncl: Int, endExclusive: Int, fields: LinkedHashMap<String, BsonValue>) {
    var c = startIncl
    for ((name, vv) in fields) c = bsonWriteElement(buf, c, endExclusive, name, vv)
    c = writeU8(buf, c, 0)
    check(c == endExclusive) { "BSON inner size mismatch cursor=$c end=$endExclusive" }
}

private fun bsonWriteElement(buf: ByteArray, cursor: Int, endEx: Int, name: String, vv: BsonValue): Int {
    val ku = bsonKeyUtf(name)
    var q = writeU8(buf, cursor, vv.bsonType.byte.toInt() and 0xFF)
    q = writeCString(buf, q, ku)
    return bsonWriteValuePayload(buf, q, endEx, vv)
}

private fun bsonWriteValuePayload(buf: ByteArray, cursor: Int, endEx: Int, vv: BsonValue): Int = when (vv) {
    is BsonDouble -> {
        bsonNeedRoom(cursor, 8, endEx)
        if (!vv.value.isFinite()) throw BsonEncodeException("BSON double must be finite")
        putLongLe(buf, cursor, vv.value.toBits())
    }
    is BsonString -> writeBsonStringPayload(buf, cursor, utfBytes(vv.value))
    is BsonBinary -> {
        val ln = vv.data.size
        bsonNeedRoom(cursor, ln + 5, endEx)
        var q = putIntLe(buf, cursor, ln)
        q = writeU8(buf, q, vv.subtype.toInt() and 0xFF)
        vv.data.copyInto(buf, destinationOffset = q)
        q + ln
    }
    is BsonBoolean -> {
        bsonNeedRoom(cursor, 1, endEx)
        writeU8(buf, cursor, if (vv.value) 1 else 0)
    }
    is BsonInt32 -> {
        bsonNeedRoom(cursor, 4, endEx)
        putIntLe(buf, cursor, vv.value)
    }
    is BsonInt64 -> {
        bsonNeedRoom(cursor, 8, endEx)
        putLongLe(buf, cursor, vv.value)
    }
    is BsonDateTime -> {
        bsonNeedRoom(cursor, 8, endEx)
        putLongLe(buf, cursor, vv.epochMillis)
    }
    is BsonNull -> cursor
    is BsonDocument -> bsonWriteEmbedded(buf, cursor, endEx, vv.fields)
    is BsonArray -> bsonWriteEmbedded(buf, cursor, endEx, bsonArrayMap(vv))
}

internal fun bsonWriteEmbedded(buf: ByteArray, cursor: Int, endEx: Int, inner: LinkedHashMap<String, BsonValue>): Int {
    val total = bsonDocumentEncodedSize(inner)
    bsonNeedRoom(cursor, total, endEx)
    val anchor = cursor
    val bodyStart = putIntLe(buf, cursor, total)
    bsonEncodeInnerBare(buf = buf, startIncl = bodyStart, endExclusive = anchor + total, fields = inner)
    return anchor + total
}

private fun bsonNeedRoom(cursor: Int, need: Int, endEx: Int) {
    if (cursor > endEx || cursor + need > endEx) throw BsonEncodeException("BSON write past document boundary")
}
