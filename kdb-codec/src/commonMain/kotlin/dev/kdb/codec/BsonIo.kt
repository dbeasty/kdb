package dev.kdb.codec

import dev.kdb.codec.internal.bsonDecodeRoot
import dev.kdb.codec.internal.bsonDocumentEncodedSize
import dev.kdb.codec.internal.bsonEncode
import dev.kdb.codec.internal.bsonEncodedValuePayloadSize
import dev.kdb.codec.internal.readIntLe
import dev.kdb.error.BsonDecodeException
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.readByteArray

public fun BsonDocument.toBytes(): ByteArray =
    bsonEncode(fields)

public fun BsonDocument.writeTo(sink: Sink) {
    val blob = bsonEncode(fields)
    sink.write(blob, 0, blob.size)
}

public fun BsonDocument.Companion.fromBytes(bytes: ByteArray): BsonDocument =
    bsonDecodeRoot(bytes)

public fun BsonDocument.Companion.fromSource(source: Source): BsonDocument {
    val hdr = source.readByteArray(4)
    val declared = readIntLe(hdr, 0)
    if (declared < 5) throw BsonDecodeException("invalid BSON length while reading Source", offset = 0)
    val restLen = declared - 4
    val body = source.readByteArray(restLen)
    val all =
        ByteArray(declared).also {
            hdr.copyInto(it, 0, 0, 4)
            body.copyInto(it, destinationOffset = 4, startIndex = 0, endIndex = restLen)
        }
    return bsonDecodeRoot(all)
}

public fun BsonDocument.encodedSize(): Int =
    bsonDocumentEncodedSize(fields)

public fun BsonValue.encodedSize(): Int =
    bsonEncodedValuePayloadSize(this)
