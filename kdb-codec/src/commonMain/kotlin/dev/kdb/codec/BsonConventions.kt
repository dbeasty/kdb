package dev.kdb.codec

import dev.kdb.error.BsonDecodeException

public const val BsonCompanionMicrosecondsField: String = "_us"

public fun KdbUuid.toBsonBinary(): BsonBinary =
    BsonBinary(subtype = BsonBinarySubtype.UUID, data = uuidBytes())

public fun BsonBinary.toKdbUuid(): KdbUuid {
    if (subtype != BsonBinarySubtype.UUID || data.size != 16) {
        throw BsonDecodeException(
            message = "expected UUID subtype 0x04 and length 16",
            offset = -1,
        )
    }
    return KdbUuid.fromBytes(data)
}

public fun KdbHash.toBsonBinary(): BsonBinary =
    BsonBinary(subtype = BsonBinarySubtype.GENERIC, data = bytes.copyOf())

public fun BsonBinary.toKdbHash(): KdbHash {
    if (subtype != BsonBinarySubtype.GENERIC || data.size != 32) {
        throw BsonDecodeException(message = "expected hash subtype 0x00 and length 32", offset = -1)
    }
    return KdbHash(data.copyOf())
}

public fun KdbTimestamp.toBsonDate(): BsonDateTime =
    BsonDateTime(epochMillis)

public fun BsonDateTime.toKdbTimestamp(microRemainder: Long = 0L): KdbTimestamp {
    val mr = microRemainder.toInt().coerceIn(0, 999)
    return KdbTimestamp(epochMillis = epochMillis, microRemainder = mr)
}

/**
 * Embedding used by tests for microsecond round-trip alongside a BSON date value.
 */
public fun KdbTimestamp.toEmbeddedUtcWithMicroseconds(utcField: String = "utc"): BsonDocument {
    val d = BsonDocument()
    d[utcField] = toBsonDate()
    if (microRemainder != 0) {
        d[BsonCompanionMicrosecondsField] = BsonInt32(microRemainder)
    }
    return d
}

public fun BsonDocument.toKdbTimestampFromEmbeddedUtc(utcField: String = "utc"): KdbTimestamp {
    val dt = getDateTime(utcField)
        ?: throw BsonDecodeException(message = "missing BSON date field $utcField", offset = -1)
    val mic = getInt32(BsonCompanionMicrosecondsField)?.toLong() ?: 0L
    return dt.toKdbTimestamp(mic)
}
