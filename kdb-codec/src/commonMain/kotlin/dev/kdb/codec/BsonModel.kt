package dev.kdb.codec

/** Invoked during BSON decode when a duplicate field name is encountered (`last write wins`). */
public var bsonOnDuplicateDocumentKey: (String) -> Unit = { _ -> }

public object BsonBinarySubtype {
    public const val GENERIC: Byte = 0x00 // KdbHash
    public const val UUID: Byte = 0x04 // KdbUuid
}

public enum class BsonType(public val byte: Byte) {
    DOUBLE(0x01),
    STRING(0x02),
    DOCUMENT(0x03),
    ARRAY(0x04),
    BINARY(0x05),
    BOOLEAN(0x08),
    DATETIME(0x09),
    NULL(0x0A),
    INT32(0x10),
    INT64(0x12),
    ;

    public companion object {
        public fun fromOrNull(byte: Byte): BsonType? = entries.firstOrNull { it.byte == byte }
    }
}

/** Sum type for BSON values handled by Layer 0. */
public sealed class BsonValue {
    public abstract val bsonType: BsonType
}

public data class BsonString(val value: String) : BsonValue() {
    override val bsonType: BsonType get() = BsonType.STRING
}

public data class BsonInt32(val value: Int) : BsonValue() {
    override val bsonType: BsonType get() = BsonType.INT32
}

public data class BsonInt64(val value: Long) : BsonValue() {
    override val bsonType: BsonType get() = BsonType.INT64
}

public data class BsonDouble(val value: Double) : BsonValue() {
    override val bsonType: BsonType get() = BsonType.DOUBLE
}

public data class BsonBoolean(val value: Boolean) : BsonValue() {
    override val bsonType: BsonType get() = BsonType.BOOLEAN
}

public data class BsonDateTime(val epochMillis: Long) : BsonValue() {
    override val bsonType: BsonType get() = BsonType.DATETIME
}

public data class BsonBinary(val subtype: Byte, val data: ByteArray) : BsonValue() {
    override val bsonType: BsonType get() = BsonType.BINARY

    override fun equals(other: Any?): Boolean =
        other is BsonBinary && subtype == other.subtype && data.contentEquals(other.data)

    override fun hashCode(): Int =
        subtype.toInt()
            .xor(data.contentHashCode() * 31)
}

public data object BsonNull : BsonValue() {
    override val bsonType: BsonType get() = BsonType.NULL

    override fun toString(): String = "BsonNull"
}

public data class BsonDocument(val fields: LinkedHashMap<String, BsonValue> = LinkedHashMap()) : BsonValue() {

    public constructor(entries: Iterable<Pair<String, BsonValue>>) : this(LinkedHashMap<String, BsonValue>().apply { entries.forEach { (k, v) -> put(k, v) } })

    override val bsonType: BsonType get() = BsonType.DOCUMENT

    public operator fun get(key: String): BsonValue? = fields[key]

    public operator fun set(key: String, value: BsonValue) {
        fields[key] = value
    }

    public fun getString(key: String): String? = (fields[key] as? BsonString)?.value

    public fun getInt32(key: String): Int? = (fields[key] as? BsonInt32)?.value

    public fun getInt64(key: String): Long? = (fields[key] as? BsonInt64)?.value

    public fun getDouble(key: String): Double? = (fields[key] as? BsonDouble)?.value

    public fun getBoolean(key: String): Boolean? = (fields[key] as? BsonBoolean)?.value

    public fun getDocument(key: String): BsonDocument? = fields[key] as? BsonDocument

    public fun getArray(key: String): BsonArray? = fields[key] as? BsonArray

    public fun getBinary(key: String): BsonBinary? = fields[key] as? BsonBinary

    public fun getDateTime(key: String): BsonDateTime? = fields[key] as? BsonDateTime

    public fun containsKey(key: String): Boolean = fields.containsKey(key)

    public fun keys(): Set<String> = fields.keys

    public fun isEmpty(): Boolean = fields.isEmpty()

    public companion object
}

public data class BsonArray(val elements: MutableList<BsonValue> = mutableListOf()) : BsonValue() {
    override val bsonType: BsonType get() = BsonType.ARRAY

    public operator fun get(index: Int): BsonValue = elements[index]

    public fun size(): Int = elements.size

    public fun isEmpty(): Boolean = elements.isEmpty()

    public fun add(value: BsonValue) {
        elements.add(value)
    }
}
