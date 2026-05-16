package dev.kdb.codec.schema

/** Physical wire tags — Layer 0 spec §4.1 (multi-byte ints are LE unless noted). */
public enum class PhysicalKind(public val tag: Byte) {
    NULL(0x00),
    BOOLEAN(0x01),
    INT8(0x02),
    INT16(0x03),
    INT32(0x04),
    INT64(0x05),
    FLOAT32(0x06),
    FLOAT64(0x07),
    BYTES(0x08),
    STRING(0x09),
    ARRAY(0x0A.toByte()),
    MAP(0x0B.toByte()),
    RECORD(0x0C.toByte()),
    ENUM(0x0D.toByte()),
    UNION(0x0E.toByte()),
    FIXED(0x0F.toByte()),
    ;

    public companion object {
        private val BY_TAG = entries.associateBy { it.tag }

        public fun fromTag(tag: Byte): PhysicalKind? = BY_TAG[tag]
    }
}
