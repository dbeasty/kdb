package dev.kdb.codec

import dev.kdb.codec.internal.WireCodec
import kotlinx.io.Source

/**
 * Sum type for all in-memory typed values ([Layer 0 spec §6]).
 */
public sealed class KdbValue {
    /** Physical primitives */

    public data object Null : KdbValue()

    public data class Bool(val v: Boolean) : KdbValue()

    public data class Int8Val(val v: Byte) : KdbValue()

    public data class Int16Val(val v: Short) : KdbValue()

    public data class Int32Val(val v: Int) : KdbValue()

    public data class Int64Val(val v: Long) : KdbValue()

    public data class Float32Val(val v: Float) : KdbValue()

    public data class Float64Val(val v: Double) : KdbValue()

    public class BytesVal(v: ByteArray) : KdbValue() {
        public val v: ByteArray = v.copyOf()

        override fun equals(other: Any?): Boolean = other is BytesVal && v.contentEquals(other.v)

        override fun hashCode(): Int = v.contentHashCode()
    }

    public data class StringVal(val v: String) : KdbValue()

    public data class ArrayVal(val elements: List<KdbValue>) : KdbValue()

    public data class MapVal(val entries: List<Pair<KdbValue, KdbValue>>) : KdbValue()

    public data class RecordVal(val fields: Map<Int, KdbValue>) : KdbValue()

    public data class EnumVal(val ordinal: Int, val symbol: String) : KdbValue()

    public data class UnionVal(val branch: Int, val value: KdbValue) : KdbValue()

    public class FixedVal(v: ByteArray) : KdbValue() {
        public val v: ByteArray = v.copyOf()

        override fun equals(other: Any?): Boolean = other is FixedVal && v.contentEquals(other.v)

        override fun hashCode(): Int = v.contentHashCode()
    }

    /** Logical / rich representations */

    public data class DateVal(val daysSinceEpoch: Int) : KdbValue()

    public data class TimeMicrosVal(val microsSinceMidnight: Long) : KdbValue()

    public data class TimestampVal(val epochMicros: Long, val tz: String?) : KdbValue()

    public data class UuidVal(val msb: Long, val lsb: Long) : KdbValue()

    public class DecimalVal(unscaled: ByteArray, val scale: Int) : KdbValue() {
        public val unscaled: ByteArray = unscaled.copyOf()

        override fun equals(other: Any?): Boolean =
            other is DecimalVal && scale == other.scale && unscaled.contentEquals(other.unscaled)

        override fun hashCode(): Int = scale xor unscaled.contentHashCode()
    }

    public class BigIntegerVal(magnitude: ByteArray) : KdbValue() {
        public val magnitude: ByteArray = magnitude.copyOf()

        override fun equals(other: Any?): Boolean = other is BigIntegerVal && magnitude.contentEquals(other.magnitude)

        override fun hashCode(): Int = magnitude.contentHashCode()
    }

    public class BigDecimalVal(unscaled: ByteArray, val scale: Int) : KdbValue() {
        public val unscaled: ByteArray = unscaled.copyOf()

        override fun equals(other: Any?): Boolean =
            other is BigDecimalVal && scale == other.scale && unscaled.contentEquals(other.unscaled)

        override fun hashCode(): Int = scale xor unscaled.contentHashCode()
    }

    public data class DurationVal(val months: Int, val days: Int, val micros: Long) : KdbValue()

    public companion object {
        public fun decodeFromBytes(
            bytes: ByteArray,
            type: dev.kdb.codec.schema.KdbType,
            registry: dev.kdb.codec.schema.KdbTypeRegistry,
        ): KdbValue = WireCodec.decode(bytes, type, registry)

        public fun decodeFrom(
            source: Source,
            type: dev.kdb.codec.schema.KdbType,
            registry: dev.kdb.codec.schema.KdbTypeRegistry,
        ): KdbValue = WireCodec.decodeFrom(source, type, registry)
    }
}
