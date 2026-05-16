package dev.kdb.index

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.json.JsonValue
import dev.kdb.schema.KdbFieldType

public sealed class IndexKey {
    public data class StringKey(val value: String) : IndexKey()

    public data class Int32Key(val value: Int) : IndexKey()

    public data class Int64Key(val value: Long) : IndexKey()

    public data class Float64Key(val value: Double) : IndexKey()

    public data class BoolKey(val value: Boolean) : IndexKey()

    public data class TimestampKey(val epochMillis: Long) : IndexKey()

    public data class UuidKey(val id: KdbUuid) : IndexKey()

    public class VectorKey(embedding: FloatArray) : IndexKey() {
        private val embedding = embedding.copyOf()

        public fun asFloatArray(): FloatArray = embedding.copyOf()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is VectorKey) return false
            return embedding.contentEquals(other.embedding)
        }

        override fun hashCode(): Int = embedding.contentHashCode()
    }

    public data class CompositeKey(val parts: List<IndexKey>) : IndexKey()

    public object NullKey : IndexKey()
}

@Suppress("CyclomaticComplexMethod")
public fun indexKeyFromJsonValue(
    value: JsonValue?,
    fieldType: KdbFieldType,
): IndexKey =
    when {
        value == null || value === JsonValue.JNull ->
            IndexKey.NullKey

        fieldType === KdbFieldType.StringType || fieldType is KdbFieldType.EnumType ->
            (value as? JsonValue.JString)?.value?.let { IndexKey.StringKey(it) }
                ?: IndexKey.NullKey

        fieldType === KdbFieldType.Int32Type ->
            when (value) {
                is JsonValue.JInt ->
                    IndexKey.Int32Key(
                        value.value.coerceIn(Int.MIN_VALUE.toLong(), Int.MAX_VALUE.toLong()).toInt(),
                    )

                is JsonValue.JNumber -> IndexKey.Int32Key(value.value.toInt())
                else -> IndexKey.NullKey
            }

        fieldType === KdbFieldType.Int64Type ->
            when (value) {
                is JsonValue.JInt -> IndexKey.Int64Key(value.value)
                is JsonValue.JNumber -> IndexKey.Int64Key(value.value.toLong())
                else -> IndexKey.NullKey
            }

        fieldType === KdbFieldType.Float64Type ->
            when (value) {
                is JsonValue.JNumber -> IndexKey.Float64Key(value.value)
                is JsonValue.JInt -> IndexKey.Float64Key(value.value.toDouble())
                else -> IndexKey.NullKey
            }

        fieldType === KdbFieldType.BoolType ->
            (value as? JsonValue.JBool)?.let { IndexKey.BoolKey(it.value) } ?: IndexKey.NullKey

        fieldType === KdbFieldType.TimestampType ->
            (value as? JsonValue.JString)?.value?.let {
                IndexKey.TimestampKey(KdbTimestamp.fromIso8601(it).toEpochMicros() / 1000L)
            } ?: IndexKey.NullKey

        fieldType === KdbFieldType.UuidType ->
            (value as? JsonValue.JString)?.value?.let {
                IndexKey.UuidKey(KdbUuid.fromString(it))
            } ?: IndexKey.NullKey

        else -> IndexKey.NullKey
    }

public fun inferIndexType(fieldType: KdbFieldType): IndexType =
    when (fieldType) {
        KdbFieldType.Int32Type, KdbFieldType.Int64Type, KdbFieldType.Float64Type, KdbFieldType.TimestampType ->
            IndexType.BTREE

        else -> IndexType.HASH
    }

/** Lexicographic compare for in-memory btree ordering. */
public fun compareIndexKeys(
    a: IndexKey,
    b: IndexKey,
): Int {
    fun tag(k: IndexKey): Int =
        when (k) {
            IndexKey.NullKey -> 0
            is IndexKey.BoolKey -> 1
            is IndexKey.Int32Key -> 2
            is IndexKey.Int64Key -> 3
            is IndexKey.Float64Key -> 4
            is IndexKey.TimestampKey -> 5
            is IndexKey.StringKey -> 6
            is IndexKey.UuidKey -> 7
            is IndexKey.VectorKey -> 8
            is IndexKey.CompositeKey -> 9
        }

    val t = tag(a).compareTo(tag(b))
    if (t != 0) return t

    return when {
        a is IndexKey.CompositeKey && b is IndexKey.CompositeKey -> {
            val max = kotlin.math.min(a.parts.size, b.parts.size)
            for (i in 0 until max) {
                val c = compareIndexKeys(a.parts[i], b.parts[i])
                if (c != 0) return c
            }
            a.parts.size.compareTo(b.parts.size)
        }

        a is IndexKey.NullKey -> 0
        a is IndexKey.BoolKey && b is IndexKey.BoolKey -> a.value.compareTo(b.value)
        a is IndexKey.Int32Key && b is IndexKey.Int32Key -> a.value.compareTo(b.value)
        a is IndexKey.Int64Key && b is IndexKey.Int64Key -> a.value.compareTo(b.value)
        a is IndexKey.Float64Key && b is IndexKey.Float64Key -> a.value.compareTo(b.value)
        a is IndexKey.TimestampKey && b is IndexKey.TimestampKey -> a.epochMillis.compareTo(b.epochMillis)
        a is IndexKey.StringKey && b is IndexKey.StringKey -> a.value.compareTo(b.value)
        a is IndexKey.UuidKey && b is IndexKey.UuidKey -> a.id.toString().compareTo(b.id.toString())
        else -> 0
    }
}
