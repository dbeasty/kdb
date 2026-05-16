package dev.kdb.json

import dev.kdb.codec.KdbValue
import dev.kdb.error.JsonPathException

/**
 * Typed JSON tree for JSON functions and codec bridging.
 */
public sealed class JsonValue {
    public data class JString(public val value: String) : JsonValue()

    public data class JNumber(public val value: Double) : JsonValue()

    public data class JInt(public val value: Long) : JsonValue()

    public data class JBool(public val value: Boolean) : JsonValue()

    public data object JNull : JsonValue()

    public data class JObject(public val fields: LinkedHashMap<String, JsonValue>) : JsonValue() {
        public constructor(entries: Map<String, JsonValue>) : this(linkedMapOf<String, JsonValue>().also { m -> entries.forEach { (k, v) -> m[k] = v } })
    }

    public data class JArray(public val elements: List<JsonValue>) : JsonValue()

    public fun toJsonString(): String = JsonWriter.write(this)

    public fun toKdbValue(): KdbValue =
        when (this) {
            is JString -> KdbValue.StringVal(value)
            is JNumber -> KdbValue.Float64Val(value)
            is JInt -> KdbValue.Int64Val(value)
            is JBool -> KdbValue.Bool(value)
            JNull -> KdbValue.Null
            is JObject ->
                KdbValue.MapVal(
                    fields.map { (k, v) -> KdbValue.StringVal(k) to v.toKdbValue() },
                )
            is JArray -> KdbValue.ArrayVal(elements.map { it.toKdbValue() })
        }

    public companion object {
        public fun fromJsonString(json: String): JsonValue = JsonParser(json).parseValue()

        public fun fromKdbValue(value: KdbValue): JsonValue =
            when (value) {
                KdbValue.Null -> JNull
                is KdbValue.Bool -> JBool(value.v)
                is KdbValue.Int64Val -> JInt(value.v)
                is KdbValue.Float64Val -> JNumber(value.v)
                is KdbValue.StringVal -> JString(value.v)
                is KdbValue.ArrayVal -> JArray(value.elements.map { fromKdbValue(it) })
                is KdbValue.MapVal -> {
                    val m = LinkedHashMap<String, JsonValue>()
                    for ((k, v) in value.entries) {
                        val ks = k as? KdbValue.StringVal ?: throw JsonPathException("Map key must be string", "\$")
                        m[ks.v] = fromKdbValue(v)
                    }
                    JObject(m)
                }
                else -> throw JsonPathException("unsupported KdbValue for JSON subtree", "\$")
            }
    }
}

public fun KdbValue.toJsonValue(): JsonValue = JsonValue.fromKdbValue(this)
