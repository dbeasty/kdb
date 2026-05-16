@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package dev.kdb.codec

import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import dev.kdb.codec.schema.LogicalAnnotation
import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.error.KdbDecodeException
import dev.kdb.error.KdbEncodeException
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put

private val jsonFlat =
    Json {
        ignoreUnknownKeys = true
    }

public fun KdbValue.Companion.fromJson(
    json: String,
    type: KdbType,
    registry: KdbTypeRegistry,
): KdbValue =
    try {
        jsonToValue(jsonFlat.parseToJsonElement(json), type, registry)
    } catch (e: Throwable) {
        throw KdbDecodeException(message = "JSON: ${e.message}", offset = 0, cause = e)
    }

public fun KdbValue.toJson(type: KdbType, registry: KdbTypeRegistry): String =
    jsonFlat.encodeToString(valueToJson(this, type, registry))

public fun KdbValue.toPrettyJson(type: KdbType, registry: KdbTypeRegistry, indent: Int = 2): String {
    val fmt =
        Json {
            ignoreUnknownKeys = true
            prettyPrint = true
            prettyPrintIndent = " ".repeat(indent.coerceIn(1, 32))
        }
    return fmt.encodeToString(valueToJson(this, type, registry))
}

private fun jsonToValue(el: JsonElement, type: KdbType, reg: KdbTypeRegistry): KdbValue =
    when (type) {
        is KdbType.Nullable ->
            if (el is JsonNull) KdbValue.Null
            else jsonToValue(el, type.inner, reg)

        is KdbType.Union -> throw KdbDecodeException("Union JSON unsupported in iteration-1", 0)

        is KdbType.Ref -> jsonToNamed(el, type, reg)

        is KdbType.Array -> {
            if (el !is JsonArray) throw KdbDecodeException("expected array", 0)
            KdbValue.ArrayVal(el.map { jsonToValue(it, type.element, reg) })
        }

        is KdbType.Map -> {
            if (el !is JsonObject) throw KdbDecodeException("expected object for map", 0)
            if (type.key != KdbType.Primitive(PhysicalKind.STRING)) {
                throw KdbDecodeException("non-string keys need extended JSON (TODO)", 0)
            }
            val pairs =
                el.map { (k, v) ->
                    KdbValue.StringVal(k) to jsonToValue(v, type.value, reg)
                }
            KdbValue.MapVal(pairs)
        }

        is KdbType.Primitive -> jsonToPrimitive(el, type.physical, type.logical)
    }

private fun jsonToNamed(el: JsonElement, type: KdbType.Ref, reg: KdbTypeRegistry): KdbValue {
    val schema =
        try {
            reg.resolveRecord(type.fullyQualifiedName)
        } catch (_: NoSuchElementException) {
            throw KdbDecodeException("JSON record decode requires RecordSchema for ${type.fullyQualifiedName}", 0)
        }

    return jsonObjectToRecord(el, schema, reg)
}

private fun jsonObjectToRecord(el: JsonElement, schema: dev.kdb.codec.schema.RecordSchema, reg: KdbTypeRegistry): KdbValue {
    if (el !is JsonObject) throw KdbDecodeException("expected object", 0)
    val out = LinkedHashMap<Int, KdbValue>()
    for (f in schema.fields) {
        val raw = el[f.name]
        when {
            raw != null -> out[f.id] = jsonToValue(raw, f.type, reg)
            f.default != null -> out[f.id] = f.default
            else -> throw KdbDecodeException("missing field ${f.name}", 0)
        }
    }
    return KdbValue.RecordVal(out)
}

private fun jsonToPrimitive(el: JsonElement, phy: PhysicalKind, logical: LogicalAnnotation?): KdbValue {
    when (logical) {
        is LogicalAnnotation.Custom -> throw KdbDecodeException("custom logical JSON unsupported", 0)
        LogicalAnnotation.Date -> {
            require(phy == PhysicalKind.INT32)
            val s = requireStringPrimitive(el).trim().trim('"')
            return KdbValue.DateVal(LocalDate.parse(s).toEpochDays())
        }

        is LogicalAnnotation.TimestampMicros -> {
            require(phy == PhysicalKind.INT64)
            val s = requireStringPrimitive(el).trim().trim('"')
            val inst = Instant.parse(s)
            val micros = inst.epochSeconds * 1_000_000L + inst.nanosecondsOfSecond / 1000
            return KdbValue.TimestampVal(micros, logical.timezone)
        }

        is LogicalAnnotation.TimestampMillis -> {
            require(phy == PhysicalKind.INT64)
            val s = requireStringPrimitive(el).trim().trim('"')
            val inst = Instant.parse(s)
            val micros = inst.epochSeconds * 1_000_000L + inst.nanosecondsOfSecond / 1000
            return KdbValue.TimestampVal(micros, logical.timezone)
        }

        LogicalAnnotation.TimeMicros,
        LogicalAnnotation.Uuid,
        LogicalAnnotation.Duration,
        is LogicalAnnotation.Decimal,
        LogicalAnnotation.BigInteger,
        is LogicalAnnotation.BigDecimal,
        -> throw KdbDecodeException("logical type $logical JSON unsupported", 0)

        null -> Unit
    }
    return primitiveOnlyJson(el, phy)
}

private fun primitiveOnlyJson(el: JsonElement, phy: PhysicalKind): KdbValue =
    when (phy) {
        PhysicalKind.NULL -> {
            require(el is JsonNull)
            KdbValue.Null
        }

        PhysicalKind.BOOLEAN -> KdbValue.Bool(requireBoolPrimitive(el))

        PhysicalKind.INT8 -> KdbValue.Int8Val(requireLongPrimitive(el).toByte())
        PhysicalKind.INT16 -> KdbValue.Int16Val(requireLongPrimitive(el).toShort())
        PhysicalKind.INT32 -> KdbValue.Int32Val(requireLongPrimitive(el).toInt())
        PhysicalKind.INT64 -> KdbValue.Int64Val(requireLongPrimitive(el))
        PhysicalKind.FLOAT32 -> KdbValue.Float32Val(requireDoublePrimitive(el).toFloat())
        PhysicalKind.FLOAT64 -> {
            val d = requireDoublePrimitive(el)
            if (!d.isFinite()) throw KdbDecodeException("non-finite float", 0)
            KdbValue.Float64Val(d)
        }

        PhysicalKind.STRING -> KdbValue.StringVal(requireStringPrimitive(el))
        PhysicalKind.BYTES -> throw KdbDecodeException("bytes as base64 not implemented", 0)
        else -> throw KdbDecodeException("composite needs structural type", 0)
    }

private fun requireStringPrimitive(el: JsonElement): String =
    (el as? JsonPrimitive)?.content ?: throw KdbDecodeException("expected string", 0)

private fun requireBoolPrimitive(el: JsonElement): Boolean =
    (el as? JsonPrimitive)?.booleanOrNull ?: throw KdbDecodeException("expected boolean", 0)

private fun requireLongPrimitive(el: JsonElement): Long {
    val p = el as? JsonPrimitive ?: throw KdbDecodeException("expected number", 0)
    return p.longOrNull ?: throw KdbDecodeException("expected integral json", 0)
}

private fun requireDoublePrimitive(el: JsonElement): Double {
    val p = el as? JsonPrimitive ?: throw KdbDecodeException("expected number", 0)
    return p.doubleOrNull ?: throw KdbDecodeException("expected fractional json", 0)
}

// --- emit -------------------------------------------------------------------------------

private fun valueToJson(v: KdbValue, type: KdbType, reg: KdbTypeRegistry): JsonElement =
    when (type) {
        is KdbType.Nullable ->
            if (v === KdbValue.Null) JsonNull
            else valueToJson(v, type.inner, reg)

        is KdbType.Union -> throw KdbEncodeException("Union JSON unsupported")

        is KdbType.Ref -> recordToJson(v, type, reg)

        is KdbType.Array -> {
            require(v is KdbValue.ArrayVal)
            buildJsonArray {
                val et = type.element
                for (e in v.elements) add(valueToJson(e, et, reg))
            }
        }

        is KdbType.Map -> {
            require(v is KdbValue.MapVal)
            if (type.key != KdbType.Primitive(PhysicalKind.STRING)) throw KdbEncodeException("extended map keys unsupported")
            buildJsonObject {
                for ((k, ve) in v.entries) {
                    val kk = mapKeyAsString(type.key, k)
                    put(kk, valueToJson(ve, type.value, reg))
                }
            }
        }

        is KdbType.Primitive -> primitiveToJson(v, type.physical, type.logical)
    }

private fun mapKeyAsString(keyType: KdbType, k: KdbValue): String {
    if (k is KdbValue.StringVal) return k.v
    throw KdbEncodeException("extended non-string keys unsupported")
}

private fun recordToJson(v: KdbValue, type: KdbType.Ref, reg: KdbTypeRegistry): JsonElement {
    val schema = reg.resolveRecord(type.fullyQualifiedName)
    val rec = v as? KdbValue.RecordVal ?: throw KdbEncodeException("RecordVal expected")
    return buildJsonObject {
        for (f in schema.fields) {
            val vv = rec.fields[f.id] ?: continue
            put(f.name, valueToJson(vv, f.type, reg))
        }
    }
}

private fun primitiveToJson(value: KdbValue, phy: PhysicalKind, logical: LogicalAnnotation?): JsonElement {
    when (logical) {
        LogicalAnnotation.Date -> {
            require(phy == PhysicalKind.INT32)
            val dv = value as? KdbValue.DateVal ?: throw KdbEncodeException("DateVal")
            val iso = LocalDate.fromEpochDays(dv.daysSinceEpoch).toString()
            return JsonPrimitive(iso)
        }

        is LogicalAnnotation.TimestampMicros, is LogicalAnnotation.TimestampMillis -> {
            require(phy == PhysicalKind.INT64)
            val ts = value as? KdbValue.TimestampVal ?: throw KdbEncodeException("TimestampVal")
            return JsonPrimitive(isoUtcFromMicros(ts.epochMicros))
        }

        LogicalAnnotation.TimeMicros,
        LogicalAnnotation.Uuid,
        LogicalAnnotation.Duration,
        is LogicalAnnotation.Decimal,
        LogicalAnnotation.BigInteger,
        is LogicalAnnotation.BigDecimal,
        is LogicalAnnotation.Custom,
        ->
            throw KdbEncodeException("$logical JSON emit unsupported")

        null -> Unit
    }
    return barePhysicalJson(value, phy)
}

private fun isoUtcFromMicros(epochMicros: Long): String {
    val secs = epochMicros.floorDiv(1_000_000)
    val ns = epochMicros.mod(1_000_000).toInt() * 1000
    return Instant.fromEpochSeconds(secs, nanosecondAdjustment = ns).toString()
}

private fun barePhysicalJson(value: KdbValue, phy: PhysicalKind): JsonElement =
    when (phy) {
        PhysicalKind.NULL -> JsonNull
        PhysicalKind.BOOLEAN -> JsonPrimitive((value as KdbValue.Bool).v)
        PhysicalKind.INT8 -> JsonPrimitive((value as KdbValue.Int8Val).v.toInt())
        PhysicalKind.INT16 -> JsonPrimitive((value as KdbValue.Int16Val).v.toInt())
        PhysicalKind.INT32 -> JsonPrimitive((value as KdbValue.Int32Val).v.toLong())
        PhysicalKind.INT64 -> JsonPrimitive((value as KdbValue.Int64Val).v)
        PhysicalKind.FLOAT32 -> JsonPrimitive((value as KdbValue.Float32Val).v)
        PhysicalKind.FLOAT64 -> JsonPrimitive((value as KdbValue.Float64Val).v)
        PhysicalKind.STRING -> JsonPrimitive((value as KdbValue.StringVal).v)
        PhysicalKind.BYTES -> throw KdbEncodeException("bytes unsupported")
        else -> throw KdbEncodeException("composite unsupported")
    }
