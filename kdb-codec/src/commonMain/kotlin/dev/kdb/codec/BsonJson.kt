@file:OptIn(
    kotlin.io.encoding.ExperimentalEncodingApi::class,
    kotlinx.serialization.ExperimentalSerializationApi::class,
)

package dev.kdb.codec

import dev.kdb.error.BsonDecodeException
import kotlin.io.encoding.Base64
import kotlinx.datetime.Instant
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

private val jsonFlat =
    Json {

        ignoreUnknownKeys = true

    }

private fun jsonPrettyFmt(indent: Int) =
    Json {
        ignoreUnknownKeys = true
        prettyPrint = true
        prettyPrintIndent = " ".repeat(indent.coerceIn(1, 32))
    }

public fun BsonDocument.Companion.fromJson(json: String): BsonDocument =
    try {
        val el = jsonFlat.parseToJsonElement(json)
        if (el !is JsonObject) throw BsonDecodeException("top-level JSON must be object", offset = 0)
        jsonObjToDoc(el)
    } catch (e: Throwable) {
        throw BsonDecodeException(message = "JSON error: ${e.message}", offset = 0, cause = e)
    }

public fun BsonDocument.toJson(): String =
    jsonFlat.encodeToString(docToJsonObject(this))

public fun BsonDocument.toPrettyJson(indent: Int = 2): String =
    jsonPrettyFmt(indent).encodeToString(docToJsonObject(this))

private fun jsonObjToDoc(obj: JsonObject): BsonDocument {

    val map = LinkedHashMap<String, BsonValue>()

    for (k in obj.keys.sorted()) {

        val v = obj[k] ?: JsonNull

        val bson = jsonElementToBson(v)



        if (map.put(k, bson) != null) bsonOnDuplicateDocumentKey(k)

    }

    return BsonDocument(map)

}

private fun jsonElementToBson(el: JsonElement): BsonValue =
    when (el) {


        is JsonNull ->
            BsonNull

        is JsonObject ->
            jsonObjToDoc(el)

        is JsonArray ->
            bsonArrayFromJson(el)

        is JsonPrimitive ->
            jsonPrimitiveToBson(el)

        else ->
            throw BsonDecodeException("unsupported JsonElement", offset = -1)

    }

private fun bsonArrayFromJson(a: JsonArray): BsonArray {


    val lst = mutableListOf<BsonValue>()
    for (e in a)
        lst += jsonElementToBson(e)



    return BsonArray(lst)
}

private fun jsonPrimitiveToBson(p: JsonPrimitive): BsonValue {
    p.booleanOrNull?.let { return BsonBoolean(it) }
    if (p.isString) return BsonString(p.content)
    val s = p.content
    val sl = s.lowercase()
    if ('.' in s || 'e' in sl) {
        val d = p.doubleOrNull ?: throw BsonDecodeException("bad JSON fractional number '$s'", offset = -1)
        if (!d.isFinite()) throw BsonDecodeException("BSON rejects non-finite doubles", offset = -1)
        return BsonDouble(d)
    }
    val lng = p.longOrNull ?: throw BsonDecodeException("bad JSON integral '$s'", offset = -1)
    return if (lng in Int.MIN_VALUE.toLong() .. Int.MAX_VALUE.toLong()) BsonInt32(lng.toInt()) else BsonInt64(lng)
}

private fun docToJsonObject(d: BsonDocument): JsonObject =
    buildJsonObject {
        for (key in d.fields.keys.sorted()) put(key, bsonValueToJson(d.fields[key]!!))
    }

private fun bsonValueToJson(v: BsonValue): JsonElement =
    when (v) {
        is BsonDocument -> docToJsonObject(v)
        is BsonArray -> buildJsonArray { v.elements.forEach { add(bsonValueToJson(it)) } }
        is BsonString -> JsonPrimitive(v.value)
        is BsonInt32 -> JsonPrimitive(v.value)
        is BsonInt64 -> JsonPrimitive(v.value)
        is BsonDouble -> JsonPrimitive(v.value)
        is BsonBoolean -> JsonPrimitive(v.value)
        is BsonNull -> JsonNull
        is BsonDateTime -> JsonPrimitive(Instant.fromEpochMilliseconds(v.epochMillis).toString())
        is BsonBinary -> JsonPrimitive(Base64.encode(v.data))
    }

