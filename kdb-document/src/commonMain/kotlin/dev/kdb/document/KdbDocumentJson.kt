package dev.kdb.document

import dev.kdb.codec.KdbUuid
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

private val ensureIdJsonParser =
    Json {
        ignoreUnknownKeys = true
    }

/**
 * Returns [json] unchanged if the root object already has an `"id"` key; otherwise returns
 * the same object with `"id": "<canonical-uuid>"` added at the root.
 */
public fun ensureIdInJson(
    json: String,
    id: KdbUuid,
): String {
    val el =
        try {
            ensureIdJsonParser.parseToJsonElement(json)
        } catch (e: Throwable) {
            throw DocumentDecodeException("invalid json", cause = e)
        }
    if (el !is JsonObject) {
        throw DocumentDecodeException("root must be a JSON object")
    }
    if ("id" in el) {
        return json
    }
    val withId =
        buildJsonObject {
            put("id", JsonPrimitive(id.toString()))
            for ((k, v) in el) {
                put(k, v)
            }
        }
    return ensureIdJsonParser.encodeToString(JsonObject.serializer(), withId)
}
