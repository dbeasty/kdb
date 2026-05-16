package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.toUuidVal
import dev.kdb.codec.toKdbUuid
import dev.kdb.document.internal.sha256Digest
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlin.LazyThreadSafetyMode

/**
 * A KDB document: stable identity + canonical JSON object text.
 */
public data class KdbDocument(
    public val id: KdbUuid,
    public val json: String,
) {
    private val contentHashLazy =
        lazy(LazyThreadSafetyMode.PUBLICATION) {
            computeContentHash(this)
        }

    public val contentHash: KdbHash
        get() = contentHashLazy.value

    public fun toDocumentBodyValue(): KdbValue =
        KdbValue.RecordVal(
            mapOf(
                1 to id.toUuidVal(),
                2 to KdbValue.StringVal(json),
            ),
        )

    public fun merge(patchJson: String): KdbDocument {
        val baseEl =
            try {
                jsonParser.parseToJsonElement(json)
            } catch (e: Throwable) {
                throw DocumentDecodeException("invalid document json", docId = id, cause = e)
            }
        val patchEl =
            try {
                jsonParser.parseToJsonElement(patchJson)
            } catch (e: Throwable) {
                throw DocumentDecodeException("invalid patch json", docId = id, cause = e)
            }
        if (baseEl !is JsonObject) {
            throw DocumentDecodeException("document root must be a JSON object", docId = id)
        }
        if (patchEl !is JsonObject) {
            throw DocumentDecodeException("patch root must be a JSON object", docId = id)
        }
        val merged =
            buildJsonObject {
                for ((k, v) in baseEl) {
                    put(k, v)
                }
                for ((k, v) in patchEl) {
                    put(k, v)
                }
            }
        return KdbDocument(id, jsonParser.encodeToString(JsonObject.serializer(), merged))
    }

    public fun withJson(newJson: String): KdbDocument {
        validateObjectJson(newJson)
        return KdbDocument(id, newJson)
    }

    public companion object {
        private val jsonParser =
            Json {
                ignoreUnknownKeys = true
            }

        public fun fromJson(json: String): KdbDocument {
            validateObjectJson(json)
            return KdbDocument(KdbUuid.random(), json)
        }

        public fun fromJson(
            id: KdbUuid,
            json: String,
        ): KdbDocument {
            validateObjectJson(json)
            return KdbDocument(id, json)
        }

        public fun fromDocumentBodyValue(value: KdbValue): KdbDocument {
            val rec = value as? KdbValue.RecordVal ?: throw DocumentDecodeException("expected DocumentBody record")
            val idVal =
                rec.fields[1] as? KdbValue.UuidVal
                    ?: throw DocumentDecodeException("DocumentBody missing id")
            val js =
                (rec.fields[2] as? KdbValue.StringVal)?.v
                    ?: throw DocumentDecodeException("DocumentBody missing json")
            try {
                validateObjectJson(js)
            } catch (e: DocumentDecodeException) {
                throw DocumentDecodeException(e.message ?: "bad json", docId = idVal.toKdbUuid(), cause = e)
            }
            return KdbDocument(idVal.toKdbUuid(), js)
        }

        private fun validateObjectJson(json: String) {
            val el =
                try {
                    jsonParser.parseToJsonElement(json)
                } catch (e: Throwable) {
                    throw DocumentDecodeException("invalid json", cause = e)
                }
            if (el !is JsonObject) {
                throw DocumentDecodeException("root must be a JSON object")
            }
        }
    }
}

/** SHA-256 of canonical Layer 0 `DocumentBody` bytes. */
public fun computeContentHash(doc: KdbDocument): KdbHash {
    val reg = KdbDocumentWireRegistry()
    val bytes = doc.toDocumentBodyValue().encodeToBytes(DocumentBodyType, reg)
    return KdbHash.fromBytes(sha256Digest(bytes))
}
