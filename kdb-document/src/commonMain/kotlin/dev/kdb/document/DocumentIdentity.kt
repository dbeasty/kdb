package dev.kdb.document

import dev.kdb.codec.KdbUuid
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Document identity per kdb-spec-layer16 §9.4 (Component 73). Bodies round-trip byte-exact: nothing
 * is injected and nothing is reordered on any write path. A supplied top-level `id` is the identity -
 * a UUID string directly, any other non-empty string through [derivedDocumentId].
 */

/** The 16 raw bytes of `KDB_DOC_ID_NAMESPACE = 6f5b9a1c-2d3e-4f70-8a9b-1c2d3e4f5a6b` (§9.4). */
private val DOC_ID_NAMESPACE_BYTES: ByteArray =
    byteArrayOf(
        0x6f, 0x5b, 0x9a.toByte(), 0x1c, 0x2d, 0x3e, 0x4f, 0x70,
        0x8a.toByte(), 0x9b.toByte(), 0x1c, 0x2d, 0x3e, 0x4f, 0x5a, 0x6b,
    )

/** `KDB_DOC_ID_NAMESPACE`: the fixed UUID whose raw bytes prefix every derived id's hash input. */
public val KDB_DOC_ID_NAMESPACE: KdbUuid = KdbUuid.fromBytes(DOC_ID_NAMESPACE_BYTES)

/**
 * Maps an arbitrary non-UUID id string to a stable UUID: `uuid8(sha256(namespaceBytes ‖ utf8(s)))` -
 * the first 16 digest bytes with the version nibble forced to 8 and the RFC 4122 variant bits to `10`.
 * Version 8 (custom) means a derived id can never be mistaken for a random v4 one. Identical to Go's
 * `codec.DerivedUUID`; pinned by `go/testdata/golden/search/derived_id_vectors.json`.
 */
public fun derivedDocumentId(s: String): KdbUuid {
    val digest = kdbSha256(DOC_ID_NAMESPACE_BYTES + s.encodeToByteArray())
    val b = digest.copyOfRange(0, 16)
    b[6] = ((b[6].toInt() and 0x0f) or 0x80).toByte()
    b[8] = ((b[8].toInt() and 0x3f) or 0x80).toByte()
    return KdbUuid.fromBytes(b)
}

/** Outcome of [resolveDocumentId]: [supplied] is false when the body carries no `id` at all. */
public data class ResolvedDocumentId(val id: KdbUuid, val supplied: Boolean)

private val identityJsonParser = Json { ignoreUnknownKeys = true }

/**
 * Reads a body's top-level `id` and maps it to the document identity (§9.4). Absent → a fresh random
 * UUID with `supplied = false` (the caller reports it; the body is still stored untouched). A UUID
 * string is the identity; any other non-empty string goes through [derivedDocumentId]. An `id` that
 * is not a JSON string, or is the empty string, throws [DocumentDecodeException] rather than being
 * silently replaced - a caller who wrote one meant something by it.
 */
public fun resolveDocumentId(json: String): ResolvedDocumentId {
    val root =
        try {
            identityJsonParser.parseToJsonElement(json)
        } catch (e: Throwable) {
            throw DocumentDecodeException("invalid json", cause = e)
        }
    if (root !is JsonObject) {
        throw DocumentDecodeException("root must be a JSON object")
    }
    val idEl = root["id"] ?: return ResolvedDocumentId(KdbUuid.random(), supplied = false)
    if (idEl !is JsonPrimitive || !idEl.isString) {
        throw DocumentDecodeException("\"id\" field must be a string, got $idEl")
    }
    val s = idEl.content
    if (s.isEmpty()) {
        throw DocumentDecodeException("\"id\" field must not be empty")
    }
    val parsed = runCatching { KdbUuid.fromString(s) }.getOrNull()
    return ResolvedDocumentId(parsed ?: derivedDocumentId(s), supplied = true)
}
