package dev.kdb.jdbc.remote

import dev.kdb.jdbc.KdbBinding
import dev.kdb.sql.SqlParameter
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.sql.SQLException
import java.util.Base64

/**
 * Parameters in the plain JSON forms the Go server decodes (go/kdb/server/wire_listen.go
 * decodeParametersJSON): null, a string, a number, a boolean, an array of numbers (a vector), and,
 * with the `attachments` wire feature, `{"$bytes": "<standard base64>"}` and
 * `{"$attachment": {"hash": "<hex>", "size": n}}`.
 *
 * The Kotlin server's own form is the tagged `{"t": .., "v": ..}` of dev.kdb.sql.encodeSqlParameters,
 * which the Go server does not read; this form is used when the server agreed `attachments` (which
 * only the Go server does). A [KdbBinding.Stream] must have been uploaded (resolved to an
 * [KdbBinding.AttachmentRef]) before it gets here.
 */
internal fun encodePlainParameters(bindings: List<KdbBinding>): String? {
    if (bindings.isEmpty()) return null
    return JsonArray(bindings.map { it.toPlainJson() }).toString()
}

private fun KdbBinding.toPlainJson(): JsonElement =
    when (this) {
        is KdbBinding.Plain ->
            when (val p = parameter) {
                SqlParameter.NullParam -> JsonNull
                is SqlParameter.StringParam -> JsonPrimitive(p.value)
                is SqlParameter.IntParam -> JsonPrimitive(p.value)
                is SqlParameter.DoubleParam -> {
                    if (!p.value.isFinite()) throw SQLException("parameter ${p.value} is not a finite number")
                    JsonPrimitive(p.value)
                }
                is SqlParameter.BoolParam -> JsonPrimitive(p.value)
                is SqlParameter.VectorParam -> JsonArray(p.asFloatArray().map { JsonPrimitive(it.toDouble()) })
            }
        is KdbBinding.Bytes -> JsonObject(mapOf("\$bytes" to JsonPrimitive(Base64.getEncoder().encodeToString(value))))
        is KdbBinding.AttachmentRef ->
            JsonObject(
                mapOf(
                    "\$attachment" to JsonObject(mapOf("hash" to JsonPrimitive(hashHex), "size" to JsonPrimitive(size))),
                ),
            )
        is KdbBinding.Stream -> throw IllegalStateException("a stream parameter must be uploaded before encoding")
    }
