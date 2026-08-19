package dev.kdb.script

import dev.kdb.sql.ColumnSource
import dev.kdb.sql.QueryResult
import dev.kdb.sql.SqlCell
import dev.kdb.sql.SqlParameter
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.double
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put

/**
 * Converts between the plain JSON values a restricted-JS procedure passes (`kdb.query(sql,
 * ["pending"])`) and this codebase's SQL parameter/cell types. Kept deliberately separate from
 * `dev.kdb.sql.SqlParameterWire`, whose tagged `{"t":"s","v":...}` wire format is for the SQL
 * wire protocol, not for script-facing ergonomics.
 */
internal val scriptJson: Json = Json { ignoreUnknownKeys = true }

internal fun parseJsonArrayParams(paramsJson: String): List<SqlParameter> {
    if (paramsJson.isBlank()) return emptyList()
    val element = scriptJson.parseToJsonElement(paramsJson)
    val array = element as? JsonArray ?: return emptyList()
    return array.map { it.toSqlParameter() }
}

private fun JsonElement.toSqlParameter(): SqlParameter =
    when (this) {
        is JsonNull -> SqlParameter.NullParam
        is JsonObject, is JsonArray -> SqlParameter.StringParam(this.toString())
        is JsonPrimitive ->
            when {
                this.isString -> SqlParameter.StringParam(this.content)
                this.booleanOrNull != null -> SqlParameter.BoolParam(this.boolean)
                this.longOrNull != null -> SqlParameter.IntParam(this.long)
                this.doubleOrNull != null -> SqlParameter.DoubleParam(this.double)
                else -> SqlParameter.StringParam(this.content)
            }
    }

internal fun queryResultToJsonArray(result: QueryResult): JsonElement =
    buildJsonArray {
        for (row in result.rows) {
            add(
                buildJsonObject {
                    result.columns.forEachIndexed { i, col ->
                        val cell = row.values.getOrNull(i) ?: SqlCell.Null
                        put(col.name, cell.toJsonElement(col.source))
                    }
                },
            )
        }
    }

private fun SqlCell.toJsonElement(source: ColumnSource): JsonElement =
    when (this) {
        SqlCell.Null -> JsonNull
        is SqlCell.StringVal -> JsonPrimitive(value)
        is SqlCell.LongVal -> JsonPrimitive(value)
        is SqlCell.DoubleVal -> JsonPrimitive(value)
        is SqlCell.BoolVal -> JsonPrimitive(value)
        is SqlCell.JsonVal ->
            if (source == ColumnSource.DOC_JSON) {
                runCatching { scriptJson.parseToJsonElement(json) }.getOrElse { JsonPrimitive(json) }
            } else {
                JsonPrimitive(json)
            }
    }

internal fun singleDocJsonCell(result: QueryResult): String? {
    val row = result.rows.firstOrNull() ?: return null
    val idx = result.columns.indexOfFirst { it.source == ColumnSource.DOC_JSON }.takeIf { it >= 0 } ?: 0
    val cell = row.values.getOrNull(idx) ?: return null
    return (cell as? SqlCell.JsonVal)?.json
}

internal fun docIdFromJson(docJson: String): String? =
    runCatching {
        val obj = scriptJson.parseToJsonElement(docJson) as? JsonObject ?: return null
        obj["kdb_id"]?.jsonPrimitive?.content
    }.getOrNull()

/**
 * `kdb_id` is a SQL pseudo-column, not part of a document's own JSON body - so
 * [HybridScriptDataAccess.get] embeds it into what the script sees (letting `kdb.put(doc)`
 * round-trip the id for an update), and this strips it back out before writing `_doc`, so
 * stored document content isn't polluted by that injected field.
 */
internal fun withKdbId(
    docJson: String,
    id: String,
): String =
    buildJsonObject {
        val obj = runCatching { scriptJson.parseToJsonElement(docJson) as? JsonObject }.getOrNull()
        obj?.forEach { (k, v) -> put(k, v) }
        put("kdb_id", id)
    }.toString()

internal fun withoutKdbId(docJson: String): String {
    val obj = runCatching { scriptJson.parseToJsonElement(docJson) as? JsonObject }.getOrNull() ?: return docJson
    return buildJsonObject {
        obj.forEach { (k, v) -> if (k != "kdb_id") put(k, v) }
    }.toString()
}
