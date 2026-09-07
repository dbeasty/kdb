package dev.kdb.sql

import dev.kdb.json.JsonValue

/**
 * Column-path evaluation with implicit array traversal (Layer 16 §2, Mongo semantics).
 *
 * Walking `$.a.b`: when the value at a segment is an array the rest of the path is applied to
 * every element (document order); when the final value is an array its elements are the
 * candidates. Predicates are true when **any** candidate satisfies them; ORDER BY and projection
 * use the **first** candidate; no candidates is SQL NULL.
 */
internal object SqlPaths {
    fun splitPath(path: String): List<String> = path.split('.').filter { it.isNotEmpty() }

    /** Flattened candidate list per §2. */
    fun candidates(
        root: JsonValue,
        segments: List<String>,
    ): List<JsonValue> {
        val out = ArrayList<JsonValue>(1)
        walk(root, segments, 0, out, flattenFinal = true)
        return out
    }

    /**
     * Like [candidates] but a final array is kept whole — what `ARRAY_CONTAINS`/`ARRAY_LENGTH`
     * inspect. Intermediate arrays are still traversed.
     */
    fun rawValues(
        root: JsonValue,
        segments: List<String>,
    ): List<JsonValue> {
        val out = ArrayList<JsonValue>(1)
        walk(root, segments, 0, out, flattenFinal = false)
        return out
    }

    private fun walk(
        value: JsonValue,
        segments: List<String>,
        index: Int,
        out: MutableList<JsonValue>,
        flattenFinal: Boolean,
    ) {
        if (index == segments.size) {
            if (flattenFinal && value is JsonValue.JArray) out += value.elements else out += value
            return
        }
        when (value) {
            is JsonValue.JObject -> value.fields[segments[index]]?.let { walk(it, segments, index + 1, out, flattenFinal) }
            is JsonValue.JArray -> value.elements.forEach { walk(it, segments, index, out, flattenFinal) }
            else -> Unit
        }
    }

    /** Deep JSON equality; integers and doubles compare numerically (Layer 16 §4). */
    fun jsonEquals(
        a: JsonValue,
        b: JsonValue,
    ): Boolean =
        when {
            a is JsonValue.JInt && b is JsonValue.JInt -> a.value == b.value
            a is JsonValue.JNumber && b is JsonValue.JNumber -> a.value == b.value
            a is JsonValue.JInt && b is JsonValue.JNumber -> a.value.toDouble() == b.value
            a is JsonValue.JNumber && b is JsonValue.JInt -> a.value == b.value.toDouble()
            a is JsonValue.JString && b is JsonValue.JString -> a.value == b.value
            a is JsonValue.JBool && b is JsonValue.JBool -> a.value == b.value
            a === JsonValue.JNull && b === JsonValue.JNull -> true
            a is JsonValue.JArray && b is JsonValue.JArray ->
                a.elements.size == b.elements.size &&
                    a.elements.indices.all { jsonEquals(a.elements[it], b.elements[it]) }
            a is JsonValue.JObject && b is JsonValue.JObject ->
                a.fields.size == b.fields.size &&
                    a.fields.all { (k, v) -> b.fields[k]?.let { jsonEquals(v, it) } == true }
            else -> false
        }

    fun toCell(value: JsonValue): SqlCell =
        when (value) {
            JsonValue.JNull -> SqlCell.Null
            is JsonValue.JString -> SqlCell.StringVal(value.value)
            is JsonValue.JInt -> SqlCell.LongVal(value.value)
            is JsonValue.JNumber -> SqlCell.DoubleVal(value.value)
            is JsonValue.JBool -> SqlCell.BoolVal(value.value)
            is JsonValue.JArray, is JsonValue.JObject -> SqlCell.JsonVal(value.toJsonString())
        }

    fun toJson(cell: SqlCell): JsonValue =
        when (cell) {
            SqlCell.Null -> JsonValue.JNull
            is SqlCell.StringVal -> JsonValue.JString(cell.value)
            is SqlCell.LongVal -> JsonValue.JInt(cell.value)
            is SqlCell.DoubleVal -> JsonValue.JNumber(cell.value)
            is SqlCell.BoolVal -> JsonValue.JBool(cell.value)
            is SqlCell.JsonVal ->
                try {
                    JsonValue.fromJsonString(cell.json)
                } catch (_: Exception) {
                    JsonValue.JString(cell.json)
                }
        }
}
