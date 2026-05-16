package dev.kdb.json

internal object JsonWriter {
    fun write(v: JsonValue): String =
        buildString {
            appendValue(v)
        }

    private fun StringBuilder.appendValue(v: JsonValue) {
        when (v) {
            is JsonValue.JString -> appendString(v.value)
            is JsonValue.JNumber -> append(v.value.toString())
            is JsonValue.JInt -> append(v.value.toString())
            is JsonValue.JBool -> append(if (v.value) "true" else "false")
            JsonValue.JNull -> append("null")
            is JsonValue.JArray -> {
                append('[')
                v.elements.forEachIndexed { idx, e ->
                    if (idx > 0) append(',')
                    appendValue(e)
                }
                append(']')
            }
            is JsonValue.JObject -> {
                append('{')
                var first = true
                for ((k, valv) in v.fields) {
                    if (!first) append(',')
                    first = false
                    appendString(k)
                    append(':')
                    appendValue(valv)
                }
                append('}')
            }
        }
    }

    private fun StringBuilder.appendString(s: String) {
        append('"')
        for (c in s) {
            when (c) {
                '"' -> append("\\\"")
                '\\' -> append("\\\\")
                '\b' -> append("\\b")
                '\u000C' -> append("\\f")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                else ->
                    if (c.code < 0x20) {
                        append("\\u")
                        val hex = c.code.toString(16).padStart(4, '0')
                        append(hex)
                    } else {
                        append(c)
                    }
            }
        }
        append('"')
    }
}
