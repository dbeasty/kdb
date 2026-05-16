package dev.kdb.json

/**
 * SQL-facing JSON function descriptors (Layer 1 Component 4).
 */
public enum class JsonFunctionReturnType {
    JSON_STRING,
    SCALAR,
    BOOLEAN,
    INTEGER,
    STRING_LIST,
}

public data class KdbJsonFunctionDescriptor(
    val sqlName: String,
    val minArgs: Int,
    val maxArgs: Int,
    val returnType: JsonFunctionReturnType,
    val evaluate: (args: List<JsonValue?>) -> JsonValue?,
)

public object KdbJsonFunctionRegistry {
    /** All registered descriptors. */
    public val all: List<KdbJsonFunctionDescriptor> =
        listOf(
            KdbJsonFunctionDescriptor(
                sqlName = "kdb_json_get",
                minArgs = 2,
                maxArgs = 2,
                returnType = JsonFunctionReturnType.SCALAR,
                evaluate = { args ->
                    val doc = args[0] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val path = args[1] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    kdbJsonGet(doc.value, path.value)
                },
            ),
            KdbJsonFunctionDescriptor(
                sqlName = "kdb_json_set",
                minArgs = 3,
                maxArgs = 3,
                returnType = JsonFunctionReturnType.JSON_STRING,
                evaluate = { args ->
                    val doc = args[0] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val path = args[1] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val value = args[2] ?: return@KdbJsonFunctionDescriptor null
                    JsonValue.JString(kdbJsonSet(doc.value, path.value, value))
                },
            ),
            KdbJsonFunctionDescriptor(
                sqlName = "kdb_json_delete",
                minArgs = 2,
                maxArgs = 2,
                returnType = JsonFunctionReturnType.JSON_STRING,
                evaluate = { args ->
                    val doc = args[0] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val path = args[1] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    JsonValue.JString(kdbJsonDelete(doc.value, path.value))
                },
            ),
            KdbJsonFunctionDescriptor(
                sqlName = "kdb_json_merge",
                minArgs = 2,
                maxArgs = 2,
                returnType = JsonFunctionReturnType.JSON_STRING,
                evaluate = { args ->
                    val doc = args[0] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val patch = args[1] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    JsonValue.JString(kdbJsonMerge(doc.value, patch.value))
                },
            ),
            KdbJsonFunctionDescriptor(
                sqlName = "kdb_json_contains",
                minArgs = 3,
                maxArgs = 3,
                returnType = JsonFunctionReturnType.BOOLEAN,
                evaluate = { args ->
                    val doc = args[0] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor JsonValue.JBool(false)
                    val path = args[1] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor JsonValue.JBool(false)
                    val value = args[2] ?: return@KdbJsonFunctionDescriptor JsonValue.JBool(false)
                    JsonValue.JBool(kdbJsonContains(doc.value, path.value, value))
                },
            ),
            KdbJsonFunctionDescriptor(
                sqlName = "kdb_json_keys",
                minArgs = 2,
                maxArgs = 2,
                returnType = JsonFunctionReturnType.STRING_LIST,
                evaluate = { args ->
                    val doc = args[0] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val path = args[1] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val keys = kdbJsonKeys(doc.value, path.value) ?: return@KdbJsonFunctionDescriptor null
                    JsonValue.JArray(keys.map { JsonValue.JString(it) })
                },
            ),
            KdbJsonFunctionDescriptor(
                sqlName = "kdb_json_type",
                minArgs = 2,
                maxArgs = 2,
                returnType = JsonFunctionReturnType.SCALAR,
                evaluate = { args ->
                    val doc = args[0] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val path = args[1] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val t = kdbJsonType(doc.value, path.value) ?: return@KdbJsonFunctionDescriptor null
                    JsonValue.JString(t)
                },
            ),
            KdbJsonFunctionDescriptor(
                sqlName = "kdb_json_array_length",
                minArgs = 2,
                maxArgs = 2,
                returnType = JsonFunctionReturnType.INTEGER,
                evaluate = { args ->
                    val doc = args[0] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val path = args[1] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val n = kdbJsonArrayLength(doc.value, path.value) ?: return@KdbJsonFunctionDescriptor null
                    JsonValue.JInt(n.toLong())
                },
            ),
            KdbJsonFunctionDescriptor(
                sqlName = "kdb_json_get_all",
                minArgs = 2,
                maxArgs = 2,
                returnType = JsonFunctionReturnType.SCALAR,
                evaluate = { args ->
                    val doc = args[0] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    val path = args[1] as? JsonValue.JString ?: return@KdbJsonFunctionDescriptor null
                    JsonValue.JArray(kdbJsonGetAll(doc.value, path.value))
                },
            ),
        )

    private val byLowerName: Map<String, KdbJsonFunctionDescriptor> =
        all.associateBy { it.sqlName.lowercase() }

    public fun get(sqlName: String): KdbJsonFunctionDescriptor? = byLowerName[sqlName.lowercase()]
}
