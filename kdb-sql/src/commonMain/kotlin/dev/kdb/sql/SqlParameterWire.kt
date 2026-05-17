package dev.kdb.sql

import dev.kdb.json.JsonValue

/**
 * JSON wire form for prepared-statement parameters (network JDBC / SQL server).
 *
 * Each element: `{"t":"s|i|d|b|n","v":...}` (`v` omitted for null).
 */
public fun encodeSqlParameters(parameters: List<SqlParameter>): String? {
    if (parameters.isEmpty()) return null
    return JsonValue.JArray(parameters.map { it.toWireJson() }).toJsonString()
}

public fun decodeSqlParameters(parametersJson: String?): List<SqlParameter> {
    if (parametersJson.isNullOrBlank()) return emptyList()
    val root = JsonValue.fromJsonString(parametersJson.trim())
    val array = root as? JsonValue.JArray ?: return emptyList()
    return array.elements.map { wireJsonToParameter(it) }
}

private fun SqlParameter.toWireJson(): JsonValue {
    val fields = LinkedHashMap<String, JsonValue>()
    when (this) {
        SqlParameter.NullParam -> fields["t"] = JsonValue.JString("n")
        is SqlParameter.StringParam -> {
            fields["t"] = JsonValue.JString("s")
            fields["v"] = JsonValue.JString(value)
        }
        is SqlParameter.IntParam -> {
            fields["t"] = JsonValue.JString("i")
            fields["v"] = JsonValue.JInt(value)
        }
        is SqlParameter.DoubleParam -> {
            fields["t"] = JsonValue.JString("d")
            fields["v"] = JsonValue.JNumber(value)
        }
        is SqlParameter.BoolParam -> {
            fields["t"] = JsonValue.JString("b")
            fields["v"] = JsonValue.JBool(value)
        }
    }
    return JsonValue.JObject(fields)
}

private fun wireJsonToParameter(element: JsonValue): SqlParameter {
    val obj = element as? JsonValue.JObject ?: throw SqlPlanningException("invalid parameter wire object", "")
    val type = obj.fields["t"]?.let { (it as? JsonValue.JString)?.value } ?: throw SqlPlanningException("parameter missing type", "")
    return when (type) {
        "n" -> SqlParameter.NullParam
        "s" ->
            SqlParameter.StringParam(
                (obj.fields["v"] as? JsonValue.JString)?.value
                    ?: throw SqlPlanningException("string parameter missing value", ""),
            )
        "i" ->
            SqlParameter.IntParam(
                when (val v = obj.fields["v"]) {
                    is JsonValue.JInt -> v.value
                    is JsonValue.JNumber -> v.value.toLong()
                    else -> throw SqlPlanningException("int parameter missing value", "")
                },
            )
        "d" ->
            SqlParameter.DoubleParam(
                when (val v = obj.fields["v"]) {
                    is JsonValue.JNumber -> v.value
                    is JsonValue.JInt -> v.value.toDouble()
                    else -> throw SqlPlanningException("double parameter missing value", "")
                },
            )
        "b" ->
            SqlParameter.BoolParam(
                (obj.fields["v"] as? JsonValue.JBool)?.value
                    ?: throw SqlPlanningException("bool parameter missing value", ""),
            )
        else -> throw SqlPlanningException("unknown parameter type: $type", "")
    }
}
