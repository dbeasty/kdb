package dev.kdb.policy

import dev.kdb.schema.KdbSchema

public interface NamespacePolicyParser {
    public fun parse(source: String): NamespacePolicy
    public fun parseJson(json: String, schema: KdbSchema? = null): NamespacePolicy
}

public fun defaultNamespacePolicyParser(): NamespacePolicyParser = DefaultNamespacePolicyParser()

public class DefaultNamespacePolicyParser : NamespacePolicyParser {
    override fun parse(source: String): NamespacePolicy {
        val trimmed = source.trim()
        if (trimmed.startsWith("{")) {
            return parseJson(trimmed)
        }
        return parseJson(dslToJson(trimmed))
    }

    override fun parseJson(
        json: String,
        schema: KdbSchema?,
    ): NamespacePolicy = decodePolicy(json.encodeToByteArray(), schema)

    private fun dslToJson(dsl: String): String {
        var ns = "default"
        var mode = "MUTABLE"
        var history = "FULL"
        var conflict = "STRICT"
        var squash = "AUTO"
        if (dsl.contains("schema = NONE", ignoreCase = true)) {
            // pure document
        }
        val nsMatch = Regex("""namespace\s*\(\s*"([^"]+)"\s*\)""").find(dsl)
        if (nsMatch != null) {
            ns = nsMatch.groupValues[1]
        }
        if (dsl.contains("APPEND_ONLY", ignoreCase = true)) mode = "APPEND_ONLY"
        if (dsl.contains("history = NONE", ignoreCase = true)) history = "NONE"
        if (dsl.contains("ALWAYS_ACCEPT", ignoreCase = true)) conflict = "APPEND_ONLY"
        if (dsl.contains("LAST_WRITE", ignoreCase = true)) conflict = "LAST_WRITE"
        if (dsl.contains("squashAfter = NEVER", ignoreCase = true)) squash = "NEVER"
        return """{"namespaceId":"$ns","mode":"$mode","history":"$history","conflict":"$conflict","compaction":{"squashAfter":"$squash"}}"""
    }
}
