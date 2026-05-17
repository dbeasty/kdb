package dev.kdb.jdbc

data class KdbJdbcUrl(
    val mode: JdbcMode,
    val catalog: String,
    val namespaceId: String,
    val readOnly: Boolean,
) {
    fun namespaceForTable(table: String): String = "$catalog/$table"
}

enum class JdbcMode {
    MEMORY,
    FILE,
    NETWORK,
}

internal object KdbJdbcUrlParser {
    private const val PREFIX = "jdbc:kdb:"

    fun parse(
        url: String,
        info: java.util.Properties?,
    ): KdbJdbcUrl {
        require(url.startsWith(PREFIX)) { "not a KDB JDBC URL: $url" }
        val rest = url.removePrefix(PREFIX)
        val readOnly =
            info?.getProperty("readOnly")?.toBooleanStrictOrNull()
                ?: url.contains("read_only=true")
        return when {
            rest.startsWith("memory:") -> {
                val path = rest.removePrefix("memory:").trimStart('/')
                val catalog = path.substringBefore('/').ifEmpty { "default" }
                val namespace = if (path.contains('/')) path else "$catalog/main"
                KdbJdbcUrl(JdbcMode.MEMORY, catalog, namespace, readOnly)
            }
            rest.startsWith("file:") -> {
                val path = rest.removePrefix("file:").trimStart('/')
                val catalog = path.substringBefore('/').ifEmpty { "default" }
                val namespace = if (path.contains('/')) path else "$catalog/main"
                KdbJdbcUrl(JdbcMode.FILE, catalog, namespace, readOnly)
            }
            else -> {
                val withoutScheme = rest.removePrefix("//")
                val catalog = withoutScheme.substringBefore('/').substringBefore('?').ifEmpty { "default" }
                val namespace = withoutScheme.substringBefore('?').ifEmpty { "$catalog/main" }
                KdbJdbcUrl(JdbcMode.NETWORK, catalog, namespace, readOnly)
            }
        }
    }

    fun accepts(url: String): Boolean = url.startsWith(PREFIX)
}
