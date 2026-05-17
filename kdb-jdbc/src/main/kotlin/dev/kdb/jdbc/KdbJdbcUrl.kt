package dev.kdb.jdbc

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

data class KdbJdbcUrl(
    val mode: JdbcMode,
    val catalog: String,
    val namespaceId: String,
    val readOnly: Boolean,
    /** Filesystem root for [JdbcMode.FILE]; segments live under `ns/{namespaceId}/`. */
    val dataRoot: String? = null,
) {
    fun namespaceForTable(table: String): String = "$catalog/$table"
}

enum class JdbcMode {
    MEMORY,
    FILE,
    NETWORK,
}

private data class FileUrlParts(
    val dataRoot: String,
    val catalog: String,
    val namespaceId: String,
)

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
                val fileRest = rest.removePrefix("file:")
                val uri = URI(if (fileRest.startsWith("//")) "file:$fileRest" else "file://$fileRest")
                val path = Paths.get(uri).normalize()
                val nameCount = path.nameCount
                require(nameCount >= 1) { "file JDBC URL must include a data directory path" }
                val fileParts =
                    when {
                        nameCount >= 2 -> {
                            val cat = path.getName(nameCount - 2).toString()
                            val ns = "$cat/${path.getName(nameCount - 1)}"
                            val rootPath =
                                if (nameCount > 2) {
                                    path.subpath(0, nameCount - 2).let { sub ->
                                        if (path.root != null) path.root.resolve(sub) else sub
                                    }
                                } else {
                                    path.root ?: path
                                }
                            FileUrlParts(rootPath.toString(), cat, ns)
                        }
                        else -> {
                            val cat = path.fileName.toString()
                            val root = path.parent?.toString() ?: path.root?.toString() ?: "."
                            FileUrlParts(root, cat, "$cat/main")
                        }
                    }
                KdbJdbcUrl(JdbcMode.FILE, fileParts.catalog, fileParts.namespaceId, readOnly, fileParts.dataRoot)
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
