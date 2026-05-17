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
    val networkHost: String? = null,
    val networkPort: Int = 7444,
    val inprocHub: String? = null,
    /** Semicolon-separated JDBC properties embedded in the URL (memory mode). */
    val memoryParams: Map<String, String> = emptyMap(),
    val sslEnabled: Boolean = false,
    val sslTrustStore: String? = null,
    val sslTrustStorePassword: String? = null,
    val sslKeyStore: String? = null,
    val sslKeyStorePassword: String? = null,
    val sslTrustAll: Boolean = false,
) {
    fun networkWebSocketUri(): String {
        if (inprocHub != null) {
            return "inproc-ws://$inprocHub"
        }
        val host = networkHost ?: "localhost"
        val scheme = if (sslEnabled) "kdb-wss" else "kdb-ws"
        return "$scheme://$host:$networkPort/kdb"
    }
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
        val ssl = sslSettingsFromProperties(info)
        return when {
            rest.startsWith("memory:") -> {
                val memBody = rest.removePrefix("memory:").trimStart('/')
                val pathPart = memBody.substringBefore(';')
                val params = parseSemicolonParams(memBody.substringAfter(';', ""))
                val catalog = pathPart.substringBefore('/').ifEmpty { "default" }
                val namespace = if (pathPart.contains('/')) pathPart else "$catalog/main"
                KdbJdbcUrl(
                    JdbcMode.MEMORY,
                    catalog,
                    namespace,
                    readOnly,
                    memoryParams = params,
                    sslEnabled = ssl.enabled,
                    sslTrustStore = ssl.trustStore,
                    sslTrustStorePassword = ssl.trustStorePassword,
                    sslKeyStore = ssl.keyStore,
                    sslKeyStorePassword = ssl.keyStorePassword,
                    sslTrustAll = ssl.trustAll,
                )
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
                KdbJdbcUrl(
                    JdbcMode.FILE,
                    fileParts.catalog,
                    fileParts.namespaceId,
                    readOnly,
                    fileParts.dataRoot,
                    sslEnabled = ssl.enabled,
                    sslTrustStore = ssl.trustStore,
                    sslTrustStorePassword = ssl.trustStorePassword,
                    sslKeyStore = ssl.keyStore,
                    sslKeyStorePassword = ssl.keyStorePassword,
                    sslTrustAll = ssl.trustAll,
                )
            }
            rest.startsWith("inproc:") -> {
                val after = rest.removePrefix("inproc:")
                val hub = after.substringBefore('/').ifEmpty { "kdb" }
                val path = after.substringAfter('/', "").substringBefore('?')
                val catalog = path.substringBefore('/').ifEmpty { "default" }
                val namespace =
                    if (path.contains('/')) path else "$catalog/main"
                KdbJdbcUrl(
                    JdbcMode.NETWORK,
                    catalog,
                    namespace,
                    readOnly,
                    inprocHub = hub,
                    sslEnabled = ssl.enabled,
                    sslTrustStore = ssl.trustStore,
                    sslTrustStorePassword = ssl.trustStorePassword,
                    sslKeyStore = ssl.keyStore,
                    sslKeyStorePassword = ssl.keyStorePassword,
                    sslTrustAll = ssl.trustAll,
                )
            }
            else -> {
                val uri = java.net.URI("kdb:$rest")
                val host = uri.host ?: "localhost"
                val port = if (uri.port > 0) uri.port else 7444
                val path = uri.path?.trimStart('/')?.substringBefore('?').orEmpty()
                val catalog =
                    path.substringBefore('/').ifEmpty { "default" }
                val namespace =
                    if (path.contains('/')) {
                        path
                    } else {
                        "$catalog/main"
                    }
                KdbJdbcUrl(
                    JdbcMode.NETWORK,
                    catalog,
                    namespace,
                    readOnly,
                    networkHost = host,
                    networkPort = port,
                    sslEnabled = ssl.enabled,
                    sslTrustStore = ssl.trustStore,
                    sslTrustStorePassword = ssl.trustStorePassword,
                    sslKeyStore = ssl.keyStore,
                    sslKeyStorePassword = ssl.keyStorePassword,
                    sslTrustAll = ssl.trustAll,
                )
            }
        }
    }

    fun accepts(url: String): Boolean = url.startsWith(PREFIX)

    private fun parseSemicolonParams(segment: String): Map<String, String> {
        if (segment.isEmpty()) return emptyMap()
        return segment.split(';').mapNotNull { part ->
            val trimmed = part.trim()
            if (trimmed.isEmpty()) return@mapNotNull null
            val eq = trimmed.indexOf('=')
            if (eq < 0) {
                trimmed to "true"
            } else {
                trimmed.substring(0, eq).trim() to trimmed.substring(eq + 1).trim()
            }
        }.toMap()
    }
}
