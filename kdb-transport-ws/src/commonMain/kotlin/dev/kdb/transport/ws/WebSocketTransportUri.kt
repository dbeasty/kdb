package dev.kdb.transport.ws

public data class WebSocketTransportUri(
    val secure: Boolean,
    val host: String,
    val port: Int,
    val path: String,
    val query: Map<String, String>,
) {
    public fun toWireUri(): String {
        val scheme = if (secure) "wss" else "ws"
        val base = "$scheme://$host:$port$path"
        if (query.isEmpty()) return base
        val q = query.entries.joinToString("&") { "${it.key}=${it.value}" }
        return "$base?$q"
    }

    public companion object {
        internal val schemes = mapOf("ws" to false, "wss" to true, "kdb-ws" to false, "kdb-wss" to true)

        public fun accepts(uri: String): Boolean {
            val scheme = uri.substringBefore("://", missingDelimiterValue = "")
            return scheme in schemes
        }
    }
}

public object WebSocketTransportUriParser {
    public fun parse(uri: String): WebSocketTransportUri {
        require(WebSocketTransportUri.accepts(uri)) { "unsupported WebSocket URI: $uri" }
        val scheme = uri.substringBefore("://")
        val secure = WebSocketTransportUri.schemes.getValue(scheme)
        val rest = uri.substringAfter("://")
        val authority = rest.substringBefore('/').substringBefore('?')
        val pathPart = rest.substringAfter(authority, missingDelimiterValue = "")
        val path = if (pathPart.isEmpty()) "/" else pathPart.substringBefore('?')
        val queryStr = rest.substringAfter('?', "")
        val query =
            if (queryStr.isEmpty()) {
                emptyMap()
            } else {
                queryStr.split('&').mapNotNull { pair ->
                    val kv = pair.split('=', limit = 2)
                    if (kv.size == 2) kv[0] to kv[1] else null
                }.toMap()
            }
        val host =
            if (authority.startsWith('[')) {
                authority.substringAfter('[').substringBefore(']')
            } else {
                authority.substringBeforeLast(':')
            }
        val port =
            if (authority.startsWith('[')) {
                authority.substringAfter("]:").toInt()
            } else {
                val portStr = authority.substringAfterLast(':', "80")
                portStr.toInt()
            }
        return WebSocketTransportUri(secure, host, port, path.ifEmpty { "/" }, query)
    }

    public fun accepts(uri: String): Boolean = WebSocketTransportUri.accepts(uri)
}
