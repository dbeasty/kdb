package dev.kdb.transport.tcp

public data class TcpTransportUri(
    val host: String,
    val port: Int,
    val bind: Boolean = false,
) {
    public companion object {
        private val schemes = setOf("kdb-tcp", "tcp")

        public fun accepts(uri: String): Boolean {
            val scheme = uri.substringBefore("://", missingDelimiterValue = "")
            return scheme in schemes
        }

        public fun parse(uri: String): TcpTransportUri {
            require(accepts(uri)) { "unsupported TCP URI: $uri" }
            val withoutScheme = uri.substringAfter("://")
            val hostPort = withoutScheme.substringBefore('?')
            val query = withoutScheme.substringAfter('?', "")
            val bind = query.split('&').any { it == "bind=true" }
            val host =
                when {
                    hostPort.startsWith('[') -> {
                        val end = hostPort.indexOf(']')
                        require(end > 0) { "invalid IPv6 URI: $uri" }
                        hostPort.substring(1, end)
                    }
                    else -> hostPort.substringBeforeLast(':')
                }
            val port =
                when {
                    hostPort.startsWith('[') -> hostPort.substringAfter("]:").toInt()
                    else -> hostPort.substringAfterLast(':').toInt()
                }
            return TcpTransportUri(host = host, port = port, bind = bind)
        }
    }
}
