package dev.kdb.auth

import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.AttributedWireConnection

public fun connectionContextFor(conn: WireConnection): ConnectionContext =
    when (conn) {
        is AttributedWireConnection ->
            connectionContextFromHeaders(conn.attributes.httpHeaders)
        else -> ConnectionContext.EMPTY
    }
