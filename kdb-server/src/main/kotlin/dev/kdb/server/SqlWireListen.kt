package dev.kdb.server

import dev.kdb.stream.WireConnection
import dev.kdb.transport.ws.WebSocketWireTransport
import kotlinx.coroutines.flow.collect

public suspend fun runSqlWireListen(
    transport: WebSocketWireTransport,
    listenUri: String,
    host: SqlWireHost,
    perConnection: suspend (WireConnection, SqlWireHost) -> Unit = { conn, h ->
        conn.incoming().collect { frame ->
            val response = h.handleFrame(frame)
            if (response != null) {
                conn.send(response)
            }
        }
    },
) {
    transport.listen(listenUri) { conn ->
        perConnection(conn, host)
    }
}
