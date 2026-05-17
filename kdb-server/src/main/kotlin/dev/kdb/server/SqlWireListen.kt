package dev.kdb.server

import dev.kdb.auth.AllowAllAuth
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.ConnectionContext
import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.TransportConnectOptions
import dev.kdb.transport.ws.WebSocketWireTransport
import kotlinx.coroutines.flow.collect

public fun sqlWireHostFactory(
    wire: dev.kdb.wire.WireCodec,
    server: KdbServerRuntime,
    defaultNamespace: String,
    auth: AuthEngine = AllowAllAuth,
): (ConnectionContext) -> SqlWireHost =
    { ctx ->
        SqlWireHost(wire, server, defaultNamespace, auth, ctx)
    }

public suspend fun runSqlWireListen(
    transport: WebSocketWireTransport,
    listenUri: String,
    host: SqlWireHost,
    transportOptions: TransportConnectOptions = TransportConnectOptions(),
    perConnection: suspend (WireConnection, SqlWireHost) -> Unit = { conn, h ->
        conn.incoming().collect { frame ->
            val response = h.handleFrame(frame)
            if (response != null) {
                conn.send(response)
            }
        }
    },
) {
    transport.listen(listenUri, transportOptions) { conn ->
        perConnection(conn, host)
    }
}

public suspend fun runSqlWireListen(
    transport: WebSocketWireTransport,
    listenUri: String,
    hostFactory: (ConnectionContext) -> SqlWireHost,
    transportOptions: TransportConnectOptions = TransportConnectOptions(),
    perConnection: suspend (WireConnection, SqlWireHost) -> Unit = { conn, h ->
        conn.incoming().collect { frame ->
            val response = h.handleFrame(frame)
            if (response != null) {
                conn.send(response)
            }
        }
    },
) {
    transport.listen(listenUri, transportOptions) { conn ->
        val ctx = connectionContextFor(conn)
        perConnection(conn, hostFactory(ctx))
    }
}
