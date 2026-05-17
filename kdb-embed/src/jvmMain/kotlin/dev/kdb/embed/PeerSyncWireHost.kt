package dev.kdb.embed

import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.connectionContextFor
import dev.kdb.peersync.PeerSyncHost
import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.TransportConnectOptions
import dev.kdb.transport.ws.WebSocketWireTransport
import kotlinx.coroutines.flow.collect

public suspend fun runPeerSyncOverWebSocketListen(
    transport: WebSocketWireTransport,
    listenUri: String,
    host: PeerSyncHost,
    transportOptions: TransportConnectOptions = TransportConnectOptions(),
    perConnection: suspend (WireConnection, PeerSyncHost) -> Unit = { conn, h ->
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

public suspend fun runPeerSyncOverWebSocketListen(
    transport: WebSocketWireTransport,
    listenUri: String,
    hostFactory: (ConnectionContext) -> PeerSyncHost,
    transportOptions: TransportConnectOptions = TransportConnectOptions(),
    perConnection: suspend (WireConnection, PeerSyncHost) -> Unit = { conn, h ->
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
