package dev.kdb.embed

import dev.kdb.peersync.PeerSyncHost
import dev.kdb.stream.WireConnection
import kotlinx.coroutines.flow.collect
import dev.kdb.transport.ws.WebSocketWireTransport
public suspend fun runPeerSyncOverWebSocketListen(
    transport: WebSocketWireTransport,
    listenUri: String,
    host: PeerSyncHost,
    perConnection: suspend (WireConnection, PeerSyncHost) -> Unit = { conn, h ->
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
