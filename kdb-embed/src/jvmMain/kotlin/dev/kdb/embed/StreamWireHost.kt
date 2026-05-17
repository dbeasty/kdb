package dev.kdb.embed

import dev.kdb.stream.StreamBroadcastHub
import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.TransportConnectOptions
import dev.kdb.transport.ws.WebSocketWireTransport
import kotlinx.coroutines.flow.collect

public suspend fun runStreamOverWebSocketListen(
    transport: WebSocketWireTransport,
    listenUri: String,
    hub: StreamBroadcastHub,
    transportOptions: TransportConnectOptions = TransportConnectOptions(),
) {
    transport.listen(listenUri, transportOptions) { conn ->
        handleStreamConnection(conn, hub)
    }
}

public suspend fun handleStreamConnection(
    conn: WireConnection,
    hub: StreamBroadcastHub,
) {
    try {
        conn.incoming().collect { frame ->
            val ack = hub.handleFrame(conn, frame)
            if (ack != null) {
                conn.send(ack)
            }
        }
    } finally {
        hub.unregister(conn)
    }
}
