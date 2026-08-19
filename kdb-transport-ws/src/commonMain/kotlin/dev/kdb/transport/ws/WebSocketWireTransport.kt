package dev.kdb.transport.ws

import dev.kdb.stream.WireConnection
import dev.kdb.stream.WireTransport
import dev.kdb.transport.core.OptionAwareWireTransport

public interface WebSocketWireTransport : WireTransport, OptionAwareWireTransport {
    public override suspend fun connect(
        uri: String,
        options: dev.kdb.transport.core.TransportConnectOptions,
    ): dev.kdb.stream.WireConnection

    public suspend fun listen(
        uri: String,
        options: dev.kdb.transport.core.TransportConnectOptions = dev.kdb.transport.core.TransportConnectOptions(),
        handler: suspend (WireConnection) -> Unit,
    )
}

public expect fun defaultWebSocketWireTransport(): WebSocketWireTransport

public interface WebSocketServer {
    public suspend fun start(bindUri: String)
    public suspend fun stop()
    public val activeConnections: Int
}

public expect fun inProcessWebSocketServer(): WebSocketServer
