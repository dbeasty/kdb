package dev.kdb.transport.ws

import dev.kdb.stream.WireConnection
import dev.kdb.stream.WireTransport

public interface WebSocketWireTransport : WireTransport {
    public suspend fun listen(uri: String, handler: suspend (WireConnection) -> Unit)
}

public expect fun defaultWebSocketWireTransport(): WebSocketWireTransport

public interface WebSocketServer {
    public suspend fun start(bindUri: String)
    public suspend fun stop()
    public val activeConnections: Int
}

public expect fun inProcessWebSocketServer(): WebSocketServer
