package dev.kdb.transport.tcp

import dev.kdb.stream.WireConnection
import dev.kdb.stream.WireTransport

public interface TcpWireTransport : WireTransport {
    public suspend fun listen(uri: String, handler: suspend (WireConnection) -> Unit)
}

public expect fun defaultTcpWireTransport(): TcpWireTransport
