package dev.kdb.transport.tcp

import dev.kdb.error.TransportException
import dev.kdb.stream.WireConnection

/**
 * Native TCP transport v1: delegates to JVM-style loopback tests on native via stub.
 * Full POSIX socket backend can replace this when native peer deployments land.
 */
public class NativeTcpWireTransport : TcpWireTransport {
    override suspend fun connect(uri: String): WireConnection {
        throw TransportException("native TCP transport not yet implemented for $uri")
    }

    override suspend fun listen(uri: String, handler: suspend (WireConnection) -> Unit) {
        throw TransportException("native TCP listen not yet implemented for $uri")
    }
}

public actual fun defaultTcpWireTransport(): TcpWireTransport = NativeTcpWireTransport()
