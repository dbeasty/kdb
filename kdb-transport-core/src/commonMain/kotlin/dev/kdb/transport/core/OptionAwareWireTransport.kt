package dev.kdb.transport.core

import dev.kdb.stream.WireConnection
import dev.kdb.stream.WireTransport

/**
 * A [WireTransport] whose `connect` can carry connection options (headers, TLS) — implemented
 * by transports where that's meaningful (e.g. WebSocket). Callers that only need to special-case
 * "does this transport accept options" can depend on this common-multiplatform interface instead
 * of a concrete, target-limited transport implementation.
 */
public interface OptionAwareWireTransport : WireTransport {
    public suspend fun connect(
        uri: String,
        options: TransportConnectOptions,
    ): WireConnection
}
