package dev.kdb.transport.core

import dev.kdb.stream.WireConnection

public data class WireConnectionAttributes(
    val httpHeaders: Map<String, String> = emptyMap(),
)

public interface AttributedWireConnection : WireConnection {
    public val attributes: WireConnectionAttributes
}
