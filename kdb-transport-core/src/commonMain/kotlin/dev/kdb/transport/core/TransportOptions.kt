package dev.kdb.transport.core

import dev.kdb.wire.DEFAULT_MAX_FRAME_BYTES

public data class TransportConnectOptions(
    val connectTimeoutMs: Long = 10_000,
    val readTimeoutMs: Long = 0,
    val maxFrameBytes: Int = DEFAULT_MAX_FRAME_BYTES,
    val connectHeaders: Map<String, String> = emptyMap(),
    val tls: TransportTlsSettings? = null,
)
