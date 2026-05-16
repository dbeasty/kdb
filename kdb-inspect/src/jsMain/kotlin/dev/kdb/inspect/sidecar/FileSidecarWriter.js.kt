package dev.kdb.inspect.sidecar

import dev.kdb.storage.DebugSidecarConfig
import dev.kdb.storage.NoOpDeltaDebugHook

internal actual class FileSidecarWriter actual constructor(
    config: DebugSidecarConfig,
) {
    actual suspend fun appendDeltaLine(namespaceId: String, line: String) {}
    actual suspend fun appendWireLine(namespaceId: String, line: String) {}
}

public actual fun deltaDebugHook(config: DebugSidecarConfig): dev.kdb.storage.DeltaDebugHook = NoOpDeltaDebugHook

public actual fun wireDebugHook(
    config: DebugSidecarConfig,
    namespaceId: String,
): WireDebugHook = NoOpWireDebugHook
