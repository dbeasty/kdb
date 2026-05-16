package dev.kdb.inspect.sidecar

import dev.kdb.inspect.InspectJson
import dev.kdb.storage.DebugSidecarConfig
import dev.kdb.storage.DeltaDebugHook
import dev.kdb.storage.NoOpDeltaDebugHook
import dev.kdb.wire.WireMessage

public fun interface WireDebugHook {
    public suspend fun onWire(
        message: WireMessage,
        direction: String,
    )
}

public object NoOpWireDebugHook : WireDebugHook {
    override suspend fun onWire(
        message: WireMessage,
        direction: String,
    ) {
    }
}

public expect fun deltaDebugHook(config: DebugSidecarConfig): DeltaDebugHook

public expect fun wireDebugHook(config: DebugSidecarConfig, namespaceId: String): WireDebugHook

internal class NoOpDeltaHook : DeltaDebugHook by NoOpDeltaDebugHook

internal class NoOpWireHook : WireDebugHook by NoOpWireDebugHook

public fun deltaDebugHookOrNoOp(config: DebugSidecarConfig?): DeltaDebugHook {
    if (config == null || !config.enabled || !config.logDelta) return NoOpDeltaDebugHook
    return deltaDebugHook(config)
}

public fun wireDebugHookOrNoOp(
    config: DebugSidecarConfig?,
    namespaceId: String,
): WireDebugHook {
    if (config == null || !config.enabled || !config.logWire) return NoOpWireDebugHook
    return wireDebugHook(config, namespaceId)
}

internal expect class FileSidecarWriter(
    config: DebugSidecarConfig,
) {
    suspend fun appendDeltaLine(namespaceId: String, line: String)
    suspend fun appendWireLine(namespaceId: String, line: String)
}

internal suspend fun FileSidecarWriter.writeDelta(
    config: DebugSidecarConfig,
    namespaceId: String,
    line: String,
) {
    if (config.enabled && config.logDelta) appendDeltaLine(namespaceId, line)
}

internal suspend fun FileSidecarWriter.writeWire(
    config: DebugSidecarConfig,
    namespaceId: String,
    line: String,
) {
    if (config.enabled && config.logWire) appendWireLine(namespaceId, line)
}

internal class DelegatingDeltaHook(
    private val config: DebugSidecarConfig,
    private val writer: FileSidecarWriter,
) : DeltaDebugHook {
    override suspend fun onAppend(
        record: dev.kdb.storage.DeltaRecord,
        segmentId: dev.kdb.codec.KdbUuid,
        offset: Long,
    ) {
        val line = InspectJson.deltaRecordToJsonLine(record, segmentId, offset)
        writer.writeDelta(config, record.namespaceId, line)
    }
}

internal class DelegatingWireHook(
    private val config: DebugSidecarConfig,
    private val namespaceId: String,
    private val writer: FileSidecarWriter,
) : WireDebugHook {
    override suspend fun onWire(
        message: WireMessage,
        direction: String,
    ) {
        val line = InspectJson.wireMessageToJsonLine(message, direction)
        writer.writeWire(config, namespaceId, line)
    }
}
