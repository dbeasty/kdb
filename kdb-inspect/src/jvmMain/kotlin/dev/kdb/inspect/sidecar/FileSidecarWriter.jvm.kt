package dev.kdb.inspect.sidecar

import dev.kdb.storage.DebugSidecarConfig
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

internal actual class FileSidecarWriter actual constructor(
    private val config: DebugSidecarConfig,
) {
    actual suspend fun appendDeltaLine(namespaceId: String, line: String) {
        appendLine(namespaceId, "delta.jsonl", line)
    }

    actual suspend fun appendWireLine(namespaceId: String, line: String) {
        appendLine(namespaceId, "wire.jsonl", line)
    }

    private suspend fun appendLine(namespaceId: String, fileName: String, line: String) =
        withContext(Dispatchers.IO) {
            val dir = Path.of(config.directory, namespaceId, "debug")
            Files.createDirectories(dir)
            val file = dir.resolve(fileName)
            Files.writeString(
                file,
                line + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
            )
        }
}

public actual fun deltaDebugHook(config: DebugSidecarConfig): dev.kdb.storage.DeltaDebugHook {
    val writer = FileSidecarWriter(config)
    return DelegatingDeltaHook(config, writer)
}

public actual fun wireDebugHook(
    config: DebugSidecarConfig,
    namespaceId: String,
): WireDebugHook {
    val writer = FileSidecarWriter(config)
    return DelegatingWireHook(config, namespaceId = namespaceId, writer = writer)
}
