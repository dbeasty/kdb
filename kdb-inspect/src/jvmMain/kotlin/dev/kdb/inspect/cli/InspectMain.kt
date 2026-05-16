package dev.kdb.inspect.cli

import dev.kdb.codec.KdbHash
import dev.kdb.inspect.BlobInspector
import dev.kdb.inspect.DeltaSegmentDump
import dev.kdb.inspect.WireFrameInspector
import dev.kdb.storage.CompressionCodec
import dev.kdb.wire.defaultWireCodec
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.readBytes
import kotlin.system.exitProcess

public fun main(args: Array<String>) {
    if (args.isEmpty()) {
        printUsage()
        exitProcess(1)
    }
    try {
        when (args[0]) {
            "dump-delta" -> dumpDelta(args.drop(1))
            "dump-wire" -> dumpWire(args.drop(1))
            "dump-commit" -> dumpCommit(args.drop(1))
            "dump-blob" -> dumpBlob(args.drop(1))
            else -> {
                System.err.println("Unknown command: ${args[0]}")
                printUsage()
                exitProcess(1)
            }
        }
    } catch (e: Exception) {
        System.err.println("Error: ${e.message}")
        e.printStackTrace()
        exitProcess(1)
    }
}

private fun dumpDelta(args: List<String>) {
    val dataDir = argValue(args, "--data-dir") ?: error("--data-dir required")
    val namespace = argValue(args, "--namespace") ?: error("--namespace required")
    val segment = argValue(args, "--segment")
    val codec = argValue(args, "--codec")?.let { CompressionCodec.valueOf(it.uppercase()) } ?: CompressionCodec.ZSTD
    val pretty = !args.contains("--compact")
    val nsDir = Path.of(dataDir, "ns", namespace, "delta")
    if (!Files.isDirectory(nsDir)) {
        error("delta directory not found: $nsDir")
    }
    val files =
        if (segment != null) {
            listOf(nsDir.resolve(segment))
        } else {
            Files.list(nsDir).filter { Files.isRegularFile(it) }.sorted().toList()
        }
  for (file in files) {
        println("=== ${file.fileName} ===")
        val bytes = file.readBytes()
        println(DeltaSegmentDump.dumpSegmentBytes(bytes, codec, pretty))
    }
}

private fun dumpWire(args: List<String>) {
    val file = argValue(args, "--file") ?: error("--file required")
    val pretty = !args.contains("--compact")
    val frame = Path.of(file).readBytes()
    val inspector = WireFrameInspector(defaultWireCodec())
    println(inspector.dumpFrame(frame, pretty))
}

private fun dumpCommit(args: List<String>) {
    val file = argValue(args, "--file") ?: error("--file required")
    val bytes = Path.of(file).readBytes()
    println(BlobInspector.dumpCommitPayload(bytes))
}

private fun dumpBlob(args: List<String>) {
    val dataDir = argValue(args, "--data-dir") ?: error("--data-dir required")
    val hashHex = argValue(args, "--hash") ?: error("--hash required")
    val hash = KdbHash.fromHex(hashHex)
    val blobPath = Path.of(dataDir, "ns", "blobs", hashHex.take(2), hashHex)
    if (!Files.exists(blobPath)) {
        error("blob not found: $blobPath")
    }
    println(BlobInspector.dumpRawBlob(blobPath.readBytes(), hash))
}

private fun argValue(args: List<String>, name: String): String? {
    val idx = args.indexOf(name)
    if (idx < 0 || idx + 1 >= args.size) return null
    return args[idx + 1]
}

private fun printUsage() {
    println(
        """
        kdb inspect — debug JSON views (non-authoritative)

        Usage:
          inspect dump-delta  --data-dir DIR --namespace NS [--segment SEG] [--codec zstd|none]
          inspect dump-wire   --file FRAME.bin [--compact]
          inspect dump-commit --file PAYLOAD.bin
          inspect dump-blob   --data-dir DIR --hash HEX
        """.trimIndent(),
    )
}
