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
            "verify" -> verifyCmd(args.drop(1))
            "repair-segments" -> repairSegmentsCmd(args.drop(1))
            "restore" -> restoreCmd(args.drop(1))
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

internal fun argValue(args: List<String>, name: String): String? {
    val idx = args.indexOf(name)
    if (idx < 0 || idx + 1 >= args.size) return null
    return args[idx + 1]
}

private fun printUsage() {
    println(
        """
        kdb inspect — debug JSON views and data-directory maintenance (kdb-spec-layer15)

        Usage:
          inspect dump-delta  --data-dir DIR --namespace NS [--segment SEG] [--codec zstd|none]
          inspect dump-wire   --file FRAME.bin [--compact]
          inspect dump-commit --file PAYLOAD.bin
          inspect dump-blob   --data-dir DIR --hash HEX

          inspect verify --data-dir DIR --namespace NS [--level L1|L2] [--codec zstd|none] [--json]
              Walk the delta log and report corruption without changing anything.

          inspect repair-segments --data-dir DIR --namespace NS [--codec zstd|none] [--dry-run]
              Truncate torn tails and quarantine corrupt frames where provably safe.
              Refuses (naming the missing commits) when a repair would drop history
              still referenced by later segments - run restore instead in that case.

          inspect restore --namespace NS --out DIR --source LABEL=PATH [--source LABEL=PATH ...] [--codec zstd|none]
              Rebuild a namespace's delta log into DIR from the verified union of one
              or more sources (a damaged local data directory, a backup directory, or
              both for a hybrid restore).

        Stop the owning process before running verify, repair-segments, or restore
        against its data directory - none of these commands take a directory lock,
        so running them concurrently with a live writer is not safe.
        """.trimIndent(),
    )
}
