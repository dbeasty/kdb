package dev.kdb.cli

import dev.kdb.jdbc.EmbeddedKdbRuntime
import dev.kdb.jdbc.file.NamespacePaths
import dev.kdb.jdbc.file.openFileRuntime
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists

public data class CliConfig(
    val dataDir: String = "${System.getProperty("user.home")}/.kdb",
    val nodeId: String = "local",
    val quiet: Boolean = false,
)

public class CliRuntime(
    public val namespaceId: String,
    internal val embedded: EmbeddedKdbRuntime,
)

public fun openCliRuntime(config: CliConfig, namespaceId: String): CliRuntime {
    val metaDir = NamespacePaths.nsDir(config.dataDir, namespaceId)
    Files.createDirectories(metaDir)
    val metaFile = metaDir.resolve("meta.json")
    if (!metaFile.exists()) {
        metaFile.toFile().writeText("""{"namespaceId":"$namespaceId","createdAt":"${System.currentTimeMillis()}"}""")
    }
    val catalog = NamespacePaths.catalogFromNamespace(namespaceId)
    return CliRuntime(namespaceId, openFileRuntime(config.dataDir, catalog, namespaceId))
}

internal sealed class CliCommand {
    data class Init(val namespace: String) : CliCommand()

    data class Put(val namespace: String, val payload: String) : CliCommand()

    data class Get(val namespace: String, val docId: String) : CliCommand()

    data class Query(val namespace: String, val sql: String) : CliCommand()

    data class Log(val namespace: String) : CliCommand()

    data class Status(val namespace: String) : CliCommand()

    data class Sync(val namespace: String, val peerUri: String) : CliCommand()
}

internal fun parseArgs(args: Array<String>): Pair<CliConfig, CliCommand?> {
    var dataDir: String? = null
    var quiet = false
    val rest = mutableListOf<String>()
    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--data-dir" -> {
                dataDir = args.getOrNull(++i)
                i++
            }
            "--quiet" -> {
                quiet = true
                i++
            }
            else -> {
                rest += args[i]
                i++
            }
        }
    }
    val config = CliConfig(dataDir = dataDir ?: CliConfig().dataDir, quiet = quiet)
    if (rest.isEmpty()) return config to null
    val cmd =
        when (rest[0]) {
            "init" -> {
                require(rest.size >= 2) { "usage: kdb init <namespace>" }
                CliCommand.Init(rest[1])
            }
            "put" -> {
                require(rest.size >= 3) { "usage: kdb put <namespace> <file|json>" }
                CliCommand.Put(rest[1], rest[2])
            }
            "get" -> {
                require(rest.size >= 3) { "usage: kdb get <namespace> <docId>" }
                CliCommand.Get(rest[1], rest[2])
            }
            "query" -> {
                require(rest.size >= 3) { "usage: kdb query <namespace> <sql>" }
                CliCommand.Query(rest[1], rest.drop(2).joinToString(" "))
            }
            "log" -> {
                require(rest.size >= 2) { "usage: kdb log <namespace>" }
                CliCommand.Log(rest[1])
            }
            "status" -> {
                require(rest.size >= 2) { "usage: kdb status <namespace>" }
                CliCommand.Status(rest[1])
            }
            "sync" -> {
                require(rest.size >= 3) { "usage: kdb sync <namespace> <peer-uri>" }
                CliCommand.Sync(rest[1], rest[2])
            }
            else -> throw IllegalArgumentException("unknown command: ${rest[0]}")
        }
    return config to cmd
}
