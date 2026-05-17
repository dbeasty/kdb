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
    public val embedded: EmbeddedKdbRuntime,
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

internal sealed interface CliCommand

internal sealed class CoreCliCommand : CliCommand {
    data class Init(val namespace: String) : CoreCliCommand()

    data class Put(val namespace: String, val payload: String) : CoreCliCommand()

    data class Get(val namespace: String, val docId: String) : CoreCliCommand()

    data class Query(val namespace: String, val sql: String) : CoreCliCommand()

    data class Log(val namespace: String) : CoreCliCommand()

    data class Status(val namespace: String) : CoreCliCommand()

    data class Sync(val namespace: String, val peerUri: String) : CoreCliCommand()

    data class Shell(val namespace: String) : CoreCliCommand()
}

internal data class FileCliWrapper(val command: FileCliCommand) : CliCommand

internal fun parseArgs(args: Array<String>): Pair<CliConfig, CliCommand?> =
    parseCoreArgs(args).let { (config, core) ->
        if (core != null) config to core else parseFileCommandAsCli(args)
    }

private fun parseCoreArgs(args: Array<String>): Pair<CliConfig, CliCommand?> {
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
    if (rest[0] == "file") return config to null
    val cmd =
        when (rest[0]) {
            "init" -> {
                require(rest.size >= 2) { "usage: kdb init <namespace>" }
                CoreCliCommand.Init(rest[1])
            }
            "put" -> {
                require(rest.size >= 3) { "usage: kdb put <namespace> <file|json>" }
                CoreCliCommand.Put(rest[1], rest[2])
            }
            "get" -> {
                require(rest.size >= 3) { "usage: kdb get <namespace> <docId>" }
                CoreCliCommand.Get(rest[1], rest[2])
            }
            "query" -> {
                require(rest.size >= 3) { "usage: kdb query <namespace> <sql>" }
                CoreCliCommand.Query(rest[1], rest.drop(2).joinToString(" "))
            }
            "log" -> {
                require(rest.size >= 2) { "usage: kdb log <namespace>" }
                CoreCliCommand.Log(rest[1])
            }
            "status" -> {
                require(rest.size >= 2) { "usage: kdb status <namespace>" }
                CoreCliCommand.Status(rest[1])
            }
            "sync" -> {
                require(rest.size >= 3) { "usage: kdb sync <namespace> <peer-uri>" }
                CoreCliCommand.Sync(rest[1], rest[2])
            }
            "shell" -> {
                require(rest.size >= 2) { "usage: kdb shell <namespace>" }
                CoreCliCommand.Shell(rest[1])
            }
            else -> throw IllegalArgumentException("unknown command: ${rest[0]}")
        }
    return config to cmd
}

internal fun parseFileCommandAsCli(args: Array<String>): Pair<CliConfig, CliCommand?> {
    val (config, fileCmd) = parseFileCommand(args)
    return config to fileCmd?.let { FileCliWrapper(it) }
}
