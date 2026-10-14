package dev.kdb.cli

import dev.kdb.jdbc.EmbeddedKdbRuntime
import dev.kdb.jdbc.file.DataDirectoryLockLease
import dev.kdb.jdbc.file.DataDirectoryLockRegistry
import dev.kdb.jdbc.file.NamespacePaths
import dev.kdb.jdbc.file.openFileRuntime
import dev.kdb.config.KdbProductConfig
import dev.kdb.config.defaultDataDirConfigPath
import dev.kdb.config.resolveKdbProductConfig
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists

public data class CliConfig(
    val dataDir: String = "${System.getProperty("user.home")}/.kdb",
    val nodeId: String = "local",
    val quiet: Boolean = false,
    val productConfig: KdbProductConfig = KdbProductConfig.DEFAULT,
)

public class CliRuntime(
    public val namespaceId: String,
    public val embedded: EmbeddedKdbRuntime,
    private val lockLease: DataDirectoryLockLease,
) : AutoCloseable {
    override fun close() {
        // Layer 16 §6.5: flush index snapshots before releasing the directory, so the next open
        // restores them instead of rebuilding every FULLTEXT/VECTOR index by scan.
        try {
            kotlinx.coroutines.runBlocking {
                embedded.indexManager.registryFor(namespaceId).flushAll()
            }
        } catch (_: Exception) {
            // A failed flush costs a rebuild on the next open; it must not fail the command.
        }
        lockLease.close()
    }

    internal fun switchNamespace(config: CliConfig, namespaceId: String): CliRuntime {
        val metaDir = NamespacePaths.nsDir(config.dataDir, namespaceId)
        Files.createDirectories(metaDir)
        val metaFile = metaDir.resolve("meta.json")
        if (!metaFile.exists()) {
            metaFile.toFile().writeText(
                """{"namespaceId":"$namespaceId","createdAt":"${System.currentTimeMillis()}"}""",
            )
        }
        val catalog = NamespacePaths.catalogFromNamespace(namespaceId)
        return CliRuntime(
            namespaceId,
            openFileRuntime(config.dataDir, catalog, namespaceId, acquireDirectoryLock = false),
            lockLease,
        )
    }
}

public fun openCliRuntime(config: CliConfig, namespaceId: String): CliRuntime {
    val metaDir = NamespacePaths.nsDir(config.dataDir, namespaceId)
    Files.createDirectories(metaDir)
    val metaFile = metaDir.resolve("meta.json")
    if (!metaFile.exists()) {
        metaFile.toFile().writeText("""{"namespaceId":"$namespaceId","createdAt":"${System.currentTimeMillis()}"}""")
    }
    val catalog = NamespacePaths.catalogFromNamespace(namespaceId)
    val lockLease = DataDirectoryLockRegistry.acquire(config.dataDir, "kdb-cli")
    return CliRuntime(
        namespaceId,
        openFileRuntime(
            config.dataDir,
            catalog,
            namespaceId,
            acquireDirectoryLock = false,
        ),
        lockLease,
    )
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

    /** `attach put|get|list`: a document's attachments (docs/kdb-attachments-plan.md). */
    data class Attach(
        val action: String,
        val namespace: String,
        val args: List<String>,
        /** `--at <commit>` (a hash or a prefix of 7+ hex digits): read the table as of that commit. */
        val at: String? = null,
    ) : CoreCliCommand()

    data class Shell(val namespace: String) : CoreCliCommand()

    data object Unlock : CoreCliCommand()
}

internal data class FileCliWrapper(val command: FileCliCommand) : CliCommand

internal fun parseArgs(args: Array<String>): Pair<CliConfig, CliCommand?> =
    parseCoreArgs(args).let { (config, core) ->
        if (core != null) config to core else parseFileCommandAsCli(args)
    }

private fun parseCoreArgs(args: Array<String>): Pair<CliConfig, CliCommand?> {
    var dataDir: String? = null
    var quiet = false
    var configPath: String? = null
    var peerSyncEnabled: Boolean? = null
    val rest = mutableListOf<String>()
    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--data-dir" -> {
                dataDir = args.getOrNull(++i)
                i++
            }
            "--config" -> {
                configPath = args.getOrNull(++i)
                i++
            }
            "--peer-sync" -> {
                peerSyncEnabled = true
                i++
            }
            "--no-peer-sync" -> {
                peerSyncEnabled = false
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
    val resolvedDataDir = dataDir ?: CliConfig().dataDir
    val productConfig =
        resolveKdbProductConfig(
            configFile = configPath?.let { Path.of(it) },
            dataDirConfig = defaultDataDirConfigPath(resolvedDataDir),
            peerSyncEnabledOverride = peerSyncEnabled,
        )
    val config =
        CliConfig(
            dataDir = resolvedDataDir,
            quiet = quiet,
            productConfig = productConfig,
        )
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
            "attach" -> {
                val usage =
                    "usage: kdb attach put <namespace> <docId> <name> <path> [mime] | " +
                        "attach get <namespace> <docId> <name> <out-path> [--at <commit>] | " +
                        "attach list <namespace> <docId> [--at <commit>]"
                val atIx = rest.indexOf("--at")
                val at = if (atIx >= 0) requireNotNull(rest.getOrNull(atIx + 1)) { usage } else null
                val plain = if (atIx >= 0) rest.filterIndexed { i, _ -> i != atIx && i != atIx + 1 } else rest
                require(plain.size >= 4) { usage }
                val need = mapOf("put" to 6, "get" to 6, "list" to 4)[plain[1]] ?: throw IllegalArgumentException(usage)
                require(plain.size >= need) { usage }
                require(at == null || plain[1] != "put") { "attach put writes at main's head; --at is for get and list" }
                CoreCliCommand.Attach(plain[1], plain[2], plain.drop(3), at)
            }
            "shell" -> {
                require(rest.size >= 2) { "usage: kdb shell <namespace>" }
                CoreCliCommand.Shell(rest[1])
            }
            "unlock" -> {
                require(rest.size == 1) { "usage: kdb unlock (removes stale .kdb.lock when holder process has exited)" }
                CoreCliCommand.Unlock
            }
            else -> throw IllegalArgumentException("unknown command: ${rest[0]}")
        }
    return config to cmd
}

internal fun parseFileCommandAsCli(args: Array<String>): Pair<CliConfig, CliCommand?> {
    val (config, fileCmd) = parseFileCommand(args)
    return config to fileCmd?.let { FileCliWrapper(it) }
}
