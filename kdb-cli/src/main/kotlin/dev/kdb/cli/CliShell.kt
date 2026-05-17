package dev.kdb.cli

import dev.kdb.error.DataDirectoryLockedException
import dev.kdb.error.KdbException
import kotlinx.coroutines.runBlocking

public fun interface LineReader {
    public fun readLine(): String?
}

public object SystemLineReader : LineReader {
    override fun readLine(): String? = kotlin.io.readlnOrNull()
}

public class ListLineReader(
    private val lines: List<String>,
) : LineReader {
    private var index = 0

    override fun readLine(): String? {
        if (index >= lines.size) return null
        return lines[index++]
    }
}

public class CliSession(
    public val config: CliConfig,
    public var namespaceId: String,
    public var runtime: CliRuntime,
)

internal fun runShell(
    config: CliConfig,
    namespaceId: String,
    input: LineReader = SystemLineReader,
): Int {
    return try {
        openCliRuntime(config, namespaceId).use { runtime ->
            val session = CliSession(config, namespaceId, runtime)
            if (!config.quiet) {
                println("KDB shell — data-dir=${config.dataDir} namespace=$namespaceId (type help)")
            }
            while (true) {
                if (!config.quiet) {
                    print("kdb:${session.namespaceId}> ")
                }
                val line = input.readLine() ?: break
                when (val result = runBlocking { executeShellLine(session, line) }) {
                    ShellLineResult.Continue -> {}
                    ShellLineResult.Exit -> return 0
                    is ShellLineResult.Error -> {
                        System.err.println("Error: ${result.message}")
                    }
                }
            }
            0
        }
    } catch (e: DataDirectoryLockedException) {
        System.err.println("Error: ${e.message}")
        1
    } catch (e: IllegalArgumentException) {
        System.err.println("Error: ${e.message}")
        2
    } catch (e: Exception) {
        System.err.println("Error: ${e.message}")
        1
    }
}

internal sealed class ShellLineResult {
    data object Continue : ShellLineResult()

    data object Exit : ShellLineResult()

    data class Error(val message: String) : ShellLineResult()
}

internal suspend fun executeShellLine(session: CliSession, line: String): ShellLineResult {
    val trimmed = line.trim()
    if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        return ShellLineResult.Continue
    }
    val space = trimmed.indexOf(' ')
    val verb = if (space < 0) trimmed else trimmed.substring(0, space)
    val rest = if (space < 0) "" else trimmed.substring(space + 1).trim()
    return try {
        when (verb.lowercase()) {
            "exit", "quit" -> ShellLineResult.Exit
            "help", "?" -> {
                printShellHelp()
                ShellLineResult.Continue
            }
            "put" -> {
                require(rest.isNotEmpty()) { "usage: put <file|json>" }
                CliCommands.executePut(session, rest)
                ShellLineResult.Continue
            }
            "get" -> {
                require(rest.isNotEmpty()) { "usage: get <docId>" }
                CliCommands.executeGet(session, rest)
                ShellLineResult.Continue
            }
            "query" -> {
                require(rest.isNotEmpty()) { "usage: query <sql>" }
                CliCommands.executeQuery(session, rest)
                ShellLineResult.Continue
            }
            "log" -> {
                CliCommands.executeLog(session)
                ShellLineResult.Continue
            }
            "status" -> {
                CliCommands.executeStatus(session)
                ShellLineResult.Continue
            }
            "sync" -> {
                require(rest.isNotEmpty()) { "usage: sync <peer-uri>" }
                CliCommands.executeSync(session, rest)
                ShellLineResult.Continue
            }
            "use" -> {
                require(rest.isNotEmpty()) { "usage: use <namespace>" }
                CliCommands.executeUse(session, rest)
                ShellLineResult.Continue
            }
            "file" -> {
                val fileArgs = arrayOf("file") + rest.split(Regex("\\s+")).filter { it.isNotEmpty() }
                if (fileArgs.size < 3) {
                    return ShellLineResult.Error("usage: file <put|get|meta> ... (see help)")
                }
                val sub = fileArgs[1]
                val ns = session.namespaceId
                val opts = fileArgs.drop(2)
                val cmd = parseFileSubcommand(sub, ns, opts)
                when (cmd) {
                    is FileCliCommand.Put -> FileCli.executePut(session, cmd)
                    is FileCliCommand.Get -> FileCli.executeGet(session, cmd)
                    is FileCliCommand.Meta -> FileCli.executeMeta(session, cmd)
                }
                ShellLineResult.Continue
            }
            else -> ShellLineResult.Error("unknown command: $verb (type help)")
        }
    } catch (e: IllegalArgumentException) {
        ShellLineResult.Error(e.message ?: "invalid argument")
    } catch (e: KdbException) {
        ShellLineResult.Error(e.message ?: "engine error")
    } catch (e: Exception) {
        ShellLineResult.Error(e.message ?: e.javaClass.simpleName)
    }
}

private fun printShellHelp() {
    println(
        """
        Shell commands (namespace is ${"fixed at start; use 'use' to switch"}):
          put <file|json>     write document + commit
          file put <path> [--id UUID] [--zip]   store opaque file (metadata + blob)
          file get --id <UUID> [-o path]      fetch file bytes
          file meta --id <UUID>               print kdb.file JSON metadata
          get <docId>         print document JSON
          query <sql>         run SELECT (single line)
          log                 commit history
          status              HEAD + namespace
          sync <peer-uri>     bidirectional peer sync
          use <namespace>     switch namespace
          help, ?             this help
          exit, quit          leave shell
        """.trimIndent(),
    )
}
