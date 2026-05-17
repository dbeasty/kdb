package dev.kdb.cli

import dev.kdb.codec.KdbUuid
import dev.kdb.file.FileAttachmentExtract
import dev.kdb.file.FileAttachmentIngest
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists

internal sealed class FileCliCommand {
    abstract val namespace: String

    data class Put(
        override val namespace: String,
        val paths: List<Path>,
        val fileId: KdbUuid?,
        val bundleId: KdbUuid?,
        val zip: Boolean?,
        val namespacePath: String?,
        val message: String,
    ) : FileCliCommand()

    data class Get(
        override val namespace: String,
        val fileId: KdbUuid?,
        val bundleId: KdbUuid?,
        val memberFileId: KdbUuid?,
        val output: Path?,
    ) : FileCliCommand()

    data class Meta(
        override val namespace: String,
        val fileId: KdbUuid?,
        val bundleId: KdbUuid?,
    ) : FileCliCommand()
}

private fun parseFileCommandInternal(args: Array<String>): Pair<CliConfig, FileCliCommand?> {
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
    if (rest.isEmpty() || rest[0] != "file") return config to null
    if (rest.size < 3) {
        throw IllegalArgumentException("usage: kdb file <put|get|meta> <namespace> ...")
    }
    val sub = rest[1]
    val ns = rest[2]
    val opts = rest.drop(3)
    return config to parseFileSubcommand(sub, ns, opts)
}

internal fun parseFileCommand(args: Array<String>): Pair<CliConfig, FileCliCommand?> =
    parseFileCommandInternal(args)

internal fun parseFileSubcommand(sub: String, namespace: String, opts: List<String>): FileCliCommand {
    var fileId: KdbUuid? = null
    var bundleId: KdbUuid? = null
    var memberId: KdbUuid? = null
    var zip: Boolean? = null
    var path: String? = null
    var message = ""
    var output: Path? = null
    val positional = mutableListOf<String>()
    var i = 0
    while (i < opts.size) {
        when (opts[i]) {
            "--id" -> {
                fileId = KdbUuid.fromString(requireOpt(opts, i + 1, "--id"))
                i += 2
            }
            "--bundle" -> {
                bundleId = KdbUuid.fromString(requireOpt(opts, i + 1, "--bundle"))
                i += 2
            }
            "--member" -> {
                memberId = KdbUuid.fromString(requireOpt(opts, i + 1, "--member"))
                i += 2
            }
            "--zip" -> {
                zip = true
                i++
            }
            "--no-zip" -> {
                zip = false
                i++
            }
            "--path" -> {
                path = requireOpt(opts, i + 1, "--path")
                i += 2
            }
            "-m", "--message" -> {
                message = requireOpt(opts, i + 1, "-m")
                i += 2
            }
            "-o", "--output" -> {
                output = Path.of(requireOpt(opts, i + 1, "-o"))
                i += 2
            }
            else -> {
                positional += opts[i]
                i++
            }
        }
    }
    return when (sub) {
        "put" -> {
            require(positional.isNotEmpty()) { "usage: kdb file put <namespace> [--id UUID] [--bundle UUID] [--zip] <files...>" }
            val paths = positional.map { Path.of(it) }
            for (p in paths) {
                require(p.exists()) { "file not found: $p" }
            }
            val useZip = zip ?: (bundleId != null && paths.size > 1)
            FileCliCommand.Put(namespace, paths, fileId, bundleId, useZip, path, message)
        }
        "get" -> {
            require(fileId != null || bundleId != null) {
                "usage: kdb file get <namespace> --id <fileId> | --bundle <bundleId> [--member <fileId>] [-o path]"
            }
            FileCliCommand.Get(namespace, fileId, bundleId, memberId, output)
        }
        "meta" -> {
            require(fileId != null || bundleId != null) {
                "usage: kdb file meta <namespace> --id <fileId> | --bundle <bundleId>"
            }
            FileCliCommand.Meta(namespace, fileId, bundleId)
        }
        else -> throw IllegalArgumentException("unknown file subcommand: $sub")
    }
}

private fun requireOpt(opts: List<String>, index: Int, flag: String): String {
    if (index >= opts.size) throw IllegalArgumentException("$flag requires a value")
    return opts[index]
}

internal object FileCli {
    suspend fun executePut(session: CliSession, cmd: FileCliCommand.Put) {
        val rt = session.runtime.embedded
        val result =
            if (cmd.bundleId != null || cmd.paths.size > 1) {
                val bundle =
                    FileAttachmentIngest.ingestBundleFromPaths(
                        session.namespaceId,
                        rt.dag,
                        rt.storage,
                        cmd.paths,
                        bundleId = cmd.bundleId ?: KdbUuid.random(),
                        zip = cmd.zip ?: true,
                        message = cmd.message,
                    )
                if (!session.config.quiet) {
                    println(
                        """{"bundleId":"${bundle.bundleId}","blobHash":"${bundle.blobHash.toHex()}","memberFileIds":${bundle.memberFileIds.map { "\"$it\"" }}}""",
                    )
                }
                bundle.commit.hash
            } else {
                val path = cmd.paths.single()
                val ingested =
                    FileAttachmentIngest.ingestSingleFromPath(
                        session.namespaceId,
                        rt.dag,
                        rt.storage,
                        path,
                        fileId = cmd.fileId ?: KdbUuid.random(),
                        namespacePath = cmd.namespacePath,
                        zip = cmd.zip ?: false,
                        message = cmd.message,
                    )
                if (!session.config.quiet) {
                    println(
                        """{"fileId":"${ingested.fileId}","blobHash":"${ingested.blobHash.toHex()}"}""",
                    )
                }
                ingested.commit.hash
            }
        if (!session.config.quiet && cmd.message.isEmpty()) {
            println(result.toHex())
        }
    }

    suspend fun executeGet(session: CliSession, cmd: FileCliCommand.Get) {
        val rt = session.runtime.embedded
        val bytes =
            when {
                cmd.bundleId != null && cmd.memberFileId != null ->
                    FileAttachmentExtract.readBundleMemberBytes(
                        session.namespaceId,
                        rt.dag,
                        rt.storage,
                        cmd.bundleId,
                        cmd.memberFileId,
                    )
                cmd.bundleId != null ->
                    FileAttachmentExtract.readBundleArchiveBytes(
                        session.namespaceId,
                        rt.dag,
                        rt.storage,
                        cmd.bundleId,
                    )
                cmd.fileId != null ->
                    FileAttachmentExtract.readFileBytes(
                        session.namespaceId,
                        rt.dag,
                        rt.storage,
                        cmd.fileId,
                    )
                else -> error("fileId or bundleId required")
            }
        if (cmd.output != null) {
            Files.write(cmd.output, bytes)
            if (!session.config.quiet) {
                println("wrote ${bytes.size} bytes to $cmd.output")
            }
        } else {
            System.out.write(bytes)
        }
    }

    suspend fun executeMeta(session: CliSession, cmd: FileCliCommand.Meta) {
        val rt = session.runtime.embedded
        val id = cmd.fileId ?: cmd.bundleId!!
        val doc =
            FileAttachmentExtract.metadataAtHead(
                session.namespaceId,
                rt.dag,
                rt.storage,
                id,
            )
        println(doc.json)
    }
}
