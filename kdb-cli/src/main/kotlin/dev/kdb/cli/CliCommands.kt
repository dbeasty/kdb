package dev.kdb.cli

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.TraversalEntry
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.schema.KdbSchema
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.DataDirectoryLockedException
import dev.kdb.error.KdbException
import dev.kdb.jdbc.file.StaleLockReleaseResult
import dev.kdb.jdbc.file.releaseStaleDataDirectoryLock
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.transport.tcp.defaultTcpWireTransport
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Path

internal object CliCommands {
    fun execute(config: CliConfig, command: CliCommand): Int {
        return try {
            when (command) {
                is CoreCliCommand.Init -> cmdInit(config, command.namespace)
                is CoreCliCommand.Put -> runBlocking { cmdPut(config, command) }
                is CoreCliCommand.Get -> runBlocking { cmdGet(config, command) }
                is CoreCliCommand.Query -> runBlocking { cmdQuery(config, command) }
                is CoreCliCommand.Log -> runBlocking { cmdLog(config, command) }
                is CoreCliCommand.Status -> runBlocking { cmdStatus(config, command) }
                is CoreCliCommand.Sync -> runBlocking { cmdSync(config, command) }
                is CoreCliCommand.Shell -> runShell(config, command.namespace)
                is CoreCliCommand.Unlock -> cmdUnlock(config)
                is FileCliWrapper -> runBlocking { cmdFile(config, command.command) }
            }
        } catch (e: IllegalArgumentException) {
            System.err.println("Error: ${e.message}")
            2
        } catch (e: DataDirectoryLockedException) {
            System.err.println("Error: ${e.message}")
            1
        } catch (e: KdbException) {
            System.err.println("Error: ${e.message}")
            1
        } catch (e: Exception) {
            System.err.println("Error: ${e.message}")
            1
        }
    }

    private fun cmdInit(config: CliConfig, namespace: String): Int {
        openCliRuntime(config, namespace).use {
            if (!config.quiet) println("Initialized namespace $namespace")
        }
        return 0
    }

    private fun cmdUnlock(config: CliConfig): Int {
        when (val result = releaseStaleDataDirectoryLock(config.dataDir)) {
            StaleLockReleaseResult.NoLockFile -> {
                if (!config.quiet) println("No lock file at ${config.dataDir}")
            }
            is StaleLockReleaseResult.Removed -> {
                val prev = result.previous
                if (!config.quiet) {
                    if (prev != null) {
                        println(
                            "Removed stale lock (pid ${prev.pid}, ${prev.holder} on ${prev.host}, acquired ${prev.acquiredAt})",
                        )
                    } else {
                        println("Removed stale lock file")
                    }
                }
            }
            is StaleLockReleaseResult.StillHeld -> {
                val info = result.info
                throw DataDirectoryLockedException(
                    "database workspace is still open (pid ${info.pid}, ${info.holder} on ${info.host}); stop that process before unlock",
                    dataRoot = config.dataDir,
                    holderPid = info.pid,
                    holderLabel = info.holder,
                )
            }
        }
        return 0
    }

    private suspend fun cmdFile(config: CliConfig, cmd: FileCliCommand): Int {
        openCliRuntime(config, cmd.namespace).use { rt ->
            val session = CliSession(config, cmd.namespace, rt)
            when (cmd) {
                is FileCliCommand.Put -> FileCli.executePut(session, cmd)
                is FileCliCommand.Get -> FileCli.executeGet(session, cmd)
                is FileCliCommand.Meta -> FileCli.executeMeta(session, cmd)
            }
        }
        return 0
    }

    private suspend fun cmdPut(config: CliConfig, cmd: CoreCliCommand.Put): Int {
        openCliRuntime(config, cmd.namespace).use { rt ->
            executePut(CliSession(config, cmd.namespace, rt), cmd.payload)
        }
        return 0
    }

    private suspend fun cmdGet(config: CliConfig, cmd: CoreCliCommand.Get): Int {
        openCliRuntime(config, cmd.namespace).use { rt ->
            executeGet(CliSession(config, cmd.namespace, rt), cmd.docId)
        }
        return 0
    }

    private suspend fun cmdQuery(config: CliConfig, cmd: CoreCliCommand.Query): Int {
        openCliRuntime(config, cmd.namespace).use { rt ->
            executeQuery(CliSession(config, cmd.namespace, rt), cmd.sql)
        }
        return 0
    }

    private suspend fun cmdLog(config: CliConfig, cmd: CoreCliCommand.Log): Int {
        openCliRuntime(config, cmd.namespace).use { rt ->
            executeLog(CliSession(config, cmd.namespace, rt))
        }
        return 0
    }

    private suspend fun cmdStatus(config: CliConfig, cmd: CoreCliCommand.Status): Int {
        openCliRuntime(config, cmd.namespace).use { rt ->
            executeStatus(CliSession(config, cmd.namespace, rt))
        }
        return 0
    }

    private suspend fun cmdSync(config: CliConfig, cmd: CoreCliCommand.Sync): Int {
        openCliRuntime(config, cmd.namespace).use { rt ->
            executeSync(CliSession(config, cmd.namespace, rt), cmd.peerUri)
        }
        return 0
    }

    internal suspend fun executePut(session: CliSession, payload: String) {
        val json = readPayload(payload)
        val element = Json.parseToJsonElement(json).jsonObject
        val docId =
            element["id"]?.jsonPrimitive?.content?.let { KdbUuid.fromString(it) }
                ?: KdbUuid.random()
        val doc = KdbDocument(docId, json)
        val ns = session.namespaceId
        session.runtime.embedded.storage.putDocument(ns, doc)
        val parent = session.runtime.embedded.dag.head()
        val parentTree = session.runtime.embedded.dag.getCommitOrThrow(parent).documentTreeHash
        val tree = session.runtime.embedded.storage.commitTree(ns, parentTree)
        val tx =
            KdbTransaction(
                KdbUuid.random(),
                parent,
                listOf(KdbOp.Write(doc.id, doc.json)),
                KdbTimestamp.now(),
                KdbUuid.random(),
            )
        val commit = session.runtime.embedded.dag.appendCommit(tx, parent, tree, null)
        if (!session.config.quiet) println(commit.hash.toHex())
    }

    internal suspend fun executeGet(session: CliSession, docId: String) {
        val id = KdbUuid.fromString(docId)
        val head = session.runtime.embedded.dag.head()
        val doc =
            session.runtime.embedded.storage.getDocument(session.namespaceId, id, head)
                ?: throw IllegalArgumentException("document not found: $docId")
        println(doc.json)
    }

    internal suspend fun executeQuery(session: CliSession, sql: String) {
        val result =
            session.runtime.embedded.hybrid.execute(
                sql,
                HybridQueryRequest(session.namespaceId, KdbSchema.NONE),
            )
        for (row in result.result.rows) {
            println(row.values.joinToString("\t") { it?.toString() ?: "null" })
        }
    }

    internal suspend fun executeLog(session: CliSession) {
        val walked = session.runtime.embedded.dag.walk(session.runtime.embedded.dag.head())
        for (entry in walked.filterIsInstance<TraversalEntry.Full>()) {
            println("${entry.commit.hash.toHex()}\t${entry.commit.message}")
        }
    }

    internal suspend fun executeStatus(session: CliSession) {
        val head: KdbHash = session.runtime.embedded.dag.head()
        println("HEAD ${head.toHex()}")
        println("namespace ${session.namespaceId}")
    }

    internal suspend fun executeSync(session: CliSession, peerUri: String) {
        val wire = defaultWireCodec()
        val transport =
            when {
                peerUri.startsWith("memory://") ->
                    dev.kdb.stream.InMemoryWireTransport()
                peerUri.startsWith("kdb-tcp://") || peerUri.startsWith("tcp://") ->
                    defaultTcpWireTransport()
                else -> throw IllegalArgumentException("unsupported peer URI: $peerUri")
            }
        val client =
            peerSyncClient(
                wire,
                transport,
                session.runtime.embedded.dag,
                session.runtime.embedded.storage,
            )
        val peerSession =
            client.connect(
                PeerClientConfig(session.namespaceId, session.config.nodeId, peerUri),
            )
        val result = peerSession.syncBidirectional()
        if (!session.config.quiet) {
            println(
                "applied=${result.appliedCommits} pushed=${result.pushedCommits} head=${result.finalHead.toHex()}",
            )
        }
        client.disconnect()
    }

    internal fun executeUse(session: CliSession, namespaceId: String) {
        session.namespaceId = namespaceId
        session.runtime = session.runtime.switchNamespace(session.config, namespaceId)
        if (!session.config.quiet) {
            println("namespace $namespaceId")
        }
    }

    private fun readPayload(payload: String): String {
        val trimmed = payload.trim()
        return if (trimmed.startsWith('{')) trimmed else Path.of(trimmed).toFile().readText()
    }
}
