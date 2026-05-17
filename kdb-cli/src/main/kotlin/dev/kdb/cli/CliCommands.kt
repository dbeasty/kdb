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
import dev.kdb.error.KdbException
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
                is CliCommand.Init -> cmdInit(config, command.namespace)
                is CliCommand.Put -> runBlocking { cmdPut(config, command) }
                is CliCommand.Get -> runBlocking { cmdGet(config, command) }
                is CliCommand.Query -> runBlocking { cmdQuery(config, command) }
                is CliCommand.Log -> runBlocking { cmdLog(config, command) }
                is CliCommand.Status -> runBlocking { cmdStatus(config, command) }
                is CliCommand.Sync -> runBlocking { cmdSync(config, command) }
                is CliCommand.Shell -> runShell(config, command.namespace)
            }
        } catch (e: IllegalArgumentException) {
            System.err.println("Error: ${e.message}")
            2
        } catch (e: KdbException) {
            System.err.println("Error: ${e.message}")
            1
        } catch (e: Exception) {
            System.err.println("Error: ${e.message}")
            1
        }
    }

    private fun cmdInit(config: CliConfig, namespace: String): Int {
        openCliRuntime(config, namespace)
        if (!config.quiet) println("Initialized namespace $namespace")
        return 0
    }

    private suspend fun cmdPut(config: CliConfig, cmd: CliCommand.Put): Int {
        val rt = openCliRuntime(config, cmd.namespace)
        executePut(CliSession(config, cmd.namespace, rt), cmd.payload)
        return 0
    }

    private suspend fun cmdGet(config: CliConfig, cmd: CliCommand.Get): Int {
        val rt = openCliRuntime(config, cmd.namespace)
        executeGet(CliSession(config, cmd.namespace, rt), cmd.docId)
        return 0
    }

    private suspend fun cmdQuery(config: CliConfig, cmd: CliCommand.Query): Int {
        val rt = openCliRuntime(config, cmd.namespace)
        executeQuery(CliSession(config, cmd.namespace, rt), cmd.sql)
        return 0
    }

    private suspend fun cmdLog(config: CliConfig, cmd: CliCommand.Log): Int {
        val rt = openCliRuntime(config, cmd.namespace)
        executeLog(CliSession(config, cmd.namespace, rt))
        return 0
    }

    private suspend fun cmdStatus(config: CliConfig, cmd: CliCommand.Status): Int {
        val rt = openCliRuntime(config, cmd.namespace)
        executeStatus(CliSession(config, cmd.namespace, rt))
        return 0
    }

    private suspend fun cmdSync(config: CliConfig, cmd: CliCommand.Sync): Int {
        val rt = openCliRuntime(config, cmd.namespace)
        executeSync(CliSession(config, cmd.namespace, rt), cmd.peerUri)
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
        session.runtime = openCliRuntime(session.config, namespaceId)
        if (!session.config.quiet) {
            println("namespace $namespaceId")
        }
    }

    private fun readPayload(payload: String): String {
        val trimmed = payload.trim()
        return if (trimmed.startsWith('{')) trimmed else Path.of(trimmed).toFile().readText()
    }
}
