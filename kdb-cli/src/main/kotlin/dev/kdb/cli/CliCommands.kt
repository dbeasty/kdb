package dev.kdb.cli

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.TraversalEntry
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.schema.KdbSchema
import dev.kdb.document.DocumentTree
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
        val runtime = openCliRuntime(config, cmd.namespace)
        val json = cmd.payload.trim().let { if (it.startsWith('{')) it else java.nio.file.Path.of(it).toFile().readText() }
        val element = Json.parseToJsonElement(json).jsonObject
        val docId =
            element["id"]?.jsonPrimitive?.content?.let { KdbUuid.fromString(it) }
                ?: KdbUuid.random()
        val doc = KdbDocument(docId, json)
        runtime.embedded.storage.putDocument(cmd.namespace, doc)
        val parent = runtime.embedded.dag.head()
        val parentTree = runtime.embedded.dag.getCommitOrThrow(parent).documentTreeHash
        val tree = runtime.embedded.storage.commitTree(cmd.namespace, parentTree)
        val tx =
            KdbTransaction(
                KdbUuid.random(),
                parent,
                listOf(KdbOp.Write(doc.id, doc.json)),
                KdbTimestamp.now(),
                KdbUuid.random(),
            )
        val commit = runtime.embedded.dag.appendCommit(tx, parent, tree, null)
        if (!config.quiet) println(commit.hash.toHex())
        return 0
    }

    private suspend fun cmdGet(config: CliConfig, cmd: CliCommand.Get): Int {
        val runtime = openCliRuntime(config, cmd.namespace)
        val id = KdbUuid.fromString(cmd.docId)
        val head = runtime.embedded.dag.head()
        val doc =
            runtime.embedded.storage.getDocument(cmd.namespace, id, head)
                ?: throw IllegalArgumentException("document not found: ${cmd.docId}")
        println(doc.json)
        return 0
    }

    private suspend fun cmdQuery(config: CliConfig, cmd: CliCommand.Query): Int {
        val runtime = openCliRuntime(config, cmd.namespace)
        val result =
            runtime.embedded.hybrid.execute(
                cmd.sql,
                HybridQueryRequest(cmd.namespace, KdbSchema.NONE),
            )
        for (row in result.result.rows) {
            println(row.values.joinToString("\t") { it?.toString() ?: "null" })
        }
        return 0
    }

    private suspend fun cmdLog(config: CliConfig, cmd: CliCommand.Log): Int {
        val runtime = openCliRuntime(config, cmd.namespace)
        val walked = runtime.embedded.dag.walk(runtime.embedded.dag.head())
        for (entry in walked.filterIsInstance<TraversalEntry.Full>()) {
            println("${entry.commit.hash.toHex()}\t${entry.commit.message}")
        }
        return 0
    }

    private suspend fun cmdStatus(config: CliConfig, cmd: CliCommand.Status): Int {
        val runtime = openCliRuntime(config, cmd.namespace)
        val head: KdbHash = runtime.embedded.dag.head()
        println("HEAD ${head.toHex()}")
        println("namespace ${cmd.namespace}")
        return 0
    }

    private suspend fun cmdSync(config: CliConfig, cmd: CliCommand.Sync): Int {
        val runtime = openCliRuntime(config, cmd.namespace)
        val wire = defaultWireCodec()
        val transport =
            when {
                cmd.peerUri.startsWith("memory://") ->
                    dev.kdb.stream.InMemoryWireTransport()
                cmd.peerUri.startsWith("kdb-tcp://") || cmd.peerUri.startsWith("tcp://") ->
                    defaultTcpWireTransport()
                else -> throw IllegalArgumentException("unsupported peer URI: ${cmd.peerUri}")
            }
        val client = peerSyncClient(wire, transport, runtime.embedded.dag, runtime.embedded.storage)
        val session =
            client.connect(
                PeerClientConfig(cmd.namespace, config.nodeId, cmd.peerUri),
            )
        val result = session.syncBidirectional()
        if (!config.quiet) {
            println("applied=${result.appliedCommits} pushed=${result.pushedCommits} head=${result.finalHead.toHex()}")
        }
        client.disconnect()
        return 0
    }
}
