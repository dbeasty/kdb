package dev.kdb.integration

import dev.kdb.codec.KdbHash
import dev.kdb.embed.materializeCommitHistory
import dev.kdb.embed.syncEmbedSchema
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.embed.putJson
import dev.kdb.embed.querySql
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.PeerHostConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.peersync.peerSyncHost
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.transport.ws.JvmNetworkWebSocketServer
import dev.kdb.transport.ws.defaultWebSocketWireTransport
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Browser/network embedding checks: WS framing echo + TCP peer sync with SQL query.
 */
class WebSocketPeerSyncIntegrationTest {
    private val transport = defaultWebSocketWireTransport()
    private val wire = defaultWireCodec()

    @Test
    fun wsEchoRoundTrip() =
        runBlocking {
            val server = JvmNetworkWebSocketServer()
            val serverJob =
                CoroutineScope(SupervisorJob() + Dispatchers.Default).launch {
                    server.start("127.0.0.1", 0, "/kdb") { conn ->
                        conn.incoming().collect { frame -> conn.send(frame) }
                    }
                }
            while (server.port == 0) delay(10)
            val client = transport.connect("kdb-ws://127.0.0.1:${server.port}/kdb")
            val frame =
                wire.encode(
                    WireMessage.Handshake(
                        WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                        HandshakePayload(
                            nodeId = "echo",
                            namespaces = listOf("demo"),
                            localHeads = emptyMap(),
                            clientMode = WireClientMode.FULL_PEER,
                        ),
                    ),
                )
            client.send(frame)
            val reply = waitForFrame(client)
            assertTrue(reply.contentEquals(frame))
            client.close()
            server.stop()
            serverJob.cancel()
        }

    @Test
    fun wsEchoLargeCommitPush() =
        runBlocking {
            val ns = "demo/users"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
            val serverRuntime = openMemoryRuntimeBlocking("demo", ns, schema)
            putJson(serverRuntime, ns, """{"userId":"remote1"}""", schema)
            val host = peerSyncHost(wire, serverRuntime.dag, serverRuntime.storage)
            host.start(PeerHostConfig(ns, "host", "ws"))
            val fetch =
                WireMessage.CommitFetch(
                    WireHeader(WireMessageType.COMMIT_FETCH, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                    ns,
                    KdbHash.fromHex("00".repeat(32)),
                    100,
                )
            val large = host.handleFrame(wire.encode(fetch))!!

            val server = JvmNetworkWebSocketServer()
            val serverJob =
                CoroutineScope(SupervisorJob() + Dispatchers.Default).launch {
                    server.start("127.0.0.1", 0, "/kdb") { conn ->
                        conn.incoming().collect {
                            conn.send(large)
                        }
                    }
                }
            while (server.port == 0) delay(10)
            val client = transport.connect("kdb-ws://127.0.0.1:${server.port}/kdb")
            client.send(wire.encode(fetch))
            val reply = waitForFrame(client)
            assertEquals(large.size, reply.size)
            client.close()
            server.stop()
            serverJob.cancel()
        }

    @Test
    fun wsPeerSyncCommitFetchOverNetwork() =
        runBlocking {
            val ns = "integration/ws-fetch"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
            val serverRuntime = openMemoryRuntimeBlocking("it", ns, schema)
            putJson(serverRuntime, ns, """{"userId":"u1"}""", schema)
            val host = peerSyncHost(wire, serverRuntime.dag, serverRuntime.storage)
            host.start(PeerHostConfig(ns, "host", "ws"))
            val server = JvmNetworkWebSocketServer()
            val serverJob =
                CoroutineScope(SupervisorJob() + Dispatchers.Default).launch {
                    server.start("127.0.0.1", 0, "/kdb") { conn ->
                        conn.incoming().collect { frame ->
                            val ack = host.handleFrame(frame)
                            if (ack != null) conn.send(ack)
                        }
                    }
                }
            while (server.port == 0) delay(10)
            val client = transport.connect("kdb-ws://127.0.0.1:${server.port}/kdb")
            val hs =
                WireMessage.Handshake(
                    WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                    HandshakePayload(
                        nodeId = "c1",
                        namespaces = listOf(ns),
                        localHeads = mapOf(ns to serverRuntime.dag.head().toHex()),
                        clientMode = WireClientMode.FULL_PEER,
                    ),
                )
            client.send(wire.encode(hs))
            val hsAck = waitForFrame(client)
            assertTrue(wire.decode(hsAck) is WireMessage.HandshakeAck)
            val localRuntime = openMemoryRuntimeBlocking("it", ns, schema)
            val fetch =
                WireMessage.CommitFetch(
                    WireHeader(WireMessageType.COMMIT_FETCH, KDB_WIRE_PROTOCOL_VERSION, 2, 0),
                    ns,
                    localRuntime.dag.head(),
                    100,
                )
            client.send(wire.encode(fetch))
            val pushFrame = waitForFrame(client)
            val push = wire.decode(pushFrame) as WireMessage.CommitPush
            assertTrue(push.commits.isNotEmpty())
            client.close()
            host.stop()
            server.stop()
            serverJob.cancel()
        }

    @Test
    fun wsPeerSyncPullAndQuery() =
        runBlocking {
            val ns = "integration/ws-query"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
            val wire = defaultWireCodec()
            val serverRuntime = openMemoryRuntimeBlocking("it", ns, schema)
            putJson(serverRuntime, ns, """{"userId":"u1","name":"Alice"}""", schema)
            val host = peerSyncHost(wire, serverRuntime.dag, serverRuntime.storage)
            host.start(PeerHostConfig(ns, "host", "ws"))
            val server = JvmNetworkWebSocketServer()
            val serverJob =
                CoroutineScope(SupervisorJob() + Dispatchers.Default).launch {
                    server.start("127.0.0.1", 0, "/kdb") { conn ->
                        conn.incoming().collect { frame ->
                            val ack = host.handleFrame(frame)
                            if (ack != null) conn.send(ack)
                        }
                    }
                }
            while (server.port == 0) delay(10)
            val localRuntime = openMemoryRuntimeBlocking("it", ns, schema)
            val client =
                peerSyncClient(
                    wire,
                    transport,
                    localRuntime.dag,
                    localRuntime.storage,
                )
            val session =
                client.connect(
                    PeerClientConfig(
                        namespaceId = ns,
                        nodeId = "client-1",
                        peerUri = "kdb-ws://127.0.0.1:${server.port}/kdb",
                    ),
                )
            assertEquals(1, session.pullMissing().appliedCommits)
            materializeCommitHistory(localRuntime, ns, schema)
            syncEmbedSchema(localRuntime, ns, schema)
            val rows = querySql(localRuntime, ns, "SELECT userId FROM users WHERE userId = 'u1'", schema)
            assertEquals(1, rows.rows.size)
            client.disconnect()
            host.stop()
            server.stop()
            serverJob.cancel()
        }

    private suspend fun waitForFrame(conn: dev.kdb.stream.WireConnection): ByteArray {
        repeat(2_000) {
            conn.tryPoll()?.let { return it }
            delay(2)
        }
        error("timeout waiting for wire frame")
    }
}
