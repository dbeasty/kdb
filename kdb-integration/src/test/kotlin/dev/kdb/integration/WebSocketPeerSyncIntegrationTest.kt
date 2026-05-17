package dev.kdb.integration

import dev.kdb.auth.ConnectionContext
import dev.kdb.codec.KdbHash
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.getJson
import dev.kdb.embed.materializeCommitHistory
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.embed.pushCommitsSinceRemoteHead
import dev.kdb.embed.putJson
import dev.kdb.embed.querySql
import dev.kdb.embed.syncEmbeddedWithPeer
import dev.kdb.embed.syncEmbedSchema
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.PeerHostConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.peersync.PeerSyncHost
import dev.kdb.peersync.peerSyncHostFactory
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
import org.junit.After
import org.junit.Before
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Browser/network embedding checks: WS framing echo + TCP peer sync with SQL query.
 */
class WebSocketPeerSyncIntegrationTest {
    private val wire = defaultWireCodec()

    private fun transport() = defaultWebSocketWireTransport()

    @Before
    fun warmWebSocketTransport() {
        runBlocking { delay(50) }
    }

    @After
    fun settleWebSocketTransport() {
        runBlocking { delay(150) }
    }

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
            val client = transport().connect("kdb-ws://127.0.0.1:${server.port}/kdb")
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
            val host =
                peerSyncHostFactory(
                    wire,
                    serverRuntime.dag,
                    serverRuntime.storage,
                    PeerHostConfig(ns, "host", "ws"),
                )(ConnectionContext.EMPTY)
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
            val client = transport().connect("kdb-ws://127.0.0.1:${server.port}/kdb")
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
            val serverRuntime = openMemoryRuntimeBlocking("it-fetch", ns, schema)
            putJson(serverRuntime, ns, """{"userId":"u1"}""", schema)
            withNetworkPeerHost(serverRuntime, ns) { _, port ->
                val localRuntime = openMemoryRuntimeBlocking("it-fetch", ns, schema)
                val client =
                    peerSyncClient(wire, transport(), localRuntime.dag, localRuntime.storage)
                val session =
                    client.connect(
                        PeerClientConfig(
                            namespaceId = ns,
                            nodeId = "c1",
                            peerUri = "kdb-ws://127.0.0.1:$port/kdb",
                        ),
                    )
                assertEquals(1, session.pullMissing().appliedCommits)
                client.disconnect()
            }
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
            val serverRuntime = openMemoryRuntimeBlocking("it", ns, schema)
            putJson(serverRuntime, ns, """{"userId":"u1","name":"Alice"}""", schema)
            withNetworkPeerHost(serverRuntime, ns) { _, port ->
                val localRuntime = openMemoryRuntimeBlocking("it", ns, schema)
                val client =
                    peerSyncClient(
                        wire,
                        transport(),
                        localRuntime.dag,
                        localRuntime.storage,
                    )
                val session =
                    client.connect(
                        PeerClientConfig(
                            namespaceId = ns,
                            nodeId = "client-1",
                            peerUri = "kdb-ws://127.0.0.1:$port/kdb",
                        ),
                    )
                assertEquals(1, session.pullMissing().appliedCommits)
                materializeCommitHistory(localRuntime, ns, schema)
                syncEmbedSchema(localRuntime, ns, schema)
                val rows = querySql(localRuntime, ns, "SELECT userId FROM users WHERE userId = 'u1'", schema)
                assertEquals(1, rows.rows.size)
                client.disconnect()
            }
        }

    @Test
    fun wsPeerSyncPushPullTwoClients() =
        runBlocking {
            val ns = "integration/ws-push-pull"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
            val serverRuntime = openMemoryRuntimeBlocking("it", ns, schema)
            withNetworkPeerHost(serverRuntime, ns) { _, port ->
                val peerUri = "kdb-ws://127.0.0.1:$port/kdb"
                val transport = transport()

                val clientA = openMemoryRuntimeBlocking("it", ns, schema)
                val docId = putJson(clientA, ns, """{"userId":"push-a","name":"Alice"}""", schema)
                val clientAConn =
                    peerSyncClient(wire, transport, clientA.dag, clientA.storage)
                val sessionA =
                    clientAConn.connect(PeerClientConfig(ns, "client-a", peerUri))
                assertEquals(1, pushCommitsSinceRemoteHead(sessionA, clientA.dag, sessionA.remoteHead).pushedCommits)
                clientAConn.disconnect()
                delay(150)

                val clientB = openMemoryRuntimeBlocking("it", ns, schema)
                val clientBConn =
                    peerSyncClient(wire, transport, clientB.dag, clientB.storage)
                val sessionB =
                    clientBConn.connect(PeerClientConfig(ns, "client-b", peerUri))
                assertEquals(1, syncEmbeddedWithPeer(clientB, sessionB, ns, schema).appliedCommits)
                val docJson = getJson(clientB, ns, docId)
                assertTrue(docJson.contains("push-a"), docJson)
                clientBConn.disconnect()
            }
        }

    @Test
    fun wsPeerSyncBidirectionalAfterPush() =
        runBlocking {
            val ns = "integration/ws-bidir"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
            val serverRuntime = openMemoryRuntimeBlocking("it", ns, schema)
            withNetworkPeerHost(serverRuntime, ns) { _, port ->
                val peerUri = "kdb-ws://127.0.0.1:$port/kdb"

                val clientA = openMemoryRuntimeBlocking("it", ns, schema)
                putJson(clientA, ns, """{"userId":"a1","name":"Alice"}""", schema)
                val clientAConn =
                    peerSyncClient(wire, transport(), clientA.dag, clientA.storage)
                val sessionA1 =
                    clientAConn.connect(PeerClientConfig(ns, "client-a", peerUri))
                pushCommitsSinceRemoteHead(sessionA1, clientA.dag, sessionA1.remoteHead)
                clientAConn.disconnect()

                val clientB = openMemoryRuntimeBlocking("it", ns, schema)
                val clientBConn =
                    peerSyncClient(wire, transport(), clientB.dag, clientB.storage)
                val sessionB =
                    clientBConn.connect(PeerClientConfig(ns, "client-b", peerUri))
                val pullB = sessionB.pullMissing()
                materializeCommitHistory(clientB, ns, schema)
                putJson(clientB, ns, """{"userId":"b1","name":"Bob"}""", schema)
                pushCommitsSinceRemoteHead(sessionB, clientB.dag, pullB.finalHead)
                clientBConn.disconnect()
                delay(100)

                val clientAConn2 =
                    peerSyncClient(wire, transport(), clientA.dag, clientA.storage)
                val sessionA2 =
                    clientAConn2.connect(PeerClientConfig(ns, "client-a-2", peerUri))
                val syncA = syncEmbeddedWithPeer(clientA, sessionA2, ns, schema)
                assertTrue(syncA.appliedCommits >= 1, "expected B's commit on sync, plan=${syncA.plan}")
                clientAConn2.disconnect()
                val rows =
                    querySql(clientA, ns, "SELECT userId FROM users WHERE userId = 'b1'", schema)
                assertEquals(1, rows.rows.size)
            }
        }

    private suspend fun waitForFrame(conn: dev.kdb.stream.WireConnection): ByteArray {
        repeat(2_000) {
            conn.tryPoll()?.let { return it }
            delay(2)
        }
        error("timeout waiting for wire frame")
    }

    private suspend fun <R> withNetworkPeerHost(
        runtime: EmbeddedKdbRuntime,
        namespaceId: String,
        block: suspend (host: PeerSyncHost, port: Int) -> R,
    ): R =
        withNetworkPeerHost(
            peerSyncHostFactory(
                wire,
                runtime.dag,
                runtime.storage,
                PeerHostConfig(namespaceId, "host", "ws"),
            )(ConnectionContext.EMPTY),
            block,
        )

    private suspend fun <R> withNetworkPeerHost(
        host: PeerSyncHost,
        block: suspend (host: PeerSyncHost, port: Int) -> R,
    ): R {
        val server = JvmNetworkWebSocketServer()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val serverJob =
            scope.launch {
                server.start("127.0.0.1", 0, "/kdb") { conn ->
                    conn.incoming().collect { frame ->
                        val ack = host.handleFrame(frame)
                        if (ack != null) conn.send(ack)
                    }
                }
            }
        return try {
            while (server.port == 0) delay(10)
            delay(100)
            block(host, server.port)
        } finally {
            server.stop()
            serverJob.cancel()
            scope.cancel()
        }
    }

}
