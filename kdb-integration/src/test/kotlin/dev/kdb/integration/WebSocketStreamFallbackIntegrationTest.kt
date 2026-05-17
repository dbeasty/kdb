package dev.kdb.integration

import dev.kdb.auth.ConnectionContext
import dev.kdb.embed.getJson
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.embed.pushCommitsSinceRemoteHead
import dev.kdb.embed.putJson
import dev.kdb.embed.recoverInboundViaPeerSync
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.PeerHostConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.peersync.peerSyncHostFactory
import dev.kdb.schema.KdbSchema
import dev.kdb.transport.ws.JvmNetworkWebSocketServer
import dev.kdb.transport.ws.defaultWebSocketWireTransport
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Phase 2b: when stream subscribe is unavailable, peer-sync recovery still delivers commits.
 */
class WebSocketStreamFallbackIntegrationTest {
    private val wire = defaultWireCodec()

    @BeforeTest
    fun settleBefore() = runBlocking { delay(100) }

    @AfterTest
    fun settleAfter() = runBlocking { delay(100) }

    @Test
    fun wsPeerSyncRecoveryWhenStreamUnavailable() =
        runBlocking {
            val ns = "integration/ws-fallback"
            val schema = KdbSchema.NONE
            val serverRuntime = openMemoryRuntimeBlocking("it", ns, schema)
            val docId = putJson(serverRuntime, ns, """{"userId":"fallback-u1"}""", schema)
            val peerHost =
                peerSyncHostFactory(
                    wire,
                    serverRuntime.dag,
                    serverRuntime.storage,
                    PeerHostConfig(ns, "host", "ws"),
                )(ConnectionContext.EMPTY)
            val server = JvmNetworkWebSocketServer()
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val serverJob =
                scope.launch {
                    server.start("127.0.0.1", 0, "/kdb") { conn ->
                        conn.incoming().collect { frame ->
                            val ack = peerHost.handleFrame(frame)
                            if (ack != null) conn.send(ack)
                        }
                    }
                }
            try {
                while (server.port == 0) delay(10)
                delay(100)
                val peerUri = "kdb-ws://127.0.0.1:${server.port}/kdb"
                val transport = defaultWebSocketWireTransport()

                val clientA = openMemoryRuntimeBlocking("it", ns, schema)
                putJson(clientA, ns, """{"userId":"fallback-u1"}""", schema)
                val clientAConn =
                    peerSyncClient(wire, transport, clientA.dag, clientA.storage)
                val sessionA =
                    clientAConn.connect(PeerClientConfig(ns, "client-a", peerUri))
                pushCommitsSinceRemoteHead(sessionA, clientA.dag, sessionA.remoteHead)
                clientAConn.disconnect()
                delay(100)

                val clientB = openMemoryRuntimeBlocking("it", ns, schema)
                val clientBConn =
                    peerSyncClient(wire, transport, clientB.dag, clientB.storage)
                val sessionB =
                    clientBConn.connect(PeerClientConfig(ns, "client-b", peerUri))
                val recovery = recoverInboundViaPeerSync(clientB, sessionB, ns, schema)
                assertTrue(recovery.appliedCommits >= 1, "expected peer sync recovery")
                val json = getJson(clientB, ns, docId)
                assertTrue(json.contains("fallback-u1"), json)
                clientBConn.disconnect()
            } finally {
                server.stop()
                serverJob.cancel()
                scope.cancel()
            }
        }
}
