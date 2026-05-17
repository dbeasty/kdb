package dev.kdb.integration

import dev.kdb.auth.ConnectionContext
import dev.kdb.embed.acceptRemoteChanges
import dev.kdb.document.KdbOp
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.embed.putJson
import dev.kdb.embed.pushCommitsSinceRemoteHead
import dev.kdb.embed.rejectRemoteChanges
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
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * Phase 3: accept / reject remote changes across two WebSocket peer clients.
 */
class WebSocketBranchIntegrationTest {
    private val wire = defaultWireCodec()

    @BeforeTest
    fun settleBefore() = runBlocking { delay(100) }

    @AfterTest
    fun settleAfter() = runBlocking { delay(100) }

    @Test
    fun wsAcceptAndRejectForkBranches() =
        runBlocking {
            val ns = "integration/ws-branch"
            val schema = KdbSchema.NONE
            val serverRuntime = openMemoryRuntimeBlocking("it", ns, schema)
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
                val docId = putJson(clientA, ns, """{"title":"from-a"}""")
                val connA = peerSyncClient(wire, transport, clientA.dag, clientA.storage)
                val sessionA = connA.connect(PeerClientConfig(ns, "a", peerUri))
                pushCommitsSinceRemoteHead(sessionA, clientA.dag, sessionA.remoteHead)
                val headAfterA = clientA.dag.head()
                connA.disconnect()

                val clientB = openMemoryRuntimeBlocking("it", ns, schema)
                val connB = peerSyncClient(wire, transport, clientB.dag, clientB.storage)
                val sessionB = connB.connect(PeerClientConfig(ns, "b", peerUri))
                sessionB.pullMissing()
                putJson(clientB, ns, """{"id":"$docId","title":"from-b"}""")
                pushCommitsSinceRemoteHead(sessionB, clientB.dag, sessionB.remoteHead)
                val remoteHeadB = clientB.dag.head()
                connB.disconnect()

                val connA2 = peerSyncClient(wire, transport, clientA.dag, clientA.storage)
                val sessionA2 = connA2.connect(PeerClientConfig(ns, "a2", peerUri))
                val reject =
                    rejectRemoteChanges(
                        clientA,
                        sessionA2,
                        ns,
                        remoteHeadB,
                        schema,
                    )
                assertEquals(headAfterA, reject.head)
                assertEquals(headAfterA, reject.commonAncestor)
                assertNotEquals(remoteHeadB, clientA.dag.head())
                putJson(clientA, ns, """{"id":"$docId","title":"from-a-fork"}""")
                val forkHead = clientA.dag.head()
                assertNotEquals(remoteHeadB, forkHead)
                pushCommitsSinceRemoteHead(sessionA2, clientA.dag, sessionA2.remoteHead)
                connA2.disconnect()

                val connA3 = peerSyncClient(wire, transport, clientA.dag, clientA.storage)
                val sessionA3 = connA3.connect(PeerClientConfig(ns, "a3", peerUri))
                val accept = acceptRemoteChanges(clientA, sessionA3, ns, remoteHeadB, schema)
                assertEquals(remoteHeadB, accept.head)
                assertTrue(clientA.dag.hasCommit(remoteHeadB))
                val remoteCommit = clientA.dag.getCommitOrThrow(remoteHeadB)
                val patch =
                    remoteCommit.operations
                        .filterIsInstance<KdbOp.Write>()
                        .single()
                        .patch
                assertTrue(patch.contains("from-b"), patch)
                connA3.disconnect()
            } finally {
                server.stop()
                serverJob.cancel()
                scope.cancel()
            }
        }
}
