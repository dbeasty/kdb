package dev.kdb.integration

import dev.kdb.auth.ConnectionContext
import dev.kdb.codec.KdbHash
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.getJson
import dev.kdb.embed.handleStreamConnection
import dev.kdb.embed.materializeCommitHistory
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.embed.pushCommitsSinceRemoteHead
import dev.kdb.embed.putJson
import dev.kdb.embed.querySql
import dev.kdb.embed.syncEmbedSchema
import dev.kdb.embed.syncEmbeddedWithPeer
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.PeerHostConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.peersync.peerSyncHostFactory
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.stream.StreamBroadcastHub
import dev.kdb.stream.StreamClientMode
import dev.kdb.stream.StreamEvent
import dev.kdb.stream.StreamSubscriberConfig
import dev.kdb.stream.publishedCommitFrom
import dev.kdb.stream.streamSubscriber
import dev.kdb.transport.ws.JvmNetworkWebSocketServer
import dev.kdb.transport.ws.defaultWebSocketWireTransport
import dev.kdb.wire.defaultWireCodec
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class WebSocketStreamIntegrationTest {
    private val wire = defaultWireCodec()

    /**
     * Owns every coroutine these tests launch outside the test body, so teardown can wait for them
     * to actually finish.
     *
     * They used to be launched into throwaway `CoroutineScope(...)` values and stopped with a bare
     * `cancel()`. Cancellation is asynchronous, so that only *requests* a stop - the coroutine can
     * still be unwinding after the test method has returned, and an exception it throws on the way
     * out (a socket closing under a `collect`, most often) has nowhere to go but the next test's
     * uncaught-exception handler. That surfaced in CI as `UncaughtExceptionsBeforeTest` failing a
     * completely unrelated test, because the victim is whichever test happens to run next.
     *
     * JUnit builds a fresh instance per test method, so one scope per instance is one scope per
     * test.
     */
    private val testScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @BeforeTest
    fun settleBefore() = runBlocking { delay(100) }

    @AfterTest
    fun stopLaunchedCoroutines() =
        runBlocking {
            // cancelAndJoin, not cancel: waiting is the entire point. This replaces a fixed
            // delay(100) that hoped the strays had settled, which held on an idle machine and
            // stopped holding on a loaded CI runner.
            testScope.coroutineContext.job.cancelAndJoin()
        }

    @Test
    fun wsStreamPushNotifiesSubscriber() =
        runBlocking {
            val ns = "integration/ws-stream"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
            val serverRuntime = openMemoryRuntimeBlocking("it", ns, schema)
            val hub =
                StreamBroadcastHub(wire, ns, headProvider = { serverRuntime.dag.head() })
            val peerHost =
                peerSyncHostFactory(
                    wire,
                    serverRuntime.dag,
                    serverRuntime.storage,
                    PeerHostConfig(
                        namespaceId = ns,
                        nodeId = "host",
                        transportHub = "ws",
                        materializeCommit = { commit ->
                            dev.kdb.embed.materializeCommit(serverRuntime, ns, commit)
                            hub.publish(publishedCommitFrom(commit))
                        },
                    ),
                )(ConnectionContext.EMPTY)
            withDualWebSocketServers(peerHost, hub) { peerPort, streamPort ->
                val peerUri = "kdb-ws://127.0.0.1:$peerPort/kdb"
                val streamUri = "kdb-ws://127.0.0.1:$streamPort/kdb/stream"

                val clientB = openMemoryRuntimeBlocking("it", ns, schema)
                val transport = defaultWebSocketWireTransport()
                val peerConnBSetup =
                    peerSyncClient(wire, transport, clientB.dag, clientB.storage)
                val sessionBSetup =
                    peerConnBSetup.connect(PeerClientConfig(ns, "sub-b-setup", peerUri))
                val resumeHead = sessionBSetup.remoteHead
                peerConnBSetup.disconnect()

                val sub = streamSubscriber(wire, transport, clientB.indexManager)
                val deltas = mutableListOf<KdbHash>()
                val collectJob =
                    testScope.launch {
                        sub.connect(
                            StreamSubscriberConfig(
                                namespaceId = ns,
                                nodeId = "sub-b",
                                mode = StreamClientMode.READ_ONLY,
                                coordinatorUri = streamUri,
                                resumeFrom = resumeHead,
                            ),
                        )
                        sub.events.collect { event ->
                            if (event is StreamEvent.DeltaReceived) {
                                deltas.add(event.commitHash)
                            }
                        }
                    }
                delay(200)

                val clientA = openMemoryRuntimeBlocking("it", ns, schema)
                val docId = putJson(clientA, ns, """{"userId":"stream-u1","name":"Alice"}""", schema)
                val peerConn =
                    peerSyncClient(wire, transport, clientA.dag, clientA.storage)
                val sessionA =
                    peerConn.connect(PeerClientConfig(ns, "client-a", peerUri))
                assertEquals(1, pushCommitsSinceRemoteHead(sessionA, clientA.dag, sessionA.remoteHead).pushedCommits)
                peerConn.disconnect()
                delay(100)

                withTimeout(5.seconds) {
                    while (deltas.isEmpty()) delay(20)
                }
                collectJob.cancel()
                sub.disconnect()

                val peerConnB =
                    peerSyncClient(wire, transport, clientB.dag, clientB.storage)
                val sessionB =
                    peerConnB.connect(PeerClientConfig(ns, "client-b", peerUri))
                val syncB = syncEmbeddedWithPeer(clientB, sessionB, ns, schema)
                assertTrue(syncB.appliedCommits >= 1, "expected commit from stream notify, head=${clientB.dag.head()}")
                peerConnB.disconnect()

                val docJson = getJson(clientB, ns, docId)
                assertTrue(docJson.contains("stream-u1"), docJson)
            }
        }

    private suspend fun <R> withDualWebSocketServers(
        peerHost: dev.kdb.peersync.PeerSyncHost,
        streamHub: StreamBroadcastHub,
        block: suspend (peerPort: Int, streamPort: Int) -> R,
    ): R {
        val peerServer = JvmNetworkWebSocketServer()
        val streamServer = JvmNetworkWebSocketServer()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val peerJob =
            scope.launch {
                peerServer.start("127.0.0.1", 0, "/kdb") { conn ->
                    conn.incoming().collect { frame ->
                        val ack = peerHost.handleFrame(frame)
                        if (ack != null) conn.send(ack)
                    }
                }
            }
        val streamJob =
            scope.launch {
                streamServer.start("127.0.0.1", 0, "/kdb/stream") { conn ->
                    handleStreamConnection(conn, streamHub)
                }
            }
        return try {
            while (peerServer.port == 0 || streamServer.port == 0) delay(10)
            delay(100)
            block(peerServer.port, streamServer.port)
        } finally {
            peerServer.stop()
            streamServer.stop()
            peerJob.cancel()
            streamJob.cancel()
            scope.cancel()
        }
    }
}
