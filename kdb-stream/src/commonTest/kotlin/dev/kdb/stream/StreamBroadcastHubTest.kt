package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StreamBroadcastHubTest {
    private val wire = defaultWireCodec()

    @Test
    fun streamUriFromPeerUriAppendsStreamPath() {
        assertEquals(
            "kdb-ws://127.0.0.1:7443/kdb/stream?bind=true",
            streamUriFromPeerUri("kdb-ws://127.0.0.1:7443/kdb?bind=true"),
        )
    }

    @Test
    fun publishDeliversDeltaCommit() =
        runTest {
            val ns = "app/broadcast"
            val dag = inMemoryCommitDag(ns)
            val parent = KdbHash.fromHex("11".repeat(32))
            val child = KdbHash.fromHex("22".repeat(32))
            val hub = StreamBroadcastHub(wire, ns, headProvider = { dag.head() })
            val conn = RecordingWireConnection()
            handshake(conn, hub, ns, parent)
            val received = Channel<ByteArray>(4)
            conn.outgoing = received
            hub.publish(
                PublishedCommit(
                    commitHash = child,
                    parentHash = parent,
                    timestampMicros = 0L,
                ),
            )
            val frame = received.receive()
            val msg = wire.decode(frame)
            assertTrue(msg is WireMessage.DeltaCommit)
            assertEquals(child, msg.payload.commitHash)
            assertEquals(parent, msg.payload.parentHash)
        }

    // Component 44 regression: publishedCommitFrom used to crash (`error(...)`) for a commit
    // with no parent - only reachable once SQL-write commits started flowing through it, since
    // peer-sync's own use of it never happened to hit a namespace's very first commit. A fresh
    // dag's genesis commit *is* a real root commit, so this exercises the actual case directly.
    @Test
    fun publishedCommitFromHandlesRootCommitWithoutCrashing() =
        runTest {
            val ns = "app/root-commit"
            val dag = inMemoryCommitDag(ns)
            val genesis = dag.getCommitOrThrow(dag.head())
            assertTrue(genesis.parentHashes.isEmpty(), "expected a genuine root commit")

            val published = publishedCommitFrom(genesis)

            assertEquals(genesis.hash, published.commitHash)
            assertEquals(KdbHash.fromHex("00".repeat(32)), published.parentHash)
        }

    // Component 46: no transactionReplayer configured - TransactionReplay must be rejected
    // explicitly (a clean, named error), not silently dropped the way it was before this fix.
    @Test
    fun transactionReplayWithNoReplayerConfiguredIsRejectedExplicitly() =
        runTest {
            val ns = "app/replay-unconfigured"
            val dag = inMemoryCommitDag(ns)
            val hub = StreamBroadcastHub(wire, ns, headProvider = { dag.head() })
            val conn = RecordingWireConnection()

            val replay =
                WireMessage.TransactionReplay(
                    WireHeader(WireMessageType.TRANSACTION_REPLAY, 1, 42, 0),
                    ns,
                    dag.head(),
                    byteArrayOf(1, 2, 3),
                )
            val replyFrame = hub.handleFrame(conn, wire.encode(replay))
            assertTrue(replyFrame != null, "expected an explicit rejection frame, not a silent drop")
            val decoded = wire.decode(replyFrame!!)
            assertTrue(decoded is WireMessage.SqlResult)
            assertTrue(decoded.error != null)
        }

    // Component 46: a configured replayer is invoked and its response is what's sent back.
    @Test
    fun transactionReplayRoutesThroughConfiguredReplayer() =
        runTest {
            val ns = "app/replay-configured"
            val dag = inMemoryCommitDag(ns)
            var replayerInvokedWith: WireMessage.TransactionReplay? = null
            val hub =
                StreamBroadcastHub(
                    wire,
                    ns,
                    headProvider = { dag.head() },
                    transactionReplayer = { msg ->
                        replayerInvokedWith = msg
                        WireMessage.SqlResult(
                            WireHeader(WireMessageType.SQL_RESULT, 1, msg.header.correlationId, 0),
                            namespace = msg.namespace,
                            sessionId = "",
                            columns = emptyList(),
                            rows = emptyList(),
                            rowsAffected = 1,
                            resolvedCommitHex = dag.head().toHex(),
                            readOnly = false,
                        )
                    },
                )
            val conn = RecordingWireConnection()
            val replay =
                WireMessage.TransactionReplay(
                    WireHeader(WireMessageType.TRANSACTION_REPLAY, 1, 99, 0),
                    ns,
                    dag.head(),
                    byteArrayOf(9, 8, 7),
                )

            val replyFrame = hub.handleFrame(conn, wire.encode(replay))!!
            val decoded = wire.decode(replyFrame) as WireMessage.SqlResult

            assertEquals(ns, replayerInvokedWith?.namespace)
            assertEquals(dag.head().toHex(), decoded.resolvedCommitHex)
            assertEquals(1, decoded.rowsAffected)
        }

    // TransactionReplay for a different namespace than this hub serves must be ignored, same as
    // Handshake's own namespace check - not routed to the replayer at all.
    @Test
    fun transactionReplayForDifferentNamespaceIsIgnored() =
        runTest {
            val ns = "app/replay-this-ns"
            val dag = inMemoryCommitDag(ns)
            var replayerInvoked = false
            val hub =
                StreamBroadcastHub(
                    wire,
                    ns,
                    headProvider = { dag.head() },
                    transactionReplayer = { _ ->
                        replayerInvoked = true
                        error("should not be called")
                    },
                )
            val conn = RecordingWireConnection()
            val replay =
                WireMessage.TransactionReplay(
                    WireHeader(WireMessageType.TRANSACTION_REPLAY, 1, 7, 0),
                    "app/some-other-ns",
                    dag.head(),
                    byteArrayOf(),
                )

            val replyFrame = hub.handleFrame(conn, wire.encode(replay))

            assertEquals(null, replyFrame)
            assertTrue(!replayerInvoked)
        }

    private suspend fun handshake(
        conn: RecordingWireConnection,
        hub: StreamBroadcastHub,
        ns: String,
        resume: KdbHash,
    ) {
        val hs =
            WireMessage.Handshake(
                WireHeader(WireMessageType.HANDSHAKE, 1, 1, 0),
                HandshakePayload(
                    nodeId = "sub",
                    namespaces = listOf(ns),
                    localHeads = mapOf(ns to resume.toHex()),
                    clientMode = WireClientMode.STREAM_READ_ONLY,
                ),
            )
        val ack = hub.handleFrame(conn, wire.encode(hs))!!
        val decoded = wire.decode(ack) as WireMessage.HandshakeAck
        assertTrue(decoded.response.accepted)
    }

    private class RecordingWireConnection : WireConnection {
        var outgoing: Channel<ByteArray>? = null

        override suspend fun send(frame: ByteArray) {
            outgoing?.trySend(frame)
        }

        override fun incoming() = Channel<ByteArray>().receiveAsFlow()

        override suspend fun close() = Unit
    }
}
