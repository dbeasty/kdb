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
