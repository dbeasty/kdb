package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class InMemoryWireTransportTest {
    @Test
    fun handshakeReachesServerHandler() =
        runTest {
            val wire = defaultWireCodec()
            val hub = InMemoryWireTransportHub.hub("t")
            val calls = mutableListOf<Int>()
            hub.serverHandler = { calls.add(it.size) }
            val conn = InMemoryWireTransport().connect("memory://t")
            val hs =
                WireMessage.Handshake(
                    WireHeader(WireMessageType.HANDSHAKE, 1, 1, 0),
                    HandshakePayload(
                        nodeId = "c",
                        namespaces = listOf("t"),
                        localHeads = mapOf("t" to KdbHash.fromHex("00".repeat(32)).toHex()),
                        clientMode = WireClientMode.STREAM_READ_ONLY,
                    ),
                )
            val frame = wire.encode(hs)
            InMemoryWireTransportHub.dispatchToServer("t", frame)
            assertTrue(calls.isNotEmpty(), "serverHandler should run")
            hub.serverHandler = { hub.serverSend(it) }
            conn.send(wire.encode(hs))
            assertNotNull(conn.tryPoll(), "serverSend should deliver to client")
        }
}
