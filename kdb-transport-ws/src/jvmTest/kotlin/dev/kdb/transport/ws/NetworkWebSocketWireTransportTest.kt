package dev.kdb.transport.ws

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
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NetworkWebSocketWireTransportTest {
    private val transport = defaultWebSocketWireTransport()
    private val wire = defaultWireCodec()

    @Test
    fun listenConnect_handshakeRoundTrip() =
        runBlocking {
            val server = JvmNetworkWebSocketServer()
            val serverJob =
                CoroutineScope(SupervisorJob() + Dispatchers.Default).launch {
                    server.start("127.0.0.1", 0, "/kdb") { conn ->
                        conn.incoming().collect { frame ->
                            val msg = wire.decode(frame)
                            if (msg is WireMessage.Handshake) {
                                val ack =
                                    WireMessage.HandshakeAck(
                                        WireHeader(
                                            WireMessageType.HANDSHAKE,
                                            KDB_WIRE_PROTOCOL_VERSION,
                                            msg.header.correlationId,
                                            0,
                                        ),
                                        dev.kdb.wire.HandshakeAckPayload(
                                            accepted = true,
                                            negotiatedEncoding = dev.kdb.wire.PayloadEncoding.KDB_BINARY,
                                            protocolVersion = KDB_WIRE_PROTOCOL_VERSION,
                                            remoteHeads = emptyMap(),
                                        ),
                                    )
                                conn.send(wire.encode(ack))
                            }
                        }
                    }
                }
            awaitServerPort(server)
            val client = transport.connect("kdb-ws://127.0.0.1:${server.port}/kdb")
            val hs =
                WireMessage.Handshake(
                    WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                    HandshakePayload(
                        nodeId = "test-client",
                        namespaces = listOf("demo"),
                        localHeads = emptyMap(),
                        clientMode = WireClientMode.FULL_PEER,
                    ),
                )
            client.send(wire.encode(hs))
            val reply = waitForFrame(client)
            assertTrue(wire.decode(reply) is WireMessage.HandshakeAck)
            client.close()
            server.stop()
            serverJob.cancel()
        }

    @Test
    fun twoWireRoundTrips() =
        runBlocking {
            val server = JvmNetworkWebSocketServer()
            val serverJob =
                CoroutineScope(SupervisorJob() + Dispatchers.Default).launch {
                    server.start("127.0.0.1", 0, "/kdb") { conn ->
                        conn.incoming().collect { frame ->
                            val msg = wire.decode(frame)
                            if (msg is WireMessage.Handshake) {
                                val ack =
                                    WireMessage.HandshakeAck(
                                        WireHeader(
                                            WireMessageType.HANDSHAKE,
                                            KDB_WIRE_PROTOCOL_VERSION,
                                            msg.header.correlationId,
                                            0,
                                        ),
                                        dev.kdb.wire.HandshakeAckPayload(
                                            accepted = true,
                                            negotiatedEncoding = dev.kdb.wire.PayloadEncoding.KDB_BINARY,
                                            protocolVersion = KDB_WIRE_PROTOCOL_VERSION,
                                            remoteHeads = emptyMap(),
                                        ),
                                    )
                                conn.send(wire.encode(ack))
                            }
                        }
                    }
                }
            awaitServerPort(server)
            val client = transport.connect("kdb-ws://127.0.0.1:${server.port}/kdb")
            val hs1 =
                handshakeFrame(
                    correlationId = 1,
                    nodeId = "c1",
                )
            val hs2 =
                handshakeFrame(
                    correlationId = 2,
                    nodeId = "c2",
                )
            client.send(hs1)
            val ack1 = wire.decode(waitForFrame(client)) as WireMessage.HandshakeAck
            assertEquals(1, ack1.header.correlationId)
            client.send(hs2)
            val ack2 = wire.decode(waitForFrame(client)) as WireMessage.HandshakeAck
            assertEquals(2, ack2.header.correlationId)
            client.close()
            server.stop()
            serverJob.cancel()
        }

    private fun handshakeFrame(
        correlationId: Int,
        nodeId: String,
    ): ByteArray {
        val hs =
            WireMessage.Handshake(
                WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, correlationId, 0),
                HandshakePayload(
                    nodeId = nodeId,
                    namespaces = listOf("demo"),
                    localHeads = emptyMap(),
                    clientMode = WireClientMode.FULL_PEER,
                ),
            )
        return wire.encode(hs)
    }

    private suspend fun awaitServerPort(server: JvmNetworkWebSocketServer) {
        repeat(500) {
            if (server.port != 0) return
            delay(10)
        }
        error("server did not bind")
    }

    private suspend fun waitForFrame(conn: dev.kdb.stream.WireConnection): ByteArray {
        repeat(2_000) {
            conn.tryPoll()?.let { return it }
            delay(2)
        }
        error("timeout waiting for wire frame")
    }
}
