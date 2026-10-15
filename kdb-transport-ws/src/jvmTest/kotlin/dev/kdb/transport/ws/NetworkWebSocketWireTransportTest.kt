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

    /** Two listeners on one fixed port at different paths (kdb-service's peer sync and stream):
     * the second used to fail to bind; now each path reaches its own handler, and the port is
     * released once both stop - even though they are cancelled together, which used to leave the
     * server listening when one release found the other holding the registry lock.
     *
     * The first listener binds port 0 and the second joins the port it got, so no other process
     * can take the port in between. */
    @Test
    fun twoListenersShareAFixedPortByPath() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            fun answer(cid: Int): suspend (dev.kdb.stream.WireConnection) -> Unit =
                { conn ->
                    conn.incoming().collect {
                        conn.send(wire.encode(hello(cid)))
                    }
                }
            val a = scope.launch { transport.listen("kdb-ws://127.0.0.1:0/kdb?bind=true", dev.kdb.transport.core.TransportConnectOptions(), answer(1)) }
            val port = awaitListenPort(transport as JvmWebSocketWireTransport)
            val b = scope.launch { transport.listen("kdb-ws://127.0.0.1:$port/kdb/stream?bind=true", dev.kdb.transport.core.TransportConnectOptions(), answer(2)) }
            delay(200)
            for ((path, cid) in listOf("/kdb" to 1, "/kdb/stream" to 2)) {
                val client = transport.connect("kdb-ws://127.0.0.1:$port$path")
                client.send(wire.encode(hello(9)))
                assertEquals(cid, wire.decode(waitForFrame(client)).header.correlationId, path)
                client.close()
            }
            a.cancel()
            b.cancel()
            a.join()
            b.join()
            java.net.ServerSocket(port).close() // released
            scope.cancel()
        }

    private fun hello(cid: Int) =
        WireMessage.Handshake(
            WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, cid, 0),
            HandshakePayload(nodeId = "n", namespaces = listOf("demo"), localHeads = emptyMap(), clientMode = WireClientMode.FULL_PEER),
        )

    private suspend fun awaitListenPort(transport: JvmWebSocketWireTransport): Int {
        repeat(500) {
            runCatching { transport.networkListenPort() }.getOrNull()?.takeIf { it != 0 }?.let { return it }
            delay(10)
        }
        error("listener did not bind")
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
