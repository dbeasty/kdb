package dev.kdb.transport.ws

import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Stopping the server, with or without a connected client, is normal shutdown and must not push an
 * exception out of the server's coroutines. With no handler in their context it lands in the JVM's
 * uncaught-exception handler, which kotlinx-coroutines-test reports against whichever test runs
 * next as `UncaughtExceptionsBeforeTest` (plan U7).
 */
class NetworkWebSocketServerShutdownTest {
    private val transport = defaultWebSocketWireTransport()
    private val wire = defaultWireCodec()
    private val uncaught = CopyOnWriteArrayList<Throwable>()
    private var previous: Thread.UncaughtExceptionHandler? = null

    @BeforeTest
    fun captureUncaught() {
        previous = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler { _, e -> uncaught += e }
    }

    @AfterTest
    fun restoreUncaught() {
        Thread.setDefaultUncaughtExceptionHandler(previous)
    }

    @Test
    fun stopIdleServer_leaksNothing() =
        runBlocking {
            val server = JvmNetworkWebSocketServer()
            server.start("127.0.0.1", 0, "/kdb") { conn -> conn.incoming().collect {} }
            delay(100)
            stopWithin10s(server)
            delay(200)
            assertEquals(emptyList(), uncaught.toList())
        }

    @Test
    fun stopWithConnectedClient_leaksNothingAndJoins() =
        runBlocking {
            val server = JvmNetworkWebSocketServer()
            val handlerDone = CompletableDeferred<Unit>()
            server.start("127.0.0.1", 0, "/kdb") { conn ->
                try {
                    conn.incoming().collect { frame -> conn.send(frame) }
                } finally {
                    handlerDone.complete(Unit)
                }
            }
            val client = transport.connect("kdb-ws://127.0.0.1:${server.port}/kdb")
            client.send(frame())
            waitForFrame(client)
            stopWithin10s(server)
            assertTrue(handlerDone.isCompleted, "stop() returned before the connection handler finished")
            client.close()
            delay(200)
            assertEquals(emptyList(), uncaught.toList())
        }

    @Test
    fun stopDuringHandshake_doesNotHang() =
        runBlocking {
            val server = JvmNetworkWebSocketServer()
            server.start("127.0.0.1", 0, "/kdb") { conn -> conn.incoming().collect {} }
            // A client that connects and never sends its HTTP upgrade request leaves the server
            // blocked in a socket read that cancellation alone cannot interrupt.
            Socket("127.0.0.1", server.port).use {
                delay(200)
                stopWithin10s(server)
            }
            delay(200)
            assertEquals(emptyList(), uncaught.toList())
        }

    /**
     * stop() is deliberately not cancellable, so a plain withTimeout around it cannot fire. Run it
     * detached and time out the wait instead, so a stop() that never returns fails the test rather
     * than hanging the build.
     */
    @OptIn(DelicateCoroutinesApi::class)
    private suspend fun stopWithin10s(server: JvmNetworkWebSocketServer) {
        val stopping = GlobalScope.async(Dispatchers.IO) { server.stop() }
        withTimeout(10_000) { stopping.await() }
    }

    private fun frame(): ByteArray =
        wire.encode(
            WireMessage.Handshake(
                WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                HandshakePayload(
                    nodeId = "shutdown-test",
                    namespaces = listOf("ns"),
                    localHeads = emptyMap(),
                    clientMode = WireClientMode.FULL_PEER,
                ),
            ),
        )

    private suspend fun waitForFrame(conn: dev.kdb.stream.WireConnection): ByteArray {
        repeat(5_000) {
            conn.tryPoll()?.let { return it }
            delay(2)
        }
        error("timeout waiting for wire frame")
    }
}
