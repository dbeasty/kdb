package dev.kdb.transport.tcp

import dev.kdb.wire.HandshakePayload
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
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A client going away, or the server stopping under a connected client, is normal shutdown. Neither
 * may push an exception out of the server's coroutines: with no handler in their context it lands
 * in the JVM's uncaught-exception handler, which kotlinx-coroutines-test reports against whichever
 * test runs next as `UncaughtExceptionsBeforeTest` (plan U7).
 */
class TcpLoopbackServerShutdownTest {
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
    fun clientDisconnect_leaksNothing() =
        runBlocking {
            val server = TcpLoopbackServer()
            val handlerDone = CompletableDeferred<Unit>()
            server.start { conn ->
                try {
                    conn.incoming().collect { frame -> conn.send(frame) }
                } finally {
                    handlerDone.complete(Unit)
                }
            }
            val client = defaultTcpWireTransport().connect(server.connectUri)
            client.send(frame())
            withTimeout(10_000) { client.incoming().first() }
            client.close()
            withTimeout(10_000) { handlerDone.await() }
            delay(200)
            server.stop()
            assertEquals(emptyList(), uncaught.toList())
        }

    @Test
    fun stopWithConnectedClient_leaksNothingAndJoins() =
        runBlocking {
            val server = TcpLoopbackServer()
            val handlerDone = CompletableDeferred<Unit>()
            server.start { conn ->
                try {
                    conn.incoming().collect { frame -> conn.send(frame) }
                } finally {
                    handlerDone.complete(Unit)
                }
            }
            val client = defaultTcpWireTransport().connect(server.connectUri)
            client.send(frame())
            withTimeout(10_000) { client.incoming().first() }
            stopWithin10s(server)
            // stop() waits for its coroutines, so the handler has already finished.
            assertTrue(handlerDone.isCompleted, "stop() returned before the connection handler finished")
            client.close()
            delay(200)
            assertEquals(emptyList(), uncaught.toList())
        }

    /**
     * stop() is deliberately not cancellable, so a plain withTimeout around it cannot fire. Run it
     * detached and time out the wait instead, so a stop() that never returns fails the test rather
     * than hanging the build.
     */
    @OptIn(DelicateCoroutinesApi::class)
    private suspend fun stopWithin10s(server: TcpLoopbackServer) {
        val stopping = GlobalScope.async(Dispatchers.IO) { server.stop() }
        withTimeout(10_000) { stopping.await() }
    }

    private fun frame(): ByteArray =
        defaultWireCodec().encode(
            WireMessage.Handshake(
                WireHeader(WireMessageType.HANDSHAKE, 1, 1, 0),
                HandshakePayload(
                    nodeId = "shutdown-test",
                    namespaces = listOf("ns"),
                    localHeads = emptyMap(),
                    clientMode = WireClientMode.STREAM_READ_ONLY,
                ),
            ),
        )
}
