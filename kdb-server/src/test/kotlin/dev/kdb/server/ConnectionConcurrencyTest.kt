package dev.kdb.server

import dev.kdb.auth.ConnectionContext
import dev.kdb.codec.KdbUuid
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.schema.KdbSchema
import dev.kdb.stream.WireConnection
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Component 73 (§12), connection half: frames on one connection are handled concurrently, so a slow
 * statement on one session cannot delay a sessionless DocumentGet on the same connection - while two
 * statements on the *same* session are still processed strictly in order.
 *
 * A test-only hook ([SqlWireHost.sqlExecHook]) suspends inside handleSqlExec so "slow" is exact
 * rather than timing-dependent. Real threads, since the property is about actual concurrency.
 */
class ConnectionConcurrencyTest {
    private val wire = defaultWireCodec()
    private val ns = "demo/concurrency"

    /** Guards: a SqlExec parked mid-statement on one session does not delay a DocumentGet on the same
     * connection - the sessionless frame replies while the statement is still in flight. */
    @Test
    fun aSlowSqlExecDoesNotDelayADocumentGetOnTheSameConnection() =
        runBlocking<Unit> {
            val runtime = openMemoryRuntime("demo", ns, KdbSchema.NONE)
            val server = KdbServerRuntime(runtime)
            val docId = KdbUuid.random()
            server.upsert(ns, docId, """{"v":1}""")
            val host = sqlWireHostFactory(wire, server, ns)(ConnectionContext.EMPTY)
            val conn = FakeWireConnection()
            val loop = launch(Dispatchers.Default) { pipelinedPerConnection(conn, host) }

            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            host.sqlExecHook = {
                entered.complete(Unit)
                release.await()
            }

            val session = sessionBegin(conn, "s1", 1)
            conn.sendRaw(sqlExecFrame(session.sessionId, 2, "SELECT kdb_id FROM t"))
            withTimeout(5_000) { entered.await() }

            // The statement is parked inside the host. A sessionless DocumentGet must still complete.
            val get =
                withTimeout(5_000) {
                    wire.decode(conn.awaitReply(3) { conn.sendRaw(documentGetFrame(docId.toString(), 3)) })
                } as WireMessage.DocumentGetResult
            assertNull(get.error)
            assertEquals("""{"v":1}""", get.json)

            release.complete(Unit)
            withTimeout(5_000) { conn.awaitReply(2) {} }
            conn.close()
            loop.join()
        }

    /** Guards: two SqlExec frames on the same session are processed in order - the second does not
     * start until the first has finished, even though the connection dispatches frames concurrently. */
    @Test
    fun twoSqlExecOnTheSameSessionAreProcessedInOrder() =
        runBlocking<Unit> {
            val runtime = openMemoryRuntime("demo", ns, KdbSchema.NONE)
            val server = KdbServerRuntime(runtime)
            val host = sqlWireHostFactory(wire, server, ns)(ConnectionContext.EMPTY)
            val conn = FakeWireConnection()
            val loop = launch(Dispatchers.Default) { pipelinedPerConnection(conn, host) }

            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val startsMutex = Mutex()
            var starts = 0
            host.sqlExecHook = {
                val ordinal = startsMutex.withLock { ++starts }
                if (ordinal == 1) {
                    entered.complete(Unit)
                    release.await()
                }
            }

            val session = sessionBegin(conn, "s1", 1)
            conn.sendRaw(sqlExecFrame(session.sessionId, 2, "SELECT kdb_id FROM t"))
            withTimeout(5_000) { entered.await() }
            conn.sendRaw(sqlExecFrame(session.sessionId, 3, "SELECT kdb_id FROM t"))

            // While the first statement is parked, the second must not even have started, and must
            // certainly not have replied.
            assertNull(withTimeoutOrNull(300) { conn.awaitReply(3) {} }, "same-session frames must not overlap")
            assertEquals(1, startsMutex.withLock { starts })

            release.complete(Unit)
            val first = withTimeout(5_000) { wire.decode(conn.awaitReply(2) {}) } as WireMessage.SqlResult
            val second = withTimeout(5_000) { wire.decode(conn.awaitReply(3) {}) } as WireMessage.SqlResult
            assertNull(first.error, "first statement failed: ${first.error}")
            assertNull(second.error, "second statement failed: ${second.error}")
            assertEquals(2, startsMutex.withLock { starts })
            assertTrue(conn.completionOrder().indexOf(2) < conn.completionOrder().indexOf(3))

            conn.close()
            loop.join()
        }

    private suspend fun sessionBegin(
        conn: FakeWireConnection,
        sessionId: String,
        corr: Int,
    ): WireMessage.SessionBeginAck {
        val frame =
            wire.encode(
                WireMessage.SessionBegin(
                    WireHeader(WireMessageType.SESSION_BEGIN, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                    namespace = ns,
                    sessionId = sessionId,
                    readConsistency = ReadConsistency.READ_COMMITTED.name,
                    baseVersionHex = null,
                ),
            )
        conn.sendRaw(frame)
        return wire.decode(conn.awaitReply(corr) {}) as WireMessage.SessionBeginAck
    }

    private fun sqlExecFrame(
        sessionId: String,
        corr: Int,
        sql: String,
    ): ByteArray =
        wire.encode(
            WireMessage.SqlExec(
                WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                namespace = ns,
                sessionId = sessionId,
                sql = sql,
                parametersJson = null,
            ),
        )

    private fun documentGetFrame(
        docId: String,
        corr: Int,
    ): ByteArray =
        wire.encode(
            WireMessage.DocumentGet(
                WireHeader(WireMessageType.DOCUMENT_GET, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                namespace = ns,
                docId = docId,
            ),
        )

    /**
     * Replies arrive out of order here (that is the point), so unlike the other host tests' fakes this
     * one demultiplexes by correlation id and remembers the order replies actually completed in.
     */
    private inner class FakeWireConnection : WireConnection {
        private val inbound = Channel<ByteArray>(Channel.UNLIMITED)
        private val mutex = Mutex()
        private val byCorrelation = mutableMapOf<Int, ByteArray>()
        private val order = mutableListOf<Int>()
        private val arrivals = Channel<Int>(Channel.UNLIMITED)

        suspend fun sendRaw(frame: ByteArray) {
            inbound.send(frame)
        }

        /** Runs [send], then waits until the reply carrying [corr] has arrived. */
        suspend fun awaitReply(
            corr: Int,
            send: suspend () -> Unit,
        ): ByteArray {
            send()
            while (true) {
                mutex.withLock { byCorrelation[corr] }?.let { return it }
                arrivals.receive()
            }
        }

        suspend fun completionOrder(): List<Int> = mutex.withLock { order.toList() }

        override suspend fun send(frame: ByteArray) {
            val corr = wire.decodeHeader(frame).correlationId
            mutex.withLock {
                byCorrelation[corr] = frame
                order += corr
            }
            arrivals.send(corr)
        }

        override fun incoming(): Flow<ByteArray> = inbound.receiveAsFlow()

        override suspend fun close() {
            inbound.close()
        }
    }
}
