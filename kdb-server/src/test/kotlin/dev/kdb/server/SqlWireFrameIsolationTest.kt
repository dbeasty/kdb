package dev.kdb.server

import dev.kdb.auth.ConnectionContext
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.embed.putJson
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.stream.WireConnection
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertNull

/**
 * Regression test for docs/kdb-finish-up-plan.md's 1-K6: [pipelinedPerConnection] used to let an
 * exception from [SqlWireHost.handleFrame] (e.g. `wire.decode` throwing on a malformed frame)
 * escape its per-frame `launch`. Under structured concurrency that tears down the whole enclosing
 * `coroutineScope`, cancelling every *other* in-flight frame on the same connection too - so one
 * bad frame could take out every session pipelined on that connection, not just itself. This
 * sends a frame too short to decode, then a normal request on the same connection, and asserts
 * the normal request still completes instead of the connection dying.
 */
class SqlWireFrameIsolationTest {
    private val wire = defaultWireCodec()

    @Test
    fun malformedFrameDoesNotKillOtherInFlightRequestsOnTheSameConnection() =
        runTest {
            val ns = "demo/frame-isolation"
            val schema =
                KdbSchema.build(
                    listOf(SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true)),
                )
            val runtime = openMemoryRuntime("demo", ns, schema)
            putJson(runtime, ns, """{"userId":"u1"}""")
            val server = KdbServerRuntime(runtime)
            val host = sqlWireHostFactory(wire, server, ns)(ConnectionContext.EMPTY)
            val conn = FakeWireConnection()
            val loop = launch { pipelinedPerConnection(conn, host) }

            val session = sessionBegin(conn, ns, "s1", 1)

            // A frame too short to even decode a header from (FRAME_HEADER_SIZE is 12 bytes) -
            // wire.decode throws WireDecodeException for this, uncaught by handleFrame itself.
            conn.sendRaw(byteArrayOf(1, 2, 3))

            // If the malformed frame above tore down the connection's coroutineScope, this
            // legitimate request would never get a reply and the test would hang/time out.
            val result = execSql(conn, ns, session.sessionId, 2, "SELECT userId FROM users WHERE userId = 'u1'")
            assertNull(result.error, "a malformed frame must not prevent other requests on the same connection from completing: ${result.error}")

            conn.close()
            loop.join()
        }

    private suspend fun sessionBegin(
        conn: FakeWireConnection,
        ns: String,
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
        return wire.decode(conn.roundTrip(frame)) as WireMessage.SessionBeginAck
    }

    private suspend fun execSql(
        conn: FakeWireConnection,
        ns: String,
        sessionId: String,
        corr: Int,
        sql: String,
    ): WireMessage.SqlResult {
        val frame =
            wire.encode(
                WireMessage.SqlExec(
                    WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                    namespace = ns,
                    sessionId = sessionId,
                    sql = sql,
                    parametersJson = null,
                ),
            )
        return wire.decode(conn.roundTrip(frame)) as WireMessage.SqlResult
    }

    /** See SqlWireDisconnectCleanupTest's identically-named class for the rationale. */
    private class FakeWireConnection : WireConnection {
        private val inbound = Channel<ByteArray>(Channel.UNLIMITED)
        private val outbound = Channel<ByteArray>(Channel.UNLIMITED)

        suspend fun roundTrip(frame: ByteArray): ByteArray {
            inbound.send(frame)
            return outbound.receive()
        }

        /** Pushes a frame in without waiting for a reply - for frames expected to produce none. */
        suspend fun sendRaw(frame: ByteArray) {
            inbound.send(frame)
        }

        override suspend fun send(frame: ByteArray) {
            outbound.send(frame)
        }

        override fun incoming(): Flow<ByteArray> = inbound.receiveAsFlow()

        override suspend fun close() {
            inbound.close()
        }
    }
}
