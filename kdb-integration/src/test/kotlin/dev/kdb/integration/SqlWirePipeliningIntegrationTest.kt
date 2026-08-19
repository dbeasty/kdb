package dev.kdb.integration

import dev.kdb.embed.openMemoryRuntime
import dev.kdb.embed.putJson
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.server.KdbServerRuntime
import dev.kdb.server.SqlWireHost
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireCodec
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Phase 6 (docs/benchmarks/phase0-baseline.md): SqlWireHost.handleFrame
 * calls for the same session must still behave as if processed one at a
 * time even when *submitted* concurrently - which is exactly what the
 * new pipelined perConnection handler now does. This exercises
 * handleFrame directly (bypassing the transport) with concurrent calls
 * on one session to prove the per-session lock actually prevents the
 * corruption that KdbSession.pending (a plain var) would otherwise be
 * exposed to.
 */
class SqlWirePipeliningIntegrationTest {
    @Test
    fun concurrentHandleFrameCallsOnSameSession_serializeCorrectly() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            putJson(runtime, ns, """{"userId":"seed"}""")
            val wire = defaultWireCodec()
            val host = SqlWireHost(wire, KdbServerRuntime(runtime), ns)
            val session = sessionBegin(host, wire, ns, "pipeline-1", 1)

            execSql(host, wire, ns, session.sessionId, 2, "BEGIN")

            val n = 50
            coroutineScope {
                // Fire all inserts concurrently on the SAME session,
                // exactly as pipelinedPerConnection now allows a client
                // to do - correctness must not depend on the caller
                // awaiting each response before sending the next.
                (0 until n)
                    .map { i ->
                        async {
                            execSql(
                                host,
                                wire,
                                ns,
                                session.sessionId,
                                100 + i,
                                "INSERT INTO users (_doc) VALUES ('{\"userId\":\"u$i\"}')",
                            )
                        }
                    }
                    .awaitAll()
                    .forEach { result -> assertNull(result.error, "insert failed: ${result.error}") }
            }

            val committed = execSql(host, wire, ns, session.sessionId, 999, "COMMIT")
            assertNull(committed.error)

            val count =
                execSql(host, wire, ns, session.sessionId, 1000, "SELECT _doc FROM users")
            assertEquals(n + 1, count.rows.size, "expected all $n concurrently-submitted inserts (+1 seed row) to land")
        }

    @Test
    fun concurrentHandleFrameCallsOnDifferentSessions_runIndependently() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            putJson(runtime, ns, """{"userId":"seed"}""")
            val wire = defaultWireCodec()
            val host = SqlWireHost(wire, KdbServerRuntime(runtime), ns)

            val sessionCount = 20
            val sessions = (0 until sessionCount).map { i -> sessionBegin(host, wire, ns, "sess-$i", i + 1) }

            coroutineScope {
                sessions
                    .mapIndexed { i, s ->
                        async {
                            execSql(host, wire, ns, s.sessionId, 1000 + i, "SELECT _doc FROM users")
                        }
                    }
                    .awaitAll()
                    .forEach { result ->
                        assertNull(result.error)
                        assertEquals(1, result.rows.size)
                    }
            }
        }

    private suspend fun sessionBegin(
        host: SqlWireHost,
        wire: WireCodec,
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
        return wire.decode(host.handleFrame(frame)!!) as WireMessage.SessionBeginAck
    }

    private suspend fun execSql(
        host: SqlWireHost,
        wire: WireCodec,
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
        return wire.decode(host.handleFrame(frame)!!) as WireMessage.SqlResult
    }
}
