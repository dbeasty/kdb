package dev.kdb.integration

import dev.kdb.embed.openMemoryRuntime
import dev.kdb.embed.putJson
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.server.KdbServerRuntime
import dev.kdb.server.SqlWireHost
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SqlTransactionIntegrationTest {
    @Test
    fun multiStatementDmlSingleCommit() =
        runTest {
            val ns = "demo/users"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                        SchemaField("name", KdbFieldType.StringType, required = true, indexed = false),
                    ),
                )
            val runtime = openMemoryRuntime("demo", ns, schema)
            putJson(runtime, ns, """{"userId":"u1","name":"Alice"}""")
            val wire = defaultWireCodec()
            val host = SqlWireHost(wire, KdbServerRuntime(runtime), ns)

            val session =
                sessionBegin(host, wire, ns, "txn-1", 1)

            val headBefore = runtime.dag.head()
            execSql(host, wire, ns, session.sessionId, 2, "BEGIN")
            val u1 =
                execSql(
                    host,
                    wire,
                    ns,
                    session.sessionId,
                    3,
                    "UPDATE users SET name = 'Bob' WHERE userId = 'u1'",
                )
            assertNull(u1.error)
            assertEquals(headBefore.toHex(), u1.resolvedCommitHex)
            val u2 =
                execSql(
                    host,
                    wire,
                    ns,
                    session.sessionId,
                    4,
                    "UPDATE users SET name = 'Carol' WHERE userId = 'u1'",
                )
            assertNull(u2.error)
            assertEquals(headBefore.toHex(), u2.resolvedCommitHex)

            val committed = execSql(host, wire, ns, session.sessionId, 5, "COMMIT")
            assertNull(committed.error)
            assertTrue(committed.resolvedCommitHex != headBefore.toHex())

            val read =
                execSql(
                    host,
                    wire,
                    ns,
                    session.sessionId,
                    6,
                    "SELECT name FROM users WHERE userId = 'u1'",
                )
            assertNull(read.error)
            assertEquals("Carol", read.rows.single().single())
        }

    @Test
    fun rollbackDiscardsBufferedWrites() =
        runTest {
            val ns = "demo/users"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                        SchemaField("name", KdbFieldType.StringType, required = true, indexed = false),
                    ),
                )
            val runtime = openMemoryRuntime("demo", ns, schema)
            putJson(runtime, ns, """{"userId":"u1","name":"Alice"}""")
            val wire = defaultWireCodec()
            val host = SqlWireHost(wire, KdbServerRuntime(runtime), ns)
            val session = sessionBegin(host, wire, ns, "txn-rb", 1)

            execSql(host, wire, ns, session.sessionId, 2, "BEGIN")
            execSql(host, wire, ns, session.sessionId, 3, "UPDATE users SET name = 'Bob' WHERE userId = 'u1'")
            execSql(host, wire, ns, session.sessionId, 4, "ROLLBACK")

            val read =
                execSql(
                    host,
                    wire,
                    ns,
                    session.sessionId,
                    5,
                    "SELECT name FROM users WHERE userId = 'u1'",
                )
            assertEquals("Alice", read.rows.single().single())
        }

    @Test
    fun secondSessionBlockedUntilCommit() =
        runTest {
            val ns = "demo/users"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                        SchemaField("name", KdbFieldType.StringType, required = true, indexed = false),
                    ),
                )
            val runtime = openMemoryRuntime("demo", ns, schema)
            putJson(runtime, ns, """{"userId":"u1","name":"Alice"}""")
            val wire = defaultWireCodec()
            val host = SqlWireHost(wire, KdbServerRuntime(runtime), ns)

            val s1 = sessionBegin(host, wire, ns, "holder", 10)
            val s2 = sessionBegin(host, wire, ns, "waiter", 11)

            execSql(host, wire, ns, s1.sessionId, 12, "BEGIN")
            execSql(host, wire, ns, s1.sessionId, 13, "UPDATE users SET name = 'Held' WHERE userId = 'u1'")

            val blocked =
                execSql(
                    host,
                    wire,
                    ns,
                    s2.sessionId,
                    14,
                    "UPDATE users SET name = 'Other' WHERE userId = 'u1'",
                )
            assertNotNull(blocked.error)
            assertTrue(blocked.error!!.contains("locked", ignoreCase = true))

            execSql(host, wire, ns, s1.sessionId, 15, "COMMIT")

            val ok =
                execSql(
                    host,
                    wire,
                    ns,
                    s2.sessionId,
                    16,
                    "UPDATE users SET name = 'Other' WHERE userId = 'u1'",
                )
            assertNull(ok.error)
        }

    private suspend fun sessionBegin(
        host: SqlWireHost,
        wire: dev.kdb.wire.WireCodec,
        ns: String,
        sessionId: String,
        corr: Int,
    ): WireMessage.SessionBeginAck {
        val frame =
            wire.encode(
                WireMessage.SessionBegin(
                    WireHeader(WireMessageType.SESSION_BEGIN, dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION, corr, 0),
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
        wire: dev.kdb.wire.WireCodec,
        ns: String,
        sessionId: String,
        corr: Int,
        sql: String,
    ): WireMessage.SqlResult {
        val frame =
            wire.encode(
                WireMessage.SqlExec(
                    WireHeader(WireMessageType.SQL_EXEC, dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                    namespace = ns,
                    sessionId = sessionId,
                    sql = sql,
                    parametersJson = null,
                ),
            )
        return wire.decode(host.handleFrame(frame)!!) as WireMessage.SqlResult
    }
}
