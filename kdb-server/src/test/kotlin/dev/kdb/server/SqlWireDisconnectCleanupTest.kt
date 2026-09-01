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
import dev.kdb.wire.WireCodec
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
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Component 45 (Layer 12 gap analysis §5.2): a client that drops its connection while holding a
 * document lock (BEGIN'd a transaction, never COMMIT/ROLLBACK, then disconnects) used to leak
 * that lock forever - [pipelinedPerConnection] never called [SqlWireHost.endSession] on the way
 * out. This drives the real per-connection read loop (not just handleFrame directly, the way
 * [dev.kdb.integration] tests do) through an actual disconnect, and proves a second connection
 * can then acquire the same document's lock - the direct, observable consequence of the fix.
 */
class SqlWireDisconnectCleanupTest {
    private val wire = defaultWireCodec()

    @Test
    fun disconnectingMidTransactionReleasesItsDocumentLocks() =
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
            val server = KdbServerRuntime(runtime)
            val hostFactory = sqlWireHostFactory(wire, server, ns)

            // "Connection A": a real per-connection read loop, holding a lock via an open
            // transaction it never commits or rolls back.
            val hostA = hostFactory(ConnectionContext.EMPTY)
            val connA = FakeWireConnection()
            val loopA = launch { pipelinedPerConnection(connA, hostA) }

            val sessionA = sessionBegin(connA, ns, "holder", 1)
            execSql(connA, ns, sessionA.sessionId, 2, "BEGIN")
            execSql(connA, ns, sessionA.sessionId, 3, "UPDATE users SET name = 'Held' WHERE userId = 'u1'")

            // A second, independent connection is blocked by connA's still-open transaction.
            val hostB = hostFactory(ConnectionContext.EMPTY)
            val connB = FakeWireConnection()
            val loopB = launch { pipelinedPerConnection(connB, hostB) }
            val sessionB = sessionBegin(connB, ns, "waiter", 10)
            val blocked = execSql(connB, ns, sessionB.sessionId, 11, "UPDATE users SET name = 'Other' WHERE userId = 'u1'")
            assertNotNull(blocked.error)
            assertTrue(blocked.error!!.contains("locked", ignoreCase = true))

            // connA drops without ever sending COMMIT or ROLLBACK - simulates a crashed or
            // network-dropped client. Closing its incoming channel ends conn.incoming()'s Flow,
            // which is what a real transport does on disconnect.
            connA.close()
            loopA.join()

            // The lock must now be released: connB can complete the write it was blocked on.
            val ok = execSql(connB, ns, sessionB.sessionId, 12, "UPDATE users SET name = 'Other' WHERE userId = 'u1'")
            assertNull(ok.error, "expected connA's disconnect to have released its document lock: ${ok.error}")

            connB.close()
            loopB.join()
        }

    /**
     * Two connections that both let the server name their session used to end up as the same lock
     * holder: the id counter lived on [SessionManager], which is created per connection, so every
     * connection's first session was `sess-1` - while [KdbServerRuntime.documentLocks] is
     * runtime-global and keys ownership by that string. Connection B could therefore walk straight
     * through a lock connection A was holding (the manager saw `owner == sessionId` and granted
     * it), and either side's `releaseAll` dropped the other's locks mid-transaction. Ids now come
     * from [KdbServerRuntime.nextSessionOrdinal] - the same fix Go's SessionManager already
     * carries, ported here.
     */
    @Test
    fun twoConnectionsLettingTheServerNameTheirSessionAreNotOneLockHolder() =
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
            val server = KdbServerRuntime(runtime)
            val hostFactory = sqlWireHostFactory(wire, server, ns)

            val hostA = hostFactory(ConnectionContext.EMPTY)
            val connA = FakeWireConnection()
            val loopA = launch { pipelinedPerConnection(connA, hostA) }
            val sessionA = sessionBegin(connA, ns, null, 1)

            val hostB = hostFactory(ConnectionContext.EMPTY)
            val connB = FakeWireConnection()
            val loopB = launch { pipelinedPerConnection(connB, hostB) }
            val sessionB = sessionBegin(connB, ns, null, 10)

            // The visible symptom of the old bug, before anything else happens.
            assertNotEquals(
                sessionA.sessionId,
                sessionB.sessionId,
                "two connections were handed the same session id",
            )

            execSql(connA, ns, sessionA.sessionId, 2, "BEGIN")
            execSql(connA, ns, sessionA.sessionId, 3, "UPDATE users SET name = 'Held' WHERE userId = 'u1'")

            execSql(connB, ns, sessionB.sessionId, 11, "BEGIN")
            val blocked = execSql(connB, ns, sessionB.sessionId, 12, "UPDATE users SET name = 'Other' WHERE userId = 'u1'")
            assertNotNull(blocked.error, "connection B wrote through a lock connection A holds")
            assertTrue(blocked.error!!.contains("locked", ignoreCase = true), "unexpected error: ${blocked.error}")

            connA.close()
            loopA.join()
            connB.close()
            loopB.join()
        }

    private suspend fun sessionBegin(
        conn: FakeWireConnection,
        ns: String,
        sessionId: String?,
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

    /**
     * A [WireConnection] driven entirely in-process: `send` and `incoming` are the two ends of
     * one channel pair, so a test can push a request frame in and read the response frame back
     * out while [pipelinedPerConnection] runs the real read loop on the other side - exercising
     * the actual connection-teardown path this test verifies, not just [SqlWireHost.handleFrame]
     * called directly (as [dev.kdb.integration.SqlTransactionIntegrationTest] does).
     */
    private class FakeWireConnection : WireConnection {
        private val inbound = Channel<ByteArray>(Channel.UNLIMITED)
        private val outbound = Channel<ByteArray>(Channel.UNLIMITED)

        suspend fun roundTrip(frame: ByteArray): ByteArray {
            inbound.send(frame)
            return outbound.receive()
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
