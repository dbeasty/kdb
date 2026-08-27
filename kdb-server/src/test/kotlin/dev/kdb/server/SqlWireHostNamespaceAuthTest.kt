package dev.kdb.server

import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.Authenticator
import dev.kdb.auth.Authorizer
import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.KdbAuthorizationException
import dev.kdb.auth.Principal
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
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Regression test for docs/kdb-finish-up-plan.md's 1-K5: handleSqlExec/handleTxCommit used to
 * authorize against the client-supplied `msg.namespace` while actually executing against the
 * session's own `session.namespaceId` (fixed at SessionBegin, when it was separately authorized).
 * A principal allowed to SessionBegin broadly but only granted SqlExec/TxCommit rights on a
 * *different* namespace could open a session against the namespace it lacks write rights to, then
 * send a SqlExec/TxCommit whose `namespace` field names the namespace it *does* have rights to -
 * the auth check passed against that field while the write actually landed in the session's real,
 * unauthorized namespace.
 */
class SqlWireHostNamespaceAuthTest {
    private val wire = defaultWireCodec()

    /** Allows SessionBegin anywhere, but SqlExec/TxCommit only for [allowedNamespace]. */
    private class NamespaceScopedAuth(private val allowedNamespace: String) : AuthEngine {
        override val authenticator: Authenticator =
            object : Authenticator {
                override suspend fun authenticate(credentials: AuthCredentials): Principal = Principal(id = "scoped-user")
            }

        override val authorizer: Authorizer =
            object : Authorizer {
                override suspend fun authorize(principal: Principal, action: AuthAction) {
                    val namespace =
                        when (action) {
                            is AuthAction.SessionBegin -> return // allowed anywhere
                            is AuthAction.SqlExec -> action.namespace
                            is AuthAction.TxCommit -> action.namespace
                            else -> return
                        }
                    if (namespace != allowedNamespace) {
                        throw KdbAuthorizationException("no grant on namespace '$namespace'")
                    }
                }
            }
    }

    @Test
    fun sqlExecIsAuthorizedAgainstTheSessionsRealNamespaceNotTheClientSuppliedOne() =
        runTest {
            val restricted = "nsA-no-grant"
            val allowed = "nsB-has-grant"
            val schema =
                KdbSchema.build(
                    listOf(SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true)),
                )
            val runtime = openMemoryRuntime("demo", restricted, schema)
            putJson(runtime, restricted, """{"userId":"u1"}""")
            val server = KdbServerRuntime(runtime)
            val hostFactory = { ctx: ConnectionContext -> SqlWireHost(wire, server, restricted, NamespaceScopedAuth(allowed), ctx) }

            val host = hostFactory(ConnectionContext.EMPTY)
            val conn = FakeWireConnection()
            val loop = launch { pipelinedPerConnection(conn, host) }

            // Session legitimately opened against the restricted namespace (SessionBegin is
            // allowed anywhere by this test's auth engine).
            val session = sessionBegin(conn, restricted, "s1", 1)
            assertNotNull(session.sessionId.ifEmpty { null }, "SessionBegin should have succeeded")

            // The SqlExec frame *claims* the allowed namespace (which this principal genuinely
            // has a grant on) while sessionId is bound to the restricted one - exactly the
            // spoofing shape the bug allowed through.
            val result =
                execSql(conn, namespace = allowed, sessionId = session.sessionId, corr = 2, sql = "UPDATE users SET userId = 'u2' WHERE userId = 'u1'")

            assertNotNull(result.error, "the write must be rejected: it actually executes against '$restricted', which this principal has no grant on")
            assertTrue(result.error!!.contains("forbidden", ignoreCase = true) || result.error!!.contains(restricted))

            // Confirm the write genuinely did not happen.
            val check = execSql(conn, namespace = allowed, sessionId = session.sessionId, corr = 3, sql = "SELECT userId FROM users WHERE userId = 'u2'")
            assertNotNull(check.error, "still forbidden - just double-checking no earlier bug let the write through despite the error")

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
        namespace: String,
        sessionId: String,
        corr: Int,
        sql: String,
    ): WireMessage.SqlResult {
        val frame =
            wire.encode(
                WireMessage.SqlExec(
                    WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                    namespace = namespace,
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

        override suspend fun send(frame: ByteArray) {
            outbound.send(frame)
        }

        override fun incoming(): Flow<ByteArray> = inbound.receiveAsFlow()

        override suspend fun close() {
            inbound.close()
        }
    }
}
