package dev.kdb.integration

import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.static.StaticAuthConfig
import dev.kdb.auth.static.StaticUserConfig
import dev.kdb.auth.static.staticAuthEngine
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.embed.putJson
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.server.KdbServerRuntime
import dev.kdb.server.SqlWireHost
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireCapabilitySet
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.WireCodec
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class AuthSqlIntegrationTest {
    private val ns = "demo/users"
    private val authEngine =
        staticAuthEngine(
            StaticAuthConfig(
                users =
                    mapOf(
                        "reader" to StaticUserConfig(secret = "r-secret", roles = listOf("reader")),
                        "writer" to StaticUserConfig(secret = "w-secret", roles = listOf("writer")),
                    ),
                roles =
                    mapOf(
                        "reader" to listOf("read:demo/*"),
                        "writer" to listOf("read:demo/*", "write:demo/*"),
                    ),
            ),
        )

    @Test
    fun handshake_withoutCredentials_rejected() =
        runTest {
            val host = sqlHost(ConnectionContext.EMPTY, openServer())
            val ack = doHandshake(host)
            assertFalse(ack.response.accepted)
            assertNotNull(ack.response.rejectionReason)
        }

    @Test
    fun reader_canSelect() =
        runTest {
            val server = openServer()
            putJson(server.runtime, ns, """{"userId":"u1","name":"Alice"}""")
            val host = sqlHost(ConnectionContext(user = "reader", password = "r-secret"), server = server)
            assertTrue(doHandshake(host).response.accepted)
            val begin = doSessionBegin(host, "client-reader")
            assertTrue(begin.headHex.isNotEmpty())
            val result = doSqlExec(host, begin.sessionId, "SELECT _doc FROM users")
            assertNull(result.error)
            assertTrue(result.rows.isNotEmpty())
        }

    @Test
    fun reader_insertForbidden() =
        runTest {
            val host = sqlHost(ConnectionContext(user = "reader", password = "r-secret"))
            assertTrue(doHandshake(host).response.accepted)
            val begin = doSessionBegin(host, "client-reader-2")
            val result =
                doSqlExec(
                    host,
                    begin.sessionId,
                    "INSERT INTO users (userId, name) VALUES ('u2', 'Bob')",
                )
            assertNotNull(result.error)
            assertTrue(result.error!!.contains("forbidden", ignoreCase = true))
        }

    private suspend fun openServer(): KdbServerRuntime {
        val runtime = openMemoryRuntime("demo", ns)
        return KdbServerRuntime(runtime)
    }

    private suspend fun sqlHost(
        ctx: ConnectionContext,
        server: KdbServerRuntime? = null,
    ): SqlWireHost =
        SqlWireHost(
            wire = defaultWireCodec(),
            server = server ?: openServer(),
            defaultNamespace = ns,
            auth = authEngine,
            connectionContext = ctx,
        )

    private suspend fun doHandshake(host: SqlWireHost): WireMessage.HandshakeAck {
        val wire = defaultWireCodec()
        return wire.decode(
            host.handleFrame(
                wire.encode(
                    WireMessage.Handshake(
                        WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                        HandshakePayload(
                            nodeId = "c1",
                            namespaces = listOf(ns),
                            localHeads = emptyMap(),
                            capabilities = WireCapabilitySet(),
                            clientMode = WireClientMode.SQL_CLIENT,
                        ),
                    ),
                ),
            )!!,
        ) as WireMessage.HandshakeAck
    }

    private suspend fun doSessionBegin(
        host: SqlWireHost,
        sessionId: String,
    ): WireMessage.SessionBeginAck {
        val wire = defaultWireCodec()
        return wire.decode(
            host.handleFrame(
                wire.encode(
                    WireMessage.SessionBegin(
                        WireHeader(WireMessageType.SESSION_BEGIN, KDB_WIRE_PROTOCOL_VERSION, 2, 0),
                        namespace = ns,
                        sessionId = sessionId,
                        readConsistency = ReadConsistency.READ_COMMITTED.name,
                        baseVersionHex = null,
                    ),
                ),
            )!!,
        ) as WireMessage.SessionBeginAck
    }

    private suspend fun doSqlExec(
        host: SqlWireHost,
        sessionId: String,
        sql: String,
    ): WireMessage.SqlResult {
        val wire = defaultWireCodec()
        return wire.decode(
            host.handleFrame(
                wire.encode(
                    WireMessage.SqlExec(
                        WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, 3, 0),
                        namespace = ns,
                        sessionId = sessionId,
                        sql = sql,
                        parametersJson = null,
                    ),
                ),
            )!!,
        ) as WireMessage.SqlResult
    }
}
