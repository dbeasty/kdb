package dev.kdb.integration

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
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class MultiClientSqlIntegrationTest {
    @Test
    fun twoSessions_snapshotRead_sameData() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            putJson(runtime, ns, """{"userId":"u1","name":"Alice"}""")
            val wire = defaultWireCodec()
            val host = SqlWireHost(wire, KdbServerRuntime(runtime), ns)

            val hsFrame =
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
                )
            val hsAck = wire.decode(host.handleFrame(hsFrame)!!)
            assertTrue(hsAck is WireMessage.HandshakeAck)

            val s1 =
                wire.decode(
                    host.handleFrame(
                        wire.encode(
                            WireMessage.SessionBegin(
                                WireHeader(WireMessageType.SESSION_BEGIN, KDB_WIRE_PROTOCOL_VERSION, 2, 0),
                                namespace = ns,
                                sessionId = "client-1",
                                readConsistency = ReadConsistency.SNAPSHOT.name,
                                baseVersionHex = null,
                            ),
                        ),
                    )!!,
                ) as WireMessage.SessionBeginAck

            val s2 =
                wire.decode(
                    host.handleFrame(
                        wire.encode(
                            WireMessage.SessionBegin(
                                WireHeader(WireMessageType.SESSION_BEGIN, KDB_WIRE_PROTOCOL_VERSION, 3, 0),
                                namespace = ns,
                                sessionId = "client-2",
                                readConsistency = ReadConsistency.SNAPSHOT.name,
                                baseVersionHex = null,
                            ),
                        ),
                    )!!,
                ) as WireMessage.SessionBeginAck

            val q1 =
                wire.decode(
                    host.handleFrame(
                        wire.encode(
                            WireMessage.SqlExec(
                                WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, 4, 0),
                                namespace = ns,
                                sessionId = s1.sessionId,
                                sql = "SELECT _doc FROM users",
                                parametersJson = null,
                            ),
                        ),
                    )!!,
                ) as WireMessage.SqlResult

            val q2 =
                wire.decode(
                    host.handleFrame(
                        wire.encode(
                            WireMessage.SqlExec(
                                WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, 5, 0),
                                namespace = ns,
                                sessionId = s2.sessionId,
                                sql = "SELECT _doc FROM users",
                                parametersJson = null,
                            ),
                        ),
                    )!!,
                ) as WireMessage.SqlResult

            assertTrue(q1.rows.isNotEmpty())
            assertTrue(q2.rows.isNotEmpty())
            assertEquals(q1.rows.single().single(), q2.rows.single().single())
        }
}
