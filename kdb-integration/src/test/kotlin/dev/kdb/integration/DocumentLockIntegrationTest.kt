package dev.kdb.integration

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.embed.putJson
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.server.KdbServerRuntime
import dev.kdb.server.SqlWireHost
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.TransactionWireCodec
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

class DocumentLockIntegrationTest {
    @Test
    fun secondCommitOnSameDocFailsWhileFirstSessionHoldsLock() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            val docId = KdbUuid.fromString(putJson(runtime, ns, """{"userId":"u1","name":"Alice"}"""))

            val wire = defaultWireCodec()
            val server = KdbServerRuntime(runtime)
            val host = SqlWireHost(wire, server, ns)

            suspend fun sessionBegin(id: String, corr: Int): WireMessage.SessionBeginAck {
                val frame =
                    wire.encode(
                        WireMessage.SessionBegin(
                            WireHeader(WireMessageType.SESSION_BEGIN, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                            namespace = ns,
                            sessionId = id,
                            readConsistency = ReadConsistency.READ_COMMITTED.name,
                            baseVersionHex = null,
                        ),
                    )
                return wire.decode(host.handleFrame(frame)!!) as WireMessage.SessionBeginAck
            }

            val s1 = sessionBegin("lock-holder", 10)
            val s2 = sessionBegin("lock-waiter", 11)

            val parent = runtime.dag.head()
            val tx1 =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = parent,
                    operations = listOf(KdbOp.Write(docId, """{"userId":"u1","name":"Alice2"}""")),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            server.documentLocks.acquireAllForTransaction(ns, s1.sessionId, tx1)
            try {
            val tx2 =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = parent,
                    operations = listOf(KdbOp.Write(docId, """{"userId":"u1","name":"Bob"}""")),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val blocked =
                wire.decode(
                    host.handleFrame(
                        wire.encode(
                            WireMessage.TxCommit(
                                WireHeader(WireMessageType.TX_COMMIT, KDB_WIRE_PROTOCOL_VERSION, 2, 0),
                                namespace = ns,
                                sessionId = s2.sessionId,
                                transactionBytes = TransactionWireCodec.encode(tx2),
                            ),
                        ),
                    )!!,
                ) as WireMessage.SqlResult
            assertNotNull(blocked.error)
            assertTrue(blocked.error!!.contains("locked"))

            server.documentLocks.releaseAll(s1.sessionId)
            val ok =
                wire.decode(
                    host.handleFrame(
                        wire.encode(
                            WireMessage.TxCommit(
                                WireHeader(WireMessageType.TX_COMMIT, KDB_WIRE_PROTOCOL_VERSION, 3, 0),
                                namespace = ns,
                                sessionId = s2.sessionId,
                                transactionBytes = TransactionWireCodec.encode(tx2),
                            ),
                        ),
                    )!!,
                ) as WireMessage.SqlResult
            assertEquals(null, ok.error)
            } finally {
                server.documentLocks.releaseAll(s1.sessionId)
                server.documentLocks.releaseAll(s2.sessionId)
            }
        }

    @Test
    fun rollbackReleasesLocks() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            val docId = KdbUuid.random()
            val wire = defaultWireCodec()
            val server = KdbServerRuntime(runtime)
            val host = SqlWireHost(wire, server, ns)

            val sess =
                wire.decode(
                    host.handleFrame(
                        wire.encode(
                            WireMessage.SessionBegin(
                                WireHeader(WireMessageType.SESSION_BEGIN, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                                namespace = ns,
                                sessionId = "rollback-sess",
                                readConsistency = ReadConsistency.READ_COMMITTED.name,
                                baseVersionHex = null,
                            ),
                        ),
                    )!!,
                ) as WireMessage.SessionBeginAck

            val parent = runtime.dag.head()
            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = parent,
                    operations = listOf(KdbOp.Write(docId, """{"id":"${docId.toString()}","x":1}""")),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            server.documentLocks.acquireAllForTransaction(ns, sess.sessionId, tx)

            wire.decode(
                host.handleFrame(
                    wire.encode(
                        WireMessage.TxRollback(
                            WireHeader(WireMessageType.TX_ROLLBACK, KDB_WIRE_PROTOCOL_VERSION, 2, 0),
                            namespace = ns,
                            sessionId = sess.sessionId,
                        ),
                    ),
                )!!,
            )

            val ok =
                wire.decode(
                    host.handleFrame(
                        wire.encode(
                            WireMessage.TxCommit(
                                WireHeader(WireMessageType.TX_COMMIT, KDB_WIRE_PROTOCOL_VERSION, 3, 0),
                                namespace = ns,
                                sessionId = sess.sessionId,
                                transactionBytes = TransactionWireCodec.encode(tx),
                            ),
                        ),
                    )!!,
                ) as WireMessage.SqlResult
            assertEquals(null, ok.error)
        }

    @Test
    fun snapshotReadUnaffectedWhileLockHeld() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            putJson(runtime, ns, """{"userId":"u1","name":"Alice"}""")
            val wire = defaultWireCodec()
            val server = KdbServerRuntime(runtime)
            val host = SqlWireHost(wire, server, ns)
            val pin = runtime.dag.head().toHex()

            val snap =
                wire.decode(
                    host.handleFrame(
                        wire.encode(
                            WireMessage.SessionBegin(
                                WireHeader(WireMessageType.SESSION_BEGIN, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                                namespace = ns,
                                sessionId = "snap",
                                readConsistency = ReadConsistency.SNAPSHOT.name,
                                baseVersionHex = pin,
                            ),
                        ),
                    )!!,
                ) as WireMessage.SessionBeginAck

            val docId = KdbUuid.random()
            server.documentLocks.tryAcquire(ns, docId, "writer")
            try {
                val q =
                    wire.decode(
                        host.handleFrame(
                            wire.encode(
                                WireMessage.SqlExec(
                                    WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, 2, 0),
                                    namespace = ns,
                                    sessionId = snap.sessionId,
                                    sql = "SELECT _doc FROM users",
                                    parametersJson = null,
                                ),
                            ),
                        )!!,
                    ) as WireMessage.SqlResult
                assertEquals(null, q.error)
                assertTrue(q.rows.isNotEmpty())
            } finally {
                server.documentLocks.releaseAll("writer")
            }
        }
}
