package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.indexManager
import dev.kdb.index.memoryIndexStoreFactory
import dev.kdb.wire.TransactionWireCodec
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Component 46 (Layer 12 gap analysis §5.3), client side: [StreamConnection.submitTransaction]
 * used to send only a transaction id (nothing the coordinator could actually replay) and never
 * awaited any response - so a mode 2 write-back client always believed its write succeeded
 * immediately, even against a coordinator that had already rejected or never received it. These
 * tests drive [DefaultStreamSubscriber] against a fake coordinator (a raw [InMemoryWireTransportHub]
 * peer, not [StreamCoordinator] - it doesn't implement TransactionReplay at all) to prove the
 * fixed client actually waits for, and correctly interprets, each of the three response shapes
 * kdb-server's SqlWireHost.handleTransactionReplay can send back, plus the timeout and
 * disconnect-while-pending edge cases.
 */
class StreamSubscriberSubmitTransactionTest {
    private val wire = defaultWireCodec()

    private fun dummyTransaction(baseVersion: KdbHash): KdbTransaction =
        KdbTransaction(
            KdbUuid.random(),
            baseVersion,
            listOf(KdbOp.Write(KdbUuid.random(), """{"k":"v"}""")),
            KdbTimestamp.now(),
            KdbUuid.random(),
        )

    private suspend fun connectWriteBackSubscriber(ns: String): Pair<DefaultStreamSubscriber, StreamConnection> {
        val transport = InMemoryWireTransport()
        val dag = inMemoryCommitDag(ns)
        val subscriber =
            streamSubscriber(
                wire,
                transport,
                indexManager(memoryIndexStoreFactory(dag)),
                transactionEngine = dev.kdb.transaction.transactionEngine(dev.kdb.transaction.ConflictPolicy.STRICT),
            ) as DefaultStreamSubscriber
        val conn =
            subscriber.connect(
                StreamSubscriberConfig(
                    namespaceId = ns,
                    nodeId = "writer",
                    mode = StreamClientMode.WRITE_BACK,
                    coordinatorUri = "memory://$ns",
                    resumeFrom = dag.head(),
                ),
            )
        return subscriber to conn
    }

    /** Wires the fake coordinator's [InMemoryWireTransportHub.Hub] to hand every
     * [WireMessage.TransactionReplay] it receives to [respond], sending back whatever it returns
     * (or nothing, if it returns null). Also completes [received] with the decoded request so a
     * test can assert on what the client actually sent. */
    private suspend fun respondToNextReplay(
        hub: InMemoryWireTransportHub.Hub,
        received: CompletableDeferred<WireMessage.TransactionReplay>,
        respond: (WireMessage.TransactionReplay) -> WireMessage?,
    ) {
        hub.serverHandler = { frame ->
            val msg = wire.decode(frame)
            if (msg is WireMessage.TransactionReplay) {
                received.complete(msg)
                respond(msg)?.let { hub.serverSend(wire.encode(it)) }
            }
        }
    }

    @Test
    fun submitTransactionSendsTheActualTransactionNotJustItsId() =
        runTest {
            val ns = "app/replay-encode"
            val (_, conn) = connectWriteBackSubscriber(ns)
            val hub = InMemoryWireTransportHub.hub(ns)
            val tx = dummyTransaction(KdbHash.fromHex("00".repeat(32)))
            val received = CompletableDeferred<WireMessage.TransactionReplay>()
            respondToNextReplay(hub, received) { msg ->
                WireMessage.SqlResult(
                    WireHeader(WireMessageType.SQL_RESULT, 1, msg.header.correlationId, 0),
                    namespace = ns,
                    sessionId = "",
                    columns = emptyList(),
                    rows = emptyList(),
                    rowsAffected = 1,
                    resolvedCommitHex = KdbHash.fromHex("11".repeat(32)).toHex(),
                    readOnly = false,
                )
            }

            val result = conn.submitTransaction(tx)

            val decodedTx = TransactionWireCodec.decode(received.await().transactionBytes)
            assertEquals(tx.id, decodedTx.id, "the coordinator must receive the real transaction, not a bare id")
            assertEquals(tx.operations, decodedTx.operations)
            assertIs<ReplayResult.Applied>(result)
        }

    @Test
    fun submitTransactionAppliedOnSuccessfulSqlResult() =
        runTest {
            val ns = "app/replay-applied"
            val (_, conn) = connectWriteBackSubscriber(ns)
            val hub = InMemoryWireTransportHub.hub(ns)
            val committed = KdbHash.fromHex("22".repeat(32))
            respondToNextReplay(hub, CompletableDeferred()) { msg ->
                WireMessage.SqlResult(
                    WireHeader(WireMessageType.SQL_RESULT, 1, msg.header.correlationId, 0),
                    namespace = ns,
                    sessionId = "",
                    columns = emptyList(),
                    rows = emptyList(),
                    rowsAffected = 1,
                    resolvedCommitHex = committed.toHex(),
                    readOnly = false,
                )
            }

            val result = conn.submitTransaction(dummyTransaction(KdbHash.fromHex("00".repeat(32))))
            assertEquals(ReplayResult.Applied(committed), result)
        }

    @Test
    fun submitTransactionRejectedWhenSqlResultCarriesAnError() =
        runTest {
            val ns = "app/replay-rejected"
            val (_, conn) = connectWriteBackSubscriber(ns)
            val hub = InMemoryWireTransportHub.hub(ns)
            respondToNextReplay(hub, CompletableDeferred()) { msg ->
                WireMessage.SqlResult(
                    WireHeader(WireMessageType.SQL_RESULT, 1, msg.header.correlationId, 0),
                    namespace = ns,
                    sessionId = "",
                    columns = emptyList(),
                    rows = emptyList(),
                    rowsAffected = 0,
                    resolvedCommitHex = "",
                    readOnly = false,
                    error = "write-back replay is not enabled on this stream host",
                )
            }

            val result = conn.submitTransaction(dummyTransaction(KdbHash.fromHex("00".repeat(32))))
            assertIs<ReplayResult.Rejected>(result)
            assertTrue(result.reason.contains("not enabled"))
        }

    @Test
    fun submitTransactionConflictWhenCoordinatorReportsOne() =
        runTest {
            val ns = "app/replay-conflict"
            val (_, conn) = connectWriteBackSubscriber(ns)
            val hub = InMemoryWireTransportHub.hub(ns)
            val reportBytes =
                """{"transactionId":"tx-1","baseHash":"${"00".repeat(32)}","targetHash":"${"33".repeat(32)}"}"""
                    .encodeToByteArray()
            respondToNextReplay(hub, CompletableDeferred()) { msg ->
                WireMessage.ConflictReport(
                    WireHeader(WireMessageType.CONFLICT_REPORT, 1, msg.header.correlationId, 0),
                    namespace = ns,
                    reportBytes = reportBytes,
                )
            }

            val result = conn.submitTransaction(dummyTransaction(KdbHash.fromHex("00".repeat(32))))
            assertIs<ReplayResult.Conflict>(result)
            assertEquals("tx-1", result.report.transactionId)
        }

    @Test
    fun submitTransactionTimesOutWhenCoordinatorNeverResponds() =
        runTest {
            val ns = "app/replay-timeout"
            val (_, conn) = connectWriteBackSubscriber(ns)

            // No responder installed at all - the fake coordinator never answers, exactly the
            // pre-fix bug scenario (a coordinator that doesn't understand TransactionReplay).
            // runTest's virtual clock skips the real 10s wait instantly.
            val result = conn.submitTransaction(dummyTransaction(KdbHash.fromHex("00".repeat(32))))
            assertIs<ReplayResult.Rejected>(result)
            assertTrue(result.reason.contains("timed out"), result.reason)
        }

    @Test
    fun disconnectFailsAnyPendingSubmitTransactionInsteadOfHangingForever() =
        runTest {
            val ns = "app/replay-disconnect"
            val (subscriber, conn) = connectWriteBackSubscriber(ns)
            val hub = InMemoryWireTransportHub.hub(ns)
            val received = CompletableDeferred<WireMessage.TransactionReplay>()
            // The coordinator "receives" the request but deliberately never responds.
            respondToNextReplay(hub, received) { null }

            val tx = dummyTransaction(KdbHash.fromHex("00".repeat(32)))
            val call = async(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) { conn.submitTransaction(tx) }
            received.await()
            subscriber.disconnect()
            val result = call.await()

            assertIs<ReplayResult.Rejected>(result)
            assertTrue(result.reason.contains("disconnected"), result.reason)
        }
}
