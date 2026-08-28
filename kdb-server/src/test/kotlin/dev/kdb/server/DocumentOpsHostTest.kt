package dev.kdb.server

import dev.kdb.auth.ConnectionContext
import dev.kdb.codec.KdbUuid
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.schema.KdbSchema
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
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Component 40's direct-document ops (DocumentGet/Upsert) against the real Kotlin wire host.
 *
 * These four message types (codes 0x14-0x17) existed only in the Go implementation until now: a
 * Go client sending one at a JVM server hit `decodeHeader`'s "unknown message type", which was
 * uncaught and killed the entire connection - the gap recorded in docs/kdb-finish-up-plan.md's
 * Phase 0 log, which forced go/kdb/client/jvm_ws_interop_test.go to verify JVM-side writes via
 * SqlExec's `_doc` column instead of GetJSON. Semantics here mirror go/kdb/server/wire_listen.go's
 * handleDocumentGet/handleUpsert exactly.
 */
class DocumentOpsHostTest {
    private val wire = defaultWireCodec()
    private val ns = "demo/docops"

    @Test
    fun upsertCreatesThenReplaces_andDocumentGetReadsItBack() =
        runTest {
            val (conn, loop) = startHost()
            val docId = KdbUuid.random().toString()

            // Create: Upsert never needs a BaseVersion and never conflicts (spec §5).
            val created = upsert(conn, docId, """{"v":1}""", corr = 1)
            assertNull(created.error, "upsert failed: ${created.error}")
            assertTrue(created.commitHex.isNotEmpty(), "upsert returned no commit hash")

            val read = documentGet(conn, docId, corr = 2)
            assertNull(read.error)
            assertEquals("""{"v":1}""", read.json)
            assertEquals(created.commitHex, read.commitHex, "read should be at the head the upsert produced")

            // Replace: same doc id, no base version, must succeed rather than conflict.
            val replaced = upsert(conn, docId, """{"v":2}""", corr = 3)
            assertNull(replaced.error, "second upsert conflicted: ${replaced.error}")
            assertEquals("""{"v":2}""", documentGet(conn, docId, corr = 4).json)

            conn.close()
            loop.join()
        }

    @Test
    fun documentGetOnAnAbsentDocumentReturnsNullJsonNotAnError() =
        runTest {
            val (conn, loop) = startHost()
            // Matches Go's handleDocumentGet: "not found" is json=null WITH the current head,
            // deliberately distinct from a failure (which sets error).
            val result = documentGet(conn, KdbUuid.random().toString(), corr = 1)
            assertNull(result.error)
            assertNull(result.json)
            assertTrue(result.commitHex.isNotEmpty(), "an absent document should still report the head")

            conn.close()
            loop.join()
        }

    @Test
    fun malformedDocumentIdIsRejectedWithAnErrorNotAThrownException() =
        runTest {
            val (conn, loop) = startHost()

            val get = documentGet(conn, "not-a-uuid", corr = 1)
            assertNotNull(get.error, "a malformed docId must come back as an error frame")
            assertTrue(get.error!!.contains("invalid docId"), "unexpected error: ${get.error}")

            val put = upsert(conn, "also-not-a-uuid", """{"v":1}""", corr = 2)
            assertNotNull(put.error)
            assertTrue(put.error!!.contains("invalid docId"), "unexpected error: ${put.error}")

            // And the connection survives both - a bad id must not tear the session down.
            val ok = upsert(conn, KdbUuid.random().toString(), """{"v":3}""", corr = 3)
            assertNull(ok.error)

            conn.close()
            loop.join()
        }

    private suspend fun kotlinx.coroutines.CoroutineScope.startHost(): Pair<FakeWireConnection, kotlinx.coroutines.Job> {
        val runtime = openMemoryRuntime("demo", ns, KdbSchema.NONE)
        val server = KdbServerRuntime(runtime)
        val host = sqlWireHostFactory(wire, server, ns)(ConnectionContext.EMPTY)
        val conn = FakeWireConnection()
        val loop = launch { pipelinedPerConnection(conn, host) }
        return conn to loop
    }

    private suspend fun upsert(
        conn: FakeWireConnection,
        docId: String,
        json: String,
        corr: Int,
    ): WireMessage.UpsertResult {
        val frame =
            wire.encode(
                WireMessage.Upsert(
                    WireHeader(WireMessageType.UPSERT, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                    namespace = ns,
                    docId = docId,
                    json = json,
                ),
            )
        return wire.decode(conn.roundTrip(frame)) as WireMessage.UpsertResult
    }

    private suspend fun documentGet(
        conn: FakeWireConnection,
        docId: String,
        corr: Int,
    ): WireMessage.DocumentGetResult {
        val frame =
            wire.encode(
                WireMessage.DocumentGet(
                    WireHeader(WireMessageType.DOCUMENT_GET, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                    namespace = ns,
                    docId = docId,
                ),
            )
        return wire.decode(conn.roundTrip(frame)) as WireMessage.DocumentGetResult
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
