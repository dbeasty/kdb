package dev.kdb.wire

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Component 40's direct-document wire ops (DocumentGet/Upsert), ported to Kotlin from
 * go/kdb/wire/document_ops.go. These four message types previously existed only on the Go side,
 * so a Go client sending one at a JVM server got an uncaught "unknown message type" that killed
 * the whole connection (recorded in docs/kdb-finish-up-plan.md's Phase 0 log).
 *
 * The message codes are the load-bearing part: 0x14-0x17 were reserved by the Go implementation
 * and MUST match, or the two implementations silently disagree about what a frame is.
 */
class DocumentOpsWireTest {
    private val codec = defaultWireCodec()

    private fun header(correlationId: Int, type: WireMessageType): WireHeader =
        WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, correlationId, 0)

    @Test
    fun messageCodesMatchTheGoImplementationsReservedValues() {
        // go/kdb/wire/types.go: MsgDocumentGet 0x14, MsgDocumentGetResult 0x15,
        // MsgUpsert 0x16, MsgUpsertResult 0x17.
        assertEquals(0x14.toShort(), WireMessageType.DOCUMENT_GET.code)
        assertEquals(0x15.toShort(), WireMessageType.DOCUMENT_GET_RESULT.code)
        assertEquals(0x16.toShort(), WireMessageType.UPSERT.code)
        assertEquals(0x17.toShort(), WireMessageType.UPSERT_RESULT.code)
        assertEquals(WireMessageType.DOCUMENT_GET, WireMessageType.fromCode(0x14))
        assertEquals(WireMessageType.UPSERT_RESULT, WireMessageType.fromCode(0x17))
    }

    @Test
    fun documentGetRoundTrips() {
        val msg =
            WireMessage.DocumentGet(
                header(7, WireMessageType.DOCUMENT_GET),
                namespace = "app/data",
                docId = "3f2a4bde9c1d4e8fa6b7c8d9e0f1a2b3",
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.DocumentGet
        assertEquals("app/data", back.namespace)
        assertEquals("3f2a4bde9c1d4e8fa6b7c8d9e0f1a2b3", back.docId)
        assertEquals(7, back.header.correlationId)
    }

    @Test
    fun documentGetResultRoundTripsIncludingTheAbsentDocumentCase() {
        val found =
            WireMessage.DocumentGetResult(
                header(8, WireMessageType.DOCUMENT_GET_RESULT),
                namespace = "app/data",
                docId = "aa".repeat(16),
                json = """{"name":"x"}""",
                commitHex = "cc".repeat(32),
            )
        val backFound = codec.decode(codec.encode(found)) as WireMessage.DocumentGetResult
        assertEquals("""{"name":"x"}""", backFound.json)
        assertEquals("cc".repeat(32), backFound.commitHex)
        assertNull(backFound.error)

        // A missing document is null json with no error - distinct from a failure, matching
        // Go's handleDocumentGet (found=false leaves JSON nil and still reports the head).
        val missing = found.copy(json = null)
        val backMissing = codec.decode(codec.encode(missing)) as WireMessage.DocumentGetResult
        assertNull(backMissing.json)
        assertNull(backMissing.error)
        assertEquals("cc".repeat(32), backMissing.commitHex)
    }

    @Test
    fun documentGetResultCarriesAnErrorString() {
        val msg =
            WireMessage.DocumentGetResult(
                header(9, WireMessageType.DOCUMENT_GET_RESULT),
                namespace = "app/data",
                docId = "bad-id",
                json = null,
                commitHex = "",
                error = "invalid docId",
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.DocumentGetResult
        assertEquals("invalid docId", back.error)
        assertNull(back.json)
    }

    @Test
    fun upsertRoundTrips() {
        val msg =
            WireMessage.Upsert(
                header(10, WireMessageType.UPSERT),
                namespace = "app/data",
                docId = "dd".repeat(16),
                json = """{"v":1}""",
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.Upsert
        assertEquals("app/data", back.namespace)
        assertEquals("dd".repeat(16), back.docId)
        assertEquals("""{"v":1}""", back.json)
    }

    @Test
    fun upsertResultRoundTripsIncludingTypedBackpressureFields() {
        val ok =
            WireMessage.UpsertResult(
                header(11, WireMessageType.UPSERT_RESULT),
                namespace = "app/data",
                commitHex = "ee".repeat(32),
            )
        val backOk = codec.decode(codec.encode(ok)) as WireMessage.UpsertResult
        assertEquals("ee".repeat(32), backOk.commitHex)
        assertNull(backOk.error)
        assertNull(backOk.errorCode)

        // errorCode/retryAfterMs are Component 51 §8.1's typed BUSY/DEADLINE_EXCEEDED signals -
        // they must survive the round trip, since a Go client reads them to decide whether and
        // when to retry.
        val busy =
            WireMessage.UpsertResult(
                header(12, WireMessageType.UPSERT_RESULT),
                namespace = "app/data",
                commitHex = "",
                error = "server busy",
                errorCode = "BUSY",
                retryAfterMs = 50,
            )
        val backBusy = codec.decode(codec.encode(busy)) as WireMessage.UpsertResult
        assertEquals("server busy", backBusy.error)
        assertEquals("BUSY", backBusy.errorCode)
        assertEquals(50, backBusy.retryAfterMs)
    }
}
