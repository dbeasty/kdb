package dev.kdb.wire

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Layer 16 Component 68 (§11): the SEARCH / SEARCH_RESULT pair. Codes 0x1D/0x1E are shared with the Go
 * tree and must match exactly, or the two implementations disagree about what a frame is.
 */
class SearchWireTest {
    private val codec = defaultWireCodec()

    private fun header(correlationId: Int, type: WireMessageType): WireHeader =
        WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, correlationId, 0)

    /** Guards: the message codes are the spec's 0x1D / 0x1E. */
    @Test
    fun messageCodesMatchTheSpec() {
        assertEquals(0x1D.toShort(), WireMessageType.SEARCH.code)
        assertEquals(0x1E.toShort(), WireMessageType.SEARCH_RESULT.code)
        assertEquals(WireMessageType.SEARCH, WireMessageType.fromCode(0x1D))
        assertEquals(WireMessageType.SEARCH_RESULT, WireMessageType.fromCode(0x1E))
    }

    /** Guards: a two-arm SEARCH with every optional field round-trips through the codec unchanged. */
    @Test
    fun searchRoundTripsWithBothArms() {
        val msg =
            WireMessage.Search(
                header(3, WireMessageType.SEARCH),
                namespace = "app/tasks",
                sessionId = "sess-1",
                text = WireMessage.SearchTextArm("tasks_text", "deploy staging", depth = 100, minScore = 0.5f, weight = 2.0),
                vector = WireMessage.SearchVectorArm("embedding", listOf(0.1, 0.2, 0.3), depth = 50, minScore = null, weight = 1.0),
                fusion = "weighted",
                limit = 20,
                includeJson = true,
                atCommitHex = "ab".repeat(32),
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.Search
        assertEquals(msg, back.copy(header = msg.header))
    }

    /** Guards: a single-arm SEARCH with the optional fields absent decodes with nulls/defaults. */
    @Test
    fun searchRoundTripsWithOneArmAndDefaults() {
        val msg =
            WireMessage.Search(
                header(4, WireMessageType.SEARCH),
                namespace = "app/tasks",
                text = WireMessage.SearchTextArm("tasks_text", "deploy"),
                limit = 5,
            )
        val back = codec.decode(codec.encode(msg)) as WireMessage.Search
        // The decoded header carries the real payload length; everything else must match exactly.
        assertEquals(msg, back.copy(header = msg.header))
        assertNull(back.vector)
        assertNull(back.fusion)
        assertEquals(false, back.includeJson)
    }

    /** Guards: SEARCH_RESULT round-trips hits (with and without json) and the error fields. */
    @Test
    fun searchResultRoundTrips() {
        val ok =
            WireMessage.SearchResult(
                header(5, WireMessageType.SEARCH_RESULT),
                namespace = "app/tasks",
                hits =
                    listOf(
                        WireMessage.SearchHit("3f2a4bde-9c1d-4e8f-a6b7-c8d9e0f1a2b3", 1.5f, """{"title":"a"}"""),
                        WireMessage.SearchHit("3f2a4bde-9c1d-4e8f-a6b7-c8d9e0f1a2b4", 0.25f),
                    ),
                resolvedCommitHex = "cd".repeat(32),
            )
        assertEquals(ok, (codec.decode(codec.encode(ok)) as WireMessage.SearchResult).copy(header = ok.header))

        val err =
            WireMessage.SearchResult(
                header(6, WireMessageType.SEARCH_RESULT),
                namespace = "app/tasks",
                hits = emptyList(),
                resolvedCommitHex = "",
                error = "no FULLTEXT index for tasks_text",
                errorCode = "SCHEMA_VIOLATION",
                retryAfterMs = null,
            )
        assertEquals(err, (codec.decode(codec.encode(err)) as WireMessage.SearchResult).copy(header = err.header))
    }

    /** Guards: SqlResult's additive errorCode/retryAfterMs survive the codec (Layer 16 §9.6 relies on
     * UNIQUE_VIOLATION reaching the client). */
    @Test
    fun sqlResultCarriesErrorCode() {
        val msg =
            WireMessage.SqlResult(
                header(7, WireMessageType.SQL_RESULT),
                namespace = "app/tasks",
                sessionId = "sess-1",
                columns = emptyList(),
                rows = emptyList(),
                rowsAffected = 0,
                resolvedCommitHex = "",
                readOnly = false,
                error = "unique constraint violated",
                errorCode = "UNIQUE_VIOLATION",
            )
        assertEquals(msg, (codec.decode(codec.encode(msg)) as WireMessage.SqlResult).copy(header = msg.header))
    }
}
