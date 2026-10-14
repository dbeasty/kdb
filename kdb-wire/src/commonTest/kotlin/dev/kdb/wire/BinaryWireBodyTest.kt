package dev.kdb.wire

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Binary frames (encoding tag 2). The hex frames below are copied from
 * packages/kdb-client/test/golden/binary-frames.json, which go/cmd/kdb-tsfixtures writes with the
 * Go encoder: Kotlin must decode them to the same message and encode that message back to the
 * same bytes.
 */
class BinaryWireBodyTest {
    private val json = defaultWireCodec(PayloadEncoding.JSON)
    private val binary = json.withWireFeatures(listOf(WIRE_FEATURE_BINARY))

    private val head = "00".repeat(32)
    private val doc = """{"name":"ada","quote":"\"q\"","unicode":"é☃","nl":"a\nb"}"""
    private val docId = "6f9619ff-8b86-d011-b42d-00cf4fc964ff"

    private fun hdr(type: WireMessageType, correlationId: Int, hex: String): WireHeader =
        WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, correlationId, hex.length / 2 - 12)

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(it * 2, it * 2 + 2).toInt(16).toByte() }

    private fun toHex(b: ByteArray): String = b.joinToString("") { (it.toInt() and 0xFF).toString(16).padStart(2, '0') }

    // Fixtures every field of which Kotlin's messages carry: decode and re-encode byte for byte.
    private val sessionBeginAt =
        "670000000e00010004000000020a096170702f75736572731201731a08534e415053484f54224030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030"
    private val sqlExec =
        "4a0000000f00010005000000020a096170702f75736572731206736573732d311a2053454c454354202a2046524f4d207573657273205748455245206964203d203f22065b227531225d"
    private val sqlResultError =
        "380000001000010007000000020a096170702f75736572731206736573732d31420d6e6f2073756368207461626c6552044255535958fa01"
    private val txCommit =
        "2c0000001100010008000000020a096170702f75736572731206736573732d311a0a7b226964223a2278227d"
    private val conflictReport =
        "360000000700010008000000020a096170702f757365727312107b22636f6e666c69637473223a5b5d7d1a08434f4e464c4943542000"
    private val documentGetResultFound =
        "be000000150001000b000000020a096170702f7573657273122436663936313966662d386238362d643031312d623432642d3030636634666339363466661a3c7b226e616d65223a22616461222c2271756f7465223a225c22715c22222c22756e69636f6465223a22c3a9e29883222c226e6c223a22615c6e62227d224030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030"
    private val documentGetResultMissing =
        "80000000150001000d000000020a096170702f7573657273122436663936313966662d386238362d643031312d623432642d303063663466633936346666224030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030"

    // Fixtures carrying Go-only fields (a continuation token, an upsert's session id): decoded with
    // those fields skipped.
    private val sqlResultRows =
        "c80000001000010005000000020a096170702f75736572731206736573732d311a0269641a046e616d651a03646f632244027531036164613c7b226e616d65223a22616461222c2271756f7465223a225c22715c22222c22756e69636f6465223a22c3a9e29883222c226e6c223a22615c6e62227d22070275320002c3a9324030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030380162046e657874"
    private val upsert =
        "84000000160001000f000000020a096170702f7573657273122436663936313966662d386238362d643031312d623432642d3030636634666339363466661a3c7b226e616d65223a22616461222c2271756f7465223a225c22715c22222c22756e69636f6465223a22c3a9e29883222c226e6c223a22615c6e62227d2206736573732d31"

    private fun exact(): List<Pair<String, WireMessage>> =
        listOf(
            sessionBeginAt to
                WireMessage.SessionBegin(hdr(WireMessageType.SESSION_BEGIN, 4, sessionBeginAt), "app/users", "s", "SNAPSHOT", head),
            sqlExec to
                WireMessage.SqlExec(
                    hdr(WireMessageType.SQL_EXEC, 5, sqlExec), "app/users", "sess-1", "SELECT * FROM users WHERE id = ?", """["u1"]""",
                ),
            sqlResultError to
                WireMessage.SqlResult(
                    hdr(WireMessageType.SQL_RESULT, 7, sqlResultError), "app/users", "sess-1", emptyList(), emptyList(), 0, "", false,
                    error = "no such table", errorCode = "BUSY", retryAfterMs = 250,
                ),
            txCommit to
                WireMessage.TxCommit(hdr(WireMessageType.TX_COMMIT, 8, txCommit), "app/users", "sess-1", """{"id":"x"}""".encodeToByteArray()),
            conflictReport to
                WireMessage.ConflictReport(
                    hdr(WireMessageType.CONFLICT_REPORT, 8, conflictReport), "app/users", """{"conflicts":[]}""".encodeToByteArray(),
                    errorCode = "CONFLICT", retryAfterMs = 0,
                ),
            documentGetResultFound to
                WireMessage.DocumentGetResult(hdr(WireMessageType.DOCUMENT_GET_RESULT, 11, documentGetResultFound), "app/users", docId, doc, head),
            documentGetResultMissing to
                WireMessage.DocumentGetResult(hdr(WireMessageType.DOCUMENT_GET_RESULT, 13, documentGetResultMissing), "app/users", docId, null, head),
        )

    @Test
    fun decodesGoBinaryFrames() {
        for ((frame, want) in exact()) {
            assertEquals(want, json.decode(hex(frame)), frame)
        }
    }

    @Test
    fun encodesGoBinaryFramesByteForByte() {
        for ((frame, msg) in exact()) {
            assertEquals(frame, toHex(binary.encode(msg)))
        }
    }

    @Test
    fun skipsGoOnlyFields() {
        val result = json.decode(hex(sqlResultRows)) as WireMessage.SqlResult
        assertEquals(listOf("id", "name", "doc"), result.columns)
        assertEquals(listOf(listOf("u1", "ada", doc), listOf("u2", "", "é")), result.rows)
        assertTrue(result.readOnly)
        assertEquals(head, result.resolvedCommitHex)
        val up = json.decode(hex(upsert)) as WireMessage.Upsert
        assertEquals(doc, up.json)
        assertEquals(docId, up.docId)
    }

    @Test
    fun jsonCodecNeverWritesBinaryAndBinaryKeepsJsonForOtherTypes() {
        val (_, msg) = exact().first()
        assertEquals(1, json.encode(msg)[12].toInt())
        val ack =
            WireMessage.HandshakeAck(
                WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                HandshakeAckPayload(true, PayloadEncoding.JSON, 1, wireFeatures = listOf(WIRE_FEATURE_BINARY)),
            )
        val frame = binary.encode(ack)
        assertEquals(1, frame[12].toInt())
        assertEquals(listOf(WIRE_FEATURE_BINARY), (binary.decode(frame) as WireMessage.HandshakeAck).response.wireFeatures)
    }

    @Test
    fun handshakeWithoutFeaturesIsUnchanged() {
        val hs =
            WireMessage.Handshake(
                WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                HandshakePayload("n", emptyList(), emptyMap(), clientMode = WireClientMode.SQL_CLIENT),
            )
        assertTrue("wireFeatures" !in json.encode(hs).decodeToString())
        val offered = hs.copy(request = hs.request.copy(wireFeatures = SUPPORTED_WIRE_FEATURES))
        assertEquals(SUPPORTED_WIRE_FEATURES, (json.decode(json.encode(offered)) as WireMessage.Handshake).request.wireFeatures)
        assertEquals(listOf(WIRE_FEATURE_BINARY, WIRE_FEATURE_ZSTD), negotiateWireFeatures(listOf("zstd-frames", WIRE_FEATURE_BINARY, "x")))
        assertEquals(emptyList(), negotiateWireFeatures(listOf("zstd-frames")))
    }

    @Test
    fun rejectsDamagedBodiesWithADecodeError() {
        for ((frame, _) in exact()) {
            val bytes = hex(frame)
            for (n in 13 until bytes.size) {
                val cut = bytes.copyOf(n)
                cut[0] = (n and 0xFF).toByte()
                cut[1] = ((n shr 8) and 0xFF).toByte()
                try {
                    json.decode(cut)
                } catch (e: WireDecodeException) {
                    // expected for most cuts
                }
            }
        }
        val bad = hex(txCommit)
        bad[13] = 0x08 // namespace (field 1) as a varint
        assertFailsWith<WireDecodeException> { json.decode(bad) }
    }
}
