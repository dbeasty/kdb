package dev.kdb.wire

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * binary-frames-2 and zstd frames. The hex frames are copied from
 * packages/kdb-client/test/golden/binary-frames-2.json (go/cmd/kdb-tsfixtures, the Go encoder).
 */
class BinaryWireBody2Test {
    private val json = defaultWireCodec(PayloadEncoding.JSON)
    private val all = json.withWireFeatures(SUPPORTED_WIRE_FEATURES)
    private val core = json.withWireFeatures(listOf(WIRE_FEATURE_BINARY))
    private val head = "00".repeat(32)
    private val docId = "6f9619ff-8b86-d011-b42d-00cf4fc964ff"
    private val doc = """{"name":"ada","quote":"\"q\""}"""

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(it * 2, it * 2 + 2).toInt(16).toByte() }

    private fun toHex(b: ByteArray): String = b.joinToString("") { (it.toInt() and 0xFF).toString(16).padStart(2, '0') }

    private fun hdr(type: WireMessageType, correlationId: Int, hex: String): WireHeader =
        WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, correlationId, hex.length / 2 - 12)

    private val search =
        "720000001d00010018000000020a096170702f75736572731a1f0a04626f6479120c616461206c6f76656c61636518322100" +
            "0000000000d03f22290a03656d6211000000000000e03f11000000000000f0bf11000000000000c03f290000000000000040" +
            "2a087765696768746564300a3801"
    private val searchResult =
        "dc0000001e00010018000000020a096170702f7573657273124f0a2436663936313966662d386238362d643031312d623432" +
            "642d30306366346663393634666611000000000000e83f1a1e7b226e616d65223a22616461222c2271756f7465223a225c22" +
            "715c22227d122f0a2436663936313966662d386238362d643031312d623432642d3030636634666339363466661100000000" +
            "0000e03f1a403030303030303030303030303030303030303030303030303030303030303030303030303030303030303030" +
            "3030303030303030303030303030303030303030"
    private val searchResultError =
        "270000001e00010019000000020a096170702f75736572732204627573792a044255535930fa01"
    private val sqlResultTyped =
        "86000000100001001e000000020a096170702f75736572731206736573732d311a046e616d651a016e1a01781a046e6f7465" +
            "1a03646f63222b0361646102343203312e35001e7b226e616d65223a22616461222c2271756f7465223a225c22715c22227d" +
            "221203626f62022d3704302e323500046e756c6c38016a0a00020301050002030005"
    private val sqlResultZstd =
        "57020000100001001f0000000328b52ffd641053e511000263521c401330cdea1e28b279d8dcffff5f6766a6c499a27a4388" +
            "73caf62c0e00989999999989888888887877777777676666666656555555554544444444f4ffffffdbb66ddb6ddbb6ed7c3e" +
            "9fcfe7f3f97c9e9999999989888888887877777777676666666656555555554544444444f4ffffffdbb66ddb6ddbb6ed783c" +
            "1e8fc7e3f1789c9999999989888888887877777777676666666656555555554544444444f4ffffffdbb66ddb6ddbb6edb43c" +
            "685a1e342d0f9a96074dcb83a6e541d3f2a06979d0b43c685a1e947856747732f1ace8ee60e259d1ddb9c4b3a2bb63896745" +
            "77a712cf8aee0e259e15dd9d279e15dd1d279e15dd6d11788bc013cf8aee9600271d9092a1c19040362834221e1204870422" +
            "41481e9206862432f1a091202419904849181248c58345c2a13025c049c48584c0810445c0411322114864c0209201899405" +
            "0c8c078282818fa811903758ff0fe16bc80112f8ffffa7f0074992489254928824a948925412892449249554228924492449" +
            "2a24892449244952928424c984c1603018180c061392242a494292a492249544449224922495442291249224954822491292" +
            "24154922491249d2a2a05028140a8542895492a4249248925492882449449254128924492449542289244924492a12892449" +
            "2449521a4c1feaaf7aa8b3daa1ce2a873aab1beaac6aa8b39aa1ce2a863aab17eaac5a9825924464048924c9409048263910" +
            "2492240381449264409048121908124992814042926420482449ea18fc9b2a83bc00200adc1ff78f2a5cd2ef07721522eb"

    private fun exact(): List<Pair<String, WireMessage>> =
        listOf(
            search to
                WireMessage.Search(
                    hdr(WireMessageType.SEARCH, 24, search), "app/users", null,
                    text = WireMessage.SearchTextArm("body", "ada lovelace", 50, 0.25f, null),
                    vector = WireMessage.SearchVectorArm("emb", listOf(0.5, -1.0, 0.125), null, null, 2.0),
                    fusion = "weighted", limit = 10, includeJson = true, atCommitHex = null,
                ),
            searchResult to
                WireMessage.SearchResult(
                    hdr(WireMessageType.SEARCH_RESULT, 24, searchResult), "app/users",
                    listOf(WireMessage.SearchHit(docId, 0.75f, doc), WireMessage.SearchHit(docId, 0.5f, null)), head,
                ),
            searchResultError to
                WireMessage.SearchResult(
                    hdr(WireMessageType.SEARCH_RESULT, 25, searchResultError), "app/users", emptyList(), "",
                    error = "busy", errorCode = "BUSY", retryAfterMs = 250,
                ),
            sqlResultTyped to
                WireMessage.SqlResult(
                    hdr(WireMessageType.SQL_RESULT, 30, sqlResultTyped), "app/users", "sess-1",
                    listOf("name", "n", "x", "note", "doc"),
                    listOf(listOf("ada", "42", "1.5", "", doc), listOf("bob", "-7", "0.25", "", "null")),
                    0, "", true, cellKinds = "0231502305",
                ),
        )

    @Test
    fun decodesGoFrames() {
        for ((frame, want) in exact()) {
            assertEquals(want, json.decode(hex(frame)), frame.take(40))
        }
    }

    @Test
    fun encodesGoFramesByteForByte() {
        for ((frame, msg) in exact()) {
            assertEquals(frame, toHex(all.encode(msg)))
        }
    }

    @Test
    fun searchStaysJsonUnderBinaryFramesAlone() {
        val (_, msg) = exact().first()
        assertEquals(1, core.encode(msg)[12].toInt())
        assertEquals(2, all.encode(msg)[12].toInt())
    }

    @Test
    fun decodesAGoZstdFrame() {
        val frame = hex(sqlResultZstd)
        assertEquals(3, frame[12].toInt())
        val result = json.decode(frame) as WireMessage.SqlResult
        assertEquals(400, result.rows.size)
        assertEquals(listOf("u399", """{"name":"user","tags":["a","b"],"active":true}"""), result.rows.last())
    }

    @Test
    fun compressesLargeBodiesAndReadsThemBack() {
        val rows = (0 until 400).map { listOf("u$it", """{"name":"user","tags":["a","b"],"active":true}""") }
        val msg =
            WireMessage.SqlResult(
                WireHeader(WireMessageType.SQL_RESULT, KDB_WIRE_PROTOCOL_VERSION, 3, 0), "n", "s",
                listOf("id", "doc"), rows, 0, "", true,
            )
        val frame = all.encode(msg)
        assertEquals(3, frame[12].toInt())
        assertTrue(frame.size < core.encode(msg).size / 4)
        assertEquals(rows, (json.decode(frame) as WireMessage.SqlResult).rows)
        // A small body is not worth compressing.
        assertEquals(2, all.encode(msg.copy(rows = rows.take(2)))[12].toInt())
    }
}
