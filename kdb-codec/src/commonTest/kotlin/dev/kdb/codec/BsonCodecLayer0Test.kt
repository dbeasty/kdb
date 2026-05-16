package dev.kdb.codec

import dev.kdb.error.BsonDecodeException
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.io.Buffer
import kotlinx.serialization.json.Json
import kotlinx.io.readByteArray

class BsonCodecLayer0Test {
    private val compareJson = Json { ignoreUnknownKeys = true }

    @Test
    fun roundtrip_empty_document() {
        val d = BsonDocument()
        assertEquals(d, BsonDocument.fromBytes(d.toBytes()))
    }

    @Test
    fun roundtrip_all_scalar_types() {
        val d =
            BsonDocument(
                LinkedHashMap<String, BsonValue>().apply {
                    put("s", BsonString("\u3030 BSON"))
                    put("i32", BsonInt32(Int.MIN_VALUE))
                    put("i64", BsonInt64(Long.MAX_VALUE))
                    put("dbl", BsonDouble(-1.5))
                    put("tf", BsonBoolean(true))
                    put("utc", BsonDateTime(-5L))
                    put(
                        "bin",
                        BsonBinary(
                            subtype = 0,
                            byteArrayOf(1, 2, 3, -1),
                        ),
                    )
                    put("n", BsonNull)
                },
            )
        assertEquals(d, BsonDocument.fromBytes(d.toBytes()))
    }

    @Test
    fun roundtrip_nested_document_and_array() {
        val inner = BsonDocument()
        inner["x"] = BsonInt32(7)
        val arr = BsonArray(mutableListOf(BsonBoolean(false), inner))
        val root = BsonDocument()
        root["sub"] = BsonDocument(LinkedHashMap<String, BsonValue>().apply { put("a", arr) })
        assertEquals(root, BsonDocument.fromBytes(root.toBytes()))
    }

    @Test
    fun uuid_codec_roundtrip_subtype_and_length() {
        val u = KdbUuid.random()
        val b = u.toBsonBinary()
        assertEquals(BsonBinarySubtype.UUID, b.subtype)
        assertEquals(16, b.data.size)
        assertEquals(u, b.toKdbUuid())
    }

    @Test
    fun hash_codec_roundtrip_subtype_and_length() {
        val raw = ByteArray(32) { it.toByte() }
        val h = KdbHash(raw)
        val b = h.toBsonBinary()
        assertEquals(BsonBinarySubtype.GENERIC, b.subtype)
        assertEquals(32, b.data.size)
        assertEquals(h.bytes.toList(), b.toKdbHash().bytes.toList())
    }

    @Test
    fun timestamp_microsecond_precision_embedded_roundtrip() {
        val ts = KdbTimestamp(epochMillis = 1_700_000_000_000L, microRemainder = 987)
        val doc = ts.toEmbeddedUtcWithMicroseconds()
        assertEquals(ts, doc.toKdbTimestampFromEmbeddedUtc())
    }

    @Test
    fun encoded_size_matches_to_bytes_large_doc() {
        val d =
            buildDocument {
                repeat(50) { i ->
                    put("field_$i", BsonString("payload_${i}_${"x".repeat(20)}"))
                }
            }
        assertEquals(d.encodedSize(), d.toBytes().size)
    }

    @Test
    fun json_bson_json_semantic_roundtrip() {
        val raw = """{"a":1,"b":"hello","c":true,"d":null,"e":[1,2,3]}"""
        val back = BsonDocument.fromJson(raw).toJson()
        assertEquals(compareJson.parseToJsonElement(raw), compareJson.parseToJsonElement(back))
    }

    @Test
    fun large_document_strings_encode_decode() {
        val d =
            buildDocument {
                repeat(10_000) { i ->
                    put("k_$i", BsonString("v".repeat(100)))
                }
            }
        val bytes = d.toBytes()
        assertFalse(bytes.isEmpty())
        assertEquals(d, BsonDocument.fromBytes(bytes))
    }

    @Test
    fun truncated_bytes_throws_bson_decode_exception() {
        val full = sampleDoc().toBytes()
        assertTrue(full.size > 5)
        val bad = full.copyOf(full.size - 1)
        val ex = assertFailsWith<BsonDecodeException> { BsonDocument.fromBytes(bad) }
        assertTrue(ex.message!!.isNotBlank())
        assertTrue(ex.offset >= 0)
    }

    @Test
    fun unknown_type_byte_throws() {
        val good = sampleDoc().toBytes()
        val hacked = good.copyOf()
        assertEquals(BsonType.INT32.byte, hacked[4])
        hacked[4] = (0xFF).toByte()
        val ex = assertFailsWith<BsonDecodeException> { BsonDocument.fromBytes(hacked) }
        assertTrue(ex.message!!.contains("unsupported BSON type"))
    }

    @Test
    fun from_source_reads_exact_one_document_boundary() {
        val a = sampleDoc().toBytes()
        val b =
            buildDocument {
                put("zzz", BsonString("two"))
            }.toBytes()
        val sink = Buffer()
        sink.write(a, startIndex = 0, endIndex = a.size)
        sink.write(b, startIndex = 0, endIndex = b.size)
        assertEquals(sampleDoc(), BsonDocument.fromSource(sink))
        assertContentEquals(b, sink.readByteArray(b.size))    }

    @Test
    fun registry_primitive_codecs() {
        val u = KdbUuid.random()
        assertEquals(u, u.toBsonValue().decode<KdbUuid>())
        val hs = ByteArray(32) { (it xor 42).toByte() }
        val h = KdbHash(hs)
        assertContentEquals(h.bytes, h.toBsonValue().decode<KdbHash>().bytes)
        val kt = KdbTimestamp(555L, microRemainder = 0)
        assertEquals(kt, kt.toBsonValue().decode<KdbTimestamp>())
    }
}

private fun sampleDoc(): BsonDocument =
    buildDocument {
        put("n", BsonInt32(-3))
    }

private inline fun buildDocument(block: LinkedHashMap<String, BsonValue>.() -> Unit): BsonDocument =
    BsonDocument(LinkedHashMap<String, BsonValue>().apply(block))
