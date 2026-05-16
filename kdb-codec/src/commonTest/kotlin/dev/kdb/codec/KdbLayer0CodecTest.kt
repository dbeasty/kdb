package dev.kdb.codec

import dev.kdb.codec.schema.FieldSchema
import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import dev.kdb.codec.schema.LogicalAnnotation
import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.codec.schema.RecordSchema
import dev.kdb.error.KdbDecodeException
import kotlinx.datetime.Instant
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class KdbLayer0CodecTest {

    private val jsonCmp =
        Json {
            ignoreUnknownKeys = true
        }

    private fun rr(reg: KdbTypeRegistry, v: KdbValue, t: KdbType): KdbValue =
        KdbValue.decodeFromBytes(v.encodeToBytes(t, reg), t, reg)

    @Test
    fun roundtrip_null() {
        val reg = KdbTypeRegistry.builtin()
        assertEquals(KdbValue.Null, rr(reg, KdbValue.Null, KdbType.Primitive(PhysicalKind.NULL)))
    }

    @Test
    fun roundtrip_core_primitives() {
        val reg = KdbTypeRegistry.builtin()
        assertEquals(KdbValue.Int32Val(Int.MIN_VALUE), rr(reg, KdbValue.Int32Val(Int.MIN_VALUE), P.INT32))
        assertEquals(KdbValue.Int64Val(Long.MAX_VALUE), rr(reg, KdbValue.Int64Val(Long.MAX_VALUE), P.INT64))
        assertEquals(KdbValue.Float64Val(-1.5), rr(reg, KdbValue.Float64Val(-1.5), P.FLOAT64))
        assertEquals(KdbValue.Bool(true), rr(reg, KdbValue.Bool(true), P.BOOL))
        assertEquals(KdbValue.StringVal("\u3030 KDB"), rr(reg, KdbValue.StringVal("\u3030 KDB"), P.STR))
    }

    @Test
    fun nested_record_array_roundtrip() {
        val inner =
            RecordSchema(
                name = "Inner",
                namespace = "t",
                fields = listOf(FieldSchema(1, "x", P.INT32)),
            )
        val root =
            RecordSchema(
                name = "Root",
                namespace = "t",
                fields = listOf(FieldSchema(2, "a", KdbType.Array(KdbType.Ref("t.Inner")))),
            )

        val reg = KdbTypeRegistry.create()
        reg.registerRecord(inner)
        reg.registerRecord(root)
        reg.freeze()

        val innerVal = KdbValue.RecordVal(mapOf(1 to KdbValue.Int32Val(7)))
        val outerVal =
            KdbValue.RecordVal(
                mapOf(
                    2 to KdbValue.ArrayVal(listOf(innerVal)),
                ),
            )
        assertEquals(outerVal, rr(reg, outerVal, KdbType.Ref("t.Root")))
    }

    @Test
    fun uuid_hash_timestamp_helpers() {
        val u = KdbUuid.random()
        assertEquals(u, u.toKdbValue().decode<KdbUuid>())

        val h = KdbHash(ByteArray(32) { (it xor 0x17).toByte() })
        assertContentEquals(h.bytes, h.toKdbValue().decode<KdbHash>().bytes)

        val kt = KdbTimestamp(epochMillis = 555L, microRemainder = 0)
        assertEquals(kt, kt.toKdbValue().decode<KdbTimestamp>())
    }

    @Test
    fun encoded_size_equals_bytes() {
        val reg = KdbTypeRegistry.create()
        val rec =
            RecordSchema(
                name = "Wide",
                namespace = "t",
                fields =
                    (0 until 50).map { i ->
                        FieldSchema(id = i + 1, name = "f_$i", P.STR)
                    },
            )
        reg.registerRecord(rec)
        reg.freeze()

        val fields =
            (0 until 50).associate { i ->
                (i + 1) to KdbValue.StringVal("payload_${i}_${"x".repeat(20)}")
            }
        val rv = KdbValue.RecordVal(fields)
        val t = KdbType.Ref("t.Wide")
        val blob = rv.encodeToBytes(t, reg)
        assertEquals(blob.size, rv.encodedSize(t, reg))
    }

    @Test
    fun json_flat_object_roundtrip() {
        val raw = """{"a":1,"b":"hello","c":true,"d":null}"""
        val reg = mkDoc(listOf(
            Pair("a", P.INT32),
            Pair("b", P.STR),
            Pair("c", P.BOOL),
            Pair("d", KdbType.Nullable(P.INT32)),
        ))
        val t = KdbType.Ref("demo.Doc")
        val v = KdbValue.fromJson(raw, t, reg)
        val back = v.toJson(t, reg)
        assertEquals(jsonCmp.parseToJsonElement(raw), jsonCmp.parseToJsonElement(back))
    }

    @Test
    fun date_json_spec() {
        val reg = KdbTypeRegistry.builtin()
        val t = KdbType.Primitive(PhysicalKind.INT32, LogicalAnnotation.Date)
        val v = KdbValue.fromJson("\"2024-01-15\"", t, reg) as KdbValue.DateVal
        assertEquals(19737, v.daysSinceEpoch)
        assertEquals("\"2024-01-15\"", v.toJson(t, reg))
    }

    @Test
    fun timestamp_micro_json_frac() {
        val reg = KdbTypeRegistry.builtin()
        val t = KdbType.Primitive(PhysicalKind.INT64, LogicalAnnotation.TimestampMicros(null))
        val rawIso = "\"2024-01-15T12:00:00.000123Z\""
        val decoded = KdbValue.fromJson(rawIso, t, reg) as KdbValue.TimestampVal
        assertTrue(decoded.epochMicros > 0L)
        val back = decoded.toJson(t, reg)
        val prim = jsonCmp.parseToJsonElement(back) as JsonPrimitive
        val inst = Instant.parse(prim.content)
        val microBack = inst.epochSeconds * 1_000_000L + inst.nanosecondsOfSecond / 1000
        assertEquals(decoded.epochMicros, microBack)
    }

    @Test
    fun truncated_payload_throws() {
        val reg = mkDoc(listOf(Pair("n", P.INT32)))
        val t = KdbType.Ref("demo.Doc")
        val blob = KdbValue.fromJson("""{"n":-3}""", t, reg).encodeToBytes(t, reg)
        assertTrue(blob.size > 2)
        val bad = blob.copyOf(blob.size - 1)
        assertFailsWith<KdbDecodeException> {
            KdbValue.decodeFromBytes(bad, t, reg)
        }
    }

    @Test
    fun source_reads_first_value_boundary() {
        val reg = mkDoc(listOf(Pair("n", P.INT32)))
        val t = KdbType.Ref("demo.Doc")

        fun blob(nv: Int) = KdbValue.fromJson("""{"n":$nv}""", t, reg).encodeToBytes(t, reg)

        val a = blob(-3)
        val b = blob(999)
        val sink = Buffer()
        sink.write(a, 0, a.size)
        sink.write(b, 0, b.size)

        val first = KdbValue.decodeFrom(sink, t, reg)
        assertEquals(KdbValue.fromJson("""{"n":-3}""", t, reg), first)
        val tail = sink.readByteArray()
        assertContentEquals(b, tail)
        assertEquals(KdbValue.fromJson("""{"n":999}""", t, reg), KdbValue.decodeFromBytes(tail, t, reg))
    }
}

private object P {
    val INT32 = KdbType.Primitive(PhysicalKind.INT32)
    val INT64 = KdbType.Primitive(PhysicalKind.INT64)
    val FLOAT64 = KdbType.Primitive(PhysicalKind.FLOAT64)
    val BOOL = KdbType.Primitive(PhysicalKind.BOOLEAN)
    val STR = KdbType.Primitive(PhysicalKind.STRING)
}

private fun mkDoc(props: List<Pair<String, KdbType>>): KdbTypeRegistry {
    val reg = KdbTypeRegistry.create()
    val fields =
        props.mapIndexed { idx, pair ->
            FieldSchema(idx + 1, pair.first, pair.second)
        }
    reg.registerRecord(RecordSchema(name = "Doc", namespace = "demo", fields = fields))
    reg.freeze()
    return reg
}
