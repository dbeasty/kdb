package dev.kdb.codec.internal

import dev.kdb.codec.KdbValue
import dev.kdb.codec.schema.EnumSchema
import dev.kdb.codec.schema.FixedSchema
import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import dev.kdb.codec.schema.LogicalAnnotation
import dev.kdb.codec.schema.LogicalTypeRegistry
import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.codec.schema.RecordSchema
import dev.kdb.error.KdbDecodeException
import dev.kdb.error.KdbEncodeException
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.io.readByteArray

/**
 * Layer-0 wire codec (package-private implementation entry).
 */
object WireCodec {

    fun encode(value: KdbValue, type: KdbType, reg: KdbTypeRegistry): ByteArray =
        Buffer().also { encodeInto(it, value, type, reg) }.drain()

    fun decode(bytes: ByteArray, type: KdbType, reg: KdbTypeRegistry): KdbValue {
        val p = Pos(0)
        val v = decodeValue(BytesCursor(bytes, p, bytes.size), type, reg)
        if (p.i != bytes.size) throw KdbDecodeException("trailing bytes", p.mark())
        return v
    }

    fun decodeFrom(source: Source, type: KdbType, reg: KdbTypeRegistry): KdbValue =
        decodeValue(SourceCursor(SourcePull(source)), type, reg)

    fun encodedSize(value: KdbValue, type: KdbType, reg: KdbTypeRegistry): Int =
        encode(value, type, reg).size

    internal fun encodeInto(out: Buffer, value: KdbValue, type: KdbType, reg: KdbTypeRegistry) {
        checkMatches(type, value, reg)
        encodeValue(out, value, type, reg)
    }

    private fun Buffer.drain(): ByteArray = readByteArray()

    private interface Cursor {
        fun mark(): Int
        fun u8(): Byte
        fun raw(n: Int): ByteArray
        fun leb(): Long
    }

    private class BytesCursor(private val bytes: ByteArray, private val pos: Pos, private val limit: Int) : Cursor {
        override fun mark(): Int = pos.mark()
        override fun u8(): Byte {
            if (pos.i >= limit) throw KdbDecodeException("eof u8", mark())
            return bytes[pos.i++]
        }

        override fun raw(n: Int): ByteArray {
            if (n < 0 || pos.i + n > limit) throw KdbDecodeException("eof raw", mark())
            val s = bytes.copyOfRange(pos.i, pos.i + n)
            pos.i += n
            return s
        }

        override fun leb(): Long {
            val v = readLeb128U64(bytes, pos)
            if (pos.i > limit) throw KdbDecodeException("leb past limit", mark())
            return v
        }
    }

    private class SourceCursor(private val pull: SourcePull) : Cursor {
        override fun mark(): Int = pull.offset
        override fun u8(): Byte = pull.readExact(1)[0]
        override fun raw(n: Int): ByteArray = pull.readExact(n)
        override fun leb(): Long = pull.readLeb128U64()
    }

    // --- encode -----------------------------------------------------------------------------

    private fun encodeValue(out: Buffer, value: KdbValue, type: KdbType, reg: KdbTypeRegistry) {
        when (type) {
            is KdbType.Nullable -> {
                if (value === KdbValue.Null) {
                    out.write(byteArrayOf(0))
                } else {
                    out.write(byteArrayOf(1))
                    encodeValue(out, value, type.inner, reg)
                }
            }

            is KdbType.Union -> {
                val uv = value as? KdbValue.UnionVal ?: throw KdbEncodeException("UnionVal expected")
                if (uv.branch !in type.branches.indices) throw KdbEncodeException("branch")
                out.write(byteArrayOf(uv.branch.toByte()))
                encodeValue(out, uv.value, type.branches[uv.branch], reg)
            }

            is KdbType.Primitive -> encodePrim(out, value, type.physical, type.logical, reg)
            is KdbType.Array -> {
                val a = value as? KdbValue.ArrayVal ?: throw KdbEncodeException("ArrayVal expected")
                out.putAll(encodeLeb128U32(a.elements.size.toUInt()))
                for (e in a.elements) encodeValue(out, e, type.element, reg)
            }

            is KdbType.Map -> {
                val m = value as? KdbValue.MapVal ?: throw KdbEncodeException("MapVal expected")
                out.putAll(encodeLeb128U32(m.entries.size.toUInt()))
                for ((k, v) in m.entries) {
                    encodeValue(out, k, type.key, reg)
                    encodeValue(out, v, type.value, reg)
                }
            }

            is KdbType.Ref -> encodeNamed(out, value, type, reg)
        }
    }

    private fun encodeNamed(out: Buffer, value: KdbValue, type: KdbType.Ref, reg: KdbTypeRegistry) {
        when (val schema = reg.resolve(type.fullyQualifiedName)) {
            is RecordSchema -> encodeRecord(out, value as? KdbValue.RecordVal ?: throw KdbEncodeException("RecordVal"), schema, reg)
            is EnumSchema -> {
                val e = value as? KdbValue.EnumVal ?: throw KdbEncodeException("EnumVal")
                if (e.ordinal !in schema.symbols.indices) throw KdbEncodeException("enum ordinal range")
                putLe32(out, e.ordinal)
            }

            is FixedSchema -> {
                val f = value as? KdbValue.FixedVal ?: throw KdbEncodeException("FixedVal")
                require(f.v.size == schema.size) { "fixed bytes" }
                out.putAll(f.v)
            }

            else -> throw KdbEncodeException("unsupported named type")
        }
    }

    private fun encodeRecord(out: Buffer, rec: KdbValue.RecordVal, schema: RecordSchema, reg: KdbTypeRegistry) {
        val body = Buffer()
        val sorted = schema.fields.sortedBy { it.id }
        for (f in sorted) {
            val cur = rec.fields[f.id]
            if (omitField(cur, f.default)) continue
            if (cur == null) throw KdbEncodeException("missing field ${f.name}")
            body.putAll(encodeLeb128U32(f.id.toUInt()))
            body.write(byteArrayOf(physicalTag(f.type, reg).tag))
            encodeValue(body, cur, f.type, reg)
        }
        val slab = body.drain()
        out.putAll(encodeLeb128U32(slab.size.toUInt()))
        out.putAll(slab)
    }

    private fun physicalTag(type: KdbType, reg: KdbTypeRegistry): PhysicalKind =
        when (type) {
            is KdbType.Primitive -> type.physical
            is KdbType.Array -> PhysicalKind.ARRAY
            is KdbType.Map -> PhysicalKind.MAP
            is KdbType.Union, is KdbType.Nullable -> PhysicalKind.UNION
            is KdbType.Ref -> when (reg.resolve(type.fullyQualifiedName)) {
                is RecordSchema -> PhysicalKind.RECORD
                is EnumSchema -> PhysicalKind.ENUM
                is FixedSchema -> PhysicalKind.FIXED
                else -> error("unexpected")
            }
        }

    private fun encodePrim(out: Buffer, value: KdbValue, phy: PhysicalKind, logical: LogicalAnnotation?, reg: KdbTypeRegistry) {
        if (logical is LogicalAnnotation.Custom) {
            val handler = LogicalTypeRegistry.resolve(logical.id) ?: throw KdbEncodeException("custom logical ${logical.id}")
            handler.validate(logical, phy)
            encodePrim(out, handler.encode(value, logical), phy, null, reg)
            return
        }
        when (logical) {
            LogicalAnnotation.Date -> {
                require(phy == PhysicalKind.INT32)
                val days =
                    when (value) {
                        is KdbValue.DateVal -> value.daysSinceEpoch
                        is KdbValue.Int32Val -> value.v
                        else -> throw KdbEncodeException("DATE value")
                    }
                putLe32(out, days)
            }

            LogicalAnnotation.TimeMicros -> {
                require(phy == PhysicalKind.INT64)
                val v =
                    when (value) {
                        is KdbValue.TimeMicrosVal -> value.microsSinceMidnight
                        is KdbValue.Int64Val -> value.v
                        else -> throw KdbEncodeException("TIME value")
                    }
                putLe64(out, v)
            }

            is LogicalAnnotation.TimestampMicros -> {
                require(phy == PhysicalKind.INT64)
                val ts = value as? KdbValue.TimestampVal ?: throw KdbEncodeException("timestamp")
                putLe64(out, ts.epochMicros)
            }

            is LogicalAnnotation.TimestampMillis -> {
                require(phy == PhysicalKind.INT64)
                val ts = value as? KdbValue.TimestampVal ?: throw KdbEncodeException("timestamp")
                putLe64(out, ts.epochMicros / 1000L)
            }

            LogicalAnnotation.Uuid -> {
                require(phy == PhysicalKind.FIXED)
                out.putAll(uuidWire(value))
            }

            LogicalAnnotation.Duration -> {
                require(phy == PhysicalKind.FIXED)
                val d = value as? KdbValue.DurationVal ?: throw KdbEncodeException("duration")
                val b = Buffer()
                putLe32(b, d.months)
                putLe32(b, d.days)
                putLe32(b, (d.micros and 0xFFFF_FFFFL).toInt())
                val raw = b.drain()
                require(raw.size == 12)
                out.putAll(raw)
            }

            is LogicalAnnotation.Decimal, LogicalAnnotation.BigInteger, is LogicalAnnotation.BigDecimal ->
                throw KdbEncodeException("decimal/big encode not implemented in iteration-1")

            null -> encodePhysicalOnly(out, value, phy)
            is LogicalAnnotation.Custom -> error("unreachable")
        }
    }

    private fun encodePhysicalOnly(out: Buffer, value: KdbValue, phy: PhysicalKind) {
        when (phy) {
            PhysicalKind.NULL -> if (value !== KdbValue.Null) throw KdbEncodeException("null")
            PhysicalKind.BOOLEAN -> out.write(byteArrayOf(if ((value as KdbValue.Bool).v) 1 else 0))
            PhysicalKind.INT8 -> out.write(byteArrayOf((value as KdbValue.Int8Val).v))
            PhysicalKind.INT16 -> putLe16(out, (value as KdbValue.Int16Val).v)
            PhysicalKind.INT32 -> putLe32(out, (value as KdbValue.Int32Val).v)
            PhysicalKind.INT64 -> putLe64(out, (value as KdbValue.Int64Val).v)
            PhysicalKind.FLOAT32 -> putFloat32(out, (value as KdbValue.Float32Val).v)
            PhysicalKind.FLOAT64 -> {
                val d = (value as KdbValue.Float64Val).v
                if (!d.isFinite()) throw KdbEncodeException("non-finite")
                putFloat64(out, d)
            }

            PhysicalKind.BYTES -> out.putAll(lebPrefix((value as KdbValue.BytesVal).v))
            PhysicalKind.STRING -> out.putAll(lebPrefix((value as KdbValue.StringVal).v.encodeToByteArray()))
            else -> throw KdbEncodeException("composite needs structural type")
        }
    }

    // --- decode -----------------------------------------------------------------------------

    private fun decodeValue(c: Cursor, type: KdbType, reg: KdbTypeRegistry): KdbValue =
        when (type) {
            is KdbType.Nullable -> {
                when (val m = c.u8().toInt() and 0xFF) {
                    0 -> KdbValue.Null
                    1 -> decodeValue(c, type.inner, reg)
                    else -> throw KdbDecodeException("bad nullable", c.mark())
                }
            }

            is KdbType.Union -> {
                val br = c.u8().toInt() and 0xFF
                if (br !in type.branches.indices) throw KdbDecodeException("bad union", c.mark())
                val inner = decodeValue(c, type.branches[br], reg)
                KdbValue.UnionVal(br, inner)
            }

            is KdbType.Primitive -> decodePrim(c, type.physical, type.logical, reg)
            is KdbType.Array -> {
                val n = c.leb().toInt()
                val list = ArrayList<KdbValue>(n)
                repeat(n) { list += decodeValue(c, type.element, reg) }
                KdbValue.ArrayVal(list)
            }

            is KdbType.Map -> {
                val n = c.leb().toInt()
                val pairs = ArrayList<Pair<KdbValue, KdbValue>>(n)
                repeat(n) {
                    val k = decodeValue(c, type.key, reg)
                    val v = decodeValue(c, type.value, reg)
                    pairs += k to v
                }
                KdbValue.MapVal(pairs)
            }

            is KdbType.Ref -> decodeNamed(c, type, reg)
        }

    private fun decodeNamed(c: Cursor, type: KdbType.Ref, reg: KdbTypeRegistry): KdbValue =
        when (val schema = reg.resolve(type.fullyQualifiedName)) {
            is RecordSchema -> decodeRecord(c, schema, reg)
            is EnumSchema -> {
                val ord = readLe32(c.raw(4), Pos(0))
                val sym = schema.symbols.getOrNull(ord) ?: "<unknown>"
                KdbValue.EnumVal(ord, sym)
            }

            is FixedSchema -> upliftFixed(schema, c.raw(schema.size))
            else -> throw KdbDecodeException("named", c.mark())
        }

    private fun decodeRecord(c: Cursor, schema: RecordSchema, reg: KdbTypeRegistry): KdbValue {
        val slab = c.raw(c.leb().toInt())
        val pos = Pos(0)
        val map = linkedMapOf<Int, KdbValue>()
        while (pos.i < slab.size) {
            val fid = readLeb128U64(slab, pos).toInt()
            val tag = slab[pos.i++]
            val kind =
                PhysicalKind.fromTag(tag) ?: throw KdbDecodeException("unknown record tag", pos.mark())
            val field = schema.fieldsById[fid]
            if (field == null) {
                skipTaggedBytes(slab, pos, slab.size)
                continue
            }
            if (kind != physicalTag(field.type, reg)) {
                throw KdbDecodeException("record tag mismatch field $fid", pos.mark())
            }
            val sub = BytesCursor(slab, pos, slab.size)
            map[fid] = decodeValue(sub, field.type, reg)
        }
        for (f in schema.fields) {
            if (!map.containsKey(f.id) && f.default != null) {
                map[f.id] = f.default
            }
        }
        return KdbValue.RecordVal(map)
    }

    private fun decodePrim(c: Cursor, phy: PhysicalKind, logical: LogicalAnnotation?, reg: KdbTypeRegistry): KdbValue {
        if (logical is LogicalAnnotation.Custom) {
            val handler = LogicalTypeRegistry.resolve(logical.id) ?: throw KdbDecodeException("custom logical", c.mark())
            val base = decodePrim(c, phy, null, reg)
            return handler.decode(base, logical)
        }
        return when (logical) {
            LogicalAnnotation.Date -> {
                require(phy == PhysicalKind.INT32)
                KdbValue.DateVal(readLe32(c.raw(4), Pos(0)))
            }

            LogicalAnnotation.TimeMicros -> {
                require(phy == PhysicalKind.INT64)
                KdbValue.TimeMicrosVal(readLe64(c.raw(8), Pos(0)))
            }

            is LogicalAnnotation.TimestampMicros -> {
                require(phy == PhysicalKind.INT64)
                KdbValue.TimestampVal(readLe64(c.raw(8), Pos(0)), logical.timezone)
            }

            is LogicalAnnotation.TimestampMillis -> {
                require(phy == PhysicalKind.INT64)
                val ms = readLe64(c.raw(8), Pos(0))
                KdbValue.TimestampVal(ms * 1000L, logical.timezone)
            }

            LogicalAnnotation.Uuid -> {
                require(phy == PhysicalKind.FIXED)
                val b = c.raw(16)
                val p = Pos(0)
                KdbValue.UuidVal(readBe64(b, p), readBe64(b, p))
            }

            LogicalAnnotation.Duration -> {
                require(phy == PhysicalKind.FIXED)
                val b = c.raw(12)
                val p = Pos(0)
                val months = readLe32(b, p)
                val days = readLe32(b, p)
                val mus = readLe32(b, p).toLong() and 0xFFFF_FFFFL
                KdbValue.DurationVal(months, days, mus)
            }

            is LogicalAnnotation.Decimal, LogicalAnnotation.BigInteger, is LogicalAnnotation.BigDecimal ->
                throw KdbDecodeException("decimal/big decode not implemented", c.mark())

            null -> decodePhysicalOnly(c, phy)
            is LogicalAnnotation.Custom -> error("unreachable")
        }
    }

    private fun decodePhysicalOnly(c: Cursor, phy: PhysicalKind): KdbValue =
        when (phy) {
            PhysicalKind.NULL -> KdbValue.Null
            PhysicalKind.BOOLEAN -> KdbValue.Bool(c.u8() != 0.toByte())
            PhysicalKind.INT8 -> KdbValue.Int8Val(c.u8())
            PhysicalKind.INT16 -> KdbValue.Int16Val(readLe16(c.raw(2), Pos(0)))
            PhysicalKind.INT32 -> KdbValue.Int32Val(readLe32(c.raw(4), Pos(0)))
            PhysicalKind.INT64 -> KdbValue.Int64Val(readLe64(c.raw(8), Pos(0)))
            PhysicalKind.FLOAT32 -> KdbValue.Float32Val(readFloat32(c.raw(4), Pos(0)))
            PhysicalKind.FLOAT64 -> KdbValue.Float64Val(readFloat64(c.raw(8), Pos(0)))
            PhysicalKind.BYTES -> KdbValue.BytesVal(readLebBytes(c))
            PhysicalKind.STRING -> KdbValue.StringVal(readLebString(c))
            else -> throw KdbDecodeException("unexpected composite physical", c.mark())
        }

    // --- helpers ----------------------------------------------------------------------------

    private fun lebPrefix(raw: ByteArray): ByteArray {
        val p = encodeLeb128U32(raw.size.toUInt())
        return p + raw
    }

    private fun readLebBytes(c: Cursor): ByteArray {
        val n = c.leb().toInt()
        return c.raw(n)
    }

    private fun readLebString(c: Cursor): String {
        val raw = readLebBytes(c)
        return decodeUtf8Strict(raw, 0, raw.size)
    }

    private fun uuidWire(v: KdbValue): ByteArray {
        when (v) {
            is KdbValue.UuidVal -> {
                val out = ByteArray(16)
                writeBe64(v.msb, out, 0)
                writeBe64(v.lsb, out, 8)
                return out
            }

            is KdbValue.FixedVal -> {
                require(v.v.size == 16)
                return v.v.copyOf()
            }

            else -> throw KdbEncodeException("uuid value")
        }
    }

    private fun upliftFixed(schema: FixedSchema, bytes: ByteArray): KdbValue {
        if (bytes.size != schema.size) throw KdbDecodeException("fixed size mismatch", -1)
        return when (schema.logical) {
            LogicalAnnotation.Uuid -> {
                val p = Pos(0)
                KdbValue.UuidVal(readBe64(bytes, p), readBe64(bytes, p))
            }

            LogicalAnnotation.Duration -> {
                val p = Pos(0)
                val months = readLe32(bytes, p)
                val days = readLe32(bytes, p)
                val micros = readLe32(bytes, p).toLong() and 0xFFFF_FFFFL
                KdbValue.DurationVal(months, days, micros)
            }

            else -> KdbValue.FixedVal(bytes.copyOf())
        }
    }

    private fun readBe64(b: ByteArray, p: Pos): Long {
        if (p.i + 8 > b.size) throw KdbDecodeException("be64", p.mark())
        var v = 0L
        repeat(8) {
            v = (v shl 8) or (b[p.i++].toLong() and 0xFFL)
        }
        return v
    }

    private fun writeBe64(v: Long, out: ByteArray, off: Int) {
        var x = v
        for (i in 0 until 8) {
            out[off + i] = ((x ushr ((7 - i) * 8)) and 0xFFL).toByte()
        }
    }

    private fun Buffer.putAll(bs: ByteArray) {
        write(bs, startIndex = 0, endIndex = bs.size)
    }

    private fun checkMatches(type: KdbType, value: KdbValue, reg: KdbTypeRegistry) {
        // Structural checks done at encode-time; keep hook for future validation
        when (type) {
            is KdbType.Ref -> reg.resolve(type.fullyQualifiedName)
            else -> Unit
        }
    }
}
