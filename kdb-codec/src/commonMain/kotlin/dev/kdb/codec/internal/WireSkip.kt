package dev.kdb.codec.internal

import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.error.KdbDecodeException

private fun consumeExact(raw: ByteArray, pos: Pos, n: Int, limitExclusive: Int) {
    if (n < 0 || pos.i + n > limitExclusive.coerceAtMost(raw.size)) {
        throw KdbDecodeException("truncated primitive skip", pos.mark())
    }
    pos.i += n
}

/** Skip a tagged value whose first byte is a [PhysicalKind] tag. */
internal fun skipTaggedBytes(raw: ByteArray, pos: Pos, limitExclusive: Int) {
    if (pos.i >= limitExclusive) {
        throw KdbDecodeException("skip past end", pos.mark())
    }
    val tag = raw[pos.i++]
        val tagU = tag.toInt() and 0xFF
        val kind =
            PhysicalKind.fromTag(tag)
                ?: throw KdbDecodeException(
                    "unknown physical tag 0x${tagU.toString(16).padStart(2, '0')}",
                    pos.mark(),
                )
    when (kind) {
        PhysicalKind.NULL -> Unit
        PhysicalKind.BOOLEAN, PhysicalKind.INT8 -> consumeExact(raw, pos, 1, limitExclusive)
        PhysicalKind.INT16 -> consumeExact(raw, pos, 2, limitExclusive)
        PhysicalKind.INT32, PhysicalKind.FLOAT32, PhysicalKind.ENUM -> consumeExact(raw, pos, 4, limitExclusive)
        PhysicalKind.INT64, PhysicalKind.FLOAT64 -> consumeExact(raw, pos, 8, limitExclusive)
        PhysicalKind.STRING, PhysicalKind.BYTES -> {
            val n = readLeb128U64(raw, pos).toInt()
            if (n < 0) throw KdbDecodeException("negative string/bytes length", pos.mark())
            consumeExact(raw, pos, n, limitExclusive)
        }

        PhysicalKind.ARRAY -> {
            val cnt = readLeb128U64(raw, pos).toInt()
            repeat(cnt) { skipTaggedBytes(raw, pos, limitExclusive) }
        }

        PhysicalKind.MAP -> {
            val cnt = readLeb128U64(raw, pos).toInt()
            repeat(cnt) {
                skipTaggedBytes(raw, pos, limitExclusive)
                skipTaggedBytes(raw, pos, limitExclusive)
            }
        }

        PhysicalKind.RECORD -> {
            val body = readLeb128U64(raw, pos).toInt()
            if (body < 0) throw KdbDecodeException("negative record fragment", pos.mark())
            val endInner = pos.i + body
            val lim = limitExclusive.coerceAtMost(raw.size)
            if (endInner > lim) throw KdbDecodeException("record fragment past boundary", pos.mark())
            while (pos.i < endInner) {
                readLeb128U64(raw, pos)
                skipTaggedBytes(raw, pos, endInner)
            }
            if (pos.i != endInner) throw KdbDecodeException("misaligned record skip", pos.mark())
        }

        PhysicalKind.UNION ->
            throw KdbDecodeException("cannot skip UNION without branch schema context", pos.mark())

        PhysicalKind.FIXED -> throw KdbDecodeException("cannot skip FIXED without declared size", pos.mark())
    }
}
