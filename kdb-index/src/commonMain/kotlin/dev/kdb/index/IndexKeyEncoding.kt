package dev.kdb.index

import dev.kdb.codec.KdbUuid

/** Canonical order-preserving [IndexKey] bytes (Layer 5 Component 12). */
public fun encodeIndexKey(key: IndexKey): ByteArray =
    when (key) {
        IndexKey.NullKey -> byteArrayOf(0x00)
        is IndexKey.BoolKey -> byteArrayOf(0x05, if (key.value) 1 else 0)
        is IndexKey.Int32Key -> {
            val v = key.value
            byteArrayOf(0x02, (v shr 24).toByte(), (v shr 16).toByte(), (v shr 8).toByte(), v.toByte())
        }
        is IndexKey.Int64Key -> {
            val v = key.value
            byteArrayOf(
                0x03,
                (v shr 56).toByte(),
                (v shr 48).toByte(),
                (v shr 40).toByte(),
                (v shr 32).toByte(),
                (v shr 24).toByte(),
                (v shr 16).toByte(),
                (v shr 8).toByte(),
                v.toByte(),
            )
        }
        is IndexKey.Float64Key -> {
            val bits = key.value.toRawBits()
            byteArrayOf(
                0x04,
                (bits shr 56).toByte(),
                (bits shr 48).toByte(),
                (bits shr 40).toByte(),
                (bits shr 32).toByte(),
                (bits shr 24).toByte(),
                (bits shr 16).toByte(),
                (bits shr 8).toByte(),
                bits.toByte(),
            )
        }
        is IndexKey.TimestampKey -> {
            val v = key.epochMillis
            byteArrayOf(
                0x06,
                (v shr 56).toByte(),
                (v shr 48).toByte(),
                (v shr 40).toByte(),
                (v shr 32).toByte(),
                (v shr 24).toByte(),
                (v shr 16).toByte(),
                (v shr 8).toByte(),
                v.toByte(),
            )
        }
        is IndexKey.StringKey -> {
            val utf = key.value.encodeToByteArray()
            byteArrayOf(0x01) + utf
        }
        is IndexKey.UuidKey -> {
            val b = ByteArray(17)
            b[0] = 0x07
            key.id.msb.let { msb ->
                b[1] = (msb shr 56).toByte()
                b[2] = (msb shr 48).toByte()
                b[3] = (msb shr 40).toByte()
                b[4] = (msb shr 32).toByte()
                b[5] = (msb shr 24).toByte()
                b[6] = (msb shr 16).toByte()
                b[7] = (msb shr 8).toByte()
                b[8] = msb.toByte()
            }
            key.id.lsb.let { lsb ->
                b[9] = (lsb shr 56).toByte()
                b[10] = (lsb shr 48).toByte()
                b[11] = (lsb shr 40).toByte()
                b[12] = (lsb shr 32).toByte()
                b[13] = (lsb shr 24).toByte()
                b[14] = (lsb shr 16).toByte()
                b[15] = (lsb shr 8).toByte()
                b[16] = lsb.toByte()
            }
            b
        }
        is IndexKey.VectorKey -> throw IllegalArgumentException("VECTOR keys are not encodable in hash/btree indexes")
        is IndexKey.CompositeKey -> {
            val parts = key.parts.flatMap { encodeIndexKey(it).toList() }
            byteArrayOf(0x08) + parts.toByteArray()
        }
    }
