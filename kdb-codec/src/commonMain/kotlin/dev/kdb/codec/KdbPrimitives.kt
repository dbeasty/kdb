package dev.kdb.codec

import dev.kdb.codec.internal.secureRandomBytes
import kotlin.jvm.JvmInline
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant

/**
 * 128-bit UUID (RFC 4122 string form). Multiplatform replacement for `java.util.UUID`.
 */
public data class KdbUuid(public val msb: Long, public val lsb: Long) {

    /** Standard `8-4-4-4-12` lower-case hexadecimal form. */
    override fun toString(): String =
        uuidBytes().toUidashString()

    public companion object {
        public fun random(): KdbUuid {
            val b = secureRandomBytes(16)
            // Version 4
            b[6] = (b[6].toInt() and 0x0F or 0x40).toByte()
            b[8] = (b[8].toInt() and 0x3F or 0x80).toByte()
            return fromBytes(b)
        }

        public fun fromString(s: String): KdbUuid {
            val hex = s.replace("-", "").lowercase()
            require(hex.length == 32 && hex.all { it.isDigit() || it in 'a'..'f' }) { "invalid UUID string" }
            return fromHexNoDash(hex)
        }

        public fun fromBytes(bytes: ByteArray): KdbUuid {
            require(bytes.size == 16) { "UUID requires 16 bytes" }
            return KdbUuid(
                msb = readBeLong(bytes, 0),
                lsb = readBeLong(bytes, 8),
            )
        }
    }

    internal fun uuidBytes(): ByteArray {
        val out = ByteArray(16)
        writeBeLong(msb, out, 0)
        writeBeLong(lsb, out, 8)
        return out
    }
}

private fun fromHexNoDash(hex32: String): KdbUuid {
    var msb = 0L
    var lsb = 0L
    for (i in 0 until 16) {
        msb = (msb shl 4) or hexDigitAt(hex32, i).toLong()
    }
    for (i in 16 until 32) {
        lsb = (lsb shl 4) or hexDigitAt(hex32, i).toLong()
    }
    return KdbUuid(msb, lsb)
}

private fun hexDigitAt(hex: String, index: Int): Int {
    val c = hex[index].code
    return when {
        c <= '9'.code -> c - '0'.code
        c <= 'F'.code && c >= 'A'.code -> 10 + c - 'A'.code
        else -> 10 + c - 'a'.code
    }.also {
        check(it in 0..15)
    }
}

private fun readBeLong(bytes: ByteArray, offset: Int): Long {
    var v = 0UL
    for (i in 0 until 8) {
        v = (v shl 8) or (bytes[offset + i].toInt() and 0xFF).toULong()
    }
    return v.toLong()
}

private fun writeBeLong(v: Long, out: ByteArray, offset: Int) {
    val u = v.toULong()
    for (i in 0 until 8) {
        val shift = (7 - i) * 8
        out[offset + i] = ((u shr shift) and 0xFFUL).toByte()
    }
}

/** `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. */
internal fun ByteArray.toUidashString(): String {
    require(size == 16)
    fun hex(byte: Byte): CharArray =
        charArrayOf(nibbleHex(byte.toInt().ushr(4) and 0xF), nibbleHex(byte.toInt() and 0xF))

    fun appendHex(sb: StringBuilder, idx: Int) {
        val h = hex(this[idx])
        sb.append(h[0]).append(h[1])
    }

    return buildString(36) {
        for (i in 0 until 4) appendHex(this, i)
        append('-')
        for (i in 4 until 6) appendHex(this, i)
        append('-')
        for (i in 6 until 8) appendHex(this, i)
        append('-')
        for (i in 8 until 10) appendHex(this, i)
        append('-')
        for (i in 10 until 16) appendHex(this, i)
    }
}

private fun nibbleHex(v: Int): Char = HEX[v and 0xF]

private val HEX = "0123456789abcdef".toCharArray()

/**
 * SHA-256 hash (32 raw bytes).
 */
@JvmInline
public value class KdbHash(public val bytes: ByteArray) {

    init {
        require(bytes.size == HASH_LEN) { "KdbHash requires 32 bytes" }
    }

    public fun toHex(): String =
        buildString(HASH_LEN * 2) {
            for (b in bytes) {
                val v = b.toInt() and 0xFF
                append(nibbleHex(v shr 4))
                append(nibbleHex(v))
            }
        }

    public companion object {
        private const val HASH_LEN = 32

        public fun fromHex(hex: String): KdbHash {
            require(hex.length == HASH_LEN * 2)
            val lower = hex.lowercase()
            val out = ByteArray(HASH_LEN)
            for (i in 0 until HASH_LEN) {
                val hi = hexDigit(lower[i * 2])
                val lo = hexDigit(lower[i * 2 + 1])
                out[i] = ((hi shl 4) or lo).toByte()
            }
            return KdbHash(out)
        }

        public fun fromBytes(bytes: ByteArray): KdbHash {
            require(bytes.size == HASH_LEN)
            return KdbHash(bytes.copyOf())
        }
    }
}

private fun hexDigit(c: Char): Int =
    when {
        c in '0'..'9' -> c.code - '0'.code
        c in 'a'..'f' -> 10 + (c.code - 'a'.code)
        else -> throw IllegalArgumentException("invalid hex")
    }

/**
 * Logical instant with microsecond resolution: whole milliseconds plus 0–999 μs within that millisecond.
 */
public data class KdbTimestamp(
    public val epochMillis: Long,
    public val microRemainder: Int = 0,
) : Comparable<KdbTimestamp> {

    init {
        require(microRemainder in 0..999) { "microRemainder must be 0..999" }
    }

    public fun toEpochMicros(): Long = epochMillis * 1000L + microRemainder

    override fun compareTo(other: KdbTimestamp): Int =
        compareValuesBy(this, other, KdbTimestamp::epochMillis, KdbTimestamp::microRemainder)

    public companion object {
        public fun now(): KdbTimestamp {
            val instant = Clock.System.now()
            val microsTotal = instant.epochSeconds * 1_000_000L + instant.nanosecondsOfSecond / 1000L
            val ms = microsTotal.floorDiv(1000L)
            val r = microsTotal.mod(1000L).toInt()
            return KdbTimestamp(ms, r)
        }

        /** Whole microsecond epoch; remainder uses Kotlin `floorDiv` / `mod` pairing. */
        public fun fromEpochMicros(micros: Long): KdbTimestamp {
            val ms = micros.floorDiv(1000L)
            val r = micros.mod(1000L).toInt()
            return KdbTimestamp(ms, r)
        }

        /** ISO-8601 instant string (typically Zulu). */
        public fun fromIso8601(s: String): KdbTimestamp {
            val instant = Instant.parse(s)
            val microsTotal = instant.epochSeconds * 1_000_000L + instant.nanosecondsOfSecond / 1000L
            val ms = microsTotal.floorDiv(1000L)
            val r = microsTotal.mod(1000L).toInt()
            return KdbTimestamp(ms, r)
        }
    }
}

public fun KdbUuid.toUuidVal(): KdbValue.UuidVal = KdbValue.UuidVal(msb, lsb)

public fun KdbValue.UuidVal.toKdbUuid(): KdbUuid = KdbUuid(msb, lsb)

public fun KdbTimestamp.toTimestampVal(tz: String? = null): KdbValue.TimestampVal =
    KdbValue.TimestampVal(toEpochMicros(), tz)

public fun KdbValue.TimestampVal.toKdbTimestamp(): KdbTimestamp = KdbTimestamp.fromEpochMicros(epochMicros)
