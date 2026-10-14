package dev.kdb.replica

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

/** Byte helpers shared by the commit list codec, the trie and the stores. */

private const val HEX = "0123456789abcdef"

internal fun ByteArray.toHex(): String =
    buildString(size * 2) {
        for (b in this@toHex) {
            val v = b.toInt() and 0xff
            append(HEX[v shr 4])
            append(HEX[v and 0x0f])
        }
    }

internal fun hexToBytes(hex: String): ByteArray {
    require(hex.length % 2 == 0) { "kdb replica: not a hex string: ${hex.take(80)}" }
    val out = ByteArray(hex.length / 2)
    for (i in out.indices) {
        val hi = hexDigit(hex[i * 2])
        val lo = hexDigit(hex[i * 2 + 1])
        out[i] = ((hi shl 4) or lo).toByte()
    }
    return out
}

private fun hexDigit(c: Char): Int =
    when (c) {
        in '0'..'9' -> c - '0'
        in 'a'..'f' -> c - 'a' + 10
        in 'A'..'F' -> c - 'A' + 10
        else -> throw IllegalArgumentException("kdb replica: not a hex digit: $c")
    }

/** A 32-byte hash from its lowercase hex form; anything else is refused. */
internal fun hex32(hex: String): ByteArray {
    require(hex.length == 64 && hex.all { it in '0'..'9' || it in 'a'..'f' }) {
        "kdb replica: not a 32-byte hex hash: $hex"
    }
    return hexToBytes(hex)
}

private val UUID_RE = Regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

/**
 * A document id's 16 bytes, big-endian: Go's `uuidWire` writes MSB then LSB, which is the
 * canonical string with its dashes removed. The trie keys on these bytes, and their order is the
 * order of the lowercase strings, which is why document keys in the store use the lowercase form.
 */
internal fun uuidBytes(id: String): ByteArray {
    val lower = id.lowercase()
    require(UUID_RE.matches(lower)) { "kdb replica: not a UUID: \"$id\"" }
    return hexToBytes(lower.replace("-", ""))
}

internal fun uuidString(b: ByteArray): String {
    require(b.size == 16) { "kdb replica: a UUID is 16 bytes, got ${b.size}" }
    val h = b.toHex()
    return "${h.substring(0, 8)}-${h.substring(8, 12)}-${h.substring(12, 16)}-${h.substring(16, 20)}-${h.substring(20)}"
}

internal fun parseUuid(id: String): KdbUuid = KdbUuid.fromBytes(uuidBytes(id))

/** A random (version 4) UUID string. */
public fun randomUuid(): String = KdbUuid.random().toString()

internal val ZERO32 = ByteArray(32)

internal fun ByteArray.isZero(): Boolean = all { it == 0.toByte() }

/** Go's encoding/json form of `[]byte`: standard base64 with padding. */
@OptIn(ExperimentalEncodingApi::class)
internal fun toBase64(b: ByteArray): String = Base64.Default.encode(b)

@OptIn(ExperimentalEncodingApi::class)
internal fun fromBase64(s: String?): ByteArray = if (s.isNullOrEmpty()) ByteArray(0) else Base64.Default.decode(s)

/**
 * The key form of a namespace name: JavaScript's encodeURIComponent, so the TS replica and this
 * one lay their stores out identically (a name may hold '/', which must not split a key).
 */
internal fun encodeKeyComponent(s: String): String =
    buildString {
        for (b in s.encodeToByteArray()) {
            val c = (b.toInt() and 0xff)
            val ch = c.toChar()
            if (c < 0x80 && (ch.isLetterOrDigit() || ch in "-_.!~*'()")) {
                append(ch)
            } else {
                append('%')
                append(HEX[c shr 4].uppercaseChar())
                append(HEX[c and 0x0f].uppercaseChar())
            }
        }
    }

/** The current wall clock in microseconds since the epoch. */
internal fun nowMicros(): Long = KdbTimestamp.now().toEpochMicros()
