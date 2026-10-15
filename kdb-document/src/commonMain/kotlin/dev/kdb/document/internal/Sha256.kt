package dev.kdb.document.internal

/** SHA-256: the platform's own where it has a faster one (the JVM), else [sha256DigestPortable]. */
internal expect fun sha256Digest(message: ByteArray): ByteArray

/**
 * Pure Kotlin SHA-256 (RFC 6234). Multiplatform-safe: what JS and native use, and what the JVM's
 * platform digest is checked against.
 *
 * Parentheses matter: `ushr`/`shl` bind tighter than `or` in `rotr`; for Maj/Ch and σ xor
 * chains, `and`/`xor` must be grouped explicitly — Kotlin does not match Java `&`/`^`
 * precedence for the infix `and`/`xor` spellings.
 */
internal fun sha256DigestPortable(message: ByteArray): ByteArray = sha256Resume(SHA256_H0.copyOf(), 0L, message, 0)

/** SHA-256's block size in bytes. */
internal const val SHA256_BLOCK: Int = 64

private fun i32(u: UInt): Int = u.toInt()

private val SHA256_K: IntArray =
    intArrayOf(
        i32(0x428a2f98u),
        i32(0x71374491u),
        i32(0xb5c0fbcfu),
        i32(0xe9b5dba5u),
        i32(0x3956c25bu),
        i32(0x59f111f1u),
        i32(0x923f82a4u),
        i32(0xab1c5ed5u),
        i32(0xd807aa98u),
        i32(0x12835b01u),
        i32(0x243185beu),
        i32(0x550c7dc3u),
        i32(0x72be5d74u),
        i32(0x80deb1feu),
        i32(0x9bdc06a7u),
        i32(0xc19bf174u),
        i32(0xe49b69c1u),
        i32(0xefbe4786u),
        i32(0x0fc19dc6u),
        i32(0x240ca1ccu),
        i32(0x2de92c6fu),
        i32(0x4a7484aau),
        i32(0x5cb0a9dcu),
        i32(0x76f988dau),
        i32(0x983e5152u),
        i32(0xa831c66du),
        i32(0xb00327c8u),
        i32(0xbf597fc7u),
        i32(0xc6e00bf3u),
        i32(0xd5a79147u),
        i32(0x06ca6351u),
        i32(0x14292967u),
        i32(0x27b70a85u),
        i32(0x2e1b2138u),
        i32(0x4d2c6dfcu),
        i32(0x53380d13u),
        i32(0x650a7354u),
        i32(0x766a0abbu),
        i32(0x81c2c92eu),
        i32(0x92722c85u),
        i32(0xa2bfe8a1u),
        i32(0xa81a664bu),
        i32(0xc24b8b70u),
        i32(0xc76c51a3u),
        i32(0xd192e819u),
        i32(0xd6990624u),
        i32(0xf40e3585u),
        i32(0x106aa070u),
        i32(0x19a4c116u),
        i32(0x1e376c08u),
        i32(0x2748774cu),
        i32(0x34b0bcb5u),
        i32(0x391c0cb3u),
        i32(0x4ed8aa4au),
        i32(0x5b9cca4fu),
        i32(0x682e6ff3u),
        i32(0x748f82eeu),
        i32(0x78a5636fu),
        i32(0x84c87814u),
        i32(0x8cc70208u),
        i32(0x90befffau),
        i32(0xa4506cebu),
        i32(0xbef9a3f7u),
        i32(0xc67178f2u),
    )

internal val SHA256_H0: IntArray =
    intArrayOf(
        i32(0x6a09e667u),
        i32(0xbb67ae85u),
        i32(0x3c6ef372u),
        i32(0xa54ff53au),
        i32(0x510e527fu),
        i32(0x9b05688cu),
        i32(0x1f83d9abu),
        i32(0x5be0cd19u),
    )

/** Compresses the 64-byte block at [off] of [block] into [state], using [w] as scratch. */
internal fun sha256Compress(
    state: IntArray,
    block: ByteArray,
    off: Int,
    w: IntArray,
) {
    // Locals, not top-level declarations: Kotlin/JS starts every top-level function of a file with
    // top-level properties with that file's lazy-initialisation check, and runs a top-level
    // property read through it too; in the round loop that made the digest ~8x slower.
    val k = SHA256_K

    // Parentheses required: `ushr`/`shl` bind tighter than `or`.
    fun rotr(
        x: Int,
        n: Int,
    ): Int = ((x ushr n) or (x shl (32 - n)))
    for (t in 0 until 16) {
        val o = off + t * 4
        w[t] =
            ((block[o].toInt() and 0xFF) shl 24) or
            ((block[o + 1].toInt() and 0xFF) shl 16) or
            ((block[o + 2].toInt() and 0xFF) shl 8) or
            (block[o + 3].toInt() and 0xFF)
    }
    for (t in 16 until 64) {
        val wp15 = w[t - 15]
        val s0 = (rotr(wp15, 7) xor rotr(wp15, 18)) xor (wp15 ushr 3)
        val wp2 = w[t - 2]
        val s1 = (rotr(wp2, 17) xor rotr(wp2, 19)) xor (wp2 ushr 10)
        w[t] = w[t - 16] + s0 + w[t - 7] + s1
    }
    var a = state[0]
    var b = state[1]
    var c = state[2]
    var d = state[3]
    var e = state[4]
    var f = state[5]
    var g = state[6]
    var h = state[7]
    for (t in 0 until 64) {
        val s1r = (rotr(e, 6) xor rotr(e, 11)) xor rotr(e, 25)
        val ch = ((e and f) xor ((e.inv()) and g))
        val t1 = h + s1r + ch + k[t] + w[t]
        val s0a = (rotr(a, 2) xor rotr(a, 13)) xor rotr(a, 22)
        val maj = ((a and b) xor (a and c)) xor (b and c)
        val t2 = s0a + maj
        h = g
        g = f
        f = e
        e = d + t1
        d = c
        c = b
        b = a
        a = t1 + t2
    }
    state[0] += a
    state[1] += b
    state[2] += c
    state[3] += d
    state[4] += e
    state[5] += f
    state[6] += g
    state[7] += h
}

/**
 * Finishes a SHA-256 whose first [prefixLen] bytes (a whole number of blocks) are already folded
 * into [state] (modified in place), with [message] from [from] as the rest of the input. With the
 * initial state and no prefix this is the plain digest; with a precomputed midstate it hashes a
 * long constant-prefixed input without redoing the prefix's blocks.
 */
internal fun sha256Resume(
    state: IntArray,
    prefixLen: Long,
    message: ByteArray,
    from: Int,
): ByteArray {
    val w = IntArray(64)
    var off = from
    while (message.size - off >= SHA256_BLOCK) {
        sha256Compress(state, message, off, w)
        off += SHA256_BLOCK
    }
    // The tail: what is left of the message, 0x80, zeros, and the 64-bit bit length.
    val rest = message.size - off
    val tail = ByteArray(if (rest + 9 <= SHA256_BLOCK) SHA256_BLOCK else 2 * SHA256_BLOCK)
    message.copyInto(tail, 0, off, message.size)
    tail[rest] = 0x80.toByte()
    var bl = (prefixLen + message.size - from) * 8L
    for (i in tail.size - 1 downTo tail.size - 8) {
        tail[i] = (bl and 0xFF).toInt().toByte()
        bl = bl ushr 8
    }
    var t = 0
    while (t < tail.size) {
        sha256Compress(state, tail, t, w)
        t += SHA256_BLOCK
    }
    val out = ByteArray(32)
    for (i in 0 until 8) {
        val v = state[i]
        out[i * 4] = (v ushr 24).toByte()
        out[i * 4 + 1] = (v ushr 16).toByte()
        out[i * 4 + 2] = (v ushr 8).toByte()
        out[i * 4 + 3] = v.toByte()
    }
    return out
}
