package dev.kdb.document.internal

/**
 * Pure Kotlin SHA-256 (RFC 6234). Multiplatform-safe.
 */
internal fun sha256Digest(message: ByteArray): ByteArray {
    val msgLen = message.size.toLong()
    val bitLen = msgLen * 8L
    val padLen = (56 - (message.size + 1) % 64 + 64) % 64
    val totalLen = (msgLen + 1 + padLen + 8).toInt()
    val padded = ByteArray(totalLen)
    message.copyInto(padded)
    padded[msgLen.toInt()] = 0x80.toByte()
    var bl = bitLen
    var i = padded.size - 8
    repeat(8) {
        padded[i + 7 - it] = (bl and 0xFF).toInt().toByte()
        bl = bl ushr 8
    }

    fun i32(u: UInt): Int = u.toInt()

    val k =
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

    fun rotr(x: Int, n: Int): Int = (x ushr n) or (x shl (32 - n))

    val h0 =
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
    val w = IntArray(64)
    var offset = 0
    while (offset < padded.size) {
        for (t in 0 until 16) {
            w[t] =
                ((padded[offset + t * 4].toInt() and 0xFF) shl 24) or
                    ((padded[offset + t * 4 + 1].toInt() and 0xFF) shl 16) or
                    ((padded[offset + t * 4 + 2].toInt() and 0xFF) shl 8) or
                    (padded[offset + t * 4 + 3].toInt() and 0xFF)
        }
        for (t in 16 until 64) {
            val s0 = rotr(w[t - 15], 7) xor rotr(w[t - 15], 18) xor (w[t - 15] ushr 3)
            val s1 = rotr(w[t - 2], 17) xor rotr(w[t - 2], 19) xor (w[t - 2] ushr 10)
            w[t] = w[t - 16] + s0 + w[t - 7] + s1
        }

        var a = h0[0]
        var b = h0[1]
        var c = h0[2]
        var d = h0[3]
        var e = h0[4]
        var f = h0[5]
        var g = h0[6]
        var h = h0[7]

        for (t in 0 until 64) {
            val s1 = rotr(e, 6) xor rotr(e, 11) xor rotr(e, 25)
            val ch = e and f xor (e.inv() and g)
            val t1 = h + s1 + ch + k[t] + w[t]
            val s0a = rotr(a, 2) xor rotr(a, 13) xor rotr(a, 22)
            val maj = a and b xor a and c xor b and c
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

        h0[0] += a
        h0[1] += b
        h0[2] += c
        h0[3] += d
        h0[4] += e
        h0[5] += f
        h0[6] += g
        h0[7] += h
        offset += 64
    }

    val out = ByteArray(32)
    for (i in 0 until 8) {
        val v = h0[i]
        out[i * 4] = (v ushr 24).toByte()
        out[i * 4 + 1] = (v ushr 16).toByte()
        out[i * 4 + 2] = (v ushr 8).toByte()
        out[i * 4 + 3] = v.toByte()
    }
    return out
}
