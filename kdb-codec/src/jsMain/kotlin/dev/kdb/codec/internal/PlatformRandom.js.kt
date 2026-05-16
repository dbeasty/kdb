package dev.kdb.codec.internal

internal actual fun secureRandomBytes(count: Int): ByteArray {
    require(count >= 0)
    val out = ByteArray(count)
    val crypto = js("globalThis.crypto")
    crypto.getRandomValues(out)
    return out
}
