package dev.kdb.codec.internal

import java.security.SecureRandom

private val rnd = SecureRandom()

internal actual fun secureRandomBytes(count: Int): ByteArray {
    require(count >= 0)
    return ByteArray(count).also { rnd.nextBytes(it) }
}
