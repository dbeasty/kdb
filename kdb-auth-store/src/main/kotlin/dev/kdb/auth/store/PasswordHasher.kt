package dev.kdb.auth.store

import java.security.SecureRandom
import java.security.spec.KeySpec
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec

/**
 * Slow password hashing (PBKDF2-HMAC-SHA256, 120k iterations). This replaces the raw `secret`
 * field the static/dev auth provider stores in plaintext config — every credential that ends up
 * in [UserStore] goes through this, never stored or compared in the clear.
 */
public object PasswordHasher {
    private const val ITERATIONS = 120_000
    private const val KEY_LENGTH_BITS = 256
    private const val SALT_LENGTH_BYTES = 16
    private const val ALGORITHM = "PBKDF2WithHmacSHA256"

    public fun hash(
        password: String,
        salt: ByteArray = randomSalt(),
    ): Pair<String, String> {
        val digest = deriveKey(password, salt)
        return digest.toHex() to salt.toHex()
    }

    public fun verify(
        password: String,
        expectedHashHex: String,
        saltHex: String,
    ): Boolean {
        val salt = saltHex.decodeHex()
        val digest = deriveKey(password, salt)
        return constantTimeEquals(digest, expectedHashHex.decodeHex())
    }

    private fun deriveKey(
        password: String,
        salt: ByteArray,
    ): ByteArray {
        val spec: KeySpec = PBEKeySpec(password.toCharArray(), salt, ITERATIONS, KEY_LENGTH_BITS)
        val factory = SecretKeyFactory.getInstance(ALGORITHM)
        return factory.generateSecret(spec).encoded
    }

    private fun randomSalt(): ByteArray = ByteArray(SALT_LENGTH_BYTES).also { SecureRandom().nextBytes(it) }

    private fun constantTimeEquals(
        a: ByteArray,
        b: ByteArray,
    ): Boolean {
        if (a.size != b.size) return false
        var diff = 0
        for (i in a.indices) diff = diff or (a[i].toInt() xor b[i].toInt())
        return diff == 0
    }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }

    private fun String.decodeHex(): ByteArray {
        require(length % 2 == 0) { "invalid hex string" }
        return ByteArray(length / 2) { i -> ((Character.digit(this[i * 2], 16) shl 4) + Character.digit(this[i * 2 + 1], 16)).toByte() }
    }
}
