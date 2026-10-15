package dev.kdb.auth.store

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.security.MessageDigest

/**
 * Revocable access tokens (qtask production plan U12), the Kotlin side of Go's
 * `go/kdb/auth/tokens.go`: a registry written by either language works with both.
 *
 * A token names one client: it is issued for a user, carries that user's roles, can expire, and
 * can be revoked on its own. The string is `kdbt_<id>_<secret>`; the registry keeps only the hex
 * SHA-256 of the secret, in the users namespace under the document id derived from
 * `"token:<id>"` - an id no user id can produce.
 *
 * [createdAt]/[expiresAt] are RFC 3339 timestamps; a null [expiresAt] never expires.
 */
public data class TokenRecord(
    val tokenId: String,
    val user: String,
    val label: String = "",
    val secretSha256: String,
    val createdAt: String,
    val expiresAt: String? = null,
    val revoked: Boolean = false,
)

public class TokenNotFoundException(tokenId: String) : RuntimeException("token not found: $tokenId")

internal object AccessTokens {
    const val PREFIX: String = "kdbt_"
    private const val KIND = "token"

    fun isAccessToken(s: String): Boolean = s.startsWith(PREFIX)

    /** Splits `kdbt_<id>_<secret>`; null when malformed. */
    fun parse(s: String): Pair<String, String>? {
        if (!isAccessToken(s)) return null
        val rest = s.substring(PREFIX.length)
        val i = rest.indexOf('_')
        if (i <= 0 || i == rest.length - 1) return null
        return rest.substring(0, i) to rest.substring(i + 1)
    }

    fun secretDigest(secret: String): String =
        MessageDigest.getInstance("SHA-256").digest(secret.encodeToByteArray()).joinToString("") {
            (it.toInt() and 0xff).toString(16).padStart(2, '0')
        }

    /** Constant-time comparison of two hex digests. */
    fun digestsEqual(
        a: String,
        b: String,
    ): Boolean = MessageDigest.isEqual(a.encodeToByteArray(), b.encodeToByteArray())

    /**
     * The record as Go's `json.Marshal(TokenRecord)` writes it: the same keys in the same order,
     * with `label`, `expiresAt` and `revoked` left out when empty (Go's omitempty). Go lists
     * tokens by looking for `"kind":"token"` in the stored body, so the form is load-bearing.
     */
    fun encode(record: TokenRecord): String =
        buildJsonObject {
            put("kind", KIND)
            put("tokenId", record.tokenId)
            put("user", record.user)
            if (record.label.isNotEmpty()) put("label", record.label)
            put("secretSha256", record.secretSha256)
            put("createdAt", record.createdAt)
            record.expiresAt?.takeIf { it.isNotEmpty() }?.let { put("expiresAt", it) }
            if (record.revoked) put("revoked", true)
        }.toString()

    /** Decodes a stored body; null when it is not a token record (a user, say). */
    fun decode(body: String): TokenRecord? {
        val obj =
            try {
                Json.parseToJsonElement(body).jsonObject
            } catch (_: Exception) {
                return null
            }
        if (obj.string("kind") != KIND) return null
        return TokenRecord(
            tokenId = obj.string("tokenId") ?: return null,
            user = obj.string("user") ?: return null,
            label = obj.string("label") ?: "",
            secretSha256 = obj.string("secretSha256") ?: return null,
            createdAt = obj.string("createdAt") ?: "",
            expiresAt = obj.string("expiresAt")?.takeIf { it.isNotEmpty() },
            revoked = (obj["revoked"] as? JsonPrimitive)?.booleanOrNull ?: false,
        )
    }

    private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)?.takeIf { it.isString }?.contentOrNull
}
