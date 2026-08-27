package dev.kdb.auth.static

import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.KdbAuthenticationException
import dev.kdb.auth.KdbAuthorizationException
import dev.kdb.auth.Authenticator
import dev.kdb.auth.Authorizer
import dev.kdb.auth.Principal
import dev.kdb.auth.ResourcePath
import dev.kdb.auth.permissionKind
import dev.kdb.auth.principalHasPermission
import kotlinx.serialization.json.Json

public fun staticAuthEngine(config: StaticAuthConfig): AuthEngine = StaticAuthEngine(config)

public fun staticAuthEngineFromJson(json: String): AuthEngine =
    staticAuthEngine(Json { ignoreUnknownKeys = true }.decodeFromString(StaticAuthConfig.serializer(), json))

public fun staticAuthEngineFromFile(path: String): AuthEngine =
    staticAuthEngineFromJson(java.nio.file.Files.readString(java.nio.file.Path.of(path)))

/**
 * `!=` on two Strings short-circuits at the first differing character, so response time leaks
 * how many leading characters of a guess matched - enough of a side channel to brute-force a
 * static-config secret character-by-character over a network round trip (docs/kdb-finish-up-plan.md
 * 1-K8). Compares every character regardless of where the two strings first diverge, and (like a
 * proper MAC compare) still walks the longer string's full length even when the lengths differ,
 * instead of returning early on a length mismatch.
 */
internal fun constantTimeEquals(a: String, b: String): Boolean {
    val longer = if (a.length >= b.length) a else b
    var diff = a.length xor b.length
    for (i in longer.indices) {
        val ca = if (i < a.length) a[i].code else 0
        val cb = if (i < b.length) b[i].code else 0
        diff = diff or (ca xor cb)
    }
    return diff == 0
}

private class StaticAuthEngine(
    config: StaticAuthConfig,
) : AuthEngine {
    private val users = config.users
    private val roles: Map<String, Set<String>> =
        config.roles.mapValues { (_, perms) -> perms.toSet() }

    override val authenticator: Authenticator =
        StaticAuthenticator(users)

    override val authorizer: Authorizer =
        StaticAuthorizer(roles)
}

private class StaticAuthenticator(
    private val users: Map<String, StaticUserConfig>,
) : Authenticator {
    override suspend fun authenticate(credentials: AuthCredentials): Principal {
        val (user, secret) = resolveUserAndSecret(credentials)
        val entry =
            users[user]
                ?: throw KdbAuthenticationException("unknown user: $user")
        if (!constantTimeEquals(secret, entry.secret)) {
            throw KdbAuthenticationException("invalid credentials for user: $user")
        }
        return Principal(id = user, roles = entry.roles.toSet())
    }

    private fun resolveUserAndSecret(credentials: AuthCredentials): Pair<String, String> {
        if (credentials.user != null && credentials.password != null) {
            return credentials.user!! to credentials.password!!
        }
        val token = credentials.token
        if (token != null) {
            val colon = token.indexOf(':')
            if (colon > 0) {
                return token.substring(0, colon) to token.substring(colon + 1)
            }
        }
        throw KdbAuthenticationException("missing credentials")
    }
}

private class StaticAuthorizer(
    private val roles: Map<String, Set<String>>,
) : Authorizer {
    override suspend fun authorize(
        principal: Principal,
        action: AuthAction,
    ) {
        val (kind, resource) =
            when (action) {
                is AuthAction.SessionBegin ->
                    "read" to ResourcePath.of(action.namespace)
                is AuthAction.SqlExec ->
                    (if (action.readOnly) "read" else "write") to ResourcePath.of(action.namespace)
                is AuthAction.TxCommit ->
                    "write" to ResourcePath.of(action.namespace)
                is AuthAction.PeerSync ->
                    "sync" to ResourcePath.of(action.namespace)
                is AuthAction.DocumentWrite ->
                    "write" to ResourcePath.of(action.namespace, action.docId)
                is AuthAction.DocumentDelete ->
                    "write" to ResourcePath.of(action.namespace, action.docId)
                is AuthAction.DocumentRead ->
                    "read" to ResourcePath.of(action.namespace, action.docId)
                is AuthAction.Admin ->
                    "admin" to ResourcePath.of(action.scope)
                is AuthAction.ProcExec ->
                    (if (action.readOnly) "proc-read" else "proc-write") to ResourcePath.of(action.namespace)
                is AuthAction.ProcManage ->
                    "proc-manage" to ResourcePath.of(action.namespace)
            }
        if (!principalHasPermission(principal, roles, kind, resource)) {
            throw KdbAuthorizationException(
                "principal ${principal.id} lacks $kind on ${resource.namespaceId}" +
                    (resource.documentId?.let { "/$it" } ?: ""),
            )
        }
    }
}
