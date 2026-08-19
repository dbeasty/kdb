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
        if (secret != entry.secret) {
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
            }
        if (!principalHasPermission(principal, roles, kind, resource)) {
            throw KdbAuthorizationException(
                "principal ${principal.id} lacks $kind on ${resource.namespaceId}" +
                    (resource.documentId?.let { "/$it" } ?: ""),
            )
        }
    }
}
