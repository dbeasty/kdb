package dev.kdb.auth.store

import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.Authenticator
import dev.kdb.auth.Authorizer
import dev.kdb.auth.KdbAuthenticationException
import dev.kdb.auth.KdbAuthorizationException
import dev.kdb.auth.Principal
import dev.kdb.auth.ResourcePath
import dev.kdb.auth.principalHasPermission

/**
 * [AuthEngine] backed by a [RegistryAuthStore] instead of a fixed startup config — every
 * authenticate/authorize call reads the current user/role registry, so `CREATE ROLE`/`GRANT`/
 * `CREATE USER` (once wired through the admin surface, see docs/kdb-rbac-plan.md phase 4) take
 * effect immediately without a server restart.
 */
public fun dynamicAuthEngine(store: RegistryAuthStore): AuthEngine = DynamicAuthEngine(store)

private class DynamicAuthEngine(
    private val store: RegistryAuthStore,
) : AuthEngine {
    override val authenticator: Authenticator =
        object : Authenticator {
            override suspend fun authenticate(credentials: AuthCredentials): Principal {
                val (user, password) = resolveUserAndSecret(credentials)
                val record =
                    store.getUser(user)
                        ?: throw KdbAuthenticationException("unknown user: $user")
                if (!store.verifyPassword(user, password)) {
                    throw KdbAuthenticationException("invalid credentials for user: $user")
                }
                return Principal(id = user, roles = record.roles)
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

    override val authorizer: Authorizer =
        object : Authorizer {
            override suspend fun authorize(
                principal: Principal,
                action: AuthAction,
            ) {
                val roles = store.grantsByRole()
                val (kind, resource) =
                    when (action) {
                        is AuthAction.SessionBegin -> "read" to ResourcePath.of(action.namespace)
                        is AuthAction.SqlExec -> (if (action.readOnly) "read" else "write") to ResourcePath.of(action.namespace)
                        is AuthAction.TxCommit -> "write" to ResourcePath.of(action.namespace)
                        is AuthAction.PeerSync -> "sync" to ResourcePath.of(action.namespace)
                        is AuthAction.DocumentWrite -> "write" to ResourcePath.of(action.namespace, action.docId)
                        is AuthAction.DocumentDelete -> "write" to ResourcePath.of(action.namespace, action.docId)
                        is AuthAction.DocumentRead -> "read" to ResourcePath.of(action.namespace, action.docId)
                        is AuthAction.Admin -> "admin" to ResourcePath.of(action.scope)
                    }
                if (!principalHasPermission(principal, roles, kind, resource)) {
                    throw KdbAuthorizationException(
                        "principal ${principal.id} lacks $kind on ${resource.namespaceId}" +
                            (resource.documentId?.let { "/$it" } ?: ""),
                    )
                }
            }
        }
}
