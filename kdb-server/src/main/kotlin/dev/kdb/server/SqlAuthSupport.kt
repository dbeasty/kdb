package dev.kdb.server

import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.KdbAuthenticationException
import dev.kdb.auth.KdbAuthorizationException
import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.Principal
import dev.kdb.sql.SqlStatement
import dev.kdb.sql.isDdlStatement
import dev.kdb.sql.isDmlStatement

internal fun sqlRequiresWrite(stmt: SqlStatement): Boolean =
    isDmlStatement(stmt) ||
        isDdlStatement(stmt) ||
        stmt is SqlStatement.Commit

internal class SqlAuthSupport(
    private val auth: AuthEngine,
    private val connectionContext: ConnectionContext,
) {
    var connectionPrincipal: Principal? = null

    suspend fun authenticateConnection(): Principal {
        val principal = auth.authenticator.authenticate(connectionContext.toCredentials())
        connectionPrincipal = principal
        return principal
    }

    suspend fun authorize(
        principal: Principal?,
        action: AuthAction,
    ) {
        val effective = principal ?: connectionPrincipal ?: authenticateConnection()
        auth.authorizer.authorize(effective, action)
    }

    fun authFailureMessage(e: Throwable): String =
        when (e) {
            is KdbAuthenticationException -> "authentication failed: ${e.message}"
            is KdbAuthorizationException -> "forbidden: ${e.message}"
            else -> e.message ?: e.toString()
        }
}
