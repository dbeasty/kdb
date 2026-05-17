package dev.kdb.server

import dev.kdb.auth.ConnectionAuthSupport
import dev.kdb.auth.ConnectionContext
import dev.kdb.sql.SqlStatement
import dev.kdb.sql.isDdlStatement
import dev.kdb.sql.isDmlStatement

internal fun sqlRequiresWrite(stmt: SqlStatement): Boolean =
    isDmlStatement(stmt) ||
        isDdlStatement(stmt) ||
        stmt is SqlStatement.Commit

internal class SqlAuthSupport(
    auth: dev.kdb.auth.AuthEngine,
    connectionContext: ConnectionContext,
) {
    private val delegate = ConnectionAuthSupport(auth, connectionContext)

    val connectionPrincipal: dev.kdb.auth.Principal?
        get() = delegate.connectionPrincipal

    suspend fun authenticateConnection() = delegate.authenticateConnection()

    suspend fun authorize(
        principal: dev.kdb.auth.Principal?,
        action: dev.kdb.auth.AuthAction,
    ) = delegate.authorize(principal, action)

    fun authFailureMessage(e: Throwable): String = delegate.authFailureMessage(e)
}
