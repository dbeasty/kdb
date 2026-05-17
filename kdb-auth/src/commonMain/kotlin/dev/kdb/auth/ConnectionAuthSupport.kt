package dev.kdb.auth

public class ConnectionAuthSupport(
    private val auth: AuthEngine,
    private val connectionContext: ConnectionContext,
) {
    public var connectionPrincipal: Principal? = null

    public suspend fun authenticateConnection(): Principal {
        val principal = auth.authenticator.authenticate(connectionContext.toCredentials())
        connectionPrincipal = principal
        return principal
    }

    public suspend fun authorize(
        principal: Principal?,
        action: AuthAction,
    ) {
        val effective = principal ?: connectionPrincipal ?: authenticateConnection()
        auth.authorizer.authorize(effective, action)
    }

    public fun authFailureMessage(e: Throwable): String =
        when (e) {
            is KdbAuthenticationException -> "authentication failed: ${e.message}"
            is KdbAuthorizationException -> "forbidden: ${e.message}"
            else -> e.message ?: e.toString()
        }
}
