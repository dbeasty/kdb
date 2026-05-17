package dev.kdb.auth

public data class Principal(
    val id: String,
    val roles: Set<String> = emptySet(),
    val claims: Map<String, String> = emptyMap(),
)

public data class AuthCredentials(
    val user: String? = null,
    val password: String? = null,
    val token: String? = null,
)

public data class ConnectionContext(
    val user: String? = null,
    val password: String? = null,
    val token: String? = null,
    val headers: Map<String, String> = emptyMap(),
) {
    public fun toCredentials(): AuthCredentials =
        AuthCredentials(
            user = user,
            password = password,
            token = token,
        )

    public companion object {
        public val EMPTY: ConnectionContext = ConnectionContext()
    }
}

public sealed class AuthAction {
    public data class SessionBegin(val namespace: String) : AuthAction()

    public data class SqlExec(
        val namespace: String,
        val readOnly: Boolean,
    ) : AuthAction()

    public data class TxCommit(val namespace: String) : AuthAction()

    public data class PeerSync(val namespace: String) : AuthAction()
}
