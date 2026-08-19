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

    /** Per-document write/delete check, resolved at document > collection > database grant
     * specificity. Raised by the Transaction Engine for each op in a transaction, not just at
     * the wire layer, so a grant scoped below the namespace can actually be enforced. */
    public data class DocumentWrite(val namespace: String, val docId: String) : AuthAction()

    public data class DocumentDelete(val namespace: String, val docId: String) : AuthAction()

    public data class DocumentRead(val namespace: String, val docId: String) : AuthAction()

    /** RBAC admin surface: CREATE/DROP ROLE, GRANT/REVOKE, CREATE/DROP USER (see
     * docs/kdb-rbac-plan.md phase 4). Gated behind the "admin" permission kind — deliberately
     * separate from "write" so ordinary write access to a namespace never implies the ability to
     * change who has access to it. [scope] defaults to the reserved system namespace; it is not
     * the target database of the GRANT/REVOKE, since managing roles isn't itself scoped to one
     * business namespace. */
    public data class Admin(val scope: String = "_system") : AuthAction()

    /** May this principal invoke the named stored procedure at all (Layer 11 Component 32)? */
    public data class ProcExec(
        val namespace: String,
        val procName: String,
        val readOnly: Boolean,
    ) : AuthAction()

    /** May this principal define/update/delete a stored procedure in this namespace? */
    public data class ProcManage(val namespace: String) : AuthAction()
}
