package dev.kdb.server

import dev.kdb.auth.Principal
import dev.kdb.codec.KdbHash
import dev.kdb.query.hybrid.CheckoutHandle
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.transaction.TransactionBuilder

public data class SessionId(val value: String)

public data class KdbSession(
    val id: SessionId,
    val namespaceId: String,
    var baseVersion: KdbHash,
    var readPin: KdbHash?,
    var readConsistency: ReadConsistency,
    var pending: TransactionBuilder?,
    var sessionCheckout: CheckoutHandle? = null,
    var autoCommit: Boolean = true,
    var principal: Principal? = null,
    // Drops the DAG retention pin taken for readPin - null whenever readPin is, since a
    // non-SNAPSHOT session reads the live head, which is a branch head and therefore already a
    // retention root. Holding the hash in readPin is not by itself protection: nothing stops a
    // compaction reclaiming that commit while a session reads at it, because a read pin is not a
    // branch head - see CommitDag.pin. Set by SessionManager.begin and SqlWireHost's
    // finishCommittedSession; released by SessionManager.end.
    var pinRelease: (suspend () -> Unit)? = null,
)
