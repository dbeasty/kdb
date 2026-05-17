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
)
