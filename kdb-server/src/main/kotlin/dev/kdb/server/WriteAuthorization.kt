package dev.kdb.server

import dev.kdb.auth.AuthAction
import dev.kdb.auth.Principal
import dev.kdb.document.KdbOp
import dev.kdb.transaction.WriteAuthorizer

/** Builds the per-transaction [WriteAuthorizer] SqlWireHost passes into [KdbServerRuntime.commit]
 * / [KdbServerRuntime.replay], so every document write/delete in a committed transaction is
 * checked against [principal]'s grants — not just the namespace-level check already done at
 * tx-commit time. File writes and schema migrations aren't document-scoped, so they're covered
 * by that namespace-level check alone. */
internal fun SqlAuthSupport.writeAuthorizerFor(principal: Principal?): WriteAuthorizer =
    WriteAuthorizer { namespaceId, op ->
        val action =
            when (op) {
                is KdbOp.Write -> AuthAction.DocumentWrite(namespaceId, op.docId.toString())
                is KdbOp.Delete -> AuthAction.DocumentDelete(namespaceId, op.docId.toString())
                is KdbOp.FileWrite -> null
                is KdbOp.SchemaMigration -> null
            }
        if (action != null) authorize(principal, action)
    }
