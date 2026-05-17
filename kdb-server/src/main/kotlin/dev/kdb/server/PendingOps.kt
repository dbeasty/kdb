package dev.kdb.server

import dev.kdb.document.KdbOp
import dev.kdb.sql.SqlPlanningException

internal suspend fun appendPendingOps(
    builder: LockingTransactionBuilder,
    operations: List<KdbOp>,
) {
    for (op in operations) {
        when (op) {
            is KdbOp.Write -> builder.write(op.docId, op.patch)
            is KdbOp.Delete -> builder.delete(op.docId)
            else ->
                throw SqlPlanningException(
                    "operation ${op::class.simpleName} not supported in SQL transaction",
                    "",
                )
        }
    }
}
