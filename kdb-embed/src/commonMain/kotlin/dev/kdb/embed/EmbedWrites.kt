package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbTransaction
import dev.kdb.error.ConflictException
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.transaction.DocumentLockManager
import dev.kdb.transaction.TransactionAbortedException
import dev.kdb.transaction.TransactionEngine
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.transactionEngine

/** Commits via [TransactionEngine] using namespace conflict policy. */
public suspend fun commitViaEngine(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    transaction: KdbTransaction,
    schema: KdbSchema = runtime.schema,
    engine: TransactionEngine? = null,
    documentLocks: DocumentLockManager? = null,
    sessionId: String? = null,
    targetHead: KdbHash? = null,
): KdbCommit {
    val policy = runtime.policyRegistry.get(namespaceId)
    val txEngine = engine ?: transactionEngine(policy.conflict)
    val lockSession = sessionId ?: "embed-single-shot"
    if (documentLocks != null) {
        documentLocks.acquireAllForTransaction(namespaceId, lockSession, transaction)
    }
    return try {
        when (val result = txEngine.commit(transaction, runtime.dag, runtime.storage, schema, targetHead)) {
        is TransactionResult.Success -> {
            if (!schema.isNone) {
                runtime.indexManager.writer.applyCommit(
                    result.commit,
                    runtime.indexManager.registryFor(namespaceId),
                    runtime.storage,
                    schema,
                )
            }
            result.commit
        }
        is TransactionResult.Conflict ->
            throw ConflictException(
                "transaction conflict: ${result.report.conflicts.size} operation(s)",
                result.report,
            )
        is TransactionResult.SchemaError ->
            throw IllegalArgumentException(
                "schema rejection: ${result.violations.size} violation(s)",
            )
        is TransactionResult.Aborted ->
            throw TransactionAbortedException(
                "transaction aborted: ${result.cause.message ?: result.cause.toString()}",
                result.cause,
            )
        }
    } finally {
        if (documentLocks != null) {
            documentLocks.releaseAll(lockSession)
        }
    }
}
