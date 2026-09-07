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
import dev.kdb.transaction.UniqueConstraintViolationException
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
    message: String = "",
): KdbCommit {
    val policy = runtime.policyRegistry.get(namespaceId)
    val txEngine = engine ?: transactionEngine(policy.conflict)
    val lockSession = sessionId ?: "embed-single-shot"
    if (documentLocks != null) {
        documentLocks.acquireAllForTransaction(namespaceId, lockSession, transaction)
    }
    return try {
        when (val result = txEngine.commit(transaction, runtime.dag, runtime.storage, schema, targetHead, message)) {
        is TransactionResult.Success -> {
            // Unconditional since Layer 16 (§9.2, §10): FULLTEXT and VECTOR indexes are allowed on
            // schemaless namespaces - their fields are JSON paths, not schema columns - so gating
            // this on a declared schema left every such index silently unfed. applyCommit is a
            // no-op when the namespace has no indexes at all.
            runtime.indexManager.writer.applyCommit(
                result.commit,
                runtime.indexManager.registryFor(namespaceId),
                runtime.storage,
                schema,
            )
            // Component 44: the one place every commit path (SQL wire, embedded JDBC, any
            // future caller) converges, so notifying here is what actually covers all of them.
            runtime.notifyCommit(namespaceId, result.commit)
            result.commit
        }
        is TransactionResult.Conflict ->
            throw ConflictException(
                "transaction conflict: ${result.report.conflicts.size} operation(s)",
                result.report,
            )
        is TransactionResult.SchemaError -> {
            // Layer 16 §9.6: a unique-constraint violation is a distinct, typed failure (the transaction
            // is well-formed; the value is taken) so the wire can report UNIQUE_VIOLATION.
            result.violations.firstNotNullOfOrNull {
                UniqueConstraintViolationException.fromViolation(namespaceId, it)
            }?.let { throw it }
            throw IllegalArgumentException(
                "schema rejection: ${result.violations.size} violation(s)",
            )
        }
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
