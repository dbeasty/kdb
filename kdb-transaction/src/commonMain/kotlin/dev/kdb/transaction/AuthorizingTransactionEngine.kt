package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter

/** Per-operation authorization check, invoked once for every [KdbOp] in a transaction before it
 * is committed or replayed. Implementations reject by throwing; this module has no opinion on
 * what they throw (the caller — typically the wire layer — owns that). */
public fun interface WriteAuthorizer {
    public suspend fun authorize(
        namespaceId: String,
        op: KdbOp,
    )
}

/**
 * Wraps [delegate] so every operation in a transaction passed to [commit]/[replay] is checked by
 * [authorizer] before it is committed — closing the gap where authorization was previously only
 * checked at the wire layer (session-begin/sql-exec/tx-commit), never at the point writes
 * actually become durable. A caller with its own call path into [TransactionEngine] (bypassing
 * the wire layer) still gets checked. See docs/kdb-rbac-plan.md phase 3.
 *
 * This wraps [TransactionEngine] rather than reaching into [DefaultTransactionEngine]'s
 * internals, so it applies uniformly to any concrete engine and does not touch the
 * conflict-detection/schema-validation hot path. [merge] is passed through unchecked — it
 * reconciles already-committed history rather than introducing new content from a request
 * principal.
 */
public fun authorizingTransactionEngine(
    delegate: TransactionEngine,
    namespaceId: String,
    authorizer: WriteAuthorizer,
): TransactionEngine = AuthorizingTransactionEngine(delegate, namespaceId, authorizer)

private class AuthorizingTransactionEngine(
    private val delegate: TransactionEngine,
    private val namespaceId: String,
    private val authorizer: WriteAuthorizer,
) : TransactionEngine {
    override val conflictPolicy: ConflictPolicy get() = delegate.conflictPolicy
    override val customResolver: ConflictResolver? get() = delegate.customResolver

    override suspend fun commit(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
        targetHead: KdbHash?,
        message: String,
    ): TransactionResult {
        authorizeAll(transaction)
        return delegate.commit(transaction, dag, storage, schema, targetHead, message)
    }

    override suspend fun replay(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
        replayTarget: KdbHash,
        message: String,
    ): TransactionResult {
        authorizeAll(transaction)
        return delegate.replay(transaction, dag, storage, schema, replayTarget, message)
    }

    override suspend fun merge(
        primaryHead: KdbHash,
        mergedHead: KdbHash,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
        message: String,
    ): TransactionResult = delegate.merge(primaryHead, mergedHead, dag, storage, schema, message)

    override suspend fun validate(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
    ): List<OperationViolation> = delegate.validate(transaction, dag, storage, schema)

    private suspend fun authorizeAll(transaction: KdbTransaction) {
        for (op in transaction.operations) {
            authorizer.authorize(namespaceId, op)
        }
    }
}
