package dev.kdb.transaction

import dev.kdb.codec.*
import dev.kdb.dag.CommitDag
import dev.kdb.document.*
import dev.kdb.schema.*
import dev.kdb.storage.StorageAdapter

public interface TransactionEngine {
    val conflictPolicy: ConflictPolicy
    val customResolver: ConflictResolver?

    suspend fun commit(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema = KdbSchema.NONE,
        targetHead: KdbHash? = null,
        message: String = "",
    ): TransactionResult

    suspend fun replay(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema = KdbSchema.NONE,
        replayTarget: KdbHash,
        message: String = "",
    ): TransactionResult

    suspend fun merge(
        primaryHead: KdbHash,
        mergedHead: KdbHash,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema = KdbSchema.NONE,
        message: String = "",
    ): TransactionResult

    suspend fun validate(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
    ): List<OperationViolation>
}

/**
 * [uniqueKeys] (Layer 16 §9.6): when non-null, every commit/replay plans its unique-constraint effect
 * against this registry and rejects violations as [TransactionResult.SchemaError] with
 * [dev.kdb.error.ViolationType.UNIQUE_CONSTRAINT]; the registry is updated only once the commit is in
 * the DAG, and rebuilt when a commit changes the schema. Null keeps the pre-Layer-16 behaviour.
 */
public fun transactionEngine(
    conflictPolicy: ConflictPolicy,
    customResolver: ConflictResolver? = null,
    uniqueKeys: UniqueKeyRegistry? = null,
): TransactionEngine = DefaultTransactionEngine(conflictPolicy, customResolver, uniqueKeys)

public suspend fun transactionBuilder(
    namespaceId: String,
    dag: CommitDag,
    authorNodeId: KdbUuid,
    schema: KdbSchema = KdbSchema.NONE,
): TransactionBuilder =
    TransactionBuilder(
        namespaceId = namespaceId,
        baseVersion = dag.head(),
        authorNodeId = authorNodeId,
        schema = schema,
    )
