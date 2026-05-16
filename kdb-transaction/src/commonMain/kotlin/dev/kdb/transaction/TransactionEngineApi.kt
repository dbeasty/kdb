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

public fun transactionEngine(
    conflictPolicy: ConflictPolicy,
    customResolver: ConflictResolver? = null,
): TransactionEngine = DefaultTransactionEngine(conflictPolicy, customResolver)

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
