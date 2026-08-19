package dev.kdb.script

import dev.kdb.auth.AuthAction
import dev.kdb.auth.Authorizer
import dev.kdb.auth.Principal
import dev.kdb.dag.CommitDag
import dev.kdb.index.IndexManager
import dev.kdb.query.hybrid.HybridQueryEngine
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter
import dev.kdb.transaction.TransactionEngine

public interface ProcedureRuntime {
    public suspend fun invoke(
        principal: Principal,
        namespaceId: String,
        name: String,
        argsJson: String,
        limits: ProcLimits = ProcLimits.DEFAULT,
    ): ProcResult
}

public fun graalProcedureRuntime(
    registry: ProcedureRegistry,
    hybrid: HybridQueryEngine,
    dag: CommitDag,
    storage: StorageAdapter,
    schema: KdbSchema,
    txEngine: TransactionEngine,
    indexManager: IndexManager,
    authorizer: Authorizer,
    maxCallDepth: Int = 3,
): ProcedureRuntime =
    GraalProcedureRuntime(registry, hybrid, dag, storage, schema, txEngine, indexManager, authorizer, maxCallDepth)
