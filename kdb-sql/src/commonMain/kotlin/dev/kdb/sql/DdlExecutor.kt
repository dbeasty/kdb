package dev.kdb.sql

import dev.kdb.dag.CommitDag
import dev.kdb.index.IndexManager
import dev.kdb.index.IndexStoreFactory
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.schema.isNone
import dev.kdb.storage.StorageAdapter

internal class DdlExecutor(
    private val indexManager: IndexManager,
    private val dag: CommitDag,
    private val storage: StorageAdapter,
    private val indexStoreFactory: IndexStoreFactory,
) {
    suspend fun executeCreateTable(
        ddl: CreateTableStatement,
        context: QueryContext,
    ): KdbSchema {
        if (!context.schema.isNone) {
            throw SqlPlanningException("CREATE TABLE: schema already exists for namespace", ddl.table.name)
        }
        val fields =
            ddl.columns.map { col ->
                SchemaField(
                    name = col.name,
                    type = col.type,
                    required = col.required,
                    indexed = col.indexed,
                )
            }
        return applySchema(context.namespaceId, KdbSchema.NONE, KdbSchema.build(fields))
    }

    suspend fun executeAlterTableAddColumn(
        ddl: AlterTableAddColumnStatement,
        context: QueryContext,
    ): KdbSchema {
        if (context.schema.isNone) {
            throw SqlPlanningException("ALTER TABLE: no schema on namespace", ddl.table.name)
        }
        if (context.schema.hasField(ddl.column.name)) {
            throw SqlPlanningException("column already exists: ${ddl.column.name}", ddl.table.name)
        }
        val fields =
            context.schema.fields +
                SchemaField(
                    name = ddl.column.name,
                    type = ddl.column.type,
                    required = ddl.column.required,
                    indexed = ddl.column.indexed,
                )
        return applySchema(
            context.namespaceId,
            context.schema,
            KdbSchema.build(fields, version = context.schema.version + 1),
        )
    }

    suspend fun executeDropTable(
        table: TableRef,
        context: QueryContext,
    ): KdbSchema {
        if (context.schema.isNone) return KdbSchema.NONE
        return applySchema(context.namespaceId, context.schema, KdbSchema.NONE)
    }

    private suspend fun applySchema(
        namespaceId: String,
        from: KdbSchema,
        to: KdbSchema,
    ): KdbSchema {
        val registry = indexManager.registryFor(namespaceId)
        registry.syncSchema(
            from,
            to,
            indexStoreFactory,
            dag,
            storage,
        )
        if (!to.isNone) {
            indexManager.writer.rebuildAll(
                dag.head(),
                dag,
                registry,
                storage,
                to,
            )
        }
        return to
    }
}
