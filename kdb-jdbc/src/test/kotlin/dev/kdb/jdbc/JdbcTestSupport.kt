package dev.kdb.jdbc

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.jdbc.memory.MemoryRuntimeRegistry
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField

internal object JdbcTestSupport {
    fun clearMemoryRegistries() {
        MemoryRuntimeRegistry.clearAllBlocking()
    }

    fun usersSchema(): KdbSchema =
        KdbSchema.build(
            listOf(
                SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                SchemaField("status", KdbFieldType.StringType, required = true, indexed = true),
            ),
        )

    fun seedUsers(
        conn: KdbConnection,
        schema: KdbSchema = usersSchema(),
    ) {
        conn.blocking {
            val runtime = conn.embedded
            val ns = runtime.defaultNamespace
            val dag = runtime.dag
            val storage = runtime.storage
            val manager = runtime.indexManager
            manager.registryFor(ns).syncSchema(
                KdbSchema.NONE,
                schema,
                compositeIndexStoreFactory(dag, storage),
                dag,
                storage,
            )
            val doc = KdbDocument(KdbUuid.random(), """{"userId":"u1","status":"active"}""")
            storage.putDocument(ns, doc)
            val parent = dag.head()
            val tree = storage.commitTree(ns, dag.getCommitOrThrow(parent).documentTreeHash)
            val tx =
                KdbTransaction(
                    KdbUuid.random(),
                    parent,
                    listOf(KdbOp.Write(doc.id, doc.json)),
                    KdbTimestamp.now(),
                    KdbUuid.random(),
                )
            val commit = dag.appendCommit(tx, parent, tree, null)
            manager.writer.applyCommit(commit, manager.registryFor(ns), storage, schema)
            conn.applyQuerySchema(schema)
        }
    }
}
