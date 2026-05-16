package dev.kdb.query.hybrid

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitRef
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.index.productionIndexManager
import dev.kdb.policy.HistoryMode
import dev.kdb.policy.cacheNoHistory
import dev.kdb.policy.inMemoryNamespacePolicyRegistry
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.sql.QueryContext
import dev.kdb.sql.SqlCell
import dev.kdb.sql.sqlEngine
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class HybridQueryTest {
    @Test
    fun stripAtVersion() {
        val (sql, version) =
            DefaultHybridSqlParser.stripVersionClause(
                "SELECT * FROM users AT VERSION 'v1'",
            )
        assertEquals("SELECT * FROM users", sql)
        assertTrue(version is VersionClause.AtTag)
        assertEquals("v1", (version as VersionClause.AtTag).tag)
    }

    @Test
    fun historyNoneRejectsVersion() =
        runTest {
            val ns = "app/users"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val policies = inMemoryNamespacePolicyRegistry()
            policies.put(cacheNoHistory(ns))
            val engine =
                hybridQueryEngine(
                    sqlEngine(productionIndexManager(dag, storage), storage, dag),
                    dag,
                    policies,
                )
            assertFailsWith<HistoryDisabledException> {
                engine.execute(
                    "SELECT * FROM users AT VERSION 'v1'",
                    HybridQueryRequest(ns, KdbSchema.NONE),
                )
            }
        }

    @Test
    fun selectDocAtHead() =
        runTest {
            val ns = "app/users"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val manager = productionIndexManager(dag, storage)
            manager.bindNamespace(ns, dag)
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
            val policies = inMemoryNamespacePolicyRegistry()
            val registry = manager.registryFor(ns)
            registry.syncSchema(
                KdbSchema.NONE,
                schema,
                compositeIndexStoreFactory(dag, storage),
                dag,
                storage,
            )
            val docId = KdbUuid.random()
            val doc = KdbDocument(docId, """{"userId":"u1"}""")
            storage.putDocument(ns, doc)
            val parent = dag.head()
            val tree = storage.commitTree(ns, dag.getCommitOrThrow(parent).documentTreeHash)
            val tx =
                KdbTransaction(
                    KdbUuid.random(),
                    parent,
                    listOf(KdbOp.Write(docId, doc.json)),
                    KdbTimestamp.now(),
                    KdbUuid.random(),
                )
            val commit = dag.appendCommit(tx, parent, tree, null)
            manager.writer.applyCommit(commit, manager.registryFor(ns), storage, schema)
            val hybrid =
                hybridQueryEngine(
                    sqlEngine(manager, storage, dag),
                    dag,
                    policies,
                )
            val result =
                hybrid.execute(
                    "SELECT _doc FROM users WHERE userId = 'u1'",
                    HybridQueryRequest(ns, schema),
                )
            assertTrue(result.result.rows.isNotEmpty())
            val docCell = result.result.rows.first().values.last() as SqlCell.JsonVal
            assertEquals("""{"userId":"u1"}""", docCell.json)
        }
}
