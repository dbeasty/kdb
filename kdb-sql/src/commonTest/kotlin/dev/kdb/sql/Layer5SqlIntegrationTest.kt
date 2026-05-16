package dev.kdb.sql

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.codec.KdbTimestamp
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexType
import dev.kdb.index.productionIndexManager
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class Layer5SqlIntegrationTest {
    @Test
    fun selectByIndexedField() =
        runTest {
            val ns = "app/users"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val manager = productionIndexManager(dag, storage)
            manager.bindNamespace(ns, dag)

            val schema =
                KdbSchema.build(
                    fields =
                        listOf(
                            SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true, unique = true),
                            SchemaField("status", KdbFieldType.StringType, required = true, indexed = true),
                        ),
                )

            val registry = manager.registryFor(ns)
            registry.syncSchema(
                KdbSchema.NONE,
                schema,
                dev.kdb.index.compositeIndexStoreFactory(dag, storage),
                dag,
                storage,
            )

            val docId = KdbUuid.random()
            val doc =
                KdbDocument(
                    id = docId,
                    json = """{"userId":"u1","status":"active","extra":1}""",
                )
            storage.putDocument(ns, doc)
            val parentCommit = dag.head()
            val parentTree = dag.getCommitOrThrow(parentCommit).documentTreeHash
            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = parentCommit,
                    operations = listOf(KdbOp.Write(docId, doc.json)),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val tree = storage.commitTree(ns, parentTree)
            assertNotNull(storage.getDocument(ns, docId, tree.treeHash))
            val commit = dag.appendCommit(tx, parentCommit, tree, schemaHash = null)
            manager.writer.applyCommit(commit, registry, storage, schema)

            val hashStore =
                checkNotNull(registry.get("userId", IndexType.HASH)) {
                    "HASH index for userId should exist after syncSchema"
                }
            assertEquals(
                listOf(docId),
                hashStore.lookup(IndexKey.StringKey("u1"), null),
                "index should contain doc after applyCommit",
            )

            val engine = sqlEngine(manager, storage, dag)
            val result =
                engine.execute(
                    "SELECT userId, status, _doc FROM users WHERE userId = 'u1'",
                    QueryContext(namespaceId = ns, schema = schema),
                )
            assertTrue(result.rows.isNotEmpty())
            assertEquals("u1", (result.rows.first().values[0] as SqlCell.StringVal).value)
        }

    @Test
    fun parseSelectStar() {
        val q = defaultSqlParser().parse("SELECT * FROM notes") as SqlStatement.Select
        assertTrue(q.query.projections.any { it is SelectProjection.Star })
    }
}
