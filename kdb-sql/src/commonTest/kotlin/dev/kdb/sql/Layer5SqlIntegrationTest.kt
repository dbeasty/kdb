package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.codec.KdbTimestamp
import dev.kdb.index.IndexManager
import dev.kdb.index.IndexRegistry
import dev.kdb.index.productionIndexManager
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class Layer5SqlIntegrationTest {
    @Test
    fun selectByIndexedField() =
        runTest {
            val fx = seededFixture()
            val result =
                fx.engine.execute(
                    "SELECT userId, status, _doc FROM users WHERE userId = 'u1'",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertTrue(result.rows.isNotEmpty())
            assertEquals("u1", (result.rows.first().values[0] as SqlCell.StringVal).value)
        }

    @Test
    fun parseSelectStar() {
        val q = defaultSqlParser().parse("SELECT * FROM notes") as SqlStatement.Select
        assertTrue(q.query.projections.any { it is SelectProjection.Star })
    }

    @Test
    fun orderByLimit() =
        runTest {
            val fx =
                seededFixture(
                    initialJson = """{"userId":"u1","status":"active","rank":2}""",
                    fields =
                        listOf(
                            SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                            SchemaField("status", KdbFieldType.StringType, required = true, indexed = true),
                            SchemaField("rank", KdbFieldType.Int64Type, required = true, indexed = true),
                        ),
                )
            fx.commitDoc("""{"userId":"u2","status":"active","rank":1}""")
            val desc =
                fx.engine.execute(
                    "SELECT userId FROM users ORDER BY rank DESC LIMIT 1",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertEquals(1, desc.rows.size)
            assertEquals("u1", (desc.rows.first().values[0] as SqlCell.StringVal).value)
        }

    @Test
    fun betweenPredicate() =
        runTest {
            val fx =
                seededFixture(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                        SchemaField("score", KdbFieldType.Int64Type, required = true, indexed = true),
                    ),
                    initialJson = """{"userId":"u1","status":"active","score":10}""",
                )
            fx.commitDoc("""{"userId":"u2","status":"active","score":20}""")
            val result =
                fx.engine.execute(
                    "SELECT userId FROM users WHERE score BETWEEN 5 AND 15",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertEquals(1, result.rows.size)
            assertEquals("u1", (result.rows.first().values[0] as SqlCell.StringVal).value)
        }

    @Test
    fun preparedParameterBinding() =
        runTest {
            val fx = seededFixture()
            val pq =
                fx.engine.prepare(
                    "SELECT userId FROM users WHERE userId = ?",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertEquals(1, pq.parameterCount)
            val result =
                pq.execute(
                    listOf(SqlParameter.StringParam("u1")),
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertEquals(1, result.rows.size)
        }

    @Test
    fun explainShowsIndexScan() =
        runTest {
            val fx = seededFixture()
            val plan =
                fx.engine.explain(
                    "SELECT userId FROM users WHERE userId = 'u1'",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertTrue(containsIndexScan(plan.plan))
        }

    @Test
    fun unknownColumnThrows() =
        runTest {
            val fx = seededFixture()
            assertFailsWith<SqlPlanningException> {
                fx.engine.execute(
                    "SELECT notACol FROM users",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            }
        }

    @Test
    fun parseErrorOnBadSyntax() {
        assertFailsWith<SqlParseException> {
            defaultSqlParser().parse("SELEC * FROM users")
        }
    }

    @Test
    fun distinctRows() =
        runTest {
            val fx = seededFixture()
            fx.commitDoc("""{"userId":"u2","status":"active"}""")
            val result =
                fx.engine.execute(
                    "SELECT DISTINCT status FROM users",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertEquals(1, result.rows.size)
        }

    @Test
    fun inListPredicate() =
        runTest {
            val fx = seededFixture()
            fx.commitDoc("""{"userId":"u2","status":"inactive"}""")
            val result =
                fx.engine.execute(
                    "SELECT userId FROM users WHERE userId IN ('u1', 'u2')",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertEquals(2, result.rows.size)
        }

    @Test
    fun countStar() =
        runTest {
            val fx = seededFixture()
            fx.commitDoc("""{"userId":"u2","status":"active"}""")
            val result =
                fx.engine.execute(
                    "SELECT COUNT(*) AS n FROM users",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertEquals(1, result.rows.size)
            assertEquals(2L, (result.rows.first().values[0] as SqlCell.LongVal).value)
        }

    @Test
    fun isNotNullPredicate() =
        runTest {
            val fx = seededFixture()
            val result =
                fx.engine.execute(
                    "SELECT userId FROM users WHERE userId IS NOT NULL",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertEquals(1, result.rows.size)
        }

    @Test
    fun groupByStatusWithCount() =
        runTest {
            val fx = seededFixture()
            fx.commitDoc("""{"userId":"u2","status":"active"}""")
            val result =
                fx.engine.execute(
                    "SELECT status, COUNT(*) AS n FROM users GROUP BY status",
                    QueryContext(namespaceId = fx.ns, schema = fx.schema),
                )
            assertEquals(1, result.rows.size)
            assertEquals(2L, (result.rows.first().values[1] as SqlCell.LongVal).value)
        }

    @Test
    fun innerJoinUsersAndOrders() =
        runTest {
            val fx = joinFixture()
            val ctx =
                QueryContext(
                    namespaceId = fx.usersNs,
                    schema = fx.usersSchema,
                    namespacesByTable =
                        mapOf(
                            "users" to NamespaceBinding(fx.usersNs, fx.usersSchema),
                            "orders" to NamespaceBinding(fx.ordersNs, fx.ordersSchema),
                        ),
                )
            val result =
                fx.engine.execute(
                    "SELECT u.userId, o.orderId FROM users u INNER JOIN orders o ON u.userId = o.userId",
                    ctx,
                )
            assertEquals(1, result.rows.size)
            assertEquals("u1", (result.rows.first().values[0] as SqlCell.StringVal).value)
            assertEquals("o1", (result.rows.first().values[1] as SqlCell.StringVal).value)
        }

    private fun containsIndexScan(plan: PhysicalPlan): Boolean =
        when (plan) {
            is PhysicalPlan.IndexScan -> true
            is PhysicalPlan.InListScan -> true
            is PhysicalPlan.Filter -> containsIndexScan(plan.input)
            is PhysicalPlan.Limit -> containsIndexScan(plan.input)
            is PhysicalPlan.Project -> containsIndexScan(plan.input)
            is PhysicalPlan.Sort -> containsIndexScan(plan.input)
            else -> false
        }

    private class SqlFixture(
        val engine: SqlEngine,
        val ns: String,
        val schema: KdbSchema,
        private val dag: CommitDag,
        private val storage: StorageAdapter,
        private val manager: IndexManager,
        private val registry: IndexRegistry,
    ) {
        suspend fun commitDoc(json: String) {
            val docId = KdbUuid.random()
            storage.putDocument(ns, KdbDocument(docId, json))
            val parentCommit = dag.head()
            val parentTree = dag.getCommitOrThrow(parentCommit).documentTreeHash
            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = parentCommit,
                    operations = listOf(KdbOp.Write(docId, json)),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val tree = storage.commitTree(ns, parentTree)
            val commit = dag.appendCommit(tx, parentCommit, tree, schemaHash = null)
            manager.writer.applyCommit(commit, registry, storage, schema)
        }
    }

    private suspend fun seededFixture(
        fields: List<SchemaField> =
            listOf(
                SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true, unique = true),
                SchemaField("status", KdbFieldType.StringType, required = true, indexed = true),
            ),
        initialJson: String = """{"userId":"u1","status":"active","extra":1}""",
    ): SqlFixture {
        val ns = "app/users"
        val dag = inMemoryCommitDag(ns)
        val storage = InMemoryStorageAdapter()
        val manager = productionIndexManager(dag, storage)
        manager.bindNamespace(ns, dag)
        val schema = KdbSchema.build(fields = fields)
        val registry = manager.registryFor(ns)
        registry.syncSchema(
            KdbSchema.NONE,
            schema,
            dev.kdb.index.compositeIndexStoreFactory(dag, storage),
            dag,
            storage,
        )
        val fx = SqlFixture(sqlEngine(manager, storage, dag), ns, schema, dag, storage, manager, registry)
        fx.commitDoc(initialJson)
        return fx
    }

    private suspend fun joinFixture(): JoinFixture {
        val usersNs = "app/users"
        val ordersNs = "app/orders"
        val usersDag = inMemoryCommitDag(usersNs)
        val ordersDag = inMemoryCommitDag(ordersNs)
        val storage = InMemoryStorageAdapter()
        val manager = productionIndexManager(usersDag, storage)
        manager.bindNamespace(usersNs, usersDag)
        manager.bindNamespace(ordersNs, ordersDag)
        val usersSchema =
            KdbSchema.build(
                listOf(
                    SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                ),
            )
        val ordersSchema =
            KdbSchema.build(
                listOf(
                    SchemaField("orderId", KdbFieldType.StringType, required = true, indexed = true),
                    SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                ),
            )
        val usersRegistry = manager.registryFor(usersNs)
        usersRegistry.syncSchema(
            KdbSchema.NONE,
            usersSchema,
            dev.kdb.index.compositeIndexStoreFactory(usersDag, storage),
            usersDag,
            storage,
        )
        val ordersRegistry = manager.registryFor(ordersNs)
        ordersRegistry.syncSchema(
            KdbSchema.NONE,
            ordersSchema,
            dev.kdb.index.compositeIndexStoreFactory(ordersDag, storage),
            ordersDag,
            storage,
        )
        val engine =
            sqlEngine(
                manager,
                storage,
                usersDag,
                namespaceDags = mapOf(ordersNs to ordersDag),
            )
        val fx =
            JoinFixture(
                engine,
                usersNs,
                ordersNs,
                usersSchema,
                ordersSchema,
                usersDag,
                ordersDag,
                storage,
                manager,
                usersRegistry,
                ordersRegistry,
            )
        fx.commitUsers("""{"userId":"u1"}""")
        fx.commitOrders("""{"orderId":"o1","userId":"u1"}""")
        return fx
    }

    private class JoinFixture(
        val engine: SqlEngine,
        val usersNs: String,
        val ordersNs: String,
        val usersSchema: KdbSchema,
        val ordersSchema: KdbSchema,
        private val usersDag: CommitDag,
        private val ordersDag: CommitDag,
        private val storage: StorageAdapter,
        private val manager: IndexManager,
        private val usersRegistry: IndexRegistry,
        private val ordersRegistry: IndexRegistry,
    ) {
        suspend fun commitUsers(json: String) = commitDoc(usersNs, usersDag, usersRegistry, usersSchema, json)

        suspend fun commitOrders(json: String) = commitDoc(ordersNs, ordersDag, ordersRegistry, ordersSchema, json)

        private suspend fun commitDoc(
            ns: String,
            dag: CommitDag,
            registry: IndexRegistry,
            schema: KdbSchema,
            json: String,
        ) {
            val docId = KdbUuid.random()
            storage.putDocument(ns, KdbDocument(docId, json))
            val parentCommit = dag.head()
            val parentTree = dag.getCommitOrThrow(parentCommit).documentTreeHash
            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = parentCommit,
                    operations = listOf(KdbOp.Write(docId, json)),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val tree = storage.commitTree(ns, parentTree)
            val commit = dag.appendCommit(tx, parentCommit, tree, schemaHash = null)
            manager.writer.applyCommit(commit, registry, storage, schema)
        }
    }
}
