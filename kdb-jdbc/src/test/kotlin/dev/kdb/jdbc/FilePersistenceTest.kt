package dev.kdb.jdbc

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.jdbc.file.openFileRuntime
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import java.sql.DriverManager
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before

class FilePersistenceTest {
    @Before
    fun setUp() {
        JdbcTestSupport.clearMemoryRegistries()
    }

    @After
    fun tearDown() {
        JdbcTestSupport.clearMemoryRegistries()
    }
    private val usersSchema =
        KdbSchema.build(
            listOf(
                SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
            ),
        )

    init {
        KdbDriver
    }

    @Test
    fun fileRuntime_roundTrip() =
        runBlocking {
            val root = createTempDirectory("kdb-file").toString()
            val ns = "demo/users"
            seedUsers(openFileRuntime(root, "demo", ns, usersSchema), usersSchema)
            val again = openFileRuntime(root, "demo", ns, usersSchema)
            val result =
                again.hybrid.execute(
                    "SELECT _doc FROM users WHERE userId = 'u1'",
                    dev.kdb.query.hybrid.HybridQueryRequest(ns, usersSchema),
                )
            assertEquals(1, result.result.rows.size)
        }

    @Test
    fun jdbcFileUrl_roundTrip() {
        val root = createTempDirectory("kdb-jdbc-file").toString()
        val url = "jdbc:kdb:file://$root/demo/users"
        runBlocking {
            seedUsers((DriverManager.getConnection(url) as KdbConnection).embedded, usersSchema)
        }
        DriverManager.getConnection(url).use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT _doc FROM users")
            var found = false
            while (rs.next()) {
                if (rs.getString(1)?.contains("u1") == true) {
                    found = true
                    break
                }
            }
            assertTrue(found)
        }
    }

    @Test
    fun fileJdbcEmbeddedTransaction() {
        val root = createTempDirectory("kdb-file-tx").toString()
        val url = "jdbc:kdb:file://$root/demo/users"
        val schema = JdbcTestSupport.usersSchema()
        DriverManager.getConnection(url).use { conn ->
            val kdb = conn as KdbConnection
            JdbcTestSupport.seedUsers(kdb, schema)
            kdb.autoCommit = false
            val updated =
                kdb.createStatement().executeUpdate(
                    "UPDATE users SET status = 'committed' WHERE userId = 'u1'",
                )
            assertEquals(1, updated)
            kdb.commit()
        }
        DriverManager.getConnection(url).use { conn ->
            val kdb = conn as KdbConnection
            kdb.applyQuerySchema(schema)
            val rs = kdb.createStatement().executeQuery("SELECT _doc FROM users")
            var found = false
            while (rs.next()) {
                val doc = rs.getString(1) ?: continue
                if (doc.contains("u1") && doc.contains("committed")) {
                    found = true
                    break
                }
            }
            assertTrue(found)
        }
    }

    @Test
    fun replay_idempotent() =
        runBlocking {
            val root = createTempDirectory("kdb-replay").toString()
            val ns = "demo/t"
            seedUsers(openFileRuntime(root, "demo", ns, usersSchema), usersSchema)
            val a = openFileRuntime(root, "demo", ns, usersSchema)
            val b = openFileRuntime(root, "demo", ns, usersSchema)
            assertEquals(a.dag.head(), b.dag.head())
        }

    private suspend fun seedUsers(runtime: EmbeddedKdbRuntime, schema: KdbSchema) {
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
        val doc = KdbDocument(KdbUuid.random(), """{"userId":"u1"}""")
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
    }
}
