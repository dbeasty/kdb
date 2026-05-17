package dev.kdb.jdbc

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import java.sql.DriverManager
import java.sql.SQLException
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class KdbJdbcTest {
    init {
        KdbDriver
    }

    @Test
    fun driverRegisters() {
        assertNotNull(DriverManager.getDriver("jdbc:kdb:memory:///demo/users"))
    }

    @Test
    fun acceptsMemoryUrl() {
        val driver = KdbDriver()
        assertTrue(driver.acceptsURL("jdbc:kdb:memory:///demo/users"))
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost/db"))
    }

    @Test
    fun selectStar() {
        val conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users") as KdbConnection
        seedUsers(conn)
        val rs = conn.createStatement().executeQuery("SELECT _doc FROM users WHERE userId = 'u1'")
        assertTrue(rs.next())
        assertTrue(rs.getString(1)!!.contains("u1"))
        conn.close()
    }

    @Test
    fun catalogMatchesUrl() {
        val conn = DriverManager.getConnection("jdbc:kdb:memory:///myapp/users") as KdbConnection
        assertEquals("myapp", conn.catalog)
        conn.close()
    }

    @Test
    fun metadataTables() {
        val conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users") as KdbConnection
        val rs = conn.metaData.getTables(null, null, null, null)
        assertTrue(rs.next())
        assertEquals("demo", rs.getString("TABLE_CAT"))
        conn.close()
    }

    @Test
    fun metadataColumns() {
        val conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users") as KdbConnection
        val rs = conn.metaData.getColumns(null, null, "users", null)
        val names = mutableListOf<String>()
        while (rs.next()) {
            names += rs.getString("COLUMN_NAME")
        }
        assertTrue("kdb_id" in names)
        assertTrue("_doc" in names)
        conn.close()
    }

    @Test
    fun preparedSelect() {
        val conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users") as KdbConnection
        seedUsers(conn)
        val ps = conn.prepareStatement("SELECT _doc FROM users WHERE userId = 'u1'")
        val rs = ps.executeQuery()
        assertTrue(rs.next())
        assertTrue(rs.getString(1)!!.contains("u1"))
        conn.close()
    }

    @Test
    fun readOnlyRejectsUpdate() {
        val conn =
            DriverManager.getConnection(
                "jdbc:kdb:memory:///demo/users",
                java.util.Properties().apply { setProperty("readOnly", "true") },
            ) as KdbConnection
        assertFailsWith<SQLException> {
            conn.createStatement().executeUpdate("UPDATE users SET userId = 'x'")
        }
        conn.close()
    }

    @Test
    fun closeConnection() {
        val conn = DriverManager.getConnection("jdbc:kdb:memory:///demo/users")
        conn.close()
        assertFailsWith<SQLException> {
            conn.createStatement().executeQuery("SELECT 1")
        }
    }

    private fun seedUsers(conn: KdbConnection) =
        runBlocking {
        val runtime = conn.embedded
        val ns = runtime.defaultNamespace
        val dag = runtime.dag
        val storage = runtime.storage
        val manager = runtime.indexManager
        val schema =
            KdbSchema.build(
                listOf(
                    SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                ),
            )
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
