package dev.kdb.jdbc

import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.junit.After
import org.junit.Before

class KdbJdbcTest {
    init {
        KdbDriver
    }

    @Before
    fun setUp() {
        JdbcTestSupport.clearMemoryRegistries()
    }

    @After
    fun tearDown() {
        JdbcTestSupport.clearMemoryRegistries()
    }

    private fun memoryUrl(): String = "jdbc:kdb:memory:///demo/users;unique=true"

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
        val conn = DriverManager.getConnection(memoryUrl()) as KdbConnection
        JdbcTestSupport.seedUsers(conn)
        val rs = conn.createStatement().executeQuery("SELECT _doc FROM users WHERE userId = 'u1'")
        assertTrue(rs.next())
        assertTrue(rs.getString(1)!!.contains("u1"))
        conn.close()
    }

    @Test
    fun catalogMatchesUrl() {
        val conn =
            DriverManager.getConnection("jdbc:kdb:memory:///myapp/users;unique=true") as KdbConnection
        assertEquals("myapp", conn.catalog)
        conn.close()
    }

    @Test
    fun metadataTables() {
        val conn = DriverManager.getConnection(memoryUrl()) as KdbConnection
        val rs = conn.metaData.getTables(null, null, null, null)
        assertTrue(rs.next())
        assertEquals("demo", rs.getString("TABLE_CAT"))
        conn.close()
    }

    @Test
    fun metadataColumns() {
        val conn = DriverManager.getConnection(memoryUrl()) as KdbConnection
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
        val conn = DriverManager.getConnection(memoryUrl()) as KdbConnection
        JdbcTestSupport.seedUsers(conn)
        val ps = conn.prepareStatement("SELECT _doc FROM users WHERE userId = 'u1'")
        val rs = ps.executeQuery()
        assertTrue(rs.next())
        assertTrue(rs.getString(1)!!.contains("u1"))
        conn.close()
    }

    @Test
    fun executeUpdateSchemaField() {
        val conn = DriverManager.getConnection(memoryUrl()) as KdbConnection
        JdbcTestSupport.seedUsers(conn)
        val updated =
            conn.createStatement().executeUpdate(
                "UPDATE users SET status = 'inactive' WHERE userId = 'u1'",
            )
        assertEquals(1, updated)
        val rs = conn.createStatement().executeQuery("SELECT _doc FROM users WHERE userId = 'u1'")
        assertTrue(rs.next())
        assertTrue(rs.getString(1)!!.contains("inactive"))
        conn.close()
    }

    @Test
    fun readOnlyRejectsUpdate() {
        val conn =
            DriverManager.getConnection(
                memoryUrl(),
                java.util.Properties().apply { setProperty("readOnly", "true") },
            ) as KdbConnection
        assertFailsWith<SQLException> {
            conn.createStatement().executeUpdate("UPDATE users SET userId = 'x'")
        }
        conn.close()
    }

    @Test
    fun closeConnection() {
        val conn = DriverManager.getConnection(memoryUrl())
        conn.close()
        assertFailsWith<SQLException> {
            conn.createStatement().executeQuery("SELECT 1")
        }
    }
}
