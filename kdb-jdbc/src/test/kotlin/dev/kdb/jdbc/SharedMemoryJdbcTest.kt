package dev.kdb.jdbc

import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.After
import org.junit.Before

class SharedMemoryJdbcTest {
    private fun memoryUrl(
        test: String,
        shared: Boolean = false,
    ): String {
        if (shared) {
            return "jdbc:kdb:memory:///pooldemo/users;isolate=$test"
        }
        return "jdbc:kdb:memory:///pooldemo/users;unique=true"
    }

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

    @Test
    fun twoConnectionsShareData() {
        val schema = JdbcTestSupport.usersSchema()
        val shared = memoryUrl("twoConnectionsShareData", shared = true)
        val conn1 = DriverManager.getConnection(shared) as KdbConnection
        JdbcTestSupport.seedUsers(conn1, schema)
        conn1.close()

        val conn2 = DriverManager.getConnection(shared) as KdbConnection
        assertTrue(conn1.embedded === conn2.embedded, "connections must share one runtime")
        conn2.applyQuerySchema(schema)
        val rs = conn2.createStatement().executeQuery("SELECT userId FROM users WHERE userId = 'u1'")
        assertTrue(rs.next())
        assertEquals("u1", rs.getString(1))
        conn2.close()
    }

    @Test
    fun uniqueUrlIsolatesDatabases() {
        val isolated = memoryUrl("uniqueUrlIsolatesDatabases")
        val schema = JdbcTestSupport.usersSchema()
        val a = DriverManager.getConnection(isolated) as KdbConnection
        JdbcTestSupport.seedUsers(a, schema)
        a.close()

        val b = DriverManager.getConnection(isolated) as KdbConnection
        b.applyQuerySchema(schema)
        val rs = b.createStatement().executeQuery("SELECT userId FROM users")
        assertFalse(rs.next())
        b.close()
    }

    @Test
    fun reopenAfterAllConnectionsClosedKeepsData() {
        val schema = JdbcTestSupport.usersSchema()
        val shared = memoryUrl("reopenAfterAllConnectionsClosedKeepsData", shared = true)
        DriverManager.getConnection(shared).use { conn1 ->
            JdbcTestSupport.seedUsers(conn1 as KdbConnection, schema)
        }
        DriverManager.getConnection(shared).use { conn2 ->
            val kdb = conn2 as KdbConnection
            kdb.applyQuerySchema(schema)
            val rs =
                kdb.createStatement().executeQuery("SELECT userId FROM users WHERE userId = 'u1'")
            assertTrue(rs.next())
            assertEquals("u1", rs.getString(1))
        }
    }

    @Test
    fun embeddedTransactionCommitVisibleToOtherConnection() {
        val schema = JdbcTestSupport.usersSchema()
        val shared = memoryUrl("embeddedTransactionCommitVisibleToOtherConnection", shared = true)
        val conn1 = DriverManager.getConnection(shared) as KdbConnection
        JdbcTestSupport.seedUsers(conn1, schema)

        conn1.createStatement().execute("BEGIN")
        val updated =
            conn1.createStatement().executeUpdate(
                "UPDATE users SET status = 'inactive' WHERE userId = 'u1'",
            )
        assertEquals(1, updated)
        conn1.createStatement().execute("COMMIT")
        conn1.close()

        val conn2 = DriverManager.getConnection(shared) as KdbConnection
        conn2.applyQuerySchema(schema)
        val rs =
            conn2.createStatement().executeQuery(
                "SELECT status FROM users WHERE userId = 'u1'",
            )
        assertTrue(rs.next())
        assertEquals("inactive", rs.getString(1))
        conn2.close()
    }

    @Test
    fun beginCommitSqlTransaction() {
        val schema = JdbcTestSupport.usersSchema()
        val shared = memoryUrl("beginCommitSqlTransaction", shared = true)
        val conn = DriverManager.getConnection(shared) as KdbConnection
        JdbcTestSupport.seedUsers(conn, schema)

        conn.autoCommit = false
        conn.createStatement().executeUpdate(
            "UPDATE users SET status = 'pending' WHERE userId = 'u1'",
        )
        conn.commit()
        conn.close()

        val read = DriverManager.getConnection(shared) as KdbConnection
        read.applyQuerySchema(schema)
        val rs =
            read.createStatement().executeQuery(
                "SELECT status FROM users WHERE userId = 'u1'",
            )
        assertTrue(rs.next())
        assertEquals("pending", rs.getString(1))
        read.close()
    }

    @Test
    fun dropOnCloseRemovesDatabase() {
        val dropUrl = "jdbc:kdb:memory:///pooldemo/users;isolate=dropOnClose;dropOnClose=true"
        val schema = JdbcTestSupport.usersSchema()
        val conn = DriverManager.getConnection(dropUrl) as KdbConnection
        JdbcTestSupport.seedUsers(conn, schema)
        conn.close()

        val again = DriverManager.getConnection(dropUrl) as KdbConnection
        again.applyQuerySchema(schema)
        val rs = again.createStatement().executeQuery("SELECT userId FROM users")
        assertFalse(rs.next())
        again.close()
    }
}
