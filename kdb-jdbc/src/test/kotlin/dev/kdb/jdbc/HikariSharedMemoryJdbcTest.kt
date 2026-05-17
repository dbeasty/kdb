package dev.kdb.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.After
import org.junit.Before

class HikariSharedMemoryJdbcTest {
    private fun poolUrl(test: String): String =
        "jdbc:kdb:memory:///pooldemo/users;isolate=hikari_$test"

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
    fun poolConnectionsShareDatabase() {
        val schema = JdbcTestSupport.usersSchema()
        val config =
            HikariConfig().apply {
                jdbcUrl = poolUrl("poolConnectionsShareDatabase")
                maximumPoolSize = 2
                poolName = "kdb-test-pool"
            }
        val ds = HikariDataSource(config)
        try {
            ds.connection.use { c1 ->
                JdbcTestSupport.seedUsers(c1.unwrap(KdbConnection::class.java), schema)
            }
            ds.connection.use { c2 ->
                val conn2 = c2.unwrap(KdbConnection::class.java)
                conn2.applyQuerySchema(schema)
                val rs =
                    conn2.createStatement().executeQuery(
                        "SELECT userId FROM users WHERE userId = 'u1'",
                    )
                assertTrue(rs.next())
                assertEquals("u1", rs.getString(1))
            }
        } finally {
            ds.close()
        }
    }

    @Test
    fun poolTransactionVisibleAfterReturn() {
        val schema = JdbcTestSupport.usersSchema()
        val config =
            HikariConfig().apply {
                jdbcUrl = poolUrl("poolTransactionVisibleAfterReturn")
                maximumPoolSize = 2
                poolName = "kdb-test-pool-tx"
            }
        val ds = HikariDataSource(config)
        try {
            ds.connection.use { c1 ->
                val conn1 = c1.unwrap(KdbConnection::class.java)
                JdbcTestSupport.seedUsers(conn1, schema)
                conn1.autoCommit = false
                conn1.createStatement().execute("BEGIN")
                conn1.createStatement().executeUpdate(
                    "UPDATE users SET status = 'pooled' WHERE userId = 'u1'",
                )
                conn1.createStatement().execute("COMMIT")
            }
            ds.connection.use { c2 ->
                val conn2 = c2.unwrap(KdbConnection::class.java)
                conn2.applyQuerySchema(schema)
                val rs =
                    conn2.createStatement().executeQuery(
                        "SELECT status FROM users WHERE userId = 'u1'",
                    )
                assertTrue(rs.next())
                assertEquals("pooled", rs.getString(1))
            }
        } finally {
            ds.close()
        }
    }
}
