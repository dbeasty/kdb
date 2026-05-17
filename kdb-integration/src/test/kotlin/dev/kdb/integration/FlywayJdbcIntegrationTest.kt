package dev.kdb.integration

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Flyway-compatible migration smoke test.
 *
 * Flyway 10 does not ship a KDB database adapter; this test runs the same ordered SQL
 * scripts against the JDBC driver the way Flyway would apply them.
 */
class FlywayJdbcIntegrationTest {
    @Test
    fun orderedMigrationScriptsApplyViaJdbc() {
        Class.forName("dev.kdb.jdbc.KdbDriver")
        val url = "jdbc:kdb:memory:///flyway/users;isolate=fly-${UUID.randomUUID()}"
        val migration =
            Files.readString(
                Path.of("src/test/resources/db/migration/V1__create_users.sql"),
            )
        DriverManager.getConnection(url).use { conn ->
            migration
                .split(';')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .forEach { statement ->
                    conn.createStatement().use { st ->
                        st.executeUpdate(statement)
                    }
                }
            conn.prepareStatement("SELECT status FROM users WHERE userId = ?").use { ps ->
                ps.setString(1, "u1")
                ps.executeQuery().use { rs ->
                    assertTrue(rs.next())
                    assertEquals("active", rs.getString(1))
                }
            }
        }
    }
}
