package dev.kdb.hibernate

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.sql.DriverManager
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.hibernate.cfg.AvailableSettings
import org.hibernate.cfg.Configuration

class HibernateJdbcIntegrationTest {
    @Test
    fun hibernateNativeQueryViaKdbJdbc() {
        Class.forName("dev.kdb.jdbc.KdbDriver")
        val url = "jdbc:kdb:memory:///hibernate/users;isolate=hib-${UUID.randomUUID()}"
        seedUsersTable(url)
        val sessionFactory =
            Configuration()
                .setProperty(AvailableSettings.JAKARTA_JDBC_URL, url)
                .setProperty(AvailableSettings.JAKARTA_JDBC_DRIVER, "dev.kdb.jdbc.KdbDriver")
                .setProperty(AvailableSettings.DIALECT, KdbDialect::class.java.name)
                .setProperty(AvailableSettings.HBM2DDL_AUTO, "none")
                .addAnnotatedClass(UserEntity::class.java)
                .buildSessionFactory()
        sessionFactory.openSession().use { session ->
            val doc =
                session
                    .createNativeQuery("SELECT _doc FROM users", String::class.java)
                    .singleResult
            assertTrue(doc.contains("u1") && doc.contains("active"))
        }
        sessionFactory.close()
    }

    private fun seedUsersTable(url: String) {
        DriverManager.getConnection(url).use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate(
                    """
                    CREATE TABLE users (
                        userId VARCHAR NOT NULL,
                        status VARCHAR NOT NULL
                    )
                    """.trimIndent(),
                )
            }
            val inserted =
                conn.prepareStatement("INSERT INTO users (userId, status) VALUES (?, ?)").use { ps ->
                    ps.setString(1, "u1")
                    ps.setString(2, "active")
                    ps.executeUpdate()
                }
            assertEquals(1, inserted)
        }
    }

    @Entity
    @Table(name = "users")
    class UserEntity {
        @Id
        @Column(name = "userId")
        var userId: String = ""

        @Column(name = "status")
        var status: String = ""
    }
}
