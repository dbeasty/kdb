package dev.kdb.jdbc

import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.schema.isNone
import java.sql.DriverManager
import java.sql.Statement
import java.sql.Types
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.After
import org.junit.Before

class JdbcPhase4Test {
    @Before
    fun setUp() {
        JdbcTestSupport.clearMemoryRegistries()
    }

    @After
    fun tearDown() {
        JdbcTestSupport.clearMemoryRegistries()
    }

    @Test
    fun createTableViaJdbc() {
        val url = "jdbc:kdb:memory:///phase4/users;unique=true"
        DriverManager.getConnection(url).use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate(
                    """CREATE TABLE users (
                    userId VARCHAR NOT NULL,
                    status VARCHAR NOT NULL
                )""",
                )
            }
            val kdb = conn as KdbConnection
            assertTrue(!kdb.effectiveKdbSchema().isNone)
        }
    }

    @Test
    fun insertReturnsGeneratedKeys() {
        val url = "jdbc:kdb:memory:///phase4/users;unique=true"
        DriverManager.getConnection(url).use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate(
                    """CREATE TABLE users (
                    userId VARCHAR NOT NULL,
                    status VARCHAR NOT NULL
                )""",
                )
            }
            conn.createStatement().use { st ->
                val count =
                    st.executeUpdate(
                        "INSERT INTO users (userId, status) VALUES ('u1', 'active')",
                        Statement.RETURN_GENERATED_KEYS,
                    )
                assertEquals(1, count)
                st.generatedKeys.use { keys ->
                    assertTrue(keys.next())
                    keys.getString(1)
                    assertTrue(keys.getString(1)!!.isNotEmpty())
                }
            }
        }
    }

    @Test
    fun preparedStatementBatchInsert() {
        val url = "jdbc:kdb:memory:///phase4/batch;unique=true"
        val schema =
            KdbSchema.build(
                listOf(
                    SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                ),
            )
        DriverManager.getConnection(url).use { conn ->
            val kdb = conn as KdbConnection
            JdbcTestSupport.seedUsers(kdb, schema)
            conn.prepareStatement("UPDATE users SET userId = ? WHERE userId = ?").use { ps ->
                ps.setString(1, "u2")
                ps.setString(2, "u1")
                ps.addBatch()
                ps.setString(1, "u3")
                ps.setString(2, "u2")
                ps.addBatch()
                val counts = ps.executeBatch()
                assertEquals(2, counts.size)
                assertEquals(1, counts[0])
                assertEquals(1, counts[1])
            }
        }
    }

    @Test
    fun metadataSupportsBatch() {
        val url = "jdbc:kdb:memory:///phase4/meta;unique=true"
        DriverManager.getConnection(url).use { conn ->
            assertTrue(conn.metaData.supportsBatchUpdates())
            assertTrue(conn.metaData.supportsGroupBy())
        }
    }
}
