package dev.kdb.jdbc

import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.embed.putJson
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.server.KdbServerRuntime
import dev.kdb.server.SqlWireHost
import dev.kdb.transport.ws.JvmNetworkWebSocketServer
import dev.kdb.wire.defaultWireCodec
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class NetworkJdbcIntegrationTest {
    @Test
    fun remotePreparedSelect() =
        runBlocking {
            withTcpSqlServer { jdbcUrl ->
                DriverManager.getConnection(jdbcUrl).use { conn ->
                    conn.prepareStatement("SELECT _doc FROM users WHERE userId = ?").use { ps ->
                        ps.setString(1, "u1")
                        ps.executeQuery().use { rs ->
                            assertTrue(rs.next(), "prepared SELECT should return the seeded row")
                            assertTrue(rs.getString(1)!!.contains("u1"))
                        }
                    }
                    assertTrue(conn.metaData.supportsBatchUpdates())
                }
            }
        }

    @Test
    fun remoteInsertGeneratedKeys() =
        runBlocking {
            val ns = "demo/items"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("itemId", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
            withTcpSqlServer(ns, schema) { jdbcUrl ->
                DriverManager.getConnection(jdbcUrl).use { conn ->
                    conn.prepareStatement(
                        "INSERT INTO items (itemId) VALUES (?)",
                        Statement.RETURN_GENERATED_KEYS,
                    ).use { ps ->
                        ps.setString(1, "i1")
                        assertEquals(1, ps.executeUpdate())
                        ps.generatedKeys.use { keys ->
                            assertTrue(keys.next())
                            assertTrue(keys.getString(1)!!.isNotEmpty())
                        }
                    }
                }
            }
        }

    private suspend fun withTcpSqlServer(
        namespaceId: String = "demo/users",
        schema: KdbSchema? = null,
        block: suspend (String) -> Unit,
    ) {
        val usersSchema =
            schema
                ?: KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                        SchemaField("status", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
        val runtime = openMemoryRuntimeBlocking("demo", namespaceId, usersSchema)
        if (schema == null) {
            putJson(runtime, namespaceId, """{"userId":"u1","status":"active"}""")
        }
        val wire = defaultWireCodec()
        val host = SqlWireHost(wire, KdbServerRuntime(runtime), namespaceId)
        val server = JvmNetworkWebSocketServer()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val serverJob =
            scope.launch {
                server.start("127.0.0.1", 0, "/kdb") { conn ->
                    conn.incoming().collect { frame ->
                        val response = host.handleFrame(frame)
                        if (response != null) {
                            conn.send(response)
                        }
                    }
                }
            }
        while (server.port == 0) {
            delay(10)
        }
        try {
            block("jdbc:kdb://127.0.0.1:${server.port}/$namespaceId")
        } finally {
            runCatching { server.stop() }
            runBlocking {
                withTimeout(2_000) {
                    serverJob.join()
                }
            }
            scope.cancel()
        }
    }
}
