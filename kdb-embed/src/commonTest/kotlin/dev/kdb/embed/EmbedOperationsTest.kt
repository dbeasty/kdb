package dev.kdb.embed

import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EmbedOperationsTest {
    @Test
    fun putAndQueryDoc() =
        runTest {
            val ns = "demo/users"
            val runtime = openMemoryRuntime("demo", ns)
            putJson(runtime, ns, """{"userId":"u1","name":"Alice"}""")
            val result = querySql(runtime, ns, "SELECT _doc FROM users", KdbSchema.NONE)
            assertTrue(result.rows.isNotEmpty())
            assertEquals("_doc", result.columns.single())
        }

    @Test
    fun indexedWhere() =
        runTest {
            val ns = "demo/users"
            val schema =
                KdbSchema.build(
                    listOf(
                        SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
                    ),
                )
            val runtime = openMemoryRuntime("demo", ns, schema)
            putJson(runtime, ns, """{"userId":"u1"}""", schema)
            val result = querySql(runtime, ns, "SELECT userId FROM users WHERE userId = 'u1'", schema)
            assertEquals(1, result.rows.size)
        }
}
