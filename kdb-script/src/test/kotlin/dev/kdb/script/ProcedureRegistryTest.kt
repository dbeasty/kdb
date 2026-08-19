package dev.kdb.script

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ProcedureRegistryTest {
    @Test
    fun putBumpsRevisionAndGetReturnsLatest() =
        runTest {
            val registry = inMemoryProcedureRegistry()
            val v1 =
                registry.put(
                    ProcedureDefinition(namespaceId = "orders", name = "shipOrder", source = "function main(args) { return 1; }"),
                )
            assertEquals(1L, v1.revision)
            val v2 =
                registry.put(
                    ProcedureDefinition(namespaceId = "orders", name = "shipOrder", source = "function main(args) { return 2; }"),
                )
            assertEquals(2L, v2.revision)
            assertEquals(v2, registry.get("orders", "shipOrder"))
        }

    @Test
    fun listAndDelete() =
        runTest {
            val registry = inMemoryProcedureRegistry()
            registry.put(ProcedureDefinition(namespaceId = "orders", name = "a", source = "function main(){}"))
            registry.put(ProcedureDefinition(namespaceId = "orders", name = "b", source = "function main(){}"))
            registry.put(ProcedureDefinition(namespaceId = "other", name = "c", source = "function main(){}"))
            assertEquals(listOf("a", "b"), registry.list("orders"))
            assertEquals(true, registry.delete("orders", "a"))
            assertEquals(listOf("b"), registry.list("orders"))
        }

    @Test
    fun getMissingReturnsNull() =
        runTest {
            val registry = inMemoryProcedureRegistry()
            assertNull(registry.get("orders", "nope"))
        }
}
