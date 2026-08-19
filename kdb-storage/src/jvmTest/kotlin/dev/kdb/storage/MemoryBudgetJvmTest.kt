package dev.kdb.storage

import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * JVM-specific because it exercises totalSystemMemoryBytes(), which is
 * only implemented (non-null) on the JVM target - see MemoryBudget.jvm.kt.
 */
class MemoryBudgetJvmTest {
    @Test
    fun defaultsWhenUnconfigured() {
        assertEquals(DEFAULT_HOT_TIER_BYTES, resolveHotTierBytes(HotTierMemoryConfig()))
    }

    @Test
    fun fixedBytesWins() {
        val got = resolveHotTierBytes(HotTierMemoryConfig(fixedBytes = 1L shl 30, percentOfAvailable = 50.0))
        assertEquals(1L shl 30, got)
    }

    @Test
    fun percentOfAvailableUsesRealSystemMemoryOnJvm() {
        val total = totalSystemMemoryBytes()
        assertTrue(total != null && total > 0, "expected JVM to report total system memory")
        val got = resolveHotTierBytes(HotTierMemoryConfig(percentOfAvailable = 10.0))
        val want = (total!! * 10 / 100)
        assertEquals(want, got)
        assertTrue(got > DEFAULT_HOT_TIER_BYTES, "expected 10% of a real machine's memory to exceed the default")
    }

    @Test
    fun percentClampedAt100() {
        val total = totalSystemMemoryBytes()!!
        val got = resolveHotTierBytes(HotTierMemoryConfig(percentOfAvailable = 250.0))
        assertEquals(total, got)
    }

    @Test
    fun validateRejectsOutOfRangeValues() {
        assertEquals(null, validateHotTierMemoryConfig(HotTierMemoryConfig()))
        assertEquals(null, validateHotTierMemoryConfig(HotTierMemoryConfig(fixedBytes = 1024)))
        assertEquals(null, validateHotTierMemoryConfig(HotTierMemoryConfig(percentOfAvailable = 25.0)))
        assertTrue(validateHotTierMemoryConfig(HotTierMemoryConfig(fixedBytes = -1)) != null)
        assertTrue(validateHotTierMemoryConfig(HotTierMemoryConfig(percentOfAvailable = -1.0)) != null)
        assertTrue(validateHotTierMemoryConfig(HotTierMemoryConfig(percentOfAvailable = 101.0)) != null)
    }

    @Test
    fun resolvedGlobalMemoryBudgetBytesOnConfig() {
        val explicit = StorageEngineConfig(globalMemoryBudgetBytes = 42, ioShim = InMemoryPlatformIoShim())
        assertEquals(42, explicit.resolvedGlobalMemoryBudgetBytes())

        val unset = StorageEngineConfig(ioShim = InMemoryPlatformIoShim())
        assertEquals(DEFAULT_HOT_TIER_BYTES, unset.resolvedGlobalMemoryBudgetBytes())
    }
}
