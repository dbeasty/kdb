package dev.kdb.storage

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Kotlin/Native-specific because it exercises totalSystemMemoryBytes()'s
 * real POSIX sysconf-based implementation - see MemoryBudget.native.kt.
 */
class MemoryBudgetNativeTest {
    @Test
    fun percentOfAvailableUsesRealSystemMemoryOnNative() {
        val total = totalSystemMemoryBytes()
        assertTrue(total != null && total > 0, "expected sysconf to report total system memory")
        val got = resolveHotTierBytes(HotTierMemoryConfig(percentOfAvailable = 10.0))
        val want = (total!! * 10 / 100)
        assertEquals(want, got)
        assertTrue(got > DEFAULT_HOT_TIER_BYTES, "expected 10% of a real machine's memory to exceed the default")
    }
}
