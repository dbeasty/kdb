package dev.kdb.storage

/**
 * Not implemented for Kotlin/Native targets (would need POSIX cinterop
 * per-platform); callers using percentOfAvailable here fall back to
 * DEFAULT_HOT_TIER_BYTES. The JVM server target (see MemoryBudget.jvm.kt)
 * is what Phase 5 of the reengineering plan is scoped to.
 */
public actual fun totalSystemMemoryBytes(): Long? = null
