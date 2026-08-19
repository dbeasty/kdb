package dev.kdb.storage

import java.lang.management.ManagementFactory

/**
 * Reads total physical memory via com.sun.management's extended
 * OperatingSystemMXBean. Not on any hot path - called once per engine
 * construction, not per write. Falls back to null (caller uses
 * DEFAULT_HOT_TIER_BYTES) if the extended bean isn't available, e.g. on
 * a non-HotSpot JVM.
 */
public actual fun totalSystemMemoryBytes(): Long? =
    try {
        val bean = ManagementFactory.getOperatingSystemMXBean()
        val sunBean = bean as? com.sun.management.OperatingSystemMXBean
        sunBean?.totalMemorySize
    } catch (_: Throwable) {
        null
    }
