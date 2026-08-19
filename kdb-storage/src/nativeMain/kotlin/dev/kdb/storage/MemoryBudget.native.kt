@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.kdb.storage

import platform.posix._SC_PAGESIZE
import platform.posix._SC_PHYS_PAGES
import platform.posix.sysconf

/**
 * Total physical memory via POSIX sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE).
 * Available on both Linux and Darwin (this module's two native targets,
 * linuxX64/macosArm64 - see build.gradle.kts), unlike the JVM's
 * com.sun.management extended bean this mirrors on the JVM target
 * (MemoryBudget.jvm.kt). Falls back to null (caller uses
 * DEFAULT_HOT_TIER_BYTES) if either sysconf call reports an error.
 */
public actual fun totalSystemMemoryBytes(): Long? {
    val pages = sysconf(_SC_PHYS_PAGES)
    val pageSize = sysconf(_SC_PAGESIZE)
    if (pages <= 0 || pageSize <= 0) return null
    return pages * pageSize
}
