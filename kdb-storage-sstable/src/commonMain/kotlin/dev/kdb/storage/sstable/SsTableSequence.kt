package dev.kdb.storage.sstable

import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.io.SegmentNameBuilder
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Hands out the ages a namespace's new tables are named with: 16 hex digits, one past the highest
 * age the namespace holds, then a UUID - `ns/<ns>/sstable/L<n>/<age>-<uuid>`. Names sort by age,
 * which is the order the tables were written in; Go's DiscoverTables registers them in that order,
 * and a lookup searches them newest first, so a tombstone shadows what it did before a restart.
 *
 * The same scheme as Go's sstable.Sequence (go/kdb/storage/sstable/sequence.go): no clock, so no
 * clock's resolution or steps can make two tables tie, and either runtime carries on a namespace
 * the other wrote. It learns the highest age from the namespace's listing the first time it is
 * asked, and from every table it is shown after ([observe]).
 */
public class SsTableSequence(
    private val ioShim: PlatformIoShim,
    private val namespaceId: String,
) {
    private val mutex = Mutex()
    private var listed = false
    private var last = 0L

    /** The age of the next table: later than every table the namespace holds. */
    public suspend fun next(): String =
        mutex.withLock {
            if (!listed) {
                val prefix = SegmentNameBuilder.namespacePrefix(namespaceId) + "sstable/"
                for (name in ioShim.listSegments(namespaceId)) {
                    if (name.startsWith(prefix)) observeLocked(name)
                }
                listed = true
            }
            last++
            last.toString(16).padStart(AGE_DIGITS, '0')
        }

    /** Makes every later [next] newer than the table named [segmentName]. */
    public suspend fun observe(segmentName: String) {
        mutex.withLock { observeLocked(segmentName) }
    }

    private fun observeLocked(segmentName: String) {
        val age = ageOf(segmentName) ?: return
        if (age > last) last = age
    }

    public companion object {
        internal const val AGE_DIGITS: Int = 16

        /**
         * The age [segmentName] starts with, or null for a name without one: a bare UUID, as tables
         * were named before ages.
         */
        public fun ageOf(segmentName: String): Long? {
            val base = segmentName.substringAfterLast('/')
            if (base.length <= AGE_DIGITS + 1 || base[AGE_DIGITS] != '-') return null
            val digits = base.substring(0, AGE_DIGITS)
            if (!digits.all { it in '0'..'9' || it in 'a'..'f' }) return null
            return digits.toLongOrNull(16)
        }
    }
}
