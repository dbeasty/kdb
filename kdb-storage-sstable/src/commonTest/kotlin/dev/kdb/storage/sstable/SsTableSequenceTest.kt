package dev.kdb.storage.sstable

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.kdbSha256
import dev.kdb.storage.io.FileBackedPlatformIoShimFactory
import dev.kdb.storage.io.PlatformIoConfig
import dev.kdb.storage.io.SegmentNameBuilder
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Tables are named `<age>-<uuid>`, the age a sequence as Go's sstable.Sequence hands out: Go
 * registers a namespace's tables in age order, so a Kotlin table's age is what puts it in the right
 * place in Go's search order (it was a bare UUID, which Go could only count as the oldest).
 */
class SsTableSequenceTest {
    private fun shim() = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(fsyncOnFlush = true))

    private fun key(s: String): KdbHash = KdbHash.fromBytes(kdbSha256(s.encodeToByteArray()))

    @Test
    fun aStoresTablesAreNumberedInSequence() =
        runTest {
            val io = shim()
            val store = LsmBlobStore(io, "seq", BlockCache(1 shl 20))
            for (i in 1L..3L) {
                val w = store.newWriter(0)
                w.put(key("k$i"), "v".encodeToByteArray())
                val h = w.finish()
                assertEquals(i, SsTableSequence.ageOf(h.segmentName), h.segmentName)
                store.addTable(h)
            }
        }

    @Test
    fun aSequenceCarriesOnAboveTheHighestAgeOnDisk() =
        runTest {
            val io = shim()
            val ns = "seq-disk"
            val goAge = 0x18de7c433619e988L
            // A table as Go names it, and one named before ages (a bare UUID starting high).
            for (fileId in listOf(goAge.toString(16).padStart(16, '0') + "-" + KdbUuid.random(), "ffffffff-0000-4000-8000-000000000000")) {
                val name = SegmentNameBuilder.sstable(ns, 0, fileId)
                io.appendToSegment(name, byteArrayOf(1))
                io.sealSegment(name)
            }
            val w = DefaultSsTableWriter(io, ns, 0)
            w.put(key("k"), "v".encodeToByteArray())
            assertEquals(goAge + 1, SsTableSequence.ageOf(w.finish().segmentName))
            assertNull(SsTableSequence.ageOf(SegmentNameBuilder.sstable(ns, 0, "ffffffff-0000-4000-8000-000000000000")))
        }
}
