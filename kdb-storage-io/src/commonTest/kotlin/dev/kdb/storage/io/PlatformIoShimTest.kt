package dev.kdb.storage.io

import dev.kdb.storage.PlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class PlatformIoShimTest {
    private fun shim(): PlatformIoShim =
        FileBackedPlatformIoShimFactory.open(PlatformIoConfig(fsyncOnFlush = true))

    @Test
    fun appendRead_roundtrip() = runTest {
        val io = shim()
        val name = SegmentNameBuilder.wal("ns1", "w1")
        val data = ByteArray(1024) { it.toByte() }
        val size = io.appendToSegment(name, data)
        assertEquals(1024L, size)
        val read = io.readFromSegment(name, 0, 1024)
        assertContentEquals(data, read)
    }

    @Test
    fun sealBlocksAppend() = runTest {
        val io = shim()
        val name = SegmentNameBuilder.delta("ns1", "d1")
        io.appendToSegment(name, byteArrayOf(1))
        io.sealSegment(name)
        assertFailsWith<PlatformIoException> {
            io.appendToSegment(name, byteArrayOf(2))
        }
    }

    @Test
    fun listSegments_prefix() = runTest {
        val io = shim()
        io.appendToSegment(SegmentNameBuilder.delta("ns1", "a"), byteArrayOf(1))
        io.appendToSegment(SegmentNameBuilder.wal("ns1", "b"), byteArrayOf(1))
        io.appendToSegment(SegmentNameBuilder.delta("ns2", "c"), byteArrayOf(1))
        val listed = io.listSegments("ns1")
        assertEquals(2, listed.size)
        assertTrue(listed.all { it.startsWith("ns/ns1/") })
    }

    @Test
    fun deleteSegment_removes() = runTest {
        val io = shim()
        val name = SegmentNameBuilder.wal("ns1", "del")
        io.appendToSegment(name, byteArrayOf(9))
        io.sealSegment(name)
        io.deleteSegment(name)
        assertTrue(io.listSegments("ns1").isEmpty())
        assertFailsWith<PlatformIoException> {
            io.readFromSegment(name, 0, 1)
        }
    }

    @Test
    fun maxAppend_guard() = runTest {
        val io = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(maxAppendBytes = 100))
        val name = SegmentNameBuilder.wal("ns1", "big")
        assertFailsWith<PlatformIoException> {
            io.appendToSegment(name, ByteArray(101))
        }
    }

    @Test
    fun readPastEnd_partial() = runTest {
        val io = shim()
        val name = SegmentNameBuilder.wal("ns1", "end")
        io.appendToSegment(name, byteArrayOf(1, 2, 3))
        val read = io.readFromSegment(name, 3, 10)
        assertEquals(0, read.size)
    }

    @Test
    fun segmentNameBuilder_sstable() {
        assertEquals("ns/app/sstable/L2/abc", SegmentNameBuilder.sstable("app", 2, "abc"))
    }

    @Test
    fun browser_snapshot_roundtrip() = runTest {
        val io = FileBackedPlatformIoShimFactory.open(PlatformIoConfig())
        val key = SnapshotKeyBuilder.enlistment("e1")
        val data = byteArrayOf(10, 20, 30)
        io.writeSnapshot(key, data)
        assertContentEquals(data, io.readSnapshot(key))
        io.deleteSnapshot(key)
        assertNull(io.readSnapshot(key))
    }
}
