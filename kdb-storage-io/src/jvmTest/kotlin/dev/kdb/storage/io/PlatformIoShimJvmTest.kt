package dev.kdb.storage.io

import java.io.File
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals

class PlatformIoShimJvmTest {
    @Test
    fun flushSurvivesReopen() = runTest {
        val root = File.createTempFile("kdb-io", null).apply { delete(); mkdirs() }.absolutePath
        val name = SegmentNameBuilder.wal("ns1", "w2")
        val data = byteArrayOf(1, 2, 3, 4)
        val io1 = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root))
        io1.appendToSegment(name, data)
        io1.flushSegment(name)
        val io2 = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root))
        val read = io2.readFromSegment(name, 0, 4)
        assertContentEquals(data, read)
    }
}
