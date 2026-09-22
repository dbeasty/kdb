package dev.kdb.storage.io

import java.io.File
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

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

    private fun tempRoot(): String =
        File.createTempFile("kdb-io", null).apply { delete(); mkdirs() }.absolutePath

    /**
     * A namespace whose name is a prefix of another's must be able to hold a snapshot, and so must
     * the one nested under it. The slash in an id used to become a real path separator, so
     * "repro/account" wrote the FILE snap/kdb_checkpoint_repro/account while "repro/account/ro"
     * needed that same "account" to be a directory, and the second could never be written.
     */
    @Test
    fun snapshotKeysThatArePrefixesOfEachOther() = runTest {
        val io = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = tempRoot()))
        val outer = "kdb:checkpoint:repro/account"
        val nested = "kdb:checkpoint:repro/account/ro"

        io.writeSnapshot(outer, "outer".encodeToByteArray())
        io.writeSnapshot(nested, "nested".encodeToByteArray())

        assertContentEquals("outer".encodeToByteArray(), io.readSnapshot(outer))
        assertContentEquals("nested".encodeToByteArray(), io.readSnapshot(nested))

        io.deleteSnapshot(outer)
        assertContentEquals("nested".encodeToByteArray(), io.readSnapshot(nested))
    }

    /** One flat file per key under snap/, never a directory tree - and the same name Go writes. */
    @Test
    fun snapshotIsOneFlatFile() = runTest {
        val root = tempRoot()
        val io = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root))
        io.writeSnapshot("kdb:checkpoint:repro/account", byteArrayOf(1))

        val entries = File(File(root), "snap").listFiles().orEmpty()
        assertEquals(1, entries.size, "snap/ holds ${entries.map { it.name }}")
        assertFalse(entries[0].isDirectory, "snap/${entries[0].name} is a directory")
        assertEquals("kdb_checkpoint_repro%2Faccount", entries[0].name)
    }

    /**
     * A checkpoint written by a build that nested the slash has to still be found: a namespace
     * bootstrapped from a peer's snapshot has nothing else to open from. Writing then retires it.
     */
    @Test
    fun snapshotReadsAndRetiresAPreFlatteningFile() = runTest {
        val root = tempRoot()
        val io = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root))
        val key = "kdb:checkpoint:repro/account"

        val legacy = File(File(File(root), "snap"), "kdb_checkpoint_repro${File.separatorChar}account")
        legacy.parentFile.mkdirs()
        legacy.writeBytes("from an older build".encodeToByteArray())

        assertContentEquals("from an older build".encodeToByteArray(), io.readSnapshot(key))

        io.writeSnapshot(key, "current".encodeToByteArray())
        assertFalse(legacy.exists(), "the pre-flattening file survived a write")
        assertContentEquals("current".encodeToByteArray(), io.readSnapshot(key))

        io.deleteSnapshot(key)
        assertNull(io.readSnapshot(key))
    }
}
