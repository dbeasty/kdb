package dev.kdb.storage.sstable

import dev.kdb.codec.KdbHash
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import java.io.File
import java.nio.file.Paths
import kotlinx.coroutines.test.runTest
import kotlin.io.path.isDirectory
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * SSTable half of the physical-layer conformance suite -
 * docs/kdb-physical-layer-compat-test-plan.md §4.4, cases S1-S10. The Go counterpart is
 * go/kdb/interop/sstable_physical_golden_test.go.
 *
 * Whole segments, not bare codec calls: the footer is only locatable via its trailing indexLen,
 * so end-to-end is the only way to pin that the trailer, the index lines and the block offsets
 * agree with each other as well as with Go.
 *
 * Block *bodies* are ZSTD and can never be compared across languages - zstd-jni and Go's
 * klauspost/compress emit different, equally valid frames (test plan §2). What is compared is the
 * footer, which holds no compressed bytes, and the values each side reads out of the other's
 * segment.
 */
class SsTablePhysicalGoldenTest {

    @Test
    fun exportSsTableGolden() =
        runTest {
            val (_, _, segment) = buildFixtureSegment()
            writeHex("sstable_segment.hex", segment)
            writeHex("sstable_footer.hex", footerOf(segment))
        }

    // S3: the same table must produce the same footer every time. Go's index came out of a map,
    // so its byte order was randomized per run; this pins that Kotlin's insertion order - the
    // order Go now matches - is what lands on disk.
    @Test
    fun footerIsDeterministic() =
        runTest {
            val first = footerOf(buildFixtureSegment().third)
            repeat(20) {
                assertContentEquals(first, footerOf(buildFixtureSegment().third), "footer must not vary between writes")
            }
        }

    // S7: a key written twice is one entry holding the later value. Go used to append a second,
    // leaving a stale unreachable block ahead of the live one and feeding both values into the
    // fileHash preimage - so the same call sequence produced different content hashes per language.
    @Test
    fun duplicateKeyCollapsesToLastValue() =
        runTest {
            val (shim, handle, segment) = buildFixtureSegment()
            assertEquals(4, footerIndexLines(footerOf(segment)).size, "four distinct keys were written")
            assertEquals("alpha-rewritten", DefaultSsTableReader(shim, handle).get(hash(1))?.decodeToString())
        }

    // S5: a tombstone is a fourth field on its index line and no block at all.
    @Test
    fun tombstoneLineFormat() =
        runTest {
            val (shim, handle, segment) = buildFixtureSegment()
            val deleted = hash(3).toHex()
            val line = footerIndexLines(footerOf(segment)).firstOrNull { it.startsWith(deleted) }
            assertEquals("$deleted:0:0:1", line)
            val entry = assertNotNull(DefaultSsTableReader(shim, handle).lookup(hash(3)))
            assertTrue(entry.deleted, "the deleted key must read back as a tombstone, not as absent")
        }

    // S6: the last four bytes duplicate indexLen. Without that trailer the footer is unlocatable.
    @Test
    fun footerTrailerLocatesTheFooter() =
        runTest {
            val footer = footerOf(buildFixtureSegment().third)
            assertEquals(0x4B444253, readIntBe(footer, 0), "footer magic")
            assertEquals(footer.size - 44, readIntBe(footer, 4), "indexLen inside the footer must match the trailer")
        }

    /**
     * S4/S8/S9/S11. The footer is *not* compared byte-for-byte: its per-block offset and
     * compressed-size fields are downstream of the compressor, and zstd-jni and Go's
     * klauspost/compress emit different (equally valid) frame lengths for the same input - see
     * the test plan's "What ZSTD drags out of C1". What must match exactly is everything the
     * compressor cannot touch: the index line order, each key, each tombstone flag, and the
     * fileHash - the segment's content address, and so the field that decides whether a
     * Go-written SSTable keeps its identity on the JVM.
     */
    @Test
    fun goSsTableGoldenMatches() =
        runTest {
            val goSegment = readGoHex("sstable_segment.hex") ?: return@runTest
            val (_, _, kotlinSegment) = buildFixtureSegment()
            val goFooter = footerOf(goSegment)
            val kotlinFooter = footerOf(kotlinSegment)

            assertEquals(
                footerIndexLines(kotlinFooter).map { it.keyAndFlag() },
                footerIndexLines(goFooter).map { it.keyAndFlag() },
                "index line order, keys and tombstone flags must match",
            )
            assertContentEquals(fileHashOf(kotlinFooter), fileHashOf(goFooter), "fileHash must be byte-identical")

            val shim = InMemoryPlatformIoShim()
            val name = "ns/$NAMESPACE_ID/sstable/L0/go-fixture"
            shim.appendToSegment(name, goSegment)
            val reader = DefaultSsTableReader(shim, SsTableHandle(hash(0), 0, name))
            assertEquals("alpha-rewritten", reader.get(hash(1))?.decodeToString())
            assertContentEquals(PAYLOAD, reader.get(hash(2)))
            assertContentEquals(ByteArray(0), reader.get(hash(4)))
            assertTrue(assertNotNull(reader.lookup(hash(3))).deleted)
        }

    // S10: a flipped byte inside a block body must fail loudly rather than decode to garbage.
    @Test
    fun corruptBlockIsRejected() =
        runTest {
            val (_, handle, segment) = buildFixtureSegment()
            val corrupt = segment.copyOf()
            // Offset 20 is inside the first block's compressed body (its header is 16 bytes).
            corrupt[20] = (corrupt[20].toInt() xor 0xFF).toByte()
            val shim = InMemoryPlatformIoShim()
            shim.appendToSegment(handle.segmentName, corrupt)
            val failed =
                try {
                    DefaultSsTableReader(shim, handle).get(hash(1))
                    false
                } catch (_: Throwable) {
                    true
                }
            assertTrue(failed, "a corrupted block body must be rejected, not decoded")
        }

    // --- fixtures -------------------------------------------------------------------------

    private companion object {
        const val NAMESPACE_ID = "fixture-ns"
        val PAYLOAD: ByteArray = byteArrayOf(0, 1, 2, 3, 0x7F, -128, -1, 0x2A, 0x2A, 0x2A)

        fun hash(seed: Int): KdbHash = KdbHash.fromBytes(ByteArray(32) { (seed + it).toByte() })

        fun readIntBe(b: ByteArray, off: Int): Int =
            ((b[off].toInt() and 0xFF) shl 24) or ((b[off + 1].toInt() and 0xFF) shl 16) or
                ((b[off + 2].toInt() and 0xFF) shl 8) or (b[off + 3].toInt() and 0xFF)
    }

    /**
     * The put/delete sequence both runtimes apply, including a key written twice - the case where
     * the two writers disagreed.
     */
    private suspend fun buildFixtureSegment(): Triple<PlatformIoShim, SsTableHandle, ByteArray> {
        val shim = InMemoryPlatformIoShim()
        val w = DefaultSsTableWriter(shim, NAMESPACE_ID, 0)
        w.put(hash(1), "alpha".encodeToByteArray())
        w.put(hash(2), PAYLOAD)
        w.delete(hash(3))
        w.put(hash(1), "alpha-rewritten".encodeToByteArray())
        w.put(hash(4), ByteArray(0))
        val handle = w.finish()
        return Triple(shim, handle, readWholeSegment(shim, handle.segmentName))
    }

    private suspend fun readWholeSegment(shim: PlatformIoShim, name: String): ByteArray {
        var out = ByteArray(0)
        var off = 0L
        while (true) {
            val p = shim.readFromSegment(name, off, 8192)
            if (p.isEmpty()) return out
            out += p
            off += p.size
            if (p.size < 8192) return out
        }
    }

    /**
     * Slices the footer out of a whole segment using the same trailing-indexLen bootstrap the
     * readers use: magic(4) + indexLen(4) + index + fileHash(32), then a 4-byte indexLen copy.
     */
    private fun footerOf(segment: ByteArray): ByteArray {
        assertTrue(segment.size >= 44, "segment is ${segment.size} bytes, too short to hold a footer")
        val indexLen = readIntBe(segment, segment.size - 4)
        val start = segment.size - (40 + indexLen) - 4
        assertTrue(start in 0..segment.size, "footer trailer says indexLen=$indexLen, which does not fit")
        return segment.copyOfRange(start, segment.size)
    }

    /** An index line stripped of its compressor-dependent offset and size: `<keyHex>[:1]`. */
    private fun String.keyAndFlag(): String {
        val parts = split(':')
        return if (parts.size > 3) "${parts[0]}:${parts[3]}" else parts[0]
    }

    /** The 32-byte fileHash sitting between the index and the trailer. */
    private fun fileHashOf(footer: ByteArray): ByteArray {
        val indexLen = readIntBe(footer, 4)
        return footer.copyOfRange(8 + indexLen, 8 + indexLen + 32)
    }

    private fun footerIndexLines(footer: ByteArray): List<String> {
        val indexLen = readIntBe(footer, 4)
        val body = footer.copyOfRange(8, 8 + indexLen).decodeToString()
        return if (body.isEmpty()) emptyList() else body.split("\n")
    }

    private fun goldenDir(side: String): File {
        val root = Paths.get(System.getProperty("user.dir"))
        val repo = generateSequence(root) { it.parent }.firstOrNull { it.resolve("go").isDirectory() } ?: root
        return repo.resolve("go/testdata/golden/physical/$side").toFile().also { it.mkdirs() }
    }

    private fun writeHex(name: String, bytes: ByteArray) {
        goldenDir("kotlin").resolve(name).writeText(bytes.joinToString("") { "%02x".format(it) } + "\n")
    }

    /** Null when Go's exporter has not been run - a setup gap, not a compatibility failure. */
    private fun readGoHex(name: String): ByteArray? {
        val f = goldenDir("go").resolve(name)
        if (!f.exists()) return null
        val hex = f.readText().trim()
        return ByteArray(hex.length / 2) { hex.substring(it * 2, it * 2 + 2).toInt(16).toByte() }
    }
}
