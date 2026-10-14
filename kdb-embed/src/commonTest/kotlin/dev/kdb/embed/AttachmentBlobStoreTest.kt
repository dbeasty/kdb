package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.document.ATTACHMENT_CHUNK_BYTES
import dev.kdb.document.attachmentContentHash
import dev.kdb.document.kdbSha256
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/** The attachment store - go/kdb/storage/attachstore/store_test.go's cases, in Kotlin. */
class AttachmentBlobStoreTest {
    private val ns = "app/data"
    private val chunk = ATTACHMENT_CHUNK_BYTES

    private fun random(
        seed: Int,
        n: Int,
    ): ByteArray = Random(seed).nextBytes(n)

    /** [n] versions of one blob, each a few edits from the last. */
    private fun versions(
        seed: Int,
        count: Int,
        n: Int,
    ): List<ByteArray> {
        val r = Random(seed)
        val out = mutableListOf(r.nextBytes(n))
        repeat(count - 1) {
            val next = out.last().copyOf()
            repeat(5) { next[r.nextInt(n)] = r.nextInt().toByte() }
            out += next
        }
        return out
    }

    private suspend fun sleep(d: Duration) = withContext(Dispatchers.Default) { delay(d) }

    private suspend fun AttachmentBlobStore.readBack(h: KdbHash): ByteArray {
        val r = open(h)
        try {
            return r.readAll()
        } finally {
            r.close()
        }
    }

    private suspend fun AttachmentBlobStore.putBytes(b: ByteArray) = put(AttachmentSource.of(b))

    private suspend fun segments(
        shim: InMemoryPlatformIoShim,
        kind: String,
    ) = shim.listSegments(ns).count { it.startsWith("ns/$ns/attachment/$kind") }

    @Test
    fun blobsRoundTripUnderTheirChunkedHash() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val s = AttachmentBlobStore(shim, ns, smallMaxBytes = 1024)
            for (n in listOf(0, 1, 1024, 1025, chunk - 1, chunk, chunk + 1, 2 * chunk + 5)) {
                val data = random(n, n)
                val blob = s.putBytes(data)
                assertEquals(attachmentContentHash(data), blob.hash, "size $n")
                assertEquals(n.toLong(), blob.size)
                assertTrue(s.has(blob.hash))
                assertContentEquals(data, s.readBack(blob.hash), "size $n")
            }
            // The empty blob is never stored; small ones live in their pointers.
            assertEquals(2, s.list().count { s.open(it).let { r -> r.close(); r.size <= 1024 } })
            assertEquals(2 * 5, segments(shim, "u-")) // bytes and leaves for the five large ones
        }

    @Test
    fun aCorruptChunkIsCaughtAtTheChunk() =
        runTest {
            val s = AttachmentBlobStore(InMemoryPlatformIoShim(), ns)
            val data = random(1, 2 * chunk + 10)
            val blob = s.putBytes(data)
            s.corruptForTest(blob.hash, chunk + 3L)
            val r = s.open(blob.hash)
            val buf = ByteArray(10)
            assertEquals(10, r.readAt(0, buf))
            assertContentEquals(data.copyOf(10), buf)
            assertFailsWith<AttachmentCorruptException> { r.readAt(chunk.toLong(), buf) }
            assertEquals(10, r.readAt(2L * chunk, buf)) // the third chunk is fine
            r.close()
        }

    @Test
    fun largeBlobsDedupAndSurviveReopen() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val data = random(2, chunk + 100)
            val a = AttachmentBlobStore(shim, ns).putBytes(data)
            val b = AttachmentBlobStore(shim, ns).putBytes(data)
            assertEquals(a, b)
            assertEquals(2, segments(shim, "u-"))
            assertContentEquals(data, AttachmentBlobStore(shim, ns).readBack(a.hash))
        }

    @Test
    fun anUploadOverTheLimitLeavesNothing() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            assertFailsWith<AttachmentTooLargeException> { AttachmentBlobStore(shim, ns, maxBytes = 5).putBytes(ByteArray(6)) }
            assertFailsWith<AttachmentTooLargeException> {
                AttachmentBlobStore(shim, ns, maxBytes = chunk + 5L).putBytes(ByteArray(2 * chunk))
            }
            assertEquals(emptyList(), shim.listSegments(ns))
        }

    @Test
    fun storeAsDeltaAndCollect() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val s = AttachmentBlobStore(shim, ns)
            val v = versions(3, 3, chunk + 777)
            val h = v.map { s.putBytes(it).hash }
            s.committed(h)
            assertTrue(s.storeAsDelta(h[0], h[1]))
            assertTrue(s.storeAsDelta(h[1], h[2]))
            assertFalse(s.storeAsDelta(h[1], h[2])) // already a delta
            for (i in v.indices) assertContentEquals(v[i], s.readBack(h[i]))
            // Random access through two deltas.
            val r = s.open(h[0])
            val buf = ByteArray(4500)
            assertEquals(buf.size, r.readAt(chunk - 4000L, buf))
            assertContentEquals(v[0].copyOfRange(chunk - 4000, chunk + 500), buf)
            r.close()
            // Keeping only the oldest keeps its chain.
            assertEquals(0, s.collect { it == h[0] })
            assertContentEquals(v[0], AttachmentBlobStore(shim, ns).readBack(h[0]))
            assertEquals(3, s.collect { false }) // (a reopened store would hold them again: the hold is in the pointers)
            assertEquals(emptyList(), shim.listSegments(ns))
        }

    @Test
    fun storeAsDeltaSkipsUnrelatedVersions() =
        runTest {
            val s = AttachmentBlobStore(InMemoryPlatformIoShim(), ns)
            val a = s.putBytes(random(4, chunk + 10))
            val b = s.putBytes(random(5, chunk + 10))
            assertFalse(s.storeAsDelta(a.hash, b.hash))
            val small = s.putBytes(random(6, 100))
            assertFalse(s.storeAsDelta(small.hash, b.hash)) // inline is never a delta
            assertFalse(AttachmentBlobStore(InMemoryPlatformIoShim(), ns, deltaMaxBytes = 10).storeAsDelta(a.hash, b.hash))
        }

    @Test
    fun readersFinishOnTheirVersion() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val s = AttachmentBlobStore(shim, ns)
            val v = versions(7, 2, chunk + 500)
            val old = s.putBytes(v[0]).hash
            val new = s.putBytes(v[1]).hash
            s.committed(listOf(old, new))
            val r = s.open(old)
            assertTrue(s.storeAsDelta(old, new))
            assertEquals(1, s.collect { it == new }) // the old version goes, but the reader has it
            assertTrue(s.pinnedSegments().second > 0)
            assertContentEquals(v[0], r.readAll())
            r.close()
            assertEquals(0 to 0, s.pinnedSegments())
            assertFalse(s.has(old))
            assertContentEquals(v[1], s.readBack(new))
        }

    @Test
    fun aStalledReaderLeaseExpires() =
        runTest {
            val s = AttachmentBlobStore(InMemoryPlatformIoShim(), ns, readLease = 1.milliseconds)
            val blob = s.putBytes(random(8, 2 * chunk))
            val r = s.open(blob.hash)
            sleep(10.milliseconds)
            s.collect { false }
            assertFailsWith<AttachmentReadLeaseExpiredException> { r.readAll() }
            r.close()
            assertEquals(0 to 0, s.pinnedSegments())
        }

    @Test
    fun pointerGenerationsAfterACrash() =
        runTest {
            // A crash between writing a delta's pointer and deleting the whole copy leaves both; the
            // reopened store follows the newer generation and collect retires the older.
            val shim = InMemoryPlatformIoShim()
            val s = AttachmentBlobStore(shim, ns)
            val v = versions(9, 2, chunk + 500)
            val old = s.putBytes(v[0]).hash
            val new = s.putBytes(v[1]).hash
            val stale = shim.listSegments(ns).filter { it.contains("/h-${old.toHex()}.") }
            val staleBytes = stale.associateWith { shim.readFromSegment(it, 0, 1 shl 16) }
            assertTrue(s.storeAsDelta(old, new))
            for ((name, b) in staleBytes) if (name !in shim.listSegments(ns)) shim.appendToSegment(name, b)
            val again = AttachmentBlobStore(shim, ns)
            assertContentEquals(v[0], again.readBack(old))
            again.collect { true }
            assertEquals(1, shim.listSegments(ns).count { it.contains("/h-${old.toHex()}.") })
            assertContentEquals(v[0], AttachmentBlobStore(shim, ns).readBack(old))
        }

    @Test
    fun theMarkSurvivesReopenAndCollect() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val s = AttachmentBlobStore(shim, ns)
            assertEquals(emptyList(), s.mark())
            val a = KdbHash(kdbSha256("a".encodeToByteArray()))
            val b = KdbHash(kdbSha256("b".encodeToByteArray()))
            s.setMark(listOf(a))
            s.setMark(listOf(a, b))
            s.collect { false }
            assertEquals(listOf(a, b), AttachmentBlobStore(shim, ns).mark())
            assertEquals(1, segments(shim, "m-"))
            // A torn newer mark reads as the older one.
            shim.appendToSegment("ns/$ns/attachment/m-000099", "KDBATM1".encodeToByteArray())
            assertEquals(listOf(a, b), AttachmentBlobStore(shim, ns).mark())
            val c = AttachmentBlobStore(shim, ns)
            c.setMark(listOf(b))
            assertTrue("ns/$ns/attachment/m-000100" in shim.listSegments(ns)) // never over the torn one
            assertEquals(listOf(b), AttachmentBlobStore(shim, ns).mark())
        }

    @Test
    fun aFinishedUploadIsHeldForItsGrace() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val grace = 50.milliseconds
            val small = AttachmentBlobStore(shim, ns, uploadGrace = grace).putBytes(random(10, 100))
            val large = AttachmentBlobStore(shim, ns, uploadGrace = grace).putBytes(random(11, chunk + 1))
            shim.appendToSegment("ns/$ns/attachment/u-abandoned", ByteArray(10)) // a crashed upload: no grace
            val s = AttachmentBlobStore(shim, ns, uploadGrace = grace) // the hold survives a reopen
            assertEquals(0, s.collect { false })
            assertFalse("ns/$ns/attachment/u-abandoned" in shim.listSegments(ns))
            assertTrue(s.has(small.hash) && s.has(large.hash))
            sleep(100.milliseconds)
            assertEquals(2, s.collect { false })
            assertEquals(emptyList(), shim.listSegments(ns))
        }

    @Test
    fun holdAndReuploadRestartTheGrace() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val grace = 400.milliseconds
            val s = AttachmentBlobStore(shim, ns, uploadGrace = grace)
            val a = s.putBytes(random(12, 100))
            val b = s.putBytes(random(13, 100))
            sleep(250.milliseconds)
            assertTrue(s.hold(a.hash))
            s.putBytes(random(13, 100))
            assertFalse(s.hold(KdbHash(ByteArray(32))))
            sleep(250.milliseconds)
            // Past the first grace, within the restarted one - and recorded, so a reopened store sees it.
            assertEquals(0, AttachmentBlobStore(shim, ns, uploadGrace = grace).collect { false })
            sleep(300.milliseconds)
            assertEquals(2, AttachmentBlobStore(shim, ns, uploadGrace = grace).collect { false })
            assertFalse(AttachmentBlobStore(shim, ns).has(b.hash))
        }

    @Test
    fun committedAndAReferenceEndTheHold() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val s = AttachmentBlobStore(shim, ns)
            val a = s.putBytes(random(14, 100))
            val b = s.putBytes(random(15, 100))
            s.committed(listOf(a.hash))
            assertEquals(1, s.collect { false })
            assertEquals(0, s.collect { it == b.hash }) // seen referenced: references decide now
            assertEquals(1, s.collect { false })
        }

    @Test
    fun aVersion2PointerReadsWithoutAGrace() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val data = random(16, 100)
            val h = attachmentContentHash(data)
            // KDBATP2: no finish time.
            val body = "KDBATP2".encodeToByteArray() + byteArrayOf(2) + ByteArray(7) + byteArrayOf(100) + byteArrayOf(0, 0) + data
            shim.appendToSegment("ns/$ns/attachment/h-${h.toHex()}.000000", body + kdbSha256(body).copyOf(4))
            val s = AttachmentBlobStore(shim, ns)
            assertContentEquals(data, s.readBack(h))
            assertEquals(1, s.collect { false })
        }

    @Test
    fun aDeltaVersion2IsReadByRandomAccess() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val s = AttachmentBlobStore(shim, ns)
            val v = versions(17, 2, 3 * chunk + 999)
            val old = s.putBytes(v[0]).hash
            val new = s.putBytes(v[1]).hash
            val seg = "ns/$ns/attachment/u-v2"
            shim.appendToSegment(seg, deltaV2(v[1], v[0], frame = 300_000))
            s.installDeltaForTest(old, new, seg, v2 = true)
            val again = AttachmentBlobStore(shim, ns)
            assertContentEquals(v[0], again.readBack(old))
            val r = again.open(old)
            val rnd = Random(1)
            repeat(50) {
                val off = rnd.nextLong(v[0].size.toLong() - 10_000)
                val buf = ByteArray(rnd.nextInt(1, 10_000))
                assertEquals(buf.size, r.readAt(off, buf))
                assertContentEquals(v[0].copyOfRange(off.toInt(), off.toInt() + buf.size), buf)
            }
            r.close()
            // A damaged footer is corruption, not a crash.
            val bad = "ns/$ns/attachment/u-v2-bad"
            shim.appendToSegment(bad, deltaV2(v[1], v[0], frame = 300_000).let { it.copyOf(it.size - 1) })
            again.installDeltaForTest(old, new, bad, v2 = true)
            assertFailsWith<AttachmentCorruptException> { AttachmentBlobStore(shim, ns).open(old) }
        }

    /**
     * Writes target as a version-2 delta against base, as the format describes it: a frame every
     * [frame] bytes - a copy where target equals base there, a literal otherwise - with a few
     * unreferenced bytes between frames, which readers must ignore.
     */
    private fun deltaV2(
        base: ByteArray,
        target: ByteArray,
        frame: Int,
    ): ByteArray {
        val out = ArrayList<Byte>()
        val index = ArrayList<LongArray>()

        fun uv(v: Long): List<Byte> {
            val b = ArrayList<Byte>()
            var x = v
            while (x >= 0x80) {
                b += ((x and 0x7f) or 0x80).toByte()
                x = x ushr 7
            }
            b += x.toByte()
            return b
        }
        var t = 0
        while (t < target.size) {
            val end = minOf(t + frame, target.size)
            out += listOf<Byte>(9, 9, 9) // junk
            val litOff = out.size.toLong()
            val ops = ArrayList<Byte>()
            // Half the frame as a copy (when equal), the rest as a literal.
            val mid = (t + end) / 2
            if (target.copyOfRange(t, mid).contentEquals(base.copyOfRange(t, mid))) {
                ops += 1.toByte()
                ops += uv(t.toLong())
                ops += uv((mid - t).toLong())
            } else {
                out += target.copyOfRange(t, mid).toList()
                ops += 0.toByte()
                ops += uv((mid - t).toLong())
            }
            out += target.copyOfRange(mid, end).toList()
            ops += 0.toByte()
            ops += uv((end - mid).toLong())
            val opsOff = out.size.toLong()
            out += ops
            index += longArrayOf(t.toLong(), litOff, opsOff, ops.size.toLong())
            t = end
        }
        val indexOff = out.size.toLong()
        fun be(v: Long) = (0 until 8).map { (v ushr (56 - 8 * it)).toByte() }
        for (e in index) for (v in e) out += be(v)
        out += be(indexOff)
        out += be(index.size.toLong())
        out += be(target.size.toLong())
        out += "KDBDLT2\n".encodeToByteArray().toList()
        return out.toByteArray()
    }

    @Test
    fun goldenEncodings() =
        runTest {
            // go/kdb/storage/attachstore/kotlin_fixture_test.go, TestKotlinGoldenEncodings.
            val s = AttachmentBlobStore(InMemoryPlatformIoShim(), ns)
            val at = 1760000000123456000L
            val base = KdbHash(ByteArray(32).also { it[0] = 0xab.toByte() })
            val inline = "hello attachments".encodeToByteArray()
            assertEquals(
                GOLDEN_INLINE,
                s.encodePointer(AttachmentBlobStore.Pointer(0, "", "", "", inline.size.toLong(), false, false, null, inline, at)).toHex(),
            )
            assertEquals(
                GOLDEN_WHOLE,
                s.encodePointer(AttachmentBlobStore.Pointer(0, "", "ns/app/data/attachment/u-1", "ns/app/data/attachment/u-2", 3L shl 20, false, false, null, null, at)).toHex(),
            )
            assertEquals(
                GOLDEN_DELTA,
                s.encodePointer(AttachmentBlobStore.Pointer(0, "", "ns/app/data/attachment/u-3", "ns/app/data/attachment/u-2", 3L shl 20, true, false, base, null, at)).toHex(),
            )
            val newer = fixtureText(400000, 7)
            var d = ByteArray(0)
            encodeDelta(MemSource(newer), MemSource(fixtureEdit(newer))) { d += it }
            assertEquals(GOLDEN_DELTA_BYTES, "${d.size}:${kdbSha256(d).toHex()}")
            // And it reads back.
            assertContentEquals(fixtureEdit(newer), ByteArray(newer.size).also { deltaSourceV1(MemSource(newer), memSeg(d)).readAt(it, 0, it.size, 0) })
        }

    private suspend fun memSeg(d: ByteArray): SegSource {
        val shim = InMemoryPlatformIoShim()
        shim.appendToSegment("ns/x/attachment/u-d", d)
        return SegSource(shim, "ns/x/attachment/u-d", d.size.toLong())
    }

    private fun ByteArray.toHex() = joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }

    companion object {
        const val GOLDEN_INLINE =
            "4b444241545033020000000000000011186cc6acdc0bca00000068656c6c6f206174746163686d656e74730faab79d"
        const val GOLDEN_WHOLE =
            "4b444241545033000000000000300000186cc6acdc0bca001a6e732f6170702f646174612f6174746163686d656e742f752d311a6e732f6170702f646174612f6174746163686d656e742f752d32200b1342"
        const val GOLDEN_DELTA =
            "4b444241545033010000000000300000186cc6acdc0bca001a6e732f6170702f646174612f6174746163686d656e742f752d331a6e732f6170702f646174612f6174746163686d656e742f752d32ab0000000000000000000000000000000000000000000000000000000000000035950ee4"
        const val GOLDEN_DELTA_BYTES = "13956:2fd6239864c3eb31fb49521d4d4862edb07007d25d32d80ab52d4eede3bdaa1f"

        /** go's fixtureText: [n] bytes of numbered lines. */
        fun fixtureText(
            n: Int,
            seed: Int,
        ): ByteArray {
            val sb = StringBuilder()
            var i = 0
            while (sb.length < n) {
                sb.append("seed ").append(seed).append(" line ").append(i.toString().padStart(7, '0'))
                    .append(" the quick brown fox jumps over the lazy dog\n")
                i++
            }
            return sb.toString().encodeToByteArray().copyOf(n)
        }

        /** go's fixtureEdit: "EDITED" at every 100000th offset after the first. */
        fun fixtureEdit(base: ByteArray): ByteArray {
            val out = base.copyOf()
            var off = 100000
            while (off + 6 <= out.size) {
                "EDITED".encodeToByteArray().copyInto(out, off)
                off += 100000
            }
            return out
        }
    }
}
