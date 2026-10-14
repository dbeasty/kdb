package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.document.attachmentContentHash
import dev.kdb.document.kdbSha256
import dev.kdb.embed.AttachmentBlobStore
import dev.kdb.embed.AttachmentSource
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.File
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Kotlin's attachment store and Go's (go/kdb/storage/attachstore) share one on-disk layout. Each
 * side builds a store from the same recipe and dumps its segments; each opens the other's dump,
 * reads every blob back (whole and ranged), finds the replaced version stored as a delta and the
 * mark, and collects without losing a segment. Go's twin is kotlin_fixture_test.go.
 *
 * KDB_WRITE_FIXTURES=1 rewrites the Kotlin-built dump Go reads
 * (go/kdb/storage/attachstore/testdata/kotlin-store.dump.gz).
 */
class GoAttachStoreFixtureTest {
    private val ns = "app/data"

    private fun text(
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

    private fun edit(base: ByteArray): ByteArray {
        val out = base.copyOf()
        var off = 100000
        while (off + 6 <= out.size) {
            "EDITED".encodeToByteArray().copyInto(out, off)
            off += 100000
        }
        return out
    }

    /** go's fixtureRecipe: inline at each end of the small range, a one- and a three-chunk blob,
     * and a replaced version (old, then new, last). */
    private fun recipe(): List<Pair<String, ByteArray>> {
        val newer = text(1200000, 5)
        return listOf(
            "tiny" to "hello attachments".encodeToByteArray(),
            "small-max" to text(256 shl 10, 1),
            "one-chunk" to text(300000, 2),
            "three-chunks" to text(2 * (1 shl 20) + 12345, 3),
            "delta-old" to edit(newer),
            "delta-new" to newer,
        )
    }

    private val mark = listOf("c1", "c2").map { KdbHash(kdbSha256(it.encodeToByteArray())) }

    private fun repoRoot(): File {
        var d: File? = File("").absoluteFile
        while (d != null && !File(d, "settings.gradle.kts").exists()) d = d.parentFile
        return d ?: error("repository root not found")
    }

    private suspend fun dump(shim: InMemoryPlatformIoShim): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(GZIPOutputStream(bytes)).use { out ->
            out.write(MAGIC.encodeToByteArray())
            for (name in shim.listSegments(ns).sorted()) {
                val data = ByteArrayOutputStream()
                var off = 0L
                while (true) {
                    val b = shim.readFromSegment(name, off, 1 shl 20)
                    if (b.isEmpty()) break
                    data.write(b)
                    off += b.size
                }
                val n = name.encodeToByteArray()
                out.writeInt(n.size)
                out.write(n)
                out.writeLong(data.size().toLong())
                out.write(data.toByteArray())
            }
        }
        return bytes.toByteArray()
    }

    private suspend fun load(bytes: ByteArray): InMemoryPlatformIoShim {
        val shim = InMemoryPlatformIoShim()
        DataInputStream(GZIPInputStream(bytes.inputStream())).use { input ->
            val magic = ByteArray(MAGIC.length)
            input.readFully(magic)
            check(magic.decodeToString() == MAGIC) { "not a segment dump" }
            while (true) {
                val n =
                    try {
                        input.readInt()
                    } catch (_: EOFException) {
                        break
                    }
                val name = ByteArray(n).also { input.readFully(it) }.decodeToString()
                val data = ByteArray(input.readLong().toInt()).also { input.readFully(it) }
                shim.appendToSegment(name, data)
            }
        }
        return shim
    }

    private suspend fun build(): InMemoryPlatformIoShim {
        val shim = InMemoryPlatformIoShim()
        val s = AttachmentBlobStore(shim, ns)
        val hashes = recipe().map { s.put(AttachmentSource.of(it.second)).hash }
        assertTrue(s.storeAsDelta(hashes[4], hashes[5]))
        s.setMark(mark)
        s.collect { true } // retire the replaced pointer
        return shim
    }

    private suspend fun check(shim: InMemoryPlatformIoShim) {
        val before = shim.listSegments(ns).sorted()
        val s = AttachmentBlobStore(shim, ns)
        for ((name, data) in recipe()) {
            val h = attachmentContentHash(data)
            val r = s.open(h)
            try {
                assertContentEquals(data, r.readAll(), name)
                if (data.size > 2000) {
                    val mid = ByteArray(1000)
                    assertEquals(1000, r.readAt(data.size / 2L, mid))
                    assertContentEquals(data.copyOfRange(data.size / 2, data.size / 2 + 1000), mid, name)
                }
            } finally {
                r.close()
            }
        }
        val old = attachmentContentHash(recipe()[4].second)
        // A delta's pointer is past generation 0 (it replaced the whole one), and its bytes are small.
        assertTrue(before.any { it.startsWith("ns/$ns/attachment/h-${old.toHex()}.") && !it.endsWith(".000000") }, "$before")
        assertEquals(mark, s.mark())
        assertEquals(0, s.collect { true })
        assertEquals(before, shim.listSegments(ns).sorted())
    }

    @Test
    fun readsTheStoreGoBuilt() =
        runBlocking {
            val bytes = javaClass.getResourceAsStream("/attachstore/go-store.dump.gz")!!.readBytes()
            check(load(bytes))
        }

    @Test
    fun buildsAStoreGoReads() =
        runBlocking {
            val dumped = dump(build())
            check(load(dumped))
            if (System.getenv("KDB_WRITE_FIXTURES") == "1") {
                File(repoRoot(), "go/kdb/storage/attachstore/testdata/kotlin-store.dump.gz").apply {
                    parentFile.mkdirs()
                    writeBytes(dumped)
                }
            }
        }

    private companion object {
        const val MAGIC = "KDBSEGDUMP1\n"
    }
}
