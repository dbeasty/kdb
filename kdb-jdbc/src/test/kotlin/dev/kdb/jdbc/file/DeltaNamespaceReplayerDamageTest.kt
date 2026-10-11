package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.delta.DamagedSegmentException
import dev.kdb.storage.delta.DeltaSegmentFactory
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import dev.kdb.storage.engine.DefaultStorageEngineFactory
import dev.kdb.storage.engine.StorageEngineTarget
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

/**
 * N5, Kotlin's side of the rule Go's replay applies (go/kdb/embed/damaged_segment_test.go): a torn
 * tail is bytes at the very end of a segment with nothing intact after them, and is recovered in
 * the newest segment and, after a restart opened a newer one, in an older one. Damage that an
 * intact frame follows is refused wherever it is - a garbled frame header used to end a segment
 * as quietly as a torn tail in any segment, newest included.
 */
class DeltaNamespaceReplayerDamageTest {
    private val ns = "app/n5"
    private val shim = InMemoryPlatformIoShim()
    private val config = StorageEngineConfig(globalMemoryBudgetBytes = 2_000_000, ioShim = shim, compressionCodec = CompressionCodec.NONE)

    private fun chain(n: Int): List<KdbCommit> =
        runBlocking {
            var parent = inMemoryCommitDag(ns).head()
            List(n) { i ->
                KdbCommit
                    .build(
                        parentHashes = listOf(parent),
                        namespaceId = ns,
                        transactionId = KdbUuid.random(),
                        timestamp = KdbTimestamp.now(),
                        authorNodeId = KdbUuid.random(),
                        operations = listOf(KdbOp.Write(KdbUuid.random(), """{"n":$i}""")),
                        documentTreeHash = KdbHash.fromHex("cc".repeat(32)),
                        schemaHash = null,
                    ).also { parent = it.hash }
            }
        }

    /** Writes each group of commits into a segment of its own, as one run per group would. */
    private fun writeSegments(vararg groups: List<KdbCommit>) =
        runBlocking {
            for (group in groups) {
                val w = DeltaSegmentFactory(config).openWriter(ns)
                for (c in group) {
                    w.append(
                        DeltaRecord(
                            commitHash = c.hash,
                            namespaceId = ns,
                            authorship = DeltaAuthorshipEnvelope("p", c.timestamp, "", ""),
                            commitPayload = c.toPayloadBytes(),
                            documentPatches = emptyList(),
                        ),
                    )
                }
                w.seal()
            }
        }

    /** Frame offsets of segment [seq], read by the frames' own declared lengths. */
    private fun frameOffsets(raw: ByteArray): List<Int> {
        val out = mutableListOf<Int>()
        var off = 0
        while (off + 20 <= raw.size) {
            val n = ((raw[off + 8].toInt() and 0xFF) shl 24) or ((raw[off + 9].toInt() and 0xFF) shl 16) or
                ((raw[off + 10].toInt() and 0xFF) shl 8) or (raw[off + 11].toInt() and 0xFF)
            out += off
            off += 20 + n
        }
        return out
    }

    private fun edit(
        seq: Long,
        change: (ByteArray, List<Int>) -> ByteArray,
    ) = runBlocking {
        // Rewrite every segment, in sequence order: the in-memory shim lists segments in the order
        // they were created, and replay relies on that being sequence order.
        val names = shim.listSegments(ns).sorted()
        val contents = names.associateWith { shim.readFromSegment(it, 0, 1 shl 20) }.toMutableMap()
        val name = SegmentNameBuilder.deltaSequenced(ns, seq)
        val raw = contents.getValue(name)
        contents[name] = change(raw.copyOf(), frameOffsets(raw))
        for (n in names) shim.deleteSegment(n)
        for (n in names) shim.appendToSegment(n, contents.getValue(n))
    }

    /** Replays through the server storage engine, as openFileRuntime does. */
    private fun replay(): KdbHash =
        runBlocking {
            val handle = DefaultStorageEngineFactory(StorageEngineTarget.SERVER).open(ns, config)
            DeltaNamespaceReplayer.replay(inMemoryCommitDag(ns), handle.adapter, handle.deltaReader!!)
        }

    @Test
    fun aGarbledHeaderInTheMiddleOfTheNewestSegmentIsRefused() {
        val c = chain(6)
        writeSegments(c.subList(0, 3), c.subList(3, 6))
        edit(1) { b, f -> b.also { it[f[1]] = (it[f[1]].toInt() xor 0xFF).toByte() } }
        val e = assertFailsWith<DamagedSegmentException> { replay() }
        assertEquals(1, e.sequenceNumber)
        assertTrue(e.nextIntactFrame > e.offset)
    }

    @Test
    fun aGarbledHeaderInTheMiddleOfAnOlderSegmentIsRefused() {
        val c = chain(6)
        writeSegments(c.subList(0, 3), c.subList(3, 6))
        edit(0) { b, f -> b.also { it[f[1]] = (it[f[1]].toInt() xor 0xFF).toByte() } }
        val e = assertFailsWith<DamagedSegmentException> { replay() }
        assertEquals(0, e.sequenceNumber)
        assertTrue(!e.builtOn, "the damage is visible in the segment itself")
    }

    // The kill -9 sequence: a run dies mid-append, the next opens a newer segment and writes on
    // top of the last intact commit. The torn tail now ends a segment that is not the newest.
    @Test
    fun aTornTailInAnOlderSegmentStillRecovers() {
        val c = chain(3)
        val lost = c[2]
        var parent = c[1].hash
        val later =
            List(2) { i ->
                KdbCommit
                    .build(
                        parentHashes = listOf(parent),
                        namespaceId = ns,
                        transactionId = KdbUuid.random(),
                        timestamp = KdbTimestamp.now(),
                        authorNodeId = KdbUuid.random(),
                        operations = listOf(KdbOp.Write(KdbUuid.random(), """{"later":$i}""")),
                        documentTreeHash = KdbHash.fromHex("cc".repeat(32)),
                        schemaHash = null,
                    ).also { parent = it.hash }
            }
        writeSegments(c, later)
        // The last append's body never fully landed: a frame failing its CRC with nothing after it.
        edit(0) { b, f -> b.also { for (i in (f[2] + b.size) / 2 until b.size) it[i] = 0 } }
        assertEquals(later.last().hash, replay(), "replay should end at the last commit, ${lost.hash} dropped with the torn tail")
    }

    // A last frame damaged after later segments built on it reads like a torn tail on its own;
    // the later commits' missing parent is what gives it away.
    @Test
    fun aDamagedLastFrameThatLaterCommitsBuildOnIsRefused() {
        val c = chain(6)
        writeSegments(c.subList(0, 3), c.subList(3, 6))
        edit(0) { b, f -> b.also { it[f[2] + 22] = (it[f[2] + 22].toInt() xor 0x01).toByte() } }
        val e = assertFailsWith<DamagedSegmentException> { replay() }
        assertEquals(0, e.sequenceNumber)
        assertTrue(e.builtOn)
    }
}
