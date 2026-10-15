package dev.kdb.jdbc.file

import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.schema.KdbType
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.AttachmentSource
import dev.kdb.embed.PutAttachmentOptions
import dev.kdb.embed.listAttachments
import dev.kdb.embed.putAttachment
import dev.kdb.embed.putJson
import dev.kdb.storage.io.FileBackedPlatformIoShimFactory
import dev.kdb.storage.io.PlatformIoConfig
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Files
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The file runtime's checkpoint policy (NamespaceCheckpointer, wired in openFileRuntime): written in
 * Go's format at close and after a full replay, and never where it could claim what this runtime
 * does not hold or replace a checkpoint it cannot read. CheckpointInteropTest (kdb-integration) has
 * Go open what is written here.
 */
class NamespaceCheckpointerTest {
    private val ns = "app/data"

    private fun open(dir: String): EmbeddedKdbRuntime = openFileRuntime(dir, "app", ns, acquireDirectoryLock = false)

    private fun cpFile(dir: String) = File(dir, "snap/kdb_checkpoint_app%2Fdata")

    private suspend fun read(dir: String): GoCheckpoint? {
        val io = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = dir))
        return GoCheckpoint.read(io, ns)
    }

    private suspend fun put(rt: EmbeddedKdbRuntime, n: Int) {
        putJson(rt, ns, """{"id":"${KdbUuid(0x1000, n.toLong())}","n":$n}""")
    }

    @Test
    fun closeWritesACheckpointTheNextOpenUses() =
        runBlocking {
            val dir = createTempDirectory("kdb-cpw").toString()
            val rt = open(dir)
            for (i in 1..20) put(rt, i)
            val head = rt.dag.head()
            rt.close()
            rt.close() // a second close does nothing
            val cp = assertNotNull(read(dir), "no checkpoint after close")
            assertEquals(GoCheckpoint.VERSION_LEGACY, cp.version)
            assertEquals(0, cp.throughSequence, "the sealed segment is claimed")
            assertEquals(head, cp.branches.single { it.name == "main" }.headHash)
            assertEquals(20, cp.liveTree.size)
            assertEquals(1, cp.segments.size)
            assertNotNull(cp.segments.single().stampSize, "the segment is stamped, so the next open reads none")

            val again = open(dir)
            assertEquals(head, again.dag.head())
            assertTrue(""""n":7""" in again.storage.getDocument(ns, KdbUuid(0x1000, 7), head)!!.json)
            again.close()
            // Nothing changed in that session: the checkpoint is not rewritten.
            assertEquals(0, read(dir)!!.throughSequence)
        }

    @Test
    fun operationCountsSurviveACheckpointOfACheckpoint() =
        runBlocking {
            // Commits restored without their operations keep their counts in the next checkpoint:
            // a count of 0 would read as a commit that wrote nothing.
            val dir = createTempDirectory("kdb-cpw-ops").toString()
            val rt = open(dir)
            for (i in 1..5) put(rt, i)
            rt.close()
            val again = open(dir)
            put(again, 6)
            again.close()
            val cp = read(dir)!!
            assertEquals(1, cp.throughSequence)
            val counts = cp.commits().map { it.commit.message to it.operationCount }.toList()
            assertEquals(7, counts.size, "genesis and six writes: $counts")
            assertEquals(6, counts.count { it.second == 1 }, "$counts")
            val third = open(dir)
            for (i in 1..6) assertNotNull(third.storage.getDocument(ns, KdbUuid(0x1000, i.toLong()), third.dag.head()))
            third.close()
        }

    @Test
    fun aSegmentAnotherRuntimeIsStillWritingIsNotClaimed() =
        runBlocking {
            val dir = createTempDirectory("kdb-cpw-two").toString()
            val a = open(dir)
            put(a, 1) // segment 0
            val b = open(dir) // replays segment 0, writes through it, and logs to segment 1
            assertEquals(0, read(dir)!!.throughSequence)
            put(a, 2) // segment 0 now ends at a commit b never saw
            val before = Files.readAllBytes(cpFile(dir).toPath())
            put(b, 3)
            b.close() // would claim segments 0 and 1
            assertContentEquals(before, Files.readAllBytes(cpFile(dir).toPath()), "b claimed a's segment")
            a.close() // claims only segment 0, which a holds whole
            val reopened = open(dir)
            for (i in 1..3) assertNotNull(reopened.storage.getDocument(ns, KdbUuid(0x1000, i.toLong()), reopened.dag.head()), "document $i")
        }

    @Test
    fun aCheckpointThisBuildCannotReadIsLeftAsItIs() =
        runBlocking {
            val dir = createTempDirectory("kdb-cpw-foreign").toString()
            val rt = open(dir)
            for (i in 1..3) put(rt, i)
            // No close: a checkpoint from a newer build is on disk instead.
            val io = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = dir))
            val newer =
                KdbValue.RecordVal(
                    mapOf(1 to KdbValue.Int32Val(99), 2 to KdbValue.StringVal(ns), 3 to KdbValue.Int64Val(0), 4 to KdbValue.ArrayVal(emptyList()),
                        5 to KdbValue.ArrayVal(emptyList()), 6 to KdbValue.ArrayVal(emptyList()), 7 to KdbValue.ArrayVal(emptyList()),
                        8 to KdbValue.ArrayVal(emptyList()), 9 to KdbValue.ArrayVal(emptyList()), 10 to KdbValue.Int64Val(0),
                        11 to KdbValue.ArrayVal(emptyList()), 12 to KdbValue.ArrayVal(emptyList()), 13 to KdbValue.BytesVal(ByteArray(0)),
                        14 to KdbValue.BytesVal(ByteArray(0)), 15 to KdbValue.BytesVal(ByteArray(0)), 16 to KdbValue.StringVal(""),
                        17 to KdbValue.FixedVal(ByteArray(32)), 18 to KdbValue.Int64Val(0), 19 to KdbValue.Int64Val(0), 20 to KdbValue.Int64Val(0),
                        21 to KdbValue.Bool(false)),
                ).encodeToBytes(KdbType.Ref(GoCheckpoint.FQN_CHECKPOINT), GoCheckpoint.registry)
            io.writeSnapshot(GoCheckpoint.key(ns), newer)
            val again = open(dir) // replays in full; must not write over it
            put(again, 4)
            again.close()
            assertContentEquals(newer, Files.readAllBytes(cpFile(dir).toPath()))
            val last = open(dir)
            assertEquals(4, last.dag.getDocumentTreeOrThrow(last.dag.getCommitOrThrow(last.dag.head()).documentTreeHash).size)
        }

    @Test
    fun attachmentsMakeItVersion6WithOneSectionReplacedEachTime() =
        runBlocking {
            // Version 2 has no place for attachment tables, so this is version 6 whatever its size
            // (the past-the-bound case, 21,000 entries, is CheckpointInteropTest's).
            val dir = createTempDirectory("kdb-cpw-v6").toString()
            val rt = open(dir)
            for (i in 1..10) put(rt, i)
            rt.putAttachment(ns, KdbUuid(0x1000, 1), AttachmentSource.of(ByteArray(100) { it.toByte() }), PutAttachmentOptions(name = "a.bin"))
            rt.close()
            val cp = read(dir)!!
            assertEquals(GoCheckpoint.VERSION_SECTION, cp.version)
            assertNotNull(cp.attachmentTables, "the head's attachment table is in the checkpoint")
            val sections = { File(dir, "snap").list()!!.filter { it.startsWith("kdb_checkpoint-section_") } }
            val first = sections().single()
            val again = open(dir)
            assertEquals(1, again.listAttachments(ns, KdbUuid(0x1000, 1)).size)
            put(again, 11)
            again.close()
            assertFalse(first == sections().single(), "the section was not replaced")
            assertEquals(11, read(dir)!!.liveTree.size)
        }
}
