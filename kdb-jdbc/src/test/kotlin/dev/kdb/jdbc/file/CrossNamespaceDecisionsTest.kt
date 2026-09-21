package dev.kdb.jdbc.file

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.commitViaEngine
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.zip.CRC32C
import kotlin.io.path.createTempDirectory
import kotlin.io.path.isDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

/**
 * The read side of Go's cross-namespace transaction protocol, in the Kotlin reference: the
 * formats (pinned against fixtures Go exports) and the replay rule (a part whose group never
 * committed is rolled back, with everything built on it).
 */
class CrossNamespaceDecisionsTest {
    private val g1 = "01020304-0506-0708-090a-0b0c0d0e0f10"
    private val g2 = "11121314-1516-1718-191a-1b1c1d1e1f20"

    @Test
    fun goDecisionFileAndMarkerReadIdentically() {
        val file = readGoFixture("txn_decision_file.hex")?.let { hex -> ByteArray(hex.length / 2) { hex.substring(it * 2, it * 2 + 2).toInt(16).toByte() } }
        val marker = readGoFixture("txn_group_marker.txt")
        if (file == null || marker == null) {
            println("Go txn fixtures missing - run `go test ./kdb/embed -run TestExportTxnGoldenFixtures`")
            return
        }
        // Two committed groups; the torn record after them is ignored, as Go ignores it.
        assertEquals(setOf(g1, g2), CrossNamespaceDecisions.readDecisionFile(file))

        val parsed = assertNotNull(CrossNamespaceDecisions.parseMarker(marker))
        assertEquals("host-a", parsed.host)
        assertEquals(7L, parsed.epoch)
        assertEquals(g1, parsed.group)
        assertEquals(listOf("bank/accounts", "bank/ledger"), parsed.parts)
        assertNull(CrossNamespaceDecisions.parseMarker("an ordinary commit message"))
    }

    @Test
    fun replayRollsBackAGroupThatNeverCommittedAndEverythingBuiltOnIt() =
        runBlocking {
            val root = createTempDirectory("kdb-xns").toString()
            val ns = "bank/accounts"
            val rt = openFileRuntime(root, "bank", ns)
            val before = write(rt, ns, """{"before":true}""")
            // A part of group g1 - epoch 7, this data root's host - then an ordinary commit on top.
            val part = write(rt, ns, """{"part":true}""", marker(host = "host-a", epoch = 7, group = g1))
            val after = write(rt, ns, """{"after":true}""")

            File(root, "txn").mkdirs()
            File(root, "txn/HOST").writeText("host-a\n")

            // The epoch's writer died without recording g1: rolled back, and the commit built on
            // it with it.
            writeDecisionFile(root, 7, CrossNamespaceDecisions.DEAD_SUFFIX, g2)
            openFileRuntime(root, "bank", ns).let {
                assertTrue(has(it, ns, before))
                assertFalse(has(it, ns, part), "a part of a group that never committed survived replay")
                assertFalse(has(it, ns, after), "a commit built on a rolled-back part survived replay")
            }

            // Recorded as committed: kept.
            writeDecisionFile(root, 7, CrossNamespaceDecisions.DEAD_SUFFIX, g1, g2)
            openFileRuntime(root, "bank", ns).let {
                assertTrue(has(it, ns, part))
                assertTrue(has(it, ns, after))
            }

            // No file for the epoch at all: it was sealed, so everything in it committed.
            File(root, "txn/" + CrossNamespaceDecisions.decisionFileName(7, CrossNamespaceDecisions.DEAD_SUFFIX)).delete()
            openFileRuntime(root, "bank", ns).let { assertTrue(has(it, ns, part)) }

            // A part written under another data root's log is not this log's to judge.
            writeDecisionFile(root, 7, CrossNamespaceDecisions.DEAD_SUFFIX, g2)
            File(root, "txn/HOST").writeText("host-b\n")
            openFileRuntime(root, "bank", ns).let { assertTrue(has(it, ns, part)) }
        }

    private fun marker(host: String, epoch: Long, group: String): String =
        "kdb:xns/1 host=$host epoch=$epoch group=$group parts=bank/accounts,bank/ledger"

    private suspend fun write(
        rt: EmbeddedKdbRuntime,
        ns: String,
        json: String,
        message: String = "",
    ): KdbUuid {
        val id = KdbUuid.random()
        val tx =
            KdbTransaction(
                KdbUuid.random(),
                rt.dag.head(),
                listOf(KdbOp.Write(id, json)),
                KdbTimestamp.now(),
                KdbUuid.random(),
            )
        commitViaEngine(rt, ns, tx, message = message)
        return id
    }

    private suspend fun has(
        rt: EmbeddedKdbRuntime,
        ns: String,
        id: KdbUuid,
    ): Boolean {
        val head = rt.dag.getCommitOrThrow(rt.dag.head())
        return rt.storage.getDocument(ns, id, head.documentTreeHash) != null
    }

    /** A decision file laid out exactly as Go writes one (pinned by the golden test above). */
    private fun writeDecisionFile(
        root: String,
        epoch: Long,
        suffix: String,
        vararg groups: String,
    ) {
        val buf = ByteBuffer.allocate(16 + 20 * groups.size)
        buf.put("KDBXNSD1".toByteArray(Charsets.US_ASCII))
        buf.putLong(epoch)
        for (g in groups) {
            val uuid = java.util.UUID.fromString(g)
            val rec = ByteBuffer.allocate(16).putLong(uuid.mostSignificantBits).putLong(uuid.leastSignificantBits).array()
            buf.put(rec)
            buf.putInt(CRC32C().apply { update(rec) }.value.toInt())
        }
        File(root, "txn/" + CrossNamespaceDecisions.decisionFileName(epoch, suffix)).writeBytes(buf.array())
    }

    private fun readGoFixture(name: String): String? {
        val start = Paths.get(System.getProperty("user.dir"))
        val repo = generateSequence(start) { it.parent }.firstOrNull { it.resolve("go").isDirectory() } ?: return null
        val f = repo.resolve("go/testdata/golden/physical/go/$name").toFile()
        return if (f.exists()) f.readText().trim() else null
    }
}
