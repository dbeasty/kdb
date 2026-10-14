package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.toTimestampVal
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.kdbSha256
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Go's version 6 checkpoint as go/kdb/embed/checkpoint_section.go writes it, built here byte for
 * byte, and every way its section can fail to match its record. The interop tests in
 * go/kdb/integration/checkpoint_interop_test.go cover the versions a Go build writes end to end.
 */
class GoCheckpointTest {
    private val ns = "app/data"

    private fun hash(seed: Int): KdbHash = KdbHash.fromBytes(kdbSha256(byteArrayOf(seed.toByte())))

    private fun le64(v: Long) = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    /** One commit packed as dag.appendPackedCommit packs it. */
    private fun packCommit(
        c: KdbCommit,
        opCount: Int,
    ): ByteArray {
        val msg = c.message.encodeToByteArray()
        val b = ByteBuffer.allocate(116 + 32 * c.parentHashes.size + (if (c.schemaHash != null) 32 else 0) +
            (if (c.attachmentTreeHash != null) 32 else 0) + msg.size).order(ByteOrder.LITTLE_ENDIAN)
        b.put(c.hash.bytes).put(c.documentTreeHash.bytes)
        b.putLong(c.transactionId.msb).putLong(c.transactionId.lsb).putLong(c.authorNodeId.msb).putLong(c.authorNodeId.lsb)
        b.putLong(c.timestamp.epochMillis).putShort(c.timestamp.microRemainder.toShort())
        b.put(((if (c.schemaHash != null) 1 else 0) or (if (c.attachmentTreeHash != null) 2 else 0)).toByte())
        b.put(c.parentHashes.size.toByte()).putInt(opCount).putInt(msg.size)
        c.parentHashes.forEach { b.put(it.bytes) }
        c.schemaHash?.let { b.put(it.bytes) }
        c.attachmentTreeHash?.let { b.put(it.bytes) }
        b.put(msg)
        return b.array()
    }

    private val docs = (1..5).map { KdbUuid(0x1000L + it, it.toLong()) to hash(it) }

    private val commit =
        KdbCommit(
            hash = hash(100),
            parentHashes = listOf(hash(101)),
            namespaceId = ns,
            transactionId = KdbUuid(7, 8),
            timestamp = KdbTimestamp(1_700_000_000_123L, 456),
            authorNodeId = KdbUuid(9, 10),
            operations = emptyList(),
            documentTreeHash = DocumentTree.build(docs.toMap()).treeHash,
            schemaHash = hash(102),
            message = "packed",
            attachmentTreeHash = hash(103),
        )

    private class Built(
        val record: ByteArray,
        val sections: MutableMap<String, ByteArray>,
        val key: String,
    )

    private fun build(
        mutate: (MutableMap<Int, KdbValue>) -> Unit = {},
        section: (ByteArray) -> ByteArray = { it },
    ): Built {
        val tree = ByteArray(docs.size * 48)
        docs.sortedWith { a, b -> java.util.Arrays.compareUnsigned(a.first.uuidBytesBE(), b.first.uuidBytesBE()) }
            .forEachIndexed { i, (id, h) ->
                id.uuidBytesBE().copyInto(tree, i * 48)
                h.bytes.copyInto(tree, i * 48 + 16)
            }
        val commits = packCommit(commit, 3)
        val treeDigest = kdbSha256(tree)
        val commitsDigest = kdbSha256(commits)
        val key = "kdb:checkpoint-section:$ns:" +
            ByteArray(16) { (treeDigest[it].toInt() xor commitsDigest[it].toInt()).toByte() }.joinToString("") { "%02x".format(it) }
        val body = "KDBCPS01".encodeToByteArray() + le64(tree.size.toLong()) + le64(commits.size.toLong()) + tree + commits
        val crc = CRC32().apply { update(body) }.value.toInt()
        val bytes = body + ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(crc).array()
        val ts = KdbTimestamp(1_700_000_000_000L, 0).toTimestampVal()
        val fields =
            mutableMapOf<Int, KdbValue>(
                1 to KdbValue.Int32Val(6),
                2 to KdbValue.StringVal(ns),
                3 to KdbValue.Int64Val(4),
                4 to KdbValue.ArrayVal(emptyList()),
                5 to KdbValue.ArrayVal(listOf(KdbValue.RecordVal(mapOf(1 to KdbValue.StringVal("main"), 2 to KdbValue.FixedVal(commit.hash.bytes), 3 to ts, 4 to ts)))),
                6 to KdbValue.ArrayVal(emptyList()),
                7 to KdbValue.ArrayVal(emptyList()),
                8 to KdbValue.ArrayVal(emptyList()),
                9 to KdbValue.ArrayVal(emptyList()),
                10 to KdbValue.Int64Val(0),
                // At their defaults, so the encoder leaves them out of the bytes as Go's does.
                11 to KdbValue.ArrayVal(emptyList()),
                12 to KdbValue.ArrayVal(emptyList()),
                13 to KdbValue.BytesVal(ByteArray(0)),
                14 to KdbValue.BytesVal(ByteArray(0)),
                15 to KdbValue.BytesVal(byteArrayOf(1, 2, 3)),
                16 to KdbValue.StringVal(key),
                17 to KdbValue.FixedVal(treeDigest),
                18 to KdbValue.Int64Val(docs.size.toLong()),
                19 to KdbValue.Int64Val(tree.size.toLong()),
                20 to KdbValue.Int64Val(commits.size.toLong()),
                21 to KdbValue.Bool(true),
            )
        mutate(fields)
        val record = KdbValue.RecordVal(fields).encodeToBytes(KdbType.Ref(GoCheckpoint.FQN_CHECKPOINT), GoCheckpoint.registry)
        return Built(record, mutableMapOf(key to section(bytes)), key)
    }

    private fun KdbUuid.uuidBytesBE(): ByteArray = ByteBuffer.allocate(16).putLong(msb).putLong(lsb).array()

    private fun decode(b: Built): GoCheckpoint = runBlocking { GoCheckpoint.decode(ns, b.record) { b.sections[it] } }

    @Test
    fun version6_readsTheTreeAndCommitsFromItsSection() {
        val cp = decode(build())
        assertEquals(6, cp.version)
        assertEquals(4, cp.throughSequence)
        assertEquals(docs.toMap(), cp.liveTree.entries)
        assertEquals(commit.documentTreeHash, cp.liveTree.treeHash)
        val restored = cp.commits().toList().single()
        assertEquals(commit, restored.commit)
        assertEquals(3, restored.operationCount)
        assertTrue(cp.attachmentTables!!.contentEquals(byteArrayOf(1, 2, 3)))
        assertEquals("main", cp.branches.single().name)
    }

    @Test
    fun version6_attachmentTablesOnlyWhenField21SaysSo() {
        assertEquals(null, decode(build(mutate = { it[21] = KdbValue.Bool(false) })).attachmentTables)
    }

    private fun assertUnusable(
        b: Built,
        what: String,
    ) {
        val e = assertFailsWith<UnusableCheckpointException> { decode(b).commits().toList() }
        assertTrue(e.message!!.contains(what), "${e.message} should mention $what")
    }

    @Test
    fun version6_aSectionThatDoesNotMatchItsRecordIsUnusable() {
        assertUnusable(build().also { it.sections.clear() }, "missing")
        assertUnusable(build(section = { it.copyOf().also { s -> s[30] = (s[30] + 1).toByte() } }), "checksum")
        assertUnusable(build(section = { it.copyOf(it.size - 1) }), "bytes")
        assertUnusable(build(section = { it.copyOf().also { s -> s[0] = 'X'.code.toByte() } }), "does not match")
        // The tree changed and the CRC recomputed over it: only the digest catches it.
        assertUnusable(
            build(section = { s ->
                val c = s.copyOf()
                c[24 + 20] = (c[24 + 20] + 1).toByte()
                val crc = CRC32().apply { update(c, 0, c.size - 4) }.value.toInt()
                ByteBuffer.wrap(c).order(ByteOrder.LITTLE_ENDIAN).putInt(c.size - 4, crc)
                c
            }),
            "different tree",
        )
        assertUnusable(build(mutate = { it[18] = KdbValue.Int64Val(4) }), "malformed")
        assertUnusable(build(mutate = { it[2] = KdbValue.StringVal("app/other") }), "describes namespace")
        // A section filed under another key: not the one this record's digests name.
        val b = build()
        val other = "kdb:checkpoint-section:$ns:" + "0".repeat(32)
        val moved = build(mutate = { it[16] = KdbValue.StringVal(other) })
        moved.sections[other] = b.sections[b.key]!!
        assertUnusable(moved, "named after")
    }

    @Test
    fun versionsThisBuildDoesNotKnowAreUnusable() {
        assertUnusable(build(mutate = { it[1] = KdbValue.Int32Val(7) }), "format version 7")
        assertUnusable(build(mutate = { it[1] = KdbValue.Int32Val(1) }), "format version 1")
    }

    @Test
    fun packedTreesOutOfOrderAreUnusable() {
        val a = ByteArray(48).also { it[0] = 2 }
        val b = ByteArray(48).also { it[0] = 1 }
        assertFailsWith<UnusableCheckpointException> { GoCheckpoint.packedTree(a + b) }
        assertFailsWith<UnusableCheckpointException> { GoCheckpoint.packedTree(ByteArray(47)) }
        assertEquals(2, GoCheckpoint.packedTree(b + a).size)
    }
}
