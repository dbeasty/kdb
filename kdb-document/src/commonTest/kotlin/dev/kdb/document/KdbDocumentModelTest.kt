package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class KdbDocumentModelTest {

    @Test
    fun fromJson_assignsFreshId() {
        val d = KdbDocument.fromJson("""{"a":1}""")
        assertTrue(d.id.toString().matches(Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")))
        assertEquals("""{"a":1}""", d.json)
    }

    @Test
    fun fromJson_withId_preservesId() {
        val id = KdbUuid.fromString("12345678-1234-4123-8123-123456789012")
        val d = KdbDocument.fromJson(id, """{"x":true}""")
        assertEquals(id, d.id)
    }

    @Test
    fun documentBody_roundTrip() {
        val d = KdbDocument.fromJson(KdbUuid.random(), """{"k":"v"}""")
        val back = KdbDocument.fromDocumentBodyValue(d.toDocumentBodyValue())
        assertEquals(d.id, back.id)
        assertEquals(d.json, back.json)
    }

    @Test
    fun contentHash_deterministic() {
        val id = KdbUuid.random()
        val a = KdbDocument(id, """{"a":1}""")
        val b = KdbDocument(id, """{"a":1}""")
        assertEquals(a.contentHash.toHex(), b.contentHash.toHex())
    }

    @Test
    fun merge_rootLevelOverwrite() {
        val d = KdbDocument.fromJson("""{"a":1,"b":2}""")
        val m = d.merge("""{"b":99,"c":3}""")
        assertEquals("""{"a":1,"b":99,"c":3}""", m.json)
    }

    @Test
    fun merge_preservesNestedJson() {
        val d = KdbDocument.fromJson("""{"a":{"x":1,"y":2}}""")
        val m = d.merge("""{"z":3}""")
        assertEquals("""{"a":{"x":1,"y":2},"z":3}""", m.json)
    }

    @Test
    fun documentTree_withAndWithout() {
        val id = KdbUuid.random()
        val h = KdbHash(ByteArray(32) { 3 })
        val t = DocumentTree.EMPTY.with(id, h).without(id)
        assertEquals(DocumentTree.EMPTY.treeHash.toHex(), t.treeHash.toHex())
    }

    @Test
    fun documentTree_hashDeterministic() {
        val id1 = KdbUuid.fromString("11111111-1111-4111-8111-111111111111")
        val id2 = KdbUuid.fromString("22222222-2222-4222-8222-222222222222")
        val h1 = KdbHash(ByteArray(32) { 1 })
        val h2 = KdbHash(ByteArray(32) { 2 })
        val a = DocumentTree.build(mapOf(id1 to h1, id2 to h2))
        val b = DocumentTree.build(mapOf(id2 to h2, id1 to h1))
        assertEquals(a.treeHash.toHex(), b.treeHash.toHex())
    }

    @Test
    fun commitHash_deterministic() {
        val c =
            sampleCommit(
                KdbHash.fromHex("01".repeat(32)),
            )
        val h1 = computeCommitHash(c)
        val h2 = computeCommitHash(c)
        assertEquals(h1.toHex(), h2.toHex())
    }

    @Test
    fun op_roundTrip_allTypes() {
        val id = KdbUuid.random()
        val hash = KdbHash(ByteArray(32) { 7 })
        val ops: List<KdbOp> =
            listOf(
                KdbOp.Write(id, """{"p":1}"""),
                KdbOp.Delete(id),
                KdbOp.FileWrite("p/x", hash),
                KdbOp.SchemaMigration(id, """{"m":1}"""),
            )
        for (op in ops) {
            assertEquals(op, KdbOp.fromKdbValue(op.toKdbValue()))
        }
    }

    @Test
    fun fromDocumentBody_badUuid_throws() {
        val bad =
            KdbValue.RecordVal(
                mapOf(
                    2 to KdbValue.StringVal("{}"),
                ),
            )
        assertFailsWith<DocumentDecodeException> {
            KdbDocument.fromDocumentBodyValue(bad)
        }
    }

    @Test
    fun mergeCommit_twoParents() {
        val p1 = KdbHash(ByteArray(32) { 1 })
        val p2 = KdbHash(ByteArray(32) { 2 })
        val c =
            KdbCommit(
                hash = KdbHash(ByteArray(32)), // placeholder
                parentHashes = listOf(p1, p2),
                namespaceId = "ns",
                transactionId = KdbUuid.random(),
                timestamp = KdbTimestamp(epochMillis = 1L, microRemainder = 0),
                authorNodeId = KdbUuid.random(),
                operations = emptyList(),
                documentTreeHash = KdbHash(ByteArray(32) { 4 }),
                schemaHash = null,
                message = "m",
            )
        val real = c.copy(hash = computeCommitHash(c))
        val bytes = real.toPayloadBytes()
        val back = KdbCommit.fromPayloadBytes(bytes)
        assertEquals(2, back.parentHashes.size)
        assertEquals(real, back)
    }

    @Test
    fun commitStub_preservesOriginalHash() {
        val reg = KdbDocumentWireRegistry()
        val oh = KdbHash(ByteArray(32) { 9 })
        val stub =
            CommitStub(
                originalHash = oh,
                archiveLocation = "s3://x",
                stubbedAt = KdbTimestamp(epochMillis = 5L, microRemainder = 0),
            )
        val blob = stub.toKdbValue().encodeToBytes(CommitStubWireType, reg)
        val round = KdbValue.decodeFromBytes(blob, CommitStubWireType, reg)
        val s2 = CommitStub.fromKdbValue(round)
        assertContentEquals(oh.bytes, s2.originalHash.bytes)
    }

    @Test
    fun fromJson_arrayRoot_throws() {
        assertFailsWith<DocumentDecodeException> {
            KdbDocument.fromJson("""[1,2,3]""")
        }
    }

    private fun sampleCommit(treeHash: KdbHash): KdbCommit {
        val base =
            KdbCommit(
                hash = KdbHash(ByteArray(32)),
                parentHashes = listOf(KdbHash(ByteArray(32) { 2 })),
                namespaceId = "n",
                transactionId = KdbUuid.random(),
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
                operations =
                    listOf(
                        KdbOp.Write(KdbUuid.random(), "{}"),
                    ),
                documentTreeHash = treeHash,
                schemaHash = null,
                message = "hi",
            )
        return base.copy(hash = computeCommitHash(base))
    }
}
