package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Cross-language parity for the trie-based tree hash (DocumentTreeTrie.kt
 * / go/kdb/document/document_tree_trie.go). Since there is no automated
 * interop test comparing live Go and Kotlin hash output (the existing
 * TestKotlinPutThenGoGet_InteropDelta only round-trips content, not
 * hashes - see docs/benchmarks/phases-1-6-summary.md), these fixed
 * vectors were generated once from the Go implementation and hardcoded
 * on both sides. If either implementation's algorithm changes, both this
 * test and its Go counterpart must be regenerated together or Go/Kotlin
 * will silently diverge.
 */
class DocumentTreeTrieParityTest {
    private fun uuid(msb: Long, lsb: Long) = KdbUuid(msb, lsb)

    private fun hash(fill: Byte): KdbHash = KdbHash(ByteArray(32) { fill })

    @Test
    fun emptyTreeHashIsAllZero() {
        assertEquals(
            "0000000000000000000000000000000000000000000000000000000000000000",
            DocumentTree.EMPTY.treeHash.toHex(),
        )
    }

    @Test
    fun singleEntryMatchesGoVector() {
        val id1 = uuid(0x0102030405060708L, 0x0910111213141516L)
        val h1 = hash(0xAA.toByte())
        val tree = DocumentTree.build(mapOf(id1 to h1))
        assertEquals(
            "cad0411538c8a709c1d77ae06e5ba73f3bd3a81c8279bd7f8df9924c7cabbc81",
            tree.treeHash.toHex(),
        )
    }

    @Test
    fun threeEntriesMatchesGoVector() {
        val id1 = uuid(0x0102030405060708L, 0x0910111213141516L)
        val h1 = hash(0xAA.toByte())
        val id2 = uuid(0x2122232425262728L, 0x2930313233343536L)
        val h2 = hash(0xBB.toByte())
        val id3 = uuid(0x4142434445464748L, 0x4950515253545556L)
        val h3 = hash(0xCC.toByte())

        val fromScratch = DocumentTree.build(mapOf(id1 to h1, id2 to h2, id3 to h3))
        val expected = "966e605b63935339dba21d8557c0bfb6e1fe117c4ffa1ab57d2682a65cd36831"
        assertEquals(expected, fromScratch.treeHash.toHex())

        // Incremental (different insertion order) must match the same vector.
        var incremental = DocumentTree.EMPTY
        incremental = incremental.with(id2, h2)
        incremental = incremental.with(id1, h1)
        incremental = incremental.with(id3, h3)
        assertEquals(expected, incremental.treeHash.toHex())
    }
}
