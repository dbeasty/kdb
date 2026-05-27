package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbTransaction
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class InMemoryCommitDagTest {
    /** Matches Go `dag.NewInMemoryCommitDag("app/t")` genesis after RFC-6234 SHA-256 alignment. */
    @Test
    fun genesisCommitHash_matchesGoForNamespaceAppSlashT() =
        runTest {
            val dag = inMemoryCommitDag("app/t")
            assertEquals(
                "5ed3ef44f65fc2f179890bbeb3e5fb05fa0ed5972e3c8cb9bac045f1fe6602d8",
                dag.head().toHex(),
            )
        }

    @Test
    fun tc01_appendAndHeadMovesLinear() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val root = dag.head()
            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = root,
                    operations = emptyList(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val tree = DocumentTree.EMPTY
            val c = dag.appendCommit(tx, root, tree, schemaHash = null)
            assertEquals(c.hash, dag.head())
            assertNotNull(dag.getCommit(c.hash))
        }

    @Test
    fun tc02_putCommitIdempotent() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val root = dag.head()
            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = root,
                    operations = emptyList(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val c = dag.appendCommit(tx, root, DocumentTree.EMPTY, null)
            dag.putCommit(c, requireParents = true)
            dag.putCommit(c, requireParents = true)
            assertEquals(c.hash, dag.head())
        }

    @Test
    fun tc06_diffMaps() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val id1 = KdbUuid.random()
            val id2 = KdbUuid.random()
            val id3 = KdbUuid.random()
            val h1 = KdbHash.fromBytes(ByteArray(32) { 1 })
            val h2 = KdbHash.fromBytes(ByteArray(32) { 2 })
            val h1m = KdbHash.fromBytes(ByteArray(32) { 3 })
            val h3 = KdbHash.fromBytes(ByteArray(32) { 4 })

            val root = dag.head()
            val t1 =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = root,
                    operations = emptyList(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val treeA =
                DocumentTree.build(
                    mapOf(id1 to h1, id2 to h2),
                )
            val ca = dag.appendCommit(t1, root, treeA, null)

            val t2 =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = ca.hash,
                    operations = emptyList(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val treeB =
                DocumentTree.build(
                    mapOf(id1 to h1m, id3 to h3),
                )
            val cb = dag.appendCommit(t2, ca.hash, treeB, null)

            val d = dag.diff(ca.hash, cb.hash)
            assertEquals(id1, d.modified.single().docId)
            assertEquals(id3, d.added.single().docId)
            assertEquals(id2, d.removed.single().docId)
        }

    @Test
    fun tc07_emptyDiffSameCommit() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val h = dag.head()
            assertTrue(dag.diff(h, h).isEmpty)
        }

    @Test
    fun tc09_stubShowsInWalk() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val genesis = dag.head()
            val tx1 =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = genesis,
                    operations = emptyList(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val c1 = dag.appendCommit(tx1, genesis, DocumentTree.EMPTY, null)
            val tx2 =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = c1.hash,
                    operations = emptyList(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val c2 = dag.appendCommit(tx2, c1.hash, DocumentTree.EMPTY, null)
            dag.stubCommit(genesis, archiveLocation = "ice://bucket/obj")
            val w = dag.walk(from = c2.hash, limit = 10)
            assertTrue(w.any { it is TraversalEntry.Stubbed })
        }

    @Test
    fun compactionBlockedWhenBranchHeadInSquash() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val head = dag.head()
            assertFailsWith<CompactionSafetyException> {
                dag.squash(
                    squashHashes = listOf(head),
                    boundary = head,
                    syntheticTree = DocumentTree.EMPTY,
                    syntheticSchemaHash = null,
                )
            }
        }

    @Test
    fun missingParentRejected() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val bogusParent = KdbHash.fromBytes(ByteArray(32) { 0x7f })
            val bad =
                KdbCommit.build(
                    parentHashes = listOf(bogusParent),
                    namespaceId = "ns",
                    transactionId = KdbUuid.random(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                    operations = emptyList(),
                    documentTreeHash = DocumentTree.EMPTY.treeHash,
                    schemaHash = null,
                )
            assertFailsWith<DagConsistencyException> {
                dag.putCommit(bad, requireParents = true)
            }
        }
}
