package dev.kdb.replica

import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Fast-forwarding through Go's chain fixture (packages/kdb-replica/test/golden/chain.json), and local commits. */
class NamespaceTest {
    private val chain = golden("chain.json").obj
    private val nsName = chain.s("namespace")
    private val byHash: Map<String, ReplicaCommit> =
        chain["commits"]!!.arr.map { ReplicaCommit.decode(hexToBytes(it.obj.s("payloadHex"))) }.associateBy { it.hash }

    private class Step(val from: String, val to: String, val commits: List<String>, val docs: Map<String, String>)

    private val steps =
        chain["steps"]!!.arr.map { s ->
            val o = s.obj
            Step(o.s("from"), o.s("to"), o["commits"]!!.arr.map { it.toString().trim('"') }, o["docs"]!!.obj.mapValues { it.value.toString().let { v -> wireJson.decodeFromString<String>(v) } })
        }

    private suspend fun docs(ns: ReplicaNamespace): Map<String, String> = ns.scan().associate { it.id to it.json }

    private fun commitsOf(step: Step) = step.commits.map { byHash.getValue(it) }

    @Test
    fun fastForwardStepByStepReachesEveryDeclaredTree() =
        runBlocking {
            val store = MemoryReplicaStore()
            val ns = ReplicaNamespace.open(store, nsName, NODE)
            assertEquals(steps[0].from, ns.head, "the replica starts at Go's genesis")
            for (step in steps) {
                assertEquals(step.from, ns.head)
                ns.fastForward(commitsOf(step), step.to)
                assertEquals(step.to, ns.head)
                assertEquals(byHash.getValue(step.to).treeHash, ns.rootHash())
                assertEquals(step.docs, docs(ns))
            }
            // Reopening reads the same state back from the store.
            val again = ReplicaNamespace.open(store, nsName, NODE)
            assertEquals(steps.last().to, again.head)
            assertEquals(steps.last().docs, docs(again))
            assertEquals(again.head, again.headCommit()?.hash)
        }

    @Test
    fun oneFastForwardOverTheWholeChainEqualsTheSteps() =
        runBlocking {
            val ns = ReplicaNamespace.open(MemoryReplicaStore(), nsName, NODE)
            ns.fastForward(byHash.values.reversed(), steps.last().to)
            assertEquals(steps.last().docs, docs(ns))
        }

    @Test
    fun aCommitWhoseTreeDoesNotMatchItsOpsWritesNothing() =
        runBlocking {
            val store = MemoryReplicaStore()
            val ns = ReplicaNamespace.open(store, nsName, NODE)
            val first = steps[0]
            ns.fastForward(commitsOf(first), first.to)
            val real = byHash.getValue(steps[1].to)
            // The same commit with one body changed: a valid commit with its own hash, whose ops no
            // longer build the tree it declares.
            val forged =
                ReplicaCommit.build(
                    parents = real.parents,
                    namespace = real.commit.namespaceId,
                    transactionId = real.commit.transactionId,
                    timestampMicros = real.timestampMicros,
                    authorNodeId = real.commit.authorNodeId,
                    ops = real.ops.map { if (it is KdbOp.Write) KdbOp.Write(it.docId, """{"forged":1}""") else it },
                    treeHash = real.treeHash,
                    message = real.commit.message,
                )
            val applies = store.applies
            assertFailsWith<TreeMismatchException> { ns.fastForward(listOf(forged), forged.hash) }
            assertEquals(applies, store.applies, "nothing written")
            assertEquals(first.to, ns.head)
            assertEquals(first.docs, docs(ns))
            ns.fastForward(listOf(real), real.hash)
            assertEquals(steps[1].docs, docs(ns))
        }

    @Test
    fun aRangeThatDoesNotDescendFromTheHeadIsRefused() =
        runBlocking {
            val ns = ReplicaNamespace.open(MemoryReplicaStore(), nsName, NODE)
            ns.fastForward(commitsOf(steps[0]), steps[0].to)
            assertFailsWith<NotFastForwardException> { ns.fastForward(commitsOf(steps.last()), steps.last().to) }
            val other = ReplicaNamespace.open(MemoryReplicaStore(), "x", NODE)
            assertFailsWith<NotFastForwardException> { other.fastForward(listOf(byHash.getValue(steps[1].to)), steps[1].to) }
            Unit
        }

    @Test
    fun localCommitsChainOnTheHeadVerifyAndBlockPullsUntilPushed() =
        runBlocking {
            val ns = ReplicaNamespace.open(MemoryReplicaStore(), nsName, NODE)
            val first = steps[0]
            ns.fastForward(commitsOf(first), first.to)
            val id = "0b1c2d3e-0000-4000-8000-000000000001"
            val c =
                ns.commitLocal(
                    listOf(KdbOp.Write(KdbUuid.fromString(id), """{"local":true}""")),
                    KdbUuid.fromString("0b1c2d3e-0000-4000-8000-0000000000aa"),
                    nowMicros = 5, // a slow clock: the commit still orders after what the replica has seen
                )
            assertEquals(listOf(first.to), c.parents)
            assertTrue(c.timestampMicros > byHash.getValue(first.to).timestampMicros)
            assertEquals(c.treeHash, ns.rootHash())
            assertEquals(c.hash, ReplicaCommit.decode(c.payload).hash)
            assertEquals(listOf(c.hash), ns.pending)
            assertEquals("""{"local":true}""", ns.get(id))
            // A second replica fast-forwarding through the local commit builds the same tree.
            val other = ReplicaNamespace.open(MemoryReplicaStore(), nsName, NODE)
            other.fastForward(commitsOf(first) + c, c.hash)
            assertEquals(c.treeHash, other.rootHash())
            assertFailsWith<NotFastForwardException> { ns.fastForward(commitsOf(steps[1]), steps[1].to) }
            Unit
        }

    @Test
    fun scanPagesById() =
        runBlocking {
            val ns = ReplicaNamespace.open(MemoryReplicaStore(), nsName, NODE)
            ns.fastForward(byHash.values.toList(), steps.last().to)
            val ids = steps.last().docs.keys.sorted()
            val page1 = ns.scan(limit = 2)
            assertEquals(ids.take(2), page1.map { it.id })
            val page2 = ns.scan(after = page1.last().id, limit = 100)
            assertEquals(ids.drop(2), page2.map { it.id })
        }

    @Test
    fun aProjectionResetReplacesPageByPage() =
        runBlocking {
            fun id(n: Int) = "${n.toString(16).padStart(8, '0')}-0000-4000-8000-000000000000"
            val p = ProjectionNamespace.open(MemoryReplicaStore(), "ns", "TRUE", true)
            p.applyPage("", "", ProjectPage("a1", false, (1..6).map { ReplicaDocument(id(it), """{"n":$it}""") }, emptyList(), "", true))
            // A reset delivering 2 and 5, in two pages: (start, id(3)] then (id(3), end).
            p.applyPage("", "", ProjectPage("b2", true, listOf(ReplicaDocument(id(2), """{"n":22}""")), emptyList(), id(3), false))
            assertEquals(listOf(id(2), id(4), id(5), id(6)), p.scan().map { it.id })
            assertEquals(id(3), p.transfer?.after)
            p.applyPage("", id(3), ProjectPage("b2", true, listOf(ReplicaDocument(id(5), """{"n":55}""")), emptyList(), "", true))
            assertEquals(mapOf(id(2) to """{"n":22}""", id(5) to """{"n":55}"""), p.scan().associate { it.id to it.json })
            assertEquals("b2", p.lastSource)
            assertNull(p.transfer)
        }

    private companion object {
        const val NODE = "99999999-9999-4999-8999-999999999999"
    }
}
