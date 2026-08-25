package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.stream.InMemoryWireTransport
import dev.kdb.transaction.ConflictPolicy
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Component 39 fix tests (spec §7). Test 1 (simple fast-forward) is already covered by
 * [PeerSyncTest.pullMissing] and stays green with this fix in place - see that test's assertions
 * for the "already worked by accident, must keep working" regression check.
 *
 * Test 8 (RBAC interaction) is deliberately not covered here - the component spec's own
 * Non-Goals section defaults it to a separate fix unless scoping it in during implementation
 * "makes clear sense"; it doesn't here, since it's an orthogonal authorization concern, not a
 * divergence-detection one.
 */
class PeerSyncConflictDetectionTest {
    private val wire = defaultWireCodec()

    private class Side(val dag: CommitDag, val storage: InMemoryStorageAdapter)

    /** Two independent DAGs for the same namespace, sharing a deterministic genesis commit. */
    private fun forkTwoSides(ns: String): Pair<Side, Side> =
        Side(inMemoryCommitDag(ns), InMemoryStorageAdapter()) to
            Side(inMemoryCommitDag(ns), InMemoryStorageAdapter())

    private suspend fun writeDoc(
        side: Side,
        ns: String,
        parent: KdbHash,
        docId: KdbUuid,
        json: String,
    ): KdbCommit {
        val doc = KdbDocument(docId, json)
        side.storage.putDocument(ns, doc)
        val tree = side.storage.commitTree(ns, side.dag.getCommitOrThrow(parent).documentTreeHash)
        val tx =
            KdbTransaction(
                KdbUuid.random(),
                parent,
                listOf(KdbOp.Write(doc.id, doc.json)),
                KdbTimestamp.now(),
                KdbUuid.random(),
            )
        return side.dag.appendCommit(tx, parent, tree, null)
    }

    /**
     * Simulates a foreign commit crossing from one side's dag+storage to another's - what
     * handleCommitPush's put loop plus a real materializeCommit callback (kdb-embed's
     * EmbedOperations.kt) does together in a real deployment: putCommit stores the commit
     * record, and replaying its operations into local storage (then committing the resulting
     * tree) keeps `to.storage` able to build on top of it later, e.g. in resolveDivergence's
     * auto-merge path. Without this, `to.storage.commitTree` would fail with "missing parent
     * tree" for a commit whose tree was only ever registered on the *other* side.
     */
    private suspend fun receiveCommit(
        to: Side,
        ns: String,
        commit: KdbCommit,
    ) {
        to.dag.putCommit(commit, requireParents = true)
        val parentTree = to.dag.getCommitOrThrow(commit.parentHashes.first()).documentTreeHash
        for (op in commit.operations) {
            when (op) {
                is KdbOp.Write -> to.storage.putDocument(ns, KdbDocument(op.docId, op.patch))
                is KdbOp.Delete -> to.storage.deleteDocument(ns, op.docId)
                else -> Unit
            }
        }
        to.storage.commitTree(ns, parentTree)
    }

    @Test
    fun trueDivergence_strictSameDocument_producesConflictReport_headUnmoved() =
        runTest {
            val ns = "app/conflict-same-doc"
            val (a, b) = forkTwoSides(ns)
            val genesis = a.dag.head()
            val docId = KdbUuid.random()
            val commitA = writeDoc(a, ns, genesis, docId, """{"v":"a"}""")
            val commitB = writeDoc(b, ns, genesis, docId, """{"v":"b"}""")

            // Same as handleCommitPush's put loop: history always stored first.
            receiveCommit(a, ns, commitB)

            val outcome = resolveDivergence(a.dag, a.storage, ns, commitA.hash, commitB.hash, ConflictPolicy.STRICT)
            val conflict = outcome as? CommitPushOutcome.Conflict
            assertTrue(conflict != null, "expected Conflict, got $outcome")
            assertEquals(1, conflict.report.conflicts.size)
            assertEquals(docId.toString(), conflict.report.conflicts.single().documentId)

            // main did not move to either side unilaterally - this is the actual regression the
            // pre-fix code had (dag.setHead("main", msg.commits.last().hash) unconditionally).
            assertEquals(commitA.hash, a.dag.head())

            // No history lost (test 6): both commits are still present even though only one is
            // reachable from main.
            assertTrue(a.dag.hasCommit(commitA.hash))
            assertTrue(a.dag.hasCommit(commitB.hash))
        }

    @Test
    fun trueDivergence_strictDisjointDocuments_autoMerges() =
        runTest {
            val ns = "app/conflict-disjoint"
            val (a, b) = forkTwoSides(ns)
            val genesis = a.dag.head()
            val docIdA = KdbUuid.random()
            val docIdB = KdbUuid.random()
            val commitA = writeDoc(a, ns, genesis, docIdA, """{"v":"a"}""")
            val commitB = writeDoc(b, ns, genesis, docIdB, """{"v":"b"}""")
            receiveCommit(a, ns, commitB)

            // Chosen behavior for "different documents, no real content conflict" (§5, test 3):
            // auto-merge, same as APPEND_ONLY - this is the STRICT-policy case explicitly, to
            // prove the auto-merge path isn't gated on policy at all, only on whether a real
            // per-document conflict exists.
            val outcome = resolveDivergence(a.dag, a.storage, ns, commitA.hash, commitB.hash, ConflictPolicy.STRICT)
            val merged = outcome as? CommitPushOutcome.Merged
            assertTrue(merged != null, "expected Merged, got $outcome")

            // Merge-commit shape (test 10): a real two-parent commit, git-style.
            assertEquals(setOf(commitA.hash, commitB.hash), merged.mergeCommit.parentHashes.toSet())
            assertEquals(merged.mergeCommit.hash, a.dag.head())
            assertTrue(a.dag.hasCommit(commitA.hash))
            assertTrue(a.dag.hasCommit(commitB.hash))
        }

    @Test
    fun trueDivergence_appendOnly_bothSidesReachableAfterMerge() =
        runTest {
            val ns = "app/append-only"
            val (a, b) = forkTwoSides(ns)
            val genesis = a.dag.head()
            val docIdA = KdbUuid.random()
            val docIdB = KdbUuid.random()
            val commitA = writeDoc(a, ns, genesis, docIdA, """{"v":"a"}""")
            val commitB = writeDoc(b, ns, genesis, docIdB, """{"v":"b"}""")
            receiveCommit(a, ns, commitB)

            val outcome = resolveDivergence(a.dag, a.storage, ns, commitA.hash, commitB.hash, ConflictPolicy.APPEND_ONLY)
            val merged = outcome as? CommitPushOutcome.Merged
            assertTrue(merged != null, "expected Merged, got $outcome")

            // Zolik's match_results case: both sides' documents are actually present in the
            // resulting tree after merge, not just "some commit exists somewhere".
            val mergedTree = a.dag.getDocumentTreeOrThrow(merged.mergeCommit.documentTreeHash)
            assertTrue(mergedTree.contains(docIdA))
            assertTrue(mergedTree.contains(docIdB))
        }

    @Test
    fun threeWay_secondSyncDetectsAgainstAlreadyUpdatedHead() =
        runTest {
            val ns = "app/three-way"
            val coordinator = Side(inMemoryCommitDag(ns), InMemoryStorageAdapter())
            val peerA = Side(inMemoryCommitDag(ns), InMemoryStorageAdapter())
            val peerB = Side(inMemoryCommitDag(ns), InMemoryStorageAdapter())
            val genesis = coordinator.dag.head()
            val docIdA = KdbUuid.random()
            val docIdB = KdbUuid.random()
            val commitA = writeDoc(peerA, ns, genesis, docIdA, """{"v":"a"}""")
            val commitB = writeDoc(peerB, ns, genesis, docIdB, """{"v":"b"}""")

            // Sync A -> coordinator: coordinator was at genesis, A is a pure descendant.
            receiveCommit(coordinator, ns, commitA)
            val outcome1 = resolveDivergence(coordinator.dag, coordinator.storage, ns, genesis, commitA.hash, ConflictPolicy.APPEND_ONLY)
            assertTrue(outcome1 is CommitPushOutcome.FastForwarded)
            assertEquals(commitA.hash, coordinator.dag.head())

            // Sync B -> coordinator: coordinator's head is now commitA, NOT the original
            // genesis - the second sync must diverge-detect against that, not re-run against
            // genesis (which would wrongly see B as a plain fast-forward from genesis and lose
            // A's commit off main).
            receiveCommit(coordinator, ns, commitB)
            val localHeadBeforeSecondSync = coordinator.dag.head()
            assertEquals(commitA.hash, localHeadBeforeSecondSync)
            val outcome2 =
                resolveDivergence(
                    coordinator.dag,
                    coordinator.storage,
                    ns,
                    localHeadBeforeSecondSync,
                    commitB.hash,
                    ConflictPolicy.APPEND_ONLY,
                )
            val merged = outcome2 as? CommitPushOutcome.Merged
            assertTrue(merged != null, "expected Merged, got $outcome2")
            assertEquals(setOf(commitA.hash, commitB.hash), merged.mergeCommit.parentHashes.toSet())
            assertEquals(merged.mergeCommit.hash, coordinator.dag.head())
        }

    @Test
    fun ancestryLookupException_whenNoCommonAncestor() =
        runTest {
            // Two dags for *different* namespaces have no shared genesis - not something a real
            // deployment should ever hand to resolveDivergence (peer-sync only ever compares
            // heads within one namespace), but this pins §6's contract: an undeterminable
            // ancestry must fail loudly, not be silently treated as a fast-forward or a
            // divergence.
            val nsA = "app/isolated-a"
            val nsB = "app/isolated-b"
            val a = Side(inMemoryCommitDag(nsA), InMemoryStorageAdapter())
            val bDag = inMemoryCommitDag(nsB)
            var threw = false
            try {
                resolveDivergence(a.dag, a.storage, nsA, a.dag.head(), bDag.head(), ConflictPolicy.STRICT)
            } catch (e: AncestryLookupException) {
                threw = true
            }
            assertTrue(threw, "expected AncestryLookupException")
        }

    @Test
    fun pushAndPull_sameDivergence_sameClassification() =
        runTest {
            val ns = "app/push-pull-symmetry"

            // Push side: drive resolveDivergence directly, exactly as
            // PeerSyncFrameHandler.handleCommitPush does internally.
            val (pushLocal, pushRemote) = forkTwoSides(ns)
            val pushGenesis = pushLocal.dag.head()
            val pushDocId = KdbUuid.random()
            val pushCommitLocal = writeDoc(pushLocal, ns, pushGenesis, pushDocId, """{"v":"local"}""")
            val pushCommitIncoming = writeDoc(pushRemote, ns, pushGenesis, pushDocId, """{"v":"incoming"}""")
            receiveCommit(pushLocal, ns, pushCommitIncoming)
            val pushOutcome =
                resolveDivergence(
                    pushLocal.dag,
                    pushLocal.storage,
                    ns,
                    pushCommitLocal.hash,
                    pushCommitIncoming.hash,
                    ConflictPolicy.STRICT,
                )

            // Pull side: the same shaped divergence (same document, different content), driven
            // through the real host+client wire path so pullMissing's actual code executes, not
            // just a second direct resolveDivergence call.
            val pullHostSide = Side(inMemoryCommitDag(ns), InMemoryStorageAdapter())
            val pullGenesis = pullHostSide.dag.head()
            val pullDocId = KdbUuid.random()
            val pullCommitHost = writeDoc(pullHostSide, ns, pullGenesis, pullDocId, """{"v":"host"}""")
            val host = peerSyncHost(wire, pullHostSide.dag, pullHostSide.storage)
            host.start(PeerHostConfig(ns, "host", ns, conflictPolicy = ConflictPolicy.STRICT))

            val pullLocalDag = inMemoryCommitDag(ns)
            val pullLocalStorage = InMemoryStorageAdapter()
            val pullCommitLocal = writeDoc(Side(pullLocalDag, pullLocalStorage), ns, pullGenesis, pullDocId, """{"v":"local"}""")
            val client = peerSyncClient(wire, InMemoryWireTransport(), pullLocalDag, pullLocalStorage)
            val session = client.connect(PeerClientConfig(ns, "puller", "memory://$ns", conflictPolicy = ConflictPolicy.STRICT))
            val pullResult = session.pullMissing()
            client.disconnect()
            host.stop()

            // Both scenarios are "same document, divergent content" - both must classify as a
            // real conflict, and both must leave main unmoved from the local side's own commit.
            assertTrue(pushOutcome is CommitPushOutcome.Conflict, "push: expected Conflict, got $pushOutcome")
            assertTrue(pullResult.conflict != null, "pull: expected a conflict report, got $pullResult")
            assertEquals(pullCommitLocal.hash, pullLocalDag.head(), "pull must not have moved main to the host's commit")
        }
}
