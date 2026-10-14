package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.error.ConflictReport
import dev.kdb.storage.StorageAdapter
import dev.kdb.transaction.ConflictPolicy

/** A page's byte budget when the request names none (Go's peersync.DefaultPageBytes). */
internal const val DEFAULT_PAGE_BYTES: Int = 4 shl 20

/** Go's trackingBranchPrefix: local bookkeeping about another peer, never replicated. */
private const val TRACKING_BRANCH_PREFIX = "peer/"

/**
 * What a v2 sync needs of the namespace it runs on: its history, its live documents, its blobs
 * (null: this node keeps none, and does not advertise attachments), and the hook that applies an
 * adopted commit to live state (indexes, streams).
 */
public class PeerSyncNamespace(
    public val namespaceId: String,
    public val dag: CommitDag,
    public val storage: StorageAdapter,
    public val blobs: PeerBlobStore? = null,
    public val materializeCommit: CommitMaterializer? = null,
    public val conflictPolicy: ConflictPolicy = ConflictPolicy.STRICT,
)

/** One ref, as v2 names it. */
internal data class NamedRef(
    val kind: String,
    val name: String,
    val hex: String,
) {
    val key: String get() = "$kind:$name"
}

/** A namespace's refs as a v2 node advertises them. */
internal suspend fun refsOf(
    namespaceId: String,
    dag: CommitDag,
): NamespaceRefs {
    val branches = LinkedHashMap<String, String>()
    for (b in dag.listBranches()) {
        if (b.name.startsWith(TRACKING_BRANCH_PREFIX)) continue
        branches[b.name] = b.headHash.toHex()
    }
    val tags = dag.listTags().associate { it.name to it.commitHash.toHex() }
    return NamespaceRefs(namespaceId, branches, tags)
}

/** main, then the other branches, then tags, each sorted (Go's orderedRefs). */
internal fun orderedRefs(refs: NamespaceRefs): List<NamedRef> {
    val out = ArrayList<NamedRef>()
    refs.branches[MAIN_BRANCH]?.let { out += NamedRef(SyncV2.REF_BRANCH, MAIN_BRANCH, it) }
    for (name in refs.branches.keys.filter { it != MAIN_BRANCH }.sorted()) out += NamedRef(SyncV2.REF_BRANCH, name, refs.branches.getValue(name))
    for (name in refs.tags.keys.sorted()) out += NamedRef(SyncV2.REF_TAG, name, refs.tags.getValue(name))
    return out
}

/** What [applyRef] did. */
internal data class RefOutcome(
    val outcome: String,
    val head: KdbHash,
    val conflict: ConflictReport? = null,
)

/**
 * Decides what a proposed ref value does to this node's ref of the same name, and does it (Go's
 * ApplyRef). The proposed commit must already be stored.
 *  - main goes through [resolveDivergence]: fast-forward, merge, or a conflict report - the same
 *    decision a v1 push or pull makes. A fast-forward applies the commits it adopts to live state.
 *  - any other branch moves only forward: created if absent, fast-forwarded if the proposal
 *    descends from it, left alone if it is already ahead, a conflict if the two diverged.
 *  - a tag never moves: created if absent, a conflict if it names another commit.
 */
internal suspend fun applyRef(
    ns: PeerSyncNamespace,
    kind: String,
    name: String,
    proposed: KdbHash,
): RefOutcome {
    val dag = ns.dag
    if (!dag.hasCommit(proposed)) throw PeerSyncException("peer sync: $kind \"$name\" names commit ${proposed.toHex()}, which was never sent")
    when (kind) {
        SyncV2.REF_TAG -> {
            val t = dag.getTag(name)
            if (t == null) {
                dag.createTag(name, proposed, "replicated")
                return RefOutcome(SyncV2.OUTCOME_CREATED, proposed)
            }
            if (t.commitHash == proposed) return RefOutcome(SyncV2.OUTCOME_NOOP, proposed)
            return RefOutcome(SyncV2.OUTCOME_CONFLICT, t.commitHash, refConflict(t.commitHash, proposed))
        }
        SyncV2.REF_BRANCH, "" -> {
            if (name == MAIN_BRANCH) return applyMain(ns, proposed)
            val b = dag.getBranch(name)
            if (b == null) {
                dag.createBranch(name, proposed)
                return RefOutcome(SyncV2.OUTCOME_CREATED, proposed)
            }
            return when (resolveHeadUpdate(dag, b.headHash, proposed)) {
                HeadUpdate.FastForward -> {
                    dag.setHead(name, proposed)
                    RefOutcome(SyncV2.OUTCOME_FAST_FORWARDED, proposed)
                }
                HeadUpdate.AlreadyAncestor -> RefOutcome(SyncV2.OUTCOME_NOOP, b.headHash)
                HeadUpdate.Diverged -> RefOutcome(SyncV2.OUTCOME_CONFLICT, b.headHash, refConflict(b.headHash, proposed))
            }
        }
        else -> throw PeerSyncException("peer sync: unknown ref kind \"$kind\"")
    }
}

private fun refConflict(
    local: KdbHash,
    proposed: KdbHash,
) = ConflictReport(transactionId = proposed.toHex(), baseHash = local.toHex(), targetHash = proposed.toHex(), conflicts = emptyList())

private suspend fun applyMain(
    ns: PeerSyncNamespace,
    proposed: KdbHash,
): RefOutcome {
    // Live state follows main: the commits main just took on, parents first - applied under the
    // divergence lock, so a concurrent push cannot merge onto the new head before its tree exists.
    val apply = ns.materializeCommit
    val onFastForward: (suspend (KdbHash) -> Unit)? =
        apply?.let { { before -> for (c in missingCommits(ns.dag, listOf(proposed), listOf(before))) apply(c) } }
    return when (val o = resolveDivergence(ns.dag, ns.storage, ns.namespaceId, proposed, ns.conflictPolicy, onFastForward)) {
        CommitPushOutcome.FastForwarded -> RefOutcome(SyncV2.OUTCOME_FAST_FORWARDED, ns.dag.head())
        CommitPushOutcome.NoOp -> RefOutcome(SyncV2.OUTCOME_NOOP, ns.dag.head())
        is CommitPushOutcome.Merged -> RefOutcome(SyncV2.OUTCOME_MERGED, ns.dag.head())
        is CommitPushOutcome.Conflict -> RefOutcome(SyncV2.OUTCOME_CONFLICT, ns.dag.head(), o.report)
    }
}

/**
 * Stores [commits] - a page, parents first - that this node lacks. Storing never moves a ref (the
 * decision is [applyRef]'s, on the last page). Returns how many were new.
 */
internal suspend fun storeCommits(
    dag: CommitDag,
    commits: List<KdbCommit>,
): Int {
    var stored = 0
    for (c in commits) {
        if (dag.hasCommit(c.hash)) continue
        dag.putCommit(c, requireParents = true)
        stored++
    }
    return stored
}

/**
 * Every commit reachable from [wants] and from none of [haves], parents first (ties broken by
 * hash, as Go orders them). Works the set out afresh: Go keeps a cursor across a transfer's
 * pages; here each page walks again, which is the price of holding no state between requests.
 */
internal suspend fun missingCommits(
    dag: CommitDag,
    wants: List<KdbHash>,
    haves: List<KdbHash>,
): List<KdbCommit> {
    val have = HashSet<KdbHash>()
    val stack = ArrayDeque(haves.filter { dag.hasCommit(it) })
    while (stack.isNotEmpty()) {
        val h = stack.removeLast()
        if (!have.add(h)) continue
        val c = dag.getCommit(h) ?: continue
        for (p in c.parentHashes) if (p !in have) stack.addLast(p)
    }
    val set = LinkedHashMap<KdbHash, KdbCommit>()
    val queue = ArrayDeque(wants)
    while (queue.isNotEmpty()) {
        val h = queue.removeFirst()
        if (h in have || h in set) continue
        val c = dag.getCommit(h) ?: continue
        set[h] = c
        queue.addAll(c.parentHashes)
    }
    return topoSort(set)
}

/** Parents first; among commits ready at once, the smallest hash first. */
private fun topoSort(set: Map<KdbHash, KdbCommit>): List<KdbCommit> {
    val pending = HashMap<KdbHash, Int>()
    val children = HashMap<KdbHash, MutableList<KdbHash>>()
    for ((h, c) in set) {
        val inSet = c.parentHashes.filter { it in set }
        pending[h] = inSet.size
        for (p in inSet) children.getOrPut(p) { ArrayList() } += h
    }
    // Kept sorted descending, so the smallest hash is taken from the end.
    val ready = ArrayList<String>()
    fun ready(hex: String) {
        var i = ready.binarySearch(hex, reverseOrder())
        if (i < 0) i = -i - 1
        ready.add(i, hex)
    }
    val byHex = HashMap<String, KdbHash>()
    for ((h, n) in pending) {
        byHex[h.toHex()] = h
        if (n == 0) ready(h.toHex())
    }
    val out = ArrayList<KdbCommit>(set.size)
    while (ready.isNotEmpty()) {
        val h = byHex.getValue(ready.removeAt(ready.size - 1))
        out += set.getValue(h)
        for (child in children[h].orEmpty()) {
            val left = pending.getValue(child) - 1
            pending[child] = left
            if (left == 0) ready(child.toHex())
        }
    }
    return out
}

/** The first commits of [ordered] that fit [maxBytes] of encoded payload (at least one), and
 * whether that is all of them. */
internal fun pageOf(
    ordered: List<KdbCommit>,
    maxBytes: Int,
): Pair<List<KdbCommit>, Boolean> {
    val budget = if (maxBytes <= 0) DEFAULT_PAGE_BYTES else maxBytes
    var size = 0
    var n = 0
    for (c in ordered) {
        val bytes = c.toPayloadBytes().size + 4
        if (n > 0 && size + bytes > budget) break
        size += bytes
        n++
    }
    return ordered.subList(0, n) to (n == ordered.size)
}

/** The page's tips: its commits no other commit of the page names as a parent. A peer naming them
 * as haves holds the whole page. */
internal fun pageTips(commits: List<KdbCommit>): List<KdbHash> {
    val named = HashSet<KdbHash>()
    for (c in commits) named.addAll(c.parentHashes)
    return commits.map { it.hash }.filter { it !in named }
}

/** A ConflictReport as JSON, the shape Go's RefUpdateAck.ConflictBytes and v1 CONFLICT_REPORT carry. */
internal fun encodeConflictReport(report: ConflictReport): ByteArray {
    val items =
        report.conflicts.joinToString(",") { item ->
            val local = item.localDoc?.let(::jsonString) ?: "null"
            val incoming = item.incomingDoc?.let(::jsonString) ?: "null"
            """{"documentId":"${item.documentId}","operationType":"${item.operationType}","localDoc":$local,"incomingDoc":$incoming}"""
        }
    return (
        """{"transactionId":"${report.transactionId}","baseHash":"${report.baseHash}",""" +
            """"targetHash":"${report.targetHash}","conflicts":[$items]}"""
    ).encodeToByteArray()
}

private fun jsonString(s: String): String = kotlinx.serialization.json.JsonPrimitive(s).toString()
