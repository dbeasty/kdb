package dev.kdb.replica

import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

public class TreeMismatchException(
    public val commit: String,
    public val declared: String,
    public val built: String,
) : RuntimeException("kdb replica: commit $commit declares tree $declared, applying its range built $built")

public class NotFastForwardException(message: String) : RuntimeException(message)

public class ReplicaBusyException(message: String) : RuntimeException(message)

/** A commit the replica cannot apply faithfully, refused rather than misread. */
public class UnsupportedCommitException(message: String) : RuntimeException(message)

/** A document as the replica holds it. */
public data class ReplicaDocument(val id: String, val json: String)

/**
 * A head this namespace is converging to by snapshot or tree diff, set while documents are being
 * installed in pages. While it is set, the documents are a mix of the old head's and the target's,
 * so the namespace takes no local writes; a sync resumes the convergence, and the target becomes
 * the head only once the built tree is the target's.
 */
@Serializable
public data class ConvergeTarget(
    val hash: String,
    val payloadHex: String,
    /** "snapshot" or "diff". */
    val via: String,
    val after: String,
)

@Serializable
public data class NamespaceMeta(
    val name: String,
    /** The replica's own node id: the author of its commits. */
    val nodeId: String,
    val head: String?,
    /** Local commits not yet confirmed by the host, oldest first. */
    val pending: List<String> = emptyList(),
    /** The highest commit timestamp seen or made, so local timestamps never go backwards. */
    val lastMicros: Long = 0,
    val target: ConvergeTarget? = null,
    /** The host's main as of the last sync: the oldHex a push proposes to move. */
    val remoteMain: String? = null,
)

/** One position of the trie, as TREE_NODES reports it (go/kdb/document/document_tree_node.go). */
public data class TreeNodeView(
    val prefix: String,
    /** "" when the subtree is empty. */
    val hash: String,
    /** 16 child hashes, "" for an empty child; empty when the subtree is empty. */
    val children: List<String>,
    /** id -> content hash, when the subtree holds no more than the entry limit. */
    val entries: Map<String, String>?,
)

internal val metaJson = Json {
    ignoreUnknownKeys = true
    explicitNulls = false
}

internal fun namespaceKey(name: String): String = encodeKeyComponent(name)

/** Topological order of commits by their parents within the set; ties broken by hash. */
public fun topoSort(commits: List<ReplicaCommit>): List<ReplicaCommit> {
    val byHash = commits.associateBy { it.hash }
    val indegree = HashMap<String, Int>()
    val children = HashMap<String, MutableList<String>>()
    for (c in byHash.values) {
        var n = 0
        for (p in c.parents) {
            if (p !in byHash) continue
            n++
            children.getOrPut(p) { ArrayList() }.add(c.hash)
        }
        indegree[c.hash] = n
    }
    val ready = byHash.values.filter { indegree[it.hash] == 0 }.map { it.hash }.sorted().toMutableList()
    val out = ArrayList<ReplicaCommit>(commits.size)
    while (ready.isNotEmpty()) {
        val h = ready.removeAt(0)
        out.add(byHash.getValue(h))
        for (child in children[h].orEmpty()) {
            val n = indegree.getValue(child) - 1
            indegree[child] = n
            if (n == 0) {
                // keep `ready` sorted so the order is deterministic
                val at = ready.binarySearch(child).let { if (it < 0) -it - 1 else it }
                ready.add(at, child)
            }
        }
    }
    if (out.size != byHash.size) throw NotFastForwardException("kdb replica: commit range has a cycle")
    return out
}

/** The last op per document over commits in order: what a fast-forward applies. */
public fun netEffect(ordered: List<ReplicaCommit>): LinkedHashMap<String, KdbOp> {
    val final = LinkedHashMap<String, KdbOp>()
    for (c in ordered) {
        for (op in c.ops) {
            when (op) {
                is KdbOp.Write -> final[op.docId.toString()] = op
                is KdbOp.Delete -> final[op.docId.toString()] = op
                else -> {}
            }
        }
    }
    return final
}

/**
 * The replica does not hold attachments yet (plan §9): a commit carrying them would leave its
 * attachment table unverified and its blobs unknown, so it is refused rather than half-applied.
 */
private fun requireSupported(c: ReplicaCommit) {
    if (c.commit.attachmentTreeHash != null ||
        c.ops.any { it is KdbOp.AttachmentPut || it is KdbOp.AttachmentDelete }
    ) {
        throw UnsupportedCommitException("kdb replica: commit ${c.hash} carries attachments, which this replica does not hold yet")
    }
}

/**
 * One namespace held by the replica: its documents, its trie, its head commit and the local
 * commits not yet pushed. It holds no other history.
 *
 * The replica never merges. It applies only fast-forwards: given the commits between its head and
 * a target, it topo-sorts them, keeps each document's last op, applies those, and requires the
 * tree it built to equal the target's declared tree hash. That is go/kdb/peersync/ingest.go's
 * fastForward → netEffect → applyAndVerify. A merge commit carries every document its parents
 * disagree on, so the last op in any topological order is the merged state. Everything a step
 * changes (documents, trie, head) is one store transaction, written only after the tree hash
 * checks out.
 */
public class ReplicaNamespace private constructor(
    private val store: ReplicaStore,
    @kotlin.concurrent.Volatile private var meta: NamespaceMeta,
) {
    public val name: String = meta.name
    internal val key: String = namespaceKey(meta.name)
    private val trie = TrieReader(store, key)
    private val lock = Mutex()
    private val genesisHash = genesisCommit(name).hash

    public companion object {
        public fun metaKey(name: String): String = "m/ns/${namespaceKey(name)}"

        /** Opens namespace [name] in [store], creating its (empty) metadata on first use. */
        public suspend fun open(store: ReplicaStore, name: String, nodeId: String): ReplicaNamespace {
            val raw = store.get(metaKey(name))
            if (raw != null) return ReplicaNamespace(store, metaJson.decodeFromString(raw.decodeToString()))
            val genesis = genesisCommit(name)
            val meta = NamespaceMeta(name = name, nodeId = nodeId, head = genesis.hash)
            store.apply(
                Batch().apply {
                    put(metaKey(name), metaJson.encodeToString(NamespaceMeta.serializer(), meta).encodeToByteArray())
                    put("c/${namespaceKey(name)}/${genesis.hash}", genesis.payload)
                },
            )
            return ReplicaNamespace(store, meta)
        }
    }

    public val head: String? get() = meta.head

    /** Whether the namespace is still at its genesis, holding nothing: what a snapshot bootstraps. */
    public val isEmpty: Boolean get() = meta.head == genesisHash && meta.pending.isEmpty() && meta.target == null

    public val pending: List<String> get() = meta.pending
    public val target: ConvergeTarget? get() = meta.target
    public val remoteMain: String? get() = meta.remoteMain
    public val nodeId: String get() = meta.nodeId

    private fun docKey(id: String) = "d/$key/${id.lowercase()}"

    private fun commitKey(hash: String) = "c/$key/$hash"

    private fun writeMeta(batch: Batch, m: NamespaceMeta) {
        batch[metaKey(name)] = metaJson.encodeToString(NamespaceMeta.serializer(), m).encodeToByteArray()
    }

    private suspend fun commitMeta(batch: Batch, m: NamespaceMeta) {
        writeMeta(batch, m)
        store.apply(batch)
        meta = m
    }

    public suspend fun rootHash(): String = trie.rootHash()

    public suspend fun commit(hash: String): ReplicaCommit? = store.get(commitKey(hash))?.let { ReplicaCommit.decode(it) }

    public suspend fun headCommit(): ReplicaCommit? = meta.head?.let { commit(it) }

    /** The document's JSON, or null. */
    public suspend fun get(id: String): String? = store.get(docKey(id))?.let { it.decodeToString(32, it.size) }

    /** The document's content hash, or null. */
    public suspend fun contentHashOf(id: String): String? = store.get(docKey(id))?.copyOfRange(0, 32)?.toHex()

    /** Documents in id order, after [after] (exclusive) and before [before], at most [limit]. */
    public suspend fun scan(after: String? = null, before: String? = null, limit: Int = Int.MAX_VALUE): List<ReplicaDocument> {
        val base = "d/$key/"
        val start = if (after != null) base + after.lowercase() + "\u0000" else base
        val end = if (before != null) base + before.lowercase() else prefixEnd(base)
        return store.scan(start, end, limit).map { (k, v) -> ReplicaDocument(k.substring(base.length), v.decodeToString(32, v.size)) }
    }

    /** Stages ops into [batch] and a trie edit; returns the edit (not yet finished). */
    private suspend fun stage(ops: Iterable<KdbOp>, batch: Batch): TrieEdit {
        val edit = TrieEdit(store, key)
        for (op in ops) {
            when (op) {
                is KdbOp.Write -> {
                    val id = op.docId.toString()
                    val ch = contentHash(id, op.patch)
                    edit.set(id, ch)
                    batch[docKey(id)] = hexToBytes(ch) + op.patch.encodeToByteArray()
                }
                is KdbOp.Delete -> {
                    val id = op.docId.toString()
                    edit.delete(id)
                    batch[docKey(id)] = null
                }
                else -> {}
            }
        }
        return edit
    }

    /**
     * Fast-forwards to [target], given the commits between the current head and target (any
     * order). The range must reach the current head; the tree built must be target's. Returns the
     * ids of the documents that changed.
     */
    public suspend fun fastForward(commits: List<ReplicaCommit>, target: String): List<String> =
        lock.withLock {
            if (meta.pending.isNotEmpty()) {
                throw NotFastForwardException("kdb replica: local commits are not yet pushed; push before pulling")
            }
            if (meta.target != null) throw NotFastForwardException("kdb replica: finish converging before fast-forwarding")
            if (target == meta.head) return@withLock emptyList()
            val byHash = commits.associateBy { it.hash }
            val tip = byHash[target] ?: throw NotFastForwardException("kdb replica: the range does not contain its target $target")
            checkReachesHead(byHash, target)
            commits.forEach(::requireSupported)
            val ordered = topoSort(commits)
            val effect = netEffect(ordered)
            val batch = Batch()
            val edit = stage(effect.values, batch)
            val built = edit.finish()
            if (built != tip.treeHash) throw TreeMismatchException(target, tip.treeHash, built)
            edit.writeTo(batch)
            val next = replaceHead(batch, tip, maxMicros(meta.lastMicros, ordered))
            commitMeta(batch, next)
            effect.keys.toList()
        }

    private fun checkReachesHead(byHash: Map<String, ReplicaCommit>, target: String) {
        val head = meta.head
        val seen = HashSet<String>()
        val queue = ArrayDeque(listOf(target))
        var reached = false
        var roots = false
        while (queue.isNotEmpty()) {
            val h = queue.removeLast()
            if (!seen.add(h)) continue
            val c = byHash[h]
            if (c == null) {
                if (h == head) reached = true
                continue
            }
            if (c.parents.isEmpty()) roots = true
            queue.addAll(c.parents)
        }
        if (head == null) {
            if (!roots || seen.any { it !in byHash }) {
                throw NotFastForwardException("kdb replica: a namespace with no head needs the whole history or a snapshot")
            }
        } else if (!reached) {
            throw NotFastForwardException("kdb replica: $target does not descend from the head $head")
        }
    }

    private fun replaceHead(batch: Batch, tip: ReplicaCommit, lastMicros: Long): NamespaceMeta {
        val old = meta.head
        if (old != null && old != tip.hash) batch[commitKey(old)] = null
        batch[commitKey(tip.hash)] = tip.payload
        return meta.copy(head = tip.hash, lastMicros = lastMicros)
    }

    /**
     * Commits [ops] locally on top of the head and queues the commit for push. Its timestamp is
     * the wall clock, but never at or below one the replica has already seen or made, so a device
     * with a slow clock still orders after the host's history (the host refuses skewed commits).
     */
    public suspend fun commitLocal(
        ops: List<KdbOp>,
        transactionId: KdbUuid,
        message: String = "",
        nowMicros: Long = nowMicros(),
    ): ReplicaCommit =
        lock.withLock {
            if (meta.target != null) {
                throw ReplicaBusyException("kdb replica: $name is being brought up to date; retry once it has synced")
            }
            val batch = Batch()
            val edit = stage(ops, batch)
            val tree = edit.finish()
            edit.writeTo(batch)
            val ts = if (nowMicros > meta.lastMicros) nowMicros else meta.lastMicros + 1
            val c =
                ReplicaCommit.build(
                    parents = listOfNotNull(meta.head),
                    namespace = name,
                    transactionId = transactionId,
                    timestampMicros = ts,
                    authorNodeId = parseUuid(meta.nodeId),
                    ops = ops,
                    treeHash = tree,
                    message = message,
                )
            batch[commitKey(c.hash)] = c.payload
            // The head's own payload stays: a pending commit's parent may be needed for a push.
            commitMeta(batch, meta.copy(head = c.hash, pending = meta.pending + c.hash, lastMicros = ts))
            c
        }

    /** Records the host's main as of this sync. */
    public suspend fun setRemoteMain(hash: String?) {
        lock.withLock { commitMeta(Batch(), meta.copy(remoteMain = hash)) }
    }

    /**
     * Starts converging to [target] by [via] ("snapshot" or "diff"); a no-op if already converging
     * to it. Refused while local commits are unpushed: a replica never drops them.
     */
    public suspend fun beginConverge(target: ReplicaCommit, via: String) {
        lock.withLock {
            val t = meta.target
            if (t != null && t.hash == target.hash && t.via == via) return@withLock
            if (meta.pending.isNotEmpty()) {
                throw NotFastForwardException("kdb replica: local commits are not yet pushed; push before converging")
            }
            commitMeta(Batch(), meta.copy(target = ConvergeTarget(target.hash, target.payload.toHex(), via, "")))
        }
    }

    /**
     * Applies one page of a convergence: final states for some documents (null deletes), and the
     * cursor to resume after. Unverified until [finishConverge], but durable, so an interrupted
     * convergence resumes rather than restarts.
     */
    public suspend fun applyConvergePage(docs: List<Pair<String, String?>>, after: String) {
        lock.withLock {
            val t = meta.target ?: error("kdb replica: not converging")
            val ops =
                docs.map { (id, json) ->
                    if (json == null) KdbOp.Delete(parseUuid(id)) else KdbOp.Write(parseUuid(id), json)
                }
            val batch = Batch()
            val edit = stage(ops, batch)
            edit.finish()
            edit.writeTo(batch)
            commitMeta(batch, meta.copy(target = t.copy(after = after)))
        }
    }

    /**
     * Completes a convergence: the built tree must be the target's. Then the target is the head, a
     * root this replica holds without its history. Throws [TreeMismatchException] otherwise, and
     * stays converging - a diff against the target's tree then repairs whatever differs.
     */
    public suspend fun finishConverge() {
        lock.withLock {
            val t = meta.target ?: return@withLock
            val tip = ReplicaCommit.decode(hexToBytes(t.payloadHex))
            val built = trie.rootHash()
            if (built != tip.treeHash) throw TreeMismatchException(tip.hash, tip.treeHash, built)
            val batch = Batch()
            val next = replaceHead(batch, tip, maxMicros(meta.lastMicros, listOf(tip)))
            commitMeta(batch, next.copy(target = null))
        }
    }

    /** Marks the oldest [n] pending commits as held by the host. */
    public suspend fun markPushed(n: Int, remoteMain: String) {
        lock.withLock {
            val batch = Batch()
            // Pushed commits other than the head are history the replica does not keep.
            for (h in meta.pending.take(n)) if (h != meta.head) batch[commitKey(h)] = null
            commitMeta(batch, meta.copy(pending = meta.pending.drop(n), remoteMain = remoteMain))
        }
    }

    /** The unpushed local commits, oldest first. */
    public suspend fun pendingCommits(): List<ReplicaCommit> =
        meta.pending.map { h -> commit(h) ?: error("kdb replica: pending commit $h is missing from the store") }

    /**
     * The subtree at [prefix], as Go's DocumentTree.Node reports it, so a diff against a host's
     * TREE_NODES compares like with like - including for a subtree stored folded into one leaf.
     */
    public suspend fun treeNode(prefix: String, entryLimit: Int): TreeNodeView {
        val empty = TreeNodeView(prefix, "", emptyList(), null)
        var at = ""
        var rec = trie.record(at)
        while (rec is TrieRecord.Internal && at.length < prefix.length) {
            at = prefix.substring(0, at.length + 1)
            rec = trie.record(at)
        }
        if (rec == null) return empty
        val depth = prefix.length
        if (rec is TrieRecord.Leaf) {
            val idHex = rec.id.toHex()
            if (!idHex.startsWith(prefix)) return empty
            val children = MutableList(16) { "" }
            if (depth < 32) children[idHex[depth].digitToInt(16)] = foldLeaf(rec.id, rec.content, depth + 1).toHex()
            return TreeNodeView(
                prefix,
                foldLeaf(rec.id, rec.content, depth).toHex(),
                children,
                if (entryLimit >= 1) mapOf(uuidString(rec.id) to rec.content.toHex()) else null,
            )
        }
        rec as TrieRecord.Internal
        val children = (0 until 16).map { childHashHex(rec.children, it) }
        var entries: Map<String, String>? = null
        if (entryLimit > 0) {
            val found = LinkedHashMap<String, String>()
            val complete =
                trie.walk(prefix) { id, ch ->
                    found[id] = ch
                    found.size <= entryLimit
                }
            entries = if (complete) found else null
        }
        return TreeNodeView(prefix, rec.hash!!.toHex(), children, entries)
    }

    /** Every document as (id, contentHash), in id order, from the trie. */
    public suspend fun walkTree(visit: suspend (id: String, contentHash: String) -> Unit) {
        trie.walk { id, ch ->
            visit(id, ch)
            true
        }
    }
}

private fun maxMicros(last: Long, commits: List<ReplicaCommit>): Long {
    var m = last
    for (c in commits) if (c.timestampMicros > m) m = c.timestampMicros
    return m
}
