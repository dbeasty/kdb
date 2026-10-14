package dev.kdb.replica

import dev.kdb.document.KdbOp

/**
 * One sync of one namespace over an open v2 connection (packages/kdb-replica/src/sync.ts).
 *
 * Full mode (plan §4.1), in order:
 * 1. Finish a convergence left half-done (by snapshot pages or by tree diff).
 * 2. Push local commits (REF_UPDATE). The host merges them if it moved meanwhile; the replica
 *    never merges. A conflict stops here: the commits stay pending and the app is told.
 * 3. Pull to the host's main:
 *    - an empty namespace bootstraps from a snapshot (SNAPSHOT_FETCH), not from history;
 *    - otherwise SQUASH_HEAD says how far behind the replica is, and the catch-up route decides:
 *      replay the commits (FETCH/PACK_PAGE, then fast-forward) or converge by tree diff
 *      (TREE_NODES + OBJECT_FETCH, the C7 squash), as go/kdb/peersync/squash.go's trySquash does.
 */

/** A single-home namespace refused a full replica's commits (plan D6): hold it filtered with write-back. */
public class SingleHomeException(message: String) : RuntimeException(message)

/** How a full namespace that is behind catches up. */
public enum class CatchUp { REPLAY, SQUASH, AUTO }

/** go/kdb/peersync/squash.go: auto squashes from this backlog, at this many commits per change. */
public const val AUTO_SQUASH_MIN_BACKLOG: Int = 1000
public const val AUTO_SQUASH_BACKLOG_PER_CHANGE: Int = 2

public data class SyncOptions(
    val catchUp: CatchUp = CatchUp.AUTO,
    val pageBytes: Int = DEFAULT_PAGE_BYTES,
    /** Where blobs the replica wrote wait for upload (needed to push commits with attachments). */
    val blobs: BlobStore? = null,
)

public data class NamespaceSyncResult(
    val namespace: String,
    val pushed: Int,
    /** "fast-forwarded", "merged", "created", "noop" or "conflict", when something was pushed. */
    val pushOutcome: String?,
    val conflict: ConflictReport?,
    /** How the namespace caught up, if it moved: "snapshot", "replay" or "squash". */
    val route: String?,
    val changed: List<String>,
    val head: String?,
)

public suspend fun syncNamespace(
    conn: PeerConnection,
    ns: ReplicaNamespace,
    remote: NamespaceRefs?,
    opts: SyncOptions,
): NamespaceSyncResult {
    if (remote == null) throw ProtocolException("kdb replica: the host did not grant ${ns.name}")
    var remoteMain = remote.branches["main"].orEmpty()
    val changed = ArrayList<String>()
    var route: String? = null
    var pushed = 0
    var pushOutcome: String? = null

    val t = ns.target
    if (t != null) {
        route = if (t.via == "snapshot") "snapshot" else "squash"
        changed += resumeConverge(conn, ns, opts, null)
    }

    if (ns.pending.isNotEmpty()) {
        if (remote.access == "pull") {
            throw ProtocolException("kdb replica: ${ns.name} is read-only for this token; its local commits cannot be pushed")
        }
        val ack =
            try {
                push(conn, ns, remoteMain, opts)
            } catch (e: RemoteException) {
                // NOT_HOME, or - once the host knows the handover point - its refusal of a commit
                // made after it by a node that is not the home (server.StaleFenceError, sent as
                // CONFLICT; the wire has no code of its own for it, so it is recognised by its
                // message).
                if (e.code == "NOT_HOME" || (e.code == "CONFLICT" && "home is now at fence" in e.remoteMessage)) {
                    // Plan D6: a single-home namespace takes writes only at its home, so a replica
                    // that wants to write it holds it filtered with write-back, which queues writes
                    // until the home is reachable. Its local commits here can never be pushed.
                    throw SingleHomeException(
                        "kdb replica: ${ns.name} is single-home (${e.remoteMessage}); open it with mode FILTERED and writeBack to write to it",
                    )
                }
                throw e
            }
        pushOutcome = ack.outcome
        if (ack.outcome == "conflict") {
            return NamespaceSyncResult(ns.name, 0, pushOutcome, ack.conflict, route, changed, ns.head)
        }
        pushed = ns.pending.size
        remoteMain = ack.headHex.ifEmpty { ns.head ?: remoteMain }
        ns.markPushed(ns.pending.size, remoteMain)
    }

    if (remote.access != "push" && remoteMain.isNotEmpty() && remoteMain != ns.head) {
        if (ns.isEmpty) {
            route = "snapshot"
            changed += bootstrap(conn, ns, remoteMain, opts)
        } else {
            val (r, c) = catchUp(conn, ns, remoteMain, opts)
            route = r
            changed += c
        }
    }
    ns.setRemoteMain(ns.head ?: remoteMain.ifEmpty { null })
    return NamespaceSyncResult(ns.name, pushed, pushOutcome, null, route, changed, ns.head)
}

private suspend fun push(conn: PeerConnection, ns: ReplicaNamespace, remoteMain: String, opts: SyncOptions): RefUpdateAck {
    val commits = ns.pendingCommits()
    // The host stores a commit only once it holds the blobs it references: upload first (plan §9).
    val uploaded = uploadPinned(conn, ns.name, opts.blobs, commits.flatMap { c -> c.ops.filterIsInstance<KdbOp.AttachmentPut>().map { it.contentHash.toHex() } })
    var page = ArrayList<ReplicaCommit>()
    var size = 0
    for ((i, c) in commits.withIndex()) {
        page.add(c)
        size += c.payload.size
        val last = i == commits.size - 1
        if (!last && size < opts.pageBytes) continue
        val ack = conn.refUpdate(ns.name, remoteMain, ns.head!!, page, !last)
        if (last) {
            if (ack.outcome != "conflict") for (h in uploaded) opts.blobs?.unpin(h)
            return ack
        }
        page = ArrayList()
        size = 0
    }
    error("kdb replica: nothing to push")
}

/** An empty namespace takes the host's state at main from a snapshot, paged by document id. */
private suspend fun bootstrap(conn: PeerConnection, ns: ReplicaNamespace, at: String, opts: SyncOptions): List<String> {
    val first = conn.snapshot(ns.name, at, "", opts.pageBytes)
    ns.beginConverge(first.commit, "snapshot")
    return resumeConverge(conn, ns, opts, first)
}

private suspend fun resumeConverge(
    conn: PeerConnection,
    ns: ReplicaNamespace,
    opts: SyncOptions,
    firstPage: SnapshotPage?,
): List<String> {
    val t = ns.target!!
    val changed = ArrayList<String>()
    if (t.via == "snapshot") {
        var page = firstPage
        var after = t.after
        while (true) {
            val p = page ?: conn.snapshot(ns.name, t.hash, after, opts.pageBytes)
            if (p.commit.hash != t.hash) {
                throw ProtocolException("kdb replica: snapshot page is of ${p.commit.hash}, not ${t.hash}")
            }
            if (!p.done) after = p.next
            ns.applyConvergePage(p.docs.map { it.id to it.json }, after)
            p.docs.mapTo(changed) { it.id }
            if (p.done) {
                // The last page carries the commit's whole attachment table: the replica has no
                // history to derive it from.
                if (p.commit.attachmentRoot != null || ns.attachmentRoot() != null) {
                    ns.applyConvergeAttachments(parsePackedAttachmentTable(p.attachmentTables), whole = true)
                }
                break
            }
            page = null
        }
        try {
            ns.finishConverge()
            return changed
        } catch (_: TreeMismatchException) {
            // A snapshot laid over what an earlier, interrupted attempt left can disagree with the
            // target; a diff against the target's tree repairs exactly that.
        } catch (_: AttachmentRootMismatchException) {
            // Likewise for the attachment table: the diff below repairs it set by set.
        }
    }
    val target = ReplicaCommit.decode(hexToBytes(ns.target!!.payloadHex))
    changed += converge(conn, ns, target.treeHash, null)
    convergeAttachments(conn, ns, target)
    ns.finishConverge()
    return changed
}

private suspend fun catchUp(
    conn: PeerConnection,
    ns: ReplicaNamespace,
    remoteMain: String,
    opts: SyncOptions,
): Pair<String, List<String>> {
    if (opts.catchUp != CatchUp.REPLAY && conn.has("squash") && conn.has("repair")) {
        val head = conn.squashHead(ns.name, ns.head!!)
        val target = head.commit
        // Refused: the host wants every replica to hold full history, so replay. Not descending:
        // the host's main does not build on this replica's head (it rolled back, or squashed
        // away), and only converging can follow it - this replica has no unpushed commits to lose
        // by then.
        if (head.refused.isEmpty() && target != null) {
            var diff: Diff? = null
            var squash = opts.catchUp == CatchUp.SQUASH || !head.descends
            if (!squash && head.backlog >= AUTO_SQUASH_MIN_BACKLOG) {
                diff = diffTrees(conn, ns, target.treeHash)
                squash = head.backlog >= AUTO_SQUASH_BACKLOG_PER_CHANGE.toLong() * diff.changes.size
            }
            if (squash) {
                ns.beginConverge(target, "diff")
                val changed = converge(conn, ns, target.treeHash, diff)
                convergeAttachments(conn, ns, target)
                ns.finishConverge()
                return "squash" to changed
            }
        }
    }
    return "replay" to replay(conn, ns, remoteMain, opts)
}

/** FETCH/PACK_PAGE from the head to [want], fast-forwarding whenever a page closes on one tip. */
private suspend fun replay(conn: PeerConnection, ns: ReplicaNamespace, want: String, opts: SyncOptions): List<String> {
    val changed = LinkedHashSet<String>()
    var held = ArrayList<ReplicaCommit>()
    while (true) {
        val haves = listOf(ns.head!!) + tipsOf(held)
        val (commits, done) = conn.fetch(ns.name, listOf(want), haves, opts.pageBytes)
        held.addAll(commits)
        val tips = tipsOf(held)
        val finished = done || commits.isEmpty()
        if (tips.size == 1) {
            changed += ns.fastForward(held, tips[0])
            held = ArrayList()
        }
        if (finished) break
    }
    if (held.isNotEmpty() || ns.head != want) {
        throw ProtocolException("kdb replica: replay of ${ns.name} ended at ${ns.head}, not at $want")
    }
    return changed.toList()
}

/** Commits no other commit in the set names as a parent. */
private fun tipsOf(commits: List<ReplicaCommit>): List<String> {
    val parents = HashSet<String>()
    for (c in commits) parents.addAll(c.parents)
    return commits.filter { it.hash !in parents }.map { it.hash }
}

// --- tree diff (go/kdb/document/document_tree_node.go DiffTrees) ------------------------------

private class Diff(
    val treeHex: String,
    /** id -> the host's content hash, or "" where the host does not hold it; id order. */
    val changes: List<Pair<String, String>>,
)

private fun fromWire(n: WireTreeNode): TreeNodeView =
    TreeNodeView(n.prefix, n.hash, n.children, if (n.hasEntries) n.entries else null)

/**
 * Diffs the namespace's document tree against the host's tree [treeHex] or, with [attachmentsAt],
 * its attachment table against that commit's; the host's listed attachment sets go into [sets].
 */
private suspend fun diffTrees(
    conn: PeerConnection,
    ns: ReplicaNamespace,
    treeHex: String,
    attachmentsAt: String? = null,
    sets: MutableMap<String, List<WireAttachment>>? = null,
): Diff {
    val changes = ArrayList<Pair<String, String>>()
    var frontier = listOf("")
    val kind = if (attachmentsAt == null) TreeKind.DOCUMENTS else TreeKind.ATTACHMENTS
    while (frontier.isNotEmpty()) {
        val next = ArrayList<String>()
        for (batch in frontier.chunked(MAX_TREE_NODE_PREFIXES)) {
            val nodes = conn.treeNodes(ns.name, treeHex, batch, attachmentsAt = attachmentsAt)
            if (sets != null) for (n in nodes) for ((id, list) in n.attachments) sets[id.lowercase()] = list
            val remote = nodes.map(::fromWire)
            if (remote.size != batch.size) throw ProtocolException("kdb replica: TREE_NODES answered the wrong number of nodes")
            for ((j, prefix) in batch.withIndex()) {
                val l = ns.treeNode(prefix, DEFAULT_TREE_ENTRY_LIMIT, kind)
                val r = remote[j]
                if (l.hash == r.hash) continue
                if ((l.entries != null || l.hash.isEmpty()) && (r.entries != null || r.hash.isEmpty())) {
                    val le = l.entries.orEmpty()
                    val re = r.entries.orEmpty()
                    for ((id, h) in le) if (re[id] != h) changes += id to re[id].orEmpty()
                    for ((id, h) in re) if (id !in le) changes += id to h
                    continue
                }
                if (prefix.length == 32) throw ProtocolException("kdb replica: subtree $prefix differs but lists no entries")
                for (nib in 0 until 16) {
                    if (l.children.getOrElse(nib) { "" } != r.children.getOrElse(nib) { "" }) next += prefix + nibbleChar(nib)
                }
            }
        }
        frontier = next
    }
    changes.sortBy { it.first }
    return Diff(treeHex, changes)
}

/**
 * Makes the namespace's documents equal the host's tree [treeHex]: diff, then fetch the changed
 * bodies (each checked against its content hash) and apply them a chunk at a time.
 */
private suspend fun converge(conn: PeerConnection, ns: ReplicaNamespace, treeHex: String, known: Diff?): List<String> {
    val diff = known ?: diffTrees(conn, ns, treeHex)
    for (chunk in diff.changes.chunked(MAX_OBJECT_FETCH_ITEMS)) {
        val wanted = chunk.filter { it.second.isNotEmpty() }
        val bodies = HashMap<String, String>()
        if (wanted.isNotEmpty()) {
            val (docs, missing) = conn.objectFetch(ns.name, wanted, treeHex)
            for (d in docs) bodies[d.id.lowercase()] = d.json
            if (missing.isNotEmpty()) {
                throw ProtocolException("kdb replica: the host could not supply ${missing.size} changed documents of ${ns.name}")
            }
        }
        val page = ArrayList<Pair<String, String?>>()
        for ((id, h) in chunk) {
            if (h.isEmpty()) {
                page += id to null
                continue
            }
            val body = bodies[id]
            if (body == null || contentHash(id, body) != h) {
                throw ProtocolException("kdb replica: the host's body for $id does not match its content hash")
            }
            page += id to body
        }
        ns.applyConvergePage(page, chunk.last().first)
    }
    return diff.changes.map { it.first }
}

/**
 * Makes the namespace's attachment table equal the target commit's: a diff of the attachment tries
 * (TREE_NODES with tree "attachments"), taking each differing document's whole set from the host's
 * listing and checking it against the set hash the host's trie names.
 */
private suspend fun convergeAttachments(conn: PeerConnection, ns: ReplicaNamespace, target: ReplicaCommit) {
    if (ns.attachmentRoot() == target.attachmentRoot) return
    if (!conn.has("attachments")) throw ProtocolException("kdb replica: ${ns.name} has attachments, and the host does not serve them")
    val listed = HashMap<String, List<WireAttachment>>()
    val diff = diffTrees(conn, ns, "", target.hash, listed)
    val sets = LinkedHashMap<String, List<ReplicaAttachment>>()
    for ((id, h) in diff.changes) {
        if (h.isEmpty()) {
            sets[id] = emptyList()
            continue
        }
        val set = listed[id.lowercase()].toReplicaSet()
        if (set.isEmpty() || attachmentSetHash(set) != h) {
            throw ProtocolException("kdb replica: the host's attachment set for $id does not match its hash")
        }
        sets[id] = set
    }
    ns.applyConvergeAttachments(sets, whole = false)
}

/** Uploads the blobs among [hashes] that were written here and are still pinned; returns them. */
private suspend fun uploadPinned(conn: PeerConnection, namespace: String, blobs: BlobStore?, hashes: List<String>): List<String> {
    if (blobs == null) return emptyList()
    val uploaded = ArrayList<String>()
    for (h in hashes.distinct()) {
        if (!blobs.pinned(h)) continue
        if (!conn.has("attachments")) throw ProtocolException("kdb replica: the host does not take attachments for $namespace")
        blobs.upload(conn, namespace, h)
        uploaded += h
    }
    return uploaded
}

// --- filtered namespaces (plan §4.2) ----------------------------------------------------------

public data class RejectedWrite(
    val txId: String,
    val outcome: String,
    val reason: String,
    val docs: List<WriteBackDoc>,
    val current: List<CurrentDoc>,
)

public data class ProjectionSyncResult(
    val namespace: String,
    val filter: String,
    /** Outbox entries the source applied this round. */
    val applied: Int,
    /** Outbox entries the source did not apply: their documents went back to the source's state. */
    val rejected: List<RejectedWrite>,
    /** Why write-back stopped early (a transient refusal such as NOT_HOME); the rest stays queued. */
    val deferred: String?,
    val reset: Boolean,
    val changed: List<String>,
    val lastSource: String,
)

public suspend fun syncProjection(conn: PeerConnection, p: ProjectionNamespace, opts: SyncOptions): ProjectionSyncResult {
    var applied = 0
    val rejected = ArrayList<RejectedWrite>()
    var deferred: String? = null
    val changed = ArrayList<String>()
    if (p.writeBack) {
        if (!conn.has("writeback")) throw ProtocolException("kdb replica: the host does not take write-back for ${p.name}")
        for (entry in p.outbox()) {
            // The source checks a write's blobs are there: the ones written here go up first.
            val uploaded =
                try {
                    uploadPinned(conn, p.name, opts.blobs, entry.docs.flatMap { d -> d.attachments.orEmpty().map { it.hash.lowercase() } })
                } catch (e: RemoteException) {
                    deferred = "${e.code}: ${e.remoteMessage}"
                    break
                }
            val r =
                try {
                    conn.projectWrite(p.name, entry.txId, entry.docs)
                } catch (e: RemoteException) {
                    // Transient at the source (not its home, busy): keep the queue, in order, for next time.
                    deferred = "${e.code}: ${e.remoteMessage}"
                    break
                }
            for (h in uploaded) opts.blobs?.unpin(h) // the source holds them now
            if (r.outcome == "applied") {
                p.resolve(entry, null)
                applied++
            } else {
                p.resolve(entry, r.current)
                rejected += RejectedWrite(entry.txId, r.outcome, r.reason, entry.docs, r.current)
                r.current.mapTo(changed) { it.docId }
            }
        }
    }
    val t = p.beginTransfer()
    var at = t.at
    var after = t.after
    var reset = false
    while (true) {
        val page = conn.projectFetch(p.name, p.filter, t.from, at, after, opts.pageBytes)
        if (at.isEmpty()) at = page.atHex
        if (page.atHex != at) throw ProtocolException("kdb replica: projection page is at ${page.atHex}, not $at")
        reset = reset || page.reset
        changed += p.applyPage(t.from, after, page)
        if (page.done) break
        after = page.next
    }
    return ProjectionSyncResult(p.name, p.filter, applied, rejected, deferred, reset, changed, p.lastSource)
}
