package dev.kdb.peersync

import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.ConnectionAuthSupport
import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.KdbAuthorizationException
import dev.kdb.auth.Principal
import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbCommit
import dev.kdb.wire.WireCodec
import dev.kdb.wire.WireMessage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel

/**
 * One v2 peer-sync connection, host side (Go's V2Host): SYNC_HELLO, REFS_REQUEST, FETCH_REQUEST,
 * REF_UPDATE, and the attachment frames BLOB_FETCH and ATTACHMENT_UPLOAD. A Kotlin host holds one
 * namespace, and grants it when the hello's patterns select it.
 *
 * Every request gets a reply; a failure is a PEER_ERROR, never a dropped connection. Only a frame
 * that cannot be decoded at all throws.
 */
internal class SyncV2HostSession(
    private val wire: WireCodec,
    private val ns: PeerSyncNamespace,
    private val nodeId: String,
    private val auth: AuthEngine,
    private val connectionContext: ConnectionContext,
) {
    private var helloDone = false
    private var granted = false
    private var principal: Principal? = null
    private var zstdPack = false
    private var peerAttachments = false
    private var connectionAuth = ConnectionAuthSupport(auth, connectionContext)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val blobs = ns.blobs?.let { BlobHostSession(ns.dag, it, scope) }

    /** Serves one frame - a v2 sync frame or an attachment frame - returning the reply. */
    suspend fun handleFrame(frame: ByteArray): ByteArray {
        val code = frameTypeCode(frame)
        val cid = frameCorrelationId(frame)
        return try {
            if (SyncV2.isSyncFrame(code)) {
                serveSync(SyncV2Codec.decode(frame))
            } else {
                serveWire(wire.decode(frame))
            }
        } catch (e: SyncV2DecodeException) {
            throw e
        } catch (e: kotlinx.coroutines.CancellationException) {
            throw e
        } catch (e: Throwable) {
            SyncV2Codec.encode(SyncV2Message.PeerError(cid, ns.namespaceId, classify(e), e.message ?: e.toString()))
        }
    }

    private fun classify(e: Throwable): String =
        when (e) {
            is AttachmentsUnsupportedException -> PeerErrorCodes.UNSUPPORTED
            is UnsupportedFrameException -> PeerErrorCodes.UNSUPPORTED
            is NotGrantedException -> PeerErrorCodes.UNAUTHORIZED
            is KdbAuthorizationException -> PeerErrorCodes.UNAUTHORIZED
            is RemotePeerException -> e.errorCode
            else -> PeerErrorCodes.INTERNAL
        }

    private suspend fun serveSync(msg: SyncV2Message): ByteArray {
        if (msg is SyncV2Message.Hello) return SyncV2Codec.encode(hello(msg))
        if (!helloDone) throw PeerSyncException("peer sync: SYNC_HELLO must come first")
        val reply: SyncV2Message =
            when (msg) {
                is SyncV2Message.RefsRequest -> SyncV2Message.RefsResult(msg.correlationId, msg.namespaces.map { refs(it) })
                is SyncV2Message.FetchRequest -> fetch(msg)
                is SyncV2Message.RefUpdate -> refUpdate(msg)
                is SyncV2Message.SnapshotFetch -> snapshotPage(msg)
                else -> throw UnsupportedFrameException(frameName(msg))
            }
        return SyncV2Codec.encode(reply)
    }

    private suspend fun serveWire(msg: WireMessage): ByteArray {
        if (!helloDone) throw PeerSyncException("peer sync: SYNC_HELLO must come first")
        val reply: WireMessage =
            when (msg) {
                is WireMessage.BlobFetch -> {
                    authorize(msg.namespace)
                    (blobs ?: throw UnsupportedFrameException("BLOB_FETCH")).blobFetch(msg)
                }
                is WireMessage.AttachmentUpload -> {
                    authorize(msg.namespace)
                    (blobs ?: throw UnsupportedFrameException("ATTACHMENT_UPLOAD")).upload(msg)
                }
                else -> throw UnsupportedFrameException(msg.header.messageType.name)
            }
        return wire.encode(reply)
    }

    private suspend fun hello(m: SyncV2Message.Hello): SyncV2Message.HelloAck {
        fun reject(reason: String) =
            SyncV2Message.HelloAck(m.correlationId, accepted = false, nodeId = nodeId, protocol = SyncV2.PROTOCOL, reason = reason)
        if (nodeId.isNotEmpty() && m.nodeId == nodeId) {
            return reject(
                "peer sync: the connecting node has this node's own identity $nodeId; a copied data root must take a new node id",
            )
        }
        // Credentials in the hello win; without any, the transport's (an HTTP upgrade's headers).
        if (m.user != null || m.password != null || m.token != null) {
            connectionAuth = ConnectionAuthSupport(auth, connectionContext.copy(user = m.user, password = m.password, token = m.token))
        }
        val p =
            try {
                connectionAuth.authenticateConnection()
            } catch (e: Throwable) {
                return reject(connectionAuth.authFailureMessage(e))
            }
        principal = p
        // A pattern is a request: a namespace it selects that this principal may sync is granted,
        // one it may not is left out rather than refusing the whole hello.
        val selected = selectNamespaces(m.namespaces, listOf(ns.namespaceId)).isNotEmpty()
        granted = selected && runCatching { connectionAuth.authorize(p, AuthAction.PeerSync(ns.namespaceId)) }.isSuccess
        helloDone = true
        val caps = capabilities().filter { it in m.capabilities }
        zstdPack = SyncV2.CAP_ZSTD_PACK in caps
        peerAttachments = SyncV2.CAP_ATTACHMENTS in caps
        return SyncV2Message.HelloAck(
            m.correlationId, accepted = true, nodeId = nodeId, protocol = SyncV2.PROTOCOL, capabilities = caps,
            refs = if (granted) listOf(localRefs()) else emptyList(),
        )
    }

    private fun capabilities(): List<String> =
        SyncV2.CAPABILITIES.filter {
            (it != SyncV2.CAP_ATTACHMENTS || ns.blobs != null) && (it != SyncV2.CAP_SNAPSHOT || ns.snapshots != null)
        }

    private suspend fun localRefs(): NamespaceRefs = refsOf(ns.namespaceId, ns.dag, ns.snapshots?.shallowRoots().orEmpty())

    /** Re-authorizes on every frame, so a grant revoked mid-session takes effect at once. */
    private suspend fun authorize(namespace: String) {
        if (!granted || namespace != ns.namespaceId) throw NotGrantedException(namespace)
        connectionAuth.authorize(principal, AuthAction.PeerSync(ns.namespaceId))
    }

    private suspend fun refs(namespace: String): NamespaceRefs {
        authorize(namespace)
        return localRefs()
    }

    /**
     * The session's fetch in progress (Go's missingCursor): what the peer still lacks, in the order
     * pages serve it, and the last page served. Working the missing set out walks every commit the
     * peer's haves reach, so doing it for every page made a long transfer quadratic. A request that
     * continues the transfer - the same wants, naming the last page's tips as held - takes the next
     * page from here; anything else starts over. A page refused and asked for again is the same page.
     */
    private class FetchCursor(
        val wants: List<KdbHash>,
        val order: List<KdbHash>,
    ) {
        var pos = 0 // order[pos..] is what the peer does not hold yet
        var pageLen = 0
        var pageTips: List<KdbHash> = emptyList()
        var lastHaves: Set<KdbHash> = emptySet() // what the request for the last page named

        /** The same request again (a page refused or lost): the same page. */
        fun repeats(m: SyncV2Message.FetchRequest): Boolean = m.wants == wants && m.haves.toSet() == lastHaves

        fun continues(m: SyncV2Message.FetchRequest): Boolean =
            pageLen > 0 && m.wants == wants && m.haves.toSet().containsAll(pageTips)
    }

    private var fetchCursor: FetchCursor? = null

    /** How many times the session worked a missing set out (tests: once per transfer). */
    internal var missingWalks = 0
        private set

    /** The next page from [order] at [from]: commits loaded only as far as the page reaches (at least one),
     * and whether that is all of them; null when one of them is gone from the DAG. */
    private suspend fun pageFromOrder(
        order: List<KdbHash>,
        from: Int,
        maxBytes: Int,
    ): Pair<List<KdbCommit>, Boolean>? {
        val budget = if (maxBytes <= 0) DEFAULT_PAGE_BYTES else maxBytes
        val page = ArrayList<KdbCommit>()
        var size = 0
        for (i in from until order.size) {
            val c = ns.dag.getCommit(order[i]) ?: return null
            val bytes = c.toPayloadBytes().size + 4
            if (page.isNotEmpty() && size + bytes > budget) break
            size += bytes
            page += c
        }
        return page to (from + page.size == order.size)
    }

    private suspend fun fetch(m: SyncV2Message.FetchRequest): SyncV2Message.PackPage {
        authorize(m.namespace)
        var cur = fetchCursor?.takeIf { it.continues(m) || it.repeats(m) }
        if (cur != null && !cur.repeats(m)) cur.pos += cur.pageLen
        // A commit the cursor ordered can be gone (squashed since): then work the set out afresh.
        var paged = cur?.let { pageFromOrder(it.order, it.pos, m.maxBytes) }
        if (cur == null || paged == null) {
            cur = FetchCursor(m.wants, missingCommits(ns.dag, m.wants, m.haves).map { it.hash })
            missingWalks++
            paged = pageFromOrder(cur.order, 0, m.maxBytes) ?: throw PeerSyncException("peer sync: the missing commits changed while paging")
        }
        val (page, done) = paged
        cur.pageLen = page.size
        cur.pageTips = pageTips(page)
        cur.lastHaves = m.haves.toSet()
        fetchCursor = if (done) null else cur
        if (!peerAttachments) {
            firstCommitWithAttachments(page)?.let { throw AttachmentsUnsupportedException(m.namespace, it.hash.toHex()) }
        }
        return SyncV2Message.PackPage(m.correlationId, m.namespace, page, done = done, zstd = zstdPack)
    }

    private suspend fun refUpdate(m: SyncV2Message.RefUpdate): SyncV2Message.RefUpdateAck {
        authorize(m.namespace)
        if (m.stubs.isNotEmpty()) throw UnsupportedFrameException("REF_UPDATE with archived-commit stubs")
        val stored =
            try {
                storeCommits(ns.dag, m.commits)
            } catch (e: Exception) {
                throw PeerSyncException("peer sync: storing a pushed page: ${e.message}", e)
            }
        val ack = SyncV2Message.RefUpdateAck(m.correlationId, m.namespace, m.kind, m.ref, SyncV2.OUTCOME_STORED, stored = stored)
        if (m.more) return ack
        val res = applyRef(ns, m.kind, m.ref, KdbHash.fromHex(m.newHex))
        return ack.copy(outcome = res.outcome, headHex = res.head.toHex(), conflictBytes = res.conflict?.let(::encodeConflictReport))
    }

    /**
     * One page of this node's state at main's head, documents in id order after [m]'s cursor (Go's
     * snapshotPage). Only the head: the documents are read from live storage, and a peer asks for
     * the head it was just told of. The last page carries the head's attachment table.
     */
    private suspend fun snapshotPage(m: SyncV2Message.SnapshotFetch): SyncV2Message.SnapshotPage {
        authorize(m.namespace)
        val snapshots = ns.snapshots ?: throw UnsupportedFrameException("SNAPSHOT_FETCH")
        val head = ns.dag.head()
        if (m.atHex.isNotEmpty() && m.atHex != head.toHex()) {
            throw RemotePeerException(PeerErrorCodes.NOT_FOUND, "this node serves a snapshot of its head ${head.toHex()} only, not ${m.atHex}")
        }
        val commit = ns.dag.getCommitOrThrow(head)
        if (!peerAttachments && commit.attachmentTreeHash != null) throw AttachmentsUnsupportedException(m.namespace, commit.hash.toHex())
        val tree = snapshots.headTree(commit)
        val budget = if (m.maxBytes <= 0) DEFAULT_PAGE_BYTES else m.maxBytes
        val (docs, done) = snapshotPageDocs(tree, m.after, budget, head.toHex()) { snapshots.documentsAtHead(it) }
        val tables = if (done && commit.attachmentTreeHash != null) ns.dag.packAttachmentTables(listOf(head)) else null
        return SyncV2Message.SnapshotPage(
            m.correlationId, m.namespace, commit, docs, if (done) "" else docs.last().docId, done, tree.size, tables,
        )
    }

    /** Aborts unfinished uploads. The connection's owner calls it when the connection ends. */
    fun close() {
        blobs?.close()
        scope.cancel()
    }

    private fun frameName(m: SyncV2Message): String = m::class.simpleName ?: "frame"
}

/**
 * The documents of one SNAPSHOT_FETCH page after cursor [after], and whether they finish the
 * snapshot: ids in order, bodies read [fetch] 128 at a time, at least one document per page, then
 * as many as fit [budget] (each counted at its body's length plus 64).
 */
internal suspend fun snapshotPageDocs(
    tree: DocumentTree,
    after: String,
    budget: Int,
    headHex: String,
    fetch: suspend (List<KdbUuid>) -> List<KdbDocument?>,
): Pair<List<SnapshotDoc>, Boolean> {
    val ids = snapshotIdsAfter(tree, after)
    val docs = ArrayList<SnapshotDoc>()
    var size = 0
    while (true) {
        val chunk = ArrayList<KdbUuid>(128)
        while (chunk.size < 128 && ids.hasNext()) chunk += ids.next()
        if (chunk.isEmpty()) return docs to true
        val bodies = fetch(chunk)
        for ((j, d) in bodies.withIndex()) {
            val id = chunk[j].toString()
            val body = d?.json ?: throw PeerSyncException("peer sync: document $id of $headHex has no body here")
            if (docs.isNotEmpty() && size + body.length > budget) return docs to false
            size += body.length + 64
            docs += SnapshotDoc(id, body)
        }
    }
}

/**
 * The ids of [tree] a SNAPSHOT_FETCH cursor [after] has yet to see, in the order pages carry them:
 * every id whose canonical lowercase string sorts after the cursor string, which is UUID byte order.
 * A cursor this node wrote (or Go wrote) is a canonical id, so the trie seeks straight to it; any
 * other string is compared the way it always was, against every id in order.
 */
internal fun snapshotIdsAfter(
    tree: DocumentTree,
    after: String,
): Iterator<KdbUuid> {
    if (after.isEmpty()) return tree.idsAfter(null)
    val cursor = runCatching { KdbUuid.fromString(after) }.getOrNull()
    if (cursor != null && cursor.toString() == after) return tree.idsAfter(cursor)
    return tree.idsAfter(null).asSequence().filter { it.toString() > after }.iterator()
}

/** A frame this host does not serve (Go's UnsupportedFrameError): answered UNSUPPORTED. */
internal class UnsupportedFrameException(what: String) : PeerSyncException("peer sync: $what is not supported by this node")

/** A frame naming a namespace the session was not granted (Go's NotGrantedError). */
internal class NotGrantedException(namespace: String) : PeerSyncException("peer sync: namespace \"$namespace\" was not granted to this session")

/** Whether [pattern] selects [namespace]: "*" one segment, "**" any number (Go's MatchNamespace). */
internal fun matchNamespace(
    pattern: String,
    namespace: String,
): Boolean = matchSegments(pattern.split("/"), namespace.split("/"))

private fun matchSegments(
    pat: List<String>,
    segs: List<String>,
): Boolean {
    if (pat.isEmpty()) return segs.isEmpty()
    return when (pat[0]) {
        "**" -> (0..segs.size).any { matchSegments(pat.drop(1), segs.drop(it)) }
        "*" -> segs.isNotEmpty() && matchSegments(pat.drop(1), segs.drop(1))
        else -> segs.isNotEmpty() && segs[0] == pat[0] && matchSegments(pat.drop(1), segs.drop(1))
    }
}

/** The namespaces of [available] that match a pattern and no "!" exclusion, sorted (Go's SelectNamespaces). */
internal fun selectNamespaces(
    patterns: List<String>,
    available: List<String>,
): List<String> {
    val include = patterns.filter { !it.startsWith("!") }
    val exclude = patterns.filter { it.startsWith("!") }.map { it.substring(1) }
    return available.filter { n -> include.any { matchNamespace(it, n) } && exclude.none { matchNamespace(it, n) } }.sorted()
}
