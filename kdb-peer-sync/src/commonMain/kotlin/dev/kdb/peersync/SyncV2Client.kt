package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbCommit
import dev.kdb.error.ConflictReport
import dev.kdb.stream.WireConnection
import dev.kdb.wire.WireCodec
import dev.kdb.wire.WireMessage
import kotlinx.coroutines.delay
import kotlin.time.TimeSource

/** One v2 connection's request/reply plumbing: sync frames are JSON envelopes, attachment frames
 * kdb-wire's binary ones, and a PEER_ERROR reply to either is thrown as [RemotePeerException]. */
internal class V2Conn(
    private val wire: WireCodec,
    private val conn: WireConnection,
    private val timeoutMillis: Long,
) {
    private var correlation = 10_000

    /** The capabilities both nodes have, from the hello ack. */
    var caps: List<String> = emptyList()

    fun next(): Int = ++correlation

    suspend fun requestSync(msg: SyncV2Message): SyncV2Message {
        val reply = request(SyncV2Codec.encode(msg), msg.correlationId)
        if (!SyncV2.isSyncFrame(frameTypeCode(reply))) throw PeerSyncException("peer sync: expected a v2 reply, got type 0x${frameTypeCode(reply).toString(16)}")
        return SyncV2Codec.decode(reply)
    }

    suspend fun requestWire(msg: WireMessage): WireMessage = wire.decode(request(wire.encode(msg), msg.header.correlationId))

    private suspend fun request(frame: ByteArray, cid: Int): ByteArray {
        conn.send(frame)
        val deadline = TimeSource.Monotonic.markNow()
        var idle = 0L
        while (deadline.elapsedNow().inWholeMilliseconds < timeoutMillis) {
            val reply = conn.tryPoll()
            if (reply == null) {
                // Back off from a spin to a few milliseconds: a reply usually lands at once.
                delay(minOf(5L, idle++ / 64))
                continue
            }
            idle = 0
            if (frameCorrelationId(reply) != cid) continue
            if (frameTypeCode(reply) == SyncV2.PEER_ERROR) {
                val e = SyncV2Codec.decode(reply) as SyncV2Message.PeerError
                throw RemotePeerException(e.code, e.message)
            }
            return reply
        }
        throw PeerSyncException("peer sync: no reply to correlation $cid within ${timeoutMillis}ms")
    }
}

/** What a v2 sync did to the namespace. */
public data class V2SyncResult(
    val pulled: Int = 0,
    val pushed: Int = 0,
    /** Each ref's outcome here and on the peer, keyed "branch:<name>" / "tag:<name>". */
    val local: Map<String, String> = emptyMap(),
    val remote: Map<String, String> = emptyMap(),
    /** Refs other than main this sync could not take or send, by key; main failing throws. */
    val refErrors: Map<String, String> = emptyMap(),
    val conflict: ConflictReport? = null,
)

/**
 * A v2 session against one peer, for one namespace (Go's v2Conn.syncNamespace): pull first - every
 * ref the peer advertises, main first, the blobs each page attaches fetched before the page is
 * stored - then push every local ref the peer lacks, the blobs each page attaches uploaded before
 * the page's REF_UPDATE.
 */
internal class V2PeerSession(
    private val conn: V2Conn,
    private val connection: WireConnection,
    private val ns: PeerSyncNamespace,
    private var remote: NamespaceRefs,
    private val pageBytes: Int = DEFAULT_PAGE_BYTES,
) : PeerSession {
    override val namespaceId: String get() = ns.namespaceId

    override val remoteHead: KdbHash
        get() = remote.branches[MAIN_BRANCH]?.let(KdbHash::fromHex) ?: KdbHash(ByteArray(32))

    private val attachments: Boolean get() = SyncV2.CAP_ATTACHMENTS in conn.caps && ns.blobs != null

    override suspend fun pullMissing(): PeerSyncResult {
        val r = pull()
        return result(r)
    }

    override suspend fun syncBidirectional(): PeerSyncResult {
        val pulled = pull()
        val pushed = push()
        return result(pulled.copy(pushed = pushed.pushed, remote = pushed.remote, refErrors = pulled.refErrors + pushed.refErrors))
    }

    /** Pushes main's [commits] (parents first) and proposes the last as the peer's main. */
    override suspend fun pushCommits(commits: List<KdbCommit>): Int {
        if (commits.isEmpty()) return 0
        val ack = sendRef(NamedRef(SyncV2.REF_BRANCH, MAIN_BRANCH, commits.last().hash.toHex()), commits)
        return ack.stored
    }

    override suspend fun fetchCommitsSince(sinceHash: KdbHash?): List<KdbCommit> {
        val page = conn.requestSync(SyncV2Message.FetchRequest(conn.next(), ns.namespaceId, listOf(remoteHead), listOfNotNull(sinceHash), pageBytes))
        return (page as? SyncV2Message.PackPage ?: throw PeerSyncException("peer sync: expected PACK_PAGE")).commits
    }

    suspend fun close() = connection.close()

    private suspend fun result(r: V2SyncResult): PeerSyncResult {
        val head = ns.dag.head()
        return PeerSyncResult(r.pulled, r.pushed, head, computeSyncPlan(ns.dag, head, remoteHead), r.conflict)
    }

    /** Pulls every ref the peer advertises (Go's v2Conn.pull). */
    suspend fun pull(): V2SyncResult {
        if (remote.access == SyncV2.ACCESS_PUSH) return V2SyncResult()
        var haves = localHeads()
        var pulled = 0
        val local = LinkedHashMap<String, String>()
        val refErrors = LinkedHashMap<String, String>()
        var conflict: ConflictReport? = null
        for (ref in orderedRefs(remote)) {
            val isMain = ref.kind == SyncV2.REF_BRANCH && ref.name == MAIN_BRANCH
            try {
                val h = KdbHash.fromHex(ref.hex)
                if (!ns.dag.hasCommit(h)) {
                    val (n, grown) = fetchRef(h, haves)
                    pulled += n
                    haves = grown
                }
                val res = applyRef(ns, ref.kind, ref.name, h)
                local[ref.key] = res.outcome
                if (isMain && res.conflict != null) conflict = res.conflict
            } catch (e: Exception) {
                if (isMain) throw e
                refErrors[ref.key] = e.message ?: e.toString()
            }
        }
        return V2SyncResult(pulled = pulled, local = local, refErrors = refErrors, conflict = conflict)
    }

    /** Pages in everything [want] needs that this node lacks; returns how many commits were new and
     * the haves grown by what arrived. */
    private suspend fun fetchRef(
        want: KdbHash,
        startHaves: List<KdbHash>,
    ): Pair<Int, List<KdbHash>> {
        var haves = startHaves
        var stored = 0
        while (true) {
            val reply = conn.requestSync(SyncV2Message.FetchRequest(conn.next(), ns.namespaceId, listOf(want), haves, pageBytes))
            val page = reply as? SyncV2Message.PackPage ?: throw PeerSyncException("peer sync: expected PACK_PAGE, got $reply")
            if (page.stubs.isNotEmpty()) throw PeerSyncException("peer sync: the peer sent archived-commit stubs, which this node cannot store")
            // The blobs the page attaches come first: no stored commit names bytes this node lacks.
            if (attachments) fetchBlobs(conn, ns.blobs!!, ns.namespaceId, page.commits)
            stored += storeCommits(ns.dag, page.commits)
            // Pages come parents first, so the last page's tips stand for everything received.
            haves = startHaves + pageTips(page.commits)
            if (page.done || page.commits.isEmpty()) return stored to haves
        }
    }

    /** Pushes every local ref the peer lacks (Go's v2Conn.push). */
    suspend fun push(): V2SyncResult {
        if (remote.access == SyncV2.ACCESS_PULL) return V2SyncResult()
        var pushed = 0
        val outcomes = LinkedHashMap<String, String>()
        val refErrors = LinkedHashMap<String, String>()
        for (ref in orderedRefs(refsOf(ns.namespaceId, ns.dag))) {
            val remoteHex = if (ref.kind == SyncV2.REF_TAG) remote.tags[ref.name] else remote.branches[ref.name]
            if (remoteHex == ref.hex) continue
            val local = KdbHash.fromHex(ref.hex)
            val known = remoteTargets().filter { ns.dag.hasCommit(it) }
            val missing = missingCommits(ns.dag, listOf(local), known)
            if (!attachments) {
                firstCommitWithAttachments(missing)?.let {
                    refErrors[ref.key] = AttachmentsUnsupportedException(ns.namespaceId, it.hash.toHex()).message!!
                    continue
                }
            }
            val ack = sendRef(ref, missing, remoteHex ?: "")
            pushed += ack.stored
            outcomes[ref.key] = ack.outcome
            if (ref.kind == SyncV2.REF_BRANCH && ref.name == MAIN_BRANCH && ack.headHex.isNotEmpty()) {
                remote = remote.copy(branches = remote.branches + (MAIN_BRANCH to ack.headHex))
            }
        }
        return V2SyncResult(pushed = pushed, remote = outcomes, refErrors = refErrors)
    }

    /** Sends [missing] (parents first) in pages, then proposes [ref]'s value on the last. */
    private suspend fun sendRef(
        ref: NamedRef,
        missing: List<KdbCommit>,
        oldHex: String = "",
    ): SyncV2Message.RefUpdateAck {
        var rest = missing
        var stored = 0
        while (true) {
            val (page, done) = pageOf(rest, pageBytes)
            if (attachments) uploadBlobs(conn, ns.blobs!!, ns.namespaceId, page)
            val reply =
                conn.requestSync(
                    SyncV2Message.RefUpdate(
                        conn.next(), ns.namespaceId, ref.kind, ref.name, oldHex, ref.hex, page, more = !done,
                        zstd = SyncV2.CAP_ZSTD_PACK in conn.caps,
                    ),
                )
            val ack = reply as? SyncV2Message.RefUpdateAck ?: throw PeerSyncException("peer sync: expected REF_UPDATE_ACK, got $reply")
            stored += ack.stored
            if (done) return ack.copy(stored = stored)
            rest = rest.subList(page.size, rest.size)
        }
    }

    private suspend fun localHeads(): List<KdbHash> =
        (ns.dag.listBranches().map { it.headHash } + ns.dag.listTags().map { it.commitHash }).distinct()

    private fun remoteTargets(): List<KdbHash> = (remote.branches.values + remote.tags.values).distinct().map(KdbHash::fromHex)
}

/**
 * Opens a v2 session over [connection]: SYNC_HELLO for [ns], answered by the peer's grant and refs.
 * Returns null when the peer only speaks v1 (it answered PEER_ERROR UNSUPPORTED) - the caller then
 * syncs over v1 on a fresh connection.
 */
internal suspend fun openV2Session(
    wire: WireCodec,
    connection: WireConnection,
    ns: PeerSyncNamespace,
    config: PeerClientConfig,
): V2PeerSession? {
    val conn = V2Conn(wire, connection, config.requestTimeoutMillis)
    val caps = if (ns.blobs == null) SyncV2.CAPABILITIES - SyncV2.CAP_ATTACHMENTS else SyncV2.CAPABILITIES
    val ctx = config.connectionContext
    val reply =
        try {
            conn.requestSync(
                SyncV2Message.Hello(
                    conn.next(), config.nodeId, SyncV2.PROTOCOL, caps, listOf(ns.namespaceId), ctx.user, ctx.password, ctx.token,
                ),
            )
        } catch (e: RemotePeerException) {
            if (e.errorCode == PeerErrorCodes.UNSUPPORTED) return null
            throw e
        }
    val ack = reply as? SyncV2Message.HelloAck ?: throw PeerSyncException("peer sync: expected SYNC_HELLO_ACK, got $reply")
    if (!ack.accepted) throw PeerSyncException("peer sync: peer refused sync: ${ack.reason}")
    conn.caps = ack.capabilities
    val refs =
        ack.refs.firstOrNull { it.namespace == ns.namespaceId }
            ?: throw PeerSyncException("peer sync: the peer did not grant namespace ${ns.namespaceId}")
    return V2PeerSession(conn, connection, ns, refs)
}
