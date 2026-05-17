package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.storage.StorageAdapter
import dev.kdb.stream.WireConnection
import dev.kdb.stream.WireTransport
import dev.kdb.transaction.TransactionEngine
import dev.kdb.wire.*
import kotlinx.coroutines.delay

public interface PeerSyncClient {
    public suspend fun connect(config: PeerClientConfig): PeerSession
    public suspend fun disconnect()
}

public interface PeerSession {
    public val namespaceId: String
    public val remoteHead: KdbHash
    public suspend fun pullMissing(): PeerSyncResult
    public suspend fun pushCommits(commits: List<KdbCommit>): Int
    public suspend fun syncBidirectional(): PeerSyncResult
}

public fun peerSyncClient(
    wire: WireCodec,
    transport: WireTransport,
    dag: CommitDag,
    storage: StorageAdapter,
    transactionEngine: TransactionEngine? = null,
): PeerSyncClient = DefaultPeerSyncClient(wire, transport, dag, storage, transactionEngine)

internal class DefaultPeerSyncClient(
    private val wire: WireCodec,
    private val transport: WireTransport,
    private val dag: CommitDag,
    @Suppress("UNUSED_PARAMETER") private val storage: StorageAdapter,
    @Suppress("UNUSED_PARAMETER") private val transactionEngine: TransactionEngine?,
) : PeerSyncClient {
    private var connection: WireConnection? = null
    private var correlation = 2000

    override suspend fun connect(config: PeerClientConfig): PeerSession {
        val conn = transport.connect(config.peerUri)
        connection = conn
        val localHead = dag.head()
        val hs =
            WireMessage.Handshake(
                WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, nextCorrelation(), 0),
                HandshakePayload(
                    nodeId = config.nodeId,
                    namespaces = listOf(config.namespaceId),
                    localHeads = mapOf(config.namespaceId to localHead.toHex()),
                    clientMode = WireClientMode.FULL_PEER,
                ),
            )
        val ack =
            request(conn, hs) as? WireMessage.HandshakeAck
                ?: throw PeerSyncException("expected HandshakeAck")
        if (!ack.response.accepted) {
            throw PeerSyncException(ack.response.rejectionReason ?: "handshake rejected")
        }
        val remoteHex =
            ack.response.remoteHeads[config.namespaceId]
                ?: throw PeerSyncException("remote head missing for ${config.namespaceId}")
        val remoteHead = KdbHash.fromHex(remoteHex)
        return DefaultPeerSession(this, dag, config.namespaceId, remoteHead, conn)
    }

    override suspend fun disconnect() {
        connection?.close()
        connection = null
    }

    internal fun nextCorrelation(): Int = correlation++

    internal suspend fun request(
        conn: WireConnection,
        message: WireMessage,
    ): WireMessage {
        val cid =
            when (message) {
                is WireMessage.Handshake -> message.header.correlationId
                is WireMessage.CommitFetch -> message.header.correlationId
                is WireMessage.CommitPush -> message.header.correlationId
                else -> message.header.correlationId
            }
        conn.send(wire.encode(message))
        repeat(200) {
            val frame = conn.tryPoll()
            if (frame != null) {
                val decoded = wire.decode(frame)
                if (decoded.header.correlationId == cid) return decoded
            }
            delay(1)
        }
        throw PeerSyncException("no response for correlation $cid")
    }

    internal suspend fun fetchRemote(
        conn: WireConnection,
        namespaceId: String,
        sinceHash: KdbHash?,
        maxCommits: Int = 100,
    ): List<KdbCommit> {
        val fetch =
            WireMessage.CommitFetch(
                WireHeader(WireMessageType.COMMIT_FETCH, KDB_WIRE_PROTOCOL_VERSION, nextCorrelation(), 0),
                namespaceId,
                sinceHash,
                maxCommits,
            )
        val response =
            request(conn, fetch) as? WireMessage.CommitPush
                ?: throw PeerSyncException("expected CommitPush response to CommitFetch")
        return response.commits
    }

    internal suspend fun pushToRemote(
        conn: WireConnection,
        namespaceId: String,
        commits: List<KdbCommit>,
    ): Int {
        if (commits.isEmpty()) return 0
        val push =
            WireMessage.CommitPush(
                WireHeader(WireMessageType.COMMIT_PUSH, KDB_WIRE_PROTOCOL_VERSION, nextCorrelation(), 0),
                namespaceId,
                commits,
            )
        request(conn, push)
        return commits.size
    }
}

internal class DefaultPeerSession(
    private val client: DefaultPeerSyncClient,
    private val dag: CommitDag,
    override val namespaceId: String,
    override val remoteHead: KdbHash,
    private val conn: WireConnection,
) : PeerSession {

    override suspend fun pullMissing(): PeerSyncResult {
        val localHead = dag.head()
        if (localHead == remoteHead) {
            return PeerSyncResult(0, 0, localHead, computeSyncPlan(dag, localHead, remoteHead))
        }
        val fetched = client.fetchRemote(conn, namespaceId, sinceHash = localHead)
        var applied = 0
        for (commit in fetched) {
            if (dag.hasCommit(commit.hash)) continue
            dag.putCommit(commit, requireParents = true)
            applied++
        }
        if (fetched.isNotEmpty()) {
            dag.setHead("main", fetched.last().hash)
        }
        val finalHead = dag.head()
        val plan = computeSyncPlan(dag, finalHead, remoteHead)
        return PeerSyncResult(applied, 0, finalHead, plan)
    }

    override suspend fun pushCommits(commits: List<KdbCommit>): Int = client.pushToRemote(conn, namespaceId, commits)

    override suspend fun syncBidirectional(): PeerSyncResult {
        val pull = pullMissing()
        val localHead = dag.head()
        val toPush =
            if (dag.hasCommit(remoteHead)) {
                val walked = dag.walk(from = localHead, until = remoteHead, limit = 100)
                walked.filterIsInstance<dev.kdb.dag.TraversalEntry.Full>().map { it.commit }.reversed()
            } else {
                emptyList()
            }
        val pushed = pushCommits(toPush)
        return pull.copy(pushedCommits = pushed, finalHead = dag.head())
    }
}
