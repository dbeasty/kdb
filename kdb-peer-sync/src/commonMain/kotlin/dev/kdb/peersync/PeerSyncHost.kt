package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.dag.TraversalEntry
import dev.kdb.document.KdbCommit
import dev.kdb.storage.StorageAdapter
import dev.kdb.stream.InMemoryWireTransportHub
import dev.kdb.transaction.TransactionEngine
import dev.kdb.wire.*

public interface PeerSyncHost {
    public suspend fun start(config: PeerHostConfig)
    public suspend fun stop()
    public suspend fun handleFrame(frame: ByteArray): ByteArray?
}

public fun peerSyncHost(
    wire: WireCodec,
    dag: CommitDag,
    storage: StorageAdapter,
    transactionEngine: TransactionEngine? = null,
): PeerSyncHost = DefaultPeerSyncHost(wire, dag, storage, transactionEngine)

internal class DefaultPeerSyncHost(
    private val wire: WireCodec,
    private val dag: CommitDag,
    @Suppress("UNUSED_PARAMETER") private val storage: StorageAdapter,
    @Suppress("UNUSED_PARAMETER") private val transactionEngine: TransactionEngine?,
) : PeerSyncHost {
    private var config: PeerHostConfig? = null

    override suspend fun start(config: PeerHostConfig) {
        this.config = config
        InMemoryWireTransportHub.hub(config.transportHub).serverHandler = { frame ->
            val response = handleFrame(frame)
            if (response != null) {
                InMemoryWireTransportHub.hub(config.transportHub).serverSend(response)
            }
        }
    }

    override suspend fun stop() {
        val cfg = config ?: return
        InMemoryWireTransportHub.hub(cfg.transportHub).serverHandler = null
        this.config = null
    }

    override suspend fun handleFrame(frame: ByteArray): ByteArray? {
        val cfg = config ?: throw PeerSyncException("PeerSyncHost not started")
        return when (val msg = wire.decode(frame)) {
            is WireMessage.Handshake -> handleHandshake(msg, cfg)
            is WireMessage.CommitFetch -> handleCommitFetch(msg, cfg)
            is WireMessage.CommitPush -> handleCommitPush(msg, cfg)
            else -> null
        }
    }

    private suspend fun handleHandshake(
        msg: WireMessage.Handshake,
        cfg: PeerHostConfig,
    ): ByteArray {
        val heads =
            mapOf(cfg.namespaceId to dag.head().toHex())
        val ack =
            WireMessage.HandshakeAck(
                WireHeader(
                    WireMessageType.HANDSHAKE,
                    KDB_WIRE_PROTOCOL_VERSION,
                    msg.header.correlationId,
                    0,
                ),
                HandshakeAckPayload(
                    accepted = true,
                    negotiatedEncoding = PayloadEncoding.KDB_BINARY,
                    protocolVersion = KDB_WIRE_PROTOCOL_VERSION,
                    remoteHeads = heads,
                ),
            )
        return wire.encode(ack)
    }

    private suspend fun handleCommitFetch(
        msg: WireMessage.CommitFetch,
        cfg: PeerHostConfig,
    ): ByteArray {
        require(msg.namespace == cfg.namespaceId) {
            throw PeerSyncException("namespace mismatch: ${msg.namespace}")
        }
        val commits = fetchCommits(msg.sinceHash, msg.maxCommits)
        val push =
            WireMessage.CommitPush(
                WireHeader(
                    WireMessageType.COMMIT_PUSH,
                    KDB_WIRE_PROTOCOL_VERSION,
                    msg.header.correlationId,
                    0,
                ),
                msg.namespace,
                commits,
            )
        return wire.encode(push)
    }

    private suspend fun handleCommitPush(
        msg: WireMessage.CommitPush,
        cfg: PeerHostConfig,
    ): ByteArray {
        require(msg.namespace == cfg.namespaceId) {
            throw PeerSyncException("namespace mismatch: ${msg.namespace}")
        }
        var applied = 0
        for (commit in msg.commits) {
            if (dag.hasCommit(commit.hash)) continue
            try {
                dag.putCommit(commit, requireParents = true)
                applied++
            } catch (e: Exception) {
                throw PeerSyncException("failed to apply commit ${commit.hash.toHex()}", e)
            }
        }
        val ack =
            WireMessage.CommitPush(
                WireHeader(
                    WireMessageType.COMMIT_PUSH,
                    KDB_WIRE_PROTOCOL_VERSION,
                    msg.header.correlationId,
                    0,
                ),
                msg.namespace,
                emptyList(),
            )
        return wire.encode(ack)
    }

    internal suspend fun fetchCommits(
        sinceHash: KdbHash?,
        maxCommits: Int,
    ): List<KdbCommit> {
        val head = dag.head()
        val walked = dag.walk(from = head, until = sinceHash, limit = maxCommits)
        return walked
            .filterIsInstance<TraversalEntry.Full>()
            .map { it.commit }
            .reversed()
    }
}
