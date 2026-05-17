package dev.kdb.peersync

import dev.kdb.auth.AllowAllAuth
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.ConnectionContext
import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.storage.StorageAdapter
import dev.kdb.stream.InMemoryWireTransportHub
import dev.kdb.transaction.TransactionEngine
import dev.kdb.wire.WireCodec

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
    auth: AuthEngine = AllowAllAuth,
    connectionContext: ConnectionContext = ConnectionContext.EMPTY,
): PeerSyncHost = DefaultPeerSyncHost(wire, dag, storage, transactionEngine, auth, connectionContext)

public fun peerSyncHostFactory(
    wire: WireCodec,
    dag: CommitDag,
    storage: StorageAdapter,
    config: PeerHostConfig,
    auth: AuthEngine = AllowAllAuth,
    transactionEngine: TransactionEngine? = null,
): (ConnectionContext) -> PeerSyncHost =
    { ctx ->
        ConnectionPeerSyncHost(
            PeerSyncFrameHandler(wire, dag, storage, config, auth, ctx),
        )
    }

internal class ConnectionPeerSyncHost(
    private val handler: PeerSyncFrameHandler,
) : PeerSyncHost {
    override suspend fun start(config: PeerHostConfig) {
        error("ConnectionPeerSyncHost does not support in-memory hub start")
    }

    override suspend fun stop() = Unit

    override suspend fun handleFrame(frame: ByteArray): ByteArray? = handler.handleFrame(frame)
}

internal class DefaultPeerSyncHost(
    private val wire: WireCodec,
    private val dag: CommitDag,
    private val storage: StorageAdapter,
    @Suppress("UNUSED_PARAMETER") private val transactionEngine: TransactionEngine?,
    private val auth: AuthEngine,
    private val connectionContext: ConnectionContext,
) : PeerSyncHost {
    private var config: PeerHostConfig? = null
    private var handler: PeerSyncFrameHandler? = null

    override suspend fun start(config: PeerHostConfig) {
        this.config = config
        handler =
            PeerSyncFrameHandler(
                wire = wire,
                dag = dag,
                storage = storage,
                cfg = config,
                auth = auth,
                connectionContext = connectionContext,
            )
        val hub = InMemoryWireTransportHub.hub(config.transportHub)
        hub.serverHandler = { frame ->
            val response = handler?.handleFrame(frame)
            if (response != null) {
                hub.serverSend(response)
            }
        }
    }

    override suspend fun stop() {
        val cfg = config ?: return
        InMemoryWireTransportHub.hub(cfg.transportHub).serverHandler = null
        this.config = null
        handler = null
    }

    override suspend fun handleFrame(frame: ByteArray): ByteArray? {
        val h = handler ?: throw PeerSyncException("PeerSyncHost not started")
        return h.handleFrame(frame)
    }

    internal suspend fun fetchCommits(
        sinceHash: KdbHash?,
        maxCommits: Int,
    ): List<KdbCommit> {
        val h = handler ?: throw PeerSyncException("PeerSyncHost not started")
        return h.fetchCommits(sinceHash, maxCommits)
    }
}
