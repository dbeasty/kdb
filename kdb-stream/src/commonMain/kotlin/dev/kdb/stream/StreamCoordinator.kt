package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.index.IndexManager
import dev.kdb.storage.StorageAdapter
import kotlinx.coroutines.flow.Flow
import dev.kdb.inspect.sidecar.WireDebugHook
import dev.kdb.transaction.TransactionEngine
import dev.kdb.wire.*

public interface StreamCoordinator {
    public suspend fun start(session: StreamSessionConfig)
    public suspend fun stop()
    public suspend fun publish(commit: PublishedCommit)
    public val subscribers: kotlinx.coroutines.flow.Flow<SubscriberState>
}

public fun streamCoordinator(
    wire: WireCodec,
    transport: WireTransport,
    indexManager: IndexManager,
    dag: CommitDag,
    storage: StorageAdapter,
    transactionEngine: TransactionEngine? = null,
    wireDebugHook: WireDebugHook? = null,
): StreamCoordinator =
    DefaultStreamCoordinator(wire, transport, indexManager, dag, storage, transactionEngine, wireDebugHook)

internal class DefaultStreamCoordinator(
    private val wire: WireCodec,
    private val transport: WireTransport,
    @Suppress("UNUSED_PARAMETER") private val indexManager: IndexManager,
    private val dag: CommitDag,
    @Suppress("UNUSED_PARAMETER") private val storage: StorageAdapter,
    private val transactionEngine: TransactionEngine?,
    private val wireDebugHook: WireDebugHook? = null,
) : StreamCoordinator {
    private val subscriberEvents = kotlinx.coroutines.flow.MutableSharedFlow<SubscriberState>(extraBufferCapacity = 16)
    private var session: StreamSessionConfig? = null
    private var correlation = 1000

    override val subscribers: Flow<SubscriberState> = subscriberEvents

    override suspend fun start(session: StreamSessionConfig) {
        this.session = session
        if (transport !is InMemoryWireTransport) return
        val hub = InMemoryWireTransportHub.hub(session.namespaceId)
        hub.serverHandler = { frame -> handleServerFrame(session, frame, hub) }
    }

    override suspend fun stop() {
        if (transport is InMemoryWireTransport) {
            InMemoryWireTransportHub.hub(session?.namespaceId ?: return).serverHandler = null
        }
    }

    override suspend fun publish(commit: PublishedCommit) {
        val cfg = session ?: return
        val msg =
            WireMessage.DeltaCommit(
                WireHeader(
                    WireMessageType.DELTA_COMMIT,
                    KDB_WIRE_PROTOCOL_VERSION,
                    correlation++,
                    0,
                ),
                DeltaCommitPayload(
                    namespace = cfg.namespaceId,
                    commitHash = commit.commitHash,
                    parentHash = commit.parentHash,
                    timestampMicros = commit.timestampMicros,
                    operations = commit.operations,
                    indexHints = commit.indexHints,
                ),
            )
        wireDebugHook?.onWire(msg, "out")
        if (transport is InMemoryWireTransport) {
            InMemoryWireTransportHub.hub(cfg.namespaceId).serverSend(wire.encode(msg))
        }
    }

    private suspend fun handleServerFrame(
        session: StreamSessionConfig,
        frame: ByteArray,
        hub: InMemoryWireTransportHub.Hub,
    ) {
        val message = wire.decode(frame)
        wireDebugHook?.onWire(message, "in")
        when (message) {
            is WireMessage.Handshake -> {
                val ack =
                    HandshakeAckPayload(
                        accepted = true,
                        negotiatedEncoding = PayloadEncoding.KDB_BINARY,
                        protocolVersion = KDB_WIRE_PROTOCOL_VERSION,
                        remoteHeads = message.request.localHeads,
                    )
                val ackMsg =
                    WireMessage.HandshakeAck(
                        WireHeader(
                            WireMessageType.HANDSHAKE,
                            KDB_WIRE_PROTOCOL_VERSION,
                            message.header.correlationId,
                            0,
                        ),
                        ack,
                    )
                hub.serverSend(wire.encode(ackMsg))
                val mode =
                    when (message.request.clientMode) {
                        WireClientMode.STREAM_WRITE_BACK -> StreamClientMode.WRITE_BACK
                        else -> StreamClientMode.READ_ONLY
                    }
                updateSubscriber(message.request.nodeId, mode, session.headProvider())
            }

            is WireMessage.PositionAck -> {
                val existing = lastSubscriber
                if (existing != null) {
                    updateSubscriber(existing.nodeId, existing.mode, message.commitHash)
                }
            }

            else -> {}
        }
    }

    private var lastSubscriber: SubscriberState? = null

    private suspend fun updateSubscriber(nodeId: String, mode: StreamClientMode, lastAck: KdbHash) {
        val state = SubscriberState(nodeId, mode, lastAck)
        lastSubscriber = state
        subscriberEvents.emit(state)
    }
}
