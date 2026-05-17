package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.wire.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Fan-out [WireMessage.DeltaCommit] to WebSocket (or other) subscribers for one namespace.
 */
public class StreamBroadcastHub(
    private val wire: WireCodec,
    private val namespaceId: String,
    private val headProvider: suspend () -> KdbHash,
) {
    private val mutex = Mutex()
    private val subscribers = mutableListOf<RegisteredSubscriber>()
    private var correlation = 5000

    private data class RegisteredSubscriber(
        val nodeId: String,
        val connection: WireConnection,
        var lastAck: KdbHash?,
    )

    public suspend fun handleFrame(
        connection: WireConnection,
        frame: ByteArray,
    ): ByteArray? =
        when (val msg = wire.decode(frame)) {
            is WireMessage.Handshake -> handleHandshake(connection, msg)
            is WireMessage.PositionAck -> {
                updateLastAck(connection, msg.commitHash)
                null
            }
            else -> null
        }

    public suspend fun unregister(connection: WireConnection) {
        mutex.withLock {
            subscribers.removeAll { it.connection === connection }
        }
    }

    public suspend fun publish(commit: PublishedCommit) {
        val encoded = wire.encode(deltaMessage(commit))
        val targets =
            mutex.withLock {
                subscribers.toList()
            }
        for (sub in targets) {
            try {
                sub.connection.send(encoded)
            } catch (_: Exception) {
                unregister(sub.connection)
            }
        }
    }

    private suspend fun handleHandshake(
        connection: WireConnection,
        msg: WireMessage.Handshake,
    ): ByteArray {
        val mode = msg.request.clientMode
        if (mode != WireClientMode.STREAM_READ_ONLY && mode != WireClientMode.STREAM_WRITE_BACK) {
            return wire.encode(
                handshakeAck(
                    msg,
                    accepted = false,
                    heads = emptyMap(),
                    rejectionReason = "STREAM_READ_ONLY or STREAM_WRITE_BACK required",
                ),
            )
        }
        if (namespaceId !in msg.request.namespaces) {
            return wire.encode(
                handshakeAck(
                    msg,
                    accepted = false,
                    heads = emptyMap(),
                    rejectionReason = "namespace mismatch",
                ),
            )
        }
        val head = headProvider()
        val resumeHex = msg.request.localHeads[namespaceId]
        val resume = resumeHex?.let { KdbHash.fromHex(it) }
        mutex.withLock {
            subscribers.removeAll { it.connection === connection }
            subscribers +=
                RegisteredSubscriber(
                    nodeId = msg.request.nodeId,
                    connection = connection,
                    lastAck = resume,
                )
        }
        return wire.encode(
            handshakeAck(
                msg,
                accepted = true,
                heads = mapOf(namespaceId to head.toHex()),
                rejectionReason = null,
            ),
        )
    }

    private suspend fun updateLastAck(
        connection: WireConnection,
        commitHash: KdbHash,
    ) {
        mutex.withLock {
            subscribers.filter { it.connection === connection }.forEach { it.lastAck = commitHash }
        }
    }

    private fun handshakeAck(
        msg: WireMessage.Handshake,
        accepted: Boolean,
        heads: Map<String, String>,
        rejectionReason: String?,
    ): WireMessage.HandshakeAck =
        WireMessage.HandshakeAck(
            WireHeader(
                WireMessageType.HANDSHAKE,
                KDB_WIRE_PROTOCOL_VERSION,
                msg.header.correlationId,
                0,
            ),
            HandshakeAckPayload(
                accepted = accepted,
                negotiatedEncoding = PayloadEncoding.KDB_BINARY,
                protocolVersion = KDB_WIRE_PROTOCOL_VERSION,
                remoteHeads = heads,
                rejectionReason = rejectionReason,
            ),
        )

    private fun deltaMessage(commit: PublishedCommit): WireMessage.DeltaCommit =
        WireMessage.DeltaCommit(
            WireHeader(
                WireMessageType.DELTA_COMMIT,
                KDB_WIRE_PROTOCOL_VERSION,
                correlation++,
                0,
            ),
            DeltaCommitPayload(
                namespace = namespaceId,
                commitHash = commit.commitHash,
                parentHash = commit.parentHash,
                timestampMicros = commit.timestampMicros,
                operations = commit.operations,
                indexHints = commit.indexHints,
            ),
        )
}

public fun publishedCommitFrom(commit: KdbCommit): PublishedCommit {
    val parent =
        commit.parentHashes.firstOrNull()
            ?: error("commit ${commit.hash.toHex()} has no parent")
    return PublishedCommit(
        commitHash = commit.hash,
        parentHash = parent,
        operations = commit.operations,
        indexHints = emptyList(),
        timestampMicros = commit.timestamp.toEpochMicros(),
    )
}

/** Derives stream coordinator URI from peer-sync URI (`/kdb` → `/kdb/stream`). */
public fun streamUriFromPeerUri(peerUri: String): String {
    val trimmed = peerUri.trim()
    val pathEnd = trimmed.indexOf('?')
    val base = if (pathEnd >= 0) trimmed.substring(0, pathEnd) else trimmed
    val query = if (pathEnd >= 0) trimmed.substring(pathEnd) else ""
    val streamBase =
        when {
            base.endsWith("/kdb") -> base + "/stream"
            base.endsWith("/") -> base + "kdb/stream"
            else -> base.trimEnd('/') + "/stream"
        }
    return streamBase + query
}
