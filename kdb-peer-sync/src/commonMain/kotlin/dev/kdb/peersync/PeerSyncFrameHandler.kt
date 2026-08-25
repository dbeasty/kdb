package dev.kdb.peersync

import dev.kdb.auth.AllowAllAuth
import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.ConnectionAuthSupport
import dev.kdb.auth.ConnectionContext
import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.dag.TraversalEntry
import dev.kdb.document.KdbCommit
import dev.kdb.error.ConflictReport
import dev.kdb.storage.StorageAdapter
import dev.kdb.wire.*

internal class PeerSyncFrameHandler(
    private val wire: WireCodec,
    private val dag: CommitDag,
    private val storage: StorageAdapter,
    private val cfg: PeerHostConfig,
    auth: AuthEngine = AllowAllAuth,
    connectionContext: ConnectionContext = ConnectionContext.EMPTY,
) {
    private val connectionAuth = ConnectionAuthSupport(auth, connectionContext)

    suspend fun handleFrame(frame: ByteArray): ByteArray? =
        when (val msg = wire.decode(frame)) {
            is WireMessage.Handshake -> handleHandshake(msg)
            is WireMessage.CommitFetch -> handleCommitFetch(msg)
            is WireMessage.CommitPush -> handleCommitPush(msg)
            else -> null
        }

    private suspend fun handleHandshake(msg: WireMessage.Handshake): ByteArray {
        if (msg.request.clientMode != WireClientMode.FULL_PEER) {
            return wire.encode(
                handshakeAck(
                    msg,
                    accepted = false,
                    heads = emptyMap(),
                    rejectionReason = "FULL_PEER mode required",
                ),
            )
        }
        val principal =
            try {
                connectionAuth.authenticateConnection()
            } catch (e: Throwable) {
                return wire.encode(
                    handshakeAck(
                        msg,
                        accepted = false,
                        heads = emptyMap(),
                        rejectionReason = connectionAuth.authFailureMessage(e),
                    ),
                )
            }
        try {
            connectionAuth.authorize(principal, AuthAction.PeerSync(cfg.namespaceId))
        } catch (e: Throwable) {
            return wire.encode(
                handshakeAck(
                    msg,
                    accepted = false,
                    heads = emptyMap(),
                    rejectionReason = connectionAuth.authFailureMessage(e),
                ),
            )
        }
        val heads = mapOf(cfg.namespaceId to dag.head().toHex())
        return wire.encode(handshakeAck(msg, accepted = true, heads = heads, rejectionReason = null))
    }

    private suspend fun handleCommitFetch(msg: WireMessage.CommitFetch): ByteArray {
        requireNamespace(msg.namespace)
        authorizePeerSync()
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

    private suspend fun handleCommitPush(msg: WireMessage.CommitPush): ByteArray {
        requireNamespace(msg.namespace)
        authorizePeerSync()
        // putCommit always stores, regardless of what happens to "main" below (component 39
        // spec §5: history must never be lost, only the branch-pointer decision is gated).
        for (commit in msg.commits) {
            if (dag.hasCommit(commit.hash)) continue
            try {
                dag.putCommit(commit, requireParents = true)
                cfg.materializeCommit?.invoke(commit)
            } catch (e: Exception) {
                throw PeerSyncException("failed to apply commit ${commit.hash.toHex()}", e)
            }
        }
        val incomingHead = msg.commits.lastOrNull()?.hash
        if (incomingHead != null) {
            val localHead = dag.head()
            val outcome = resolveDivergence(dag, storage, cfg.namespaceId, localHead, incomingHead, cfg.conflictPolicy)
            if (outcome is CommitPushOutcome.Conflict) {
                val conflictMsg =
                    WireMessage.ConflictReport(
                        WireHeader(
                            WireMessageType.CONFLICT_REPORT,
                            KDB_WIRE_PROTOCOL_VERSION,
                            msg.header.correlationId,
                            0,
                        ),
                        msg.namespace,
                        encodeConflictReport(outcome.report),
                    )
                return wire.encode(conflictMsg)
            }
            // NoOp/FastForwarded/Merged all succeed - fall through to the ordinary ack below,
            // same shape the (buggy) unconditional-setHead version always returned.
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

    private fun encodeConflictReport(report: ConflictReport): ByteArray {
        val items =
            report.conflicts.joinToString(",") { item ->
                val local = item.localDoc ?: "null"
                val incoming = item.incomingDoc ?: "null"
                """{"documentId":"${item.documentId}","operationType":"${item.operationType}","localDoc":$local,"incomingDoc":$incoming}"""
            }
        return (
            """{"transactionId":"${report.transactionId}","baseHash":"${report.baseHash}",""" +
                """"targetHash":"${report.targetHash}","conflicts":[$items]}"""
        ).encodeToByteArray()
    }

    private suspend fun authorizePeerSync() {
        connectionAuth.authorize(
            connectionAuth.connectionPrincipal,
            AuthAction.PeerSync(cfg.namespaceId),
        )
    }

    private fun requireNamespace(namespace: String) {
        require(namespace == cfg.namespaceId) {
            throw PeerSyncException("namespace mismatch: $namespace")
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
