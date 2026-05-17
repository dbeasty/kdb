package dev.kdb.server

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbTransaction
import dev.kdb.embed.toQueryResultJson
import dev.kdb.error.ConflictException
import dev.kdb.error.ConflictReport
import dev.kdb.error.DocumentLockedException
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.sql.SqlCell
import dev.kdb.sql.defaultSqlParser
import dev.kdb.sql.isDdlStatement
import dev.kdb.sql.isTransactionControlStatement
import dev.kdb.sql.SqlStatement
import dev.kdb.transaction.TransactionBuilder
import dev.kdb.transaction.TransactionResult
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.TransactionWireCodec
import dev.kdb.wire.WireCodec
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive

public class SqlWireHost(
    private val wire: WireCodec,
    private val server: KdbServerRuntime,
    private val defaultNamespace: String,
) {
    private val sessions = SessionManager(server)
    private var correlation = 1

    public suspend fun handleFrame(frame: ByteArray): ByteArray? {
        val message = wire.decode(frame)
        val reply =
            when (message) {
                is WireMessage.Handshake -> handleHandshake(message)
                is WireMessage.SessionBegin -> handleSessionBegin(message)
                is WireMessage.SqlExec -> handleSqlExec(message)
                is WireMessage.TxCommit -> handleTxCommit(message)
                is WireMessage.TxRollback -> handleTxRollback(message)
                is WireMessage.TransactionReplay -> handleTransactionReplay(message)
                else -> null
            }
        return reply?.let { wire.encode(it) }
    }

    private suspend fun handleHandshake(msg: WireMessage.Handshake): WireMessage.HandshakeAck {
        val accepted = msg.request.clientMode == dev.kdb.wire.WireClientMode.SQL_CLIENT
        val head = server.runtime.dag.head().toHex()
        if (accepted && msg.request.namespaces.isNotEmpty()) {
            val ns = msg.request.namespaces.first()
            sessions.begin(ns, ReadConsistency.READ_COMMITTED)
        } else if (accepted) {
            sessions.begin(defaultNamespace, ReadConsistency.READ_COMMITTED)
        }
        return WireMessage.HandshakeAck(
            header(msg.header.correlationId, WireMessageType.HANDSHAKE),
            dev.kdb.wire.HandshakeAckPayload(
                accepted = accepted,
                negotiatedEncoding = wire.encoding,
                protocolVersion = KDB_WIRE_PROTOCOL_VERSION,
                remoteHeads = mapOf(defaultNamespace to head),
                rejectionReason = if (accepted) null else "SQL_CLIENT mode required",
            ),
        )
    }

    private suspend fun handleSessionBegin(msg: WireMessage.SessionBegin): WireMessage.SessionBeginAck {
        val consistency = ReadConsistency.valueOf(msg.readConsistency)
        val session =
            sessions.begin(
                namespaceId = msg.namespace,
                readConsistency = consistency,
                baseVersionHex = msg.baseVersionHex,
                sessionId = msg.sessionId,
            )
        return WireMessage.SessionBeginAck(
            header(msg.header.correlationId, WireMessageType.SESSION_BEGIN_ACK),
            namespace = msg.namespace,
            sessionId = session.id.value,
            headHex = server.runtime.dag.head().toHex(),
            readConsistency = session.readConsistency.name,
        )
    }

    private suspend fun handleSqlExec(msg: WireMessage.SqlExec): WireMessage {
        val session =
            sessions.get(msg.sessionId)
                ?: return sqlError(msg, "unknown session: ${msg.sessionId}")
        val parsed = defaultSqlParser().parse(msg.sql.trim())
        if (isTransactionControlStatement(parsed)) {
            return handleTransactionControlSql(msg, session, parsed)
        }
        if (!session.autoCommit && isDdlStatement(parsed)) {
            return sqlError(msg, "DDL not allowed inside a transaction")
        }
        return try {
            val deferCommit = !session.autoCommit
            val result =
                server.runtime.hybrid.execute(
                    msg.sql,
                    HybridQueryRequest(
                        namespaceId = session.namespaceId,
                        schema = server.runtime.schema,
                        readConsistency = session.readConsistency,
                        readPin = session.readPin,
                        sessionCheckout = session.sessionCheckout,
                        writeSessionId = session.id.value,
                        documentLocks = server.documentLocks,
                        deferCommit = deferCommit,
                        transactionBase = if (deferCommit) session.baseVersion else null,
                        bufferOps =
                            if (deferCommit) {
                                { ops ->
                                    appendPendingOps(sessions.pendingBuilder(session), ops)
                                }
                            } else {
                                null
                            },
                    ),
                )
            val json = result.toQueryResultJson()
            WireMessage.SqlResult(
                header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                namespace = msg.namespace,
                sessionId = msg.sessionId,
                columns = json.columns,
                rows = json.rows.map { row -> row.map { cellToString(it) } },
                rowsAffected = result.result.rowsAffected,
                resolvedCommitHex = json.resolvedCommit,
                readOnly = json.readOnly,
                error = null,
            )
        } catch (e: Throwable) {
            sqlError(msg, e.message ?: e.toString())
        }
    }

    private suspend fun handleTransactionControlSql(
        msg: WireMessage.SqlExec,
        session: KdbSession,
        stmt: SqlStatement,
    ): WireMessage =
        when (stmt) {
            SqlStatement.BeginTransaction ->
                when {
                    session.pending != null ->
                        sqlError(msg, "transaction already in progress")
                    !session.autoCommit ->
                        sqlSuccess(
                            msg,
                            session,
                            resolvedCommitHex = session.baseVersion.toHex(),
                        )
                    else -> {
                        session.autoCommit = false
                        sqlSuccess(
                            msg,
                            session,
                            resolvedCommitHex = session.baseVersion.toHex(),
                        )
                    }
                }
            SqlStatement.Commit ->
                commitSession(
                    correlationId = msg.header.correlationId,
                    namespace = msg.namespace,
                    session = session,
                ) { err -> sqlError(msg, err) }
            SqlStatement.Rollback -> {
                rollbackSession(session)
                sqlSuccess(
                    msg,
                    session,
                    resolvedCommitHex = server.runtime.dag.head().toHex(),
                )
            }
            else -> sqlError(msg, "unsupported transaction statement")
        }

    private suspend fun handleTxCommit(msg: WireMessage.TxCommit): WireMessage {
        val session =
            sessions.get(msg.sessionId)
                ?: return sqlError(msg, "unknown session: ${msg.sessionId}")
        if (msg.transactionBytes.isNotEmpty()) {
            return commitEncodedTransaction(msg, session)
        }
        return commitSession(
            correlationId = msg.header.correlationId,
            namespace = msg.namespace,
            session = session,
        ) { err -> sqlError(msg, err) }
    }

    private suspend fun commitEncodedTransaction(
        msg: WireMessage.TxCommit,
        session: KdbSession,
    ): WireMessage =
        try {
            val tx = TransactionWireCodec.decode(msg.transactionBytes)
            val effective =
                if (tx.operations.isEmpty() && session.pending != null) {
                    session.pending!!.build()
                } else {
                    tx
                }
            val commit =
                server.commit(
                    session.namespaceId,
                    effective,
                    sessionId = session.id.value,
                )
            finishCommittedSession(session, commit.hash, effective.operations.size)
            WireMessage.SqlResult(
                header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                namespace = msg.namespace,
                sessionId = msg.sessionId,
                columns = emptyList(),
                rows = emptyList(),
                rowsAffected = effective.operations.size,
                resolvedCommitHex = commit.hash.toHex(),
                readOnly = false,
            )
        } catch (e: ConflictException) {
            abortSessionAfterFailedCommit(session)
            WireMessage.ConflictReport(
                header(msg.header.correlationId, WireMessageType.CONFLICT_REPORT),
                namespace = msg.namespace,
                reportBytes = encodeConflictReport(e.report),
            )
        } catch (e: DocumentLockedException) {
            abortSessionAfterFailedCommit(session)
            sqlError(msg, e.message ?: "document locked")
        } catch (e: Throwable) {
            abortSessionAfterFailedCommit(session)
            sqlError(msg, e.message ?: e.toString())
        }

    private suspend fun commitSession(
        correlationId: Int,
        namespace: String,
        session: KdbSession,
        onError: (String) -> WireMessage,
    ): WireMessage =
        try {
            val effective =
                session.pending?.build()
                    ?: return onError("no pending transaction to commit")
            val commit =
                server.commit(
                    session.namespaceId,
                    effective,
                    sessionId = session.id.value,
                )
            finishCommittedSession(session, commit.hash, effective.operations.size)
            WireMessage.SqlResult(
                header(correlationId, WireMessageType.SQL_RESULT),
                namespace = namespace,
                sessionId = session.id.value,
                columns = emptyList(),
                rows = emptyList(),
                rowsAffected = effective.operations.size,
                resolvedCommitHex = commit.hash.toHex(),
                readOnly = false,
            )
        } catch (e: ConflictException) {
            abortSessionAfterFailedCommit(session)
            WireMessage.ConflictReport(
                header(correlationId, WireMessageType.CONFLICT_REPORT),
                namespace = namespace,
                reportBytes = encodeConflictReport(e.report),
            )
        } catch (e: DocumentLockedException) {
            abortSessionAfterFailedCommit(session)
            onError(e.message ?: "document locked")
        } catch (e: Throwable) {
            abortSessionAfterFailedCommit(session)
            onError(e.message ?: e.toString())
        }

    private suspend fun finishCommittedSession(
        session: KdbSession,
        commitHash: KdbHash,
        @Suppress("UNUSED_PARAMETER") rowsAffected: Int,
    ) {
        sessions.clearPending(session)
        server.documentLocks.releaseAll(session.id.value)
        session.baseVersion = commitHash
        if (session.readConsistency == ReadConsistency.SNAPSHOT) {
            session.readPin = commitHash
        }
    }

    private suspend fun abortSessionAfterFailedCommit(session: KdbSession) {
        server.documentLocks.releaseAll(session.id.value)
        sessions.clearPending(session)
    }

    private suspend fun rollbackSession(session: KdbSession) {
        server.documentLocks.releaseAll(session.id.value)
        sessions.clearPending(session)
    }

    private suspend fun handleTxRollback(msg: WireMessage.TxRollback): WireMessage.SqlResult {
        val session = sessions.get(msg.sessionId)
        if (session != null) {
            rollbackSession(session)
        }
        return WireMessage.SqlResult(
            header(msg.header.correlationId, WireMessageType.SQL_RESULT),
            namespace = msg.namespace,
            sessionId = msg.sessionId,
            columns = emptyList(),
            rows = emptyList(),
            rowsAffected = 0,
            resolvedCommitHex = server.runtime.dag.head().toHex(),
            readOnly = false,
        )
    }

    private fun sqlSuccess(
        msg: WireMessage.SqlExec,
        session: KdbSession,
        resolvedCommitHex: String,
        rowsAffected: Int = 0,
    ): WireMessage.SqlResult =
        WireMessage.SqlResult(
            header(msg.header.correlationId, WireMessageType.SQL_RESULT),
            namespace = msg.namespace,
            sessionId = session.id.value,
            columns = emptyList(),
            rows = emptyList(),
            rowsAffected = rowsAffected,
            resolvedCommitHex = resolvedCommitHex,
            readOnly = false,
        )

    private suspend fun handleTransactionReplay(msg: WireMessage.TransactionReplay): WireMessage {
        val tx = TransactionWireCodec.decode(msg.transactionBytes)
        val replayTarget = server.runtime.dag.head()
        return when (val result = server.replay(msg.namespace, tx, replayTarget)) {
            is TransactionResult.Success ->
                WireMessage.SqlResult(
                    header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                    namespace = msg.namespace,
                    sessionId = "",
                    columns = emptyList(),
                    rows = emptyList(),
                    rowsAffected = tx.operations.size,
                    resolvedCommitHex = result.commit.hash.toHex(),
                    readOnly = false,
                )
            is TransactionResult.Conflict ->
                WireMessage.ConflictReport(
                    header(msg.header.correlationId, WireMessageType.CONFLICT_REPORT),
                    namespace = msg.namespace,
                    reportBytes = encodeConflictReport(result.report),
                )
            is TransactionResult.SchemaError ->
                WireMessage.SqlResult(
                    header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                    namespace = msg.namespace,
                    sessionId = "",
                    columns = emptyList(),
                    rows = emptyList(),
                    rowsAffected = 0,
                    resolvedCommitHex = replayTarget.toHex(),
                    readOnly = false,
                    error = "schema rejection",
                )
        }
    }

    private fun sqlError(
        msg: WireMessage.SqlExec,
        error: String,
    ): WireMessage.SqlResult =
        WireMessage.SqlResult(
            header(msg.header.correlationId, WireMessageType.SQL_RESULT),
            namespace = msg.namespace,
            sessionId = msg.sessionId,
            columns = emptyList(),
            rows = emptyList(),
            rowsAffected = 0,
            resolvedCommitHex = "",
            readOnly = true,
            error = error,
        )

    private fun sqlError(
        msg: WireMessage.TxCommit,
        error: String,
    ): WireMessage.SqlResult =
        WireMessage.SqlResult(
            header(msg.header.correlationId, WireMessageType.SQL_RESULT),
            namespace = msg.namespace,
            sessionId = msg.sessionId,
            columns = emptyList(),
            rows = emptyList(),
            rowsAffected = 0,
            resolvedCommitHex = "",
            readOnly = false,
            error = error,
        )

    private fun header(
        correlationId: Int,
        type: WireMessageType,
    ): WireHeader = WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, correlationId, 0)

    private fun cellToString(cell: kotlinx.serialization.json.JsonElement): String =
        when {
            cell is JsonPrimitive && cell.isString -> cell.content
            else -> cell.toString()
        }

    private fun encodeConflictReport(report: ConflictReport): ByteArray =
        (
            """{"transactionId":"${report.transactionId}","baseHash":"${report.baseHash}","targetHash":"${report.targetHash}"}"""
        ).encodeToByteArray()
}
