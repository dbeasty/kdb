package dev.kdb.wire

import dev.kdb.codec.KdbHash
import dev.kdb.compaction.CompactionIntent
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.index.IndexHint

internal fun WireMessage.toEnvelope(): WirePayloadEnvelope =
    when (this) {
        is WireMessage.Handshake ->
            WirePayloadEnvelope(
                kind = "handshake",
                handshake =
                    HandshakeDto(
                        nodeId = request.nodeId,
                        namespaces = request.namespaces,
                        localHeads = request.localHeads,
                        supportsZstd = request.capabilities.supportsZstd,
                        supportsIndexHints = request.capabilities.supportsIndexHints,
                        supportsDirectDeltaIngest = request.capabilities.supportsDirectDeltaIngest,
                        maxFrameBytes = request.capabilities.maxFrameBytes,
                        preferredEncodings = request.preferredEncodings.map { it.name },
                        clientMode = request.clientMode.name,
                        protocolVersion = request.protocolVersion,
                    ),
            )

        is WireMessage.HandshakeAck ->
            WirePayloadEnvelope(
                kind = "handshakeAck",
                handshakeAck =
                    HandshakeAckDto(
                        accepted = response.accepted,
                        negotiatedEncoding = response.negotiatedEncoding.name,
                        protocolVersion = response.protocolVersion,
                        remoteHeads = response.remoteHeads,
                        rejectionReason = response.rejectionReason,
                    ),
            )

        is WireMessage.DeltaCommit ->
            WirePayloadEnvelope(
                kind = "deltaCommit",
                deltaCommit =
                    DeltaCommitDto(
                        namespace = payload.namespace,
                        commitHashHex = payload.commitHash.toHex(),
                        parentHashHex = payload.parentHash.toHex(),
                        timestampMicros = payload.timestampMicros,
                        operations = payload.operations.map { it.toOpDto() },
                        indexHints = payload.indexHints.map { it.toHintDto() },
                        schemaDeltaBytes = payload.schemaDeltaBytes,
                    ),
            )

        is WireMessage.CommitFetch ->
            WirePayloadEnvelope(
                kind = "commitFetch",
                commitFetch =
                    CommitFetchDto(
                        namespace = namespace,
                        sinceHashHex = sinceHash?.toHex(),
                        maxCommits = maxCommits,
                    ),
            )

        is WireMessage.CommitPush ->
            WirePayloadEnvelope(
                kind = "commitPush",
                commitPush =
                    CommitPushDto(
                        namespace = namespace,
                        commitsPayload = CommitPushCodec.encodeCommits(commits),
                    ),
            )

        is WireMessage.DagDiff ->
            WirePayloadEnvelope(
                kind = "dagDiff",
                dagDiff =
                    DagDiffDto(
                        namespace = namespace,
                        localHeadHex = localHead.toHex(),
                        remoteHeadHex = remoteHead.toHex(),
                    ),
            )

        is WireMessage.TransactionReplay ->
            WirePayloadEnvelope(
                kind = "transactionReplay",
                transactionReplay =
                    TransactionReplayDto(
                        namespace = namespace,
                        baseVersionHex = baseVersion.toHex(),
                        transactionBytes = transactionBytes,
                    ),
            )

        is WireMessage.ConflictReport ->
            WirePayloadEnvelope(
                kind = "conflictReport",
                conflictReport =
                    ConflictReportDto(
                        namespace = namespace,
                        reportBytes = reportBytes,
                    ),
            )

        is WireMessage.CompactionNotice ->
            WirePayloadEnvelope(
                kind = "compactionNotice",
                compactionNotice =
                    CompactionNoticeDto(
                        namespaceId = intent.namespaceId,
                        boundaryHex = intent.boundary.toHex(),
                        issuedAtMillis = intent.issuedAtMillis,
                    ),
            )

        is WireMessage.IceArchiveNotice ->
            WirePayloadEnvelope(
                kind = "iceArchiveNotice",
                iceArchiveNotice =
                    IceArchiveNoticeDto(
                        namespace = namespace,
                        originalHashHex = originalHash.toHex(),
                        archiveLocation = archiveLocation,
                        bundleHashHex = bundleHash.toHex(),
                    ),
            )

        is WireMessage.SnapshotRequest ->
            WirePayloadEnvelope(
                kind = "snapshotRequest",
                snapshotRequest =
                    SnapshotRequestDto(
                        namespace = namespace,
                        anchorHashHex = anchorHash?.toHex(),
                    ),
            )

        is WireMessage.SnapshotResponse ->
            WirePayloadEnvelope(
                kind = "snapshotResponse",
                snapshotResponse =
                    SnapshotResponseDto(
                        namespace = namespace,
                        anchorHashHex = anchorHash.toHex(),
                        snapshotBytes = snapshotBytes,
                        compressed = compressed,
                    ),
            )

        is WireMessage.PositionAck ->
            WirePayloadEnvelope(
                kind = "positionAck",
                positionAck =
                    PositionAckDto(
                        namespace = namespace,
                        commitHashHex = commitHash.toHex(),
                    ),
            )

        is WireMessage.SchemaPush ->
            WirePayloadEnvelope(
                kind = "schemaPush",
                schemaPush =
                    SchemaPushDto(
                        namespace = namespace,
                        schemaBytes = schemaBytes,
                        revision = revision,
                    ),
            )

        is WireMessage.SessionBegin ->
            WirePayloadEnvelope(
                kind = "sessionBegin",
                sessionBegin =
                    SessionBeginDto(
                        namespace = namespace,
                        sessionId = sessionId,
                        readConsistency = readConsistency,
                        baseVersionHex = baseVersionHex,
                    ),
            )

        is WireMessage.SessionBeginAck ->
            WirePayloadEnvelope(
                kind = "sessionBeginAck",
                sessionBeginAck =
                    SessionBeginAckDto(
                        namespace = namespace,
                        sessionId = sessionId,
                        headHex = headHex,
                        readConsistency = readConsistency,
                    ),
            )

        is WireMessage.SqlExec ->
            WirePayloadEnvelope(
                kind = "sqlExec",
                sqlExec =
                    SqlExecDto(
                        namespace = namespace,
                        sessionId = sessionId,
                        sql = sql,
                        parametersJson = parametersJson,
                    ),
            )

        is WireMessage.SqlResult ->
            WirePayloadEnvelope(
                kind = "sqlResult",
                sqlResult =
                    SqlResultDto(
                        namespace = namespace,
                        sessionId = sessionId,
                        columns = columns,
                        rows = rows,
                        rowsAffected = rowsAffected,
                        resolvedCommitHex = resolvedCommitHex,
                        readOnly = readOnly,
                        error = error,
                    ),
            )

        is WireMessage.TxCommit ->
            WirePayloadEnvelope(
                kind = "txCommit",
                txCommit =
                    TxCommitDto(
                        namespace = namespace,
                        sessionId = sessionId,
                        transactionBytes = transactionBytes,
                    ),
            )

        is WireMessage.TxRollback ->
            WirePayloadEnvelope(
                kind = "txRollback",
                txRollback =
                    TxRollbackDto(
                        namespace = namespace,
                        sessionId = sessionId,
                    ),
            )
    }

internal fun WirePayloadEnvelope.toMessage(header: WireHeader): WireMessage =
    when (kind) {
        "handshake" -> {
            val h =
                handshake ?: throw WireDecodeException("missing handshake body")
            WireMessage.Handshake(
                header,
                HandshakePayload(
                    nodeId = h.nodeId,
                    namespaces = h.namespaces,
                    localHeads = h.localHeads,
                    capabilities =
                        WireCapabilitySet(
                            supportsZstd = h.supportsZstd,
                            supportsIndexHints = h.supportsIndexHints,
                            supportsDirectDeltaIngest = h.supportsDirectDeltaIngest,
                            maxFrameBytes = h.maxFrameBytes,
                        ),
                    preferredEncodings = h.preferredEncodings.map { PayloadEncoding.valueOf(it) },
                    clientMode = WireClientMode.valueOf(h.clientMode),
                    protocolVersion = h.protocolVersion,
                ),
            )
        }

        "handshakeAck" -> {
            val a = handshakeAck ?: throw WireDecodeException("missing handshakeAck body")
            WireMessage.HandshakeAck(
                header,
                HandshakeAckPayload(
                    accepted = a.accepted,
                    negotiatedEncoding = PayloadEncoding.valueOf(a.negotiatedEncoding),
                    protocolVersion = a.protocolVersion,
                    remoteHeads = a.remoteHeads,
                    rejectionReason = a.rejectionReason,
                ),
            )
        }

        "deltaCommit" -> {
            val d = deltaCommit ?: throw WireDecodeException("missing deltaCommit body")
            WireMessage.DeltaCommit(
                header,
                DeltaCommitPayload(
                    namespace = d.namespace,
                    commitHash = KdbHash.fromHex(d.commitHashHex),
                    parentHash = KdbHash.fromHex(d.parentHashHex),
                    timestampMicros = d.timestampMicros,
                    operations = d.operations.map { it.toKdbOp() },
                    indexHints = d.indexHints.map { it.toIndexHint() },
                    schemaDeltaBytes = d.schemaDeltaBytes,
                ),
            )
        }

        "commitFetch" -> {
            val c = commitFetch ?: throw WireDecodeException("missing commitFetch body")
            WireMessage.CommitFetch(
                header,
                c.namespace,
                c.sinceHashHex?.let { KdbHash.fromHex(it) },
                c.maxCommits,
            )
        }

        "commitPush" -> {
            val c = commitPush ?: throw WireDecodeException("missing commitPush body")
            WireMessage.CommitPush(header, c.namespace, CommitPushCodec.decodeCommits(c.commitsPayload))
        }

        "dagDiff" -> {
            val d = dagDiff ?: throw WireDecodeException("missing dagDiff body")
            WireMessage.DagDiff(
                header,
                d.namespace,
                KdbHash.fromHex(d.localHeadHex),
                KdbHash.fromHex(d.remoteHeadHex),
            )
        }

        "transactionReplay" -> {
            val t = transactionReplay ?: throw WireDecodeException("missing transactionReplay body")
            WireMessage.TransactionReplay(
                header,
                t.namespace,
                KdbHash.fromHex(t.baseVersionHex),
                t.transactionBytes,
            )
        }

        "conflictReport" -> {
            val c = conflictReport ?: throw WireDecodeException("missing conflictReport body")
            WireMessage.ConflictReport(header, c.namespace, c.reportBytes)
        }

        "compactionNotice" -> {
            val c = compactionNotice ?: throw WireDecodeException("missing compactionNotice body")
            WireMessage.CompactionNotice(
                header,
                CompactionIntent(
                    namespaceId = c.namespaceId,
                    boundary = KdbHash.fromHex(c.boundaryHex),
                    issuedAtMillis = c.issuedAtMillis,
                ),
            )
        }

        "iceArchiveNotice" -> {
            val i = iceArchiveNotice ?: throw WireDecodeException("missing iceArchiveNotice body")
            WireMessage.IceArchiveNotice(
                header,
                i.namespace,
                KdbHash.fromHex(i.originalHashHex),
                i.archiveLocation,
                KdbHash.fromHex(i.bundleHashHex),
            )
        }

        "snapshotRequest" -> {
            val s = snapshotRequest ?: throw WireDecodeException("missing snapshotRequest body")
            WireMessage.SnapshotRequest(
                header,
                s.namespace,
                s.anchorHashHex?.let { KdbHash.fromHex(it) },
            )
        }

        "snapshotResponse" -> {
            val s = snapshotResponse ?: throw WireDecodeException("missing snapshotResponse body")
            WireMessage.SnapshotResponse(
                header,
                s.namespace,
                KdbHash.fromHex(s.anchorHashHex),
                s.snapshotBytes,
                s.compressed,
            )
        }

        "positionAck" -> {
            val p = positionAck ?: throw WireDecodeException("missing positionAck body")
            WireMessage.PositionAck(header, p.namespace, KdbHash.fromHex(p.commitHashHex))
        }

        "schemaPush" -> {
            val s = schemaPush ?: throw WireDecodeException("missing schemaPush body")
            WireMessage.SchemaPush(header, s.namespace, s.schemaBytes, s.revision)
        }

        "sessionBegin" -> {
            val s = sessionBegin ?: throw WireDecodeException("missing sessionBegin body")
            WireMessage.SessionBegin(
                header,
                s.namespace,
                s.sessionId,
                s.readConsistency,
                s.baseVersionHex,
            )
        }

        "sessionBeginAck" -> {
            val s = sessionBeginAck ?: throw WireDecodeException("missing sessionBeginAck body")
            WireMessage.SessionBeginAck(
                header,
                s.namespace,
                s.sessionId,
                s.headHex,
                s.readConsistency,
            )
        }

        "sqlExec" -> {
            val s = sqlExec ?: throw WireDecodeException("missing sqlExec body")
            WireMessage.SqlExec(header, s.namespace, s.sessionId, s.sql, s.parametersJson)
        }

        "sqlResult" -> {
            val s = sqlResult ?: throw WireDecodeException("missing sqlResult body")
            WireMessage.SqlResult(
                header,
                s.namespace,
                s.sessionId,
                s.columns,
                s.rows,
                s.rowsAffected,
                s.resolvedCommitHex,
                s.readOnly,
                s.error,
            )
        }

        "txCommit" -> {
            val c = txCommit ?: throw WireDecodeException("missing txCommit body")
            WireMessage.TxCommit(header, c.namespace, c.sessionId, c.transactionBytes)
        }

        "txRollback" -> {
            val r = txRollback ?: throw WireDecodeException("missing txRollback body")
            WireMessage.TxRollback(header, r.namespace, r.sessionId)
        }

        else -> throw WireDecodeException("unknown payload kind: $kind")
    }
