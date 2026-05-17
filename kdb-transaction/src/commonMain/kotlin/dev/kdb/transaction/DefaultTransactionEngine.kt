package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.TraversalEntry
import dev.kdb.document.*
import dev.kdb.error.*
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaEngine
import dev.kdb.schema.isNone
import dev.kdb.storage.StorageAdapter

internal class DefaultTransactionEngine(
    override val conflictPolicy: ConflictPolicy,
    override val customResolver: ConflictResolver?,
) : TransactionEngine {

    override suspend fun commit(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
        targetHead: KdbHash?,
        message: String,
    ): TransactionResult {
        val head = targetHead ?: dag.head()
        if (!dag.hasCommit(transaction.baseVersion)) {
            throw TransactionBaseNotFoundException(
                "missing base commit",
                transaction.id,
                transaction.baseVersion,
            )
        }
        findExistingCommit(transaction, dag, head, listOf(head))?.let {
            return TransactionResult.Success(it, it.documentTreeHash)
        }
        return finalizeTransaction(
            transaction = transaction,
            dag = dag,
            storage = storage,
            incomingSchema = schema,
            anchorCommit = head,
            baseDocTreeHash = dag.getCommitOrThrow(transaction.baseVersion).documentTreeHash,
            baselineDocTreeHash = dag.getCommitOrThrow(transaction.baseVersion).documentTreeHash,
            targetDocTreeHash = dag.getCommitOrThrow(head).documentTreeHash,
            message = message,
        )
    }

    override suspend fun replay(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
        replayTarget: KdbHash,
        message: String,
    ): TransactionResult {
        require(dag.hasCommit(replayTarget))
        findExistingCommit(transaction, dag, replayTarget, listOf(replayTarget))?.let {
            return TransactionResult.Success(it, it.documentTreeHash)
        }
        val baselineTree = dag.getCommitOrThrow(replayTarget).documentTreeHash
        return finalizeTransaction(
            transaction = transaction,
            dag = dag,
            storage = storage,
            incomingSchema = schema,
            anchorCommit = replayTarget,
            baseDocTreeHash = baselineTree,
            baselineDocTreeHash = baselineTree,
            targetDocTreeHash = baselineTree,
            message = message,
        )
    }

    override suspend fun merge(
        primaryHead: KdbHash,
        mergedHead: KdbHash,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
        message: String,
    ): TransactionResult {
        val ancestor =
            dag.commonAncestor(primaryHead, mergedHead)
                ?: throw MergeBaseNotFoundException(
                    "branches disjoint",
                    primaryHead,
                    mergedHead,
                )
        val branchCommits = dag.commitsSince(mergedHead, setOf(ancestor)).toSet()
        val ordered = topoSort(dag, branchCommits.ifEmpty { setOf(mergedHead) })

        var head = primaryHead
        for (hash in ordered) {
            val mc = dag.getCommitOrThrow(hash)
            if (mc.operations.isEmpty()) continue
            val step =
                KdbTransaction(
                    id = mc.transactionId,
                    baseVersion = head,
                    operations = mc.operations,
                    timestamp = mc.timestamp,
                    authorNodeId = mc.authorNodeId,
                )
            when (
                val res =
                    replay(
                        step,
                        dag,
                        storage,
                        schema,
                        replayTarget = head,
                        message = "merge-step:${mc.hash.toHex()}",
                    )
            ) {
                is TransactionResult.Success -> head = res.commit.hash
                is TransactionResult.Conflict -> return res
                is TransactionResult.SchemaError -> return res
            }
        }

        val mergedTree =
            dag.getDocumentTreeOrThrow(dag.getCommitOrThrow(head).documentTreeHash)
        val tipSchemaHash = dag.getCommitOrThrow(head).schemaHash
        val mergeMarker =
            KdbTransaction(
                id = KdbUuid.random(),
                baseVersion = primaryHead,
                operations = emptyList(),
                timestamp = KdbTimestamp.now(),
                authorNodeId = dag.getCommitOrThrow(mergedHead).authorNodeId,
            )
        val mergeCommit =
            dag.appendMergeCommit(
                mergeMarker,
                primaryHead,
                mergedHead,
                mergedTree,
                tipSchemaHash,
                message,
            )

        return TransactionResult.Success(
            mergeCommit,
            mergeCommit.documentTreeHash,
        )
    }

    override suspend fun validate(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
    ): List<OperationViolation> {
        require(dag.hasCommit(transaction.baseVersion))
        val baseline = dag.getCommitOrThrow(transaction.baseVersion).documentTreeHash
        return runSchemaPhase(transaction, storage, dag.namespaceId, schema, baseline).violations
    }

    /**
     * [baseDocTreeHash] — base snapshot for concurrency checks ([transaction.baseVersion] vs replay baseline).
     * [baselineDocTreeHash] — document tree passed to merges when preparing JSON.
     * [targetDocTreeHash] — live tree at [anchorCommit] for conflict detection.
     */
    private suspend fun finalizeTransaction(
        transaction: KdbTransaction,
        dag: CommitDag,
        storage: StorageAdapter,
        incomingSchema: KdbSchema,
        anchorCommit: KdbHash,
        baseDocTreeHash: KdbHash,
        baselineDocTreeHash: KdbHash,
        targetDocTreeHash: KdbHash,
        message: String,
    ): TransactionResult {
        val fileViolations = preflightFileWrites(transaction, storage)
        if (fileViolations.isNotEmpty()) {
            return TransactionResult.SchemaError(fileViolations)
        }

        val schemaFrame =
            runSchemaPhase(
                transaction,
                storage,
                dag.namespaceId,
                incomingSchema,
                baselineDocTreeHash,
            )
        if (schemaFrame.violations.isNotEmpty()) {
            return TransactionResult.SchemaError(schemaFrame.violations)
        }

        val writes = schemaFrame.writesByOpIndex.toMutableMap()

        val conflicts =
            when (conflictPolicy) {
                ConflictPolicy.APPEND_ONLY, ConflictPolicy.LAST_WRITE -> emptyList()
                ConflictPolicy.STRICT, ConflictPolicy.CUSTOM ->
                    detectConflicts(
                        transaction,
                        dag.namespaceId,
                        storage,
                        baseDocTreeHash,
                        targetDocTreeHash,
                        writes,
                    )
            }

        if (conflicts.isNotEmpty() && conflictPolicy == ConflictPolicy.STRICT) {
            return TransactionResult.Conflict(toReport(transaction, anchorCommit, conflicts), conflicts)
        }

        if (conflicts.isNotEmpty() && conflictPolicy == ConflictPolicy.CUSTOM) {
            val resolver =
                customResolver
                    ?: return TransactionResult.Conflict(toReport(transaction, anchorCommit, conflicts), conflicts)
            for (c in conflicts) {
                if (c.op !is KdbOp.Write) {
                    return TransactionResult.Conflict(toReport(transaction, anchorCommit, conflicts), conflicts)
                }
                val docId = (c.op as KdbOp.Write).docId
                val resolved =
                    resolver.resolve(
                        DocumentConflict(
                            docId = docId,
                            operationType = c.type,
                            existingDoc = c.existingDoc,
                            incomingDoc = writes[c.opIndex],
                            baseDoc = c.baseDoc,
                        ),
                    ) ?: return TransactionResult.Conflict(toReport(transaction, anchorCommit, conflicts), conflicts)

                when (val vr = SchemaEngine.validate(resolved, schemaFrame.rollingSchema)) {
                    is KdbResult.Success -> writes[c.opIndex] = vr.value
                    is KdbResult.Failure ->
                        return TransactionResult.SchemaError(
                            listOf(
                                OperationViolation(
                                    c.opIndex,
                                    c.op,
                                    (vr.exception as? SchemaViolationException)?.violations
                                        ?: listOf(
                                            FieldViolation(
                                                "",
                                                ViolationType.CUSTOM_CONSTRAINT,
                                                vr.exception.message ?: "schema rejection",
                                            ),
                                        ),
                                ),
                            ),
                        )
                }
            }
        }

        for ((idx, op) in transaction.operations.withIndex()) {
            when (op) {
                is KdbOp.Write ->
                    writes[idx]?.let { storage.putDocument(dag.namespaceId, it) }

                is KdbOp.Delete -> storage.deleteDocument(dag.namespaceId, op.docId)
                else -> Unit
            }
        }

        val newTree =
            storage.commitTree(dag.namespaceId, dag.getCommitOrThrow(anchorCommit).documentTreeHash)

        val schemaHashWire =
            if (schemaFrame.rollingSchema.isNone) {
                null
            } else {
                schemaFrame.rollingSchema.schemaHash
            }

        val commit =
            dag.appendCommit(
                transaction,
                anchorCommit,
                newTree,
                schemaHashWire,
                message,
            )

        return TransactionResult.Success(commit, commit.documentTreeHash)
    }

    private suspend fun detectConflicts(
        transaction: KdbTransaction,
        namespaceId: String,
        storage: StorageAdapter,
        baseTreeHash: KdbHash,
        targetTreeHash: KdbHash,
        projectedWrites: Map<Int, KdbDocument>,
    ): List<OperationConflict> {
        val out = mutableListOf<OperationConflict>()
        for ((index, op) in transaction.operations.withIndex()) {
            when (op) {
                is KdbOp.Write -> {
                    val baseDoc = storage.getDocument(namespaceId, op.docId, baseTreeHash)
                    val existingDoc = storage.getDocument(namespaceId, op.docId, targetTreeHash)
                    if (baseDoc?.contentHash == existingDoc?.contentHash) {
                        continue
                    }
                    if (baseDoc == null && existingDoc == null) {
                        continue
                    }

                    val type =
                        when {
                            existingDoc != null && baseDoc == null -> ConflictOperationType.DELETE_WRITE
                            existingDoc == null && baseDoc != null -> ConflictOperationType.WRITE_DELETE
                            else -> ConflictOperationType.CONCURRENT_WRITE
                        }

                    out +=
                        OperationConflict(
                            opIndex = index,
                            op = op,
                            type = type,
                            existingDoc = existingDoc,
                            incomingDoc = projectedWrites[index],
                            baseDoc = baseDoc,
                        )
                }

                is KdbOp.Delete -> {
                    val baseDoc = storage.getDocument(namespaceId, op.docId, baseTreeHash)
                    val existingDoc = storage.getDocument(namespaceId, op.docId, targetTreeHash)
                    if (baseDoc == null && existingDoc == null) {
                        continue
                    }
                    if (baseDoc?.contentHash == existingDoc?.contentHash) {
                        continue
                    }

                    val type =
                        if (existingDoc != null) {
                            ConflictOperationType.CONCURRENT_WRITE
                        } else {
                            ConflictOperationType.DELETE_WRITE
                        }

                    out +=
                        OperationConflict(
                            opIndex = index,
                            op = op,
                            type = type,
                            existingDoc = existingDoc,
                            incomingDoc = null,
                            baseDoc = baseDoc,
                        )
                }

                else -> Unit
            }
        }
        return out
    }

    private suspend fun preflightFileWrites(
        transaction: KdbTransaction,
        storage: StorageAdapter,
    ): List<OperationViolation> {
        val violations = mutableListOf<OperationViolation>()
        for ((index, op) in transaction.operations.withIndex()) {
            if (op !is KdbOp.FileWrite) continue
            if (storage.readBlob(op.blobHash) == null) {
                violations +=
                    OperationViolation(
                        opIndex = index,
                        op = op,
                        violations =
                            listOf(
                                FieldViolation(
                                    fieldName = op.path,
                                    violationType = ViolationType.CUSTOM_CONSTRAINT,
                                    detail = "blob not found: ${op.blobHash.toHex()}",
                                ),
                            ),
                    )
            }
        }
        return violations
    }

    private suspend fun runSchemaPhase(
        transaction: KdbTransaction,
        storage: StorageAdapter,
        namespaceId: String,
        initialSchema: KdbSchema,
        baselineTreeHash: KdbHash,
    ): MutableSchemaOutcome {
        var rolling = initialSchema
        val violations = mutableListOf<OperationViolation>()
        val writes = mutableMapOf<Int, KdbDocument>()

        for ((index, op) in transaction.operations.withIndex()) {
            when (op) {
                is KdbOp.Write -> {
                    val baseDoc = storage.getDocument(namespaceId, op.docId, baselineTreeHash)
                    val candidate =
                        try {
                            if (baseDoc != null) {
                                baseDoc.merge(op.patch)
                            } else {
                                KdbDocument.fromJson(op.docId, op.patch)
                            }
                        } catch (_: Throwable) {
                            violations +=
                                OperationViolation(
                                    index,
                                    op,
                                    listOf(
                                        FieldViolation(
                                            fieldName = op.docId.toString(),
                                            violationType = ViolationType.CUSTOM_CONSTRAINT,
                                            detail = "invalid write payload",
                                        ),
                                    ),
                                )
                            continue
                        }
                    when (val vr = SchemaEngine.validate(candidate, rolling)) {
                        is KdbResult.Success -> writes[index] = vr.value
                        is KdbResult.Failure ->
                            violations +=
                                OperationViolation(
                                    index,
                                    op,
                                    (vr.exception as SchemaViolationException).violations,
                                )
                    }
                }

                is KdbOp.SchemaMigration -> {
                    val mig =
                        try {
                            SchemaMigrationCodec.decode(op.migrationPayload)
                        } catch (ex: Throwable) {
                            violations +=
                                OperationViolation(
                                    index,
                                    op,
                                    listOf(
                                        FieldViolation(
                                            "migration",
                                            ViolationType.CUSTOM_CONSTRAINT,
                                            ex.message ?: "migration decode failure",
                                        ),
                                    ),
                                )
                            continue
                        }
                    when (val mr = SchemaEngine.applyMigration(rolling, mig)) {
                        is KdbResult.Success -> rolling = mr.value
                        is KdbResult.Failure ->
                            violations +=
                                OperationViolation(
                                    index,
                                    op,
                                    listOf(
                                        FieldViolation(
                                            "",
                                            ViolationType.CUSTOM_CONSTRAINT,
                                            (mr.exception as SchemaMigrationException).message ?: "migration rejected",
                                        ),
                                    ),
                                )
                    }
                }

                else -> Unit
            }
        }
        return MutableSchemaOutcome(rolling, violations, writes)
    }

    private suspend fun findExistingCommit(
        transaction: KdbTransaction,
        dag: CommitDag,
        anchorCommit: KdbHash,
        parents: List<KdbHash>,
    ): KdbCommit? {
        for (entry in dag.walk(from = anchorCommit, limit = 8192)) {
            if (entry !is TraversalEntry.Full) continue
            val commit = entry.commit
            if (commit.transactionId == transaction.id && commit.parentHashes == parents) {
                return commit
            }
        }
        return null
    }

    private fun toReport(
        transaction: KdbTransaction,
        anchor: KdbHash,
        conflicts: List<OperationConflict>,
    ): ConflictReport =
        ConflictReport(
            transactionId = transaction.id.toString(),
            baseHash = transaction.baseVersion.toHex(),
            targetHash = anchor.toHex(),
            conflicts =
                conflicts.map { c ->
                    ConflictItem(
                        documentId =
                            when (val op = c.op) {
                                is KdbOp.Write -> op.docId.toString()
                                is KdbOp.Delete -> op.docId.toString()
                                else -> ""
                            },
                        operationType = c.type,
                        localDoc = c.existingDoc?.json,
                        incomingDoc =
                            when {
                                c.incomingDoc != null -> c.incomingDoc.json
                                else -> null
                            },
                    )
                },
        )

    private data class MutableSchemaOutcome(
        val rollingSchema: KdbSchema,
        val violations: MutableList<OperationViolation>,
        val writesByOpIndex: MutableMap<Int, KdbDocument>,
    )

    private suspend fun topoSort(
        dag: CommitDag,
        hashes: Collection<KdbHash>,
    ): List<KdbHash> {
        val set = hashes.filter { dag.hasCommit(it) }.toSet()
        if (set.isEmpty()) return emptyList()
        val commits = set.associateWith { dag.getCommitOrThrow(it) }
        val indegree = mutableMapOf<KdbHash, Int>()
        val children = mutableMapOf<KdbHash, MutableList<KdbHash>>()
        set.forEach { h ->
            val c = commits[h]!!
            indegree[h] = c.parentHashes.count { it in set }
            for (p in c.parentHashes) {
                if (p !in set) continue
                children.getOrPut(p) { mutableListOf() }.add(h)
            }
        }
        val q = ArrayDeque<KdbHash>()
        indegree
            .filter { it.value == 0 }
            .keys
            .sortedBy { it.toHex() }
            .forEach { q.addLast(it) }
        val out = mutableListOf<KdbHash>()
        while (q.isNotEmpty()) {
            val h = q.removeFirst()
            out.add(h)
            for (down in (children[h].orEmpty().sortedBy { it.toHex() })) {
                val left = (indegree[down] ?: 0) - 1
                indegree[down] = left
                if (left == 0) {
                    q.addLast(down)
                }
            }
        }
        if (out.size != set.size) {
            return set.sortedBy { it.toHex() }
        }
        return out
    }
}
