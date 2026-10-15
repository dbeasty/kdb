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
    private val uniqueKeys: UniqueKeyRegistry? = null,
    private val blobPresence: BlobPresence? = null,
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
        val branchDocs = BranchDocuments(dag.namespaceId, storage, dag.getCommitOrThrow(ancestor).documentTreeHash)
        for (hash in ordered) {
            val mc = dag.getCommitOrThrow(hash)
            if (mc.operations.isEmpty()) continue
            val step =
                KdbTransaction(
                    id = mc.transactionId,
                    baseVersion = head,
                    operations = branchDocs.rebase(mc.operations, dag.getCommitOrThrow(head).documentTreeHash),
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
                is TransactionResult.Aborted -> return res
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
        val attachmentViolations =
            preflightAttachments(transaction, dag.namespaceId, dag, storage, blobPresence, targetDocTreeHash, anchorCommit)
        if (attachmentViolations.isNotEmpty()) {
            return TransactionResult.SchemaError(attachmentViolations)
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
                    ) +
                        detectAttachmentConflicts(
                            transaction,
                            dag.namespaceId,
                            dag,
                            storage,
                            baseDocTreeHash,
                            targetDocTreeHash,
                            anchorCommit,
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

        // Layer 16 §9.6: unique constraints are planned against the target tree before anything is
        // staged, and applied to the registry only after the commit is in the DAG. When this
        // transaction changes the schema (a migration adding a unique constraint), the registry is
        // rebuilt from the target tree under the new schema first - a pre-existing duplicate then
        // rejects the migration rather than silently surviving it.
        var effectiveRegistry = uniqueKeys
        var rebuiltRegistry: UniqueKeyRegistry? = null
        if (uniqueKeys != null && schemaFrame.rollingSchema.schemaHash != incomingSchema.schemaHash) {
            val fresh = UniqueKeyRegistry()
            try {
                fresh.rebuild(dag.namespaceId, storage, targetDocTreeHash, schemaFrame.rollingSchema)
            } catch (e: UniqueConstraintViolationException) {
                return TransactionResult.SchemaError(
                    listOf(
                        OperationViolation(
                            0,
                            transaction.operations.first(),
                            listOf(
                                FieldViolation(
                                    e.fields.joinToString(","),
                                    ViolationType.UNIQUE_CONSTRAINT,
                                    "value [${e.values.joinToString(",")}] already held by document ${e.ownerDocId}",
                                ),
                            ),
                        ),
                    ),
                )
            }
            effectiveRegistry = fresh
            rebuiltRegistry = fresh
        }
        val (uniquePlan, uniqueViolations) =
            planUniqueKeys(
                transaction,
                dag.namespaceId,
                storage,
                targetDocTreeHash,
                schemaFrame.rollingSchema,
                effectiveRegistry,
                writes,
            )
        if (uniqueViolations.isNotEmpty()) {
            return TransactionResult.SchemaError(uniqueViolations)
        }

        try {
            for ((idx, op) in transaction.operations.withIndex()) {
                when (op) {
                    is KdbOp.Write ->
                        writes[idx]?.let { storage.putDocument(dag.namespaceId, it) }

                    is KdbOp.Delete -> storage.deleteDocument(dag.namespaceId, op.docId)
                    else -> Unit
                }
            }
        } catch (t: Throwable) {
            // The write phase failed after validation/conflict checks passed - roll back
            // whatever was staged rather than leaving a half-applied transaction, and report
            // it distinctly from a hard error so callers can retry cleanly.
            storage.discardPending(dag.namespaceId)
            return TransactionResult.Aborted(t)
        }

        val newTree =
            storage.commitTree(dag.namespaceId, dag.getCommitOrThrow(anchorCommit).documentTreeHash)

        val schemaHashWire =
            if (schemaFrame.rollingSchema.isNone) {
                null
            } else {
                schemaFrame.rollingSchema.schemaHash
            }

        // Record what was stored, not what was asked for (Go: operationsAsStored in
        // go/kdb/transaction/default_engine.go). A merge-patch write is merged over the existing
        // document above, but replay (DeltaNamespaceReplayer, Go's delta_replay) reads a logged
        // write as the whole document - so logging the patch rebuilt a smaller document than the
        // one this commit's tree hash records. Substituting the staged document leaves one reading
        // of the op, and the commit hash covers it exactly as Go's does.
        val commit =
            dag.appendCommit(
                transaction.copy(operations = operationsAsStored(transaction.operations, writes)),
                anchorCommit,
                newTree,
                schemaHashWire,
                message,
            )

        if (uniqueKeys != null) {
            if (rebuiltRegistry != null) uniqueKeys.replaceWith(rebuiltRegistry)
            uniqueKeys.apply(uniquePlan.retract, uniquePlan.claim)
        }

        return TransactionResult.Success(commit, commit.documentTreeHash)
    }

    /**
     * A merge step's view of the branch being merged in. A logged write is the whole document the
     * branch stored (see [operationsAsStored]), so replaying it as a merge patch over the primary
     * would overwrite every field the primary changed concurrently. [rebase] instead turns each
     * write into the branch's own field-level change - against the document as the branch had it
     * before that write, folded forward from the common ancestor's tree - carried over onto the
     * primary's document, as a replace write. Deletes pass through. Go: branchDocuments.
     *
     * Each emitted write is a whole document, so the result does not depend on whether staging
     * resolves a write against the baseline or against the transaction's earlier operations.
     */
    private class BranchDocuments(
        private val namespaceId: String,
        private val storage: StorageAdapter,
        private val ancestorTree: KdbHash,
    ) {
        private val branch = mutableMapOf<KdbUuid, KdbDocument?>()

        suspend fun rebase(
            ops: List<KdbOp>,
            primaryTree: KdbHash,
        ): List<KdbOp> {
            val primary = mutableMapOf<KdbUuid, KdbDocument?>()
            return ops.map { op ->
                when (op) {
                    is KdbOp.Write -> {
                        val before = if (op.docId in branch) branch[op.docId] else storage.getDocument(namespaceId, op.docId, ancestorTree)
                        val onto = if (op.docId in primary) primary[op.docId] else storage.getDocument(namespaceId, op.docId, primaryTree)
                        val after = KdbDocument(op.docId, op.patch)
                        // Not a JSON object: leave it for staging to reject as it always has.
                        val rebased =
                            try {
                                after.rebasedOnto(before, onto)
                            } catch (_: DocumentDecodeException) {
                                return@map op
                            }
                        branch[op.docId] = after
                        primary[op.docId] = rebased
                        KdbOp.Write(op.docId, rebased.json, replace = true)
                    }
                    is KdbOp.Delete -> {
                        branch[op.docId] = null
                        primary[op.docId] = null
                        op
                    }
                    else -> op
                }
            }
        }
    }

    /**
     * Replaces each [KdbOp.Write]'s patch with the document that was actually staged for it, leaving
     * every other operation untouched. The result is never a replace write: replace is a property
     * of the request only and not part of the encoded op.
     */
    private fun operationsAsStored(
        ops: List<KdbOp>,
        writes: Map<Int, KdbDocument>,
    ): List<KdbOp> {
        if (writes.isEmpty()) return ops
        return ops.mapIndexed { index, op ->
            val doc = writes[index]
            if (op is KdbOp.Write && doc != null) KdbOp.Write(op.docId, doc.json) else op
        }
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
                            if (baseDoc != null && !op.replace) {
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

    /** Detects an idempotent retry: the same transaction id already produced a commit with
     * these exact parents, so the caller should see that original result rather than attempt a
     * duplicate commit. O(1) via [CommitDag.getCommitByTransactionId] - this used to walk up to
     * 8192 commits of history on every single commit/replay call regardless of whether a retry
     * was actually happening, which measured as the dominant cost (~88% of all allocation in a
     * profiled Go-side run - both implementations share this exact pattern) behind
     * kdb-service getting OOM-killed under sustained write load once history grew past a few
     * thousand commits (docs/benchmarks/lightsail-sim/README.md). [anchorCommit] is unused now
     * that the lookup is keyed by transaction id rather than a history walk from it, but stays
     * in the signature to avoid touching call sites. */
    private suspend fun findExistingCommit(
        transaction: KdbTransaction,
        dag: CommitDag,
        anchorCommit: KdbHash,
        parents: List<KdbHash>,
    ): KdbCommit? {
        val commit = dag.getCommitByTransactionId(transaction.id) ?: return null
        return commit.takeIf { it.parentHashes == parents }
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
                        documentId = c.op.addressedDocId()?.toString() ?: "",
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
