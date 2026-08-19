package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.error.ConflictException
import dev.kdb.transaction.MergeBaseNotFoundException
import dev.kdb.peersync.PeerSession
import dev.kdb.peersync.computeSyncPlan
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.transaction.TransactionAbortedException
import dev.kdb.transaction.TransactionEngine
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.transactionEngine

public data class BranchActionResult(
    val appliedCommits: Int,
    val head: KdbHash,
    val commonAncestor: KdbHash? = null,
    val localBranch: String? = null,
    val incomingBranch: String? = null,
)

public suspend fun currentHeadHex(runtime: EmbeddedKdbRuntime): String = runtime.dag.head().toHex()

public fun writeBaseVersion(runtime: EmbeddedKdbRuntime): KdbHash? = runtime.writeBaseVersion

public fun setWriteBaseVersion(
    runtime: EmbeddedKdbRuntime,
    hash: KdbHash?,
) {
    runtime.writeBaseVersion = hash
}

public fun setWriteBaseVersionHex(
    runtime: EmbeddedKdbRuntime,
    hex: String?,
) {
    runtime.writeBaseVersion = hex?.let { KdbHash.fromHex(it) }
}

/**
 * Apply peer commits through [remoteHead] and advance `main` to that tip.
 */
public suspend fun acceptRemoteChanges(
    runtime: EmbeddedKdbRuntime,
    session: PeerSession,
    namespaceId: String,
    remoteHead: KdbHash,
    schema: KdbSchema = runtime.schema,
): BranchActionResult {
    val localHead = runtime.dag.head()
    if (localHead == remoteHead) {
        runtime.writeBaseVersion = null
        return BranchActionResult(0, localHead, localHead)
    }
    val plan = computeSyncPlan(runtime.dag, localHead, remoteHead)
    val since = plan.commonAncestor ?: throw MergeBaseNotFoundException("disjoint history", localHead, remoteHead)
    val applied = applyFetchedCommits(runtime.dag, session.fetchCommitsSince(since))
    runtime.dag.setHead("main", remoteHead)
    runtime.writeBaseVersion = null
    materializeAfterBranchAction(runtime, namespaceId, schema)
    return BranchActionResult(
        appliedCommits = applied,
        head = remoteHead,
        commonAncestor = since,
    )
}

/**
 * Fork away from [remoteHead]: rewind `main` to the common ancestor and set the write parent
 * so the next [putJson] creates a sibling branch. Remote commits are fetched and kept on
 * [incomingBranchName] without moving `main` to them.
 */
public suspend fun rejectRemoteChanges(
    runtime: EmbeddedKdbRuntime,
    session: PeerSession,
    namespaceId: String,
    remoteHead: KdbHash,
    schema: KdbSchema = runtime.schema,
    incomingBranchName: String = "incoming",
    localBranchName: String = "local",
): BranchActionResult {
    val localHead = runtime.dag.head()
    val fetched = session.fetchCommitsSince(localHead)
    applyFetchedCommits(runtime.dag, fetched)
    val plan = computeSyncPlan(runtime.dag, localHead, remoteHead)
    val rejectBase =
        plan.commonAncestor
            ?: throw MergeBaseNotFoundException("disjoint history", localHead, remoteHead)
    var incomingBranch: String? = null
    var localBranch: String? = null
    if (runtime.dag.hasCommit(remoteHead) && remoteHead != rejectBase) {
        ensureBranch(runtime.dag, incomingBranchName, remoteHead)
        incomingBranch = incomingBranchName
    }
    if (localHead != rejectBase) {
        ensureBranch(runtime.dag, localBranchName, localHead)
        localBranch = localBranchName
    }
    runtime.dag.setHead("main", rejectBase)
    runtime.writeBaseVersion = rejectBase
    materializeAfterBranchAction(runtime, namespaceId, schema)
    return BranchActionResult(
        appliedCommits = 0,
        head = rejectBase,
        commonAncestor = rejectBase,
        localBranch = localBranch,
        incomingBranch = incomingBranch,
    )
}

public suspend fun mergeBranches(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    primaryBranch: String,
    mergedBranch: String,
    schema: KdbSchema = runtime.schema,
    engine: TransactionEngine? = null,
): BranchActionResult {
    val primaryHead = runtime.dag.getBranchOrThrow(primaryBranch).headHash
    val mergedHead = runtime.dag.getBranchOrThrow(mergedBranch).headHash
    val policy = runtime.policyRegistry.get(namespaceId)
    val txEngine = engine ?: transactionEngine(policy.conflict)
    when (
        val result =
            txEngine.merge(
                primaryHead = primaryHead,
                mergedHead = mergedHead,
                dag = runtime.dag,
                storage = runtime.storage,
                schema = schema,
            )
    ) {
        is TransactionResult.Success -> {
            runtime.dag.setHead("main", result.commit.hash)
            runtime.writeBaseVersion = null
            materializeAfterBranchAction(runtime, namespaceId, schema)
            return BranchActionResult(
                appliedCommits = 1,
                head = result.commit.hash,
                commonAncestor = primaryHead,
            )
        }
        is TransactionResult.Conflict ->
            throw ConflictException(
                "merge conflict: ${result.report.conflicts.size} operation(s)",
                result.report,
            )
        is TransactionResult.SchemaError ->
            throw IllegalArgumentException(
                "schema rejection: ${result.violations.size} violation(s)",
            )
        is TransactionResult.Aborted ->
            throw TransactionAbortedException(
                "merge aborted: ${result.cause.message ?: result.cause.toString()}",
                result.cause,
            )
    }
}

public fun branchActionResultJson(result: BranchActionResult): String {
    val local = result.localBranch?.let { ""","localBranch":"$it"""" } ?: ""
    val incoming = result.incomingBranch?.let { ""","incomingBranch":"$it"""" } ?: ""
    val ancestor = result.commonAncestor?.let { ""","commonAncestor":"${it.toHex()}"""" } ?: ""
    return """{"appliedCommits":${result.appliedCommits},"head":"${result.head.toHex()}"$ancestor$local$incoming}"""
}

private suspend fun applyFetchedCommits(
    dag: CommitDag,
    commits: List<dev.kdb.document.KdbCommit>,
): Int {
    var applied = 0
    for (commit in commits) {
        if (dag.hasCommit(commit.hash)) continue
        dag.putCommit(commit, requireParents = true)
        applied++
    }
    return applied
}

private suspend fun ensureBranch(
    dag: CommitDag,
    name: String,
    fromHash: KdbHash,
) {
    if (dag.getBranch(name) == null) {
        dag.createBranch(name, fromHash)
    } else {
        dag.setHead(name, fromHash)
    }
}

private suspend fun materializeAfterBranchAction(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    schema: KdbSchema,
) {
    materializeCommitHistory(runtime, namespaceId, schema)
    if (!schema.isNone) {
        syncEmbedSchema(runtime, namespaceId, schema)
    }
}
