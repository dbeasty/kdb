package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbBranch
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbTag

/**
 * One commit as a checkpoint records it (Go: dag.CheckpointCommit): every field but its
 * operations, which stay in the delta log. [operationCount] tells a commit whose operations were
 * left out from one that wrote nothing; only the former is loaded back when something reads it.
 * [commit]'s hash is the one the checkpoint stored, not recomputed - it covers the missing
 * operations.
 */
public data class CheckpointCommit(
    val commit: KdbCommit,
    val operationCount: Int,
)

/**
 * A DAG that can be rebuilt from Go's namespace checkpoint instead of from the whole delta log -
 * the Kotlin side of Go's RestoreCheckpoint, RestoreCommitOperations and PutShallowCommit
 * (go/kdb/dag/checkpoint.go).
 *
 * A commit restored without its operations is never handed out without them: every read of one
 * loads them through the [setOperationsLoader] loader, checks them against the commit's hash, and
 * throws [DagConsistencyException] when they cannot be had. A missing operation list is a loud
 * failure, never a commit that looks like it wrote nothing.
 */
public interface CheckpointRestorableDag {
    /**
     * Stores [commits] without verifying their hashes or requiring their parents (they arrive in
     * no particular order, and the checkpoint carries no operations to hash), then sets every
     * branch and tag named.
     */
    public suspend fun restoreCheckpoint(
        commits: Sequence<CheckpointCommit>,
        branches: List<KdbBranch>,
        tags: List<KdbTag>,
    )

    /**
     * Puts operations back onto commits restored without them. Each commit's hash is recomputed
     * and must match, or nothing further is installed and [DagConsistencyException] is thrown.
     */
    public suspend fun restoreCommitOperations(commits: List<KdbCommit>)

    /** Admits [commit] - hash verified - without its parents: a shallow root (Go PutShallowCommit). */
    public suspend fun putShallowCommit(commit: KdbCommit)

    /**
     * Where operations of commits restored without them come from: the commit, operations and
     * all, or null when it cannot be found. Called with the DAG locked; must not call back into it.
     */
    public fun setOperationsLoader(loader: (suspend (KdbHash) -> KdbCommit?)?)

    /** How many restored commits still have their operations only in the log. */
    public suspend fun commitsWithoutOperations(): Int

    /**
     * Every commit held, as a checkpoint records it (Go: the commits of CheckpointSnapshot), with
     * the operation count of those whose operations are still only in the log - for writing a
     * checkpoint. Null when the DAG holds what a checkpoint cannot carry (archived commit stubs).
     */
    public suspend fun checkpointCommits(): List<CheckpointCommit>?
}
