package dev.kdb.jdbc.file

import java.nio.file.Files
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Refuses a namespace whose state its delta log cannot rebuild and whose Go checkpoint - the only
 * other record of it - is missing or unusable here. Go recorded shallow roots for it in its marker
 * (meta.json, go/kdb/embed/namespace_meta.go): commits Go admitted without their parents - a
 * peer's snapshot it bootstrapped from, or a peer's head it caught up to by squash
 * (docs/kdb-memory-cassandra-plan.md C7) - whose documents no delta log frame holds. Kotlin opens
 * such a namespace from Go's checkpoint (CheckpointOpen.kt); a replay of the log alone would serve
 * an empty namespace, or the older state from before a squash, so when the checkpoint cannot be
 * used refusing is the only answer that is never wrong. Go refuses the same way
 * (SnapshotCheckpointMissingError).
 *
 * Opening the namespace in Go (which rewrites its checkpoint), or deepening it from a peer (Go:
 * `kdb deepen`, `POST /v1/ns/{ns}/deepen`, which logs the missing history and clears the marker's
 * shallow roots), makes it open here again.
 */
public class ShallowNamespaceException(
    public val namespaceId: String,
    public val shallowRoots: List<String>,
    public val checkpointProblem: String,
) : IllegalStateException(
        "kdb: namespace $namespaceId has ${shallowRoots.size} shallow root(s) (first: ${shallowRoots.first()}): " +
            "Go installed state for it from a peer's snapshot or squash catch-up that only its checkpoint records, " +
            "and the checkpoint cannot be used here ($checkpointProblem), so it cannot be opened by replaying the " +
            "delta log without serving wrong values. Open it in Go to rewrite the checkpoint, or deepen it from a " +
            "peer (kdb deepen, or POST /v1/ns/{ns}/deepen) to log the missing history, then open it here.",
    )

/**
 * A namespace whose oldest delta segments were deleted on purpose (Go's history=none truncation)
 * and whose checkpoint - the only record of what they held - cannot be used here. Replaying what is
 * left would open with everything below the floor silently gone. Go's TruncatedLogError.
 */
public class TruncatedLogException(
    public val namespaceId: String,
    public val floor: Long,
    public val checkpointProblem: String,
) : IllegalStateException(
        "kdb: namespace \"$namespaceId\" cannot be opened: its delta log has been truncated (oldest segment is " +
            "$floor), so its checkpoint is the only record of everything below that - and the checkpoint cannot be " +
            "used here ($checkpointProblem). Open it in Go, or restore it from a backup.",
    )

/** What Go's marker (meta.json) says about a namespace's history horizon. */
internal data class NamespaceMarkerRoots(
    /** Snapshot and squash roots: state only the checkpoint records. */
    val shallowRoots: List<String>,
    /** Peers' roots grafted in as side history: a replay admits them without their parents. */
    val graftRoots: List<String>,
)

internal object NamespaceMarker {
    /**
     * The roots namespaceId's marker lists - empty when there is none, or no marker, or one this
     * build cannot parse (Kotlin's own markers carry no such fields).
     */
    fun roots(
        dataRoot: String,
        namespaceId: String,
    ): NamespaceMarkerRoots {
        val none = NamespaceMarkerRoots(emptyList(), emptyList())
        val path = NamespacePaths.metaFile(dataRoot, namespaceId)
        if (!Files.isRegularFile(path)) return none
        val marker =
            runCatching { Json.parseToJsonElement(Files.readString(path)) }.getOrNull() as? JsonObject
                ?: return none
        fun list(field: String): List<String> =
            (marker[field] as? JsonArray)?.mapNotNull { (it as? JsonPrimitive)?.takeIf { p -> p.isString }?.content }
                ?: emptyList()
        return NamespaceMarkerRoots(list("shallowRoots"), list("graftRoots"))
    }

    fun shallowRoots(
        dataRoot: String,
        namespaceId: String,
    ): List<String> = roots(dataRoot, namespaceId).shallowRoots
}
