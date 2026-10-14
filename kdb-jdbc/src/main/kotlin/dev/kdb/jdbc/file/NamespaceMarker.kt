package dev.kdb.jdbc.file

import java.nio.file.Files
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Refuses a namespace whose state a log replay cannot rebuild: Go recorded shallow roots for it in
 * its marker (meta.json, go/kdb/embed/namespace_meta.go). A shallow root is a commit Go admitted
 * without its parents - a peer's snapshot it bootstrapped from, or a peer's head it caught up to
 * by squash (docs/kdb-memory-cassandra-plan.md C7) - whose documents no delta log frame holds;
 * only Go's checkpoint records them. Kotlin replays the log alone, so it would serve whatever the
 * log rebuilds: an empty namespace, or the older state from before a squash, with documents the
 * jump rewrote or deleted at their old values. Refusing is the only answer that is never wrong.
 *
 * Deepening the namespace from a peer (Go: `kdb deepen`, `POST /v1/ns/{ns}/deepen`) fetches and
 * logs the missing history and clears the marker's shallow roots, after which it opens here.
 */
public class ShallowNamespaceException(
    public val namespaceId: String,
    public val shallowRoots: List<String>,
) : IllegalStateException(
        "kdb: namespace $namespaceId has ${shallowRoots.size} shallow root(s) (first: ${shallowRoots.first()}): " +
            "Go installed state for it from a peer's snapshot or squash catch-up that only its checkpoint records, " +
            "and this build rebuilds a namespace from its delta log alone, so it cannot open it without serving wrong " +
            "values. Deepen the namespace from a peer in Go (kdb deepen, or POST /v1/ns/{ns}/deepen) to log the " +
            "missing history, then open it here.",
    )

internal object NamespaceMarker {
    /**
     * The shallow roots namespaceId's marker lists - empty when there is none, or no marker, or
     * one this build cannot parse (Kotlin's own markers carry no such field).
     */
    fun shallowRoots(
        dataRoot: String,
        namespaceId: String,
    ): List<String> {
        val path = NamespacePaths.metaFile(dataRoot, namespaceId)
        if (!Files.isRegularFile(path)) return emptyList()
        val marker =
            runCatching { Json.parseToJsonElement(Files.readString(path)) }.getOrNull() as? JsonObject
                ?: return emptyList()
        val roots = marker["shallowRoots"] as? JsonArray ?: return emptyList()
        return roots.mapNotNull { (it as? JsonPrimitive)?.takeIf { p -> p.isString }?.content }
    }

    /** Throws [ShallowNamespaceException] when namespaceId has shallow roots. */
    fun requireReplayable(
        dataRoot: String,
        namespaceId: String,
    ) {
        val roots = shallowRoots(dataRoot, namespaceId)
        if (roots.isNotEmpty()) throw ShallowNamespaceException(namespaceId, roots)
    }
}
