package dev.kdb.auth

/**
 * Structured view of a KDB resource address: database, optionally scoped down to a collection
 * and a single document. Storage/transaction code keeps addressing resources with a flat
 * `namespaceId: String` (`"database"` or `"database/collection"`); this type exists only in the
 * authorization layer, which needs to resolve grants at database, collection, and document
 * granularity. See docs/kdb-rbac-plan.md.
 */
public data class ResourcePath(
    val database: String,
    val collection: String? = null,
    val documentId: String? = null,
) {
    public val namespaceId: String
        get() = if (collection != null) "$database/$collection" else database

    /**
     * Candidate grant-match paths, most specific first: document, then collection, then
     * database. Grant resolution checks these in order so a database-level grant covers every
     * collection and document beneath it, while a document-level grant can be looked up first
     * once deny grants are introduced.
     */
    public fun candidatePaths(): List<String> =
        buildList {
            if (collection != null && documentId != null) add("$database/$collection/$documentId")
            if (collection != null) add("$database/$collection")
            add(database)
        }

    public companion object {
        public fun of(
            namespaceId: String,
            documentId: String? = null,
        ): ResourcePath {
            val slash = namespaceId.indexOf('/')
            return if (slash < 0) {
                ResourcePath(database = namespaceId, collection = null, documentId = documentId)
            } else {
                ResourcePath(
                    database = namespaceId.substring(0, slash),
                    collection = namespaceId.substring(slash + 1),
                    documentId = documentId,
                )
            }
        }
    }
}
