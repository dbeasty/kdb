package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.document.AttachmentException
import dev.kdb.document.AttachmentTable
import dev.kdb.document.KdbCommit

/*
 * Attachment tables (docs/kdb-attachments-plan.md; Go: go/kdb/dag/attachments.go).
 *
 * Every commit records the root of its namespace's attachment table (KdbCommit.attachmentTreeHash,
 * null for an empty table). The DAG derives it: a commit's table is its first parent's table with
 * the commit's operations applied (AttachmentTable.apply), so every way of appending a commit
 * records the right root without its caller computing anything. A table not cached is rebuilt from
 * the nearest ancestor whose table is known (or whose root is null, meaning empty), applying
 * operations forward and checking each step against the root the commit recorded: a log or cache
 * that disagrees with a commit is an error, never a wrong attachment.
 */

/** How many derived tables a DAG keeps besides seeded ones. Versions share trie nodes. */
internal const val ATTACHMENT_CACHE_SIZE: Int = 64

/** A small insertion-ordered cache of tables by root. Callers synchronise. */
internal class AttachmentTableCache {
    private val tables = LinkedHashMap<KdbHash, AttachmentTable>()

    fun get(root: KdbHash): AttachmentTable? = tables[root]

    fun put(
        root: KdbHash,
        table: AttachmentTable,
    ) {
        if (tables.containsKey(root)) return
        tables[root] = table
        while (tables.size > ATTACHMENT_CACHE_SIZE) tables.remove(tables.keys.first())
    }
}

/**
 * Derives the attachment table at [commit] by walking first parents through [lookup] until a commit
 * with no root, a cached table ([cached]) or a root commit, then replaying forward and checking every
 * step's root. [remember] is told each verified table.
 */
internal inline fun deriveAttachmentTable(
    namespaceId: String,
    commit: KdbHash,
    lookup: (KdbHash) -> KdbCommit?,
    cached: (KdbHash) -> AttachmentTable?,
    remember: (KdbHash, AttachmentTable) -> Unit,
): AttachmentTable {
    val chain = ArrayList<KdbCommit>() // commits whose table must be built, newest first
    var at = commit
    var base = AttachmentTable.EMPTY
    while (true) {
        val c = lookup(at) ?: throw DagConsistencyException("attachment table: commit not present", namespaceId, at)
        val root = c.attachmentTreeHash ?: break // an empty table: the base
        val t = cached(root)
        if (t != null) {
            base = t
            break
        }
        chain.add(c)
        if (c.parentHashes.isEmpty()) break // a root commit with attachments: built from empty
        at = c.parentHashes[0]
    }
    var table = base
    for (i in chain.indices.reversed()) {
        val c = chain[i]
        val next =
            try {
                table.apply(c.operations)
            } catch (e: AttachmentException) {
                throw DagConsistencyException("attachment table: ${e.message}", namespaceId, c.hash, e)
            }
        if (next.root != c.attachmentTreeHash) {
            throw DagConsistencyException(
                "attachment table: replaying the commit's ${c.operations.size} operations does not reproduce its attachment root",
                namespaceId,
                c.hash,
            )
        }
        remember(c.attachmentTreeHash!!, next)
        table = next
    }
    return table
}

/** Uncached derivation through the public [CommitDag] API - the default for implementations that
 * do not keep a cache of their own. */
internal suspend fun CommitDag.deriveAttachmentTableUncached(commit: KdbHash): AttachmentTable =
    deriveAttachmentTable(namespaceId, commit, { getCommit(it) }, { null }, { _, _ -> })
