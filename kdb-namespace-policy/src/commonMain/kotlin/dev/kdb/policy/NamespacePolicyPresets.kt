package dev.kdb.policy

import dev.kdb.schema.KdbSchema
import dev.kdb.storage.IndexRetention
import dev.kdb.transaction.ConflictPolicy

public fun defaultMutable(
    namespaceId: String,
    schema: KdbSchema? = null,
): NamespacePolicy =
    NamespacePolicy(
        namespaceId = namespaceId,
        schema = schema,
        mode = NamespaceMode.MUTABLE,
        history = HistoryMode.FULL,
        conflict = ConflictPolicy.STRICT,
        compaction =
            CompactionPolicy(
                keepTagged = true,
                keepBranchPoints = true,
                squashAfter = SquashMode.AUTO,
                retainGranularity = defaultRetainGranularity(),
            ),
    )

public fun appendOnlyEvents(namespaceId: String, schema: KdbSchema): NamespacePolicy =
    NamespacePolicy(
        namespaceId = namespaceId,
        schema = schema,
        mode = NamespaceMode.APPEND_ONLY,
        history = HistoryMode.FULL,
        conflict = ConflictPolicy.APPEND_ONLY,
        compaction =
            CompactionPolicy(
                squashAfter = SquashMode.NEVER,
                retainGranularity = emptyList(),
            ),
    )

public fun scratchDocument(namespaceId: String): NamespacePolicy =
    defaultMutable(namespaceId, schema = null)

public fun cacheNoHistory(namespaceId: String): NamespacePolicy =
    NamespacePolicy(
        namespaceId = namespaceId,
        schema = null,
        mode = NamespaceMode.MUTABLE,
        history = HistoryMode.NONE,
        conflict = ConflictPolicy.LAST_WRITE,
        compaction = CompactionPolicy(squashAfter = SquashMode.NEVER, retainGranularity = emptyList()),
        indexRetentionDefault = IndexRetention.EVICTABLE,
    )
