package dev.kdb.compaction

import dev.kdb.codec.KdbHash
import dev.kdb.storage.StorageAdapter

public interface OrphanBlobGc {
    public suspend fun sweep(
        namespaceId: String,
        reachableHashes: Set<KdbHash>,
    ): Long
}

public class DefaultOrphanBlobGc(
    @Suppress("UNUSED_PARAMETER") private val storage: StorageAdapter,
) : OrphanBlobGc {
    override suspend fun sweep(
        namespaceId: String,
        reachableHashes: Set<KdbHash>,
    ): Long {
        // v1: storage adapters do not expose blob enumeration; no-op GC
        return 0L
    }
}
