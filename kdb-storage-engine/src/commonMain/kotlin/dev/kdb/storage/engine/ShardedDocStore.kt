package dev.kdb.storage.engine

import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Replaces a single docsMu-guarded map with [SHARD_COUNT] independently
 * locked partitions, keyed by [KdbUuid.lsb]. put/get/delete for different
 * documents proceed in parallel as long as they land in different
 * shards, instead of serializing behind one namespace-wide mutex.
 * Mirrors go/kdb/storage/engine/doc_shard.go - see Phase 2 of
 * docs/benchmarks/phase0-baseline.md.
 */
public class ShardedDocStore {
    private companion object {
        const val SHARD_COUNT = 64
    }

    private class Shard {
        val mutex = Mutex()
        val docs = mutableMapOf<KdbUuid, KdbDocument>()
    }

    private val shards = Array(SHARD_COUNT) { Shard() }

    private fun shardFor(id: KdbUuid): Shard {
        // KdbUuid values are random, so the low bits of lsb are already
        // uniformly distributed - no need for a stronger hash.
        val idx = (id.lsb.toInt() and Int.MAX_VALUE) % SHARD_COUNT
        return shards[idx]
    }

    public suspend fun put(document: KdbDocument) {
        val sh = shardFor(document.id)
        sh.mutex.withLock { sh.docs[document.id] = document }
    }

    public suspend fun get(id: KdbUuid): KdbDocument? {
        val sh = shardFor(id)
        return sh.mutex.withLock { sh.docs[id] }
    }

    public suspend fun delete(id: KdbUuid) {
        val sh = shardFor(id)
        sh.mutex.withLock { sh.docs.remove(id) }
    }

    /** Snapshot across all shards. Each shard is locked only for the duration of copying its own entries. */
    public suspend fun snapshot(): List<KdbDocument> {
        val out = mutableListOf<KdbDocument>()
        for (sh in shards) {
            sh.mutex.withLock { out.addAll(sh.docs.values) }
        }
        return out
    }
}
