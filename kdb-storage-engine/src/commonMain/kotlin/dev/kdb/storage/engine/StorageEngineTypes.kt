package dev.kdb.storage.engine

import dev.kdb.storage.DeltaSegmentReader
import dev.kdb.storage.DeltaSegmentWriter
import dev.kdb.storage.EvictableStorageAdapter
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.StorageEngineConfig

public enum class StorageEngineTarget {
    SERVER,
    BROWSER,
    IN_MEMORY,
    GPU,
}

public interface StorageEngineFactory {
    public val target: StorageEngineTarget
    public suspend fun open(namespaceId: String, config: StorageEngineConfig): StorageEngineHandle

    public companion object {
        public fun forTarget(target: StorageEngineTarget, sharedConfig: StorageEngineConfig): StorageEngineFactory =
            DefaultStorageEngineFactory(target)
    }
}

public interface StorageEngineHandle : AutoCloseable {
    public val namespaceId: String
    public val adapter: StorageAdapter
    public val deltaWriter: DeltaSegmentWriter?
    public val deltaReader: DeltaSegmentReader?
    override fun close()
}

public interface StorageEngine : EvictableStorageAdapter {
    public val namespaceId: String
}
