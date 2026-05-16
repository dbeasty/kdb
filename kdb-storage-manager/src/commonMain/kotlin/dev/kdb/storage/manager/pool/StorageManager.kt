package dev.kdb.storage.manager.pool

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import dev.kdb.storage.*
import dev.kdb.storage.engine.DefaultStorageEngineFactory
import dev.kdb.storage.engine.StorageEngineHandle
import dev.kdb.storage.engine.StorageEngineTarget
import dev.kdb.storage.manager.eviction.DefaultEvictionManager
import dev.kdb.storage.manager.eviction.EvictionManager
import dev.kdb.storage.manager.rebuild.DefaultRebuildScheduler
import dev.kdb.storage.manager.rebuild.RebuildScheduler
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public interface StorageManager {
    public companion object {
        private var installed: StorageManager? = null

        public fun install(instance: StorageManager) {
            require(installed == null) { "StorageManager already installed" }
            installed = instance
        }

        public fun get(): StorageManager =
            installed ?: throw StorageManagerNotInstalledException("StorageManager not installed")
    }

    public val config: StorageEngineConfig
    public val realizedBytesInUse: StateFlow<Long>
    public suspend fun requestRealized(
        enlistmentId: KdbUuid,
        commitHash: KdbHash,
        blockingPolicy: RebuildBlockingPolicy = RebuildBlockingPolicy.WAIT,
    ): RealizedStoreHandle
}

public class StorageManagerNotInstalledException(message: String) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

public class DefaultStorageManager(
    override val config: StorageEngineConfig,
    private val target: StorageEngineTarget = StorageEngineTarget.SERVER,
) : StorageManager {
    private val pool = RealizedStorePool(config, target)
    private val eviction: EvictionManager = DefaultEvictionManager(pool)
    private val rebuild: RebuildScheduler = DefaultRebuildScheduler(pool)

    override val realizedBytesInUse: StateFlow<Long> = pool.realizedBytesInUse

    override suspend fun requestRealized(
        enlistmentId: KdbUuid,
        commitHash: KdbHash,
        blockingPolicy: RebuildBlockingPolicy,
    ): RealizedStoreHandle = pool.acquire(enlistmentId, commitHash, blockingPolicy, eviction, rebuild)
}

public class RealizedStorePool(
    private val config: StorageEngineConfig,
    private val target: StorageEngineTarget,
) {
    private val mutex = Mutex()
    private val handles = mutableMapOf<Pair<KdbUuid, KdbHash>, PooledRealizedStoreHandle>()
    private val engines = mutableMapOf<String, StorageEngineHandle>()
    private var defaultEngine: StorageEngineHandle? = null
    private val _bytes = MutableStateFlow(0L)
    val realizedBytesInUse: StateFlow<Long> = _bytes

    suspend fun acquire(
        enlistmentId: KdbUuid,
        commitHash: KdbHash,
        blockingPolicy: RebuildBlockingPolicy,
        eviction: EvictionManager,
        rebuild: RebuildScheduler,
    ): RealizedStoreHandle =
        mutex.withLock {
            val key = enlistmentId to commitHash
            handles.getOrPut(key) {
                val engine =
                    defaultEngine ?: DefaultStorageEngineFactory(target).open("default", config).also {
                        defaultEngine = it
                    }
                PooledRealizedStoreHandle(
                    enlistmentId,
                    commitHash,
                    engine.adapter,
                    onRelease = { eviction.onHandleReleased(enlistmentId) },
                )
            }.also { it.refCount++ }
        }
}

public class PooledRealizedStoreHandle(
    override val enlistmentId: KdbUuid,
    override val commitHash: KdbHash,
    override val storage: StorageAdapter,
    private val onRelease: () -> Unit,
) : RealizedStoreHandle {
    override val namespaceId: String = "default"
    var refCount: Int = 1
    override val isReady: Boolean = true

    override suspend fun awaitReady(blockingPolicy: RebuildBlockingPolicy) {}

    override fun close() {
        if (--refCount <= 0) onRelease()
    }

    override fun onIndexPinViolation(handler: (IndexPinViolationEvent) -> Unit) {}
}
